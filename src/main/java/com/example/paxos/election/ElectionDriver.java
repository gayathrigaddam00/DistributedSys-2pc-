package com.example.paxos.election;

import com.example.paxos.core.PaxosCore;
import com.example.paxos.twophase.TransactionCoordinator;
import com.example.paxos.v1.*;
import io.grpc.Deadline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class ElectionDriver implements DisposableBean {
    private static final Logger log = LoggerFactory.getLogger(ElectionDriver.class);

    private final PaxosCore core;
    private final String nodeId;
    private final Map<String, PaxosNodeGrpc.PaxosNodeBlockingStub> peers;
    private final long rpcDeadlineMs;
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private final int myRank;
    private static final int CLUSTER_SIZE = 3; 
    
    private final TransactionCoordinator transactionCoordinator;

    public ElectionDriver(
            PaxosCore core,
            @Value("${paxos.nodeId}") String nodeId,
            Map<String, PaxosNodeGrpc.PaxosNodeBlockingStub> peers,
            @Value("${paxos.rpc.deadlineMs:200}") long rpcDeadlineMs,
            @Value("${paxos.leaderTimeoutMs:2000}") long ignored,
            @Lazy TransactionCoordinator transactionCoordinator  
    ) {
        this.core = core;
        this.nodeId = nodeId;
        this.peers = peers;
        this.rpcDeadlineMs = rpcDeadlineMs;
        this.transactionCoordinator = transactionCoordinator;  
        
        int idNum = Integer.parseInt(nodeId.substring(1));
        this.myRank = (idNum - 1) % 3;
        log.info("[{}] Lazy Election Driver Initialized. My Rank: {}", nodeId, myRank);
    }

    public void startTimers() {
        log.info("[{}] Timers disabled (Lazy Mode). Waiting for TestRunner F/R triggers.", nodeId);
    }

    public void stopTimers() {
    }

    public void recordLeaderActivity() {
    }

    public void startElection() {
        if (!core.isActive()) {
            log.debug("[{}] Node inactive, skipping election", nodeId);
            return;
        }

        if (electionInProgress.get()) {
            log.info("[{}] Election already in progress, skipping", nodeId);
            return;
        }

        long currentEpoch = core.getHighestSeen().getEpoch();
        long targetEpoch = currentEpoch;

        do {
            targetEpoch++;
        } while ((targetEpoch - 1) % 3 != myRank);

        log.info("[{}] Starting Election.)", 
            nodeId, currentEpoch, targetEpoch, myRank);
            
        runElection(targetEpoch);
    }

    private void runElection(long epochToPropose) {
        if (!electionInProgress.compareAndSet(false, true)) {
            return;
        }

        ExecutorService fanout = null;
        
        try {
            PrepareReq prepare = core.buildPrepare(epochToPropose);
            Ballot b = prepare.getB();

            log.info("[{}] Sending PREPARE for {}", nodeId, fmt(b));
            PromiseRes selfPromise = core.onPrepare(prepare);
            List<PromiseRes> validPromises = Collections.synchronizedList(new ArrayList<>());
            validPromises.add(selfPromise);
            int neededVotes = (CLUSTER_SIZE / 2) + 1;
            log.info("[{}] Need {} votes out of {} total nodes (including self)", 
                nodeId, neededVotes, CLUSTER_SIZE);
            CompletableFuture<Void> quorumReached = new CompletableFuture<>();
            fanout = Executors.newFixedThreadPool(Math.max(1, peers.size()));
            for (Map.Entry<String, PaxosNodeGrpc.PaxosNodeBlockingStub> e : peers.entrySet()) {
                if (e.getKey().equals(nodeId)) continue;
                
                final String peerId = e.getKey();
                final PaxosNodeGrpc.PaxosNodeBlockingStub stub = e.getValue();
                
                fanout.submit(() -> {
                    try {
                        PromiseRes res = stub
                                .withDeadline(Deadline.after(rpcDeadlineMs, TimeUnit.MILLISECONDS))
                                .prepare(prepare);
                        
                        if (res.getB().getEpoch() == b.getEpoch()) {
                            validPromises.add(res);
                            log.debug("[{}] Got promise from {} ({}/{})", 
                                nodeId, peerId, validPromises.size(), neededVotes);
                            if (validPromises.size() >= neededVotes && !quorumReached.isDone()) {
                                quorumReached.complete(null);
                            }
                        }
                    } catch (Exception ex) {
                        log.debug("[{}] Failed to get promise from {}: {}", nodeId, peerId, ex.getMessage());
                    }
                });
            }

            try {
                quorumReached.get(rpcDeadlineMs + 100, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                log.debug("[{}] Timeout waiting for quorum", nodeId);
            } catch (Exception e) {
                log.debug("[{}] Exception waiting for quorum: {}", nodeId, e.getMessage());
            }

            if (validPromises.size() >= neededVotes) {
                log.info("[{}] QUORUM REACHED ({}/{}). I am now LEADER for {}", 
                    nodeId, validPromises.size(), CLUSTER_SIZE, fmt(b));
                core.becomeLeader(b);
                
                PaxosCore.NewViewResult result = core.buildNewViewFromPromises(b, new ArrayList<>(validPromises));
                core.updateSeqAfterElection(result.maxSeq());
                
                core.onNewView(result.req()); 
                broadcastNewView(result.req());
                recoverPreparedTransactions();
            } else {
                log.warn("[{}] ELECTION FAILED. Only got {}/{} votes (needed {}).", 
                    nodeId, validPromises.size(), CLUSTER_SIZE, neededVotes);
            }
        } catch (Exception ex) {
            log.error("[{}] Election fatal error", nodeId, ex);
        } finally {
            if (fanout != null) {
                fanout.shutdownNow();
                try {
                    fanout.awaitTermination(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            electionInProgress.set(false);
        }
    }

    private void recoverPreparedTransactions() {
        Map<String, Long> preparedTxns = core.getPreparedCrossShardTxns();
        
        if (preparedTxns.isEmpty()) {
            log.info("[{}] No prepared cross-shard transactions to recover", nodeId);
            return;
        }
        
        log.info("[{}] Recovering {} prepared cross-shard transactions", nodeId, preparedTxns.size());
        transactionCoordinator.recoverPreparedTransactions();
    }

    private void broadcastNewView(NewViewReq nv) {
        for (Map.Entry<String, PaxosNodeGrpc.PaxosNodeBlockingStub> e : peers.entrySet()) {
            if (e.getKey().equals(nodeId)) continue;
            
            final String peerId = e.getKey();
            final PaxosNodeGrpc.PaxosNodeBlockingStub stub = e.getValue();
            
            CompletableFuture.runAsync(() -> {
                try {
                    stub.withDeadline(Deadline.after(rpcDeadlineMs, TimeUnit.MILLISECONDS))
                        .newView(nv);
                    log.debug("[{}] Sent NewView to {}", nodeId, peerId);
                } catch (Exception ex) {
                    log.debug("[{}] Failed to send NewView to {}: {}", nodeId, peerId, ex.getMessage());
                }
            });
        }
    }

    private static String fmt(Ballot b) {
        return "E" + b.getEpoch() + ":" + b.getLeaderId();
    }

    @Override
    public void destroy() {
        log.info("[{}] ElectionDriver shutting down", nodeId);
    }
}
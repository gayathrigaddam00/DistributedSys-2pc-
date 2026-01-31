package com.example.paxos.twophase;

import com.example.paxos.core.PaxosCore;
import com.example.paxos.lock.LockManager;
import com.example.paxos.storage.SQLiteStorage;
import com.example.paxos.v1.*;
import com.example.paxos.wal.WriteAheadLog;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class TransactionCoordinator {
    private static final Logger log = LoggerFactory.getLogger(TransactionCoordinator.class);

    private final String nodeId;
    private final String clusterId;
    private final PaxosCore paxosCore;
    private final LockManager lockManager;
    private final SQLiteStorage storage;
    private final WriteAheadLog wal;
    private final Map<String, Map<String, TwoPhaseCommitGrpc.TwoPhaseCommitBlockingStub>> crossClusterStubs;
    private final Map<String, PaxosNodeGrpc.PaxosNodeBlockingStub> peerStubs;
    private final long twoPhaseTimeoutMs;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    private final ConcurrentHashMap<String, TwoPCState> pendingTxns = new ConcurrentHashMap<>();

    public static class TwoPCState {
        public final Txn txn;
        public volatile long seqNum;
        public final boolean isCoordinator;
        public volatile boolean prepared;
        public volatile boolean committed;
        public volatile boolean aborted;
        public final long startTime;

        public TwoPCState(Txn txn, boolean isCoordinator) {
            this.txn = txn;
            this.seqNum = -1;
            this.isCoordinator = isCoordinator;
            this.prepared = false;
            this.committed = false;
            this.aborted = false;
            this.startTime = System.currentTimeMillis();
        }
    }

    public TransactionCoordinator(
            String nodeId,
            String clusterId,
            PaxosCore paxosCore,
            LockManager lockManager,
            SQLiteStorage storage,
            WriteAheadLog wal,
            Map<String, Map<String, TwoPhaseCommitGrpc.TwoPhaseCommitBlockingStub>> crossClusterStubs,
            Map<String, PaxosNodeGrpc.PaxosNodeBlockingStub> peerStubs,
            long twoPhaseTimeoutMs) {
        this.nodeId = nodeId;
        this.clusterId = clusterId;
        this.paxosCore = paxosCore;
        this.lockManager = lockManager;
        this.storage = storage;
        this.wal = wal;
        this.crossClusterStubs = crossClusterStubs;
        this.peerStubs = peerStubs;
        this.twoPhaseTimeoutMs = twoPhaseTimeoutMs;
    }

    private void broadcast(AcceptReq req) {
        if (req == null) return;
        for (Map.Entry<String, PaxosNodeGrpc.PaxosNodeBlockingStub> entry : peerStubs.entrySet()) {
            if (!entry.getKey().equals(nodeId)) {
                try {
                    entry.getValue()
                        .withDeadlineAfter(500, TimeUnit.MILLISECONDS)
                        .accept(req);
                } catch (Exception e) {
                    log.debug("[{}] Failed to broadcast accept to {}: {}", nodeId, entry.getKey(), e.getMessage());
                }
            }
        }
    }

    private PreparedMsg sendPrepareWithRetry(String participantCluster, PrepareMsg prepareMsg, int maxRetries) {
        Map<String, TwoPhaseCommitGrpc.TwoPhaseCommitBlockingStub> clusterStubs = crossClusterStubs.get(participantCluster);
        
        if (clusterStubs == null || clusterStubs.isEmpty()) {
            log.error("[{}] No stubs available for cluster {}", nodeId, participantCluster);
            return null;
        }

        String priorityNode = null;
        
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            List<String> nodeOrder = new ArrayList<>(clusterStubs.keySet());
            if (priorityNode != null && clusterStubs.containsKey(priorityNode)) {
                nodeOrder.remove(priorityNode);
                nodeOrder.add(0, priorityNode);
                log.debug("[{}] Using leader hint: {} for attempt {}/{}", 
                    nodeId, priorityNode, attempt + 1, maxRetries);
            }
            
            for (String targetNode : nodeOrder) {
                TwoPhaseCommitGrpc.TwoPhaseCommitBlockingStub stub = clusterStubs.get(targetNode);
                
                try {
                    log.debug("[{}] Attempting PREPARE to {} (attempt {}/{})", 
                        nodeId, targetNode, attempt + 1, maxRetries);
                    
                    PreparedMsg response = stub
                        .withDeadlineAfter(1000, TimeUnit.MILLISECONDS)
                        .prepare2PC(prepareMsg);
                    
                    if (response.getSuccess()) {
                        log.info("[{}] PREPARE successful via {}", nodeId, targetNode);
                        return response;
                    }
                    
                    if (!response.getCurrentLeaderId().isEmpty()) {
                        priorityNode = response.getCurrentLeaderId();
                        log.info("[{}] Got leader hint from {}: {}, will use in next attempt", 
                            nodeId, targetNode, priorityNode);
                        break;
                    } else {
                        log.debug("[{}] Node {} rejected but gave no leader hint", nodeId, targetNode);
                    }
                    
                } catch (StatusRuntimeException e) {
                    if (e.getStatus().getCode() == io.grpc.Status.Code.UNAVAILABLE ||
                        e.getStatus().getCode() == io.grpc.Status.Code.DEADLINE_EXCEEDED) {
                        log.debug("[{}] Node {} unavailable, trying next", nodeId, targetNode);
                        continue;
                    }
                    log.warn("[{}] Error from {}: {}", nodeId, targetNode, e.getMessage());
                } catch (Exception e) {
                    log.warn("[{}] Unexpected error from {}: {}", nodeId, targetNode, e.getMessage());
                }
            }
            
            if (attempt < maxRetries - 1) {
                try {
                    Thread.sleep(priorityNode != null ? 100 : 200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        log.error("[{}] Failed to send PREPARE to {} after {} retries", nodeId, participantCluster, maxRetries);
        return null;
    }

    private void sendCommitWithRetry(String participantCluster, TwoPCCommitMsg commitMsg, int maxRetries) {
        Map<String, TwoPhaseCommitGrpc.TwoPhaseCommitBlockingStub> clusterStubs = crossClusterStubs.get(participantCluster);
        
        if (clusterStubs == null || clusterStubs.isEmpty()) {
            return;
        }

        String priorityNode = null;
        
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            List<String> nodeOrder = new ArrayList<>(clusterStubs.keySet());
            if (priorityNode != null && clusterStubs.containsKey(priorityNode)) {
                nodeOrder.remove(priorityNode);
                nodeOrder.add(0, priorityNode);
            }
            
            for (String targetNode : nodeOrder) {
                try {
                    AckMsg ack = clusterStubs.get(targetNode)
                        .withDeadlineAfter(1000, TimeUnit.MILLISECONDS)
                        .commit2PC(commitMsg);
                    
                    if (ack.getSuccess()) {
                        log.info("[{}] COMMIT ACK received via {}", nodeId, targetNode);
                        return;
                    }
                    
                    if (!ack.getCurrentLeaderId().isEmpty()) {
                        priorityNode = ack.getCurrentLeaderId();
                        log.info("[{}] Got leader hint for commit: {}", nodeId, priorityNode);
                        break;
                    }
                    
                } catch (Exception e) {
                    log.debug("[{}] Failed COMMIT to {}: {}", nodeId, targetNode, e.getMessage());
                }
            }
            
            if (attempt < maxRetries - 1) {
                try {
                    Thread.sleep(priorityNode != null ? 100 : 200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    public CompletableFuture<ClientReply> handleCrossShard(Txn txn) {
        String txnId = getTxnId(txn);
        log.info("[{}] Starting 2PC as COORDINATOR for txn: {} -> {}", nodeId, txn.getFrom(), txn.getTo());

        CompletableFuture<ClientReply> future = new CompletableFuture<>();

      String senderCluster = paxosCore.getItemCluster(txn.getFrom());
if (!senderCluster.equals(clusterId)) {
    log.warn("[{}] Sender {} belongs to cluster {}, but we are {}", 
        nodeId, txn.getFrom(), senderCluster, clusterId);
    future.complete(ClientReply.newBuilder()
        .setSuccess(false)
        .setMessage("WRONG_SHARD:" + senderCluster)  
        .build());
    return future;
}

        if (!storage.containsKey(txn.getFrom())) {
            log.warn("[{}] Sender {} is in our cluster logically but not in storage (resharding in progress?)", 
                nodeId, txn.getFrom());
            future.complete(ClientReply.newBuilder()
                .setSuccess(false)
                .setMessage("DATA_NOT_AVAILABLE")
                .build());
            return future;
        }

        if (!lockManager.tryLock(txn.getFrom(), txnId)) {
            log.warn("[{}] Sender {} is locked", nodeId, txn.getFrom());
            return future;
        }

        int balance = storage.get(txn.getFrom());
        if (balance < txn.getAmount()) {
            lockManager.unlock(txn.getFrom());
            log.warn("[{}] Insufficient balance: {} < {}", nodeId, balance, txn.getAmount());
            future.complete(ClientReply.newBuilder()
                .setSuccess(false)
                .setMessage("INSUFFICIENT_BALANCE")
                .build());
            return future;
        }

        TwoPCState state = new TwoPCState(txn, true);
        pendingTxns.put(txnId, state);

        executor.submit(() -> {
            try {
                var attempt = paxosCore.runConsensus(txn, PhaseType.PREPARE);
                broadcast(attempt.acceptReq());
                Long prepareSeq = attempt.future().get(5, TimeUnit.SECONDS);
                state.seqNum = prepareSeq;

                log.info("[{}] Prepare consensus done, seq={}", nodeId, prepareSeq);
                paxosCore.markCrossShardPrepared(txnId, prepareSeq);

                String participantCluster = getClusterForKey(txn.getTo());
                PrepareMsg prepareMsg = PrepareMsg.newBuilder()
                    .setTxn(txn)
                    .setCoordinatorId(nodeId)
                    .setTimestamp(System.currentTimeMillis())
                    .build();

                PreparedMsg response = sendPrepareWithRetry(participantCluster, prepareMsg, 5);

                if (response != null && response.getSuccess()) {
                    log.info("[{}] Received PREPARED from participant", nodeId);
                    state.prepared = true;
                    commitTransactionAsync(txn, prepareSeq, participantCluster, future);
                } else {
                    log.warn("[{}] Participant rejected prepare or unreachable", nodeId);
                    paxosCore.markCrossShardCommitted(txnId);
                    abortTransactionAsync(txn, prepareSeq, "PARTICIPANT_ABORT");
                    future.complete(ClientReply.newBuilder().setSuccess(false).setMessage("ABORTED").build());
                }
            } catch (Exception e) {
                log.error("[{}] Error in coordinator prepare phase: {}", nodeId, e.getMessage());
                lockManager.unlock(txn.getFrom());
                pendingTxns.remove(txnId);
                
            }
        });
        return future;
    }

    private void commitTransactionAsync(Txn txn, long seqNum, String participantCluster, CompletableFuture<ClientReply> clientFuture) {
        String txnId = getTxnId(txn);
        log.info("[{}] Starting COMMIT phase for txn {}", nodeId, txnId);

        executor.submit(() -> {
            try {
                var attempt = paxosCore.runConsensus(txn, PhaseType.COMMIT);
                broadcast(attempt.acceptReq());
                Long commitSeq = attempt.future().get(5, TimeUnit.SECONDS);

                log.info("[{}] Commit consensus done", nodeId);
                paxosCore.markCrossShardCommitted(txnId);

                TwoPCCommitMsg commitMsg = TwoPCCommitMsg.newBuilder()
                    .setTxn(txn)
                    .setCoordinatorId(nodeId)
                    .setTimestamp(System.currentTimeMillis())
                    .build();

                sendCommitWithRetry(participantCluster, commitMsg, 3);
                TwoPCState state = pendingTxns.get(txnId);
                if (state != null) {
                    state.committed = true;
                }
                pendingTxns.remove(txnId);
                clientFuture.complete(ClientReply.newBuilder()
                    .setSuccess(true)
                    .setMessage("COMMITTED")
                    .setSeq(Seq.newBuilder().setNum(seqNum))
                    .build());
            }  catch (Exception e) {
    log.error("[{}] Coordinator COMMIT consensus failed - ABORTING: {}", 
        nodeId, e.getMessage());
    paxosCore.markCrossShardCommitted(txnId);
    abortTransactionAsync(txn, seqNum, "COMMIT_CONSENSUS_FAILED");
    
    clientFuture.complete(ClientReply.newBuilder()
        .setSuccess(false)
        .setMessage("ABORTED") 
        .build());
}
        });
    }

    private void abortTransactionAsync(Txn txn, long seqNum, String reason) {
        String txnId = getTxnId(txn);
        log.info("[{}] Aborting transaction {}: {}", nodeId, txnId, reason);

        executor.submit(() -> {
            try {
                paxosCore.markCrossShardCommitted(txnId);
                var attempt = paxosCore.runConsensus(txn, PhaseType.ABORT);
                broadcast(attempt.acceptReq());
                attempt.future().get(5, TimeUnit.SECONDS);
                TwoPCState state = pendingTxns.get(txnId);
                if (state != null) {
                    state.aborted = true;
                }
                pendingTxns.remove(txnId);
            } catch (Exception e) {
                log.error("[{}] Abort consensus failed: {}", nodeId, e.getMessage());
            }
        });
    }

    public PreparedMsg handlePrepareRequest(PrepareMsg request) {
        Txn txn = request.getTxn();
        String txnId = getTxnId(txn);
        log.info("[{}] Received PREPARE as PARTICIPANT for txn: {} -> {}", nodeId, txn.getFrom(), txn.getTo());
        String receiverCluster = paxosCore.getItemCluster(txn.getTo());
        if (!receiverCluster.equals(clusterId)) {
            log.warn("[{}] Receiver {} belongs to cluster {}, but we are {}", 
                nodeId, txn.getTo(), receiverCluster, clusterId);
            return PreparedMsg.newBuilder()
                .setTxn(txn)
                .setParticipantId(nodeId)
                .setSuccess(false)
                .setTimestamp(System.currentTimeMillis())
                .build();
        }
        if (!storage.containsKey(txn.getTo())) {
            log.warn("[{}] Receiver {} is in our cluster logically but not in storage (resharding in progress?)", 
                nodeId, txn.getTo());
            return PreparedMsg.newBuilder()
                .setTxn(txn)
                .setParticipantId(nodeId)
                .setSuccess(false)
                .setTimestamp(System.currentTimeMillis())
                .build();
        }

        if (!paxosCore.isLeader()) {
            String actualLeader = paxosCore.getCurrentLeader() != null ? 
                paxosCore.getCurrentLeader().getLeaderId() : "";
            
            log.warn("[{}] Not leader for PREPARE, actual leader is: {}", nodeId, actualLeader);
            
            return PreparedMsg.newBuilder()
                .setTxn(txn)
                .setParticipantId(nodeId)
                .setSuccess(false)
                .setCurrentLeaderId(actualLeader)
                .setTimestamp(System.currentTimeMillis())
                .build();
        }

        if (!lockManager.tryLock(txn.getTo(), txnId)) {
            log.warn("[{}] Receiver {} is locked", nodeId, txn.getTo());
            
            CompletableFuture.runAsync(() -> {
                try {
                    var attempt = paxosCore.runConsensus(txn, PhaseType.ABORT);
                    broadcast(attempt.acceptReq());
                    attempt.future().get(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                    log.error("[{}] Abort consensus failed", nodeId);
                }
            }, executor);
            
            return PreparedMsg.newBuilder()
                .setTxn(txn)
                .setParticipantId(nodeId)
                .setSuccess(false)
                .setTimestamp(System.currentTimeMillis())
                .build();
        }

        try {
            var attempt = paxosCore.runConsensus(txn, PhaseType.PREPARE);
            broadcast(attempt.acceptReq());
            Long seqNum = attempt.future().get(twoPhaseTimeoutMs, TimeUnit.MILLISECONDS);
            
            TwoPCState state = new TwoPCState(txn, false);
            state.seqNum = seqNum;
            state.prepared = true;
            pendingTxns.put(txnId, state);

            log.info("[{}] PREPARED successfully", nodeId);
            return PreparedMsg.newBuilder()
                .setTxn(txn)
                .setParticipantId(nodeId)
                .setSuccess(true)
                .setCurrentLeaderId(nodeId)
                .setTimestamp(System.currentTimeMillis())
                .build();
        } catch (Exception e) {
            log.error("[{}] Prepare consensus failed: {}", nodeId, e.getMessage());
            lockManager.unlock(txn.getTo());
            return PreparedMsg.newBuilder()
                .setTxn(txn)
                .setParticipantId(nodeId)
                .setSuccess(false)
                .setTimestamp(System.currentTimeMillis())
                .build();
        }
    }

    public AckMsg handleCommitRequest(TwoPCCommitMsg request) {
        Txn txn = request.getTxn();
        String txnId = getTxnId(txn);
        log.info("[{}] Received COMMIT as PARTICIPANT for txn {}", nodeId, txnId);

        if (!paxosCore.isLeader()) {
            String actualLeader = paxosCore.getCurrentLeader() != null ? 
                paxosCore.getCurrentLeader().getLeaderId() : "";
            
            log.warn("[{}] Not leader for COMMIT, actual leader is: {}", nodeId, actualLeader);
            
            return AckMsg.newBuilder()
                .setTxn(txn)
                .setNodeId(nodeId)
                .setSuccess(false)
                .setCurrentLeaderId(actualLeader)
                .build();
        }

        try {
            var attempt = paxosCore.runConsensus(txn, PhaseType.COMMIT);
            broadcast(attempt.acceptReq());
            attempt.future().get(twoPhaseTimeoutMs, TimeUnit.MILLISECONDS);
            
            TwoPCState state = pendingTxns.get(txnId);
            if (state != null) {
                state.committed = true;
            }
            pendingTxns.remove(txnId);

            log.info("[{}] COMMITTED successfully", nodeId);
            return AckMsg.newBuilder()
                .setTxn(txn)
                .setNodeId(nodeId)
                .setSuccess(true)
                .setCurrentLeaderId(nodeId)
                .build();
        } catch (Exception e) {
            log.error("[{}] Commit consensus failed: {}", nodeId, e.getMessage());
            return AckMsg.newBuilder()
                .setTxn(txn)
                .setNodeId(nodeId)
                .setSuccess(false)
                .build();
        }
    }

   public AckMsg handleAbortRequest(TwoPCAbortMsg request) {
        Txn txn = request.getTxn();
        String txnId = getTxnId(txn);
        log.info("[{}] Received ABORT as PARTICIPANT for txn {}", nodeId, txnId);

        CompletableFuture.runAsync(() -> {
            try {
                var attempt = paxosCore.runConsensus(txn, PhaseType.ABORT);
                broadcast(attempt.acceptReq());
                attempt.future().get(5, TimeUnit.SECONDS);
                TwoPCState state = pendingTxns.get(txnId);
                if (state != null) {
                    state.aborted = true;
                }
                pendingTxns.remove(txnId);
            } catch (Exception e) {
                log.error("[{}] Abort failed", nodeId);
            }
        }, executor);

        return AckMsg.newBuilder()
            .setTxn(txn)
            .setNodeId(nodeId)
            .setSuccess(true)
            .build();
    }

    public void recoverPreparedTransactions() {
        log.info("[{}] Scanning Paxos log for incomplete cross-shard transactions", nodeId);
        
        long maxSeq = paxosCore.getNextSeq() - 1;
        Map<String, PrepareInfo> preparedTxns = new HashMap<>();
        for (long seq = 1; seq <= maxSeq; seq++) {
            Map<String, PaxosCore.Status> statusMap = paxosCore.printStatus(seq);
            if (statusMap.isEmpty() || !statusMap.values().contains(PaxosCore.Status.C)) {
                continue;
            }
            PhaseType phase = paxosCore.getPhaseForSeq(seq);
            if (phase == null) continue;

            Txn txn = paxosCore.getTxnForSeq(seq);
            if (txn == null || txn.getType() != TxnType.CROSS_SHARD) continue;
            
            String txnId = getTxnId(txn);
            
            if (phase == PhaseType.PREPARE) {
                preparedTxns.putIfAbsent(txnId, new PrepareInfo(seq, txn));
                log.debug("[{}] Found PREPARE for {} at seq {}", nodeId, txnId, seq);
            } else if (phase == PhaseType.COMMIT) {
                PrepareInfo info = preparedTxns.get(txnId);
                if (info != null) {
                    info.hasCommit = true;
                    log.debug("[{}] Found COMMIT for {} at seq {}", nodeId, txnId, seq);
                }
            } else if (phase == PhaseType.ABORT) {
                preparedTxns.remove(txnId);
                log.debug("[{}] Found ABORT for {} at seq {}", nodeId, txnId, seq);
            }
        }
        
        List<PrepareInfo> needsCommit = preparedTxns.values().stream()
            .filter(info -> !info.hasCommit)
            .toList();
        
        if (needsCommit.isEmpty()) {
            log.info("[{}] No incomplete prepared transactions found", nodeId);
            return;
        }
        
        log.warn("[{}] Found {} prepared transactions without COMMIT - completing them now", 
            nodeId, needsCommit.size());
        
        for (PrepareInfo info : needsCommit) {
            String txnId = getTxnId(info.txn);
            log.info("[{}] Completing COMMIT for prepared transaction {}", nodeId, txnId);
            
            executor.submit(() -> {
                try {
                    var attempt = paxosCore.runConsensus(info.txn, PhaseType.COMMIT);
                    broadcast(attempt.acceptReq());
                    Long commitSeq = attempt.future().get(5, TimeUnit.SECONDS);
                    log.info("[{}] Recovery COMMIT successful for {} at seq {}", 
                        nodeId, txnId, commitSeq);
                    String participantCluster = getClusterForKey(info.txn.getTo());
                    TwoPCCommitMsg commitMsg = TwoPCCommitMsg.newBuilder()
                        .setTxn(info.txn)
                        .setCoordinatorId(nodeId)
                        .setTimestamp(System.currentTimeMillis())
                        .build();
                    
                    sendCommitWithRetry(participantCluster, commitMsg, 3);
                } catch (Exception e) {
                    log.error("[{}] Failed to complete COMMIT for {}: {}", 
                        nodeId, txnId, e.getMessage());
                }
            });
        }
    }

    private static class PrepareInfo {
        final long prepareSeq;
        final Txn txn;
        boolean hasCommit = false;
        
        PrepareInfo(long prepareSeq, Txn txn) {
            this.prepareSeq = prepareSeq;
            this.txn = txn;
        }
    }

    private String getTxnId(Txn txn) {
        return txn.getClientId() + "-" + txn.getClientTs();
    }
    


    private String getClusterForKey(String key) {
        return paxosCore.getItemCluster(key);
    }

    public void flushState() {
        pendingTxns.clear();
    }
}
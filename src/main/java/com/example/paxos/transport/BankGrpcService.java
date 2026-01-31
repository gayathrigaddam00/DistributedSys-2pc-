package com.example.paxos.transport;

import com.example.paxos.core.PaxosCore;
import com.example.paxos.election.ElectionDriver;
import com.example.paxos.performance.PerformanceTracker;
import com.example.paxos.twophase.TransactionCoordinator;
import com.example.paxos.v1.*;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.context.annotation.Lazy;
import com.google.protobuf.Empty;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@GrpcService
public class BankGrpcService extends BankGrpc.BankImplBase {

    private final PaxosCore core;
    private final String nodeId;
    private final Map<String, PaxosNodeGrpc.PaxosNodeBlockingStub> peers;
    private final ElectionDriver electionDriver;
    private final PerformanceTracker performanceTracker;
    private final TransactionCoordinator transactionCoordinator;
    private final long clientWaitTimeoutMillis = 5000;
    private final long peerTimeoutMillis = 500;
    private final int INITIAL_BALANCE = 10;

    public BankGrpcService(
            PaxosCore core, 
            String nodeId, 
            Map<String, PaxosNodeGrpc.PaxosNodeBlockingStub> peers, 
            @Lazy ElectionDriver electionDriver,
            PerformanceTracker performanceTracker,
            TransactionCoordinator transactionCoordinator) {
        this.core = core;
        this.nodeId = nodeId;
        this.peers = peers;
        this.electionDriver = electionDriver;
        this.performanceTracker = performanceTracker;
        this.transactionCoordinator = transactionCoordinator;
    }

    private void printFullLog(String type, String msgType, com.google.protobuf.MessageLite msg) {
        if (msg == null) return;
        String content = msg.toString().replaceAll("\\s+", " ");
        System.out.println("[" + type + "] Node " + nodeId + " " + msgType + ": " + content);
    }

    @Override
public void submit(ClientRequest req, StreamObserver<ClientReply> responseObserver) {
    String txnId = req.getTxn().getClientId() + "-" + req.getTxn().getClientTs();
    printFullLog("RECV", "ClientRequest", req);
    
    performanceTracker.recordTransactionStart(txnId);
    
    try {
        if (!core.isLeader()) {
            performanceTracker.recordTransactionEnd(txnId, false);
            ClientReply reply = ClientReply.newBuilder()
                    .setB(core.getCurrentLeader() != null ? core.getCurrentLeader() : Ballot.getDefaultInstance())
                    .setClientId(req.getTxn().getClientId())
                    .setClientTs(req.getTxn().getClientTs())
                    .setSuccess(false)
                    .setMessage("NOT_LEADER")
                    .build();
            printFullLog("SENT", "ClientReply (Not Leader)", reply);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
            return;
        }
        if (req.getTxn().getType() == TxnType.CROSS_SHARD) {
            handleCrossShardTransaction(req.getTxn(), txnId, responseObserver);
            return;
        }
        Object action = core.onClientSubmit(req.getTxn());
        if (action == null) {
            performanceTracker.recordTransactionEnd(txnId, false);
            return;
        }
        
        if (action instanceof PaxosCore.ReplyAction ra) {
            performanceTracker.recordTransactionEnd(txnId, ra.reply.getSuccess());
            printFullLog("SENT", "ClientReply (Direct)", ra.reply);
            responseObserver.onNext(ra.reply);
            responseObserver.onCompleted();
            return;
        }
        
        if (action instanceof PaxosCore.Broadcast b) {
            // Log and broadcast internal Paxos messages
            for (Object m : b.messages) {
                if (m instanceof com.google.protobuf.MessageLite protoMsg) {
                     printFullLog("SENT", "Broadcast " + m.getClass().getSimpleName(), protoMsg);
                }
                
                if (m instanceof AcceptReq a) {
                    for (Map.Entry<String, PaxosNodeGrpc.PaxosNodeBlockingStub> e : peers.entrySet()) {
                        if (!e.getKey().equals(nodeId)) {
                            try { 
                                e.getValue()
                                 .withDeadlineAfter(peerTimeoutMillis, TimeUnit.MILLISECONDS)
                                 .accept(a);  
                            } catch (Exception ignored) {}
                        }
                    }
                }
                else if (m instanceof CommitReq c) {
                    for (Map.Entry<String, PaxosNodeGrpc.PaxosNodeBlockingStub> e : peers.entrySet()) {
                        if (!e.getKey().equals(nodeId)) {
                            try { 
                                e.getValue()
                                 .withDeadlineAfter(peerTimeoutMillis, TimeUnit.MILLISECONDS)
                                 .commit(c);  
                            } catch (Exception ignored) {}
                        }
                    }
                }
            }

            String clientId = req.getTxn().getClientId();
            long clientTs = req.getTxn().getClientTs();
            Instant deadline = Instant.now().plusMillis(clientWaitTimeoutMillis);
            ClientReply cached = core.getCachedReply(clientId);
            
            while (cached == null || cached.getClientTs() < clientTs) {
                if (Instant.now().isAfter(deadline)) {
                    ClientReply timeoutReply = ClientReply.newBuilder()
                            .setB(core.getCurrentLeader() != null ? core.getCurrentLeader() : Ballot.getDefaultInstance())
                            .setClientId(clientId).setClientTs(clientTs).setSuccess(false).setMessage("TIMEOUT_WAITING_FOR_COMMIT").build();
                    
                    performanceTracker.recordTransactionEnd(txnId, false);
                    printFullLog("SENT", "ClientReply (Timeout)", timeoutReply);
                    responseObserver.onNext(timeoutReply);
                    responseObserver.onCompleted();
                    return;
                }
                try { TimeUnit.MILLISECONDS.sleep(50); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
                cached = core.getCachedReply(clientId);
            }
            
            performanceTracker.recordTransactionEnd(txnId, cached.getSuccess());
            printFullLog("SENT", "ClientReply (Consensus)", cached);
            responseObserver.onNext(cached);
            responseObserver.onCompleted();
            return;
        }
        
        performanceTracker.recordTransactionEnd(txnId, false);
        responseObserver.onError(io.grpc.Status.INTERNAL.withDescription("Unknown PaxosCore result").asRuntimeException());
    } catch (Exception ex) {
        performanceTracker.recordTransactionEnd(txnId, false);
        responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(ex.getMessage()).withCause(ex).asRuntimeException());
    }
}

    private void handleCrossShardTransaction(Txn txn, String txnId, StreamObserver<ClientReply> responseObserver) {
        transactionCoordinator.handleCrossShard(txn).thenAccept(reply -> {
            performanceTracker.recordTransactionEnd(txnId, reply.getSuccess());
            printFullLog("SENT", "ClientReply (2PC)", reply);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }).exceptionally(ex -> {
            performanceTracker.recordTransactionEnd(txnId, false);
            responseObserver.onError(io.grpc.Status.INTERNAL
                .withDescription("Cross-shard failed: " + ex.getMessage())
                .withCause(ex)
                .asRuntimeException());
            return null;
        });
    }

    @Override
    public void getBalance(BalanceRequest request, StreamObserver<BalanceResponse> responseObserver) {
        String accountId = request.getAccountId();
        BalanceResponse.Builder builder = BalanceResponse.newBuilder();
        builder.setSuccess(true);
        builder.setMessage("Success");

        int localBal = core.getStorage().get(accountId);
        builder.putBalances(nodeId, localBal);

        for (Map.Entry<String, PaxosNodeGrpc.PaxosNodeBlockingStub> entry : peers.entrySet()) {
            if (!entry.getKey().equals(nodeId)) {
                try {
                    PrintDBResponse dbRes = entry.getValue()
                            .withDeadlineAfter(peerTimeoutMillis, TimeUnit.MILLISECONDS)
                            .getDB(Empty.getDefaultInstance());
                    
                    int peerBal = dbRes.getDbMap().getOrDefault(accountId, INITIAL_BALANCE);
                    builder.putBalances(entry.getKey(), peerBal);
                } catch (Exception e) {
                }
            }
        }
        
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void printDB(com.google.protobuf.Empty request, 
                        io.grpc.stub.StreamObserver<com.example.paxos.v1.PrintAllDBResponse> responseObserver) {
        com.example.paxos.v1.PrintAllDBResponse.Builder responseBuilder = com.example.paxos.v1.PrintAllDBResponse.newBuilder();

        com.example.paxos.v1.PrintDBResponse localDb = com.example.paxos.v1.PrintDBResponse.newBuilder().putAllDb(core.printDB()).build();
        responseBuilder.putAllDbs(nodeId, localDb);

        for (java.util.Map.Entry<String, com.example.paxos.v1.PaxosNodeGrpc.PaxosNodeBlockingStub> entry : peers.entrySet()) {
            if (!entry.getKey().equals(nodeId)) {
                try {
                    com.example.paxos.v1.PrintDBResponse peerDb = entry.getValue().getDB(com.google.protobuf.Empty.newBuilder().build());
                    responseBuilder.putAllDbs(entry.getKey(), peerDb);
                } catch (Exception e) {
                }
            }
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void printLog(PrintLogRequest request, StreamObserver<PrintLogResponse> responseObserver) {
        PrintLogResponse.Builder response = PrintLogResponse.newBuilder();
        response.addAllLogEntries(core.printLog());
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void printStatus(com.example.paxos.v1.PrintStatusRequest request, 
                            io.grpc.stub.StreamObserver<com.example.paxos.v1.PrintAllStatusResponse> responseObserver) {
        com.example.paxos.v1.PrintAllStatusResponse.Builder responseBuilder = com.example.paxos.v1.PrintAllStatusResponse.newBuilder();
        com.example.paxos.v1.PrintStatusResponse.Builder localStatusBuilder = com.example.paxos.v1.PrintStatusResponse.newBuilder();
        java.util.Map<String, com.example.paxos.core.PaxosCore.Status> localStatusMap = core.printStatus(request.getSeq());
        for (java.util.Map.Entry<String, com.example.paxos.core.PaxosCore.Status> entry : localStatusMap.entrySet()) {
            localStatusBuilder.putStatus(entry.getKey(), entry.getValue().name());
        }
        responseBuilder.putAllStatuses(nodeId, localStatusBuilder.build());

        for (java.util.Map.Entry<String, com.example.paxos.v1.PaxosNodeGrpc.PaxosNodeBlockingStub> entry : peers.entrySet()) {
            if (!entry.getKey().equals(nodeId)) {
                try {
                    com.example.paxos.v1.PrintStatusResponse peerStatus = entry.getValue().getStatus(request);
                    responseBuilder.putAllStatuses(entry.getKey(), peerStatus);
                } catch (Exception e) {
                }
            }
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void printView(Empty request, StreamObserver<PrintViewResponse> responseObserver) {
        PrintViewResponse.Builder response = PrintViewResponse.newBuilder();
        response.addAllViews(core.printView());
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void performance(Empty request, StreamObserver<PerformanceResponse> responseObserver) {
        performanceTracker.endBenchmark();
        
        PerformanceResponse response = PerformanceResponse.newBuilder()
                .setTotalTxns(performanceTracker.getTotalTransactions())
                .setSuccessTxns(performanceTracker.getSuccessfulTransactions())
                .setThroughput(performanceTracker.getThroughput())
                .setAvgLatency(performanceTracker.getAverageLatencyMs())
                .build();
        
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
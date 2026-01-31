package com.example.paxos.client;

import com.example.paxos.v1.*;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

public class BankingClient implements AutoCloseable {
    private final ManagedChannel channel;
    private final BankGrpc.BankBlockingStub blockingStub;
    private final AdminServiceGrpc.AdminServiceBlockingStub adminStub;
    private final PaxosNodeGrpc.PaxosNodeBlockingStub paxosStub;
    private final DataMigrationGrpc.DataMigrationBlockingStub dataMigrationStub;

    public BankingClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        blockingStub = BankGrpc.newBlockingStub(channel);
        adminStub = AdminServiceGrpc.newBlockingStub(channel);
        paxosStub = PaxosNodeGrpc.newBlockingStub(channel);
        dataMigrationStub = DataMigrationGrpc.newBlockingStub(channel);
    }

    public ClientReply submit(com.example.paxos.v1.Txn txn) {
        ClientRequest request = ClientRequest.newBuilder().setTxn(txn).build();
        return blockingStub.submit(request);
    }

  
    public ClientReply submitWithTimeout(com.example.paxos.v1.Txn txn, long timeoutMs) {
        ClientRequest request = ClientRequest.newBuilder().setTxn(txn).build();
        return blockingStub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).submit(request);
    }

    public void setNodeStatus(boolean isActive) {
        String statusString = isActive ? "UP" : "DOWN"; 
        SetStatusRequest request = SetStatusRequest.newBuilder().setStatus(statusString).build();
        adminStub.setNodeStatus(request);
    }

    public boolean isLeader() {
        IsLeaderResponse response = adminStub.isLeader(Empty.newBuilder().build());
        return response.getIsLeader();
    }
    
    public void startElection() {
        adminStub.failLeader(Empty.newBuilder().build());
    }

    public void stopTimers() {
        adminStub.stopTimers(Empty.newBuilder().build());
    }

    public void startTimers() {
        adminStub.startTimers(Empty.newBuilder().build());
    }
    
    public void flushState() {
        adminStub.flushState(Empty.newBuilder().build());
    }

    public void pauseNode(String reason) {
        PauseRequest request = PauseRequest.newBuilder().setReason(reason).build();
        adminStub.pauseNode(request);
    }

    public void resumeNode(String reason) {
        ResumeRequest request = ResumeRequest.newBuilder().setReason(reason).build();
        adminStub.resumeNode(request);
    }

    public GetDataResponse getData(String itemId) {
        GetDataRequest request = GetDataRequest.newBuilder().setItemId(itemId).build();
        return dataMigrationStub.getData(request);
    }

    public TransferDataResponse transferData(String itemId, int value, String fromCluster, String toCluster) {
        TransferDataRequest request = TransferDataRequest.newBuilder()
            .setItemId(itemId)
            .setValue(value)
            .setFromCluster(fromCluster)
            .setToCluster(toCluster)
            .build();
        return dataMigrationStub.transferData(request);
    }

    public DeleteDataResponse deleteData(String itemId, String cluster) {
        DeleteDataRequest request = DeleteDataRequest.newBuilder()
            .setItemId(itemId)
            .setCluster(cluster)
            .build();
        return dataMigrationStub.deleteData(request);
    }

    public PrintAllDBResponse printDB() {
        return blockingStub.printDB(Empty.newBuilder().build());
    }
    
    public BalanceResponse getBalance(String accountId) {
        BalanceRequest request = BalanceRequest.newBuilder().setAccountId(accountId).build();
        return blockingStub.getBalance(request);
    }

    public String printLog() {
        PrintLogResponse response = blockingStub.printLog(PrintLogRequest.newBuilder().build());
        return String.join("\n", response.getLogEntriesList());
    }

    public PrintAllStatusResponse printStatus(long seq) {
        PrintStatusRequest request = PrintStatusRequest.newBuilder().setSeq(seq).build();
        return blockingStub.printStatus(request);
    }

    public PrintViewResponse printView() {
        return blockingStub.printView(Empty.newBuilder().build());
    }

    public PerformanceResponse getPerformance() {
        return blockingStub.performance(Empty.newBuilder().build());
    }
    
    public UpdateShardMappingResponse updateShardMapping(java.util.Map<String, String> mapping) {
        try {
            ShardMapping shardMapping = ShardMapping.newBuilder()
                .putAllItemToCluster(mapping)
                .build();
            
            UpdateShardMappingRequest request = UpdateShardMappingRequest.newBuilder()
                .setMapping(shardMapping)
                .build();
            
            return paxosStub.updateShardMapping(request);
            
        } catch (Exception e) {
            System.err.println("Error updating shard mapping: " + e.getMessage());
            return UpdateShardMappingResponse.newBuilder()
                .setSuccess(false)
                .setMessage(e.getMessage())
                .build();
        }
    }

  
    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
 
    public ManagedChannel getChannel() {
        return channel;
    }
}
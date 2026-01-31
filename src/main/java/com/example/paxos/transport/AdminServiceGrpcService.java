package com.example.paxos.transport;

import com.example.paxos.core.PaxosCore;
import com.example.paxos.election.ElectionDriver;
import com.example.paxos.performance.PerformanceTracker;
import com.example.paxos.v1.*;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.context.annotation.Lazy;

@GrpcService
public class AdminServiceGrpcService extends AdminServiceGrpc.AdminServiceImplBase {

    private final PaxosCore core;
    private final String nodeId;
    private final ElectionDriver electionDriver;
    private final PerformanceTracker performanceTracker;

    public AdminServiceGrpcService(
            PaxosCore core, 
            String nodeId, 
            @Lazy ElectionDriver electionDriver,
            PerformanceTracker performanceTracker) {
        this.core = core;
        this.nodeId = nodeId;
        this.electionDriver = electionDriver;
        this.performanceTracker = performanceTracker;
    }

    @Override
    public void startTimers(Empty request, StreamObserver<Empty> responseObserver) {
        electionDriver.startTimers();
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void stopTimers(Empty request, StreamObserver<Empty> responseObserver) {
        electionDriver.stopTimers();
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void setNodeStatus(SetStatusRequest request, StreamObserver<Empty> responseObserver) {
        try {
            String status = request.getStatus();
            boolean isActive = "UP".equalsIgnoreCase(status);
            core.setActive(isActive);
            System.out.println("[" + nodeId + "] Node status changed to: " + status + " (active=" + isActive + ")");  
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            System.err.println("[" + nodeId + "] Error setting node status: " + ex.getMessage());
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to set node status: " + ex.getMessage())
                .withCause(ex)
                .asRuntimeException());
        }
    }

    @Override
    public void isLeader(Empty request, StreamObserver<IsLeaderResponse> responseObserver) {
        boolean isLeader = core.isLeader();
        IsLeaderResponse response = IsLeaderResponse.newBuilder()
            .setIsLeader(isLeader)
            .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void failLeader(Empty request, StreamObserver<Empty> responseObserver) {
        electionDriver.startElection(); 
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }
    
    @Override
    public void flushState(Empty request, StreamObserver<Empty> responseObserver) {
        try {
            core.flushState();
            performanceTracker.reset();
            performanceTracker.startBenchmark();
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            System.err.println("[" + nodeId + "] Error flushing state: " + ex.getMessage());
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to flush state: " + ex.getMessage())
                .withCause(ex)
                .asRuntimeException());
        }
    }

   
    @Override
    public void pauseNode(PauseRequest request, StreamObserver<Empty> responseObserver) {
        try {
            core.setPaused(true);
            System.out.println("[" + nodeId + "] Node PAUSED for resharding: " + request.getReason());
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            System.err.println("[" + nodeId + "] Error pausing node: " + ex.getMessage());
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to pause node: " + ex.getMessage())
                .withCause(ex)
                .asRuntimeException());
        }
    }

    @Override
    public void resumeNode(ResumeRequest request, StreamObserver<Empty> responseObserver) {
        try {
            core.setPaused(false);
            System.out.println("[" + nodeId + "] Node RESUMED after resharding: " + request.getReason());
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            System.err.println("[" + nodeId + "] Error resuming node: " + ex.getMessage());
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to resume node: " + ex.getMessage())
                .withCause(ex)
                .asRuntimeException());
        }
    }
}
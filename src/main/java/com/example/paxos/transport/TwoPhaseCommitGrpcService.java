package com.example.paxos.transport;

import com.example.paxos.core.PaxosCore;
import com.example.paxos.election.ElectionDriver;
import com.example.paxos.twophase.TransactionCoordinator;
import com.example.paxos.v1.*;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;

@GrpcService
public class TwoPhaseCommitGrpcService extends TwoPhaseCommitGrpc.TwoPhaseCommitImplBase {
    private static final Logger log = LoggerFactory.getLogger(TwoPhaseCommitGrpcService.class);

    private final TransactionCoordinator coordinator;
    private final String nodeId;
    private final PaxosCore core;
    private final ElectionDriver electionDriver;

    public TwoPhaseCommitGrpcService(
            TransactionCoordinator coordinator, 
            String nodeId,
            PaxosCore core,
            @Lazy ElectionDriver electionDriver) {
        this.coordinator = coordinator;
        this.nodeId = nodeId;
        this.core = core;
        this.electionDriver = electionDriver;
    }

    private void printFullLog(String type, String msgType, com.google.protobuf.MessageLite msg) {
        if (msg == null) return;
        String content = msg.toString().replaceAll("\\s+", " ");
        System.out.println("[" + type + "] Node " + nodeId + " " + msgType + ": " + content);
    }

    @Override
    public void prepare2PC(PrepareMsg request, StreamObserver<PreparedMsg> responseObserver) {
        printFullLog("RECV", "2PC_PrepareMsg", request);
        try {
            PreparedMsg response = coordinator.handlePrepareRequest(request);
            printFullLog("SENT", "2PC_PreparedMsg", response);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("[{}] Error in prepare2PC: {}", nodeId, e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                .withDescription("Prepare failed: " + e.getMessage())
                .withCause(e)
                .asRuntimeException());
        }
    }

    @Override
    public void commit2PC(TwoPCCommitMsg request, StreamObserver<AckMsg> responseObserver) {
        printFullLog("RECV", "2PC_CommitMsg", request);
        try {
            AckMsg response = coordinator.handleCommitRequest(request);
            printFullLog("SENT", "2PC_AckMsg", response);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("[{}] Error in commit2PC: {}", nodeId, e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                .withDescription("Commit failed: " + e.getMessage())
                .withCause(e)
                .asRuntimeException());
        }
    }

    @Override
    public void abort2PC(TwoPCAbortMsg request, StreamObserver<AckMsg> responseObserver) {
        printFullLog("RECV", "2PC_AbortMsg", request);
        try {
            AckMsg response = coordinator.handleAbortRequest(request);
            printFullLog("SENT", "2PC_AckMsg (Abort)", response);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("[{}] Error in abort2PC: {}", nodeId, e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                .withDescription("Abort failed: " + e.getMessage())
                .withCause(e)
                .asRuntimeException());
        }
    }
}
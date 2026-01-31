package com.example.paxos.transport;

import com.example.paxos.core.PaxosCore;
import com.example.paxos.election.ElectionDriver;
import com.example.paxos.v1.*;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

import java.util.Map;

@GrpcService
public class PaxosNodeGrpcService extends PaxosNodeGrpc.PaxosNodeImplBase {

    private final PaxosCore core;
    private final String nodeId;
    private final Map<String, PaxosNodeGrpc.PaxosNodeBlockingStub> peers;
    private final ElectionDriver electionDriver;

    public PaxosNodeGrpcService(PaxosCore core,
                                String nodeId,
                                Map<String, PaxosNodeGrpc.PaxosNodeBlockingStub> peers,
                                ElectionDriver electionDriver) {
        this.core = core;
        this.nodeId = nodeId;
        this.peers = peers;
        this.electionDriver = electionDriver;
    }

    private void printFullLog(String type, String msgType, com.google.protobuf.MessageLite msg) {
        if (msg == null) return;
        String content = msg.toString().replaceAll("\\s+", " ");
        System.out.println("[" + type + "] Node " + nodeId + " " + msgType + ": " + content);
    }

    @Override
    public void prepare(PrepareReq req, StreamObserver<PromiseRes> responseObserver) {
        if (!core.isActive()) {
            responseObserver.onNext(PromiseRes.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        }
        try {
            printFullLog("RECV", "PrepareReq", req);
            core.logReceived(req);
            electionDriver.recordLeaderActivity(); 
            PromiseRes res = core.onPrepare(req);
            
            printFullLog("SENT", "PromiseRes", res);
            core.logSent(res);
            
            responseObserver.onNext(res);
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).withCause(ex).asRuntimeException());
        }
    }

    @Override
    public void accept(AcceptReq req, StreamObserver<Empty> responseObserver) {
        if (!core.isActive()) {
           responseObserver.onNext(Empty.getDefaultInstance());
           responseObserver.onCompleted();
           return;
        }
        try {
            printFullLog("RECV", "AcceptReq", req);
            core.logReceived(req);
            electionDriver.recordLeaderActivity();
            Object result = core.onAccept(req);
            if (result instanceof AcceptedReq ack) {
                String leader = ack.getB().getLeaderId();
                var leaderStub = peers.get(leader);
                if (leaderStub != null) {
                    printFullLog("SENT", "AcceptedReq (Unicast to " + leader + ")", ack);
                    core.logSent(ack);
                    leaderStub.accepted(ack);
                }
            }
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).withCause(ex).asRuntimeException());
        }
    }

    @Override
    public void accepted(AcceptedReq req, StreamObserver<Empty> responseObserver) {
        if (!core.isActive()) {
           responseObserver.onNext(Empty.getDefaultInstance());
           responseObserver.onCompleted();
           return;
        }
        try {
            printFullLog("RECV", "AcceptedReq", req);
            core.logReceived(req);
            Object maybe = core.onAccepted(req);
            if (maybe instanceof PaxosCore.Broadcast b) {
                broadcastObjects(b.messages);
            }
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).withCause(ex).asRuntimeException());
        }
    }

    @Override
    public void commit(CommitReq req, StreamObserver<Empty> responseObserver) {
        if (!core.isActive()) {
           responseObserver.onNext(Empty.getDefaultInstance());
           responseObserver.onCompleted();
           return;
        }
        try {
            printFullLog("RECV", "CommitReq", req);
            core.logReceived(req);
            electionDriver.recordLeaderActivity();
            core.onCommit(req);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).withCause(ex).asRuntimeException());
        }
    }

    @Override
    public void newView(NewViewReq req, StreamObserver<Empty> responseObserver) {
        if (!core.isActive()) {
              responseObserver.onNext(Empty.getDefaultInstance());
              responseObserver.onCompleted();
              return;
        }
        try {
            printFullLog("RECV", "NewViewReq", req);
            core.logReceived(req);
            electionDriver.recordLeaderActivity();
            core.onNewView(req);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).withCause(ex).asRuntimeException());
        }
    }

    @Override
    public void getDB(com.google.protobuf.Empty request,
                      io.grpc.stub.StreamObserver<com.example.paxos.v1.PrintDBResponse> responseObserver) {
        com.example.paxos.v1.PrintDBResponse.Builder response = com.example.paxos.v1.PrintDBResponse.newBuilder();
        response.putAllDb(core.printDB());
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getStatus(com.example.paxos.v1.PrintStatusRequest request,
                          io.grpc.stub.StreamObserver<com.example.paxos.v1.PrintStatusResponse> responseObserver) {
        com.example.paxos.v1.PrintStatusResponse.Builder response = com.example.paxos.v1.PrintStatusResponse.newBuilder();
        java.util.Map<String, com.example.paxos.core.PaxosCore.Status> statusMap = core.printStatus(request.getSeq());
        for (java.util.Map.Entry<String, com.example.paxos.core.PaxosCore.Status> entry : statusMap.entrySet()) {
            response.putStatus(entry.getKey(), entry.getValue().name());
        }
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }
    
    @Override
    public void updateShardMapping(UpdateShardMappingRequest request,
                                   StreamObserver<UpdateShardMappingResponse> responseObserver) {
        try {
            Map<String, String> newMapping = request.getMapping().getItemToClusterMap();
            System.out.println("[RECV] Node " + nodeId + " UpdateShardMappingRequest: " + 
                newMapping.size() + " entries");
            core.updateShardMapping(newMapping);
            
            UpdateShardMappingResponse response = UpdateShardMappingResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Mapping updated successfully")
                .build();
            System.out.println("[SENT] Node " + nodeId + " UpdateShardMappingResponse: SUCCESS");
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            System.err.println("[ERROR] Node " + nodeId + " failed to update shard mapping: " + e.getMessage());
            UpdateShardMappingResponse response = UpdateShardMappingResponse.newBuilder()
                .setSuccess(false)
                .setMessage("Failed to update mapping: " + e.getMessage())
                .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
    
    private void broadcastObjects(Iterable<Object> messages) {
        for (Object m : messages) {
            core.logSent(m);
            if (m instanceof com.google.protobuf.MessageLite protoMsg) {
                 printFullLog("SENT", "Broadcast " + m.getClass().getSimpleName(), protoMsg);
            }

            if (m instanceof AcceptReq a) {
                for (var entry : peers.entrySet()) {
                    if (entry.getKey().equals(nodeId)) continue;
                    try { entry.getValue().accept(a); } catch (Exception ignored) { }
                }
            } else if (m instanceof CommitReq c) {
                for (var entry : peers.entrySet()) {
                    if (entry.getKey().equals(nodeId)) continue;
                    try { entry.getValue().commit(c); } catch (Exception ignored) {}
                }
            } else if (m instanceof NewViewReq nv) {
                for (var entry : peers.entrySet()) {
                    if (entry.getKey().equals(nodeId)) continue;
                    try { entry.getValue().newView(nv); } catch (Exception ignored) {}
                }
            }
        }
    }
}
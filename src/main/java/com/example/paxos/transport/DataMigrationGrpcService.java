package com.example.paxos.transport;

import com.example.paxos.core.PaxosCore;
import com.example.paxos.storage.SQLiteStorage;
import com.example.paxos.v1.*;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@GrpcService
public class DataMigrationGrpcService extends DataMigrationGrpc.DataMigrationImplBase {
    private static final Logger log = LoggerFactory.getLogger(DataMigrationGrpcService.class);

    private final PaxosCore core;
    private final String nodeId;

    public DataMigrationGrpcService(PaxosCore core, String nodeId) {
        this.core = core;
        this.nodeId = nodeId;
    }

    @Override
    public void transferData(TransferDataRequest request, StreamObserver<TransferDataResponse> responseObserver) {
        String itemId = request.getItemId();
        int value = request.getValue();
        
        log.info("[{}] Received TransferData request for item {} (value={})", nodeId, itemId, value);
        
        try {
            SQLiteStorage storage = core.getStorage();
            boolean success = storage.insertFromResharding(itemId, value);
            
            TransferDataResponse response = TransferDataResponse.newBuilder()
                .setSuccess(success)
                .setMessage(success ? "Data inserted successfully" : "Failed to insert data")
                .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("[{}] Error in transferData: {}", nodeId, e.getMessage(), e);
            TransferDataResponse response = TransferDataResponse.newBuilder()
                .setSuccess(false)
                .setMessage("Exception: " + e.getMessage())
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void deleteData(DeleteDataRequest request, StreamObserver<DeleteDataResponse> responseObserver) {
        String itemId = request.getItemId();
        
        log.info("[{}] Received DeleteData request for item {}", nodeId, itemId);
        
        try {
            SQLiteStorage storage = core.getStorage();
            boolean success = storage.deleteFromResharding(itemId);
            
            DeleteDataResponse response = DeleteDataResponse.newBuilder()
                .setSuccess(success)
                .setMessage(success ? "Data deleted successfully" : "Failed to delete data")
                .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("[{}] Error in deleteData: {}", nodeId, e.getMessage(), e);
            DeleteDataResponse response = DeleteDataResponse.newBuilder()
                .setSuccess(false)
                .setMessage("Exception: " + e.getMessage())
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getData(GetDataRequest request, StreamObserver<GetDataResponse> responseObserver) {
        String itemId = request.getItemId();
        
        log.info("[{}] Received GetData request for item {}", nodeId, itemId);
        
        try {
            SQLiteStorage storage = core.getStorage();
            Integer value = storage.getForTransfer(itemId);
            
            if (value != null) {
                GetDataResponse response = GetDataResponse.newBuilder()
                    .setSuccess(true)
                    .setValue(value)
                    .setMessage("Data retrieved successfully")
                    .build();
                responseObserver.onNext(response);
            } else {
                GetDataResponse response = GetDataResponse.newBuilder()
                    .setSuccess(false)
                    .setValue(0)
                    .setMessage("Item not found")
                    .build();
                responseObserver.onNext(response);
            }
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("[{}] Error in getData: {}", nodeId, e.getMessage(), e);
            GetDataResponse response = GetDataResponse.newBuilder()
                .setSuccess(false)
                .setValue(0)
                .setMessage("Exception: " + e.getMessage())
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
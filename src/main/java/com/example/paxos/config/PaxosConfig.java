package com.example.paxos.config;

import com.example.paxos.core.PaxosCore;
import com.example.paxos.lock.LockManager;
import com.example.paxos.wal.WriteAheadLog;
import com.example.paxos.twophase.TransactionCoordinator;
import com.example.paxos.performance.PerformanceTracker;
import com.example.paxos.v1.PaxosNodeGrpc;
import com.example.paxos.v1.TwoPhaseCommitGrpc;
import com.example.paxos.v1.DataMigrationGrpc; // ✅ NEW
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.*;

@Configuration
public class PaxosConfig {

    @Value("${paxos.nodeId}")
    private String nodeId;

    private final Map<String, String> allNodeAddresses = new HashMap<>();
    private final Map<String, List<String>> clusterMap = new HashMap<>();

    public PaxosConfig() {
        for (int i = 1; i <= 9; i++) {
            allNodeAddresses.put("n" + i, "localhost:" + (50050 + i));
        }

        clusterMap.put("C1", List.of("n1", "n2", "n3"));
        clusterMap.put("C2", List.of("n4", "n5", "n6"));
        clusterMap.put("C3", List.of("n7", "n8", "n9"));
    }

    public String getMyClusterId() {
        for (Map.Entry<String, List<String>> entry : clusterMap.entrySet()) {
            if (entry.getValue().contains(nodeId)) {
                return entry.getKey();
            }
        }
        throw new RuntimeException("Node ID " + nodeId + " not found in any cluster configuration.");
    }

    @Bean
    public LockManager lockManager() {
        return new LockManager();
    }

    @Bean
    public WriteAheadLog writeAheadLog() {
        return new WriteAheadLog();
    }

    @Bean
    public PerformanceTracker performanceTracker() {
        return new PerformanceTracker();
    }

    @Bean
    public PaxosCore paxosCore(LockManager lockManager, WriteAheadLog wal) {
        String myClusterId = getMyClusterId();
        List<String> myClusterPeers = clusterMap.get(myClusterId);

        System.out.println("Node " + nodeId + " starting in Cluster " + myClusterId + " with peers: " + myClusterPeers);

        return new PaxosCore(nodeId, new HashSet<>(myClusterPeers), 
                           lockManager, wal, null, myClusterId);
    }

    @Bean
    public Map<String, PaxosNodeGrpc.PaxosNodeBlockingStub> peerStubs() {
        String myClusterId = getMyClusterId();
        List<String> myClusterPeers = clusterMap.get(myClusterId);
        
        Map<String, PaxosNodeGrpc.PaxosNodeBlockingStub> stubs = new HashMap<>();
        
        for (String peerId : myClusterPeers) {
            if (peerId.equals(nodeId)) continue;

            String address = allNodeAddresses.get(peerId);
            String[] parts = address.split(":");
            
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(parts[0], Integer.parseInt(parts[1]))
                    .usePlaintext()
                    .build();
            
            stubs.put(peerId, PaxosNodeGrpc.newBlockingStub(channel));
        }
        return stubs;
    }

    @Bean
    public Map<String, Map<String, TwoPhaseCommitGrpc.TwoPhaseCommitBlockingStub>> crossClusterStubs() {
        String myCluster = getMyClusterId();
        Map<String, Map<String, TwoPhaseCommitGrpc.TwoPhaseCommitBlockingStub>> clusterStubs = new HashMap<>();
        
        for (String cluster : List.of("C1", "C2", "C3")) {
            if (!cluster.equals(myCluster)) {
                Map<String, TwoPhaseCommitGrpc.TwoPhaseCommitBlockingStub> nodeStubs = new HashMap<>();
                
                for (String targetNode : clusterMap.get(cluster)) {
                    String address = allNodeAddresses.get(targetNode);
                    String[] parts = address.split(":");
                    
                    ManagedChannel channel = ManagedChannelBuilder
                            .forAddress(parts[0], Integer.parseInt(parts[1]))
                            .usePlaintext()
                            .build();
                    
                    nodeStubs.put(targetNode, TwoPhaseCommitGrpc.newBlockingStub(channel));
                }
                
                clusterStubs.put(cluster, nodeStubs);
                System.out.println("Node " + nodeId + " created 2PC stubs to all nodes in cluster " + cluster);
            }
        }
        return clusterStubs;
    }

    
    @Bean
    public Map<String, DataMigrationGrpc.DataMigrationBlockingStub> dataMigrationStubs() {
        Map<String, DataMigrationGrpc.DataMigrationBlockingStub> stubs = new HashMap<>();
        
        for (int i = 1; i <= 9; i++) {
            String targetNode = "n" + i;
            if (targetNode.equals(nodeId)) continue;
            
            String address = allNodeAddresses.get(targetNode);
            String[] parts = address.split(":");
            
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(parts[0], Integer.parseInt(parts[1]))
                    .usePlaintext()
                    .build();
            
            stubs.put(targetNode, DataMigrationGrpc.newBlockingStub(channel));
        }
        
        System.out.println("Node " + nodeId + " created DataMigration stubs to all other nodes");
        return stubs;
    }

    @Bean
    public TransactionCoordinator transactionCoordinator(
            PaxosCore core,
            LockManager lockManager,
            WriteAheadLog wal,
            Map<String, Map<String, TwoPhaseCommitGrpc.TwoPhaseCommitBlockingStub>> crossClusterStubs,
            Map<String, PaxosNodeGrpc.PaxosNodeBlockingStub> peerStubs) {
        
        String myCluster = getMyClusterId();
        
        return new TransactionCoordinator(
            nodeId, 
            myCluster, 
            core, 
            lockManager, 
            core.getStorage(),
            wal, 
            crossClusterStubs,
            peerStubs,
            5000L
        );
    }

    @Bean
    public String nodeId() {
        return nodeId;
    }
}
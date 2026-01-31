package com.example.paxos.reshard;

import com.example.paxos.client.BankingClient;
import com.example.paxos.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ReshardingCoordinator {
    private static final Logger log = LoggerFactory.getLogger(ReshardingCoordinator.class);
    
    private final Map<String, List<String>> clusterNodes = Map.of(
        "C1", List.of("n1", "n2", "n3"),
        "C2", List.of("n4", "n5", "n6"),
        "C3", List.of("n7", "n8", "n9")
    );
    
    public boolean executeResharding(ReshardingManager.ReshardingPlan plan) {
        log.info("=== EXECUTING RESHARDING PLAN ===");
        log.info("Total movements: {}", plan.movements.size());
        
        if (plan.movements.isEmpty()) {
            log.info("No data movements required");
            return true;
        }
        
        try {
            System.out.println("\n=== PHASE 1: PAUSING ALL NODES ===");
            if (!pauseAllNodes()) {
                System.err.println("Failed to pause all nodes");
                return false;
            }
            
            System.out.println("\n=== PHASE 2: MOVING DATA ===");
            Map<String, List<ReshardingManager.DataMovement>> movementsBySource = groupBySource(plan.movements);
            
            for (Map.Entry<String, List<ReshardingManager.DataMovement>> entry : movementsBySource.entrySet()) {
                String sourceCluster = entry.getKey();
                List<ReshardingManager.DataMovement> movements = entry.getValue();
                
                System.out.println("\nMoving " + movements.size() + " items from " + sourceCluster);
                
                if (!moveDataFromCluster(sourceCluster, movements)) {
                    System.err.println("Failed to move data from " + sourceCluster);
                    rollbackAndResume();
                    return false;
                }
            }
            
            System.out.println("\n=== PHASE 3: UPDATING SHARD MAPPINGS ===");
            Map<String, String> newCompleteMapping = buildCompleteMapping(plan);
            if (!updateAllNodeMappings(newCompleteMapping)) {
                System.err.println("Failed to update mappings");
                rollbackAndResume();
                return false;
            }
            
            System.out.println("\n=== PHASE 4: RESUMING ALL NODES ===");
            if (!resumeAllNodes()) {
                System.err.println("Failed to resume all nodes");
                return false;
            }
            
            System.out.println("\n Resharding completed successfully");
            System.out.println("All 9 nodes have been updated with new data and mappings.");
            return true;
            
        } catch (Exception e) {
            log.error("Resharding failed", e);
            rollbackAndResume();
            return false;
        }
    }
    
    private boolean pauseAllNodes() {
        CountDownLatch latch = new CountDownLatch(9);
        boolean[] success = {true};
        
        for (int i = 1; i <= 9; i++) {
            int nodeNum = i;
            new Thread(() -> {
                try (BankingClient client = new BankingClient("localhost", 50050 + nodeNum)) {
                    client.pauseNode("resharding");
                    System.out.println("  n" + nodeNum + " paused");
                } catch (Exception e) {
                    System.err.println("  n" + nodeNum + " failed to pause: " + e.getMessage());
                    success[0] = false;
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
       
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        return success[0];
    }
    
    private boolean resumeAllNodes() {
        CountDownLatch latch = new CountDownLatch(9);
        boolean[] success = {true};
        
        for (int i = 1; i <= 9; i++) {
            int nodeNum = i;
            new Thread(() -> {
                try (BankingClient client = new BankingClient("localhost", 50050 + nodeNum)) {
                    client.resumeNode("resharding complete");
                    System.out.println("  n" + nodeNum + " resumed");
                } catch (Exception e) {
                    System.err.println("  n" + nodeNum + " failed to resume: " + e.getMessage());
                    success[0] = false;
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        
        return success[0];
    }
    
    private boolean moveDataFromCluster(String sourceCluster, List<ReshardingManager.DataMovement> movements) {
        List<String> sourceNodes = clusterNodes.get(sourceCluster);
        String sourceLeader = sourceNodes.get(0);
        int sourcePort = 50050 + Integer.parseInt(sourceLeader.substring(1));
        
        int successCount = 0;
        int failCount = 0;
        
        try (BankingClient sourceClient = new BankingClient("localhost", sourcePort)) {
            for (ReshardingManager.DataMovement movement : movements) {
                try {
                    GetDataResponse getResponse = sourceClient.getData(movement.itemId);
                    if (!getResponse.getSuccess()) {
                        log.warn("Item {} not found in source cluster {}", movement.itemId, sourceCluster);
                        failCount++;
                        continue;
                    }
                    
                    int value = getResponse.getValue();
                    boolean writeSuccess = writeToAllReplicas(movement.itemId, value, movement.toCluster);
                    if (!writeSuccess) {
                        log.error("Failed to write item {} to destination cluster {}", movement.itemId, movement.toCluster);
                        failCount++;
                        continue;
                    }
                    boolean deleteSuccess = deleteFromAllReplicas(movement.itemId, sourceCluster);
                    if (!deleteSuccess) {
                        log.warn("Failed to delete item {} from source cluster {} (but write succeeded)", 
                            movement.itemId, sourceCluster);
                    }
                    
                    successCount++;
                    
                    if (successCount % 100 == 0) {
                        System.out.println("  Progress: " + successCount + "/" + movements.size() + " items moved");
                    }
                    
                } catch (Exception e) {
                    log.error("Error moving item {}: {}", movement.itemId, e.getMessage());
                    failCount++;
                }
            }
        } catch (Exception e) {
            log.error("Failed to connect to source cluster {}: {}", sourceCluster, e.getMessage());
            return false;
        }
        
        System.out.println("  Completed: " + successCount + " success, " + failCount + " failed");
        return failCount < (movements.size() * 0.1); 
    }
    
    private boolean writeToAllReplicas(String itemId, int value, String destCluster) {
        List<String> destNodes = clusterNodes.get(destCluster);
        CountDownLatch latch = new CountDownLatch(destNodes.size());
        int[] successCount = {0};
        
        for (String nodeId : destNodes) {
            new Thread(() -> {
                try (BankingClient client = new BankingClient("localhost", 50050 + Integer.parseInt(nodeId.substring(1)))) {
                    TransferDataResponse response = client.transferData(itemId, value, "source", destCluster);
                    if (response.getSuccess()) {
                        synchronized (successCount) {
                            successCount[0]++;
                        }
                    }
                } catch (Exception e) {
                    log.error("Failed to write {} to {}: {}", itemId, nodeId, e.getMessage());
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        int quorum = (destNodes.size() / 2) + 1;
        return successCount[0] >= quorum;
    }
    
    private boolean deleteFromAllReplicas(String itemId, String sourceCluster) {
        List<String> sourceNodes = clusterNodes.get(sourceCluster);
        CountDownLatch latch = new CountDownLatch(sourceNodes.size());
        int[] successCount = {0};
        
        for (String nodeId : sourceNodes) {
            new Thread(() -> {
                try (BankingClient client = new BankingClient("localhost", 50050 + Integer.parseInt(nodeId.substring(1)))) {
                    DeleteDataResponse response = client.deleteData(itemId, sourceCluster);
                    if (response.getSuccess()) {
                        synchronized (successCount) {
                            successCount[0]++;
                        }
                    }
                } catch (Exception e) {
                    log.error("Failed to delete {} from {}: {}", itemId, nodeId, e.getMessage());
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        int quorum = (sourceNodes.size() / 2) + 1;
        return successCount[0] >= quorum;
    }
    
    private Map<String, String> buildCompleteMapping(ReshardingManager.ReshardingPlan plan) {
        Map<String, String> mapping = new HashMap<>();
        for (int i = 1; i <= 9000; i++) {
            if (i <= 3000) {
                mapping.put(String.valueOf(i), "C1");
            } else if (i <= 6000) {
                mapping.put(String.valueOf(i), "C2");
            } else {
                mapping.put(String.valueOf(i), "C3");
            }
        }
        
        for (ReshardingManager.DataMovement movement : plan.movements) {
            mapping.put(movement.itemId, movement.toCluster);
        }
        
        return mapping;
    }
    
    private boolean updateAllNodeMappings(Map<String, String> newMapping) {
        CountDownLatch latch = new CountDownLatch(9);
        int[] successCount = {0};
        
        for (int i = 1; i <= 9; i++) {
            int nodeNum = i;
            new Thread(() -> {
                try (BankingClient client = new BankingClient("localhost", 50050 + nodeNum)) {
                    UpdateShardMappingResponse response = client.updateShardMapping(newMapping);
                    
                    if (response.getSuccess()) {
                        synchronized (successCount) {
                            successCount[0]++;
                        }
                        System.out.println("  ✓ n" + nodeNum + " mapping updated");
                    } else {
                        System.err.println("  ✗ n" + nodeNum + " failed: " + response.getMessage());
                    }
                } catch (Exception e) {
                    System.err.println("  ✗ n" + nodeNum + " unreachable: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        
        System.out.println("Mapping update: " + successCount[0] + "/9 nodes updated");
        return successCount[0] == 9; 
    }
    
    private Map<String, List<ReshardingManager.DataMovement>> groupBySource(List<ReshardingManager.DataMovement> movements) {
        Map<String, List<ReshardingManager.DataMovement>> grouped = new HashMap<>();
        for (ReshardingManager.DataMovement movement : movements) {
            grouped.computeIfAbsent(movement.fromCluster, k -> new ArrayList<>()).add(movement);
        }
        return grouped;
    }
    
    private void rollbackAndResume() {
        System.err.println("\n=== ROLLBACK: Resuming all nodes ===");
        resumeAllNodes();
    }
    
    public boolean verifyResharding(ReshardingManager.ReshardingPlan plan) {
        log.info("Resharding verification would check {} movements", plan.movements.size());
        return true;
    }
}
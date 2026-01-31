package com.example.paxos.reshard;

import com.example.paxos.v1.Txn;
import com.example.paxos.v1.TxnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class ReshardingManager {
    private static final Logger logger = LoggerFactory.getLogger(ReshardingManager.class);
    
    private final Queue<Txn> transactionHistory;
    private final int windowSize;
    private Map<String, String> currentMapping; 
    
    public ReshardingManager(int windowSize) {
        this.windowSize = windowSize;
        this.transactionHistory = new ConcurrentLinkedQueue<>();
        initializeMapping();
    }
    
    private void initializeMapping() {
        this.currentMapping = new HashMap<>();
        for (int i = 1; i <= 9000; i++) {
            if (i <= 3000) {
                currentMapping.put(String.valueOf(i), "C1");
            } else if (i <= 6000) {
                currentMapping.put(String.valueOf(i), "C2");
            } else {
                currentMapping.put(String.valueOf(i), "C3");
            }
        }
    }
    
    public synchronized void recordTransaction(Txn txn) {
        if (txn.getType() == TxnType.INTRA_SHARD || txn.getType() == TxnType.CROSS_SHARD) {
            transactionHistory.add(txn);
            while (transactionHistory.size() > windowSize) {
                transactionHistory.poll();
            }
        }
    }
    
public synchronized ReshardingPlan generateReshardingPlan() {
    List<Txn> txnList = new ArrayList<>(transactionHistory);
    int totalTxns = txnList.size();
    
    logger.info("Starting resharding analysis with {} transactions in history", totalTxns);
    
    if (totalTxns == 0) {
        return new ReshardingPlan(
            new ReshardingStatistics(0, 0, 0, 0, 
                Map.of("C1", 3000, "C2", 3000, "C3", 3000),
                Map.of("C1", 3000, "C2", 3000, "C3", 3000)),
            new ArrayList<>()
        );
    }
    
    List<Txn> crossShardOnly = txnList.stream()
        .filter(txn -> txn.getType() == TxnType.CROSS_SHARD)
        .collect(Collectors.toList());
    
    logger.info("Analyzing {} cross-shard transactions (out of {} total)", 
               crossShardOnly.size(), totalTxns);
    if (crossShardOnly.isEmpty()) {
        logger.info("No cross-shard transactions - resharding not beneficial");
        return new ReshardingPlan(
            new ReshardingStatistics(totalTxns, 0, 0, 0,
                calculateShardSizes(currentMapping),
                calculateShardSizes(currentMapping)),
            new ArrayList<>()
        );
    }
    
    Set<String> crossShardItems = new HashSet<>();
    for (Txn txn : crossShardOnly) {
        crossShardItems.add(txn.getFrom());
        crossShardItems.add(txn.getTo());
    }
    
    logger.info("Found {} unique items in cross-shard transactions", crossShardItems.size());
    Map<String, Set<String>> hypergraph = buildHypergraph(crossShardOnly);
    int crossShardBefore = countCrossShardTransactions(txnList, currentMapping);
    Map<String, String> newMapping = partitionData(hypergraph, crossShardItems);
    int crossShardAfter = countCrossShardTransactions(txnList, newMapping);
    double improvementPct = totalTxns > 0 ? 
        ((crossShardBefore - crossShardAfter) * 100.0 / totalTxns) : 0.0;
    List<DataMovement> movements = new ArrayList<>();
    for (String itemId : crossShardItems) {
        String oldCluster = currentMapping.get(itemId);
        String newCluster = newMapping.get(itemId);
        
        if (!oldCluster.equals(newCluster)) {
            movements.add(new DataMovement(itemId, oldCluster, newCluster));
        }
    }
    Map<String, Integer> sizesBefore = calculateShardSizes(currentMapping);
    Map<String, Integer> sizesAfter = calculateShardSizes(newMapping);
    
    ReshardingStatistics stats = new ReshardingStatistics(
        totalTxns, crossShardBefore, crossShardAfter, improvementPct,
        sizesBefore, sizesAfter
    );
    
    logger.info("Resharding plan: {} movements, {:.1f}% improvement (CS: {} -> {})", 
        movements.size(), improvementPct, crossShardBefore, crossShardAfter);
    
    return new ReshardingPlan(stats, movements);
}

public String getClusterForItem(String itemId) {
    return currentMapping.getOrDefault(itemId, getDefaultCluster(itemId));
}

private String getDefaultCluster(String itemId) {
    try {
        int id = Integer.parseInt(itemId);
        if (id <= 3000) return "C1";
        if (id <= 6000) return "C2";
        return "C3";
    } catch (NumberFormatException e) {
        return "C1";
    }
}
    private Map<String, Set<String>> buildHypergraph(List<Txn> transactions) {
        Map<String, Set<String>> graph = new HashMap<>();
        
        for (Txn txn : transactions) {
            String from = txn.getFrom();
            String to = txn.getTo();
            
            graph.computeIfAbsent(from, k -> new HashSet<>()).add(to);
            graph.computeIfAbsent(to, k -> new HashSet<>()).add(from);
        }
        
        return graph;
    }
    
    private Map<String, String> partitionData(Map<String, Set<String>> hypergraph, Set<String> accessedItems) {
        Map<String, String> newMapping = new HashMap<>(currentMapping);
        
        if (accessedItems.isEmpty()) {
            return newMapping;
        }
        Map<String, Map<String, Integer>> edgeFrequency = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : hypergraph.entrySet()) {
            String item = entry.getKey();
            for (String neighbor : entry.getValue()) {
                edgeFrequency.computeIfAbsent(item, k -> new HashMap<>())
                    .merge(neighbor, 1, Integer::sum);
            }
        }
        for (String item : accessedItems) {
            Map<String, Integer> neighborClusters = new HashMap<>();
            
            if (hypergraph.containsKey(item)) {
                for (String neighbor : hypergraph.get(item)) {
                    String neighborCluster = newMapping.get(neighbor);
                    neighborClusters.merge(neighborCluster, 1, Integer::sum);
                }
            }
            
            if (!neighborClusters.isEmpty()) {
                String bestCluster = neighborClusters.entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .orElse(currentMapping.get(item));
                
                newMapping.put(item, bestCluster);
            }
        }
        
        double temperature = 1000.0;
        double coolingRate = 0.95;
        int iterations = 500;
        Random random = new Random(42);
        
        List<String> accessedList = new ArrayList<>(accessedItems);
        int currentCost = calculateCost(hypergraph, newMapping);
        
        for (int i = 0; i < iterations; i++) {
            String item = accessedList.get(random.nextInt(accessedList.size()));
            String oldCluster = newMapping.get(item);
            String[] clusters = {"C1", "C2", "C3"};
            String newCluster = clusters[random.nextInt(3)];
            
            if (newCluster.equals(oldCluster)) continue;
            newMapping.put(item, newCluster);
            int newCost = calculateCost(hypergraph, newMapping);
            int deltaCost = newCost - currentCost;
            if (deltaCost < 0 || random.nextDouble() < Math.exp(-deltaCost / temperature)) {
                currentCost = newCost;
            } else {
                newMapping.put(item, oldCluster);
            }
            
            temperature *= coolingRate;
        }
        balanceLoad(newMapping, accessedItems);
        
        return newMapping;
    }
    
    private int calculateCost(Map<String, Set<String>> hypergraph, Map<String, String> mapping) {
        int cost = 0;
        Set<String> counted = new HashSet<>();
        
        for (Map.Entry<String, Set<String>> entry : hypergraph.entrySet()) {
            String item = entry.getKey();
            String cluster = mapping.get(item);
            
            for (String neighbor : entry.getValue()) {
                String neighborCluster = mapping.get(neighbor);
                String edge = item.compareTo(neighbor) < 0 ? item + "-" + neighbor : neighbor + "-" + item;
                
                if (!counted.contains(edge) && !cluster.equals(neighborCluster)) {
                    cost++;
                    counted.add(edge);
                }
            }
        }
        
        return cost;
    }
    
    private void balanceLoad(Map<String, String> mapping, Set<String> accessedItems) {
        Map<String, Integer> clusterCounts = calculateShardSizes(mapping);
        
        int avgSize = 3000;
        int tolerance = 300;
        boolean needsRebalancing = clusterCounts.values().stream()
            .anyMatch(count -> Math.abs(count - avgSize) > tolerance);
        
        if (!needsRebalancing) {
            return;
        }
        
        while (true) {
            String maxCluster = clusterCounts.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
            String minCluster = clusterCounts.entrySet().stream()
                .min(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
            
            if (maxCluster == null || minCluster == null) break;
            
            int maxSize = clusterCounts.get(maxCluster);
            int minSize = clusterCounts.get(minCluster);
            
            if (maxSize - minSize <= tolerance) break;
            boolean moved = false;
            for (String item : accessedItems) {
                if (mapping.get(item).equals(maxCluster)) {
                    mapping.put(item, minCluster);
                    clusterCounts.put(maxCluster, maxSize - 1);
                    clusterCounts.put(minCluster, minSize + 1);
                    moved = true;
                    break;
                }
            }
            
            if (!moved) break;
        }
    }
    
    private Map<String, Integer> calculateShardSizes(Map<String, String> mapping) {
        Map<String, Integer> sizes = new HashMap<>();
        sizes.put("C1", 0);
        sizes.put("C2", 0);
        sizes.put("C3", 0);
        
        for (String cluster : mapping.values()) {
            sizes.merge(cluster, 1, Integer::sum);
        }
        
        return sizes;
    }
    
    private int countCrossShardTransactions(List<Txn> transactions, Map<String, String> mapping) {
        int count = 0;
        for (Txn txn : transactions) {
            String fromCluster = mapping.get(txn.getFrom());
            String toCluster = mapping.get(txn.getTo());
            if (!fromCluster.equals(toCluster)) {
                count++;
            }
        }
        return count;
    }
    
    public synchronized void applyReshardingPlan(ReshardingPlan plan) {
        for (DataMovement movement : plan.movements) {
            currentMapping.put(movement.itemId, movement.toCluster);
        }
        logger.info("Applied resharding plan with {} movements", plan.movements.size());
    }
    
    public static class ReshardingPlan {
        public final ReshardingStatistics stats;
        public final List<DataMovement> movements;
        
        public ReshardingPlan(ReshardingStatistics stats, List<DataMovement> movements) {
            this.stats = stats;
            this.movements = movements;
        }
    }
    
    public static class ReshardingStatistics {
        public final int totalTransactions;
        public final int crossShardBefore;
        public final int crossShardAfter;
        public final double improvementPercent;
        public final Map<String, Integer> shardSizesBefore;
        public final Map<String, Integer> shardSizesAfter;
        
        public ReshardingStatistics(int totalTransactions, int crossShardBefore, 
                                   int crossShardAfter, double improvementPercent,
                                   Map<String, Integer> shardSizesBefore,
                                   Map<String, Integer> shardSizesAfter) {
            this.totalTransactions = totalTransactions;
            this.crossShardBefore = crossShardBefore;
            this.crossShardAfter = crossShardAfter;
            this.improvementPercent = improvementPercent;
            this.shardSizesBefore = shardSizesBefore;
            this.shardSizesAfter = shardSizesAfter;
        }
        
        @Override
        public String toString() {
            return String.format(
                "Resharding Statistics:\n" +
                "  Total Transactions Analyzed: %d\n" +
                "  Cross-Shard Before: %d (%.1f%%)\n" +
                "  Cross-Shard After: %d (%.1f%%)\n" +
                "  Improvement: %.1f%%\n" +
                "  Shard Sizes Before: %s\n" +
                "  Shard Sizes After: %s",
                totalTransactions,
                crossShardBefore, totalTransactions > 0 ? (crossShardBefore * 100.0 / totalTransactions) : 0,
                crossShardAfter, totalTransactions > 0 ? (crossShardAfter * 100.0 / totalTransactions) : 0,
                improvementPercent,
                shardSizesBefore,
                shardSizesAfter
            );
        }
    }
    
    public static class DataMovement {
        public final String itemId;
        public final String fromCluster;
        public final String toCluster;
        
        public DataMovement(String itemId, String fromCluster, String toCluster) {
            this.itemId = itemId;
            this.fromCluster = fromCluster;
            this.toCluster = toCluster;
        }
        
        @Override
        public String toString() {
            return String.format("(%s, %s, %s)", itemId, fromCluster, toCluster);
        }
    }
}
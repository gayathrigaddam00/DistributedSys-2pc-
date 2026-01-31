package com.example.paxos.client;

import com.example.paxos.reshard.ReshardingCoordinator;
import com.example.paxos.reshard.ReshardingManager;
import com.example.paxos.v1.*;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import com.example.paxos.benchmark.BenchmarkGenerator;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Command(name = "testrunner", mixinStandardHelpOptions = true, version = "1.0",
        description = "Distributed Banking System Test Runner")
public class TestRunner implements Callable<Integer> {

    @CommandLine.Option(names = {"-f", "--file"}, description = "Path to the test CSV file", defaultValue = "test.csv")
    private String testFile;

    private static final Map<String, Integer> NODE_PORTS = Map.of(
            "n1", 50051, "n2", 50052, "n3", 50053,
            "n4", 50054, "n5", 50055, "n6", 50056,
            "n7", 50057, "n8", 50058, "n9", 50059
    );

    private static final List<String> ALL_NODES = List.of(
            "n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9"
    );

    private List<TestSet> testSets = new ArrayList<>();
    private int currentSetIndex = -1;
    private ReshardingManager reshardingManager;
    private BenchmarkGenerator benchmarkGenerator;

 
    private final ConcurrentHashMap<String, QuorumStatus> quorumCache = new ConcurrentHashMap<>();
    private static final long QUORUM_CACHE_TTL_MS = 2000;

    private static class QuorumStatus {
        final boolean hasQuorum;
        final int availableNodes;
        final int requiredNodes;
        final long timestamp;
        
        QuorumStatus(boolean hasQuorum, int available, int required) {
            this.hasQuorum = hasQuorum;
            this.availableNodes = available;
            this.requiredNodes = required;
            this.timestamp = System.currentTimeMillis();
        }
        
        boolean isExpired() {
            return System.currentTimeMillis() - timestamp > QUORUM_CACHE_TTL_MS;
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new TestRunner()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        reshardingManager = new ReshardingManager(1000);
        benchmarkGenerator = new BenchmarkGenerator();
        loadTestFile(testFile);
        runCommandLineInterface();
        return 0;
    }

    private void runCommandLineInterface() {
        Scanner scanner = new Scanner(System.in);
        
        while (true) {
            System.out.print("> ");
            if (!scanner.hasNextLine()) break;
            
            String input = scanner.nextLine().trim();
            if (input.isEmpty()) continue;
            
            String[] parts = input.split("\\s+", 2);
            String command = parts[0].toLowerCase();
            
            switch (command) {
                case "next":
                    processNextSet();
                    break;
                case "skip":
                    skipCurrentSet();
                    break;
                case "benchmark":
                    handleBenchmark();
                    break;
                case "printdb":
                    handlePrintDB();
                    break;
                case "end":
                    endCurrentSet();
                    break;
                case "printbalance":
                    if (parts.length > 1) {
                        handlePrintBalance(parts[1]);
                    } else {
                        System.out.println("Usage: printbalance <itemId>");
                    }
                    break;
                case "printreshard":
                    handlePrintReshard();
                    break;
                case "printview":
                    handlePrintView();
                    break;
                case "performance":
                    handlePerformance();
                    break;
                case "help":
                    printHelp();
                    break;
                case "exit":
                case "quit":
                    System.out.println("Exiting...");
                    return;
                default:
                    System.out.println("Unknown command: '" + command + "'. Type 'help' for a list of commands.");
            }
        }
    }
private void endCurrentSet() {
    if (currentSetIndex < 0 || currentSetIndex >= testSets.size()) {
        System.out.println("No active test set to end.");
        return;
    }
    
    System.out.println("\n=== ENDING TEST SET " + testSets.get(currentSetIndex).setNumber + " ===");
    System.out.println("Stopping any pending transactions and printing performance...");
    
    handlePerformance();
    
    System.out.println("Test set ended. Type 'next' to continue to the next set.");
}
    private void loadTestFile(String filename) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            TestSet currentSet = null;
            
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                
                String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                if (parts.length < 2) continue;

                if (parts[0].trim().equalsIgnoreCase("Set Number")) {
                    continue;
                }
                
                String setNumStr = parts[0].trim();
                String command = parts[1].trim();

                if (command.startsWith("\"") && command.endsWith("\"")) {
                    command = command.substring(1, command.length() - 1);
                }
                
                if (!setNumStr.isEmpty()) {
                    try {
                        int setNumber = Integer.parseInt(setNumStr);
                        
                        if (currentSet == null || currentSet.setNumber != setNumber) {
                            if (currentSet != null) {
                                testSets.add(currentSet);
                            }
                            
                            String liveNodesStr = parts.length > 2 ? parts[2].trim() : "";
                            List<String> liveNodes = parseLiveNodes(liveNodesStr);
                            currentSet = new TestSet(setNumber, liveNodes);
                        }
                    } catch (NumberFormatException e) {
                        System.err.println("Skipping invalid set number: " + line);
                        continue;
                    }
                } else {
                    if (currentSet == null) {
                        System.err.println("Skipping orphan transaction (no set defined): " + line);
                        continue;
                    }
                }
                
                if (!command.isEmpty()) {
                    currentSet.commands.add(command);
                }
            }
            
            if (currentSet != null) {
                testSets.add(currentSet);
            }
            
            System.out.println("Loaded " + testSets.size() + " test sets from " + filename);
            
        } catch (IOException e) {
            System.err.println("Error loading test file: " + e.getMessage());
        }
    }

    private List<String> parseLiveNodes(String liveNodesStr) {
        if (liveNodesStr.isEmpty()) return new ArrayList<>(ALL_NODES);
        
        liveNodesStr = liveNodesStr.replaceAll("[\"\\[\\]]", "");
        String[] nodes = liveNodesStr.split("[,\\s]+");
        
        return Arrays.stream(nodes)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    private void processNextSet() {
        currentSetIndex++;
        
        if (currentSetIndex >= testSets.size()) {
            System.out.println("No more test sets to process.");
            return;
        }
        
        TestSet set = testSets.get(currentSetIndex);
        
        System.out.println("\n=======================================================");
        System.out.println("Processing test set " + set.setNumber);
        System.out.println("Live Nodes: " + set.liveNodes);
        System.out.println("=======================================================");
        
        flushSystemState();
        configureLiveNodes(set.liveNodes);
        triggerInitialElection();
        processCommands(set);
    }

    private void skipCurrentSet() {
        currentSetIndex++;
        if (currentSetIndex >= testSets.size()) {
            System.out.println("No more test sets to skip.");
        } else {
            System.out.println("Skipped test set " + testSets.get(currentSetIndex).setNumber);
        }
    }

    private void flushSystemState() {
        System.out.println("Flushing system state...");
        quorumCache.clear();
        
        for (String nodeId : ALL_NODES) {
            int port = NODE_PORTS.get(nodeId);
            try (BankingClient client = new BankingClient("localhost", port)) {
                client.flushState();
                System.out.println("  Flushed state for " + nodeId);
            } catch (Exception e) {
                System.err.println("  Failed to flush " + nodeId + ": " + e.getMessage());
            }
        }
        
        System.out.println("System state flush complete. Performance metrics reset.");
    }

    private void configureLiveNodes(List<String> liveNodes) {
        System.out.println("Configuring nodes...");
        
        for (String nodeId : ALL_NODES) {
            int port = NODE_PORTS.get(nodeId);
            boolean shouldBeActive = liveNodes.contains(nodeId);
            
            try (BankingClient client = new BankingClient("localhost", port)) {
                client.setNodeStatus(shouldBeActive);
            } catch (Exception e) {
                System.err.println("Failed to configure " + nodeId + ": " + e.getMessage());
            }
        }
        
        System.out.println("Node configuration complete.");
    }

    private void triggerInitialElection() {
        String[] clusters = {"C1", "C2", "C3"};
        String[] leaders = {"n1", "n4", "n7"};
        
        for (int i = 0; i < clusters.length; i++) {
            String nodeId = leaders[i];
            int port = NODE_PORTS.get(nodeId);
            
            try (BankingClient client = new BankingClient("localhost", port)) {
                client.startElection();
                System.out.println("  Triggered initial election on cluster " + clusters[i]);
            } catch (Exception e) {
                System.err.println("Failed to trigger election on " + nodeId + ": " + e.getMessage());
            }
        }
    }

    private void processCommands(TestSet set) {
        System.out.println("Processing commands...");
        
        List<String> currentBatch = new ArrayList<>();
        
        for (String command : set.commands) {
            if (command.startsWith("F(")) {
                if (!currentBatch.isEmpty()) {
                    processTransactionsInParallel(currentBatch);
                    currentBatch.clear();
                }
                
                String nodeId = command.substring(2, command.length() - 1);
                failNode(nodeId);
                
            } else if (command.startsWith("R(")) {
                if (!currentBatch.isEmpty()) {
                    processTransactionsInParallel(currentBatch);
                    currentBatch.clear();
                }
                
                String nodeId = command.substring(2, command.length() - 1);
                recoverNode(nodeId);
                
            } else {
                currentBatch.add(command);
            }
        }
        
        if (!currentBatch.isEmpty()) {
            processTransactionsInParallel(currentBatch);
        }
    }

    private void processTransactionsInParallel(List<String> transactions) {
        if (transactions.isEmpty()) return;

        ExecutorService executor = Executors.newFixedThreadPool(
            Math.min(transactions.size(), 10) 
        );
        
        List<Future<Boolean>> futures = new ArrayList<>();
        
        for (String txnStr : transactions) {
            Future<Boolean> future = executor.submit(() -> {
                parseAndSubmitTransaction(txnStr);
                return true;
            });
            futures.add(future);
        }
        
        executor.shutdown();
        try {
            executor.awaitTermination(120, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void parseAndSubmitTransaction(String line) {
        line = line.replaceAll("[\"()]", "").trim();
        
        if (line.isEmpty()) return;
        
        if (line.startsWith("-")) {
            String accountId = line.substring(1).trim();
            handleReadOnlyTransaction(accountId);
            return;
        }
        
        String[] parts = line.split(",");
        if (parts.length < 1) return;
        
        try {
            if (parts.length == 3) {
                String senderId = parts[0].trim();
                String receiverId = parts[1].trim();
                int amount = Integer.parseInt(parts[2].trim());
                handleWriteTransaction(senderId, receiverId, amount);
            }
        } catch (Exception e) {
            System.err.println("    Error processing transaction: " + e.getMessage());
        }
    }

    private boolean checkClusterQuorum(String cluster) {
        QuorumStatus cached = quorumCache.get(cluster);
        if (cached != null && !cached.isExpired()) {
            return cached.hasQuorum;
        }
        
        List<String> clusterNodes = getNodesInCluster(cluster);
        int totalNodes = clusterNodes.size();
        int requiredQuorum = (totalNodes / 2) + 1;
        
        CountDownLatch latch = new CountDownLatch(totalNodes);
        AtomicInteger responsiveNodes = new AtomicInteger(0);
        
        for (String nodeId : clusterNodes) {
            CompletableFuture.runAsync(() -> {
                int port = NODE_PORTS.get(nodeId);
                try (BankingClient client = new BankingClient("localhost", port)) {
                    client.isLeader();
                    responsiveNodes.incrementAndGet();
                } catch (Exception e) {
                
                }
                latch.countDown();
            });
        }
        
        try {
            latch.await(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        int available = responsiveNodes.get();
        boolean hasQuorum = available >= requiredQuorum;
        
        QuorumStatus status = new QuorumStatus(hasQuorum, available, requiredQuorum);
        quorumCache.put(cluster, status);
        
        if (!hasQuorum) {
            System.out.println("    ⚠ Cluster " + cluster + ": " + available + "/" + 
                             totalNodes + " nodes available (need " + requiredQuorum + " for quorum)");
        }
        
        return hasQuorum;
    }

    private void invalidateQuorumCache(String nodeId) {
        String cluster = getClusterForNode(nodeId);
        quorumCache.remove(cluster);
        System.out.println("    Quorum cache invalidated for cluster " + cluster);
    }

private void handleReadOnlyTransaction(String accountId) {
    String cluster = getClusterForItem(accountId);
    String expectedLeader = getExpectedLeader(cluster);  
    List<String> clusterNodes = getNodesInCluster(cluster);
    

    List<String> tryOrder = new ArrayList<>();
    tryOrder.add(expectedLeader);
    for (String node : clusterNodes) {
        if (!node.equals(expectedLeader)) {
            tryOrder.add(node);
        }
    }
    
    boolean success = false;
    
    for (int attempt = 0; attempt < 3 && !success; attempt++) {
        for (String nodeId : tryOrder) { 
            int port = NODE_PORTS.get(nodeId);
            
            try (BankingClient client = new BankingClient("localhost", port)) {
                Txn txn = Txn.newBuilder()
                        .setFrom(accountId)
                        .setTo(accountId)
                        .setAmount(0)
                        .setClientId("client-ro-" + accountId + "-" + System.nanoTime())
                        .setClientTs(System.currentTimeMillis())
                        .setType(TxnType.READ_ONLY)
                        .build();
                
                ClientReply reply = client.submitWithTimeout(txn, 500);
                
                if (reply.getSuccess()) {
                    success = true;
                    System.out.println("  [READ_ONLY] " + accountId + ": SUCCESS - " + reply.getMessage());
                    break;
                    
                }
            } catch (Exception e) {
               
            }
        }
        
        if (!success && attempt < 2) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}


   private void handleWriteTransaction(String senderId, String receiverId, int amount) {
    String senderCluster = getClusterForItem(senderId);
    String receiverCluster = getClusterForItem(receiverId);
    TxnType type = senderCluster.equals(receiverCluster) ? 
                  TxnType.INTRA_SHARD : TxnType.CROSS_SHARD;
    
    Txn txn = Txn.newBuilder()
            .setFrom(senderId)
            .setTo(receiverId)
            .setAmount(amount)
            .setClientId("client-" + senderId + "-" + receiverId + "-" + System.nanoTime())
            .setClientTs(System.currentTimeMillis())
            .setType(type)
            .build();
    
   
    String targetCluster = senderCluster;
    
    boolean success = false;
    int maxRetries = 3;
    long timeout = (type == TxnType.CROSS_SHARD) ? 3000 : 2000;
    
    for (int attempt = 0; attempt < maxRetries && !success; attempt++) {
        String expectedLeader = getExpectedLeader(targetCluster);
        List<String> clusterNodes = getNodesInCluster(targetCluster);
    
        List<String> tryOrder = new ArrayList<>();
        tryOrder.add(expectedLeader);
        for (String node : clusterNodes) {
            if (!node.equals(expectedLeader)) {
                tryOrder.add(node);
            }
        }
        
        for (String targetNodeId : tryOrder) {
            int port = NODE_PORTS.get(targetNodeId);
            
            try (BankingClient client = new BankingClient("localhost", port)) {
                ClientReply reply = client.submitWithTimeout(txn, timeout);
                
                if (reply.getSuccess()) {
                    reshardingManager.recordTransaction(txn);
                    success = true;
                    break;
                    
                } else if (reply.getMessage().equals("NOT_LEADER")) {
                    continue;  
                    
                } else if (reply.getMessage().startsWith("WRONG_SHARD:")) {
        
                    String correctCluster = reply.getMessage().substring("WRONG_SHARD:".length());
                    System.out.println("    Item moved to " + correctCluster + " after resharding, redirecting...");
                    targetCluster = correctCluster;
                    
                    
                    String newReceiverCluster = getClusterForItem(receiverId);
                    if (targetCluster.equals(newReceiverCluster)) {
                        type = TxnType.INTRA_SHARD;
                    } else {
                        type = TxnType.CROSS_SHARD;
                    }
                    
                 
                    txn = Txn.newBuilder()
                        .setFrom(senderId)
                        .setTo(receiverId)
                        .setAmount(amount)
                        .setClientId(txn.getClientId())
                        .setClientTs(txn.getClientTs())
                        .setType(type)
                        .build();
                    
                    break; 
                    
                } else if (reply.getMessage().equals("COMMIT_UNCERTAIN_RETRY")) {
                    Thread.sleep(100);
                    
                } else if (reply.getMessage().contains("INSUFFICIENT") || 
                           reply.getMessage().contains("DATA_NOT_AVAILABLE") ||
                           reply.getMessage().contains("ABORTED")) {
                    break; 
                    
                } else {
                    Thread.sleep(50);
                }
                
            } catch (Exception e) {
               
            }
        }
    }
}
  
    private String getExpectedLeader(String cluster) {
        switch (cluster) {
            case "C1": return "n1";
            case "C2": return "n4";
            case "C3": return "n7";
            default: return "n1";
        }
    }

    private void failNode(String nodeId) {
        System.out.println("  Failing node: " + nodeId);
        if (!NODE_PORTS.containsKey(nodeId)) {
            System.err.println("  Unknown node: " + nodeId);
            return;
        }
        int port = NODE_PORTS.get(nodeId);
        
        boolean wasLeader = false;
        try (BankingClient client = new BankingClient("localhost", port)) {
            wasLeader = client.isLeader();
            client.setNodeStatus(false);
        } catch (Exception e) {
            System.err.println("Failed to fail node " + nodeId + ": " + e.getMessage());
            return;
        }
        
        invalidateQuorumCache(nodeId);
        
        if (wasLeader) {
            System.out.println("  " + nodeId + " was a leader, triggering election...");
            triggerElectionInCluster(nodeId);
        } else {
            System.out.println("  " + nodeId + " was not a leader, no election needed");
        }
    }

    private void triggerElectionInCluster(String failedNodeId) {
        String cluster = getClusterForNode(failedNodeId);
        List<String> clusterNodes = getNodesInCluster(cluster);
        
        System.out.println("  Triggering election in cluster " + cluster);
        
        int failedIndex = clusterNodes.indexOf(failedNodeId);
        
        for (int i = 1; i <= clusterNodes.size(); i++) {
            int nextIndex = (failedIndex + i) % clusterNodes.size();
            String nextNode = clusterNodes.get(nextIndex);
            int nextPort = NODE_PORTS.get(nextNode);
            
            try (BankingClient client = new BankingClient("localhost", nextPort)) {
                client.startElection();
                System.out.println("  Successfully triggered election on " + nextNode);
                Thread.sleep(1500);
                return;
            } catch (Exception e) {
                System.err.println("  Failed to trigger election on " + nextNode + ", trying next...");
            }
        }
        
        System.err.println("  WARNING: Could not trigger election in cluster " + cluster);
    }

    private String getClusterForNode(String nodeId) {
        int nodeNum = Integer.parseInt(nodeId.substring(1));
        if (nodeNum <= 3) return "C1";
        if (nodeNum <= 6) return "C2";
        return "C3";
    }

    private void recoverNode(String nodeId) {
        System.out.println("  Recovering node: " + nodeId);
        if (!NODE_PORTS.containsKey(nodeId)) {
            System.err.println("  Unknown node: " + nodeId);
            return;
        }
        int port = NODE_PORTS.get(nodeId);
        
        try (BankingClient client = new BankingClient("localhost", port)) {
            client.setNodeStatus(true);
            Thread.sleep(1000);
            
            invalidateQuorumCache(nodeId);
            
            System.out.println("  " + nodeId + " recovered successfully");
        } catch (Exception e) {
            System.err.println("Failed to recover node " + nodeId + ": " + e.getMessage());
        }
    }

    private void handleBenchmark() {
    Scanner scanner = new Scanner(System.in);
    
    System.out.println("\n=== BENCHMARK CONFIGURATION ===");
    System.out.print("Number of transactions: ");
    int numTxns = scanner.nextInt();
    
    System.out.print("Read-only percentage (0-100): ");
    double readOnlyPercent = scanner.nextDouble();
    
    System.out.print("Cross-shard percentage for writes (0-100): ");
    double crossShardPercent = scanner.nextDouble();
    
    System.out.print("Skewness (0=uniform, 1=highly skewed): ");
    double skewness = scanner.nextDouble();
    
    System.out.println("\n=== GENERATING BENCHMARK WORKLOAD ===");
    System.out.println("Transactions: " + numTxns);
    System.out.println("Read-only: " + readOnlyPercent + "%");
    System.out.println("Cross-shard (writes): " + crossShardPercent + "%");
    System.out.println("Skewness: " + skewness);
    
    List<String> transactions = benchmarkGenerator.generateBenchmark(
        numTxns, readOnlyPercent, crossShardPercent, skewness);
    
    System.out.println("\nGenerated " + transactions.size() + " transactions");
    
    long readOnly = transactions.stream().filter(t -> t.startsWith("-")).count();
    long write = transactions.size() - readOnly;
    System.out.println("  Read-only: " + readOnly + " (" + 
        String.format("%.1f", readOnly * 100.0 / transactions.size()) + "%)");
    System.out.println("  Write: " + write + " (" + 
        String.format("%.1f", write * 100.0 / transactions.size()) + "%)");
    
    System.out.println("\n=== PREPARING SYSTEM ===");
    flushSystemState();
    configureLiveNodes(ALL_NODES);
    triggerInitialElection();
    
    System.out.println("Waiting for leader elections...");
    try {
        Thread.sleep(5000);  
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
    }
    
    System.out.println("\n=== EXECUTING BENCHMARK ===");
    long startTime = System.currentTimeMillis();
    
    int batchSize = 100;
    for (int i = 0; i < transactions.size(); i += batchSize) {
        int end = Math.min(i + batchSize, transactions.size());
        List<String> batch = transactions.subList(i, end);
        processTransactionsInParallel(batch);
    }
    
    long endTime = System.currentTimeMillis();
    double durationSec = (endTime - startTime) / 1000.0;
    
    // ✅ Calculate actual system throughput (client-side)
    double systemThroughput = numTxns / durationSec;
    
    System.out.println("\n=== BENCHMARK RESULTS ===");
    System.out.println("Duration: " + String.format("%.2f", durationSec) + " seconds");
    System.out.println("Submitted Transactions: " + numTxns);
    System.out.println("System Throughput: " + String.format("%.2f", systemThroughput) + " txn/sec");
    
    System.out.println("\n=== PER-NODE METRICS ===");
    handlePerformance(numTxns, durationSec);
}


private void handlePerformance() {
    handlePerformance(-1, -1);  
}

private void handlePerformance(int benchmarkTxns, double benchmarkDuration) {
    System.out.println("Aggregated across all 9 nodes:\n");
    
    long totalTxns = 0;
    long successTxns = 0;
    double totalLatency = 0.0;
    int nodeCount = 0;
    Map<String, PerformanceResponse> leaderStats = new HashMap<>();
    
    for (String nodeId : ALL_NODES) {
        int port = NODE_PORTS.get(nodeId);
        
        try (BankingClient client = new BankingClient("localhost", port)) {
            PerformanceResponse response = client.getPerformance();
            
            totalTxns += response.getTotalTxns();
            successTxns += response.getSuccessTxns();
            totalLatency += response.getAvgLatency();
            nodeCount++;
            if (response.getTotalTxns() > 0) {
                leaderStats.put(nodeId, response);
            }
           
            
        } catch (Exception e) {
            System.err.println("Node " + nodeId + ": ERROR - " + e.getMessage());
        }
    }
    
    if (nodeCount > 0) {
        System.out.println("=== AGGREGATE STATISTICS ===");
        System.out.println("Total Transactions (all nodes): " + totalTxns);
        System.out.println("Successful Transactions: " + successTxns);
        System.out.println("Failed Transactions: " + (totalTxns - successTxns));
        
        // ✅ Calculate success rate
        double successRate = totalTxns > 0 ? (successTxns * 100.0 / totalTxns) : 0.0;
        System.out.println("Success Rate: " + String.format("%.1f", successRate) + "%");
        
        // ✅ Show correct system throughput if available from benchmark
        if (benchmarkTxns > 0 && benchmarkDuration > 0) {
            double systemThroughput = benchmarkTxns / benchmarkDuration;
            System.out.println("System Throughput: " + String.format("%.2f", systemThroughput) + " txn/sec");
        }
        
        // Average latency across all nodes
        System.out.println("Average Latency: " + String.format("%.2f", totalLatency / nodeCount) + " ms");
        
        // ✅ Show leader-only metrics for comparison
        if (!leaderStats.isEmpty()) {
            System.out.println("\n=== LEADER NODE STATISTICS ===");
            System.out.println("Active Leader Nodes: " + leaderStats.size());
            
            double leaderAvgThroughput = leaderStats.values().stream()
                .mapToDouble(PerformanceResponse::getThroughput)
                .average()
                .orElse(0.0);
            
            double leaderAvgLatency = leaderStats.values().stream()
                .mapToDouble(PerformanceResponse::getAvgLatency)
                .average()
                .orElse(0.0);
            
            System.out.println("Average Leader Throughput: " + String.format("%.2f", leaderAvgThroughput) + " txn/sec");
            System.out.println("Average Leader Latency: " + String.format("%.2f", leaderAvgLatency) + " ms");
        }
        
    } else {
        System.err.println("Failed to retrieve performance metrics from any node.");
    }
}

    private void handlePrintDB() {
        System.out.println("Databases of all nodes (Modified items only):");
        
        for (String nodeId : ALL_NODES) {
            int port = NODE_PORTS.get(nodeId);
            
            try (BankingClient client = new BankingClient("localhost", port)) {
                PrintAllDBResponse response = client.printDB();
                Map<String, PrintDBResponse> allDbs = response.getAllDbsMap();
                
                System.out.println("Node: " + nodeId);
                
                if (allDbs.isEmpty() || !allDbs.containsKey(nodeId)) {
                    System.out.println("  (No modified items)");
                } else {
                    PrintDBResponse nodeDb = allDbs.get(nodeId);
                    Map<String, Integer> db = nodeDb.getDbMap();
                    
                    if (db.isEmpty()) {
                        System.out.println("  (No modified items)");
                    } else {
                        db.entrySet().stream()
                            .sorted(Map.Entry.comparingByKey((a, b) -> {
                                try {
                                    return Integer.compare(Integer.parseInt(a), Integer.parseInt(b));
                                } catch (NumberFormatException e) {
                                    return a.compareTo(b);
                                }
                            }))
                            .forEach(entry -> System.out.println("  " + entry.getKey() + ": " + entry.getValue()));
                    }
                }
            } catch (Exception e) {
                System.err.println("Node: " + nodeId);
                System.err.println("  Error: " + e.getMessage());
            }
        }
    }

    private void handlePrintBalance(String itemId) {
        String cluster = getClusterForItem(itemId);
        List<String> clusterNodes = getNodesInCluster(cluster);
        
        System.out.print("PrintBalance(" + itemId + "): ");
        List<String> results = new ArrayList<>();
        
        for (String nodeId : clusterNodes) {
            int port = NODE_PORTS.get(nodeId);
            
            try (BankingClient client = new BankingClient("localhost", port)) {
                BalanceResponse response = client.getBalance(itemId);
                Map<String, Integer> balances = response.getBalancesMap();
                
                if (balances.containsKey(nodeId)) {
                    int balance = balances.get(nodeId);
                    results.add(nodeId + ": " + balance);
                } else {
                    results.add(nodeId + ": NOT_FOUND");
                }
            } catch (Exception e) {
                results.add(nodeId + ": ERROR (" + e.getMessage() + ")");
            }
        }
        
        System.out.println(String.join(", ", results));
    }

    private void handlePrintReshard() {
        System.out.println("\n=== RESHARDING ANALYSIS ===");
        
        ReshardingManager.ReshardingPlan plan = reshardingManager.generateReshardingPlan();
        
        System.out.println("\nResharding Statistics:");
        System.out.println("  Total Transactions Analyzed: " + plan.stats.totalTransactions);
        System.out.println("  Cross-Shard Before: " + plan.stats.crossShardBefore + 
                         " (" + String.format("%.1f", plan.stats.totalTransactions > 0 ? 
                         (plan.stats.crossShardBefore * 100.0 / plan.stats.totalTransactions) : 0) + "%)");
        System.out.println("  Cross-Shard After: " + plan.stats.crossShardAfter + 
                         " (" + String.format("%.1f", plan.stats.totalTransactions > 0 ? 
                         (plan.stats.crossShardAfter * 100.0 / plan.stats.totalTransactions) : 0) + "%)");
        System.out.println("  Improvement: " + String.format("%.1f", plan.stats.improvementPercent) + "%");
        System.out.println("  Shard Sizes Before: " + plan.stats.shardSizesBefore);
        System.out.println("  Shard Sizes After: " + plan.stats.shardSizesAfter);
        
        System.out.println("\n=== DATA MOVEMENTS ===");
        System.out.println("Total items to move: " + plan.movements.size());
        
        if (plan.movements.isEmpty()) {
            System.out.println("No resharding needed - current partitioning is optimal.");
            return;
        }
        
        Map<String, List<ReshardingManager.DataMovement>> movementsBySource = 
            plan.movements.stream()
                .collect(Collectors.groupingBy(m -> m.fromCluster));
        
        for (Map.Entry<String, List<ReshardingManager.DataMovement>> entry : movementsBySource.entrySet()) {
            System.out.println("\nFrom " + entry.getKey() + " (" + entry.getValue().size() + " items):");
            for (ReshardingManager.DataMovement movement : entry.getValue()) {
                System.out.println("  (" + movement.itemId + ", " + 
                                 movement.fromCluster + ", " + movement.toCluster + ")");
            }
        }
        
        System.out.println("\n=== RESHARDING EXECUTION ===");
        System.out.print("Apply this resharding plan? (yes/no): ");
        
        Scanner scanner = new Scanner(System.in);
        String response = scanner.nextLine().trim().toLowerCase();
        
        if (response.equals("yes") || response.equals("y")) {
            executeResharding(plan);
        } else {
            System.out.println("Resharding cancelled.");
        }
    }

    private void executeResharding(ReshardingManager.ReshardingPlan plan) {
        ReshardingCoordinator coordinator = new ReshardingCoordinator();
        boolean success = coordinator.executeResharding(plan);
        
        if (success) {
            System.out.println("\n=== RESHARDING COMPLETE ===");
            System.out.println("All data has been successfully moved and mappings updated.");
            reshardingManager.applyReshardingPlan(plan);
        } else {
            System.out.println("\n=== RESHARDING FAILED ===");
            System.out.println("Some operations failed. Check logs for details.");
        }
    }

    private void handlePrintView() {
        System.out.println("New-View messages exchanged:");
        
        for (String nodeId : ALL_NODES) {
            int port = NODE_PORTS.get(nodeId);
            
            try (BankingClient client = new BankingClient("localhost", port)) {
                PrintViewResponse response = client.printView();
                List<NewViewReq> views = response.getViewsList();
                
                if (views.isEmpty()) {
                    System.out.println("Node " + nodeId + ": No new-view messages");
                } else {
                    System.out.println("Node " + nodeId + ":");
                    for (NewViewReq view : views) {
                        System.out.println("  Epoch: " + view.getB().getEpoch() + 
                                         ", Leader: " + view.getB().getLeaderId() +
                                         ", Rebroadcast entries: " + view.getRebroadcastBatchCount());
                    }
                }
            } catch (Exception e) {
                System.err.println("Node " + nodeId + ": Error fetching views - " + e.getMessage());
            }
        }
    }

    
    private String getClusterForItem(String itemId) {
        try {
            int id = Integer.parseInt(itemId);
            if (id <= 3000) return "C1";
            if (id <= 6000) return "C2";
            return "C3";
        } catch (NumberFormatException e) {
            return "C1";
        }
    }

    private List<String> getNodesInCluster(String cluster) {
        switch (cluster) {
            case "C1": return List.of("n1", "n2", "n3");
            case "C2": return List.of("n4", "n5", "n6");
            case "C3": return List.of("n7", "n8", "n9");
            default: return List.of();
        }
    }

    private void printHelp() {
        System.out.println("\nAvailable commands:");
        System.out.println("  next             - Process the next test set");
        System.out.println("  end              - End current test set and print performance");  // ← NEW
        System.out.println("  skip             - Skip the current test set");
        System.out.println("  benchmark        - Run configurable benchmark");
        System.out.println("  printdb          - Print modified items on all nodes");
        System.out.println("  printbalance <id> - Print balance of item <id> across cluster");
        System.out.println("  printreshard     - Analyze and execute resharding");
        System.out.println("  printview        - Print new-view messages");
        System.out.println("  performance      - Show performance metrics");
        System.out.println("  help             - Show this help message");
        System.out.println("  exit/quit        - Exit the program");
        System.out.println();
    }

    private static class TestSet {
        int setNumber;
        List<String> liveNodes;
        List<String> commands;

        TestSet(int setNumber, List<String> liveNodes) {
            this.setNumber = setNumber;
            this.liveNodes = liveNodes;
            this.commands = new ArrayList<>();
        }
    }
}

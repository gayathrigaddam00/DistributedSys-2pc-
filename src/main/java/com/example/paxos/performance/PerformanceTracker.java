package com.example.paxos.performance;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PerformanceTracker {
    private final AtomicLong totalTransactions = new AtomicLong(0);
    private final AtomicLong successfulTransactions = new AtomicLong(0);
    private final AtomicLong failedTransactions = new AtomicLong(0);
    private final AtomicLong totalLatencyMs = new AtomicLong(0);
    private volatile long startTime = 0;
    private volatile long endTime = 0;
    
    private final ConcurrentHashMap<String, Long> transactionStartTimes = new ConcurrentHashMap<>();

    public void startBenchmark() {
        this.startTime = System.currentTimeMillis();
        this.totalTransactions.set(0);
        this.successfulTransactions.set(0);
        this.failedTransactions.set(0);
        this.totalLatencyMs.set(0);
        this.transactionStartTimes.clear();
    }

    public void endBenchmark() {
        this.endTime = System.currentTimeMillis();
    }

    public void recordTransactionStart(String txnId) {
        transactionStartTimes.put(txnId, System.currentTimeMillis());
    }

    public void recordTransactionEnd(String txnId, boolean success) {
        Long startTime = transactionStartTimes.remove(txnId);
        if (startTime != null) {
            long latency = System.currentTimeMillis() - startTime;
            totalLatencyMs.addAndGet(latency);
        }
        
        totalTransactions.incrementAndGet();
        if (success) {
            successfulTransactions.incrementAndGet();
        } else {
            failedTransactions.incrementAndGet();
        }
    }

    public long getTotalTransactions() {
        return totalTransactions.get();
    }

    public long getSuccessfulTransactions() {
        return successfulTransactions.get();
    }

    public long getFailedTransactions() {
        return failedTransactions.get();
    }

    public long getDurationMs() {
        if (startTime == 0) return 0;
        long end = (endTime > 0) ? endTime : System.currentTimeMillis();
        return end - startTime;
    }

    public double getThroughput() {
        long duration = getDurationMs();
        if (duration == 0) return 0.0;
        return (totalTransactions.get() * 1000.0) / duration;
    }

    public double getAverageLatencyMs() {
        long total = totalTransactions.get();
        if (total == 0) return 0.0;
        return totalLatencyMs.get() / (double) total;
    }

    public void reset() {
        totalTransactions.set(0);
        successfulTransactions.set(0);
        failedTransactions.set(0);
        totalLatencyMs.set(0);
        startTime = 0;
        endTime = 0;
        transactionStartTimes.clear();
    }

    public String getReport() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== Performance Report ===\n");
        sb.append(String.format("Total Transactions: %d\n", getTotalTransactions()));
        sb.append(String.format("Successful: %d\n", getSuccessfulTransactions()));
        sb.append(String.format("Failed: %d\n", getFailedTransactions()));
        sb.append(String.format("Duration: %.2f seconds\n", getDurationMs() / 1000.0));
        sb.append(String.format("Throughput: %.2f txn/sec\n", getThroughput()));
        sb.append(String.format("Average Latency: %.2f ms\n", getAverageLatencyMs()));
        return sb.toString();
    }
}
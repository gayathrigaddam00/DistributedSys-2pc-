package com.example.paxos.wal;

import com.example.paxos.v1.Txn;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Map;

public class WriteAheadLog {
    
    public static class WALEntry {
        public final String txnId;
        public final String key;
        public final int oldValue;
        public final int newValue;
        public final long timestamp;
        public final long seqNum;

        public WALEntry(String txnId, String key, int oldValue, int newValue, long timestamp, long seqNum) {
            this.txnId = txnId;
            this.key = key;
            this.oldValue = oldValue;
            this.newValue = newValue;
            this.timestamp = timestamp;
            this.seqNum = seqNum;
        }

        @Override
        public String toString() {
            return String.format("WAL[txn=%s, key=%s, old=%d, new=%d, seq=%d]", 
                txnId, key, oldValue, newValue, seqNum);
        }
    }

    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<WALEntry>> walByTxn = new ConcurrentHashMap<>();
    
    public void log(String txnId, String key, int oldValue, int newValue, long seqNum) {
        WALEntry entry = new WALEntry(txnId, key, oldValue, newValue, System.currentTimeMillis(), seqNum);
        walByTxn.computeIfAbsent(txnId, k -> new ConcurrentLinkedQueue<>()).add(entry);
    }

    public ConcurrentLinkedQueue<WALEntry> getEntries(String txnId) {
        return walByTxn.get(txnId);
    }

    public void remove(String txnId) {
        walByTxn.remove(txnId);
    }

    public boolean hasEntries(String txnId) {
        return walByTxn.containsKey(txnId) && !walByTxn.get(txnId).isEmpty();
    }

   
    public void clearAll() {
        walByTxn.clear();
    }

    public Map<String, ConcurrentLinkedQueue<WALEntry>> getAllPending() {
        return Map.copyOf(walByTxn);
    }
}
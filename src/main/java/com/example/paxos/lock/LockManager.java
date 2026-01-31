package com.example.paxos.lock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class LockManager {
    private final ConcurrentHashMap<String, LockInfo> locks = new ConcurrentHashMap<>();
    private final ReentrantLock managerLock = new ReentrantLock();

    public static class LockInfo {
        public final String txnId;
        public final long timestamp;
        
        public LockInfo(String txnId, long timestamp) {
            this.txnId = txnId;
            this.timestamp = timestamp;
        }
    }

   
    public boolean tryLock(String key, String txnId) {
        managerLock.lock();
        try {
            if (locks.containsKey(key)) {
                return false;
            }
            locks.put(key, new LockInfo(txnId, System.currentTimeMillis()));
            return true;
        } finally {
            managerLock.unlock();
        }
    }

   
    public boolean tryLockMultiple(String[] keys, String txnId) {
        managerLock.lock();
        try {
            for (String key : keys) {
                if (locks.containsKey(key)) {
                    return false;
                }
            }
            long timestamp = System.currentTimeMillis();
            for (String key : keys) {
                locks.put(key, new LockInfo(txnId, timestamp));
            }
            return true;
        } finally {
            managerLock.unlock();
        }
    }


    public void unlock(String key) {
        managerLock.lock();
        try {
            locks.remove(key);
        } finally {
            managerLock.unlock();
        }
    }


    public void unlockMultiple(String[] keys) {
        managerLock.lock();
        try {
            for (String key : keys) {
                locks.remove(key);
            }
        } finally {
            managerLock.unlock();
        }
    }


    public boolean isLocked(String key) {
        return locks.containsKey(key);
    }

   
    public LockInfo getLockInfo(String key) {
        return locks.get(key);
    }

    
    public void clearAll() {
        managerLock.lock();
        try {
            locks.clear();
        } finally {
            managerLock.unlock();
        }
    }
}
package com.example.paxos.storage;

import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SQLiteStorage {
    private final String url;
    private final String nodeId;
    private int shardStartId = -1;
    private int shardEndId = -1;
    
    private final Set<String> modifiedKeys = ConcurrentHashMap.newKeySet();

    public SQLiteStorage(String nodeId) {
        this.nodeId = nodeId;
        this.url = "jdbc:sqlite:" + nodeId + ".db";
        initDB();
    }

    private void initDB() {
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
                stmt.execute("PRAGMA journal_mode=WAL");  
        stmt.execute("PRAGMA synchronous=NORMAL");
            stmt.execute("CREATE TABLE IF NOT EXISTS kv_store (" +
                         "key TEXT PRIMARY KEY, " +
                         "value INTEGER)");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize SQLite DB for " + nodeId, e);
        }
    }

    public void initShardIfEmpty(int startId, int endId) {
        this.shardStartId = startId;
        this.shardEndId = endId;
        
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT count(*) FROM kv_store")) {
                if (rs.next() && rs.getInt(1) > 0) {
                    System.out.println("Node " + nodeId + ": Data found in disk. Skipping initialization.");
                    return;
                }
            }

            System.out.println("Node " + nodeId + ": Initializing new DB with keys " + startId + "-" + endId);
            modifiedKeys.clear(); 
            
            conn.setAutoCommit(false);
            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO kv_store(key, value) VALUES(?, ?)")) {
                for (int i = startId; i <= endId; i++) {
                    pstmt.setString(1, String.valueOf(i));
                    pstmt.setInt(2, 10);
                    pstmt.addBatch();
                    if (i % 1000 == 0) pstmt.executeBatch();
                }
                pstmt.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to populate shard data", e);
        }
    }
    
    public void resetToInitial() {
        if (shardStartId == -1 || shardEndId == -1) {
            System.err.println("Node " + nodeId + ": Cannot reset - shard range not set");
            return;
        }
        
        modifiedKeys.clear();

        System.out.println("Node " + nodeId + ": Resetting DB to initial values...");
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);
            
            try {
                try (PreparedStatement pstmt = conn.prepareStatement("UPDATE kv_store SET value = 10")) {
                    int updated = pstmt.executeUpdate();
                    System.out.println("Node " + nodeId + ": Reset " + updated + " existing keys to value=10");
                }
                
                Set<String> existingKeys = new HashSet<>();
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT key FROM kv_store")) {
                    while (rs.next()) {
                        existingKeys.add(rs.getString("key"));
                    }
                }
                
                int missingCount = 0;
                try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO kv_store(key, value) VALUES(?, ?) ON CONFLICT(key) DO NOTHING")) {
                    
                    for (int i = shardStartId; i <= shardEndId; i++) {
                        String key = String.valueOf(i);
                        if (!existingKeys.contains(key)) {
                            pstmt.setString(1, key);
                            pstmt.setInt(2, 10);
                            pstmt.addBatch();
                            missingCount++;
                            
                            if (missingCount % 1000 == 0) {
                                pstmt.executeBatch();
                            }
                        }
                    }
                    
                    if (missingCount > 0) {
                        pstmt.executeBatch();
                        System.out.println("Node " + nodeId + ": Restored " + missingCount + " missing keys");
                    }
                }
                
                conn.commit();
                
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM kv_store")) {
                    if (rs.next()) {
                        int finalCount = rs.getInt(1);
                        int expectedCount = shardEndId - shardStartId + 1;
                        System.out.println("Node " + nodeId + ": DB reset complete. Total keys: " + finalCount + 
                                         " (expected: " + expectedCount + ")");
                        
                        if (finalCount != expectedCount) {
                            System.err.println("Node " + nodeId + ": WARNING - Key count mismatch!");
                        }
                    }
                }
                
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
            
        } catch (SQLException e) {
            throw new RuntimeException("Failed to reset DB for " + nodeId, e);
        }
    }

    public int get(String key) {
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement("SELECT value FROM kv_store WHERE key = ?")) {
            pstmt.setString(1, key);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("value");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public void put(String key, int value) {
        modifiedKeys.add(key);
        
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement("INSERT INTO kv_store(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = ?")) {
            pstmt.setString(1, key);
            pstmt.setInt(2, value);
            pstmt.setInt(3, value);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    public boolean containsKey(String key) {
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement("SELECT 1 FROM kv_store WHERE key = ?")) {
            pstmt.setString(1, key);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public Map<String, Integer> getAll() {
        Map<String, Integer> result = new HashMap<>();
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT key, value FROM kv_store")) {
            while (rs.next()) {
                result.put(rs.getString("key"), rs.getInt("value"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public Map<String, Integer> getModified() {
        Map<String, Integer> result = new HashMap<>();
        for (String key : modifiedKeys) {
            int val = get(key);
            result.put(key, val);
        }
        return result;
    }
    public boolean insertFromResharding(String key, int value) {
        System.out.println("[" + nodeId + "] Inserting resharded data: " + key + " = " + value);
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement(
                 "INSERT INTO kv_store(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = ?")) {
            pstmt.setString(1, key);
            pstmt.setInt(2, value);
            pstmt.setInt(3, value);
            int rowsAffected = pstmt.executeUpdate();
            return rowsAffected > 0;
        } catch (SQLException e) {
            System.err.println("[" + nodeId + "] Failed to insert resharded data for key " + key + ": " + e.getMessage());
            return false;
        }
    }
    

    public boolean deleteFromResharding(String key) {
        System.out.println("[" + nodeId + "] Deleting resharded data: " + key);
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement("DELETE FROM kv_store WHERE key = ?")) {
            pstmt.setString(1, key);
            int rowsAffected = pstmt.executeUpdate();
            modifiedKeys.remove(key);
            
            return rowsAffected > 0;
        } catch (SQLException e) {
            System.err.println("[" + nodeId + "] Failed to delete resharded data for key " + key + ": " + e.getMessage());
            return false;
        }
    }
    

    public Integer getForTransfer(String key) {
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement pstmt = conn.prepareStatement("SELECT value FROM kv_store WHERE key = ?")) {
            pstmt.setString(1, key);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("value");
                }
            }
        } catch (SQLException e) {
            System.err.println("[" + nodeId + "] Failed to get data for transfer for key " + key + ": " + e.getMessage());
        }
        return null;
    }
    public void resetToInitial(Map<String, String> currentShardMapping, String myClusterId) {
    if (shardStartId == -1 || shardEndId == -1) {
        System.err.println("Node " + nodeId + ": Cannot reset - shard range not set");
        return;
    }
    
    modifiedKeys.clear();

    System.out.println("Node " + nodeId + ": Resetting DB (respecting shard mapping)...");
    try (Connection conn = DriverManager.getConnection(url)) {
        conn.setAutoCommit(false);
        
        try {
            Set<String> itemsToKeep = new HashSet<>();
            
            if (currentShardMapping != null && myClusterId != null) {
                for (int i = 1; i <= 9000; i++) {
                    String itemId = String.valueOf(i);
                    String cluster = currentShardMapping.getOrDefault(itemId, getDefaultCluster(i));
                    if (cluster.equals(myClusterId)) {
                        itemsToKeep.add(itemId);
                    }
                }
                System.out.println("Node " + nodeId + ": Post-resharding - keeping " + 
                                 itemsToKeep.size() + " items for " + myClusterId);
            } else {
                for (int i = shardStartId; i <= shardEndId; i++) {
                    itemsToKeep.add(String.valueOf(i));
                }
                System.out.println("Node " + nodeId + ": Original partitioning - " + 
                                 itemsToKeep.size() + " items");
            }
            Set<String> existingKeys = new HashSet<>();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT key FROM kv_store")) {
                while (rs.next()) {
                    existingKeys.add(rs.getString("key"));
                }
            }
            
            int deletedCount = 0;
            try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM kv_store WHERE key = ?")) {
                for (String key : existingKeys) {
                    if (!itemsToKeep.contains(key)) {
                        pstmt.setString(1, key);
                        pstmt.addBatch();
                        deletedCount++;
                        if (deletedCount % 1000 == 0) pstmt.executeBatch();
                    }
                }
                if (deletedCount > 0) {
                    pstmt.executeBatch();
                    System.out.println("Node " + nodeId + ": Deleted " + deletedCount + 
                                     " items moved to other clusters");
                }
            }
            
            try (PreparedStatement pstmt = conn.prepareStatement("UPDATE kv_store SET value = 10")) {
                int updated = pstmt.executeUpdate();
                System.out.println("Node " + nodeId + ": Reset " + updated + " keys to value=10");
            }
            
            existingKeys.removeIf(key -> !itemsToKeep.contains(key));
            int insertedCount = 0;
            try (PreparedStatement pstmt = conn.prepareStatement(
                "INSERT INTO kv_store(key, value) VALUES(?, ?) ON CONFLICT(key) DO NOTHING")) {
                
                for (String key : itemsToKeep) {
                    if (!existingKeys.contains(key)) {
                        pstmt.setString(1, key);
                        pstmt.setInt(2, 10);
                        pstmt.addBatch();
                        insertedCount++;
                        if (insertedCount % 1000 == 0) pstmt.executeBatch();
                    }
                }
                if (insertedCount > 0) {
                    pstmt.executeBatch();
                    System.out.println("Node " + nodeId + ": Inserted " + insertedCount + 
                                     " items from other clusters");
                }
            }
            
            conn.commit();
        
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM kv_store")) {
                if (rs.next()) {
                    int finalCount = rs.getInt(1);
                    System.out.println("Node " + nodeId + ": Reset complete. Keys: " + finalCount + 
                                     " (expected: " + itemsToKeep.size() + ")");
                    if (finalCount != itemsToKeep.size()) {
                        System.err.println("Node " + nodeId + ": WARNING - Count mismatch!");
                    }
                }
            }
            
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
        
    } catch (SQLException e) {
        throw new RuntimeException("Failed to reset DB for " + nodeId, e);
    }
}

private String getDefaultCluster(int id) {
    if (id <= 3000) return "C1";
    if (id <= 6000) return "C2";
    return "C3";
}
}
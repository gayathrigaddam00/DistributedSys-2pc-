package com.example.paxos.benchmark;

import com.example.paxos.v1.Txn;
import com.example.paxos.v1.TxnType;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class BenchmarkGenerator {
    private final Random random = new Random();
    
    
    public List<String> generateBenchmark(
            int numTransactions, 
            double readOnlyPercent, 
            double crossShardPercent,
            double skewness) {
        
        List<String> transactions = new ArrayList<>();
        
        for (int i = 0; i < numTransactions; i++) {
            boolean isReadOnly = (random.nextDouble() * 100) < readOnlyPercent;
            
            if (isReadOnly) {
               
                String accountId = selectAccount(skewness);
                transactions.add("-" + accountId);
            } else {
               
                boolean isCrossShard = (random.nextDouble() * 100) < crossShardPercent;
                
                String from = selectAccount(skewness);
                String to;
                
                if (isCrossShard) {
                    to = selectAccountFromDifferentCluster(from, skewness);
                } else {
                    to = selectAccountFromSameCluster(from, skewness);
                }
                
                int amount = random.nextInt(5) + 1; 
                transactions.add(from + "," + to + "," + amount);
            }
        }
        
        return transactions;
    }
    
  
    private String selectAccount(double skewness) {
        if (skewness == 0.0) {
            
            int accountId = random.nextInt(9000) + 1; 
            return String.valueOf(accountId);
        } else {
            
            double rand = random.nextDouble();
           
            if (rand < (0.8 * skewness)) {
               
                int hotAccountRange = 1800;
                int accountId = random.nextInt(hotAccountRange) + 1;
                return String.valueOf(accountId);
            } else {
    
                int accountId = random.nextInt(7200) + 1801;
                return String.valueOf(accountId);
            }
        }
    }
    
   
    private String selectAccountFromDifferentCluster(String from, double skewness) {
        int fromId = Integer.parseInt(from);
        String fromCluster = getClusterForAccount(fromId);
        
        String targetAccount;
        int attempts = 0;
        do {
            targetAccount = selectAccount(skewness);
            int targetId = Integer.parseInt(targetAccount);
            String targetCluster = getClusterForAccount(targetId);
            
            if (!targetCluster.equals(fromCluster)) {
                return targetAccount;
            }
            attempts++;
        } while (attempts < 100); 
        
        
        return forceAccountInDifferentCluster(fromId, skewness);
    }
    
   
    private String selectAccountFromSameCluster(String from, double skewness) {
        int fromId = Integer.parseInt(from);
        String fromCluster = getClusterForAccount(fromId);
        
        String targetAccount;
        int attempts = 0;
        do {
            targetAccount = selectAccount(skewness);
            int targetId = Integer.parseInt(targetAccount);
            String targetCluster = getClusterForAccount(targetId);
            
            if (targetCluster.equals(fromCluster) && !targetAccount.equals(from)) {
                return targetAccount;
            }
            attempts++;
        } while (attempts < 100);
        
        return forceAccountInSameCluster(fromId, from);
    }
    
    private String getClusterForAccount(int accountId) {
        if (accountId <= 3000) return "C1";
        if (accountId <= 6000) return "C2";
        return "C3";
    }
    
    
    private String forceAccountInDifferentCluster(int fromId, double skewness) {
        String fromCluster = getClusterForAccount(fromId);
        List<String> otherClusters = new ArrayList<>();
        if (!fromCluster.equals("C1")) otherClusters.add("C1");
        if (!fromCluster.equals("C2")) otherClusters.add("C2");
        if (!fromCluster.equals("C3")) otherClusters.add("C3");
        
        String targetCluster = otherClusters.get(random.nextInt(otherClusters.size()));
        if (targetCluster.equals("C1")) {
            return String.valueOf(random.nextInt(3000) + 1);
        } else if (targetCluster.equals("C2")) {
            return String.valueOf(random.nextInt(3000) + 3001);
        } else {
            return String.valueOf(random.nextInt(3000) + 6001);
        }
    }
    
    private String forceAccountInSameCluster(int fromId, String from) {
        String cluster = getClusterForAccount(fromId);
        
        int targetId;
        if (cluster.equals("C1")) {
            targetId = random.nextInt(3000) + 1;
        } else if (cluster.equals("C2")) {
            targetId = random.nextInt(3000) + 3001;
        } else {
            targetId = random.nextInt(3000) + 6001;
        }
        
        while (String.valueOf(targetId).equals(from)) {
            if (cluster.equals("C1")) {
                targetId = random.nextInt(3000) + 1;
            } else if (cluster.equals("C2")) {
                targetId = random.nextInt(3000) + 3001;
            } else {
                targetId = random.nextInt(3000) + 6001;
            }
        }
        
        return String.valueOf(targetId);
    }
}
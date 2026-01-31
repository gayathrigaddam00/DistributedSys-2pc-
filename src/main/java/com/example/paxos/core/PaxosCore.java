package com.example.paxos.core;

import com.example.paxos.lock.LockManager;
import com.example.paxos.storage.SQLiteStorage;
import com.example.paxos.v1.*;
import com.example.paxos.wal.WriteAheadLog;
import com.google.protobuf.Empty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class PaxosCore {
    private static final Logger log = LoggerFactory.getLogger(PaxosCore.class);

    public record NewViewResult(NewViewReq req, long maxSeq) {}
    public record ConsensusAttempt(CompletableFuture<Long> future, AcceptReq acceptReq) {}

    private final String nodeId;
    private final Set<String> allNodeIds;
    private final int quorum;
    private final AtomicBoolean isActive = new AtomicBoolean(true);
    private final AtomicBoolean isPaused = new AtomicBoolean(false);
    
    private final String clusterId;

    private Ballot highestSeen = Ballot.newBuilder().setEpoch(0).setLeaderId("").build();
    private Ballot currentLeader = null;

    private final AtomicLong nextSeq = new AtomicLong(1);
    private final ConcurrentMap<Long, AcceptedEntry> acceptLog = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, Value> committed = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, PhaseType> committedPhases = new ConcurrentHashMap<>();
    
    private final ConcurrentMap<Long, Set<String>> acksBySeq = new ConcurrentHashMap<>();
    private final SQLiteStorage storage;
    private final LockManager lockManager;
    private final WriteAheadLog wal;
    
    private final ConcurrentMap<String, ClientReply> lastReplyByClient = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, CompletableFuture<Long>> consensusFutures = new ConcurrentHashMap<>();
    
    public enum Status { X, A, C, E }
    private final ConcurrentMap<Long, ConcurrentMap<String, Status>> statusBySeq = new ConcurrentHashMap<>();
    private final List<NewViewReq> viewHistory = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<Long, PendingClient> pendingClientBySeq = new ConcurrentHashMap<>();
    private final List<String> messageHistoryLog = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<String, Long> preparedCrossShard = new ConcurrentHashMap<>();

    private volatile Map<String, String> shardMapping;

    public static final class Broadcast {
        public final List<Object> messages;
        public Broadcast(List<Object> messages) { this.messages = messages; }
    }

    public static final class ReplyAction {
        public final ClientReply reply;
        public ReplyAction(ClientReply reply) { this.reply = reply; }
    }

    private record PendingClient(String clientId, long clientTs) {}

    public PaxosCore(String nodeId, Set<String> allNodeIds, LockManager lockManager, 
                     WriteAheadLog wal, Map<String, Integer> initialBalances, String clusterId) {
        this.nodeId = Objects.requireNonNull(nodeId);
        this.allNodeIds = new HashSet<>(Objects.requireNonNull(allNodeIds));
        this.clusterId = clusterId;
        this.lockManager = lockManager;
        this.wal = wal;
        this.storage = new SQLiteStorage(nodeId);
        
        if (!this.allNodeIds.contains(nodeId)) 
            throw new IllegalArgumentException("allNodeIds must include self");
        this.quorum = (this.allNodeIds.size() / 2) + 1;

        initializeDefaultMapping();
        initializeShardData();
    }
    
    private void initializeDefaultMapping() {
        this.shardMapping = new ConcurrentHashMap<>();
        for (int i = 1; i <= 9000; i++) {
            if (i <= 3000) {
                shardMapping.put(String.valueOf(i), "C1");
            } else if (i <= 6000) {
                shardMapping.put(String.valueOf(i), "C2");
            } else {
                shardMapping.put(String.valueOf(i), "C3");
            }
        }
        log.info("[{}] Initialized default shard mapping", nodeId);
    }
    
    public String getItemCluster(String itemId) {
        return shardMapping.getOrDefault(itemId, getDefaultCluster(itemId));
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
    
    public synchronized void updateShardMapping(Map<String, String> newMapping) {
        log.info("[{}] Updating shard mapping with {} entries", nodeId, newMapping.size());
        this.shardMapping = new ConcurrentHashMap<>(newMapping);
        log.info("[{}] Shard mapping updated successfully", nodeId);
    }
    
    private void initializeShardData() {
        int startId = 0, endId = 0;
        if ("C1".equals(clusterId)) { startId = 1; endId = 3000; } 
        else if ("C2".equals(clusterId)) { startId = 3001; endId = 6000; } 
        else if ("C3".equals(clusterId)) { startId = 6001; endId = 9000; } 
        else { log.warn("Unknown clusterId: {}", clusterId); return; }
        storage.initShardIfEmpty(startId, endId);
    }
    
    public void setActive(boolean status) { this.isActive.set(status); }
    public boolean isActive() { return this.isActive.get(); }
    

    public void setPaused(boolean paused) { 
        this.isPaused.set(paused);
        log.info("[{}] Node {} for resharding", nodeId, paused ? "PAUSED" : "RESUMED");
    }
    
    public boolean isPaused() { 
        return this.isPaused.get(); 
    }
    
public synchronized void flushState() {
    log.info("[{}] Flushing all state...", nodeId);
    
   
    highestSeen = Ballot.newBuilder().setEpoch(0).setLeaderId("").build();
    currentLeader = null;
    nextSeq.set(1);
    acceptLog.clear();
    committed.clear();
    committedPhases.clear();
    acksBySeq.clear();
    lastReplyByClient.clear();
    consensusFutures.clear();
    statusBySeq.clear();
    viewHistory.clear();
    pendingClientBySeq.clear();
    messageHistoryLog.clear();
    
   
    lockManager.clearAll();
    wal.clearAll();
    
   
    preparedCrossShard.clear();
    
   
    isPaused.set(false);
    
   
    if (shardMapping == null || shardMapping.isEmpty()) {
        log.info("[{}] First startup - initializing default shard mapping", nodeId);
        initializeDefaultMapping();
        storage.resetToInitial(null, null);
    } else {
        log.info("[{}] Preserving shard mapping with {} entries", nodeId, shardMapping.size());
        storage.resetToInitial(shardMapping, clusterId);
    }
    
    log.info("[{}] State flush complete", nodeId);
}

    public ConsensusAttempt runConsensus(Txn txn, PhaseType phase) {
        if (!isActive()) {
            CompletableFuture<Long> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("Node inactive"));
            return new ConsensusAttempt(future, null);
        }
        if (!isLeader()) {
            CompletableFuture<Long> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("Not leader"));
            return new ConsensusAttempt(future, null);
        }

        long s = nextSeq.getAndIncrement();
        Value value = Value.newBuilder().setTxn(txn).build();
        
        AcceptedEntry accepted = AcceptedEntry.newBuilder()
                .setB(currentLeader)
                .setS(Seq.newBuilder().setNum(s))
                .setValue(value)
                .setPhase(phase)
                .build();
        
        acceptLog.put(s, accepted);
        committedPhases.put(s, phase);
        markStatus(s, nodeId, Status.A);

        CompletableFuture<Long> consensusFuture = new CompletableFuture<>();
        consensusFutures.put(s, consensusFuture);

        AcceptReq acceptReq = AcceptReq.newBuilder()
                .setB(currentLeader)
                .setS(Seq.newBuilder().setNum(s))
                .setValue(value)
                .setPhase(phase)
                .build();
        
        acksBySeq.computeIfAbsent(s, k -> new HashSet<>()).add(nodeId);
        log.info("[{}] Running consensus seq={}, phase={}", nodeId, s, phase);
        
        return new ConsensusAttempt(consensusFuture, acceptReq);
    }

public synchronized Object onClientSubmit(Txn txn) {
    if (isPaused()) {
        return new ReplyAction(ClientReply.newBuilder().setB(nullSafeBallot(currentLeader))
                .setClientId(txn.getClientId()).setClientTs(txn.getClientTs())
                .setSuccess(false).setMessage("NODE_PAUSED_FOR_RESHARDING").build());
    }
    
    if (!isActive()) {
        return new ReplyAction(ClientReply.newBuilder().setB(nullSafeBallot(currentLeader))
                .setClientId(txn.getClientId()).setClientTs(txn.getClientTs())
                .setSuccess(false).setMessage("NODE_INACTIVE").build());
    }
    
    String clientId = txn.getClientId();
    long clientTs = txn.getClientTs();
    ClientReply last = lastReplyByClient.get(clientId);
    if (last != null && last.getClientTs() >= clientTs) {
        return new ReplyAction(last);
    }
    
    if (!isLeader()) {
        return new ReplyAction(ClientReply.newBuilder().setB(nullSafeBallot(currentLeader))
                .setClientId(clientId).setClientTs(clientTs)
                .setSuccess(false).setMessage("NOT_LEADER").build());
    }

    if (txn.getType() == TxnType.READ_ONLY) {
        String accountId = txn.getFrom();
        
        if (lockManager.isLocked(accountId)) {
    log.info("[{}] Skipping READ_ONLY for {} - item locked (no reply sent)", nodeId, accountId);
    return null; 
}
        if (!storage.containsKey(accountId)) {
            return new ReplyAction(ClientReply.newBuilder()
                    .setB(nullSafeBallot(currentLeader))
                    .setClientId(clientId)
                    .setClientTs(clientTs)
                    .setSuccess(false)
                    .setMessage("ACCOUNT_NOT_FOUND")
                    .build());
        }
        
        int balance = storage.get(accountId);
        ClientReply reply = ClientReply.newBuilder()
                .setB(currentLeader)
                .setClientId(clientId)
                .setClientTs(clientTs)
                .setSuccess(true)
                .setMessage("Balance: " + balance)
                .build();
        
        lastReplyByClient.put(clientId, reply);
        
        log.debug("[{}] READ_ONLY transaction for {} - balance: {}", nodeId, accountId, balance);
        return new ReplyAction(reply);
    }


    if (txn.getType() == TxnType.INTRA_SHARD) {
        String sKey = txn.getFrom();
        String rKey = txn.getTo();
        String txnId = getTxnId(txn);
        String senderCluster = getItemCluster(sKey);
    String receiverCluster = getItemCluster(rKey);
    
    if (!senderCluster.equals(clusterId)) {
        return new ReplyAction(ClientReply.newBuilder()
            .setB(nullSafeBallot(currentLeader))
            .setClientId(txn.getClientId())
            .setClientTs(txn.getClientTs())
            .setSuccess(false)
            .setMessage("WRONG_SHARD:" + senderCluster)
            .build());
    }
    
    if (!receiverCluster.equals(clusterId)) {
        return new ReplyAction(ClientReply.newBuilder()
            .setB(nullSafeBallot(currentLeader))
            .setClientId(txn.getClientId())
            .setClientTs(txn.getClientTs())
            .setSuccess(false)
            .setMessage("WRONG_SHARD:" + receiverCluster)
            .build());
    }

        if (lockManager.isLocked(sKey) || lockManager.isLocked(rKey)) {
            log.info("[{}] Skipping locked transaction {} (no reply sent)", nodeId, txnId);
            return null; 
        }
        
        if (storage.containsKey(sKey)) {
            int balance = storage.get(sKey);
            if (balance < txn.getAmount()) {
                log.warn("[{}] Insufficient balance for txn {}: {} < {}", 
                    nodeId, txnId, balance, txn.getAmount());
                return new ReplyAction(ClientReply.newBuilder()
                    .setB(nullSafeBallot(currentLeader))
                    .setClientId(txn.getClientId())
                    .setClientTs(txn.getClientTs())
                    .setSuccess(false)
                    .setMessage("INSUFFICIENT_BALANCE")
                    .build());
            }
        }
        

        if (!lockManager.tryLockMultiple(new String[]{sKey, rKey}, txnId)) {
            log.info("[{}] Skipping transaction {} - lock acquisition failed (no reply sent)", nodeId, txnId);
            return null;  
        }
    }
    
    long s = nextSeq.getAndIncrement();
    Value value = Value.newBuilder().setTxn(txn).build();
    AcceptedEntry accepted = AcceptedEntry.newBuilder()
            .setB(currentLeader).setS(Seq.newBuilder().setNum(s))
            .setValue(value).setPhase(PhaseType.NORMAL).build();
    
    acceptLog.put(s, accepted);
    committedPhases.put(s, PhaseType.NORMAL);
    markStatus(s, nodeId, Status.A);
    pendingClientBySeq.put(s, new PendingClient(clientId, clientTs));
    
    AcceptReq acceptReq = AcceptReq.newBuilder()
            .setB(currentLeader).setS(Seq.newBuilder().setNum(s))
            .setValue(value).setPhase(PhaseType.NORMAL).build();
    acksBySeq.computeIfAbsent(s, k -> new HashSet<>()).add(nodeId);
    return new Broadcast(List.of(acceptReq));
}   
    public synchronized PromiseRes onPrepare(PrepareReq req) {
        if (!isActive()) return PromiseRes.getDefaultInstance();
        Ballot incoming = req.getB();
        if (isHigher(incoming, highestSeen)) { highestSeen = incoming; }
        List<AcceptedEntry> log = new ArrayList<>(acceptLog.values());
        return PromiseRes.newBuilder().setB(highestSeen).addAllAcceptLog(log).build();
    }

    public synchronized Object onAccept(AcceptReq req) {
        if (!isActive()) return Empty.getDefaultInstance();
        Ballot b = req.getB();
        long seqNum = req.getS().getNum();
        PhaseType phase = req.getPhase();
        
        Map<String, Status> statusMap = statusBySeq.get(seqNum);
        if (statusMap != null) {
            Status currentStatus = statusMap.get(nodeId);
            if (currentStatus == Status.C || currentStatus == Status.E) {
                return AcceptedReq.newBuilder().setB(b).setS(req.getS()).setFromNodeId(nodeId).setPhase(phase).build();
            }
        }
        if (isHigher(highestSeen, b)) return Empty.getDefaultInstance();
        if (isHigher(b, currentLeader)) currentLeader = b;
        highestSeen = b;
        
        AcceptedEntry entry = AcceptedEntry.newBuilder().setB(b).setS(req.getS()).setValue(req.getValue()).setPhase(phase).build();
        acceptLog.put(seqNum, entry);
        committedPhases.put(seqNum, phase);
        markStatus(seqNum, nodeId, Status.A);
        return AcceptedReq.newBuilder().setB(b).setS(req.getS()).setFromNodeId(nodeId).setPhase(phase).build();
    }

    public synchronized Object onAccepted(AcceptedReq req) {
        if (!isActive()) return Empty.getDefaultInstance();
        long s = req.getS().getNum();
        Ballot b = req.getB();
        PhaseType phase = req.getPhase();
        
        if (currentLeader == null || !currentLeader.equals(b)) return Empty.getDefaultInstance();
        
        Set<String> acks = acksBySeq.computeIfAbsent(s, k -> new HashSet<>());
        acks.add(req.getFromNodeId());
        
        if (acks.size() >= quorum && !committed.containsKey(s)) {
            AcceptedEntry accepted = acceptLog.get(s);
            if (accepted == null) return Empty.getDefaultInstance();
            
            CommitReq commit = CommitReq.newBuilder().setB(b).setS(accepted.getS()).setValue(accepted.getValue()).setPhase(phase).build();
            onCommit(commit); 
            
            CompletableFuture<Long> future = consensusFutures.remove(s);
            if (future != null && !future.isDone()) future.complete(s);
            
            return new Broadcast(List.of(commit));
        }
        return Empty.getDefaultInstance();
    }

    public synchronized void onCommit(CommitReq req) {
        if (!isActive()) return;
        if (isHigher(req.getB(), currentLeader)) currentLeader = req.getB();
        
        long seqNum = req.getS().getNum();
        committed.putIfAbsent(seqNum, req.getValue());
        committedPhases.putIfAbsent(seqNum, req.getPhase());
        markStatus(seqNum, nodeId, Status.C);
        
        executeImmediately(seqNum);
    }
    
    public synchronized void onNewView(NewViewReq req) {
        if (!isActive()) return;
        if(isHigher(req.getB(), currentLeader)) currentLeader = req.getB();
        viewHistory.add(req);
        
        for (AcceptReq a : req.getRebroadcastBatchList()) {
            long seqNum = a.getS().getNum();
            AcceptedEntry entry = AcceptedEntry.newBuilder().setB(a.getB()).setS(a.getS()).setValue(a.getValue()).setPhase(a.getPhase()).build();
            acceptLog.put(seqNum, entry);
            committedPhases.put(seqNum, a.getPhase());
            markStatus(seqNum, nodeId, Status.A);
            
            if (!committed.containsKey(seqNum)) {
                committed.put(seqNum, a.getValue());
                committedPhases.put(seqNum, a.getPhase());
                markStatus(seqNum, nodeId, Status.C);
                executeImmediately(seqNum);
            }
        }
    }

    private void executeImmediately(long seqNum) {
        if (!committed.containsKey(seqNum)) return;
        
        Map<String, Status> statuses = statusBySeq.get(seqNum);
        if (statuses != null && statuses.get(nodeId) == Status.E) return;

        PhaseType phase = committedPhases.getOrDefault(seqNum, PhaseType.NORMAL);
        Value val = committed.get(seqNum);
        
        apply(val, phase, seqNum);
        markStatus(seqNum, nodeId, Status.E);

        PendingClient pc = pendingClientBySeq.remove(seqNum);
        if (pc != null && isLeader()) {
            ClientReply r = ClientReply.newBuilder().setB(currentLeader)
                .setClientId(pc.clientId()).setClientTs(pc.clientTs())
                .setSuccess(true).setMessage("COMMITTED")
                .setSeq(Seq.newBuilder().setNum(seqNum)).build();
            lastReplyByClient.put(pc.clientId(), r);
        }
    }

    private void apply(Value v, PhaseType phase, long seqNum) {
        if (!v.hasTxn()) return;
        Txn t = v.getTxn();
        String txnId = getTxnId(t);
        log.debug("[{}] Applying txn {} phase={} seq={}", nodeId, txnId, phase, seqNum);

        if (phase == PhaseType.PREPARE) {
            if (storage.containsKey(t.getFrom())) {
                int fromBal = storage.get(t.getFrom());
                int newFromBal = fromBal - t.getAmount();
                wal.log(txnId, t.getFrom(), fromBal, newFromBal, seqNum);
                storage.put(t.getFrom(), newFromBal);
            }
            
            if (storage.containsKey(t.getTo())) {
                int toBal = storage.get(t.getTo());
                int newToBal = toBal + t.getAmount();
                wal.log(txnId, t.getTo(), toBal, newToBal, seqNum);
                storage.put(t.getTo(), newToBal);
            }
        } 
        else if (phase == PhaseType.COMMIT) {
            safeUnlock(t.getFrom(), txnId);
            safeUnlock(t.getTo(), txnId);
            wal.remove(txnId);
        } 
        else if (phase == PhaseType.ABORT) {
            var entries = wal.getEntries(txnId);
            if (entries != null) {
                for (var entry : entries) {
                    storage.put(entry.key, entry.oldValue);
                }
            }
            safeUnlock(t.getFrom(), txnId);
            safeUnlock(t.getTo(), txnId);
            wal.remove(txnId);
        } 
        else if (phase == PhaseType.NORMAL) {
        boolean senderDebited = false;
        
        if (storage.containsKey(t.getFrom())) {
            int fromBal = storage.get(t.getFrom());
            if (fromBal >= t.getAmount()) {
                storage.put(t.getFrom(), fromBal - t.getAmount());
                senderDebited = true;
                log.debug("[{}] Debited {} by {}", nodeId, t.getFrom(), t.getAmount());
            } else {
                log.warn("[{}] Insufficient balance during apply for {}: {} < {}", 
                    nodeId, t.getFrom(), fromBal, t.getAmount());
            }
        }
        
        if (senderDebited && storage.containsKey(t.getTo())) {
            int toBal = storage.get(t.getTo());
            storage.put(t.getTo(), toBal + t.getAmount());
            log.debug("[{}] Credited {} by {}", nodeId, t.getTo(), t.getAmount());
        }

        if (t.getType() == TxnType.INTRA_SHARD) {
            safeUnlock(t.getFrom(), txnId);
            safeUnlock(t.getTo(), txnId);
        }
    }
    }
    
    private void safeUnlock(String key, String txnId) {
        if (storage.containsKey(key)) {
            var lockInfo = lockManager.getLockInfo(key);
            if (lockInfo != null && lockInfo.txnId.equals(txnId)) {
                lockManager.unlock(key);
            }
        }
    }

    public void markCrossShardPrepared(String txnId, long prepareSeq) {
        preparedCrossShard.put(txnId, prepareSeq);
        log.info("[{}] Marked cross-shard txn {} as prepared (seq={})", nodeId, txnId, prepareSeq);
    }


    public void markCrossShardCommitted(String txnId) {
        preparedCrossShard.remove(txnId);
        log.info("[{}] Removed cross-shard txn {} from prepared tracking", nodeId, txnId);
    }

    
    public Map<String, Long> getPreparedCrossShardTxns() {
        return Map.copyOf(preparedCrossShard);
    }

    public Map<String, Integer> printDB() { return storage.getModified(); }
    public List<String> printLog() { return List.copyOf(messageHistoryLog); }
    public Map<String, Status> printStatus(long seq) { 
        ConcurrentMap<String, Status> statusMap = statusBySeq.get(seq); 
        return statusMap != null ? Map.copyOf(statusMap) : Map.of(); 
    }
    public List<NewViewReq> printView() { return List.copyOf(viewHistory); }
    public ClientReply getCachedReply(String clientId) { return lastReplyByClient.get(clientId); }
    public Ballot getCurrentLeader() { return currentLeader; }
    public Ballot getHighestSeen() { return highestSeen; }
    public SQLiteStorage getStorage() { return storage; }
    public LockManager getLockManager() { return lockManager; }
    
    public synchronized PrepareReq buildPrepare(long newEpoch) { 
        Ballot b = Ballot.newBuilder().setEpoch(newEpoch).setLeaderId(nodeId).build(); 
        return PrepareReq.newBuilder().setB(b).build(); 
    }
    
    public synchronized NewViewResult buildNewViewFromPromises(Ballot b, List<PromiseRes> promises) {
        Map<Long, AcceptedEntry> highestBySeq = new HashMap<>();
        long maxSeq = 0;
        for (PromiseRes p : promises) {
            for (AcceptedEntry e : p.getAcceptLogList()) {
                long s = e.getS().getNum();
                maxSeq = Math.max(maxSeq, s);
                AcceptedEntry prev = highestBySeq.get(s);
                if (prev == null || isHigher(e.getB(), prev.getB())) { highestBySeq.put(s, e); }
            }
        }
        List<AcceptReq> batch = new ArrayList<>();
        for (long s = 1; s <= maxSeq; s++) {
            AcceptedEntry e = highestBySeq.get(s);
            Value v = (e != null) ? e.getValue() : Value.newBuilder().setNoop(Noop.getDefaultInstance()).build();
            PhaseType phase = (e != null) ? e.getPhase() : PhaseType.NORMAL;
            batch.add(AcceptReq.newBuilder().setB(b).setS(Seq.newBuilder().setNum(s)).setValue(v).setPhase(phase).build());
        }
        NewViewReq req = NewViewReq.newBuilder().setB(b).addAllRebroadcastBatch(batch).build();
        return new NewViewResult(req, maxSeq);
    }
    
    public synchronized void updateSeqAfterElection(long maxSeqSeen) {
        long currentNextSeq = this.nextSeq.get();
        if (maxSeqSeen + 1 > currentNextSeq) { this.nextSeq.set(maxSeqSeen + 1); }
    }

    public long getNextSeq() {
    return nextSeq.get();
}

public PhaseType getPhaseForSeq(long seq) {
    return committedPhases.get(seq);
}

public Txn getTxnForSeq(long seq) {
    Value val = committed.get(seq);
    if (val != null && val.hasTxn()) {
        return val.getTxn();
    }
    return null;
}
    
    public synchronized void becomeLeader(Ballot b) { this.currentLeader = b; this.highestSeen = b; }
    private void markStatus(long seq, String node, Status st) { statusBySeq.computeIfAbsent(seq, k -> new ConcurrentHashMap<>()).put(node, st); }
    private static boolean isHigher(Ballot a, Ballot b) { if (a == null || b == null) return a != null; if (a.getEpoch() != b.getEpoch()) return a.getEpoch() > b.getEpoch(); return a.getLeaderId().compareTo(b.getLeaderId()) > 0; }
    private static Ballot nullSafeBallot(Ballot b) { return b == null ? Ballot.newBuilder().build() : b; }
    public boolean isLeader() { return currentLeader != null && nodeId.equals(currentLeader.getLeaderId()); }
    public boolean hasLeader() { return this.currentLeader != null && !this.currentLeader.getLeaderId().isEmpty(); }
    private void logMessage(String direction, Object message) { String logEntry = String.format("[%s] %s: %s", direction, message.getClass().getSimpleName(), message.toString().replaceAll("\\s+", " ")); messageHistoryLog.add(logEntry); }
    public void logReceived(Object message) { logMessage("RECV", message); }
    public void logSent(Object message) { logMessage("SENT", message); }
    private String getTxnId(Txn txn) { return txn.getClientId() + "-" + txn.getClientTs(); }
}
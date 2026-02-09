
# Distributed Banking System (2PC, Paxos & Dynamic Resharding)

A high-performance, fault-tolerant distributed transaction processing system designed for scalability. This system implements **Two-Phase Commit (2PC)** for cross-shard atomicity, **Multi-Paxos** for intra-shard consensus, and a novel **Dynamic Resharding** engine that uses Hypergraph Partitioning to minimize cross-shard transaction overhead in real-time.

[![Spec](https://img.shields.io/badge/Project-Specification-red?logo=adobeacrobatreader&logoColor=white)](CSE535-F25-Project-3.pdf)

---

## 🚀 Key Features

### 1. Distributed Consensus & Atomicity
* **Multi-Paxos:** Implements a stable-leader Paxos protocol within each shard to ensure strongly consistent replication across 3 replicas per shard.
* **Two-Phase Commit (2PC):** Guarantees atomicity for transactions spanning multiple shards (Clusters). Implements failure recovery for both Coordinators and Participants.
* **Persistent Storage:** Uses **SQLite** (WAL mode) for durable storage of account balances and transaction logs on every node.

### 2. Intelligent Dynamic Resharding
The system monitors transaction history and dynamically rebalances data to reduce network overhead.
* **Hypergraph Modeling:** Models transactions as hyperedges connecting account nodes.
* **Simulated Annealing:** Uses a physics-based optimization algorithm (Simulated Annealing) to partition the hypergraph, minimizing the "cut cost" (cross-shard transactions).
* **Zero-Downtime Migration:** The `ReshardingCoordinator` orchestrates a 4-phase safe migration protocol (Pause → Move Data → Update Maps → Resume) to apply the new partition.

### 3. Advanced Benchmarking
* **Workload Generator:** Simulates realistic traffic with tunable parameters for **Zipfian Skewness** (hotspots), read/write ratios, and cross-shard probabilities.

---

## 🛠️ System Architecture

The system is partitioned into **3 Clusters** (C1, C2, C3), each containing **3 Replica Nodes** (Total 9 Nodes).

### Transaction Processing Pipeline
1.  **Request Routing:** Clients route requests to the leader of the relevant cluster.
2.  **Intra-Shard Fast Path:** If Sender and Receiver are in the same cluster, the transaction is committed via a single Paxos round (latency ~5ms).
3.  **Cross-Shard Slow Path (2PC):**
    * **Phase 1 (Prepare):** Coordinator locks local records and sends `PREPARE` to remote clusters.
    * **Phase 2 (Commit):** If all clusters vote YES, the Coordinator broadcasts `COMMIT`.

### The Resharding Engine
Periodically, the **Resharding Manager** analyzes the last $N$ transactions:
1.  **Build Hypergraph:** Accounts are vertices; transactions are edges.
2.  **Optimize Partition:** Runs Simulated Annealing to find a mapping that minimizes edges crossing cluster boundaries.
3.  **Load Balancing:** Ensures no cluster is overloaded (moves accounts from largest to smallest cluster if imbalance > tolerance).
4.  **Execute Plan:** Moves specific account data (Key-Values) between SQLite databases of different clusters.

---

## 💻 Tech Stack

* **Language:** Java 21
* **Framework:** Spring Boot 3.3.3
* **Communication:** gRPC & Protobuf
* **Storage:** SQLite (JDBC)
* **Algorithms:** Multi-Paxos, 2PC, Simulated Annealing (Optimization)

---

## 🚀 Getting Started

### Prerequisites
* Java 21+
* Maven

### Installation
1.  **Clone and Build:**
    ```bash
    git clone [https://github.com/gayathrigaddam00/DistributedSys-2pc.git](https://github.com/gayathrigaddam00/DistributedSys-2pc.git)
    cd DistributedSys-2pc
    ./mvnw clean install
    ```

2.  **Initialize Databases:**
    The system automatically creates and initializes `n1.db` through `n9.db` upon first startup.

### Running the Cluster
You need to start 9 terminal instances for the 9 nodes (n1-n9).

```bash
# Terminal 1 (Cluster 1 Leader)
java -jar target/project3.jar --spring.profiles.active=n1

# Terminal 2-3 (Cluster 1 Followers)
java -jar target/project3.jar --spring.profiles.active=n2
java -jar target/project3.jar --spring.profiles.active=n3

# ... Repeat for n4-n6 (Cluster 2) and n7-n9 (Cluster 3) ...

```

---
## 🧠 System Functionality & Design

### 1. Clustered Architecture & Data Sharding
The system is composed of **9 Nodes** organized into **3 Clusters (Shards)**.
* **Static Sharding (Initial):**
    * **Cluster 1 (n1, n2, n3):** Accounts 1 - 3000
    * **Cluster 2 (n4, n5, n6):** Accounts 3001 - 6000
    * **Cluster 3 (n7, n8, n9):** Accounts 6001 - 9000
* **Replication:** Within each cluster, data is replicated across 3 nodes using **Multi-Paxos**.
* **Persistence:** Each node maintains its own local **SQLite** database (WAL mode enabled) to ensure data durability across restarts.

### 2. Transaction Processing Engine
The system intelligently routes transactions based on the data they access:

#### A. Intra-Shard Transactions (Fast Path)
* **Definition:** Transactions where both Sender and Receiver reside in the same cluster.
* **Protocol:** **Multi-Paxos**.
* **Flow:** The request is sent to the cluster leader, sequenced, and replicated. Once a majority accepts, it is committed locally. No 2PC overhead is incurred.

#### B. Cross-Shard Transactions (Slow Path)
* **Definition:** Transactions involving accounts in different clusters (e.g., `C1` to `C3`).
* **Protocol:** **Two-Phase Commit (2PC)**.
* **Coordinator:** The leader of the sender's cluster acts as the 2PC Coordinator.
* **Phase 1 (Prepare):**
    * Coordinator sends `PREPARE` to the participant cluster.
    * Participant acquires **Row-Level Locks** via `LockManager` to isolate the account.
    * Transaction is logged to the **Write-Ahead Log (WAL)**.
* **Phase 2 (Commit/Abort):**
    * If the participant votes YES, the Coordinator broadcasts `COMMIT`.
    * Locks are released only after the transaction is finalized.

### 3. Dynamic Resharding Engine (Advanced)
To optimize performance, the system implements a novel **Hypergraph-based Resharding** mechanism that moves data closer to where it is needed.

#### The Algorithm
1.  **Workload Monitoring:** The `ReshardingManager` records a window of recent transactions.
2.  **Hypergraph Construction:** Transactions are modeled as a **Hypergraph**, where vertices are accounts and edges are transactions connecting them.
3.  **Partitioning (Simulated Annealing):** The system runs a **Simulated Annealing** algorithm to find a partition that minimizes the "Cut Cost" (number of cross-shard edges) while maintaining balanced shard sizes.

#### Migration Protocol
Once a better partition is found, the `ReshardingCoordinator` executes a safe migration:
1.  **Pause:** All nodes temporarily pause transaction processing.
2.  **Move Data:** Account data (Key-Value pairs) is physically transferred between clusters via gRPC.
3.  **Update Routing:** The global shard mapping is updated on all nodes.
4.  **Resume:** System resumes with the new, optimized layout.

## 🧪 Testing & Benchmarking

### 1. Run Standard Test Cases

Executes the predefined transaction sets (normal flow + failure recovery).

```bash
java -cp target/project3.jar com.example.paxos.client.TestRunner -f tests/CSE535-F25-Project-3-Testcases.csv

```

### 2. Run Resharding Demo

This scenario demonstrates the system adapting to changing access patterns.

1. **Generate Workload:** Creates a skewed workload where accounts 1-100 interact frequently with accounts 3001-3100 (forcing cross-shard traffic).
2. **Trigger Resharding:** The system detects the high cross-shard cost.
3. **Observation:** You will see logs indicating "Moving X items from C1 to C2".
4. **Verification:** Subsequent transactions between these groups become Intra-Shard (fast path).

```bash
# Run the benchmark tool
java -cp target/project3.jar com.example.paxos.benchmark.BenchmarkRunner --txns 1000 --skew 0.8 --cross-shard 50

```

---

## 📜 Configuration

### Cluster Mapping (Initial)

| Cluster | Node IDs | Account Range |
| --- | --- | --- |
| **C1** | n1, n2, n3 | 1 - 3000 |
| **C2** | n4, n5, n6 | 3001 - 6000 |
| **C3** | n7, n8, n9 | 6001 - 9000 |

*Note: After resharding, account ranges become dynamic and are stored in the `currentMapping` map within `ReshardingManager`.*

---

## 🔗 References

* **Paxos Made Simple** - Leslie Lamport
* **Gray & Cheriton** - "Consensus on Transaction Commit"
* **Hypergraph Partitioning** - Karypis et al.

```

```

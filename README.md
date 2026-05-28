# Adaptive AI OS (Distributed State Machine Kernel Prototype)

An experimental prototype of a partition-tolerant distributed state machine operating system control plane built on a customized Raft-like consensus protocol in Python. The system demonstrates how a consensus engine can integrate with a Bayesian belief state observer (POMDP-lite) to dynamically adapt network parameters, retry schedules, and leader reelection timeouts under varying network conditions.

---

## Capabilities and Features

1. **Consensus Core (Raft-like Prototype)**: Educational implementation of consensus rules including leader election, randomized timeouts, log replication, term validation, and quorum commits over asynchronous TCP connections.
2. **State Recovery & Compaction**: Local Write-Ahead Log (WAL) persistence and snapshotted state checkpoints. Demonstrates historical log truncation and recovery of state from disk snapshots.
3. **Pure State Reduction**: Event-sourced state machine where the current node state is reconstructed by replaying committed log entries to a state reducer.
4. **Bayesian Belief Engine (POMDP-lite)**: Monitors RPC successes and latencies to maintain a probability distribution over peer health states (`HEALTHY`, `SLOW`, `DEAD`).
5. **Adaptive Network Jitter & Backoffs**:
   - Follower reelection timeouts dynamically shrink if the current leader is suspected to have failed (speeding up failovers).
   - Leader heartbeat/retry intervals dynamically expand when sending to suspected slow or dead nodes to prevent network congestion.
   - Vote decisions verify candidate trust scores to deprioritize unstable nodes.
6. **HTTP REST API Server**: Exposes endpoints on each node to query current consensus terms, log metadata, belief matrices, and to propose state mutations.

---

## Project Structure

The codebase is organized as follows:

```text
cognitive_shell/
│
├── cognitive/
│   ├── belief_engine/
│   │   └── engine.py         # Bayesian belief updater (POMDP-lite)
│   ├── observation_layer/
│   │   └── observer.py       # Maps latency and success to observations
│   ├── cognitive.py          # Unified AI belief coordinator
│   ├── controller.py         # Updates desired states based on specifications
│   ├── evaluator.py          # Assesses priority levels
│   ├── gemini_client.py      # Stub client structure for LLM interaction
│   └── planner.py            # Generates execution plan steps
│
├── core/
│   ├── log/
│   │   └── model.py          # Raft log entries and compaction model
│   ├── raft/
│   │   └── node.py           # Core Raft consensus protocol rules
│   ├── state_machine/
│   │   └── reducer.py        # Event-sourced state machine transitions
│   └── transport/
│       ├── client.py         # Async TCP connection pool & client sockets
│       └── server.py         # Async TCP line-oriented network server
│
├── node/
│   ├── runtime/
│   │   ├── executor.py       # Subprocess task runner (executes 'sh' commands)
│   │   ├── loop.py           # Asynchronous orchestrator control loops
│   │   ├── scheduler.py      # Reconciler task scheduler
│   │   └── telemetry.py      # structured logger and telemetry recorder
│   └── server/
│       ├── __main__.py       # CLI launcher exposing HTTP API
│       └── config.py         # Typed PyYAML validation and schemas
│
├── storage/
│   ├── snapshots/
│   │   └── store.py          # Checkpoint snapshots directory serializer
│   └── wal/
│       └── store.py          # Write-Ahead Log state writer
│
├── tests/
│   └── integration/
│       └── test_cluster.py   # Complete cluster integration test suite
│
├── config.yaml               # Cluster node topology configuration
├── peers.json                # Legacy node mapping wrapper configuration
├── run.py                    # 3-node cluster simulation script
└── venv/                     # Python virtual environment directory
```

---

## REST API Interface

API HTTP servers are launched alongside each node (API Port = Raft Node Port + 1000). For example, Node 1 on port 5001 hosts its API on port `6001`.

### 1. Query State
- **Endpoint**: `GET /state`
- **Response Payload**:
  ```json
  {
    "node_id": "1",
    "state": "leader",
    "current_term": 1,
    "commit_index": 12,
    "last_applied": 12,
    "actual_state": {
      "desired_state": { "tasks": [...] },
      "task-1": "done",
      "task-2": "running"
    },
    "log_length": 6,
    "beliefs": {
      "2": [0.99, 0.009, 0.001]
    },
    "suspicion_scores": {
      "2": 0.005
    }
  }
  ```

### 2. Propose Mutation Command
- **Endpoint**: `POST /propose`
- **Request Payload**:
  ```json
  {
    "type": "DESIRED_STATE_UPDATE",
    "payload": {
      "tasks": [{"id": "new-task", "target": "sh", "command": "echo 'Hello'"}]
    }
  }
  ```

---

## Running the Simulation

Execute the local simulation to run a 3-node cluster, propose client transactions, terminate the leader, observe reelection, recover the dead node, and verify cluster state alignment:

```powershell
.\venv\Scripts\python run.py
```

---

## Running Integration Tests

Run the integration test suite covering election convergence, reboot recoveries, network partitions, and dynamic belief engine timeout transitions:

```powershell
.\venv\Scripts\python -m unittest tests/integration/test_cluster.py
```

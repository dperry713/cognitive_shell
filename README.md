# Adaptive AI OS (Distributed State Machine Kernel Prototype)

An experimental AI-driven, partition-tolerant distributed state machine operating system kernel prototype built on a Raft consensus protocol in Python. The system adapts its workload orchestration by constraining the AI controller's mutation space to desired state specifications, demonstrating cluster consistency, log replays, and snapshot recovery.

---

## Key Features

1. **Raft Consensus Protocol**: Implementation of leader election, randomized timeouts, log replication, term safety, and quorum commits over asynchronous TCP connections.
2. **State Recovery & Compaction**: Snapshot creation and log compaction. Nodes prune historical logs and recover state from disk snapshots and committed log tail replays upon reboot.
3. **Pure State Reduction**: Event-sourced state transitions. The state is reconstructed by replaying committed logs to a state reducer.
4. **Constrained AI Orchestration**: A bounded AI Controller evaluates performance metrics and updates the desired state JSON specification.
5. **Decentralized Control Loops**: Node managers that run reconciler engines on leader nodes.
6. **HTTP REST API Server**: Every node exposes a REST API server to query internal consensus states, node state machines, peer beliefs, and submit task proposals.

---

## Project Structure

```text
adaptive-ai-os/
│
├── ai/
│   ├── cognitive.py           # Unified cognitive system
│   ├── controller.py          # Bounded AI controller querying desired states
│   ├── evaluator.py           # Evaluates execution results & priorities
│   ├── gemini_client.py       # Zero-dependency Gemini API client
│   └── planner.py             # Translates goals to task command specs
│
├── api/
│   └── server.py              # HTTP REST API server
│
├── network/
│   ├── client.py              # Async TCP network client
│   └── server.py              # Async TCP network server
│
├── raft/
│   ├── log.py                 # Raft log index model and compaction
│   └── node.py                # Core Raft state and consensus logic
│
├── runtime/
│   ├── loop.py                # Asynchronous orchestrator loop
│   ├── scheduler.py           # Priority scheduler queues
│   └── telemetry.py           # Telemetry metrics and structured logging
│
├── state/
│   └── reducer.py             # Deterministic event-sourced state reducer
│
├── storage/
│   └── log_store.py           # Disk storage for logs, states, and snapshots
│
├── workers/
│   ├── executor.py            # Local process shell executor (sh/wsl)
│   └── worker.py              # Worker task dispatcher
│
├── config.yaml                # Cluster configuration file
├── run.py                     # Local cluster simulation runner
└── test_core.py               # Unit and integration test suite
```

---

## REST API Interface

API HTTP servers are launched alongside each node (API Port = Node Port + 1000). For example, Node 0 on port 5001 hosts its API on port `6001`.

### 1. Query State
* **Endpoint**: `GET /state`
* **Response Payload**:
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
    "log_length": 6
  }
  ```

### 2. Propose Mutation Command
* **Endpoint**: `POST /propose`
* **Request Payload**:
  ```json
  {
    "type": "DESIRED_STATE_UPDATE",
    "payload": {
      "tasks": [{"id": "new-task", "target": "wsl", "command": "echo 'Hello'"}]
    }
  }
  ```

---

## Running the Simulation

Execute the local simulation to run a 3-node cluster, simulate transactions, terminate the leader, witness automatic reelection, reboot the dead node, and verify complete cluster state synchronization:

```bash
python run.py
```

---

## Running Unit Tests

Run the core unit tests (covering log compaction index shifting, conflict truncations, deterministic state reductions, and term safety rules):

```bash
python test_core.py
```

# Adaptive AI OS (Distributed State Machine Kernel)

An AI-driven, partition-tolerant distributed state machine operating system kernel built on **real Raft consensus** in Python. The system adapts its workload orchestration safely by constraining the AI controller's mutation space to desired state specifications only, while guaranteeing cluster consistency, deterministic replays, and recovery from total failure.

---

## Key Features

1. **Failure Tolerance & Real Raft Consensus**: Complete implementation of leader election, randomized election timers, log replication, term safety, and quorum commits over asynchronous TCP connections.
2. **Crash Recovery & Snapshots**: Automated snapshot creation and log compaction. Nodes prune historical logs and recover state from disk snapshots and committed log tail replays upon reboot.
3. **Pure State Reduction**: Clean event-sourced state transitions. The state is deterministically reconstructed by feeding committed Raft logs to a pure state reducer.
4. **Constrained AI Orchestration**: A bounded AI Controller evaluates performance metrics and updates *only* the desired state JSON specification. No direct execution authority is granted to the AI.
5. **Decentralized Control Loops**: Independent node managers that lock onto active leadership transitions to run reconciler engines.
6. **HTTP REST API Server**: Every node exposes a REST API server to query internal consensus states, node state machines, and submit task proposals.

---

## Project Structure

```text
adaptive-ai-os/
│
├── ai/
│   ├── controller.py          # Bounded AI controller querying desired states
│   ├── evaluator.py           # Evaluates execution results & makes priorities
│   └── planner.py             # Translates goals to commands
│
├── control_plane/
│   ├── reconciler.py          # Compares desired specification to actual state
│   ├── desired_state.py       # Holds versioned target specifications
│   └── controller_manager.py  # Spawns and stops controllers on leader shift
│
├── raft/
│   ├── node.py                # Handles TCP sockets, loops, and elections
│   ├── log.py                 # Raft log index model and compaction
│   └── consensus.py           # RequestVote, AppendEntries, InstallSnapshot rules
│
├── log/
│   ├── event_log.py           # Exposes committed log audit tails
│   ├── state_engine.py        # Pure state reducer logic
│   └── snapshot.py            # Local disk persistence for snapshots
│
├── workers/
│   ├── worker.py              # Asynchronous worker task runner
│   └── executor.py            # Subprocess execution unit (e.g. WSL)
│
├── runtime/
│   ├── loop.py                # Asynchronous orchestration control loop
│   └── scheduler.py           # Load limits & priority queues
│
├── api/
│   └── server.py              # Lightweight HTTP JSON API server
│
├── run.py                     # 3-Node local cluster simulation runner
└── test_core.py               # Unit test suite (Raft log, consensus, reducers)
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

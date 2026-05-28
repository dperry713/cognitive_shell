import unittest
import json
import os
import shutil
import asyncio
from state.reducer import StateMachine
from storage.log_store import DiskStorage
from raft.log import RaftLog
from raft.node import RaftNode
from runtime.loop import Orchestrator

class TestStateMachine(unittest.TestCase):
    def test_state_machine_reduction(self):
        machine = StateMachine()
        self.assertEqual(machine.get_state(), {})

        # Apply desired state update
        machine.apply_log_entry({
            "term": 1,
            "index": 1,
            "command": {
                "type": "DESIRED_STATE_UPDATE",
                "payload": {
                    "tasks": [{"id": "t1", "target": "sh", "command": "echo 1"}],
                    "priority": "normal",
                    "replicas": 3
                }
            }
        })
        state = machine.get_state()
        self.assertIn("desired_state", state)
        self.assertEqual(state["desired_state"]["priority"], "normal")

        # Apply task running
        machine.apply_log_entry({
            "term": 1,
            "index": 2,
            "command": {"type": "TASK_RUNNING", "payload": {"id": "t1"}}
        })
        self.assertEqual(machine.get_state()["t1"]["status"], "running")

        # Apply task done
        machine.apply_log_entry({
            "term": 1,
            "index": 3,
            "command": {"type": "TASK_DONE", "payload": {"id": "t1", "result": "ok"}}
        })
        self.assertEqual(machine.get_state()["t1"]["status"], "done")

    def test_determinism(self):
        m1 = StateMachine()
        m2 = StateMachine()
        
        entries = [
            {"term": 1, "index": 1, "command": {"type": "TASK_RUNNING", "payload": {"id": "task-A"}}},
            {"term": 1, "index": 2, "command": {"type": "TASK_DONE", "payload": {"id": "task-A", "result": "res"}}}
        ]
        
        for e in entries:
            m1.apply_log_entry(e)
            m2.apply_log_entry(e)
            
        self.assertEqual(m1.get_state(), m2.get_state())


class TestStorage(unittest.TestCase):
    def setUp(self):
        if os.path.exists("node_test"):
            shutil.rmtree("node_test")
        if os.path.exists("data"):
            shutil.rmtree("data")

    def tearDown(self):
        if os.path.exists("node_test"):
            shutil.rmtree("node_test")
        if os.path.exists("data"):
            shutil.rmtree("data")

    def test_persistence_saving_and_loading(self):
        storage = DiskStorage("test")
        storage.save_state(current_term=3, voted_for="1", commit_index=5, last_applied=4)
        
        state = storage.load_state()
        self.assertEqual(state["current_term"], 3)
        self.assertEqual(state["voted_for"], "1")
        self.assertEqual(state["commit_index"], 5)
        self.assertEqual(state["last_applied"], 4)

        # Log entries
        entries = [
            {"term": 1, "index": 1, "command": "cmd1"},
            {"term": 2, "index": 2, "command": "cmd2"}
        ]
        storage.save_log(entries, last_snapshot_index=0, last_snapshot_term=0)
        
        log_data = storage.load_log()
        self.assertEqual(len(log_data["entries"]), 2)
        self.assertEqual(log_data["entries"][1]["command"], "cmd2")

        # Snapshot
        storage.save_snapshot(last_included_index=2, last_included_term=2, state_data={"key": "val"})
        snapshot = storage.load_snapshot()
        self.assertEqual(snapshot["last_included_index"], 2)
        self.assertEqual(snapshot["state"]["key"], "val")


class TestRaftNodeConsensus(unittest.TestCase):
    def test_candidate_election(self):
        config = {
            "raft": {"election_timeout_min": 0.150, "election_timeout_max": 0.300, "heartbeat_interval": 0.050}
        }
        node = RaftNode("0", ["1", "2"], config)
        self.assertEqual(node.state, "follower")

        # Trigger election timeout manually
        node.last_heartbeat_time = 0  # force timeout
        req = node.tick()
        self.assertIsNotNone(req)
        self.assertEqual(node.state, "candidate")
        self.assertEqual(node.current_term, 1)

        # Grant votes
        node.handle_rpc_response("1", req, {"term": 1, "vote_granted": True})
        self.assertEqual(node.state, "leader")
        self.assertEqual(node.current_leader, "0")

    def test_log_replication_conflicts(self):
        config = {
            "raft": {"election_timeout_min": 0.150, "election_timeout_max": 0.300, "heartbeat_interval": 0.050}
        }
        node = RaftNode("0", ["1", "2"], config)
        node.current_term = 2
        
        # Populate log
        node.log.append(1, "cmd1")
        node.log.append(1, "cmd2")
        node.log.append(2, "cmd3")

        # Append entries mismatch
        res = node.handle_append_entries({
            "term": 2,
            "leader_id": "1",
            "prev_log_index": 2,
            "prev_log_term": 2,  # conflicts: index 2 term was 1
            "entries": [],
            "leader_commit": 0
        })
        self.assertFalse(res["success"])

        # Append entries correct (overwriting index 3 with a new term 3 entry)
        res_ok = node.handle_append_entries({
            "term": 3,
            "leader_id": "1",
            "prev_log_index": 2,
            "prev_log_term": 1,
            "entries": [{"term": 3, "index": 3, "command": "cmd3_new"}],
            "leader_commit": 3
        })
        self.assertTrue(res_ok["success"])
        self.assertEqual(node.log.get_entry(3)["command"], "cmd3_new")
        self.assertEqual(node.log.get_entry(3)["term"], 3)
        self.assertEqual(node.commit_index, 3)


class TestClusterIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_3_node_cluster_election_and_replication(self):
        # 1. Clean directories
        for path in ["data", "node_10", "node_11", "node_12"]:
            if os.path.exists(path):
                shutil.rmtree(path)

        config = {
            "cluster": {
                "nodes": {
                    "10": {"host": "127.0.0.1", "raft_port": 8001, "api_port": 9001},
                    "11": {"host": "127.0.0.1", "raft_port": 8002, "api_port": 9002},
                    "12": {"host": "127.0.0.1", "raft_port": 8003, "api_port": 9003}
                }
            },
            "raft": {
                "election_timeout_min": 0.250,
                "election_timeout_max": 0.450,
                "heartbeat_interval": 0.050
            },
            "logging": {"level": "INFO"}
        }

        # Boot cluster
        orch10 = Orchestrator("10", config)
        orch11 = Orchestrator("11", config)
        orch12 = Orchestrator("12", config)

        await orch10.start()
        await orch11.start()
        await orch12.start()

        # Wait for leader election
        leader = None
        for i in range(15):
            await asyncio.sleep(0.1)
            for o in [orch10, orch11, orch12]:
                if o.node.state == "leader":
                    leader = o
                    break
            if leader:
                break

        self.assertIsNotNone(leader, "A cluster leader must be elected")

        # Propose log replication
        cmd = {"type": "DESIRED_STATE_UPDATE", "payload": {"tasks": [], "priority": "high", "replicas": 3}}
        success = leader.node.propose(cmd)
        self.assertTrue(success)

        # Wait for replication & commit
        committed_index = leader.node.log.last_log_index()
        self.assertGreater(committed_index, 0)
        
        # Verify that all running nodes replicated the log entry
        for i in range(10):
            await asyncio.sleep(0.05)
            all_synced = True
            for o in [orch10, orch11, orch12]:
                if o.node.commit_index < committed_index:
                    all_synced = False
                    break
            if all_synced:
                break
        
        # Verify state machine reduction matches
        expected_state = {"desired_state": {"tasks": [], "priority": "high", "replicas": 3}}
        self.assertEqual(orch10.state_machine.get_state(), expected_state)
        self.assertEqual(orch11.state_machine.get_state(), expected_state)
        self.assertEqual(orch12.state_machine.get_state(), expected_state)

        # Simulate leader crash
        killed_id = leader.node_id
        leader_to_kill = orch10 if killed_id == "10" else (orch11 if killed_id == "11" else orch12)
        await leader_to_kill.stop()

        # Wait for reelection via polling
        new_leader = None
        for i in range(50):  # Poll up to 5 seconds
            await asyncio.sleep(0.1)
            for o in [orch10, orch11, orch12]:
                if o.node_id != killed_id and o.node.state == "leader":
                    new_leader = o
                    break
            if new_leader:
                break

        self.assertIsNotNone(new_leader, "Reelection must elect a new leader from surviving nodes within 5 seconds")

        # Propose new state on new leader
        cmd2 = {"type": "DESIRED_STATE_UPDATE", "payload": {"tasks": [], "priority": "low", "replicas": 3}}
        new_leader.node.propose(cmd2)

        # Restart crashed node
        rebooted = Orchestrator(killed_id, config)
        await rebooted.start()

        # Wait for synchronization via polling
        synced = False
        for step in range(50):  # Poll up to 5 seconds
            await asyncio.sleep(0.1)
            rebooted_state = rebooted.state_machine.get_state()
            if "desired_state" in rebooted_state:
                if rebooted_state["desired_state"].get("priority") == "low":
                    synced = True
                    break
            
            # Print diagnostic stats every 1 second
            if step % 10 == 0:
                print(f"\n[Test Diagnostics Step {step}]")
                for o in [orch10, orch11, orch12, rebooted]:
                    active = "rebooted" if o is rebooted else ("killed" if o == leader_to_kill else "active")
                    print(f"  Node {o.node_id} ({active}): State={o.node.state}, Term={o.node.current_term}, Commit={o.node.commit_index}, Applied={o.node.last_applied}, LogLen={len(o.node.log.entries)}, State={o.state_machine.get_state()}")
                print("")

        self.assertTrue(synced, "Rebooted node failed to synchronize the low priority task spec")

        # Clean shutdown
        await orch10.stop()
        await orch11.stop()
        await orch12.stop()
        await rebooted.stop()

        # Clear directories
        for path in ["data", "node_10", "node_11", "node_12"]:
            if os.path.exists(path):
                shutil.rmtree(path)


if __name__ == "__main__":
    unittest.main()

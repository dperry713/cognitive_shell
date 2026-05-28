import os
import shutil
import asyncio
import unittest
from typing import Dict, Any, Optional

from node.runtime.loop import Orchestrator
from core.transport.client import BLOCKED_PEERS, LATENCY_INJECTIONS

class TestClusterIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        # Clear database directories
        for path in ["data", "node_10", "node_11", "node_12"]:
            if os.path.exists(path):
                shutil.rmtree(path)
        BLOCKED_PEERS.clear()
        LATENCY_INJECTIONS.clear()

    async def asyncTearDown(self) -> None:
        BLOCKED_PEERS.clear()
        LATENCY_INJECTIONS.clear()
        for path in ["data", "node_10", "node_11", "node_12"]:
            if os.path.exists(path):
                shutil.rmtree(path)

    async def test_01_election_and_replication(self) -> None:
        config = {
            "cluster": {
                "nodes": {
                    "10": {"host": "127.0.0.1", "raft_port": 8001, "api_port": 9001},
                    "11": {"host": "127.0.0.1", "raft_port": 8002, "api_port": 9002},
                    "12": {"host": "127.0.0.1", "raft_port": 8003, "api_port": 9003}
                }
            },
            "raft": {
                "election_timeout_min": 0.300,
                "election_timeout_max": 0.600,
                "heartbeat_interval": 0.080
            },
            "logging": {"level": "INFO"}
        }

        # 1. Boot cluster
        orch10 = Orchestrator("10", config)
        orch11 = Orchestrator("11", config)
        orch12 = Orchestrator("12", config)

        await orch10.start()
        await orch11.start()
        await orch12.start()

        # Wait for leader election
        leader: Optional[Orchestrator] = None
        for _ in range(30):
            await asyncio.sleep(0.1)
            for o in [orch10, orch11, orch12]:
                if o.node.state == "leader":
                    leader = o
                    break
            if leader:
                break

        self.assertIsNotNone(leader, "A cluster leader must be elected")

        # Propose log entry
        cmd = {
            "type": "DESIRED_STATE_UPDATE",
            "payload": {
                "tasks": [{"id": "t1", "target": "sh", "command": "echo 1"}],
                "priority": "normal",
                "replicas": 3
            }
        }
        
        success = leader.node.propose(cmd)
        self.assertTrue(success)

        # Wait for commit & replication
        committed_index = leader.node.log.last_log_index()
        self.assertGreater(committed_index, 0)
        
        synced = False
        for _ in range(20):
            await asyncio.sleep(0.1)
            all_synced = True
            for o in [orch10, orch11, orch12]:
                if o.node.commit_index < committed_index:
                    all_synced = False
                    break
            if all_synced:
                synced = True
                break

        self.assertTrue(synced, "Nodes failed to synchronize committed entry")

        # Verify state machine outputs
        expected_state = {
            "desired_state": {
                "tasks": [{"id": "t1", "target": "sh", "command": "echo 1"}],
                "priority": "normal",
                "replicas": 3
            }
        }
        self.assertEqual(orch10.state_machine.get_state().get("desired_state"), expected_state["desired_state"])
        self.assertEqual(orch11.state_machine.get_state().get("desired_state"), expected_state["desired_state"])
        self.assertEqual(orch12.state_machine.get_state().get("desired_state"), expected_state["desired_state"])

        # Stop orchestrators
        await orch10.stop()
        await orch11.stop()
        await orch12.stop()

    async def test_02_leader_failure_and_reelection(self) -> None:
        config = {
            "cluster": {
                "nodes": {
                    "10": {"host": "127.0.0.1", "raft_port": 8001},
                    "11": {"host": "127.0.0.1", "raft_port": 8002},
                    "12": {"host": "127.0.0.1", "raft_port": 8003}
                }
            },
            "raft": {
                "election_timeout_min": 0.300,
                "election_timeout_max": 0.600,
                "heartbeat_interval": 0.080
            },
            "logging": {"level": "INFO"}
        }

        orch10 = Orchestrator("10", config)
        orch11 = Orchestrator("11", config)
        orch12 = Orchestrator("12", config)

        await orch10.start()
        await orch11.start()
        await orch12.start()

        # Find leader
        leader: Optional[Orchestrator] = None
        for _ in range(30):
            await asyncio.sleep(0.1)
            for o in [orch10, orch11, orch12]:
                if o.node.state == "leader":
                    leader = o
                    break
            if leader:
                break

        self.assertIsNotNone(leader)
        leader_id = leader.node_id

        # Stop leader
        await leader.stop()

        # Wait for reelection of a new leader
        new_leader: Optional[Orchestrator] = None
        for _ in range(40):
            await asyncio.sleep(0.1)
            for o in [orch10, orch11, orch12]:
                if o.node_id != leader_id and o.node.state == "leader":
                    new_leader = o
                    break
            if new_leader:
                break

        self.assertIsNotNone(new_leader, "Surviving nodes failed to elect a new leader")
        self.assertNotEqual(new_leader.node_id, leader_id)

        # Stop remaining nodes
        for o in [orch10, orch11, orch12]:
            if o.node_id != leader_id:
                await o.stop()

    async def test_03_network_partition(self) -> None:
        config = {
            "cluster": {
                "nodes": {
                    "10": {"host": "127.0.0.1", "raft_port": 8001},
                    "11": {"host": "127.0.0.1", "raft_port": 8002},
                    "12": {"host": "127.0.0.1", "raft_port": 8003}
                }
            },
            "raft": {
                "election_timeout_min": 0.300,
                "election_timeout_max": 0.600,
                "heartbeat_interval": 0.080
            },
            "logging": {"level": "INFO"}
        }

        orch10 = Orchestrator("10", config)
        orch11 = Orchestrator("11", config)
        orch12 = Orchestrator("12", config)

        await orch10.start()
        await orch11.start()
        await orch12.start()

        # Find leader
        leader: Optional[Orchestrator] = None
        for _ in range(30):
            await asyncio.sleep(0.1)
            for o in [orch10, orch11, orch12]:
                if o.node.state == "leader":
                    leader = o
                    break
            if leader:
                break

        self.assertIsNotNone(leader)
        leader_id = leader.node_id

        # Partition the leader bidirectionally from both followers
        followers = [o.node_id for o in [orch10, orch11, orch12] if o.node_id != leader_id]
        for f in followers:
            BLOCKED_PEERS.add((leader_id, f))

        # Wait for the partitioned followers to elect a new leader between themselves
        new_leader: Optional[Orchestrator] = None
        for _ in range(40):
            await asyncio.sleep(0.1)
            for o in [orch10, orch11, orch12]:
                if o.node_id != leader_id and o.node.state == "leader":
                    new_leader = o
                    break
            if new_leader:
                break

        self.assertIsNotNone(new_leader, "Followers failed to elect a new leader during partition")
        self.assertNotEqual(new_leader.node_id, leader_id)

        # Clear partition (heal)
        BLOCKED_PEERS.clear()
        await asyncio.sleep(0.5)

        # Confirm leader stabilization and term sync
        self.assertEqual(orch10.node.current_term, orch11.node.current_term)
        self.assertEqual(orch11.node.current_term, orch12.node.current_term)

        # Stop all
        await orch10.stop()
        await orch11.stop()
        await orch12.stop()

    async def test_04_reboot_recovery(self) -> None:
        config = {
            "cluster": {
                "nodes": {
                    "10": {"host": "127.0.0.1", "raft_port": 8001},
                    "11": {"host": "127.0.0.1", "raft_port": 8002},
                    "12": {"host": "127.0.0.1", "raft_port": 8003}
                }
            },
            "raft": {
                "election_timeout_min": 0.300,
                "election_timeout_max": 0.600,
                "heartbeat_interval": 0.080
            },
            "logging": {"level": "INFO"}
        }

        orch10 = Orchestrator("10", config)
        orch11 = Orchestrator("11", config)
        orch12 = Orchestrator("12", config)

        await orch10.start()
        await orch11.start()
        await orch12.start()

        # Find leader
        leader: Optional[Orchestrator] = None
        for _ in range(30):
            await asyncio.sleep(0.1)
            for o in [orch10, orch11, orch12]:
                if o.node.state == "leader":
                    leader = o
                    break
            if leader:
                break

        self.assertIsNotNone(leader)
        
        # Kill one follower
        follower_to_kill = orch10 if leader.node_id != "10" else orch11
        killed_id = follower_to_kill.node_id
        await follower_to_kill.stop()

        # Propose log entry to remaining quorum
        cmd = {
            "type": "DESIRED_STATE_UPDATE",
            "payload": {
                "tasks": [{"id": "t2", "target": "sh", "command": "echo 2"}],
                "priority": "high",
                "replicas": 3
            }
        }
        success = leader.node.propose(cmd)
        self.assertTrue(success)

        # Wait for entry to commit in remaining nodes
        committed_index = leader.node.log.last_log_index()
        await asyncio.sleep(0.5)

        # Restart killed follower
        rebooted = Orchestrator(killed_id, config)
        await rebooted.start()

        # Wait for log replay recovery to synchronise state machine
        synced = False
        for _ in range(30):
            await asyncio.sleep(0.1)
            if rebooted.state_machine.get_state().get("desired_state", {}).get("priority") == "high":
                synced = True
                break

        self.assertTrue(synced, "Rebooted node failed to recover state machine to matching state")

        # Stop all
        await orch10.stop()
        await orch11.stop()
        await orch12.stop()
        await rebooted.stop()

    async def test_05_belief_state_influence(self) -> None:
        config = {
            "cluster": {
                "nodes": {
                    "10": {"host": "127.0.0.1", "raft_port": 8001},
                    "11": {"host": "127.0.0.1", "raft_port": 8002},
                    "12": {"host": "127.0.0.1", "raft_port": 8003}
                }
            },
            "raft": {
                "election_timeout_min": 0.300,
                "election_timeout_max": 0.600,
                "heartbeat_interval": 0.080
            },
            "logging": {"level": "INFO"}
        }

        orch10 = Orchestrator("10", config)
        orch11 = Orchestrator("11", config)
        orch12 = Orchestrator("12", config)

        await orch10.start()
        await orch11.start()
        await orch12.start()

        # Find leader
        leader: Optional[Orchestrator] = None
        for _ in range(30):
            await asyncio.sleep(0.1)
            for o in [orch10, orch11, orch12]:
                if o.node.state == "leader":
                    leader = o
                    break
            if leader:
                break

        self.assertIsNotNone(leader)
        leader_id = leader.node_id
        
        follower_id = None
        for p in leader.node.peers:
            if leader.node.belief_engine.suspicion_score(p) < 0.5:
                follower_id = p
                break
        self.assertIsNotNone(follower_id, "At least one follower must be trusted initially")
        
        orch_map = {"10": orch10, "11": orch11, "12": orch12}
        follower = orch_map[follower_id]

        # 1. LATENCY INFLUENCE ON RETRIES
        # Verify initial suspicion score and retry bias are low
        self.assertLess(leader.node.belief_engine.suspicion_score(follower_id), 0.5)
        pre_bias = leader.node.belief_engine.get_retry_interval_bias(follower_id)
        self.assertLess(pre_bias, 3.0)

        # Inject severe latency (0.12s) to follower to trigger SLOW belief state transitions
        LATENCY_INJECTIONS[(leader_id, follower_id)] = 0.12

        # Allow replication ticks to update beliefs
        await asyncio.sleep(0.8)

        # Verify that suspicion score has risen and retry interval bias backoff has activated
        susp = leader.node.belief_engine.suspicion_score(follower_id)
        bias = leader.node.belief_engine.get_retry_interval_bias(follower_id)
        
        self.assertGreater(susp, 0.2, "Follower failure suspicion should have increased under injected latency")
        self.assertGreater(bias, pre_bias, "Retry interval bias should back off (increase) under high latency beliefs")

        # 2. LEADER DEAD INFLUENCE ON TIMEOUTS
        # Now isolate the leader entirely to trigger DEAD belief transition in the follower
        BLOCKED_PEERS.add((leader_id, follower_id))
        
        # Set election timeout min/max higher so we don't start election during observation window
        follower.node.election_timeout_min = 1.2
        follower.node.election_timeout_max = 1.8
        
        # Initially, leader suspicion is low
        self.assertLess(follower.node.belief_engine.suspicion_score(leader_id), 0.5)
        self.assertEqual(follower.node.belief_engine.get_election_timeout_bias(leader_id), 1.0)

        # Allow some ticks to elapse while leader is isolated
        await asyncio.sleep(0.8)

        # Check follower's belief state of leader
        leader_susp = follower.node.belief_engine.suspicion_score(leader_id)
        timeout_bias = follower.node.belief_engine.get_election_timeout_bias(leader_id)

        self.assertGreater(leader_susp, 0.5, "Leader failure suspicion should be high when partition blocks responses")
        self.assertLess(timeout_bias, 1.0, "Election timeout bias should shrink (<1.0) to speed up reelection on suspected leader failure")

        # Clean shutdown
        await orch10.stop()
        await orch11.stop()
        await orch12.stop()


if __name__ == "__main__":
    unittest.main()

import asyncio
import time
import json
from typing import Any, Dict, List, Optional
from core.raft.node import RaftNode
from storage.wal.store import WALStore
from storage.snapshots.store import SnapshotStore
from core.state_machine.reducer import StateMachine
from core.transport.server import NetworkServer
from core.transport.client import NetworkClient
from cognitive.cognitive import CognitiveSystem
from cognitive.gemini_client import GeminiClient
from cognitive.planner import AIPlanner
from cognitive.evaluator import AIEvaluator
from cognitive.controller import AIController
from cognitive.observation_layer.observer import ObservationLayer
from node.runtime.telemetry import TelemetryLogger
from node.runtime.scheduler import Scheduler
from node.runtime.executor import run_task_on_node

class Orchestrator:
    def __init__(self, node_id: str, config: Dict[str, Any]) -> None:
        self.node_id = str(node_id)
        self.config = config
        
        log_level = config.get("logging", {}).get("level", "INFO")
        self.telemetry = TelemetryLogger(self.node_id, log_level)

        self.storage = WALStore(self.node_id)
        self.snapshot_storage = SnapshotStore(self.node_id)
        self.state_machine = StateMachine()

        self.network_client = NetworkClient()
        self.observation_layer = ObservationLayer()
        self.server: Optional[NetworkServer] = None
        self.scheduler = Scheduler()

        self.peer_configs = config.get("cluster", {}).get("nodes", {})
        self.my_config = self.peer_configs.get(self.node_id, {})
        peers = [pid for pid in self.peer_configs if pid != self.node_id]

        self.node = RaftNode(self.node_id, peers, config)

        gemini_client = GeminiClient()
        self.ai_planner = AIPlanner(gemini_client)
        self.ai_evaluator = AIEvaluator(gemini_client)
        self.ai_controller = AIController(gemini_client)
        
        self.cognitive = CognitiveSystem(gemini_client, self.ai_planner, self.ai_evaluator)

        self.running = False
        self.tasks: List[asyncio.Task[Any]] = []

    async def start(self) -> None:
        """Starts the node, restoring state from storage and running network servers."""
        self.running = True
        
        loop = asyncio.get_running_loop()
        try:
            original_handler = loop.get_exception_handler()
        except AttributeError:
            original_handler = None

        def custom_handler(loop: asyncio.AbstractEventLoop, context: Dict[str, Any]) -> None:
            exception = context.get('exception')
            if isinstance(exception, AssertionError):
                return
            if original_handler:
                original_handler(loop, context)
            else:
                loop.default_exception_handler(context)

        loop.set_exception_handler(custom_handler)
        
        state_data = self.storage.load_state()
        log_data = self.storage.load_log()
        snapshot_data = self.snapshot_storage.load_snapshot()
        
        self.node.load_persistent_state(state_data, log_data)
        self.state_machine.reset_to_snapshot(snapshot_data.get("state", {}))
        
        self.node.last_applied = self.node.log.last_snapshot_index
        self.apply_committed_entries(save=False)
        
        self.telemetry.log("INFO", "startup", "Restored persistent state successfully", 
                           term=self.node.current_term, 
                           log_index=self.node.log.last_log_index(),
                           commit_index=self.node.commit_index)

        host = self.my_config.get("host", "127.0.0.1")
        port = self.my_config.get("raft_port", 5001)
        self.server = NetworkServer(host, port, self.process_incoming_rpc)
        await self.server.start()

        self.tasks.append(asyncio.create_task(self.raft_tick_loop()))
        self.tasks.append(asyncio.create_task(self.adaptive_replication_spawner_loop()))
        self.tasks.append(asyncio.create_task(self.leader_control_loop()))

    async def stop(self) -> None:
        """Gracefully shuts down all loops, network servers, and persists state."""
        self.running = False
        
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()

        if self.server:
            await self.server.stop()

        self.persist_raft_state()
        self.telemetry.log("INFO", "shutdown", "Orchestrator shut down completed gracefully")

    def persist_raft_state(self) -> None:
        state_dict = self.node.get_persistent_state_dict()
        log_dict = self.node.get_log_dict()
        
        self.storage.save_state(
            state_dict["current_term"],
            state_dict["voted_for"],
            state_dict["commit_index"],
            state_dict["last_applied"]
        )
        self.storage.save_log(
            log_dict["entries"],
            log_dict["last_snapshot_index"],
            log_dict["last_snapshot_term"]
        )

    def apply_committed_entries(self, save: bool = True) -> None:
        applied_any = False
        while self.node.last_applied < self.node.commit_index:
            next_apply = self.node.last_applied + 1
            entry = self.node.log.get_entry(next_apply)
            if entry:
                old_state = self.state_machine.get_state()
                new_state = self.state_machine.apply_log_entry(entry)
                self.node.last_applied = next_apply
                applied_any = True
                
                self.telemetry.increment_metric("state_transitions")
                self.telemetry.log("INFO", "state_transition", f"Applied committed entry {next_apply}", 
                                   command=entry.command, 
                                   state=new_state)
                
                # Perform periodic snapshots every 5 entries
                if next_apply > 0 and next_apply % 5 == 0:
                    self.take_local_snapshot(next_apply, entry.term, new_state)
            else:
                break
        
        if applied_any and save:
            self.persist_raft_state()

    def take_local_snapshot(self, index: int, term: int, state: Dict[str, Any]) -> None:
        self.telemetry.log("INFO", "snapshot", f"Taking snapshot at index {index}")
        self.snapshot_storage.save_snapshot(index, term, state)
        self.node.log.compact(index, term)

    async def process_incoming_rpc(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        msg_type = msg.get("type")
        
        if msg_type == "RequestVote":
            resp = self.node.handle_request_vote(msg)
        elif msg_type == "AppendEntries":
            resp = self.node.handle_append_entries(msg)
        elif msg_type == "InstallSnapshot":
            resp = self.node.handle_install_snapshot(msg)
            if resp.get("success"):
                snapshot_data = msg.get("data", {})
                self.state_machine.reset_to_snapshot(snapshot_data)
                self.snapshot_storage.save_snapshot(self.node.commit_index, self.node.current_term, snapshot_data)
        else:
            return {"error": f"Unknown RPC: {msg_type}"}

        if self.node.storage_dirty:
            self.persist_raft_state()
            self.node.storage_dirty = False

        if self.node.commit_index > self.node.last_applied:
            self.apply_committed_entries()

        return resp

    async def raft_tick_loop(self) -> None:
        while self.running:
            try:
                await asyncio.sleep(0.010)
                old_state = self.node.state
                
                vote_req = self.node.tick()
                
                if old_state != self.node.state:
                    self.telemetry.increment_metric("state_transitions")
                    self.telemetry.log("INFO", "raft_state_change", f"Node state changed to {self.node.state}", term=self.node.current_term)

                if vote_req:
                    self.telemetry.increment_metric("elections")
                    asyncio.create_task(self.broadcast_request_votes(vote_req))

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.telemetry.log("ERROR", "tick_loop", f"Exception in tick loop: {e}")

    async def broadcast_request_votes(self, vote_req: Dict[str, Any]) -> None:
        term_at_start = self.node.current_term
        
        async def ask_peer(peer_id: str) -> None:
            peer_config = self.peer_configs.get(peer_id, {})
            if not peer_config:
                return
            
            vote_req["from"] = self.node.node_id
            start_time = time.time()
            resp = await self.network_client.send_rpc(
                peer_config["host"], peer_config["raft_port"], vote_req,
                to_node=peer_id
            )
            latency = time.time() - start_time
            
            # Observe RPC outcome in observation layer and update Bayesian belief engine!
            obs = self.observation_layer.observe_rpc(success=(resp is not None), latency=latency, response=resp)
            self.node.belief_engine.update_belief(peer_id, obs)
            
            if resp and self.node.state == "candidate" and self.node.current_term == term_at_start:
                self.node.handle_rpc_response(peer_id, vote_req, resp)
                if self.node.storage_dirty:
                    self.persist_raft_state()
                    self.node.storage_dirty = False
                if self.node.state == "leader":
                    self.telemetry.log("INFO", "election_win", f"Node {self.node_id} won election for term {self.node.current_term}")

        await asyncio.gather(*(ask_peer(p) for p in self.node.peers))

    async def adaptive_replication_spawner_loop(self) -> None:
        peer_tasks: Dict[str, asyncio.Task[Any]] = {}
        while self.running:
            try:
                await asyncio.sleep(0.05)
                if self.node.state == "leader":
                    for p in self.node.peers:
                        if p not in peer_tasks or peer_tasks[p].done():
                            peer_tasks[p] = asyncio.create_task(self.peer_replication_loop(p))
                else:
                    # Cancel any running peer tasks if we are no longer leader
                    for p, task in list(peer_tasks.items()):
                        if not task.done():
                            task.cancel()
                        del peer_tasks[p]
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.telemetry.log("ERROR", "spawner_loop", f"Spawner loop error: {e}")

        # Cancel remaining loops on exit
        for task in peer_tasks.values():
            if not task.done():
                task.cancel()

    async def peer_replication_loop(self, peer_id: str) -> None:
        while self.running and self.node.state == "leader":
            try:
                # Dynamic retry interval bias: backoff retries when peer is slow/dead
                bias = self.node.belief_engine.get_retry_interval_bias(peer_id)
                sleep_time = self.node.heartbeat_interval * bias
                await asyncio.sleep(sleep_time)
                
                if self.node.state == "leader":
                    await self.replicate_to_peer_task(peer_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.telemetry.log("ERROR", "peer_loop", f"Error replicating to peer {peer_id}: {e}")

    async def replicate_to_peers(self) -> None:
        self.telemetry.increment_metric("heartbeats")
        for p in self.node.peers:
            asyncio.create_task(self.replicate_to_peer_task(p))

    async def replicate_to_peer_task(self, peer_id: str) -> None:
        peer_config = self.peer_configs.get(peer_id, {})
        if not peer_config:
            return

        msg = self.node.get_peer_replication_payload(peer_id)
        if msg["type"] == "InstallSnapshot":
            msg["data"] = self.snapshot_storage.load_snapshot().get("state", {})
            
        msg["from"] = self.node.node_id
        
        start_time = time.time()
        resp = await self.network_client.send_rpc(
            peer_config["host"], peer_config["raft_port"], msg,
            to_node=peer_id
        )
        latency = time.time() - start_time
        self.telemetry.record_latency(latency)

        # Update Bayesian beliefs for the peer!
        obs = self.observation_layer.observe_rpc(success=(resp is not None), latency=latency, response=resp)
        self.node.belief_engine.update_belief(peer_id, obs)

        if resp and self.node.state == "leader":
            self.node.handle_rpc_response(peer_id, msg, resp)
            if self.node.storage_dirty:
                self.persist_raft_state()
                self.node.storage_dirty = False
            
            if self.node.commit_index > self.node.last_applied:
                self.apply_committed_entries()

    async def leader_control_loop(self) -> None:
        while self.running:
            try:
                await asyncio.sleep(1.0)
                if self.node.state == "leader":
                    await self.run_leader_reconciliation()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.telemetry.log("ERROR", "control_loop", f"Exception in leader control loop: {e}")

    async def run_leader_reconciliation(self) -> None:
        actual_state = self.state_machine.get_state()
        desired_spec = actual_state.get("desired_state", {
            "tasks": [],
            "priority": "normal",
            "replicas": 3
        })
        
        self.cognitive.ingest_observation(actual_state)

        eval_res = self.cognitive.evaluate_performance()
        if eval_res.get("priority") and eval_res["priority"] != desired_spec.get("priority"):
            self.telemetry.log("INFO", "ai_evaluation", "Priority adjusted by AI evaluator", response=eval_res)
            desired_spec["priority"] = eval_res["priority"]

        try:
            new_spec = self.ai_controller.evaluate_and_update(actual_state, desired_spec)
            if new_spec != desired_spec:
                if self.node.propose({
                    "type": "DESIRED_STATE_UPDATE",
                    "payload": new_spec
                }):
                    self.persist_raft_state()
                    asyncio.create_task(self.replicate_to_peers())
                desired_spec = new_spec
        except Exception as e:
            self.telemetry.log("WARN", "ai_controller", f"AI controller update failed: {e}")

        # Schedule tasks based on scheduling algorithm (prioritization & limit caps)
        scheduled_tasks = self.scheduler.schedule_tasks(
            desired_spec.get("tasks", []), desired_spec.get("replicas", 3)
        )
        
        for task in scheduled_tasks:
            task_id = task["id"]
            task_state = actual_state.get(task_id, {})
            status = task_state.get("status") if isinstance(task_state, dict) else task_state
            
            if not status or status in ["missing", "failed"]:
                self.telemetry.log("INFO", "reconciler", f"Reconciling task {task_id}. Dispatching execution...", status=status)
                
                if self.node.propose({
                    "type": "TASK_RUNNING",
                    "payload": {"id": task_id}
                }):
                    self.persist_raft_state()
                    asyncio.create_task(self.replicate_to_peers())
                
                asyncio.create_task(run_task_on_node(self, task))

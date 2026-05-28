import asyncio
import json
import os
import shutil
import urllib.request
from raft.node import RaftNode
from control_plane.reconciler import Reconciler
from workers.executor import Executor
from ai.controller import AIController
from control_plane.controller_manager import ControllerManager
from runtime.loop import ControlLoop
from api.server import APIServer

class MockGeminiClient:
    def __init__(self):
        self.count = 0

    def generate(self, prompt):
        self.count += 1
        # Generates a growing list of tasks to trigger log expansion and snapshots
        tasks = []
        for i in range(1, min(self.count + 1, 5)):
            tasks.append({
                "id": f"task-{i}",
                "target": "wsl",
                "command": f"echo 'Task {i} execution output'"
            })
            
        spec = {
            "tasks": tasks,
            "priority": "normal",
            "replicas": 3
        }
        return json.dumps(spec)


async def query_api_state(port):
    """Utility to query node internal state via HTTP GET request."""
    try:
        url = f"http://127.0.0.1:{port + 1000}/state"
        # Run blocking HTTP request in thread pool to prevent blocking asyncio
        loop = asyncio.get_running_loop()
        def fetch():
            with urllib.request.urlopen(url, timeout=0.5) as response:
                return json.loads(response.read().decode("utf-8"))
        return await loop.run_in_executor(None, fetch)
    except Exception as e:
        return {"error": str(e)}


async def run_simulation():
    print("==================================================")
    print("      LAUNCHING INTEGRATED ADAPTIVE AI OS         ")
    print("==================================================")
    
    # 1. Start fresh by clearing old node directories
    if os.path.exists("node_data"):
        shutil.rmtree("node_data")
        
    peers = {
        "0": 5001,
        "1": 5002,
        "2": 5003
    }
    
    # Instantiate nodes
    nodes = {
        "0": RaftNode("0", {"1": 5002, "2": 5003}, 5001),
        "1": RaftNode("1", {"0": 5001, "2": 5003}, 5002),
        "2": RaftNode("2", {"0": 5001, "1": 5002}, 5003),
    }
    
    # Start all nodes, API servers, and local control loops
    print("[Simulation] Launching nodes, API servers, and control loops...")
    api_servers = {}
    loops = {}
    mock_ai = AIController(MockGeminiClient())
    executor = Executor()

    for node_id, node in nodes.items():
        await node.start()
        
        # Start API HTTP Server
        api = APIServer(node, node.port)
        api.start()
        api_servers[node_id] = api

        # Start Node Control Loop & Manager
        reconciler = Reconciler(node, executor)
        manager = ControllerManager(node, reconciler, mock_ai, None)
        loop = ControlLoop(node, reconciler, mock_ai, manager, check_interval=1.0)
        await loop.start()
        loops[node_id] = loop
        
    # Wait for leader election
    print("[Simulation] Awaiting cluster leader election...")
    await asyncio.sleep(3.0)
    
    # Check who the elected leader is
    leader = None
    for node in nodes.values():
        if node.state == "leader":
            leader = node
            break
            
    if not leader:
        print("[Simulation] FATAL: No leader was elected!")
        # Clean up
        for l in loops.values():
            l.stop()
        for api in api_servers.values():
            api.stop()
        for n in nodes.values():
            await n.stop()
        return

    leader_id = leader.node_id
    print(f"[Simulation] Node {leader_id} is active leader. Querying its REST API state:")
    state_via_api = await query_api_state(leader.port)
    print(f"--> REST API Response: {json.dumps(state_via_api, indent=2)}")

    print("[Simulation] Running transactions under active consensus (6 seconds)...")
    await asyncio.sleep(6.0)
    
    print(f"\n==================================================")
    print(f"  SIMULATING FAILURE: Killing Leader Node {leader_id}")
    print(f"==================================================")
    loops[leader_id].stop()
    api_servers[leader_id].stop()
    await leader.stop()
    
    print("[Simulation] Awaiting reelection by remaining nodes...")
    await asyncio.sleep(3.0)
    
    new_leader = None
    for node in nodes.values():
        if node.state == "leader" and node.node_id != leader_id:
            new_leader = node
            break
            
    if new_leader:
        print(f"[Simulation] Reelection successful. New leader is Node {new_leader.node_id}.")
        print(f"[Simulation] Querying new leader Node {new_leader.node_id} REST API state:")
        new_state_via_api = await query_api_state(new_leader.port)
        print(f"--> REST API Response: {json.dumps(new_state_via_api, indent=2)}")
    else:
        print("[Simulation] WARNING: No new leader took over.")
        
    print(f"\n==================================================")
    print(f"  SIMULATING RECOVERY: Rebooting Node {leader_id}")
    print(f"==================================================")
    rebooted = RaftNode(leader_id, {k: v for k, v in peers.items() if k != leader_id}, peers[leader_id])
    nodes[leader_id] = rebooted
    await rebooted.start()
    
    # Start API and control loops back up on rebooted node
    api = APIServer(rebooted, rebooted.port)
    api.start()
    api_servers[leader_id] = api

    reconciler = Reconciler(rebooted, executor)
    manager = ControllerManager(rebooted, reconciler, mock_ai, None)
    loop = ControlLoop(rebooted, reconciler, mock_ai, manager, check_interval=1.0)
    await loop.start()
    loops[leader_id] = loop

    print("[Simulation] Synchronizing rebooted node (5 seconds)...")
    await asyncio.sleep(5.0)
    
    print(f"\n[Simulation] Checking state machine on rebooted Node {leader_id} post-recovery:")
    print(f"--> Log Index:     {rebooted.log.last_log_index()}")
    print(f"--> Commit Index:  {rebooted.commit_index}")
    print(f"--> State Machine: {json.dumps(rebooted.actual_state, indent=2)}")
    
    # 7. Clean shut down
    print("\n[Simulation] Shutting down simulation...")
    for l in loops.values():
        l.stop()
    await asyncio.sleep(0.5)
    for api in api_servers.values():
        api.stop()
    for n in list(nodes.values()):
        await n.stop()
    print("==================================================")
    print("            SIMULATION RUN COMPLETED              ")
    print("==================================================")


def main():
    asyncio.run(run_simulation())

if __name__ == "__main__":
    main()

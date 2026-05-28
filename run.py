import asyncio
import json
import os
import shutil
from raft.node import RaftNode
from control_plane.reconciler import Reconciler
from workers.executor import Executor
from ai.controller import AIController

class MockGeminiClient:
    def __init__(self):
        self.count = 0

    def generate(self, prompt):
        self.count += 1
        # Generates a growing list of tasks to trigger log expansion and snapshots
        tasks = []
        for i in range(1, self.count + 1):
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


async def leader_control_loop(nodes, reconciler, ai):
    iteration = 0
    while True:
        try:
            await asyncio.sleep(2.0)
            # Find active leader
            leader = None
            for node in nodes.values():
                if node.state == "leader":
                    leader = node
                    break
            
            if not leader:
                print("[Coordinator] No active leader. Waiting for election...")
                continue
                
            print(f"\n--- [Leader Control Loop Iteration {iteration} on Node {leader.node_id}] ---")
            iteration += 1
            
            # Fetch actual and desired spec from leader
            actual_state = leader.actual_state
            desired_spec = actual_state.get("desired_state", {
                "tasks": [],
                "priority": "normal",
                "replicas": 3
            })
            
            # Bounded AI mutation
            try:
                new_spec = ai.evaluate_and_update(actual_state, desired_spec)
                print(f"[AI] Proposing new desired spec: {json.dumps(new_spec)}")
                leader.propose({
                    "type": "DESIRED_STATE_UPDATE",
                    "payload": new_spec
                })
            except Exception as e:
                print(f"[Coordinator] AI execution failed: {e}")
                
            # Perform reconciliation loop
            reconciler.node = leader
            reconciler.reconcile(desired_spec, actual_state)
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[Coordinator] Loop error: {e}")


async def run_simulation():
    print("==================================================")
    print("          STARTING ADAPTIVE AI OS SIMULATOR       ")
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
    
    # Start all nodes
    print("[Simulation] Launching nodes...")
    for node in nodes.values():
        await node.start()
        
    # Wait for leader election
    print("[Simulation] Awaiting cluster leader election...")
    await asyncio.sleep(3.0)
    
    # Initialize loop components
    mock_ai = AIController(MockGeminiClient())
    executor = Executor()
    reconciler = Reconciler(None, executor)
    
    # Start leader loop
    loop_task = asyncio.create_task(leader_control_loop(nodes, reconciler, mock_ai))
    
    print("[Simulation] Running normal transactions for 8 seconds...")
    await asyncio.sleep(8.0)
    
    # Identify leader
    leader = None
    for node in nodes.values():
        if node.state == "leader":
            leader = node
            break
            
    if not leader:
        print("[Simulation] FATAL: No leader was elected!")
        loop_task.cancel()
        for n in nodes.values():
            await n.stop()
        return
        
    leader_id = leader.node_id
    print(f"\n==================================================")
    print(f"  SIMULATING FAILURE: Killing Leader Node {leader_id}")
    print(f"==================================================")
    await leader.stop()
    
    print("[Simulation] Awaiting reelection by remaining nodes...")
    await asyncio.sleep(3.0)
    
    new_leader = None
    for node in nodes.values():
        if node.state == "leader" and node.node_id != leader_id:
            new_leader = node
            break
            
    if new_leader:
        print(f"[Simulation] Node {new_leader.node_id} successfully elected as new leader.")
        print(f"[Simulation] Submitting a task post-failure to Node {new_leader.node_id}...")
        new_leader.propose({
            "type": "DESIRED_STATE_UPDATE",
            "payload": {
                "tasks": [{"id": "post-fail-task", "target": "wsl", "command": "echo 'Post-failure execution successful'"}],
                "priority": "high",
                "replicas": 3
            }
        })
    else:
        print("[Simulation] WARNING: No new leader took over.")
        
    print(f"\n==================================================")
    print(f"  SIMULATING RECOVERY: Rebooting Node {leader_id}")
    print(f"==================================================")
    # Instantiate node clean from disk recovery
    rebooted = RaftNode(leader_id, {k: v for k, v in peers.items() if k != leader_id}, peers[leader_id])
    nodes[leader_id] = rebooted
    await rebooted.start()
    
    print("[Simulation] Synchronizing rebooted node (5 seconds)...")
    await asyncio.sleep(5.0)
    
    print(f"\n[Simulation] Checking state machine on Node {leader_id} post-recovery:")
    print(f"--> Log Index:    {rebooted.log.last_log_index()}")
    print(f"--> Commit Index: {rebooted.commit_index}")
    print(f"--> State Machine: {json.dumps(rebooted.actual_state, indent=2)}")
    
    # 7. Clean shut down
    print("\n[Simulation] Shutting down simulation...")
    loop_task.cancel()
    await asyncio.sleep(0.5)
    for n in list(nodes.values()):
        await n.stop()
    print("==================================================")
    print("            SIMULATION RUN COMPLETED              ")
    print("==================================================")


def main():
    asyncio.run(run_simulation())

if __name__ == "__main__":
    main()

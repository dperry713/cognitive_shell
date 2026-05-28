import subprocess
import time
import json
import urllib.request
import sys
import os

def query_state(api_port):
    try:
        url = f"http://127.0.0.1:{api_port}/state"
        with urllib.request.urlopen(url, timeout=0.5) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception as e:
        return {"error": str(e)}

def propose_command(api_port, command):
    try:
        url = f"http://127.0.0.1:{api_port}/propose"
        data = json.dumps(command).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=1.0) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception as e:
        return {"error": str(e)}

def main():
    print("==================================================")
    print("      LAUNCHING COGNITIVE RAFT CLUSTER            ")
    print("==================================================")
    
    # Clean database state files from previous runs
    for path in ["data", "node_1", "node_2", "node_3"]:
        if os.path.exists(path):
            import shutil
            shutil.rmtree(path)
            
    processes = {}
    node_ids = ["1", "2", "3"]
    raft_ports = {"1": 5001, "2": 5002, "3": 5003}
    api_ports = {"1": 6001, "2": 6002, "3": 6003}

    print("[Simulation] Bootstrapping 3 Raft Node Processes...")
    for nid in node_ids:
        cmd = [sys.executable, "-m", "node.server", "--id", nid, "--peers", "peers.json"]
        processes[nid] = subprocess.Popen(cmd)
        
    print("[Simulation] Waiting for cluster bootstrap & leader election...")
    time.sleep(3.0)
    
    # Query state of all nodes to locate leader
    leader_id = None
    for nid, api_port in api_ports.items():
        state = query_state(api_port)
        if state.get("state") == "leader":
            leader_id = nid
            break
            
    if not leader_id:
        print("[Simulation] Fatal: No leader elected!")
        for p in processes.values():
            p.terminate()
        sys.exit(1)
        
    print(f"[Simulation] Leader is Node {leader_id}. Current state:")
    print(json.dumps(query_state(api_ports[leader_id]), indent=2))
    
    # Propose desired state update
    print("\n[Simulation] Proposing client transaction...")
    cmd = {
        "type": "DESIRED_STATE_UPDATE",
        "payload": {
            "tasks": [{"id": "task-100", "target": "sh", "command": "echo 'Consensus Active'"}],
            "priority": "normal",
            "replicas": 3
        }
    }
    res = propose_command(api_ports[leader_id], cmd)
    print(f"--> Response: {json.dumps(res)}")
    
    print("[Simulation] Awaiting consensus replication...")
    time.sleep(2.0)
    
    print("\n[Simulation] Checking State Machine Consistency across all nodes:")
    for nid, port in api_ports.items():
        st = query_state(port)
        print(f"  Node {nid} (Term={st.get('current_term')}, Commit={st.get('commit_index')}): State Machine = {json.dumps(st.get('actual_state'))}")

    # Terminate leader to trigger failover
    print(f"\n==================================================")
    print(f"  SIMULATING FAILURE: Killing Leader Node {leader_id}")
    print(f"==================================================")
    processes[leader_id].terminate()
    processes[leader_id].wait()
    
    print("[Simulation] Waiting for reelection by survivors...")
    time.sleep(3.0)
    
    new_leader_id = None
    for nid, api_port in api_ports.items():
        if nid == leader_id:
            continue
        state = query_state(api_port)
        if state.get("state") == "leader":
            new_leader_id = nid
            break
            
    if new_leader_id:
        print(f"[Simulation] Reelection successful! New leader is Node {new_leader_id}.")
        print("[Simulation] Querying new leader state:")
        new_state = query_state(api_ports[new_leader_id])
        print(json.dumps(new_state, indent=2))
        
        # Verify that belief engine suspicion score of killed node has risen
        susp = new_state.get("suspicion_scores", {}).get(leader_id, 0.0)
        print(f"\n[Simulation] New leader Node {new_leader_id} reports suspicion score for dead node {leader_id} is: {susp:.3f}")
    else:
        print("[Simulation] Warning: Reelection failed or took too long.")

    print(f"\n==================================================")
    print(f"  SIMULATING RECOVERY: Rebooting Node {leader_id}")
    print(f"==================================================")
    cmd = [sys.executable, "-m", "node.server", "--id", leader_id, "--peers", "peers.json"]
    processes[leader_id] = subprocess.Popen(cmd)
    
    print("[Simulation] Waiting for recovery & log alignment...")
    time.sleep(3.0)
    
    recovered_state = query_state(api_ports[leader_id])
    print(f"\n[Simulation] Checking state machine on rebooted Node {leader_id} post-recovery:")
    print(f"--> State Machine: {json.dumps(recovered_state.get('actual_state'), indent=2)}")
    
    # Shutdown everything
    print("\n[Simulation] Shutting down all processes...")
    for p in processes.values():
        p.terminate()
        p.wait()
    print("==================================================")
    print("            SIMULATION RUN COMPLETED              ")
    print("==================================================")

if __name__ == "__main__":
    main()

import asyncio
import json
import os
import shutil
import urllib.request
from runtime.loop import Orchestrator
from api.server import APIServer

def load_config(filepath):
    # A simple, robust zero-dependency YAML line parser
    config = {
        "cluster": {"nodes": {}},
        "raft": {
            "election_timeout_min": 0.150,
            "election_timeout_max": 0.300,
            "heartbeat_interval": 0.050
        },
        "logging": {"level": "INFO", "format": "json"}
    }
    
    current_section = None
    current_node = None
    
    with open(filepath, "r") as f:
        for line in f:
            line_strip = line.strip()
            if not line_strip or line_strip.startswith("#"):
                continue
            
            if line.startswith("cluster:"):
                current_section = "cluster"
            elif line.startswith("raft:"):
                current_section = "raft"
            elif line.startswith("logging:"):
                current_section = "logging"
            elif line.startswith("    nodes:"):
                current_section = "nodes"
            elif line.startswith('    "'):
                node_id = line_strip.split(":")[0].strip().replace('"', '')
                current_node = node_id
                config["cluster"]["nodes"][node_id] = {}
            elif line.startswith("      "):
                parts = line_strip.split(":")
                if len(parts) >= 2 and current_node:
                    key = parts[0].strip()
                    val = parts[1].strip()
                    if key in ["raft_port", "api_port"]:
                        config["cluster"]["nodes"][current_node][key] = int(val)
                    else:
                        config["cluster"]["nodes"][current_node][key] = val.replace('"', '').replace("'", "")
            elif line.startswith("  ") and not line.startswith("    "):
                parts = line_strip.split(":")
                if len(parts) >= 2 and current_section:
                    key = parts[0].strip()
                    val = parts[1].strip()
                    if current_section == "raft":
                        config["raft"][key] = float(val)
                    elif current_section == "logging":
                        config["logging"][key] = val.replace('"', '').replace("'", "")
                        
    return config


async def query_api_state(port):
    """Utility to query node internal state via HTTP GET request."""
    try:
        url = f"http://127.0.0.1:{port}/state"
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
    
    # 1. Clean up node and log data directories
    for path in ["data", "node_0", "node_1", "node_2"]:
        if os.path.exists(path):
            shutil.rmtree(path)
            
    # Load config
    config = load_config("config.yaml")
    
    # Instantiate node orchestrators
    orchestrators = {
        "0": Orchestrator("0", config),
        "1": Orchestrator("1", config),
        "2": Orchestrator("2", config)
    }
    
    # Start orchestrators and API servers
    print("[Simulation] Launching orchestrators and API servers...")
    api_servers = {}
    
    for node_id, orchestrator in orchestrators.items():
        await orchestrator.start()
        
        # Start API server
        raft_port = int(config["cluster"]["nodes"][node_id]["raft_port"])
        api = APIServer(orchestrator, raft_port)
        api.start()
        api_servers[node_id] = api
        
    # Wait for leader election
    print("[Simulation] Awaiting cluster leader election...")
    await asyncio.sleep(3.0)
    
    leader = None
    for orchestrator in orchestrators.values():
        if orchestrator.node.state == "leader":
            leader = orchestrator
            break
            
    if not leader:
        print("[Simulation] FATAL: No leader was elected!")
        for orch in orchestrators.values():
            await orch.stop()
        for api in api_servers.values():
            api.stop()
        return

    leader_id = leader.node_id
    leader_api_port = config["cluster"]["nodes"][leader_id]["api_port"]
    print(f"[Simulation] Node {leader_id} is active leader. Querying state:")
    state_via_api = await query_api_state(leader_api_port)
    print(f"--> REST API Response: {json.dumps(state_via_api, indent=2)}")

    print("[Simulation] Running transactions under active consensus (6 seconds)...")
    await asyncio.sleep(6.0)
    
    print(f"\n==================================================")
    print(f"  SIMULATING FAILURE: Killing Leader Node {leader_id}")
    print(f"==================================================")
    api_servers[leader_id].stop()
    await orchestrators[leader_id].stop()
    
    print("[Simulation] Awaiting reelection by remaining nodes...")
    await asyncio.sleep(3.0)
    
    new_leader = None
    for orchestrator in orchestrators.values():
        if orchestrator.node.state == "leader" and orchestrator.node_id != leader_id:
            new_leader = orchestrator
            break
            
    if new_leader:
        new_leader_port = config["cluster"]["nodes"][new_leader.node_id]["api_port"]
        print(f"[Simulation] Reelection successful. New leader is Node {new_leader.node_id}.")
        print(f"[Simulation] Querying new leader Node {new_leader.node_id} REST API state:")
        new_state_via_api = await query_api_state(new_leader_port)
        print(f"--> REST API Response: {json.dumps(new_state_via_api, indent=2)}")
    else:
        print("[Simulation] WARNING: No new leader took over.")
        
    print(f"\n==================================================")
    print(f"  SIMULATING RECOVERY: Rebooting Node {leader_id}")
    print(f"==================================================")
    rebooted = Orchestrator(leader_id, config)
    orchestrators[leader_id] = rebooted
    await rebooted.start()
    
    raft_port = int(config["cluster"]["nodes"][leader_id]["raft_port"])
    api = APIServer(rebooted, raft_port)
    api.start()
    api_servers[leader_id] = api

    print("[Simulation] Synchronizing rebooted node (5 seconds)...")
    await asyncio.sleep(5.0)
    
    print(f"\n[Simulation] Checking state machine on rebooted Node {leader_id} post-recovery:")
    print(f"--> Log Index:     {rebooted.node.log.last_log_index()}")
    print(f"--> Commit Index:  {rebooted.node.commit_index}")
    print(f"--> State Machine: {json.dumps(rebooted.state_machine.get_state(), indent=2)}")
    
    print("\n[Simulation] Shutting down simulation...")
    await asyncio.sleep(0.5)
    for api in api_servers.values():
        api.stop()
    for orch in list(orchestrators.values()):
        await orch.stop()
    print("==================================================")
    print("            SIMULATION RUN COMPLETED              ")
    print("==================================================")


def main():
    asyncio.run(run_simulation())

if __name__ == "__main__":
    main()

import argparse
import json
import os
import sys
import threading
import asyncio
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any, Dict, Optional

from node.runtime.loop import Orchestrator

class RaftAPIHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        # Suppress logging default lines to standard output
        return

    def get_orchestrator(self) -> Optional[Orchestrator]:
        port = self.server.server_address[1]
        return APIServer.orchestrators_map.get(port)

    def do_GET(self) -> None:
        orchestrator = self.get_orchestrator()
        if not orchestrator:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"Orchestrator not mapped to server port.")
            return

        if self.path == "/state":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            
            node = orchestrator.node
            
            # Format peer beliefs & suspicion scores for observability
            beliefs_dict = {}
            suspicion_dict = {}
            for p in node.peers:
                beliefs_dict[p] = node.belief_engine.beliefs.get(p, [0.8, 0.15, 0.05])
                suspicion_dict[p] = node.belief_engine.suspicion_score(p)

            response = {
                "node_id": node.node_id,
                "state": node.state,
                "current_term": node.current_term,
                "commit_index": node.commit_index,
                "last_applied": node.last_applied,
                "actual_state": orchestrator.state_machine.get_state(),
                "log_length": len(node.log.entries),
                "beliefs": beliefs_dict,
                "suspicion_scores": suspicion_dict
            }
            self.wfile.write(json.dumps(response).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self) -> None:
        orchestrator = self.get_orchestrator()
        if not orchestrator:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"Orchestrator not mapped to server port.")
            return

        if self.path == "/propose":
            content_length = int(self.headers["Content-Length"])
            body = self.rfile.read(content_length)
            try:
                command = json.loads(body.decode("utf-8"))
                success = orchestrator.node.propose(command)
                if success:
                    orchestrator.persist_raft_state()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"status": "success"}).encode("utf-8"))
                else:
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"status": "error", "message": "Node is not the active leader"}).encode("utf-8"))
            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(str(e).encode("utf-8"))


class APIServer:
    # Port to orchestrator lookup map
    orchestrators_map: Dict[int, Orchestrator] = {}

    def __init__(self, orchestrator: Orchestrator, port: int) -> None:
        self.orchestrator = orchestrator
        self.port = port
        self.httpd: Optional[HTTPServer] = None
        self.thread: Optional[threading.Thread] = None

    def start(self) -> None:
        APIServer.orchestrators_map[self.port] = self.orchestrator
        self.httpd = HTTPServer(("127.0.0.1", self.port), RaftAPIHandler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        print(f"[API Server] Listening on http://127.0.0.1:{self.port}/state")

    def stop(self) -> None:
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.server_close()
        if self.port in APIServer.orchestrators_map:
            del APIServer.orchestrators_map[self.port]


from node.server.config import load_and_validate_config

async def main() -> None:
    parser = argparse.ArgumentParser(description="Adaptive AI OS Raft Node Runner")
    parser.add_argument("--id", required=True, type=str, help="Unique node identifier")
    parser.add_argument("--peers", required=True, type=str, help="Path to JSON/YAML file containing cluster peer definition config")
    
    args = parser.parse_args()
    
    # Load and validate config using typed schemas
    try:
        config_obj = load_and_validate_config(args.peers)
    except Exception as e:
        print(f"Configuration validation failed: {e}")
        sys.exit(1)
        
    node_config = config_obj.nodes.get(args.id)
    if not node_config:
        print(f"Error: Node ID '{args.id}' not defined in peers file {args.peers}")
        sys.exit(1)

    # Convert ClusterConfig to dictionary structure for backward compatibility
    cluster_config = {
        "cluster": {
            "nodes": {
                nid: {
                    "host": cfg.host,
                    "raft_port": cfg.raft_port,
                    "api_port": cfg.api_port
                }
                for nid, cfg in config_obj.nodes.items()
            }
        },
        "raft": {
            "election_timeout_min": config_obj.raft.election_timeout_min,
            "election_timeout_max": config_obj.raft.election_timeout_max,
            "heartbeat_interval": config_obj.raft.heartbeat_interval
        },
        "cognitive": {
            "decay_rate": config_obj.cognitive.decay_rate,
            "latency_threshold_slow": config_obj.cognitive.latency_threshold_slow,
            "latency_threshold_dead": config_obj.cognitive.latency_threshold_dead
        },
        "logging": {"level": config_obj.logging_level}
    }
        
    orchestrator = Orchestrator(args.id, cluster_config)
    await orchestrator.start()
    
    api_port = node_config.api_port
    api_server = APIServer(orchestrator, api_port)
    api_server.start()
    
    print(f"Node {args.id} started successfully. Press Ctrl+C to terminate.")
    
    # Keep the async loop running until interrupted
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        print("Shutting down node...")
        api_server.stop()
        await orchestrator.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

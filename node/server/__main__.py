import argparse
import json
import os
import sys
import threading
import asyncio
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any, Dict

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


async def main() -> None:
    parser = argparse.ArgumentParser(description="Adaptive AI OS Raft Node Runner")
    parser.add_argument("--id", required=True, type=str, help="Unique node identifier")
    parser.add_argument("--peers", required=True, type=str, help="Path to JSON file containing cluster peer definition config")
    
    args = parser.parse_args()
    
    # Load peers JSON
    if not os.path.exists(args.peers):
        print(f"Error: Peers config file not found: {args.peers}")
        sys.exit(1)
        
    with open(args.peers, "r") as f:
        peers_data = json.load(f)
        
    # Standardised cluster config structure
    cluster_config = {
        "cluster": {
            "nodes": peers_data
        },
        "raft": {
            "election_timeout_min": 0.150,
            "election_timeout_max": 0.300,
            "heartbeat_interval": 0.050
        },
        "logging": {"level": "INFO"}
    }
    
    node_config = peers_data.get(args.id)
    if not node_config:
        print(f"Error: Node ID '{args.id}' not defined in peers file {args.peers}")
        sys.exit(1)
        
    orchestrator = Orchestrator(args.id, cluster_config)
    await orchestrator.start()
    
    api_port = node_config.get("api_port", int(node_config.get("raft_port", 5001)) + 1000)
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

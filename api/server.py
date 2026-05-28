from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import threading

class RaftAPIHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Suppress logging default lines to standard output
        return

    def get_orchestrator(self):
        port = self.server.server_address[1]
        return APIServer.orchestrators_map.get(port)

    def do_GET(self):
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
            response = {
                "node_id": node.node_id,
                "state": node.state,
                "current_term": node.current_term,
                "commit_index": node.commit_index,
                "last_applied": node.last_applied,
                "actual_state": orchestrator.state_machine.get_state(),
                "log_length": len(node.log.entries)
            }
            self.wfile.write(json.dumps(response).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
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
    orchestrators_map = {}

    def __init__(self, orchestrator, port):
        self.orchestrator = orchestrator
        self.port = port + 1000  # Listen on port + 1000 (e.g. 6001 for port 5001)
        self.httpd = None
        self.thread = None

    def start(self):
        APIServer.orchestrators_map[self.port] = self.orchestrator
        self.httpd = HTTPServer(("127.0.0.1", self.port), RaftAPIHandler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        print(f"[API Server] Listening on http://127.0.0.1:{self.port}/state")

    def stop(self):
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.server_close()
        if self.port in APIServer.orchestrators_map:
            del APIServer.orchestrators_map[self.port]

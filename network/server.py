import asyncio
import json

class NetworkServer:
    def __init__(self, host, port, rpc_handler):
        self.host = host
        self.port = port
        self.rpc_handler = rpc_handler
        self.server = None

    async def start(self):
        self.server = await asyncio.start_server(
            self.handle_connection, self.host, self.port
        )
        print(f"[NetworkServer] Listening on {self.host}:{self.port}")

    async def handle_connection(self, reader, writer):
        try:
            line = await reader.readline()
            if not line:
                return
            msg = json.loads(line.decode("utf-8"))
            
            # Route to handling function
            response = await self.rpc_handler(msg)
            
            if response:
                writer.write((json.dumps(response) + "\n").encode("utf-8"))
                await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            print("[NetworkServer] Stopped.")

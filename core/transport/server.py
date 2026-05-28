import asyncio
import json
from typing import Any, Callable, Coroutine, Dict, Optional, Set

class NetworkServer:
    def __init__(
        self,
        host: str,
        port: int,
        rpc_handler: Callable[[Dict[str, Any]], Coroutine[Any, Any, Optional[Dict[str, Any]]]]
    ) -> None:
        self.host = host
        self.port = port
        self.rpc_handler = rpc_handler
        self.server: Optional[asyncio.AbstractServer] = None
        self.active_connections: Set[asyncio.StreamWriter] = set()

    async def start(self) -> None:
        self.server = await asyncio.start_server(
            self.handle_connection, self.host, self.port
        )
        print(f"[NetworkServer] Listening on {self.host}:{self.port}")

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.active_connections.add(writer)
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                msg = json.loads(line.decode("utf-8"))
                corr_id = msg.get("correlation_id")
                
                # Route to RPC handling function
                response = await self.rpc_handler(msg)
                
                if response:
                    # Echo the correlation ID back to the client
                    if corr_id:
                        response["correlation_id"] = corr_id
                    writer.write((json.dumps(response) + "\n").encode("utf-8"))
                    await writer.drain()
        except Exception:
            pass
        finally:
            self.active_connections.discard(writer)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def stop(self) -> None:
        if self.server:
            self.server.close()
            # Force close all active client connections to avoid blocking wait_closed()
            for writer in list(self.active_connections):
                try:
                    writer.close()
                except Exception:
                    pass
            await self.server.wait_closed()
            print("[NetworkServer] Stopped.")

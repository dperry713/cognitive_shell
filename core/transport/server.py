import asyncio
import json
from typing import Any, Callable, Coroutine, Dict, Optional

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

    async def start(self) -> None:
        self.server = await asyncio.start_server(
            self.handle_connection, self.host, self.port
        )
        print(f"[NetworkServer] Listening on {self.host}:{self.port}")

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
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
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def stop(self) -> None:
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            print("[NetworkServer] Stopped.")

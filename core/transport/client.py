import asyncio
import json
import uuid
from typing import Any, Dict, Optional, Set, Tuple

# Global registries for simulating partitions and latency
BLOCKED_PEERS: Set[Tuple[str, str]] = set()
LATENCY_INJECTIONS: Dict[Tuple[str, str], float] = {}

class ConnectionPool:
    def __init__(self) -> None:
        # Maps (host, port) -> (StreamReader, StreamWriter)
        self.connections: Dict[Tuple[str, int], Tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
        # Maps (host, port) -> asyncio.Lock to prevent interleaved stream writes
        self.locks: Dict[Tuple[str, int], asyncio.Lock] = {}

    def get_lock(self, host: str, port: int) -> asyncio.Lock:
        pair = (host, port)
        if pair not in self.locks:
            self.locks[pair] = asyncio.Lock()
        return self.locks[pair]

    async def get_connection(self, host: str, port: int, timeout: float) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        pair = (host, port)
        if pair in self.connections:
            reader, writer = self.connections[pair]
            if not writer.transport.is_closing():
                return reader, writer
            # Close stale connection
            await self.close_connection(host, port)

        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout
        )
        self.connections[pair] = (reader, writer)
        return reader, writer

    async def close_connection(self, host: str, port: int) -> None:
        pair = (host, port)
        if pair in self.connections:
            reader, writer = self.connections.pop(pair)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def close_all(self) -> None:
        for (host, port) in list(self.connections.keys()):
            await self.close_connection(host, port)

class NetworkClient:
    def __init__(self) -> None:
        self.pool = ConnectionPool()

    async def send_rpc(
        self,
        host: str,
        port: int,
        msg: Dict[str, Any],
        timeout: float = 0.5,
        retries: int = 1,
        backoff: float = 0.05,
        to_node: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Sends an RPC request over a persistent/reused TCP connection and returns response.
        Utilizes correlation IDs to match requests and responses, preventing race conditions.
        """
        from_node = msg.get("from")
        if from_node and to_node:
            pair = (str(from_node), str(to_node))
            if pair in BLOCKED_PEERS or (pair[1], pair[0]) in BLOCKED_PEERS:
                await asyncio.sleep(timeout)
                return None
            if pair in LATENCY_INJECTIONS or (pair[1], pair[0]) in LATENCY_INJECTIONS:
                delay = LATENCY_INJECTIONS.get(pair) or LATENCY_INJECTIONS.get((pair[1], pair[0]), 0.0)
                await asyncio.sleep(delay)

        # 1. Generate & embed correlation ID
        corr_id = str(uuid.uuid4())
        msg["correlation_id"] = corr_id

        lock = self.pool.get_lock(host, port)
        
        for attempt in range(retries + 1):
            try:
                # Lock the connection for this peer to ensure sequential stream access
                async with lock:
                    reader, writer = await self.pool.get_connection(host, port, timeout=timeout)
                    
                    writer.write((json.dumps(msg) + "\n").encode("utf-8"))
                    await writer.drain()
                    
                    # Read single-line response with timeout
                    response_line = await asyncio.wait_for(reader.readline(), timeout=timeout)
                    if not response_line:
                        # EOF received, connection dead, force close & try next attempt
                        raise ConnectionResetError("EOF received from server")
                    
                    resp = json.loads(response_line.decode("utf-8"))
                    
                    # 2. Correlate request ID
                    if resp.get("correlation_id") == corr_id:
                        return resp
                    else:
                        # Mismatched correlation ID: correlation error, drop it
                        pass
            except Exception:
                # Close connection on error to force reconnect on next retry
                async with lock:
                    await self.pool.close_connection(host, port)
                
                if attempt < retries:
                    await asyncio.sleep(backoff * (2 ** attempt))
                else:
                    pass
        return None

    async def close(self) -> None:
        await self.pool.close_all()

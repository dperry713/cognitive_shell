import asyncio
import json
from typing import Any, Dict, Optional, Set, Tuple

# Global registry for simulating network partitions
# Contains bidirectional tuples of blocked node IDs, e.g. {("10", "11"), ("11", "10")}
BLOCKED_PEERS: Set[Tuple[str, str]] = set()

# Global registry for injecting network latency
# Maps bidirectional peer tuples to delay in seconds
LATENCY_INJECTIONS: Dict[Tuple[str, str], float] = {}

class NetworkClient:
    def __init__(self) -> None:
        pass

    async def send_rpc(
        self,
        host: str,
        port: int,
        msg: Dict[str, Any],
        timeout: float = 0.1,
        retries: int = 0,
        backoff: float = 0.05,
        to_node: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Sends an RPC request over a short-lived TCP connection and returns response.
        Retries on connection failure with backoff.
        Supports simulating network partitions and high latencies.
        """
        from_node = msg.get("from")
        if from_node and to_node:
            pair = (str(from_node), str(to_node))
            
            # Check for network partition
            if pair in BLOCKED_PEERS or (pair[1], pair[0]) in BLOCKED_PEERS:
                # Simulate network partition timeout
                await asyncio.sleep(timeout)
                return None
                
            # Check for latency injection
            if pair in LATENCY_INJECTIONS or (pair[1], pair[0]) in LATENCY_INJECTIONS:
                delay = LATENCY_INJECTIONS.get(pair) or LATENCY_INJECTIONS.get((pair[1], pair[0]), 0.0)
                await asyncio.sleep(delay)

        for attempt in range(retries + 1):
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=timeout
                )
                
                writer.write((json.dumps(msg) + "\n").encode("utf-8"))
                await writer.drain()
                
                # Read single-line response with timeout
                response_line = await asyncio.wait_for(reader.readline(), timeout=timeout)
                
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass
                
                if response_line:
                    return json.loads(response_line.decode("utf-8"))
            except Exception:
                # Connection refused or timeout
                if attempt < retries:
                    await asyncio.sleep(backoff * (2 ** attempt))
                else:
                    pass
        return None

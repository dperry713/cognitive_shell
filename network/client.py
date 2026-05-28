import asyncio
import json

class NetworkClient:
    def __init__(self):
        pass

    async def send_rpc(self, host, port, msg, timeout=0.1):
        """
        Sends an RPC request over a short-lived TCP connection and returns response.
        If connection fails or times out, returns None.
        """
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.write((json.dumps(msg) + "\n").encode("utf-8"))
            await writer.drain()
            
            # Read single-line response with timeout
            response_line = await asyncio.wait_for(reader.readline(), timeout=timeout)
            writer.close()
            await writer.wait_closed()
            
            if response_line:
                return json.loads(response_line.decode("utf-8"))
        except Exception:
            # Connection refused or timeout
            pass
        return None

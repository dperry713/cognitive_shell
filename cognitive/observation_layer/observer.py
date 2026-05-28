from typing import Any, Dict, Optional
from cognitive.belief_engine.engine import (
    LOW_LATENCY, HIGH_LATENCY, TIMEOUT, MATCH, CONFLICT
)

class ObservationLayer:
    def __init__(self) -> None:
        pass

    def observe_rpc(self, success: bool, latency: Optional[float], response: Optional[Dict[str, Any]]) -> int:
        """
        Translates physical connection/RPC metrics into POMDP observations.
        """
        if not success or latency is None:
            return TIMEOUT

        # 1. Latency check first! If latency is high, it is slow or timeout, regardless of payload success.
        if latency >= 0.25:
            return TIMEOUT
        elif latency >= 0.05:
            return HIGH_LATENCY

        # 2. If it is low latency, check for log matching/conflicts status
        if response and isinstance(response, dict):
            # Term mismatch or AppendEntries rejection
            if response.get("success") is False or response.get("vote_granted") is False:
                return CONFLICT
            # AppendEntries/RequestVote successful alignment
            if response.get("success") is True or response.get("vote_granted") is True:
                return MATCH

        return LOW_LATENCY

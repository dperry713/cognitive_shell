import json
import sys
import time
import logging
from typing import Any, Dict

class TelemetryLogger:
    def __init__(self, node_id: str, log_level: str = "INFO") -> None:
        self.node_id = str(node_id)
        self.level = getattr(logging, log_level.upper(), logging.INFO)
        
        # Performance and status metrics
        self.metrics: Dict[str, Any] = {
            "elections": 0,
            "heartbeats": 0,
            "state_transitions": 0,
            "replication_latency_sum": 0.0,
            "replication_count": 0
        }

    def log(self, level: str, event_type: str, message: str, **kwargs: Any) -> None:
        log_level_num = getattr(logging, level.upper(), logging.INFO)
        if log_level_num < self.level:
            return
            
        log_entry = {
            "timestamp": time.time(),
            "node_id": self.node_id,
            "level": level.upper(),
            "event_type": event_type,
            "message": message,
            **kwargs
        }
        print(json.dumps(log_entry), file=sys.stdout, flush=True)

    def increment_metric(self, name: str, amount: int = 1) -> None:
        if name in self.metrics:
            self.metrics[name] += amount

    def record_latency(self, latency: float) -> None:
        self.metrics["replication_latency_sum"] += latency
        self.metrics["replication_count"] += 1

    def get_metrics(self) -> Dict[str, Any]:
        avg_latency = 0.0
        if self.metrics["replication_count"] > 0:
            avg_latency = self.metrics["replication_latency_sum"] / self.metrics["replication_count"]
        
        return {
            "elections": self.metrics["elections"],
            "heartbeats": self.metrics["heartbeats"],
            "state_transitions": self.metrics["state_transitions"],
            "avg_replication_latency": avg_latency
        }

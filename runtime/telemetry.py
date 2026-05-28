import json
import sys
import time
import logging

class TelemetryLogger:
    def __init__(self, node_id, log_level="INFO"):
        self.node_id = str(node_id)
        self.level = getattr(logging, log_level.upper(), logging.INFO)
        
        # Performance and status metrics
        self.metrics = {
            "elections": 0,
            "heartbeats": 0,
            "state_transitions": 0,
            "replication_latency_sum": 0.0,
            "replication_count": 0
        }

    def log(self, level, event_type, message, **kwargs):
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

    def increment_metric(self, name, amount=1):
        if name in self.metrics:
            self.metrics[name] += amount

    def record_latency(self, latency):
        self.metrics["replication_latency_sum"] += latency
        self.metrics["replication_count"] += 1

    def get_metrics(self):
        avg_latency = 0.0
        if self.metrics["replication_count"] > 0:
            avg_latency = self.metrics["replication_latency_sum"] / self.metrics["replication_count"]
        
        return {
            "elections": self.metrics["elections"],
            "heartbeats": self.metrics["heartbeats"],
            "state_transitions": self.metrics["state_transitions"],
            "avg_replication_latency": avg_latency
        }

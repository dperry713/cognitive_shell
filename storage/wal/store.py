import os
from typing import Any, Dict, List

class WALStore:
    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self.state_dir = f"node_{self.node_id}"
        self.log_dir = "data/logs"
        
        os.makedirs(self.state_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)
        
        self.state_file = os.path.join(self.state_dir, "state.json")
        self.log_file = os.path.join(self.log_dir, f"log_{self.node_id}.json")

    def save_state(self, current_term: int, voted_for: Any, commit_index: int, last_applied: int) -> None:
        data = {
            "current_term": current_term,
            "voted_for": voted_for,
            "commit_index": commit_index,
            "last_applied": last_applied
        }
        with open(self.state_file, "w") as f:
            import json
            json.dump(data, f, indent=2)

    def load_state(self) -> Dict[str, Any]:
        if not os.path.exists(self.state_file):
            return {"current_term": 0, "voted_for": None, "commit_index": 0, "last_applied": 0}
        with open(self.state_file, "r") as f:
            import json
            return json.load(f)

    def save_log(self, entries: List[Dict[str, Any]], last_snapshot_index: int, last_snapshot_term: int) -> None:
        data = {
            "last_snapshot_index": last_snapshot_index,
            "last_snapshot_term": last_snapshot_term,
            "entries": entries
        }
        with open(self.log_file, "w") as f:
            import json
            json.dump(data, f, indent=2)

    def load_log(self) -> Dict[str, Any]:
        if not os.path.exists(self.log_file):
            return {"last_snapshot_index": 0, "last_snapshot_term": 0, "entries": []}
        with open(self.log_file, "r") as f:
            import json
            return json.load(f)

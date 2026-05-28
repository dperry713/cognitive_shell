import os
from typing import Any, Dict

class SnapshotStore:
    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self.snapshot_dir = "data/snapshots"
        os.makedirs(self.snapshot_dir, exist_ok=True)
        self.snapshot_file = os.path.join(self.snapshot_dir, f"snapshot_{self.node_id}.json")

    def save_snapshot(self, last_included_index: int, last_included_term: int, state_data: Dict[str, Any]) -> None:
        data = {
            "last_included_index": last_included_index,
            "last_included_term": last_included_term,
            "state": state_data
        }
        with open(self.snapshot_file, "w") as f:
            import json
            json.dump(data, f, indent=2)

    def load_snapshot(self) -> Dict[str, Any]:
        if not os.path.exists(self.snapshot_file):
            return {"last_included_index": 0, "last_included_term": 0, "state": {}}
        with open(self.snapshot_file, "r") as f:
            import json
            return json.load(f)

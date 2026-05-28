import os
import json

class DiskStorage:
    def __init__(self, node_id):
        self.node_id = str(node_id)
        self.state_dir = f"node_{self.node_id}"
        self.log_dir = "data/logs"
        self.snapshot_dir = "data/snapshots"
        
        os.makedirs(self.state_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)
        os.makedirs(self.snapshot_dir, exist_ok=True)
        
        self.state_file = os.path.join(self.state_dir, "state.json")
        self.log_file = os.path.join(self.log_dir, f"log_{self.node_id}.json")
        self.snapshot_file = os.path.join(self.snapshot_dir, f"snapshot_{self.node_id}.json")

    def save_state(self, current_term, voted_for, commit_index, last_applied):
        data = {
            "current_term": current_term,
            "voted_for": voted_for,
            "commit_index": commit_index,
            "last_applied": last_applied
        }
        with open(self.state_file, "w") as f:
            json.dump(data, f, indent=2)

    def load_state(self):
        if not os.path.exists(self.state_file):
            return {"current_term": 0, "voted_for": None, "commit_index": 0, "last_applied": 0}
        with open(self.state_file, "r") as f:
            return json.load(f)

    def save_log(self, entries, last_snapshot_index, last_snapshot_term):
        data = {
            "last_snapshot_index": last_snapshot_index,
            "last_snapshot_term": last_snapshot_term,
            "entries": entries
        }
        with open(self.log_file, "w") as f:
            json.dump(data, f, indent=2)

    def load_log(self):
        if not os.path.exists(self.log_file):
            return {"last_snapshot_index": 0, "last_snapshot_term": 0, "entries": []}
        with open(self.log_file, "r") as f:
            return json.load(f)

    def save_snapshot(self, last_included_index, last_included_term, state_data):
        data = {
            "last_included_index": last_included_index,
            "last_included_term": last_included_term,
            "state": state_data
        }
        with open(self.snapshot_file, "w") as f:
            json.dump(data, f, indent=2)

    def load_snapshot(self):
        if not os.path.exists(self.snapshot_file):
            return {"last_included_index": 0, "last_included_term": 0, "state": {}}
        with open(self.snapshot_file, "r") as f:
            return json.load(f)

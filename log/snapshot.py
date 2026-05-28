import os
import json

class SnapshotEngine:
    @staticmethod
    def save_snapshot(node_id, last_included_index, last_included_term, state):
        """Saves node state snapshot to disk."""
        os.makedirs("node_data", exist_ok=True)
        filename = f"node_data/node_{node_id}_snapshot.json"
        data = {
            "last_included_index": last_included_index,
            "last_included_term": last_included_term,
            "state": state
        }
        try:
            with open(filename, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"[SnapshotEngine] Failed to write snapshot to {filename}: {e}")

    @staticmethod
    def load_snapshot(node_id):
        """Loads node state snapshot from disk."""
        filename = f"node_data/node_{node_id}_snapshot.json"
        if not os.path.exists(filename):
            return 0, 0, {}
        try:
            with open(filename, "r") as f:
                data = json.load(f)
            return data["last_included_index"], data["last_included_term"], data["state"]
        except Exception as e:
            print(f"[SnapshotEngine] Error loading snapshot from {filename}: {e}")
            return 0, 0, {}

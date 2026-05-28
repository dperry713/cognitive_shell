class EventLog:
    def __init__(self, node=None):
        self.node = node

    def get_committed_events(self):
        """
        Retrieves committed log entries from the node's local Raft log.
        """
        if not self.node:
            return []
            
        # Get entries up to node's committed index
        entries = self.node.log.slice_from(self.node.log.last_snapshot_index + 1)
        return [e for e in entries if e["index"] <= self.node.commit_index]

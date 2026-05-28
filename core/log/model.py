from dataclasses import dataclass
from typing import Any, Dict, List, Optional

@dataclass
class LogEntry:
    term: int
    index: int
    command: Any

class RaftLog:
    def __init__(self) -> None:
        self.entries: List[LogEntry] = []
        self.last_snapshot_index: int = 0
        self.last_snapshot_term: int = 0

    def append(self, term: int, command: Any) -> LogEntry:
        next_index = self.last_log_index() + 1
        entry = LogEntry(term=term, index=next_index, command=command)
        self.entries.append(entry)
        return entry

    def get_entry(self, index: int) -> Optional[LogEntry]:
        if index <= self.last_snapshot_index:
            return None
        list_idx = index - 1 - self.last_snapshot_index
        if 0 <= list_idx < len(self.entries):
            return self.entries[list_idx]
        return None

    def last_log_index(self) -> int:
        if self.entries:
            return self.entries[-1].index
        return self.last_snapshot_index

    def last_log_term(self) -> int:
        if self.entries:
            return self.entries[-1].term
        return self.last_snapshot_term

    def truncate_from(self, index: int) -> None:
        if index <= self.last_snapshot_index:
            return
        list_idx = index - 1 - self.last_snapshot_index
        if 0 <= list_idx < len(self.entries):
            self.entries = self.entries[:list_idx]

    def slice_from(self, index: int) -> List[LogEntry]:
        if index <= self.last_snapshot_index:
            return self.entries
        list_idx = index - 1 - self.last_snapshot_index
        if 0 <= list_idx < len(self.entries):
            return self.entries[list_idx:]
        return []

    def compact(self, snapshot_index: int, snapshot_term: int) -> bool:
        if snapshot_index <= self.last_snapshot_index:
            return False
        list_idx = snapshot_index - 1 - self.last_snapshot_index
        if list_idx >= len(self.entries):
            self.entries = []
        else:
            self.entries = self.entries[list_idx + 1:]
        self.last_snapshot_index = snapshot_index
        self.last_snapshot_term = snapshot_term
        return True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entries": [{"term": e.term, "index": e.index, "command": e.command} for e in self.entries],
            "last_snapshot_index": self.last_snapshot_index,
            "last_snapshot_term": self.last_snapshot_term
        }

    def load_from_dict(self, data: Dict[str, Any]) -> None:
        self.last_snapshot_index = data.get("last_snapshot_index", 0)
        self.last_snapshot_term = data.get("last_snapshot_term", 0)
        self.entries = [
            LogEntry(term=e["term"], index=e["index"], command=e["command"])
            for e in data.get("entries", [])
        ]

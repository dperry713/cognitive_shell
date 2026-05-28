import copy
from typing import Any, Dict, List, Union
from core.log.model import LogEntry

class StateMachine:
    def __init__(self) -> None:
        self.state: Dict[str, Any] = {}
        self.event_history: List[Union[Dict[str, Any], LogEntry]] = []

    def append_event(self, entry: Union[Dict[str, Any], LogEntry]) -> None:
        """
        Record the event in the history log.
        """
        self.event_history.append(copy.deepcopy(entry))

    def apply_event(self, entry: Union[Dict[str, Any], LogEntry]) -> Dict[str, Any]:
        """
        Applies a single log entry event to the state machine.
        Returns the updated state.
        This must be pure and have no side effects outside mutating self.state.
        """
        self.append_event(entry)
        
        if isinstance(entry, dict):
            command = entry.get("command", {})
        else:
            command = entry.command

        if not isinstance(command, dict):
            return self.state
            
        cmd_type = command.get("type")
        payload = command.get("payload", {})
        
        # Pure reducer updates
        new_state = copy.deepcopy(self.state)
        
        if cmd_type == "TASK_DONE":
            new_state[payload["id"]] = {
                "status": "done",
                "result": payload.get("result", {})
            }
        elif cmd_type == "TASK_RUNNING":
            new_state[payload["id"]] = {
                "status": "running"
            }
        elif cmd_type == "TASK_MISSING":
            new_state[payload["id"]] = {
                "status": "missing"
            }
        elif cmd_type == "DESIRED_STATE_UPDATE":
            new_state["desired_state"] = payload

        self.state = new_state
        return self.state

    def replay_log(self, entries: List[Union[Dict[str, Any], LogEntry]]) -> Dict[str, Any]:
        """
        Reconstructs the state machine state from scratch by replaying log entries.
        """
        self.state = {}
        self.event_history = []
        for entry in entries:
            self.apply_event(entry)
        return self.state

    def snapshot(self, last_included_index: int, last_included_term: int) -> Dict[str, Any]:
        """
        Dumps the current state machine state as a snapshot.
        """
        return {
            "last_included_index": last_included_index,
            "last_included_term": last_included_term,
            "state": copy.deepcopy(self.state)
        }

    def restore_snapshot(self, snapshot_data: Dict[str, Any]) -> None:
        """
        Sets the state machine to a snapshot state.
        """
        self.state = copy.deepcopy(snapshot_data.get("state", {}))
        self.event_history = []

    # Backward compatibility mappings
    def apply_log_entry(self, entry: Union[Dict[str, Any], LogEntry]) -> Dict[str, Any]:
        return self.apply_event(entry)

    def reset_to_snapshot(self, snapshot_state: Dict[str, Any]) -> None:
        self.restore_snapshot({"state": snapshot_state})

    def get_state(self) -> Dict[str, Any]:
        return copy.deepcopy(self.state)

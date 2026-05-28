import copy
from typing import Any, Dict, Union
from core.log.model import LogEntry

class StateMachine:
    def __init__(self) -> None:
        self.state: Dict[str, Any] = {}

    def apply_log_entry(self, entry: Union[Dict[str, Any], LogEntry]) -> Dict[str, Any]:
        """
        Applies a single log entry to the state machine.
        Returns the updated state.
        This must be pure and have no side effects.
        """
        # Create a deep copy of the current state
        new_state = copy.deepcopy(self.state)
        
        if isinstance(entry, dict):
            command = entry.get("command", {})
        else:
            command = entry.command

        if not isinstance(command, dict):
            return new_state
            
        cmd_type = command.get("type")
        payload = command.get("payload", {})
        
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

    def reset_to_snapshot(self, snapshot_state: Dict[str, Any]) -> None:
        self.state = copy.deepcopy(snapshot_state)

    def get_state(self) -> Dict[str, Any]:
        return copy.deepcopy(self.state)

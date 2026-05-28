import json

class StateMachine:
    def __init__(self):
        self.state = {}

    def apply_log_entry(self, entry):
        """
        Applies a single log entry to the state machine.
        Returns the updated state.
        This must be pure and have no side effects.
        """
        # Create a deep copy of the current state
        new_state = json.loads(json.dumps(self.state))
        
        command = entry.get("command", {})
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

    def reset_to_snapshot(self, snapshot_state):
        self.state = json.loads(json.dumps(snapshot_state))

    def get_state(self):
        return json.loads(json.dumps(self.state))

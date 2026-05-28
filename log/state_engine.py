import json

def reduce_log(base_state_or_entries, entries=None):
    """
    Pure state engine reducer.
    Supports two signatures:
      1. reduce_log(entries)
      2. reduce_log(base_state, entries)
    """
    if entries is None:
        entries = base_state_or_entries
        state = {}
    else:
        state = json.loads(json.dumps(base_state_or_entries)) if base_state_or_entries else {}

    if not entries:
        return state

    for e in entries:
        command = e.get("command", e)
        cmd_type = command.get("type")
        payload = command.get("payload", {})

        if cmd_type == "TASK_DONE":
            state[payload["id"]] = "done"

        elif cmd_type == "TASK_RUNNING":
            state[payload["id"]] = "running"

        elif cmd_type == "TASK_MISSING":
            state[payload["id"]] = "missing"
            
        elif cmd_type == "DESIRED_STATE_UPDATE":
            state["desired_state"] = payload

    return state

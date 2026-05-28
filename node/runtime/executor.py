import subprocess
import asyncio
import os
from typing import Any, Dict

def execute_task(task: Dict[str, Any]) -> Dict[str, Any]:
    target = task.get("target", "sh")
    if target in ["sh", "wsl"]:
        try:
            cmd = ["wsl", "sh", "-c", task["command"]] if target == "wsl" else ["sh", "-c", task["command"]]
            try:
                res = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=5
                )
            except FileNotFoundError as e:
                if target == "sh" and os.name == "nt":
                    res = subprocess.run(
                        ["cmd.exe", "/c", task["command"]],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                else:
                    raise e
            return {
                "stdout": res.stdout,
                "stderr": res.stderr,
                "returncode": res.returncode
            }
        except Exception as e:
            return {"error": str(e)}

    return {"error": f"unsupported target: {target}"}

async def run_task_on_node(orchestrator: Any, task: Dict[str, Any]) -> None:
    """Runs a task in a thread pool and proposes TASK_DONE when finished."""
    loop = asyncio.get_running_loop()
    
    # Run CPU/subprocess execution in default thread pool to avoid blocking asyncio
    result = await loop.run_in_executor(None, execute_task, task)
    
    # Check if we are still the active leader before proposing completion
    if orchestrator.node.state == "leader":
        print(f"[Worker] Task {task['id']} execution complete. Proposing TASK_DONE.")
        if orchestrator.node.propose({
            "type": "TASK_DONE",
            "payload": {
                "id": task["id"],
                "result": result
            }
        }):
            orchestrator.persist_raft_state()
            asyncio.create_task(orchestrator.replicate_to_peers())

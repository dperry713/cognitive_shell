import subprocess
import asyncio

def execute(task):
    target = task.get("target", "sh")
    if target in ["sh", "wsl"]:
        try:
            cmd = ["wsl", "sh", "-c", task["command"]] if target == "wsl" else ["sh", "-c", task["command"]]
            res = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=5
            )
            return {
                "stdout": res.stdout,
                "stderr": res.stderr,
                "returncode": res.returncode
            }
        except Exception as e:
            # Catch file not found or path errors gracefully and return error structure
            return {"error": str(e)}

    return {"error": f"unsupported target: {target}"}

class Executor:
    def __init__(self):
        pass

    def dispatch(self, node, task):
        """Asynchronously dispatch task without blocking the Raft loop."""
        asyncio.create_task(self.async_execute(node, task))

    async def async_execute(self, node, task):
        from workers.worker import run_task_on_node
        await run_task_on_node(node, task)

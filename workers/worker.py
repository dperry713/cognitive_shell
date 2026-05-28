from workers.executor import execute
import asyncio

async def run_task_on_node(node, task):
    """Runs a task in a thread pool and proposes TASK_DONE when finished."""
    loop = asyncio.get_running_loop()
    
    # Run CPU/subprocess execution in default thread pool to avoid blocking asyncio
    result = await loop.run_in_executor(None, execute, task)
    
    # Check if we are still the active leader before proposing completion
    if node.state == "leader":
        print(f"[Worker] Task {task['id']} execution complete. Proposing TASK_DONE.")
        node.propose({
            "type": "TASK_DONE",
            "payload": {
                "id": task["id"],
                "result": result
            }
        })

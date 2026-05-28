class Reconciler:
    def __init__(self, node, executor):
        self.node = node
        self.executor = executor

    def reconcile(self, desired_spec, actual_state):
        """
        Reconciles desired vs actual task states.
        Only runs on the active leader.
        """
        if self.node.state != "leader":
            return

        desired_tasks = desired_spec.get("tasks", [])

        for task in desired_tasks:
            task_id = task["id"]

            if task_id not in actual_state:
                print(f"[Reconciler] Task {task_id} not found. Proposing TASK_RUNNING.")
                self.node.propose({
                    "type": "TASK_RUNNING",
                    "payload": {"id": task_id}
                })
                # Dispatch the task execution to the worker executor
                self.executor.dispatch(self.node, task)

            elif actual_state.get(task_id) == "missing":
                print(f"[Reconciler] Task {task_id} is missing. Retrying, proposing TASK_RUNNING.")
                self.node.propose({
                    "type": "TASK_RUNNING",
                    "payload": {"id": task_id}
                })
                self.executor.dispatch(self.node, task)

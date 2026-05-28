class Scheduler:
    def __init__(self):
        pass

    def schedule_tasks(self, tasks, replicas):
        """
        Sorts tasks by priorities and filters them to prevent resource overloading.
        """
        # Prioritize tasks based on ID strings deterministically
        scheduled = sorted(tasks, key=lambda t: t.get("id", ""))
        
        # Enforce replica concurrency limit: task count capped to replicas * 2
        capacity = replicas * 2
        if len(scheduled) > capacity:
            scheduled = scheduled[:capacity]
            
        return scheduled

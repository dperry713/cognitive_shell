class AIPlanner:
    def __init__(self):
        pass

    def plan_goal(self, goal_description):
        """
        Translates a high-level goal description into a list of structured WSL commands.
        """
        tasks = []
        normalized_goal = goal_description.lower()
        
        if "backup" in normalized_goal:
            tasks.append({
                "id": "backup-task-1",
                "target": "wsl",
                "command": "tar -czf backup.tar.gz ./node_data"
            })
        elif "cleanup" in normalized_goal or "clean" in normalized_goal:
            tasks.append({
                "id": "cleanup-task-1",
                "target": "wsl",
                "command": "rm -f *.tmp"
            })
        else:
            # Fallback diagnostic task
            tasks.append({
                "id": "diagnostic-task-1",
                "target": "wsl",
                "command": "uname -a"
            })
            
        return tasks

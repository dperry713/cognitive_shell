import json

class CognitiveSystem:
    def __init__(self, gemini_client, planner=None, evaluator=None):
        self.gemini = gemini_client
        self.planner = planner
        self.evaluator = evaluator
        
        # Memory stores
        self.short_term_memory = []  # Contextual sequence of last observations
        self.long_term_memory = {}   # Persistent summarized statistics

    def ingest_observation(self, committed_state):
        """
        Ingests the latest committed state machine state.
        Updates short term memory buffer and long term summary counters.
        """
        self.short_term_memory.append(json.loads(json.dumps(committed_state)))
        if len(self.short_term_memory) > 10:
            self.short_term_memory.pop(0)

        # Update long-term counters
        done_tasks = 0
        running_tasks = 0
        for key, val in committed_state.items():
            if key == "desired_state":
                continue
            if isinstance(val, dict):
                status = val.get("status")
                if status == "done":
                    done_tasks += 1
                elif status == "running":
                    running_tasks += 1
        
        self.long_term_memory["completed_tasks"] = done_tasks
        self.long_term_memory["active_tasks"] = running_tasks

    def execute_tools(self, command_spec):
        """
        Interface to execute external cognitive tools.
        """
        print(f"[CognitiveSystem] Tool Execution Triggered: {command_spec}")
        return {"status": "triggered", "command": command_spec}

    def evaluate_performance(self):
        """
        Runs the AI Evaluator to update priority settings.
        """
        if not self.short_term_memory or not self.evaluator:
            return {"priority": "normal", "suggest_retry": False, "analysis": "Evaluator not fully configured."}
        
        latest_state = self.short_term_memory[-1]
        return self.evaluator.evaluate_performance(latest_state)

    def plan_new_tasks(self, goal_description):
        """
        Runs the AI Planner to output new command lists.
        """
        if not self.planner:
            return []
        return self.planner.plan_goal(goal_description)

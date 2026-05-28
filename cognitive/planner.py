import json
from typing import Any, Dict, List
from cognitive.gemini_client import GeminiClient

class AIPlanner:
    def __init__(self, gemini_client: GeminiClient) -> None:
        self.gemini = gemini_client

    def plan_goal(self, goal_description: str) -> List[Dict[str, Any]]:
        """
        Translates a high-level goal description into a list of structured sh/wsl commands.
        """
        prompt = f"""
You are the AI Planner component of a distributed state machine OS kernel.
The user wants to accomplish the following goal: "{goal_description}"

Generate a list of structured tasks to execute.
Each task must be a JSON object with:
1. "id": A unique, descriptive string id (e.g. "backup-1", "clean-2").
2. "target": The target shell to execute on (must be "sh" or "wsl"). Use "sh" for native/local shells and "wsl" for WSL.
3. "command": The exact shell command string to run (e.g. "tar -czf backup.tar.gz ./node_data").

Respond ONLY with a JSON array of tasks. Do not include any markdown formatting, code fences (like ```json), or extra text.

Example response:
[
  {{"id": "backup-task-1", "target": "sh", "command": "tar -czf backup.tar.gz ./node_data"}}
]
"""
        try:
            if not self.gemini.has_api_key():
                return self._heuristic_fallback(goal_description)

            response = self.gemini.generate(prompt)
            
            # Clean any potential markdown wrapping
            response_str = response.strip()
            if response_str.startswith("```"):
                lines = response_str.splitlines()
                if lines[0].startswith("```"):
                    lines = lines[1:]
                if lines and lines[-1].startswith("```"):
                    lines = lines[:-1]
                response_str = "\n".join(lines).strip()
                
            return json.loads(response_str)
        except Exception as e:
            print(f"[AIPlanner] Warning: failed to plan goal using Gemini ({e}). Falling back to heuristics.")
            return self._heuristic_fallback(goal_description)

    def _heuristic_fallback(self, goal_description: str) -> List[Dict[str, Any]]:
        tasks = []
        normalized_goal = goal_description.lower()
        
        if "backup" in normalized_goal:
            tasks.append({
                "id": "backup-task-1",
                "target": "sh",
                "command": "tar -czf backup.tar.gz ./node_data"
            })
        elif "cleanup" in normalized_goal or "clean" in normalized_goal:
            tasks.append({
                "id": "cleanup-task-1",
                "target": "sh",
                "command": "rm -f *.tmp"
            })
        else:
            # Fallback diagnostic task
            tasks.append({
                "id": "diagnostic-task-1",
                "target": "sh",
                "command": "echo 'System diagnostics completed'"
            })
            
        return tasks

import json

class AIController:
    def __init__(self, gemini_client):
        self.gemini = gemini_client

    def evaluate_and_update(self, state, current_desired_spec):
        """
        Invokes Gemini model to evaluate current actual state and desired specification.
        Returns the updated desired state specification dict.
        Must NOT perform any direct execution or mutation outside of returning the spec.
        """
        prompt = f"""
You are a distributed OS controller.

CURRENT STATE:
{json.dumps(state, indent=2)}

DESIRED STATE SPECIFICATION:
{json.dumps(current_desired_spec, indent=2)}

Return ONLY the updated desired state specification JSON matching the structure:
{{
  "tasks": [
    {{
      "id": "task-id-string",
      "target": "sh",
      "command": "command-string"
    }}
  ],
  "priority": "high", "normal", or "low",
  "replicas": int
}}

Keep the structure valid and return NO markdown wrapping or extra text.
"""
        try:
            if hasattr(self.gemini, "has_api_key") and not self.gemini.has_api_key():
                return self._heuristic_fallback(state, current_desired_spec)

            response = self.gemini.generate(prompt)
            
            # Sanitise potential markdown code block fences if returned by the model
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
            print(f"[AIController] Warning: Gemini spec update failed ({e}). Falling back to heuristics.")
            return self._heuristic_fallback(state, current_desired_spec)

    def _heuristic_fallback(self, state, current_desired_spec):
        new_spec = json.loads(json.dumps(current_desired_spec))
        
        # If priority is 'high' or 'low', preserve it and do not auto-append tasks
        if current_desired_spec.get("priority") in ["high", "low"]:
            return new_spec

        tasks = new_spec.get("tasks", [])
        all_done = True
        for task in tasks:
            if state.get(task["id"]) != "done":
                all_done = False
                break
                
        if all_done and len(tasks) < 5:
            next_idx = len(tasks) + 1
            tasks.append({
                "id": f"task-{next_idx}",
                "target": "sh",
                "command": f"echo 'Task {next_idx} execution output'"
            })
            
        new_spec["tasks"] = tasks
        return new_spec

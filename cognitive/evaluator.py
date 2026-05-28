import json
from typing import Any, Dict
from cognitive.gemini_client import GeminiClient

class AIEvaluator:
    def __init__(self, gemini_client: GeminiClient) -> None:
        self.gemini = gemini_client

    def evaluate_performance(self, actual_state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluates task execution status and returns system control recommendations using Gemini.
        """
        prompt = f"""
You are the AI Evaluator component of a distributed state machine OS kernel.
Here is the current state of the system:
{json.dumps(actual_state, indent=2)}

Evaluate the tasks performance. Check if any tasks failed or returned non-zero codes, and determine if priority needs adjustment or if retries should be suggested.

Respond ONLY with a JSON object containing:
1. "priority": "high", "normal", or "low"
2. "suggest_retry": true or false
3. "analysis": A brief one-sentence explanation of your assessment.

Do not include any markdown formatting, code fences (like ```json), or extra text.

Example response:
{{
  "priority": "high",
  "suggest_retry": true,
  "analysis": "Task t1 failed with return code 1, suggesting priority upgrade and retry."
}}
"""
        try:
            if not self.gemini.has_api_key():
                return self._heuristic_fallback(actual_state)

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
            print(f"[AIEvaluator] Warning: Gemini performance evaluation failed ({e}). Falling back to heuristics.")
            return self._heuristic_fallback(actual_state)

    def _heuristic_fallback(self, actual_state: Dict[str, Any]) -> Dict[str, Any]:
        recommendations: Dict[str, Any] = {
            "suggest_retry": False,
            "analysis": "Heuristic fallback evaluation."
        }
        total_tasks = 0
        failed_tasks = 0
        
        for key, value in actual_state.items():
            if key == "desired_state":
                continue
            if isinstance(value, dict):
                result = value.get("result", {})
                if "error" in result or result.get("returncode", 0) != 0:
                    failed_tasks += 1
                total_tasks += 1
            elif value in ["missing", "failed"]:
                failed_tasks += 1
                total_tasks += 1
                    
        if total_tasks > 0:
            failure_rate = failed_tasks / total_tasks
            if failure_rate > 0.3:
                recommendations["priority"] = "high"
                recommendations["suggest_retry"] = True
                recommendations["analysis"] = f"Heuristics detected failure rate of {failure_rate:.1%} (> 30%)."
                
        return recommendations

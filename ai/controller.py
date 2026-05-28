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

Return ONLY the updated desired state specification JSON.
Keep the structure valid and return NO markdown wrapping or extra text.
"""
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

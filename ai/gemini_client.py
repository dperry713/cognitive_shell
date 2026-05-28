import os
import json
import urllib.request

class GeminiClient:
    def __init__(self, api_key=None, model="gemini-1.5-flash"):
        self.api_key = api_key or os.environ.get("GEMINI_API_KEY")
        self.model = model
        self.url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent"

    def has_api_key(self):
        return bool(self.api_key)

    def generate(self, prompt):
        """
        Sends a POST request to the Google Gemini API to generate text.
        Raises ValueError if API key is missing.
        """
        if not self.api_key:
            raise ValueError(
                "GEMINI_API_KEY environment variable is not set. "
                "Please configure GEMINI_API_KEY to use the real Gemini API."
            )

        headers = {
            "Content-Type": "application/json"
        }
        payload = {
            "contents": [{
                "parts": [{
                    "text": prompt
                }]
            }]
        }
        
        request_url = f"{self.url}?key={self.api_key}"
        req = urllib.request.Request(
            request_url,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST"
        )
        
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                res_data = json.loads(response.read().decode("utf-8"))
                candidates = res_data.get("candidates", [])
                if candidates:
                    content = candidates[0].get("content", {})
                    parts = content.get("parts", [])
                    if parts:
                        return parts[0].get("text", "")
                raise RuntimeError(f"Unexpected response structure from Gemini API: {res_data}")
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else str(e)
            raise RuntimeError(f"Gemini API request failed with HTTP status {e.code}: {error_body}")
        except Exception as e:
            raise RuntimeError(f"Gemini API request failed: {e}")

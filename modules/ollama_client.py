import requests
import json
import logging

OLLAMA_BASE_URL = "http://localhost:11434"

def is_ollama_running():
    """Check if Ollama server is reachable."""
    try:
        response = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=2)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def get_available_models():
    """Get list of available models from Ollama."""
    try:
        response = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
        if response.status_code == 200:
            data = response.json()
            return [model['name'] for model in data.get('models', [])]
        return []
    except Exception as e:
        logging.error("Error fetching models: %s", e)
        return []

def generate_completion(model, prompt, system_prompt=None):
    """
    Generate completion using Ollama.
    Uses the /api/generate endpoint (non-streaming by default for simplicity).
    """
    url = f"{OLLAMA_BASE_URL}/api/generate"
    
    if system_prompt:
        # Simple concatenation for raw completion models, or use specific chat formats if needed
        # For simplicity in MVP, we keep it as standard completion or rely on model's instruction template handling
        pass 

    payload = {
        "model": model,
        "prompt": prompt,
        "system": system_prompt if system_prompt else "",
        "stream": False,
        "format": "json" # Force JSON mode for better coding reliability
    }

    try:
        response = requests.post(url, json=payload, timeout=120)
        response.raise_for_status()
        result = response.json()
        return result.get("response", "")
    except requests.exceptions.RequestException as e:
        raise Exception(f"Ollama API Error: {str(e)}")

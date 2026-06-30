import requests
import json
import logging

import os
from dotenv import load_dotenv
load_dotenv()

def resolve_ollama_url():
    """Resolve the active Ollama base URL, checking env vars first, then falling back dynamically."""
    env_url = os.environ.get("OLLAMA_HOST") or os.environ.get("OLLAMA_BASE_URL")
    if env_url:
        return env_url.rstrip('/')
        
    # Check standard port 11434 first, then 11435
    for port in [11434, 11435]:
        url = f"http://127.0.0.1:{port}"
        try:
            # Quick 1.0s ping to see if server is alive on that port
            response = requests.get(f"{url}/api/tags", timeout=1.0)
            if response.status_code == 200:
                return url
        except requests.exceptions.RequestException:
            continue
            
    # Default fallback
    return "http://127.0.0.1:11434"

OLLAMA_BASE_URL = resolve_ollama_url()

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
        "format": "json", # Force JSON mode for better coding reliability
        "keep_alive": "30m"
    }

    try:
        ollama_timeout = float(os.environ.get("OLLAMA_TIMEOUT", "600.0"))
    except ValueError:
        ollama_timeout = 600.0

    try:
        response = requests.post(url, json=payload, timeout=ollama_timeout)
        response.raise_for_status()
        result = response.json()
        return result.get("response", "")
    except requests.exceptions.RequestException as e:
        raise Exception(f"Ollama API Error: {str(e)}")

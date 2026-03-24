import requests
import subprocess
import sys
import logging

# Defined "packaged" models for this project
REQUIRED_MODELS = [
    "llama3.1",
    "gemma3:12b"
]

OLLAMA_API = "http://localhost:11434/api"

def check_ollama_running() -> bool:
    """Check if Ollama is running."""
    try:
        requests.get(f"{OLLAMA_API}/tags", timeout=2)
        return True
    except Exception as e:
        logging.warning(f"Ollama health check failed: {e}")
        return False

def get_installed_models() -> list[str]:
    """Get list of installed Ollama models."""
    try:
        response = requests.get(f"{OLLAMA_API}/tags")
        if response.status_code == 200:
            models = response.json().get('models', [])
            return [m['name'] for m in models]
    except Exception as e:
        logging.error(f"Error fetching models: {e}")
    return []

def validate_models(auto_pull: bool = False) -> dict:
    """
    Validate that Ollama is running and required models are installed.
    
    Args:
        auto_pull (bool): If True, attempts to pull missing models.
        
    Returns:
        dict: Status report containing:
            - 'ollama_running' (bool)
            - 'missing_models' (list)
            - 'messages' (list of strings for UI display)
            - 'status' (str): 'ok', 'warning', 'error'
    """
    report = {
        'ollama_running': False,
        'missing_models': [],
        'messages': [],
        'status': 'error'
    }
    
    # 1. Check Ollama
    if not check_ollama_running():
        report['messages'].append("❌ Ollama is NOT running. Please start Ollama.")
        return report
    
    report['ollama_running'] = True
    
    # 2. Check Models
    installed = get_installed_models()
    missing = []
    
    for model in REQUIRED_MODELS:
        # Check for exact match or match with :latest
        if model not in installed and f"{model}:latest" not in installed:
            missing.append(model)
            
    if not missing:
        report['status'] = 'ok'
        return report
        
    report['missing_models'] = missing
    report['status'] = 'warning'
    report['messages'].append(f"⚠️ Missing models: {', '.join(missing)}")
    
    # 3. Auto-pull if requested
    if auto_pull:
        for model in missing:
            report['messages'].append(f"⬇️ Pulling {model}...")
            try:
                subprocess.run(["ollama", "pull", model], check=True, capture_output=True)
                report['messages'].append(f"✅ Successfully pulled {model}")
                # Remove from missing list if successful
                if model in report['missing_models']:
                    report['missing_models'].remove(model)
            except Exception as e:
                report['messages'].append(f"❌ Failed to pull {model}: {str(e)}")
                
        if not report['missing_models']:
             report['status'] = 'ok'
             
    return report

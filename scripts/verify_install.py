import sys
import os
import requests
from dotenv import load_dotenv
load_dotenv()

# Add root dir to sys.path
sys.path.append(os.getcwd())

print("Starting installation verification...")

# 1. Check Dependencies
dependencies = [
    "streamlit", "pandas", "sqlalchemy", "tenacity", "requests"
]
missing = []
for dep in dependencies:
    try:
        __import__(dep)
        print(f"[SUCCESS] Library found: {dep}")
    except ImportError:
        print(f"[ERROR] Library MISSING: {dep}")
        missing.append(dep)

if missing:
    print(f"[WARNING] Missing dependencies: {', '.join(missing)}")
    print("Run: pip install " + " ".join(missing))
else:
    print("All core dependencies installed.")

# 2. Check Database Connection
print("\nChecking Database...")
try:
    from core.database import init_db, SessionLocal
    init_db()
    db = SessionLocal()
    # Try a simple query
    from sqlalchemy import text
    db.execute(text("SELECT 1"))
    print("[SUCCESS] Database connected and initialized.")
    db.close()
except Exception as e:
    print(f"[ERROR] Database Error: {e}")

# 3. Check Ollama
print("\nChecking Ollama Connection...")
from modules.ollama_client import is_ollama_running, get_available_models, OLLAMA_BASE_URL
if is_ollama_running():
    print("[SUCCESS] Ollama Service is RUNNING.")
    models = get_available_models()
    if models:
        print(f"[SUCCESS] Available Models: {', '.join(models)}")
    else:
        print("[WARNING] Ollama is running but NO models returned.")
else:
    print(f"[ERROR] Ollama Service is NOT running (or not on {OLLAMA_BASE_URL}).")

# 4. Check Module Imports (Runtime check)
print("\nChecking Module Imports...")
modules_to_check = [
    "modules.data_manager",
    "modules.ollama_client",
    "modules.dashboard"
]

for mod in modules_to_check:
    try:
        __import__(mod)
        print(f"[SUCCESS] Module importable: {mod}")
    except Exception as e:
        print(f"[ERROR] Module Import Error ({mod}): {e}")

print("\nVerification Complete.")

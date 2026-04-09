import sys
import os
import requests

# Add root dir to sys.path
sys.path.append(os.getcwd())

print("🔍 Starting MVP Verification...")

# 1. Check Dependencies
dependencies = [
    "streamlit", "pandas", "praw", "sqlalchemy", "tenacity", "requests", "sklearn", "pyzotero"
]
missing = []
for dep in dependencies:
    try:
        __import__(dep)
        print(f"✅ Library found: {dep}")
    except ImportError:
        print(f"❌ Library MISSING: {dep}")
        missing.append(dep)

if missing:
    print(f"⚠️ Missing dependencies: {', '.join(missing)}")
    print("Run: pip install " + " ".join(missing))
else:
    print("✅ All core dependencies installed.")

# 2. Check Database Connection
print("\n🔍 Checking Database...")
try:
    from core.database import init_db, SessionLocal
    init_db()
    db = SessionLocal()
    # Try a simple query
    from sqlalchemy import text
    db.execute(text("SELECT 1"))
    print("✅ Database connected and initialized.")
    db.close()
except Exception as e:
    print(f"❌ Database Error: {e}")

# 3. Check Ollama
print("\n🔍 Checking Ollama Connection...")
from modules.ollama_client import is_ollama_running, get_available_models
if is_ollama_running():
    print("✅ Ollama Service is RUNNING.")
    models = get_available_models()
    if models:
        print(f"✅ Available Models: {', '.join(models)}")
    else:
        print("⚠️ Ollama is running but NO models returned.")
else:
    print("❌ Ollama Service is NOT running (or not on localhost:11434).")

# 4. Check Module Imports (Runtime check)
print("\n🔍 Checking Module Imports...")
modules_to_check = [
    "modules.reddit_scraper",
    "modules.llm_coder",
    "modules.dashboard",
    "modules.topic_modeling",
    "modules.zotero_manager"
]

for mod in modules_to_check:
    try:
        __import__(mod)
        print(f"✅ Module importable: {mod}")
    except Exception as e:
        print(f"❌ Module Import Error ({mod}): {e}")



print("\n🏁 Verification Complete.")

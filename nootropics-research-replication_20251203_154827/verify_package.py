#!/usr/bin/env python3
"""Verify replication package integrity."""
import json
import hashlib
from pathlib import Path

def calculate_hash(filepath):
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def main():
    manifest_path = Path("manifest.json")
    if not manifest_path.exists():
        print("ERROR: manifest.json not found")
        return False
    
    with open(manifest_path) as f:
        manifest = json.load(f)
    
    print(f"Verifying package generated on {manifest['generated_at']}")
    print(f"Checking {len(manifest['files'])} files...\n")
    
    errors = []
    verified = 0
    for file_info in manifest["files"]:
        check_path = Path(file_info["path"])
        
        if not check_path.exists():
            errors.append(f"MISSING: {check_path}")
            continue
        
        current_hash = calculate_hash(check_path)
        if current_hash != file_info["hash"]:
            errors.append(f"MODIFIED: {check_path}")
        else:
            verified += 1
    
    if errors:
        print("VERIFICATION FAILED")
        print(f"  Verified: {verified}/{len(manifest['files'])} files")
        for error in errors:
            print(f"  {error}")
        return False
    else:
        print("VERIFICATION PASSED")
        print(f"All {verified} files intact and unmodified.")
        return True

if __name__ == "__main__":
    import sys
    sys.exit(0 if main() else 1)

#!/usr/bin/env python3
"""
Replication Package Generator

Creates a complete, shareable research package for academic replication.
Bundles code, data exports, documentation, and generates a manifest.

Usage:
    python package_for_sharing.py [--output-dir OUTPUT_DIR] [--include-db]

Author: Research Tool
Version: 1.0
"""

import os
import sys
import json
import shutil
import hashlib
import argparse
from datetime import datetime
from pathlib import Path

PACKAGE_NAME = "nootropics-research-replication"

REQUIRED_FILES = [
    "app.py",
    "database.py",
    ".env.example",
    "LICENSE",
    "CITATION.cff",
]

OPTIONAL_FILES = [
    "requirements.txt",
    "docs/dependencies.txt",
]

CODE_DIRECTORIES = [
    "modules",
    "utils",
    ".streamlit",
]

DOCUMENTATION_FILES = [
    "docs/replication_package_guide.md",
    "docs/zotero_integration_writeup.md",
    "replit.md",
]

EXCLUDE_PATTERNS = [
    "__pycache__",
    "*.pyc",
    ".git",
    ".env",
    ".upm",
    ".cache",
    ".config",
    "venv",
    ".pythonlibs",
    "*.db",
    "poetry.lock",
]


def calculate_file_hash(filepath):
    """Calculate MD5 hash of a file for integrity verification."""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def should_exclude(path):
    """Check if path should be excluded from package."""
    path_str = str(path)
    for pattern in EXCLUDE_PATTERNS:
        if pattern.startswith("*"):
            if path_str.endswith(pattern[1:]):
                return True
        elif pattern in path_str:
            return True
    return False


def copy_directory(src, dst, manifest, package_root=None):
    """Copy directory recursively, excluding unwanted files."""
    src_path = Path(src)
    dst_path = Path(dst)
    
    if not src_path.exists():
        print(f"  Warning: {src} does not exist, skipping...")
        return
    
    for item in src_path.rglob("*"):
        if should_exclude(item):
            continue
        if item.is_file():
            relative = item.relative_to(src_path)
            dest_file = dst_path / relative
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, dest_file)
            
            # Record the relative path within the package for verification
            if package_root:
                pkg_rel_path = dest_file.relative_to(package_root)
            else:
                pkg_rel_path = dest_file
            
            manifest["files"].append({
                "path": str(pkg_rel_path),
                "hash": calculate_file_hash(item),
                "size": item.stat().st_size
            })


def copy_file(src, dst, manifest, package_root=None):
    """Copy a single file."""
    src_path = Path(src)
    dst_path = Path(dst)
    
    if not src_path.exists():
        print(f"  Warning: {src} does not exist, skipping...")
        return False
    
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src_path, dst_path)
    
    # Record the relative path within the package for verification
    if package_root:
        rel_path = dst_path.relative_to(package_root)
    else:
        rel_path = dst_path
    
    manifest["files"].append({
        "path": str(rel_path),
        "hash": calculate_file_hash(src_path),
        "size": src_path.stat().st_size
    })
    return True


def create_readme(output_dir, manifest):
    """Generate a README.md for the replication package."""
    readme_content = f"""# Nootropics Market Segmentation Research Tool - Replication Package

## Package Information
- **Generated**: {manifest['generated_at']}
- **Package Version**: {manifest['version']}
- **Total Files**: {len(manifest['files'])}

## Description
Academic research tool for qualitative analysis of Reddit data using the Push-Pull-Mooring (PPM) framework with LLM-assisted thematic coding, following Creswell & Creswell (2023) mixed methods standards.

## Quick Start

### Prerequisites
- Python 3.11 or higher
- PostgreSQL 14+ (optional, SQLite works as fallback)

### Installation

1. **Create virtual environment** (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\\Scripts\\activate
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your API credentials
   ```

4. **Run the application**:
   ```bash
   streamlit run app.py --server.port 5000
   ```

## Package Contents

### Code (`/code`)
- `app.py` - Main Streamlit application
- `database.py` - SQLAlchemy database models
- `modules/` - Analysis modules (Reddit scraper, LLM coder, etc.)
- `utils/` - Helper functions

### Data (`/data`)
- `raw/` - Original Reddit data exports
- `processed/` - Coded and analyzed data
- Place your exported CSV/JSON files here

### Documentation (`/docs`)
- `replication_package_guide.md` - Complete replication instructions
- `zotero_integration_writeup.md` - Citation integration documentation

### Output (`/output`)
- `appendices/` - Thesis appendices (A-G)
- `codebook/` - Code definitions
- Place your generated exports here

## Reproducing the Analysis

### Option A: Using Exported Data (No API keys needed)
1. Load CSV files from `data/processed/` into your preferred software
2. Codebook and coding scheme documented in `output/codebook/`

### Option B: Full Replication
1. Obtain Reddit API credentials
2. Obtain Zotero API credentials (optional)
3. Configure `.env` file
4. Follow collection parameters in Appendix A
5. Run LLM coding with settings in Appendix B

## Verification

To verify package integrity:
```python
python verify_package.py
```

## Software Versions

See `requirements.txt` for exact package versions used.

## Citation

If you use this tool or data, please cite:
```
[Your citation here]
```

## License

[Specify your license]

## Contact

[Your contact information]
"""
    
    readme_path = output_dir / "README.md"
    with open(readme_path, "w") as f:
        f.write(readme_content)
    print(f"  Created README.md")


def create_verification_script(output_dir, manifest):
    """Create a script to verify package integrity."""
    script_content = '''#!/usr/bin/env python3
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
    print(f"Checking {len(manifest['files'])} files...\\n")
    
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
'''
    
    script_path = output_dir / "verify_package.py"
    with open(script_path, "w") as f:
        f.write(script_content)
    print(f"  Created verify_package.py")


def create_package(output_dir, include_db=False):
    """Create the complete replication package."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    package_dir = Path(output_dir) / f"{PACKAGE_NAME}_{timestamp}"
    
    print(f"\n{'='*60}")
    print("REPLICATION PACKAGE GENERATOR")
    print(f"{'='*60}\n")
    print(f"Creating package at: {package_dir}\n")
    
    manifest = {
        "package_name": PACKAGE_NAME,
        "version": "1.0",
        "generated_at": datetime.now().isoformat(),
        "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        "files": []
    }
    
    # Create directory structure
    dirs_to_create = [
        package_dir / "code" / "modules",
        package_dir / "code" / "utils",
        package_dir / "code" / ".streamlit",
        package_dir / "data" / "raw",
        package_dir / "data" / "processed",
        package_dir / "output" / "appendices",
        package_dir / "output" / "codebook",
        package_dir / "output" / "reliability",
        package_dir / "docs",
    ]
    
    print("Creating directory structure...")
    for d in dirs_to_create:
        d.mkdir(parents=True, exist_ok=True)
    
    # Copy required files
    print("\nCopying core files...")
    for f in REQUIRED_FILES:
        if copy_file(f, package_dir / "code" / f, manifest, package_dir):
            print(f"  Copied {f}")
    
    # Copy optional files (like requirements.txt or dependencies.txt)
    print("\nCopying dependency files...")
    deps_copied = False
    for f in OPTIONAL_FILES:
        if Path(f).exists():
            # Put dependencies.txt as requirements.txt in the package
            dst_name = "requirements.txt" if "dependencies" in f else Path(f).name
            if copy_file(f, package_dir / "code" / dst_name, manifest, package_dir):
                print(f"  Copied {f} -> {dst_name}")
                deps_copied = True
                break  # Only need one dependency file
    if not deps_copied:
        print("  Warning: No dependency file found. Create docs/dependencies.txt or requirements.txt")
    
    # Copy code directories
    print("\nCopying code directories...")
    for d in CODE_DIRECTORIES:
        print(f"  Copying {d}/...")
        copy_directory(d, package_dir / "code" / d, manifest, package_dir)
    
    # Copy documentation
    print("\nCopying documentation...")
    for f in DOCUMENTATION_FILES:
        src = Path(f)
        if src.exists():
            if f.startswith("docs/"):
                dst = package_dir / f
            else:
                dst = package_dir / "docs" / src.name
            if copy_file(f, dst, manifest, package_dir):
                print(f"  Copied {f}")
    
    # Copy replit.md to docs
    if Path("replit.md").exists():
        copy_file("replit.md", package_dir / "docs" / "project_documentation.md", manifest, package_dir)
    
    # Create README
    print("\nGenerating package files...")
    create_readme(package_dir, manifest)
    create_verification_script(package_dir, manifest)
    
    # Save manifest
    manifest_path = package_dir / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"  Created manifest.json")
    
    # Create placeholder files for data directories
    placeholders = [
        (package_dir / "data" / "raw" / ".gitkeep", "# Place raw Reddit data exports here (CSV/JSON)\n"),
        (package_dir / "data" / "processed" / ".gitkeep", "# Place coded data exports here\n"),
        (package_dir / "output" / "appendices" / ".gitkeep", "# Place thesis appendix exports (A-G) here\n"),
        (package_dir / "output" / "codebook" / ".gitkeep", "# Place codebook exports here\n"),
        (package_dir / "output" / "reliability" / ".gitkeep", "# Place inter-coder reliability reports here\n"),
    ]
    
    for path, content in placeholders:
        with open(path, "w") as f:
            f.write(content)
    
    # Summary
    print(f"\n{'='*60}")
    print("PACKAGE CREATED SUCCESSFULLY")
    print(f"{'='*60}")
    print(f"\nLocation: {package_dir}")
    print(f"Total files: {len(manifest['files'])}")
    print(f"\nNext steps:")
    print("  1. Export your data from the app and place in data/")
    print("  2. Export thesis appendices and place in output/appendices/")
    print("  3. Review and update README.md with your details")
    print("  4. Create a ZIP file for sharing:")
    print(f"     zip -r {PACKAGE_NAME}.zip {package_dir.name}/")
    print("  5. Upload to OSF, Zenodo, or GitHub")
    
    return package_dir


def main():
    parser = argparse.ArgumentParser(
        description="Generate a replication package for academic sharing"
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Output directory for the package (default: current directory)"
    )
    parser.add_argument(
        "--include-db",
        action="store_true",
        help="Include SQLite database file if present"
    )
    
    args = parser.parse_args()
    
    try:
        package_dir = create_package(args.output_dir, args.include_db)
        print(f"\nPackage ready at: {package_dir}")
        return 0
    except Exception as e:
        print(f"\nError creating package: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
Replication Package Generator
==============================

Creates a complete, shareable research package for academic replication.
Bundles code, documentation, and generates integrity manifests for verification.

Key Design Decisions:
- All files are hashed (MD5) for integrity verification post-transfer
- Directory structure follows OSF/Zenodo conventions for academic archival
- Sensitive files (.env, databases) are excluded by default
- Placeholder directories guide researchers on where to add their data

Usage:
    python package_for_sharing.py
    python package_for_sharing.py --output-dir /path/to/output
"""

import argparse
import hashlib
import json
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

# =============================================================================
# CONFIGURATION
# =============================================================================

# Package metadata
PACKAGE_NAME = "nootropics-research-replication"
PACKAGE_VERSION = "1.0"

# Core files that must exist for a valid package
REQUIRED_FILES: List[str] = [
    "app.py",
    "database.py",
    ".env.example",
    "LICENSE",
    "CITATION.cff",
]

# Files included if present (missing is acceptable)
OPTIONAL_FILES: List[str] = [
    "requirements.txt",
    "docs/dependencies.txt",
]

# Directories containing source code to bundle
CODE_DIRECTORIES: List[str] = [
    "modules",
    "utils",
    ".streamlit",
]

# Documentation files to include
DOCUMENTATION_FILES: List[str] = [
    "docs/replication_package_guide.md",
    "docs/zotero_integration_writeup.md",
    "replit.md",
]

# Patterns for files/directories to exclude from packaging
# Prevents accidental inclusion of secrets, caches, or large binaries
EXCLUDE_PATTERNS: List[str] = [
    "__pycache__",
    "*.pyc",
    ".git",
    ".env",           # Contains secrets
    ".upm",
    ".cache",
    ".config",
    "venv",
    ".pythonlibs",
    "*.db",           # Database files should be exported separately
    "poetry.lock",
]


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def calculate_file_hash(filepath: Path) -> str:
    """
    Calculate MD5 hash of a file for integrity verification.
    
    MD5 is used for speed and compatibility (not security).
    Reads file in chunks to handle large files efficiently.
    
    Args:
        filepath: Path to the file to hash
        
    Returns:
        Hexadecimal MD5 hash string
    """
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        # Read in 4KB chunks to avoid memory issues with large files
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def should_exclude(path: Path) -> bool:
    """
    Check if a path matches any exclusion pattern.
    
    Patterns support two formats:
    - Wildcard prefix: "*.pyc" matches any file ending in .pyc
    - Substring: "__pycache__" matches any path containing that string
    
    Args:
        path: Path to check against exclusion patterns
        
    Returns:
        True if the path should be excluded from packaging
    """
    path_str = str(path)
    for pattern in EXCLUDE_PATTERNS:
        if pattern.startswith("*"):
            # Wildcard pattern: check file extension
            if path_str.endswith(pattern[1:]):
                return True
        elif pattern in path_str:
            # Substring pattern: check path contains pattern
            return True
    return False


# =============================================================================
# FILE COPYING FUNCTIONS
# =============================================================================

def copy_directory(
    src: str, 
    dst: str, 
    manifest: Dict[str, Any], 
    package_root: Path = None
) -> None:
    """
    Recursively copy a directory, excluding unwanted files.
    
    Walks through the source directory, copies each eligible file,
    and records its hash in the manifest for later verification.
    
    Args:
        src: Source directory path
        dst: Destination directory path
        manifest: Manifest dictionary to record file hashes
        package_root: Root of package (for relative path calculation)
    """
    src_path = Path(src)
    dst_path = Path(dst)
    
    if not src_path.exists():
        print(f"  Warning: {src} does not exist, skipping...")
        return
    
    # Walk all files recursively
    for item in src_path.rglob("*"):
        if should_exclude(item):
            continue
            
        if item.is_file():
            # Calculate relative path within source directory
            relative = item.relative_to(src_path)
            dest_file = dst_path / relative
            
            # Ensure parent directories exist
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, dest_file)
            
            # Record in manifest with path relative to package root
            if package_root:
                pkg_rel_path = dest_file.relative_to(package_root)
            else:
                pkg_rel_path = dest_file
            
            manifest["files"].append({
                "path": str(pkg_rel_path),
                "hash": calculate_file_hash(item),
                "size": item.stat().st_size
            })


def copy_file(
    src: str, 
    dst: str, 
    manifest: Dict[str, Any], 
    package_root: Path = None
) -> bool:
    """
    Copy a single file and record it in the manifest.
    
    Args:
        src: Source file path
        dst: Destination file path
        manifest: Manifest dictionary to record file hash
        package_root: Root of package (for relative path calculation)
        
    Returns:
        True if file was copied successfully, False if source doesn't exist
    """
    src_path = Path(src)
    dst_path = Path(dst)
    
    if not src_path.exists():
        print(f"  Warning: {src} does not exist, skipping...")
        return False
    
    # Ensure parent directories exist
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src_path, dst_path)
    
    # Record in manifest with path relative to package root
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


# =============================================================================
# GENERATED FILE CREATORS
# =============================================================================

def create_readme(output_dir: Path, manifest: Dict[str, Any]) -> None:
    """
    Generate a README.md with setup instructions and package documentation.
    
    The README follows academic conventions with clear sections for:
    - Quick start instructions
    - Package contents overview
    - Reproduction methodology (with/without API keys)
    - Verification steps
    """
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
- `utils/` - Helper functions including data anonymization

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

Please see `CITATION.cff` for citation information.

## License

See `LICENSE` file for licensing terms.
- Code: MIT License
- Data: CC-BY-4.0

## Contact

[Your contact information]
"""
    
    readme_path = output_dir / "README.md"
    with open(readme_path, "w") as f:
        f.write(readme_content)
    print("  Created README.md")


def create_verification_script(output_dir: Path, manifest: Dict[str, Any]) -> None:
    """
    Generate a Python script to verify package integrity.
    
    The script compares current file hashes against the manifest,
    detecting any modifications or missing files after distribution.
    """
    script_content = '''#!/usr/bin/env python3
"""
Package Integrity Verification Script
======================================

Verifies that all files in the replication package match their original
hashes from the manifest. Detects modifications or missing files.

Usage:
    python verify_package.py
"""

import hashlib
import json
from pathlib import Path


def calculate_hash(filepath: Path) -> str:
    """Calculate MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def main() -> bool:
    """
    Verify all files against manifest.
    
    Returns:
        True if all files verified, False if any issues found
    """
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
    print("  Created verify_package.py")


# =============================================================================
# MAIN PACKAGING LOGIC
# =============================================================================

def create_package(output_dir: str, include_db: bool = False) -> Path:
    """
    Create the complete replication package.
    
    Orchestrates the entire packaging process:
    1. Creates directory structure following academic conventions
    2. Copies and hashes all eligible files
    3. Generates README and verification script
    4. Outputs a manifest for integrity checking
    
    Args:
        output_dir: Directory where package folder will be created
        include_db: Whether to include SQLite database file (default: False)
        
    Returns:
        Path to the created package directory
    """
    # Create timestamped package directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    package_dir = Path(output_dir) / f"{PACKAGE_NAME}_{timestamp}"
    
    # Print header
    print(f"\n{'=' * 60}")
    print("REPLICATION PACKAGE GENERATOR")
    print(f"{'=' * 60}\n")
    print(f"Creating package at: {package_dir}\n")
    
    # Initialize manifest for integrity tracking
    manifest: Dict[str, Any] = {
        "package_name": PACKAGE_NAME,
        "version": PACKAGE_VERSION,
        "generated_at": datetime.now().isoformat(),
        "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        "files": []
    }
    
    # -------------------------------------------------------------------------
    # Create Directory Structure
    # -------------------------------------------------------------------------
    # Following OSF/Zenodo conventions for academic data packages
    
    dirs_to_create = [
        package_dir / "code" / "modules",
        package_dir / "code" / "utils",
        package_dir / "code" / ".streamlit",
        package_dir / "data" / "raw",           # Original collected data
        package_dir / "data" / "processed",     # Coded/analyzed data
        package_dir / "output" / "appendices",  # Thesis appendices A-G
        package_dir / "output" / "codebook",    # Code definitions
        package_dir / "output" / "reliability", # ICR reports
        package_dir / "docs",                   # Documentation
    ]
    
    print("Creating directory structure...")
    for d in dirs_to_create:
        d.mkdir(parents=True, exist_ok=True)
    
    # -------------------------------------------------------------------------
    # Copy Required Files
    # -------------------------------------------------------------------------
    
    print("\nCopying core files...")
    for f in REQUIRED_FILES:
        if copy_file(f, package_dir / "code" / f, manifest, package_dir):
            print(f"  Copied {f}")
    
    # -------------------------------------------------------------------------
    # Copy Dependency File
    # -------------------------------------------------------------------------
    # Prefer requirements.txt, fall back to docs/dependencies.txt
    
    print("\nCopying dependency files...")
    deps_copied = False
    for f in OPTIONAL_FILES:
        if Path(f).exists():
            dst_name = "requirements.txt" if "dependencies" in f else Path(f).name
            if copy_file(f, package_dir / "code" / dst_name, manifest, package_dir):
                print(f"  Copied {f} -> {dst_name}")
                deps_copied = True
                break  # Only need one dependency file
    
    if not deps_copied:
        print("  Warning: No dependency file found. Create docs/dependencies.txt or requirements.txt")
    
    # -------------------------------------------------------------------------
    # Copy Source Code Directories
    # -------------------------------------------------------------------------
    
    print("\nCopying code directories...")
    for d in CODE_DIRECTORIES:
        print(f"  Copying {d}/...")
        copy_directory(d, package_dir / "code" / d, manifest, package_dir)
    
    # -------------------------------------------------------------------------
    # Copy Documentation
    # -------------------------------------------------------------------------
    
    print("\nCopying documentation...")
    for f in DOCUMENTATION_FILES:
        src = Path(f)
        if src.exists():
            # Keep docs/ prefix for doc files, put others in docs/
            if f.startswith("docs/"):
                dst = package_dir / f
            else:
                dst = package_dir / "docs" / src.name
            if copy_file(f, dst, manifest, package_dir):
                print(f"  Copied {f}")
    
    # Copy replit.md as project documentation
    if Path("replit.md").exists():
        copy_file("replit.md", package_dir / "docs" / "project_documentation.md", manifest, package_dir)
    
    # -------------------------------------------------------------------------
    # Generate Package Files
    # -------------------------------------------------------------------------
    
    print("\nGenerating package files...")
    create_readme(package_dir, manifest)
    create_verification_script(package_dir, manifest)
    
    # Save manifest for integrity verification
    manifest_path = package_dir / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print("  Created manifest.json")
    
    # -------------------------------------------------------------------------
    # Create Placeholder Files
    # -------------------------------------------------------------------------
    # Guide researchers on where to place their data
    
    placeholders: List[Tuple[Path, str]] = [
        (package_dir / "data" / "raw" / ".gitkeep", 
         "# Place raw Reddit data exports here (CSV/JSON)\n"),
        (package_dir / "data" / "processed" / ".gitkeep", 
         "# Place coded data exports here\n"),
        (package_dir / "output" / "appendices" / ".gitkeep", 
         "# Place thesis appendix exports (A-G) here\n"),
        (package_dir / "output" / "codebook" / ".gitkeep", 
         "# Place codebook exports here\n"),
        (package_dir / "output" / "reliability" / ".gitkeep", 
         "# Place inter-coder reliability reports here\n"),
    ]
    
    for path, content in placeholders:
        with open(path, "w") as f:
            f.write(content)
    
    # -------------------------------------------------------------------------
    # Print Summary
    # -------------------------------------------------------------------------
    
    print(f"\n{'=' * 60}")
    print("PACKAGE CREATED SUCCESSFULLY")
    print(f"{'=' * 60}")
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


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

def main() -> int:
    """
    Command-line entry point for the package generator.
    
    Returns:
        Exit code (0 for success, 1 for errors)
    """
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

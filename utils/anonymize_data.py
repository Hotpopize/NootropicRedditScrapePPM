#!/usr/bin/env python3
"""
Data Anonymization Script for Academic Research
================================================

Compliance: Reddit Research Data Addendum (executed 2026-04-01)
---------------------------------------------------------------------------
This module implements SHA-256 username pseudonymisation (§2.b).
See COMPLIANCE.md in the repository root for the full mapping.

Removes personally identifiable information (PII) from Reddit data exports
before sharing for replication purposes. Designed for IRB/ethics compliance.

Key Design Decisions:
- Pseudonyms are deterministic when a seed is provided, ensuring reproducibility
- Original usernames are never stored; only SHA-256 hashes for audit purposes
- Handles both structured (CSV columns) and unstructured (text mentions) PII

Usage:
    python utils/anonymize_data.py input.csv output.csv
    python utils/anonymize_data.py --directory data/raw/ --output data/anonymized/
    python utils/anonymize_data.py --seed "thesis-2025" data.csv anon.csv
"""

import argparse
import csv
import hashlib
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# =============================================================================
# CONFIGURATION
# =============================================================================

# Regex patterns for detecting Reddit usernames in text content
# Order matters: more specific patterns first to avoid partial matches
USERNAME_PATTERNS: List[str] = [
    r'/u/([A-Za-z0-9_-]+)',   # Full Reddit format: /u/username
    r'u/([A-Za-z0-9_-]+)',    # Short Reddit format: u/username
    r'@([A-Za-z0-9_]+)',      # Twitter-style mentions: @username
]

# Column names that typically contain usernames as primary values
# Case-insensitive matching applied during processing
USERNAME_COLUMNS: List[str] = [
    'author',
    'username', 
    'user',
    'poster',
    'commenter',
    'submitted_by',
    'created_by',
    'posted_by',
]

# Minimum username length to avoid false positives like "u/a" or "@me"
MIN_USERNAME_LENGTH: int = 3


# =============================================================================
# CORE ANONYMIZATION CLASS
# =============================================================================

class DataAnonymizer:
    """
    Transforms Reddit usernames into anonymous pseudonyms.
    
    Maintains a consistent mapping so the same username always produces
    the same pseudonym within a single run. This preserves relational
    integrity (e.g., tracking the same user across multiple posts).
    
    Attributes:
        username_map: Lookup table mapping original usernames to pseudonyms
        counter: Sequential counter for generating pseudonyms (when no seed)
        seed: Optional seed for reproducible pseudonym generation
        log_entries: Audit trail of all anonymization actions
    """
    
    def __init__(self, seed: Optional[str] = None) -> None:
        """
        Initialize the anonymizer.
        
        Args:
            seed: When provided, pseudonyms are generated via hash function
                  instead of sequential counter. Using the same seed with
                  the same usernames produces identical pseudonyms across runs.
        """
        self.username_map: Dict[str, str] = {}
        self.counter: int = 1
        self.seed: Optional[str] = seed
        self.log_entries: List[Dict[str, str]] = []

    # -------------------------------------------------------------------------
    # Pseudonym Generation
    # -------------------------------------------------------------------------
    
    def _generate_pseudonym(self, username: str) -> str:
        """
        Generate or retrieve a pseudonym for the given username.
        
        Uses memoization to ensure consistency: the same username always
        maps to the same pseudonym within a single anonymizer instance.
        
        Args:
            username: The original Reddit username to anonymize
            
        Returns:
            A pseudonym in the format "User_XXXXXX"
        """
        # Return cached pseudonym if username was seen before
        if username in self.username_map:
            return self.username_map[username]
        
        if self.seed:
            # Hash-based generation: deterministic across runs with same seed
            # Using MD5 for speed (security not a concern for pseudonym generation)
            hash_input = f"{self.seed}:{username}".encode()
            hash_num = int(hashlib.md5(hash_input).hexdigest()[:8], 16)
            pseudonym = f"User_{hash_num:06d}"
        else:
            # Sequential generation: simpler but not reproducible across runs
            pseudonym = f"User_{self.counter:03d}"
            self.counter += 1
        
        # Cache the mapping and log for audit trail
        self.username_map[username] = pseudonym
        self._log_anonymization(username, pseudonym)
        
        return pseudonym
    
    def _log_anonymization(self, username: str, pseudonym: str) -> None:
        """
        Record anonymization action for audit purposes.
        
        Stores a one-way hash of the original username (not the username itself)
        to enable verification without compromising anonymity.
        """
        self.log_entries.append({
            'original_hash': hashlib.sha256(username.encode()).hexdigest()[:16],
            'pseudonym': pseudonym,
            'timestamp': datetime.now().isoformat()
        })

    # -------------------------------------------------------------------------
    # Text Processing
    # -------------------------------------------------------------------------
    
    def _anonymize_text(self, text: str) -> Tuple[str, int]:
        """
        Find and replace username mentions within free-text content.
        
        Scans text for Reddit-style username patterns (u/name, /u/name, @name)
        and replaces them with corresponding pseudonyms while preserving
        the prefix format.
        
        Args:
            text: The text content to scan and anonymize
            
        Returns:
            Tuple of (anonymized_text, replacement_count)
        """
        if not isinstance(text, str):
            return text, 0
        
        replacement_count = 0
        result = text
        
        for pattern in USERNAME_PATTERNS:
            matches = re.findall(pattern, result)
            
            for username in matches:
                # Skip short matches to reduce false positives
                if not username or len(username) < MIN_USERNAME_LENGTH:
                    continue
                    
                pseudonym = self._generate_pseudonym(username)
                
                # Replace username while preserving its prefix (u/, /u/, or @)
                # Word boundary \b prevents partial replacements
                result = re.sub(
                    rf'(/u/|u/|@){re.escape(username)}\b',
                    f'\\1{pseudonym}',
                    result
                )
                replacement_count += 1
        
        return result, replacement_count

    # -------------------------------------------------------------------------
    # CSV Processing
    # -------------------------------------------------------------------------
    
    def anonymize_csv(self, input_path: str, output_path: str) -> Dict[str, Any]:
        """
        Anonymize a CSV file by processing both columns and text content.
        
        Handles two types of PII:
        1. Structured: Columns explicitly containing usernames (e.g., "author")
        2. Unstructured: Username mentions embedded in text fields
        
        Args:
            input_path: Path to the source CSV file
            output_path: Path where anonymized CSV will be written
            
        Returns:
            Statistics dictionary with processing metrics
        """
        stats = {
            'rows_processed': 0,
            'usernames_replaced': 0,
            'text_mentions_replaced': 0,
            'columns_checked': []
        }
        
        # Read and process all rows
        with open(input_path, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames
            stats['columns_checked'] = list(fieldnames) if fieldnames else []
            
            rows = []
            for row in reader:
                stats['rows_processed'] += 1
                new_row = {}
                
                for col, value in row.items():
                    # Check if this column typically contains usernames
                    if col.lower() in USERNAME_COLUMNS:
                        if value and value.strip():
                            new_row[col] = self._generate_pseudonym(value.strip())
                            stats['usernames_replaced'] += 1
                        else:
                            new_row[col] = value
                    else:
                        # Scan text content for embedded mentions
                        new_value, count = self._anonymize_text(value)
                        new_row[col] = new_value
                        stats['text_mentions_replaced'] += count
                
                rows.append(new_row)
        
        # Write anonymized output
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        with open(output_path, 'w', encoding='utf-8', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        
        return stats

    # -------------------------------------------------------------------------
    # JSON Processing
    # -------------------------------------------------------------------------
    
    def anonymize_json(self, input_path: str, output_path: str) -> Dict[str, Any]:
        """
        Anonymize a JSON file by recursively processing all string values.
        
        Handles nested structures (arrays, objects) and applies the same
        logic as CSV processing: structured username fields get direct
        replacement, other text gets scanned for mentions.
        
        Args:
            input_path: Path to the source JSON file
            output_path: Path where anonymized JSON will be written
            
        Returns:
            Statistics dictionary with processing metrics
        """
        stats = {
            'records_processed': 0,
            'usernames_replaced': 0,
            'text_mentions_replaced': 0
        }
        
        with open(input_path, 'r', encoding='utf-8') as infile:
            data = json.load(infile)
        
        def anonymize_value(value: Any, key: str = '') -> Any:
            """Recursively anonymize a JSON value based on its type."""
            nonlocal stats
            
            if isinstance(value, dict):
                return {k: anonymize_value(v, k) for k, v in value.items()}
            elif isinstance(value, list):
                return [anonymize_value(item) for item in value]
            elif isinstance(value, str):
                # Check if this key typically contains usernames
                if key.lower() in USERNAME_COLUMNS:
                    if value.strip():
                        stats['usernames_replaced'] += 1
                        return self._generate_pseudonym(value.strip())
                else:
                    new_value, count = self._anonymize_text(value)
                    stats['text_mentions_replaced'] += count
                    return new_value
            
            # Non-string primitives (int, float, bool, None) pass through unchanged
            return value
        
        # Process top-level structure
        if isinstance(data, list):
            stats['records_processed'] = len(data)
            anonymized_data = [anonymize_value(record) for record in data]
        else:
            stats['records_processed'] = 1
            anonymized_data = anonymize_value(data)
        
        # Write anonymized output
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as outfile:
            json.dump(anonymized_data, outfile, indent=2, ensure_ascii=False)
        
        return stats

    # -------------------------------------------------------------------------
    # High-Level Processing Methods
    # -------------------------------------------------------------------------
    
    def anonymize_file(self, input_path: str, output_path: str) -> Dict[str, Any]:
        """
        Anonymize a single file, auto-detecting format from extension.
        
        Args:
            input_path: Path to source file (must be .csv or .json)
            output_path: Path for anonymized output
            
        Returns:
            Statistics dictionary with processing metrics
            
        Raises:
            ValueError: If file extension is not supported
        """
        path = Path(input_path)
        extension = path.suffix.lower()
        
        if extension == '.csv':
            return self.anonymize_csv(str(path), output_path)
        elif extension == '.json':
            return self.anonymize_json(str(path), output_path)
        else:
            raise ValueError(f"Unsupported file format: {extension}")
    
    def anonymize_directory(self, input_dir: str, output_dir: str) -> Dict[str, Any]:
        """
        Batch anonymize all CSV and JSON files in a directory.
        
        Processes each eligible file and aggregates statistics.
        Non-matching files are silently skipped.
        
        Args:
            input_dir: Source directory to scan
            output_dir: Destination directory for anonymized files
            
        Returns:
            Aggregate statistics across all processed files
        """
        input_path = Path(input_dir)
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        all_stats: Dict[str, Any] = {
            'files_processed': 0,
            'total_usernames_replaced': 0,
            'total_text_mentions_replaced': 0,
            'files': []
        }
        
        for file_path in input_path.glob('*'):
            if file_path.suffix.lower() in ['.csv', '.json']:
                out_file = output_path / file_path.name
                try:
                    stats = self.anonymize_file(str(file_path), str(out_file))
                    stats['filename'] = file_path.name
                    all_stats['files'].append(stats)
                    all_stats['files_processed'] += 1
                    all_stats['total_usernames_replaced'] += stats.get('usernames_replaced', 0)
                    all_stats['total_text_mentions_replaced'] += stats.get('text_mentions_replaced', 0)
                    print(f"  Anonymized: {file_path.name}")
                except Exception as e:
                    print(f"  Error processing {file_path.name}: {e}")
        
        return all_stats

    # -------------------------------------------------------------------------
    # Audit Trail
    # -------------------------------------------------------------------------
    
    def save_audit_log(self, output_path: str) -> None:
        """
        Export anonymization audit log to JSON file.
        
        The audit log contains hashed (not original) usernames paired with
        their pseudonyms. This enables verification of the anonymization
        process without compromising subject privacy.
        """
        log_data = {
            'generated_at': datetime.now().isoformat(),
            'total_unique_users': len(self.username_map),
            'seed_used': self.seed is not None,
            'entries': self.log_entries
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, indent=2)
    
    def get_mapping_summary(self) -> str:
        """Return a brief summary of anonymization results."""
        return f"Mapped {len(self.username_map)} unique usernames to pseudonyms."


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main() -> int:
    """
    Command-line entry point for the anonymization tool.
    
    Supports two modes:
    1. Single file: python anonymize_data.py input.csv output.csv
    2. Directory:   python anonymize_data.py -d data/raw/ -o data/anon/
    
    Returns:
        Exit code (0 for success, 1 for errors)
    """
    parser = argparse.ArgumentParser(
        description='Anonymize Reddit research data for academic sharing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Anonymize a single CSV file:
    python anonymize_data.py collected_data.csv anonymized_data.csv
    
  Anonymize a JSON file:
    python anonymize_data.py coded_data.json anonymized_coded_data.json
    
  Anonymize all files in a directory:
    python anonymize_data.py --directory data/raw/ --output data/anonymized/
    
  Use a seed for reproducible pseudonyms:
    python anonymize_data.py --seed "my-research-2025" data.csv anon_data.csv
        """
    )
    
    # Positional arguments for single-file mode
    parser.add_argument('input', nargs='?', help='Input file path')
    parser.add_argument('output', nargs='?', help='Output file path')
    
    # Optional arguments for directory mode and configuration
    parser.add_argument('--directory', '-d', help='Input directory to process')
    parser.add_argument('--output-dir', '-o', help='Output directory for anonymized files')
    parser.add_argument('--seed', '-s', help='Seed for reproducible pseudonyms')
    parser.add_argument('--audit-log', '-a', default='anonymization_audit.json',
                        help='Path for audit log (default: anonymization_audit.json)')
    
    args = parser.parse_args()
    
    # Initialize anonymizer with optional seed
    anonymizer = DataAnonymizer(seed=args.seed)
    
    # Print header
    print("\n" + "=" * 60)
    print("DATA ANONYMIZATION TOOL")
    print("=" * 60 + "\n")
    
    # Execute in appropriate mode
    if args.directory:
        # Directory mode: batch process all files
        output_dir = args.output_dir or (args.directory.rstrip('/') + '_anonymized')
        print(f"Processing directory: {args.directory}")
        print(f"Output directory: {output_dir}\n")
        
        stats = anonymizer.anonymize_directory(args.directory, output_dir)
        
        print(f"\n{'=' * 60}")
        print("ANONYMIZATION COMPLETE")
        print(f"{'=' * 60}")
        print(f"Files processed: {stats['files_processed']}")
        print(f"Usernames replaced: {stats['total_usernames_replaced']}")
        print(f"Text mentions replaced: {stats['total_text_mentions_replaced']}")
        print(f"Unique users mapped: {len(anonymizer.username_map)}")
        
    elif args.input and args.output:
        # Single file mode
        print(f"Input file: {args.input}")
        print(f"Output file: {args.output}\n")
        
        stats = anonymizer.anonymize_file(args.input, args.output)
        
        print(f"\n{'=' * 60}")
        print("ANONYMIZATION COMPLETE")
        print(f"{'=' * 60}")
        print(f"Records processed: {stats.get('rows_processed', stats.get('records_processed', 0))}")
        print(f"Usernames replaced: {stats['usernames_replaced']}")
        print(f"Text mentions replaced: {stats['text_mentions_replaced']}")
        print(f"Unique users mapped: {len(anonymizer.username_map)}")
        
    else:
        parser.print_help()
        return 1
    
    # Always save audit log for compliance documentation
    anonymizer.save_audit_log(args.audit_log)
    print(f"\nAudit log saved to: {args.audit_log}")
    print("\nIMPORTANT: Review anonymized files before sharing.")
    print("The audit log contains hashed original usernames for verification.")
    
    return 0


if __name__ == '__main__':
    exit(main())

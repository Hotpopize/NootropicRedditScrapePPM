#!/usr/bin/env python3
"""
Data Anonymization Script for Academic Research

Removes or pseudonymizes personally identifiable information (PII) from 
Reddit data exports before sharing for replication purposes.

Features:
- Replaces Reddit usernames with sequential pseudonyms (User_001, User_002, etc.)
- Maintains consistency (same username = same pseudonym across all files)
- Logs all anonymization actions for audit trail
- Supports CSV and JSON formats

Usage:
    python utils/anonymize_data.py input_file.csv output_file.csv
    python utils/anonymize_data.py input_file.json output_file.json
    python utils/anonymize_data.py --directory data/raw/ --output data/anonymized/

Author: Research Tool
Version: 1.0
"""

import argparse
import csv
import json
import re
import os
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Set, Tuple


class DataAnonymizer:
    """Anonymizes research data by replacing usernames with pseudonyms."""
    
    def __init__(self, seed: str = None):
        """
        Initialize the anonymizer.
        
        Args:
            seed: Optional seed for consistent pseudonym generation across runs.
                  Use the same seed to get the same pseudonyms for the same usernames.
        """
        self.username_map: Dict[str, str] = {}
        self.counter = 1
        self.seed = seed
        self.log_entries = []
        
        # Common username patterns to detect
        self.username_patterns = [
            r'/u/([A-Za-z0-9_-]+)',  # Reddit /u/ format
            r'u/([A-Za-z0-9_-]+)',   # Reddit u/ format without leading slash
            r'@([A-Za-z0-9_]+)',     # @ mention format
        ]
        
        # Column names that typically contain usernames
        self.username_columns = [
            'author', 'username', 'user', 'poster', 'commenter',
            'submitted_by', 'created_by', 'posted_by'
        ]
    
    def _generate_pseudonym(self, username: str) -> str:
        """Generate a consistent pseudonym for a username."""
        if username in self.username_map:
            return self.username_map[username]
        
        if self.seed:
            # Use hash for consistent pseudonyms with seed
            hash_input = f"{self.seed}:{username}".encode()
            hash_num = int(hashlib.md5(hash_input).hexdigest()[:8], 16)
            pseudonym = f"User_{hash_num:06d}"
        else:
            # Sequential pseudonyms
            pseudonym = f"User_{self.counter:03d}"
            self.counter += 1
        
        self.username_map[username] = pseudonym
        self.log_entries.append({
            'original_hash': hashlib.sha256(username.encode()).hexdigest()[:16],
            'pseudonym': pseudonym,
            'timestamp': datetime.now().isoformat()
        })
        
        return pseudonym
    
    def _anonymize_text(self, text: str) -> Tuple[str, int]:
        """
        Anonymize usernames within text content.
        
        Returns:
            Tuple of (anonymized_text, count_of_replacements)
        """
        if not isinstance(text, str):
            return text, 0
        
        replacement_count = 0
        result = text
        
        for pattern in self.username_patterns:
            matches = re.findall(pattern, result)
            for username in matches:
                if username and len(username) > 2:  # Skip very short matches
                    pseudonym = self._generate_pseudonym(username)
                    # Replace both with and without prefix
                    result = re.sub(
                        rf'(/u/|u/|@){re.escape(username)}\b',
                        f'\\1{pseudonym}',
                        result
                    )
                    replacement_count += 1
        
        return result, replacement_count
    
    def anonymize_csv(self, input_path: str, output_path: str) -> Dict:
        """
        Anonymize a CSV file.
        
        Returns:
            Statistics about the anonymization process.
        """
        stats = {
            'rows_processed': 0,
            'usernames_replaced': 0,
            'text_mentions_replaced': 0,
            'columns_checked': []
        }
        
        with open(input_path, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames
            stats['columns_checked'] = list(fieldnames)
            
            rows = []
            for row in reader:
                stats['rows_processed'] += 1
                new_row = {}
                
                for col, value in row.items():
                    if col.lower() in self.username_columns:
                        # Direct username column
                        if value and value.strip():
                            new_row[col] = self._generate_pseudonym(value.strip())
                            stats['usernames_replaced'] += 1
                        else:
                            new_row[col] = value
                    else:
                        # Check for mentions in text
                        new_value, count = self._anonymize_text(value)
                        new_row[col] = new_value
                        stats['text_mentions_replaced'] += count
                
                rows.append(new_row)
        
        # Write output
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        with open(output_path, 'w', encoding='utf-8', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        
        return stats
    
    def anonymize_json(self, input_path: str, output_path: str) -> Dict:
        """
        Anonymize a JSON file.
        
        Returns:
            Statistics about the anonymization process.
        """
        stats = {
            'records_processed': 0,
            'usernames_replaced': 0,
            'text_mentions_replaced': 0
        }
        
        with open(input_path, 'r', encoding='utf-8') as infile:
            data = json.load(infile)
        
        def anonymize_value(value, key=''):
            nonlocal stats
            
            if isinstance(value, dict):
                return {k: anonymize_value(v, k) for k, v in value.items()}
            elif isinstance(value, list):
                return [anonymize_value(item) for item in value]
            elif isinstance(value, str):
                if key.lower() in self.username_columns:
                    if value.strip():
                        stats['usernames_replaced'] += 1
                        return self._generate_pseudonym(value.strip())
                else:
                    new_value, count = self._anonymize_text(value)
                    stats['text_mentions_replaced'] += count
                    return new_value
            return value
        
        if isinstance(data, list):
            stats['records_processed'] = len(data)
            anonymized_data = [anonymize_value(record) for record in data]
        else:
            stats['records_processed'] = 1
            anonymized_data = anonymize_value(data)
        
        # Write output
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as outfile:
            json.dump(anonymized_data, outfile, indent=2, ensure_ascii=False)
        
        return stats
    
    def anonymize_file(self, input_path: str, output_path: str) -> Dict:
        """Anonymize a file based on its extension."""
        input_path = Path(input_path)
        
        if input_path.suffix.lower() == '.csv':
            return self.anonymize_csv(str(input_path), output_path)
        elif input_path.suffix.lower() == '.json':
            return self.anonymize_json(str(input_path), output_path)
        else:
            raise ValueError(f"Unsupported file format: {input_path.suffix}")
    
    def anonymize_directory(self, input_dir: str, output_dir: str) -> Dict:
        """Anonymize all CSV and JSON files in a directory."""
        input_path = Path(input_dir)
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        all_stats = {
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
    
    def save_audit_log(self, output_path: str):
        """Save the anonymization audit log (without revealing original usernames)."""
        log_data = {
            'generated_at': datetime.now().isoformat(),
            'total_unique_users': len(self.username_map),
            'seed_used': self.seed is not None,
            'entries': self.log_entries
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, indent=2)
    
    def get_mapping_summary(self) -> str:
        """Get a summary of the username mapping (for verification only, do not share)."""
        return f"Mapped {len(self.username_map)} unique usernames to pseudonyms."


def main():
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
    
    parser.add_argument('input', nargs='?', help='Input file path')
    parser.add_argument('output', nargs='?', help='Output file path')
    parser.add_argument('--directory', '-d', help='Input directory to process')
    parser.add_argument('--output-dir', '-o', help='Output directory for anonymized files')
    parser.add_argument('--seed', '-s', help='Seed for reproducible pseudonyms')
    parser.add_argument('--audit-log', '-a', default='anonymization_audit.json',
                        help='Path for audit log (default: anonymization_audit.json)')
    
    args = parser.parse_args()
    
    anonymizer = DataAnonymizer(seed=args.seed)
    
    print("\n" + "="*60)
    print("DATA ANONYMIZATION TOOL")
    print("="*60 + "\n")
    
    if args.directory:
        # Directory mode
        output_dir = args.output_dir or (args.directory.rstrip('/') + '_anonymized')
        print(f"Processing directory: {args.directory}")
        print(f"Output directory: {output_dir}\n")
        
        stats = anonymizer.anonymize_directory(args.directory, output_dir)
        
        print(f"\n{'='*60}")
        print("ANONYMIZATION COMPLETE")
        print(f"{'='*60}")
        print(f"Files processed: {stats['files_processed']}")
        print(f"Usernames replaced: {stats['total_usernames_replaced']}")
        print(f"Text mentions replaced: {stats['total_text_mentions_replaced']}")
        print(f"Unique users mapped: {len(anonymizer.username_map)}")
        
    elif args.input and args.output:
        # Single file mode
        print(f"Input file: {args.input}")
        print(f"Output file: {args.output}\n")
        
        stats = anonymizer.anonymize_file(args.input, args.output)
        
        print(f"\n{'='*60}")
        print("ANONYMIZATION COMPLETE")
        print(f"{'='*60}")
        print(f"Records processed: {stats.get('rows_processed', stats.get('records_processed', 0))}")
        print(f"Usernames replaced: {stats['usernames_replaced']}")
        print(f"Text mentions replaced: {stats['text_mentions_replaced']}")
        print(f"Unique users mapped: {len(anonymizer.username_map)}")
        
    else:
        parser.print_help()
        return 1
    
    # Save audit log
    anonymizer.save_audit_log(args.audit_log)
    print(f"\nAudit log saved to: {args.audit_log}")
    print("\nIMPORTANT: Review anonymized files before sharing.")
    print("The audit log contains hashed original usernames for verification.")
    
    return 0


if __name__ == '__main__':
    exit(main())

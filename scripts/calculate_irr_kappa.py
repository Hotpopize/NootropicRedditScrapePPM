"""
scripts/calculate_irr_kappa.py
===============================
Computes Inter-Rater Reliability (IRR) using Cohen's Kappa.
Reads Llama 3.1 and Gemma 3 (12B) from the DB, and Researcher codes
directly from outputs/gold_standard_coding_sheet (version 1).xlsb.
"""

import sqlite3
import json
import csv
import os
import sys
import math
import pandas as pd
from pathlib import Path
from sklearn.metrics import cohen_kappa_score

# Ensure UTF-8 output on Windows terminal
if sys.platform.startswith('win'):
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.quote_matcher import clean_gemma_row, DEDUCTIVE_CODES


def get_landis_koch_label(kappa: float) -> str:
    """Return Landis & Koch (1977) agreement level label."""
    if kappa is None or math.isnan(kappa):
        return "N/A"
    if kappa < 0.0:
        return "Poor"
    elif kappa < 0.20:
        return "Slight"
    elif kappa <= 0.40:
        return "Fair"
    elif kappa <= 0.60:
        return "Moderate"
    elif kappa <= 0.80:
        return "Substantial"
    else:
        return "Almost perfect"


def parse_subcodes(raw) -> set:
    """Safely parse subcodes into a set of normalized strings."""
    if not raw:
        return set()
    if isinstance(raw, str):
        try:
            val = json.loads(raw)
        except Exception:
            return set()
    else:
        val = raw
        
    if not isinstance(val, list):
        return set()
        
    out = set()
    for item in val:
        if isinstance(item, dict):
            item = item.get("code", "")
        if isinstance(item, str):
            out.add(item.strip().upper())
    return out


def is_marked(v) -> bool:
    """A cell counts as positive if it's 1 / x / X / true-ish, not 0/blank."""
    if v is None or pd.isna(v):
        return False
    s = str(v).strip().lower()
    return s in ("1", "1.0", "x", "true", "yes")


def load_researcher_data(file_path):
    """
    Reads researcher codes from CSV or XLSB file.
    For each row, collects marked codes into a set.
    """
    _, ext = os.path.splitext(file_path)
    if ext.lower() == '.xlsb':
        df = pd.read_excel(file_path, engine='pyxlsb', dtype=str)
    else:
        try:
            df = pd.read_csv(file_path, encoding="utf-8-sig", dtype=str)
        except Exception:
            df = pd.read_csv(file_path, dtype=str)
            
    df.columns = [c.strip() for c in df.columns]
    
    researcher_data = {}
    for _, row in df.iterrows():
        rid = str(row.get("reddit_id", "")).strip()
        if not rid:
            continue
        codes = set()
        for code in DEDUCTIVE_CODES:
            if code in row and is_marked(row[code]):
                codes.add(code)
        # Ambiguous check done during preflight; map directly to set.
        # NONE rows map to an empty subcode set, not ['NONE']
        researcher_data[rid] = codes
    return researcher_data


def derive_dimensions(codes_set):
    """
    Given a set of codes, returns a new set containing the original codes
    plus any derived dimensions (PUSH, PULL, MOOR-F, MOOR-I, or NONE if empty).
    Derives dimensions once to keep calculations symmetric.
    """
    out = set(codes_set)
    # Deductive dimensions based on prefix
    for code in codes_set:
        for prefix in ("PUSH", "PULL", "MOOR-F", "MOOR-I"):
            if code.startswith(prefix):
                out.add(prefix)
    # Check if any active deductive codes were added/present
    if not any(c for c in out if c in DEDUCTIVE_CODES):
        out.add("NONE")
    return out


def compute_pairwise_irr(name1, name2, rater1_data, rater2_data, min_pos=5):
    """
    Compute per-code Cohen's Kappa and overall mean over estimable codes.
    rater1_data and rater2_data are dicts mapping reddit_id -> set of subcodes + derived dimensions.
    """
    overlap_ids = sorted(list(set(rater1_data.keys()) & set(rater2_data.keys())))
    n_overlap = len(overlap_ids)
    
    if n_overlap == 0:
        results = []
        for code in DEDUCTIVE_CODES:
            category = "OTHER"
            if code.startswith("PUSH"):
                category = "PUSH"
            elif code.startswith("PULL"):
                category = "PULL"
            elif code.startswith("MOOR-F"):
                category = "MOOR-F"
            elif code.startswith("MOOR-I"):
                category = "MOOR-I"
            results.append({
                "code": code,
                "category": category,
                "pos1": 0,
                "pos2": 0,
                "n_pos": 0,
                "kappa": math.nan,
                "status": "zero overlap"
            })
        
        dimensions = ["NONE", "PUSH", "PULL", "MOOR-F", "MOOR-I"]
        dim_results = {}
        for dim_name in dimensions:
            dim_results[dim_name] = {
                "pos1": 0,
                "pos2": 0,
                "kappa": math.nan,
                "status": "zero overlap"
            }
            
        return {
            "rater1": name1,
            "rater2": name2,
            "overlap_count": 0,
            "mean_kappa": math.nan,
            "estimable_count": 0,
            "total_active_codes": len(DEDUCTIVE_CODES),
            "results": results,
            "mean_dimension_kappa": math.nan,
            "dimension_results": dim_results
        }
        
    results = []
    estimable_kappas = []
    
    for code in DEDUCTIVE_CODES:
        vec1 = [1 if code in rater1_data[rid] else 0 for rid in overlap_ids]
        vec2 = [1 if code in rater2_data[rid] else 0 for rid in overlap_ids]
        
        pos1 = sum(vec1)
        pos2 = sum(vec2)
        n_pos = sum(1 for x, y in zip(vec1, vec2) if x or y)
        
        is_estimable = (pos1 >= min_pos) and (pos2 >= min_pos)
        
        if not is_estimable:
            kappa = math.nan
            status = f"low presence (<{min_pos})"
        else:
            if len(set(vec1)) <= 1 or len(set(vec2)) <= 1:
                kappa = math.nan
                status = "constant margin"
            else:
                kappa = cohen_kappa_score(vec1, vec2)
                if kappa is None or str(kappa) == "nan":
                    kappa = 0.0
                    status = "degenerate margins"
                else:
                    status = "estimable"
                
                if not math.isnan(kappa):
                    estimable_kappas.append(kappa)
            
        category = "OTHER"
        if code.startswith("PUSH"):
            category = "PUSH"
        elif code.startswith("PULL"):
            category = "PULL"
        elif code.startswith("MOOR-F"):
            category = "MOOR-F"
        elif code.startswith("MOOR-I"):
            category = "MOOR-I"
            
        results.append({
            "code": code,
            "category": category,
            "pos1": pos1,
            "pos2": pos2,
            "n_pos": n_pos,
            "kappa": kappa,
            "status": status
        })
        
    mean_kappa = sum(estimable_kappas) / len(estimable_kappas) if estimable_kappas else math.nan
    
    # --- Compute dimension-level presence Kappas ---
    dimensions = ["NONE", "PUSH", "PULL", "MOOR-F", "MOOR-I"]
    dim_results = {}
    dim_kappas = []
    
    for dim_name in dimensions:
        vec1 = [1 if dim_name in rater1_data[rid] else 0 for rid in overlap_ids]
        vec2 = [1 if dim_name in rater2_data[rid] else 0 for rid in overlap_ids]
        
        pos1 = sum(vec1)
        pos2 = sum(vec2)
        
        if pos1 < min_pos or pos2 < min_pos:
            kappa = math.nan
            status = f"low presence (<{min_pos})"
        elif len(set(vec1)) <= 1 or len(set(vec2)) <= 1:
            kappa = math.nan
            status = "constant margin"
        else:
            kappa = cohen_kappa_score(vec1, vec2)
            status = "estimable"
            if dim_name != "NONE" and not math.isnan(kappa):
                dim_kappas.append(kappa)
                
        dim_results[dim_name] = {
            "pos1": pos1,
            "pos2": pos2,
            "kappa": kappa,
            "status": status
        }
        
    mean_dim_kappa = sum(dim_kappas) / len(dim_kappas) if dim_kappas else math.nan
    
    return {
        "rater1": name1,
        "rater2": name2,
        "overlap_count": n_overlap,
        "mean_kappa": mean_kappa,
        "estimable_count": len(estimable_kappas),
        "total_active_codes": len(DEDUCTIVE_CODES),
        "results": results,
        "mean_dimension_kappa": mean_dim_kappa,
        "dimension_results": dim_results
    }


def print_reliability_report(comp, min_pos):
    r1 = comp['rater1']
    r2 = comp['rater2']
    print("\n" + "="*80)
    print(f"   COHEN'S KAPPA RELIABILITY REPORT (Threshold: >= {min_pos}): {r1} vs {r2}")
    print("="*80)
    print(f"{'Code':<12} | {'Category':<8} | {r1+'+':<8} | {r2+'+':<8} | {'Kappa':<6} | {'Status'}")
    print("-"*80)
    
    category_order = {"PUSH": 1, "PULL": 2, "MOOR-F": 3, "MOOR-I": 4, "OTHER": 5}
    sorted_results = sorted(comp['results'], key=lambda x: (category_order.get(x["category"], 99), x["code"]))
    
    for res in sorted_results:
        k_str = f"{res['kappa']:.3f}" if not math.isnan(res['kappa']) else "N/A"
        print(f"{res['code']:<12} | {res['category']:<8} | {res['pos1']:<8} | {res['pos2']:<8} | {k_str:<6} | {res['status']}")
        
    print("="*80)
    mean_k = comp['mean_kappa']
    mean_k_str = f"{mean_k:.3f}" if not math.isnan(mean_k) else "N/A"
    mean_dk = comp.get('mean_dimension_kappa', math.nan)
    mean_dk_str = f"{mean_dk:.3f}" if not math.isnan(mean_dk) else "N/A"
    
    print(f"\n--- IRR SUMMARY METRICS ({r1} vs {r2} | Threshold: >= {min_pos}) ---")
    print(f"Overlap items:         {comp['overlap_count']:,}")
    print(f"Codes estimable:       {comp['estimable_count']}/{comp['total_active_codes']}")
    print(f"MEAN Code Cohen's Kappa:    κ = {mean_k_str} ({get_landis_koch_label(mean_k)})")
    print(f"MEAN Dimension Cohen's Kappa (PPM constructs only): κ = {mean_dk_str} ({get_landis_koch_label(mean_dk)})")
    print("Dimension breakdown:")
    for dim_name, d in comp.get('dimension_results', {}).items():
        dk_str = f"{d['kappa']:.3f}" if not math.isnan(d['kappa']) else "N/A"
        print(f"  {dim_name:<6}: {r1}+={d['pos1']:<3}, {r2}+={d['pos2']:<3}, κ={dk_str} ({d['status']})")
    print("="*80)


def main():
    db_path = Path("data/research_data.db")
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}")
        sys.exit(1)

    sheet_path = Path("outputs/gold_standard_coding_sheet.csv")
    if not sheet_path.exists():
        sheet_path = Path("outputs/gold_standard_coding_sheet (version 1).xlsb")
    
    if not sheet_path.exists():
        print(f"Error: Gold standard sheet not found at outputs/gold_standard_coding_sheet.csv")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Check if schema alterations have been run (check clean columns)
    cursor.execute("PRAGMA table_info(coded_data)")
    cols = {row[1] for row in cursor.fetchall()}
    has_clean_cols = 'ppm_subcodes_clean' in cols
    
    # 1. Load llama3.1 coding (excluding quarantined and test posts)
    if has_clean_cols:
        cursor.execute("""
            SELECT c.reddit_id, COALESCE(c.ppm_subcodes_clean, c.ppm_subcodes) as ppm_subcodes 
            FROM coded_data c
            JOIN collected_data cd ON c.reddit_id = cd.reddit_id
            WHERE c.coded_by = 'llama3.1'
              AND c.quarantine_reason IS NULL
        """)
    else:
        cursor.execute("""
            SELECT c.reddit_id, c.ppm_subcodes 
            FROM coded_data c
            JOIN collected_data cd ON c.reddit_id = cd.reddit_id
            WHERE c.coded_by = 'llama3.1'
              AND (cd.data_source IS NULL OR cd.data_source != 'json_endpoint')
        """)
    llama_raw = {r['reddit_id']: parse_subcodes(r['ppm_subcodes']) for r in cursor.fetchall()}
    print(f"Loaded {len(llama_raw):,} Llama 3.1 coded rows.")
    
    # 2. Load gemma4 coding from archive and clean dynamically in-memory
    cursor.execute("""
        SELECT c.reddit_id, c.ppm_subcodes, c.evidence_quotes, cd.text 
        FROM coded_data_gemma4_archive c
        JOIN collected_data cd ON c.reddit_id = cd.reddit_id
        WHERE (cd.data_source IS NULL OR cd.data_source != 'json_endpoint')
    """)
    gemma_raw = {}
    gemma_rows = cursor.fetchall()
    for r in gemma_rows:
        cleaned_codes, reason = clean_gemma_row(r['ppm_subcodes'], r['evidence_quotes'], r['text'])
        gemma_raw[r['reddit_id']] = cleaned_codes if cleaned_codes is not None else set()

    print(f"Loaded {len(gemma_raw):,} Gemma 3 (12B) coded rows (cleaned dynamically).")
    
    # 3. Load human/researcher coding straight from sheet
    print(f"Loading researcher coding from: {sheet_path}")
    researcher_raw = load_researcher_data(sheet_path)
    print(f"Loaded {len(researcher_raw):,} Researcher (Human) coded rows from sheet.")
    
    conn.close()

    # Derive dimensions once for all three raters
    llama_data = {rid: derive_dimensions(codes) for rid, codes in llama_raw.items()}
    gemma_data = {rid: derive_dimensions(codes) for rid, codes in gemma_raw.items()}
    researcher_data = {rid: derive_dimensions(codes) for rid, codes in researcher_raw.items()}
    
    # Run pairwise comparisons under both thresholds (>= 5 and >= 10)
    for min_pos in [5, 10]:
        comparisons = []
        
        # Llama vs Gemma
        res_lg = compute_pairwise_irr("Llama 3.1", "Gemma 3 (12B)", llama_data, gemma_data, min_pos=min_pos)
        if res_lg:
            comparisons.append(res_lg)
            
        # Llama vs Human
        res_lh = compute_pairwise_irr("Llama 3.1", "Researcher", llama_data, researcher_data, min_pos=min_pos)
        if res_lh:
            comparisons.append(res_lh)
            
        # Gemma vs Human
        res_gh = compute_pairwise_irr("Gemma 3 (12B)", "Researcher", gemma_data, researcher_data, min_pos=min_pos)
        if res_gh:
            comparisons.append(res_gh)
            
        if not comparisons:
            continue
            
        # Print reports for each available comparison
        os.makedirs("outputs", exist_ok=True)
        for comp in comparisons:
            print_reliability_report(comp, min_pos)
            
            # Write output to CSV (only for min_pos = 5 to match target output file)
            if min_pos == 5:
                r1 = comp['rater1']
                r2 = comp['rater2']
                csv_path = Path(f"outputs/irr_kappa_{r1.replace(' ', '_').lower()}_vs_{r2.replace(' ', '_').lower()}.csv")
                try:
                    category_order = {"PUSH": 1, "PULL": 2, "MOOR-F": 3, "MOOR-I": 4, "OTHER": 5}
                    sorted_results = sorted(comp['results'], key=lambda x: (category_order.get(x["category"], 99), x["code"]))
                    mean_k = comp['mean_kappa']
                    mean_k_str = f"{mean_k:.3f}" if not math.isnan(mean_k) else "N/A"
                    mean_dk = comp.get('mean_dimension_kappa', math.nan)
                    mean_dk_str = f"{mean_dk:.3f}" if not math.isnan(mean_dk) else "N/A"
                    
                    with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow(["Code", "Category", f"{r1} Positives", f"{r2} Positives", "Cohen's Kappa", "Status"])
                        for res in sorted_results:
                            k_val = f"{res['kappa']:.4f}" if not math.isnan(res['kappa']) else "N/A"
                            writer.writerow([res["code"], res["category"], res["pos1"], res["pos2"], k_val, res["status"]])
                        writer.writerow([])
                        writer.writerow(["Dimension", f"{r1} Positives", f"{r2} Positives", "Cohen's Kappa", "Status"])
                        for dim_name, d in comp.get('dimension_results', {}).items():
                            dk_val = f"{d['kappa']:.4f}" if not math.isnan(d['kappa']) else "N/A"
                            writer.writerow([dim_name, d['pos1'], d['pos2'], dk_val, d['status']])
                        writer.writerow([])
                        writer.writerow(["Overlap Posts Count", comp['overlap_count']])
                        writer.writerow(["Estimable Codes", f"{comp['estimable_count']}/{comp['total_active_codes']}"])
                        writer.writerow(["Mean Code Kappa", mean_k_str])
                        writer.writerow(["Code Agreement Level", get_landis_koch_label(mean_k)])
                        writer.writerow(["Mean Dimension Kappa", mean_dk_str])
                        writer.writerow(["Dimension Agreement Level", get_landis_koch_label(mean_dk)])
                    print(f"[Success] Wrote full Kappa results to: {csv_path}\n")
                except Exception as e:
                    print(f"Error writing to CSV: {e}")


if __name__ == "__main__":
    main()

import re
from rapidfuzz.fuzz import partial_ratio

def normalize_text(text):
    """
    Standardize text: lowercases, maps curly quotes and dashes to ASCII, Collapse whitespace.
    """
    if not text:
        return ""
    text = text.lower()
    
    # Map curly quotes/dashes to ASCII equivalents
    replacements = {
        "’": "'", "‘": "'", "“": '"', "”": '"', "—": "-", "–": "-",
        "…": "...", "æ": "ae", "œ": "oe", "\u201d": '"', "\u201c": '"',
        "\u2019": "'", "\u2018": "'", "\u00e9": "e", "\u00a0": " "
    }
    for k, v in replacements.items():
        text = text.replace(k, v)
        
    # Strip special punctuation except spaces, single quotes, dashes, and periods
    text = re.sub(r"[^a-z0-9\s'\-\.]", " ", text)
    
    # Collapse multiple whitespaces
    return " ".join(text.split())

def verify_quote(quote, body, threshold=90):
    """
    Symmetric quote verification logic.
    Splits the quote by ellipsis and checks if each part is present in the body 
    with a partial ratio >= threshold.
    """
    if not quote:
        return True
    if not body:
        return False
        
    q_norm = normalize_text(quote)
    b_norm = normalize_text(body)
    
    # Split by ellipsis (mapped to '...')
    parts = [p.strip() for p in q_norm.split("...") if p.strip()]
    if not parts:
        return True
        
    for part in parts:
        score = partial_ratio(part, b_norm)
        if score < threshold:
            return False
    return True

def pair_codes_and_quotes(codes, quotes):
    """
    Pairs codes and quotes positionally.
    If quotes list is shorter than codes list, remaining codes are paired with empty strings.
    If quotes is none or empty, all codes are paired with empty strings.
    Returns a list of dicts: [{'code': code, 'quote': quote}, ...]
    """
    pairs = []
    if not codes:
        return pairs
    for i, code in enumerate(codes):
        quote = quotes[i] if quotes and i < len(quotes) else ""
        pairs.append({
            "code": code,
            "quote": quote
        })
    return pairs


DEDUCTIVE_CODES = [
    # PUSH FACTORS (7)
    "PUSH-01", "PUSH-02", "PUSH-03", "PUSH-04", "PUSH-05", "PUSH-06", "PUSH-07",
    # PULL FACTORS (7)
    "PULL-01", "PULL-02", "PULL-03", "PULL-04", "PULL-05", "PULL-06", "PULL-07",
    # MOORING FACILITATORS (4 active)
    "MOOR-F-01", "MOOR-F-02", "MOOR-F-03", "MOOR-F-04",
    # MOORING INHIBITORS (6 active)
    "MOOR-I-01", "MOOR-I-02", "MOOR-I-03", "MOOR-I-04", "MOOR-I-05", "MOOR-I-06"
]

def is_valid_code(code):
    if not code:
        return False
    code_upper = code.strip().upper()
    if code_upper == "NONE" or code_upper == "":
        return True
    if code_upper.startswith("EMER-") or code_upper.startswith("EMERGENT"):
        return True
    return code_upper in DEDUCTIVE_CODES

def normalize_empty_codes(codes_list):
    if not codes_list:
        return []
    clean_list = []
    for c in codes_list:
        if c is None:
            continue
        c_str = str(c).strip()
        c_lower = c_str.lower()
        if c_lower in ("[empty]", "empty", "none", ""):
            continue
        clean_list.append(c_str)
    return clean_list

def clean_gemma_row(subcodes_str, quotes_str, body_text):
    """
    Symmetric Gemma cleaning logic to avoid drift.
    Returns (cleaned_codes_set_or_none, reason_str).
    If the row is quarantined/stripped, returns (None, reason).
    If it is a valid abstention (no active codes or explicitly NONE), returns (set(), "clean (abstention)").
    Otherwise returns (set_of_clean_codes, "clean (has codes: ...)").
    """
    import json
    try:
        codes = json.loads(subcodes_str) if subcodes_str else []
    except Exception:
        codes = []
    try:
        quotes = json.loads(quotes_str) if quotes_str else []
    except Exception:
        quotes = []
        
    # Pair first to preserve positional matching between raw codes and quotes
    paired = pair_codes_and_quotes(codes, quotes)
    
    # Normalize paired codes (remove empty/none codes)
    normalized_paired = []
    for p in paired:
        code = p["code"]
        if code is None:
            continue
        code_str = str(code).strip()
        code_lower = code_str.lower()
        if code_lower in ("[empty]", "empty", "none", ""):
            continue
        p["code"] = code_str
        normalized_paired.append(p)
        
    is_abstention = (len(normalized_paired) == 0)
    
    if is_abstention:
        return set(), "clean (abstention)"
        
    whitelisted = [p for p in normalized_paired if is_valid_code(p["code"])]
    if len(whitelisted) == 0:
        return None, "hallucinated_label"
        
    cleaned_codes = set()
    for p in whitelisted:
        code = p["code"]
        quote = p["quote"]
        if verify_quote(quote, body_text, threshold=90):
            cleaned_codes.add(code.strip().upper())
            
    if len(cleaned_codes) == 0:
        return None, "unverifiable_quote"
        
    return cleaned_codes, f"clean (has codes: {sorted(list(cleaned_codes))})"



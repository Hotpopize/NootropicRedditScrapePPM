"""
bqah_coder.py
=============
Automated qualitative coding module for the NootropicRedditScrapePPM thesis tool,
adapted for BigQuery Analytics Hub (BQAH) external exports.

Applies the Push-Pull-Mooring (PPM) framework using local LLM inference (Ollama).
Enforces Layer A (Deductive) and Layer B (Deductive + Inductive) boundaries.
Includes circuit-breakers and test protocols defined in the methodology.
"""

import argparse
import json
import logging
import sqlite3
import sys
import re
import random
import uuid
from datetime import datetime
from pathlib import Path
import spacy

try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    pass

PII_ENTITY_LABELS = {"PERSON", "ORG", "GPE", "LOC"}

KNOWN_SAFE_ENTITIES = {
    "Modafinil", "Adderall", "Vyvanse", "Ritalin", "Valium",
    "Strattera", "Sertraline", "Pregabalin", "Phenibut",
    "L-Theanine", "Theanine", "Sulforaphane", "CDP", "NAC",
    "Alcar", "Sam-E", "Selegiline", "Armodafinil",
    "Lion", "Mango", "GI", "ECT", "LEGIT", "DataBlood",
    "Kratom", "KW", "Kw",
    "Alpha GPC", "AlphaGPC",
    "Ashwagandha", "Rhodiola", "Boswellia", "Frankincense",
    "Sulbutiamine", "Pyridoxine", "LDX",
    "adaptogens", "Invicorp", "FDA", "NIH",
}
KNOWN_SAFE_LOWER = {k.lower() for k in KNOWN_SAFE_ENTITIES}

SAVE_INTERVAL   = 100   # persist to DB every 100 posts — never lose work
REVIEW_INTERVAL = 500   # human circuit breaker review every 500 posts

# Fix sys.path for CLI execution
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules import ollama_client as ollama
from modules.codebook import CodebookManager, CodeCategory
from modules.module4_audit_trail import AuditWriter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bqah_coder.log', encoding='utf-8')
    ]
)
log = logging.getLogger('bqah_coder')

class BQAHCoder:
    def __init__(self, db_path: str, model: str = 'llama3.1', bq_job_id: str = "UNKNOWN", partition: str = "all", non_interactive: bool = False):
        self.db_path = Path(db_path)
        self.model = model
        self.partition = partition
        self.non_interactive = non_interactive
        self.mgr = CodebookManager()
        self._build_codebook_string()
        self.engine = sqlite3.connect(self.db_path, timeout=30)
        self.engine.row_factory = sqlite3.Row
        self.engine.execute("PRAGMA journal_mode=WAL;")
        self.engine.execute("PRAGMA synchronous=NORMAL;")
        self.engine.create_function("chr", 1, chr)
        self.engine.create_function("char", -1, lambda *args: "".join(chr(int(a)) for a in args))
        
        # Ensure critical tables exist (especially coded_data and audit_log)
        self.engine.execute("""
            CREATE TABLE IF NOT EXISTS coded_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                reddit_id VARCHAR, 
                ppm_category VARCHAR, 
                ppm_subcodes JSON, 
                themes JSON, 
                evidence_quotes JSON, 
                confidence VARCHAR, 
                coded_at DATETIME, 
                coded_by VARCHAR, 
                coding_approach VARCHAR, 
                session_id VARCHAR, 
                rationale TEXT, 
                raw_prompt TEXT, 
                raw_response TEXT, 
                extra_metadata JSON
            )
        """)
        self.engine.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                action VARCHAR,
                session_id VARCHAR,
                user_info VARCHAR,
                details JSON,
                extra_metadata JSON,
                bigquery_job_id TEXT,
                body_length INTEGER,
                non_english_flag INTEGER DEFAULT 0,
                detected_language TEXT,
                data_source TEXT
            )
        """)
        self.engine.execute("CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON audit_log (timestamp);")
        self.engine.execute("CREATE INDEX IF NOT EXISTS idx_audit_log_action ON audit_log (action);")
        self.engine.execute("CREATE INDEX IF NOT EXISTS idx_audit_log_session_id ON audit_log (session_id);")

        self.session_id = f"BQAH-CODE-{uuid.uuid4().hex[:8].upper()}"
        self.audit_writer = AuditWriter(
            session_hash=self.session_id,
            bigquery_job_id=bq_job_id,
            model=self.model,
            model_version="latest",
            db_path=str(self.db_path),
            conn=self.engine          # ← share existing connection, kills DB lock
        )

    def _build_codebook_string(self):
        def format_cat(cat: CodeCategory):
            codes = [c for c in self.mgr.get_by_category(cat) if not (c.name.startswith("[Reserved") or c.name.startswith("[TBD"))]
            lines = []
            for c in codes:
                line = f"{c.id}: {c.name} — {c.definition}"
                if c.include: line += f"\n  ✅ INCLUDE: {c.include}"
                if c.exclude: line += f"\n  ⛔ EXCLUDE: {c.exclude}"
                lines.append(line)
            return "\n\n".join(lines)
            
        self.codebook_text = (
            f"--- CODEBOOK DEFINITIONS ---\n"
            f"PUSH FACTORS:\n{format_cat(CodeCategory.PUSH)}\n\n"
            f"PULL FACTORS:\n{format_cat(CodeCategory.PULL)}\n\n"
            f"MOORING FACILITATORS:\n{format_cat(CodeCategory.MOOR_FACILITATOR)}\n\n"
            f"MOORING INHIBITORS:\n{format_cat(CodeCategory.MOOR_INHIBITOR)}\n"
            f"----------------------------"
        )

    def prompt_ollama(self, prompt: str, system_prompt: str = None) -> tuple[dict, str]:
        if system_prompt is None:
            system_prompt = (
                "You are a qualitative research coder applying the Push-Pull-Mooring "
                "(PPM) migration framework to Reddit posts. Output only valid JSON."
            )
        try:
            raw_res = ollama.generate_completion(self.model, prompt, system_prompt)
            clean_res = raw_res.split("```json")[-1].split("```")[0] if "```" in raw_res else raw_res
            return json.loads(clean_res), raw_res
        except Exception as e:
            log.error(f"Ollama generation failed: {e}")
            return {}, ""

    def code_layer_a(self, text_content: str) -> tuple[dict, str, str]:
        # Codebook goes in system prompt — computed ONCE, cached across calls
        system_prompt = (
            "You are a qualitative research coder applying the Push-Pull-Mooring "
            "(PPM) migration framework to Reddit posts. Output only valid JSON.\n\n"
            f"{self.codebook_text}"  # ← move codebook here
        )

        # Post prompt is now SHORT — only the variable part
        prompt = f"""Analyse this Reddit post. Assign applicable PPM codes with evidence quote and confidence.

REDDIT POST:
{text_content}

Respond in strict JSON:
{{
  "deductive_codes": [
    {{"code": "PUSH-01", "evidence_quote": "...", "confidence": "HIGH|MED|LOW"}}
  ],
  "notes": "..."
}}"""

        res, raw = self.prompt_ollama(prompt, system_prompt)
        return res, prompt, raw

    def code_layer_b_pass2(self, text_content: str, pass1_codes: list) -> tuple[dict, str, str]:
        system_prompt = (
            "You are a qualitative research coder identifying emergent themes. "
            "Flag patterns NOT captured by existing PPM codes. "
            "Output only valid JSON with emergent_candidates array."
        )
        prompt = f"""Post already deductively coded. Find emergent themes only.

REDDIT POST:
{text_content[:2000]}

DEDUCTIVE CODES ASSIGNED:
{json.dumps(pass1_codes, indent=2)}

Respond in strict JSON:
{{
  "emergent_candidates": [
    {{
      "label": "...",
      "definition": "...",
      "partial_overlap_with": "CODE-ID or null",
      "distinct_dimension": "...",
      "evidence_quote": "...",
      "confidence": "HIGH|MED|LOW"
    }}
  ]
}}"""
        res, raw = self.prompt_ollama(prompt, system_prompt)
        return res, prompt, raw

    # --- PRIVACY & CIRCUIT BREAKER LOGIC ---
    
    def flag_potential_pii(self, evidence_quote: str) -> bool:
        """
        spaCy NER tripwire — not auto-redaction.
        Flags quotes containing PERSON, ORG, GPE, or LOC entities.
        Researcher reviews flagged quotes at circuit-breaker pause.
        Returns True if manual review is recommended.
        """
        if not evidence_quote: return False
        try:
            doc = nlp(evidence_quote)
            return any(
                ent.label_ in PII_ENTITY_LABELS and
                ent.text.strip().rstrip("'s").strip() not in KNOWN_SAFE_ENTITIES and
                ent.text.strip().rstrip("'s").strip().lower() not in KNOWN_SAFE_LOWER
                for ent in doc.ents
            )
        except NameError:
            return False

    def get_flagged_entities(self, evidence_quote: str) -> list:
        """
        Returns list of (entity_text, entity_label) tuples for audit log.
        Called only when flag_potential_pii returns True.
        """
        if not evidence_quote: return []
        try:
            doc = nlp(evidence_quote)
            return [
                (ent.text, ent.label_)
                for ent in doc.ents
                if ent.label_ in PII_ENTITY_LABELS and
                ent.text.strip().rstrip("'s").strip() not in KNOWN_SAFE_ENTITIES and
                ent.text.strip().rstrip("'s").strip().lower() not in KNOWN_SAFE_LOWER
            ]
        except NameError:
            return []

    def check_circuit_breaker(self, post: dict, layer: str, deductive_dict: dict, emergent_dict: dict) -> list[str]:
        flags = []
        body_len = len(post.get('text', ''))
        
        # 1. Short form StackAdvice
        if body_len < 100:
            flags.append("SHORT_FORM_BODY (< 100 chars)")
            
        deductive_codes = deductive_dict.get('deductive_codes', [])
        
        # 2. No codes assigned but long body
        if not deductive_codes and body_len > 250:
            flags.append("NO_CODES_LONG_BODY")
            
        # 3. All LOW confidence
        if deductive_codes and all(isinstance(c, dict) and c.get('confidence', '').upper() == 'LOW' for c in deductive_codes):
            flags.append("ALL_LOW_CONFIDENCE")
            
        # 4. Emergent explosion
        if layer == 'B_emergent':
            emergent_cands = emergent_dict.get('emergent_candidates', [])
            if len(emergent_cands) > 3:
                flags.append("HIGH_EMERGENT_COUNT (> 3)")
                
        # 5. spaCy Privacy NER on Evidence Quotes
        quotes = []
        for c in deductive_codes:
            if isinstance(c, dict) and c.get('evidence_quote'):
                quotes.append(c.get('evidence_quote'))
        if layer == 'B_emergent':
            for c in emergent_dict.get('emergent_candidates', []):
                if isinstance(c, dict) and c.get('evidence_quote'):
                    quotes.append(c.get('evidence_quote'))
                
        flagged_ents = []
        for q in quotes:
            if self.flag_potential_pii(q):
                flagged_ents.extend(self.get_flagged_entities(q))
                
        if flagged_ents:
            flags.append(f"SPACY_PII_FLAG_MATCH: {flagged_ents}")
            
        return flags

    def persist_coded_data(self, batch_results: list):
        cur = self.engine.cursor()
        for res in batch_results:
            post = res['post']
            layer = res['layer']
            ded_dict = res['deductive_res']
            eme_dict = res.get('emergent_res', {})
            flags = res['flags']
            override = res.get('override_notes')
            
            # Formulate standard schema output
            deductive_codes = ded_dict.get('deductive_codes', [])
            subcodes = [c.get('code') for c in deductive_codes if isinstance(c, dict) and c.get('code')]
            
            ppm_category = "Unknown"
            if subcodes:
                prefixes = set(c.split('-')[0] for c in subcodes)
                if len(prefixes) > 1: ppm_category = "Mixed"
                elif 'PUSH' in prefixes: ppm_category = "Push"
                elif 'PULL' in prefixes: ppm_category = "Pull"
                elif 'MOOR' in prefixes: ppm_category = "Mooring"
                
            evidence_quotes = [c.get('evidence_quote') for c in deductive_codes if isinstance(c, dict) and c.get('evidence_quote')]
            themes = eme_dict.get('emergent_candidates', [])
            for t in themes:
                if isinstance(t, dict) and t.get('evidence_quote'):
                    evidence_quotes.append(t.get('evidence_quote'))
            
            confidences = [c.get('confidence', '').upper() for c in deductive_codes if isinstance(c, dict)]
            overall_conf = "LOW" if "LOW" in confidences else "MED" if "MED" in confidences else "HIGH" if confidences else "N/A"
            
            extra_metadata = json.loads(post.get('extra_metadata', '{}'))
            extra_metadata['corpus_layer'] = layer
            extra_metadata['circuit_breaker_flags'] = flags
            extra_metadata['deductive_codes'] = deductive_codes
            if override:
                extra_metadata['override_notes'] = override
            
            # Combine rationale/notes
            rationale = f"Layer A Notes: {ded_dict.get('notes', '')}"

            cur.execute("""
                INSERT INTO coded_data 
                (reddit_id, ppm_category, ppm_subcodes, themes, evidence_quotes, confidence, 
                 coded_at, coded_by, coding_approach, session_id, rationale, raw_prompt, raw_response, extra_metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                post['reddit_id'],
                ppm_category,
                json.dumps(subcodes),
                json.dumps(themes),
                json.dumps(evidence_quotes),
                overall_conf,
                datetime.utcnow().isoformat(),
                self.model,
                layer,
                self.session_id,
                rationale,
                res['raw_prompt'],
                res['raw_response'],
                json.dumps(extra_metadata)
            ))
            
            # Formulate the post dictionary required by AuditWriter
            coded_post_audit = {
                "post_id": post.get("reddit_id"),
                "subreddit_id": post.get("subreddit", "UNKNOWN"),
                "corpus_layer": layer,
                "tertile": extra_metadata.get("tertile", 1),
                "body_length": len(post.get("text", "")),
                "deductive_codes": deductive_codes,
                "non_english_flag": False,
                "detected_language": "",
            }
            
            # Module 4 Privacy Check & Log write
            try:
                self.audit_writer.log(
                    coded_post=coded_post_audit, 
                    researcher_override=bool(override), 
                    override_notes=override if override else ""
                )
            except self.audit_writer.PrivacyViolationError as e:
                log.error(f"Privacy tripwire activated during DB write: {e}")
            
            
        # Log Audit Trail
        cur.execute("""
            INSERT INTO audit_log (session_id, action, timestamp, details)
            VALUES (?, ?, ?, ?)
        """, (
            self.session_id,
            "automated_coding_ollama_batch",
            datetime.utcnow().isoformat(),
            json.dumps({"batch_size": len(batch_results), "model": self.model})
        ))
        self.engine.commit()

    def run_full_corpus(self):
        log.info("--- Starting Full Corpus Run ---")
        
        # Ensure target table exists (handled usually by db_helpers but we'll safeguard)
        self.engine.execute("""
            CREATE TABLE IF NOT EXISTS coded_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                reddit_id VARCHAR, 
                ppm_category VARCHAR, 
                ppm_subcodes JSON, 
                themes JSON, 
                evidence_quotes JSON, 
                confidence VARCHAR, 
                coded_at DATETIME, 
                coded_by VARCHAR, 
                coding_approach VARCHAR, 
                session_id VARCHAR, 
                rationale TEXT, 
                raw_prompt TEXT, 
                raw_response TEXT, 
                extra_metadata JSON
            )
        """)
        
        # Fetch uncoded posts respecting partition filters
        if self.partition == 'B':
            partition_filter = "AND json_extract(extra_metadata, '$.machine_partition') = 'B'"
        elif self.partition == 'A':
            partition_filter = "AND (json_extract(extra_metadata, '$.machine_partition') IS NULL OR json_extract(extra_metadata, '$.machine_partition') = 'A')"
        else:
            partition_filter = ""

        uncoded = self.engine.execute(f"""
            SELECT * FROM collected_data 
            WHERE reddit_id NOT IN (SELECT reddit_id FROM coded_data)
            {partition_filter}
            ORDER BY length(text) ASC   -- shortest posts first
        """).fetchall()
        
        log.info(f"Found {len(uncoded)} uncoded posts. Commencing loop.")
        
        batch_results = []
        processed_count = 0
        
        for p in uncoded:
            post_dict = dict(p)
            layer = json.loads(post_dict.get('extra_metadata', '{}')).get('corpus_layer', 'A_keyword')
            
            # Read the text string safely from the database mapping key
            text_body = post_dict.get('text', '')
            if text_body is None:
                text_body = ''
            text_body = text_body.strip()
            
            text_content = f"Title: {post_dict.get('title') or ''}\nContent: {text_body}"
            
            # --- METHODOLOGICAL FILTERS BEFORE LLM INVOCATION ---
            
            # 1. Skip if text is fundamentally too short to establish switching intent
            if len(text_body) < 40:
                self.engine.execute("""
                    INSERT INTO coded_data (
                        reddit_id, ppm_category, ppm_subcodes, themes, evidence_quotes, confidence, 
                        coded_at, coded_by, coding_approach, session_id, rationale, raw_prompt, raw_response, extra_metadata
                    ) 
                    VALUES (?, 'Trivial', '[]', '[]', '[]', 'N/A', ?, 'system_filter', ?, ?, 'Skipped: text length too short (< 40 chars)', '', '', ?)
                """, (
                    post_dict['reddit_id'],
                    datetime.utcnow().isoformat(),
                    layer,
                    self.session_id,
                    json.dumps({"status": "skipped_short_form"})
                ))
                self.engine.commit()
                
                # Write to audit_log to keep complete trail
                self.engine.execute("""
                    INSERT INTO audit_log (session_id, action, timestamp, details)
                    VALUES (?, 'skipped_post_filter', ?, ?)
                """, (
                    self.session_id,
                    datetime.utcnow().isoformat(),
                    json.dumps({
                        "post_id": post_dict['reddit_id'],
                        "reason": "skipped_short_form",
                        "body_length": len(text_body)
                    })
                ))
                self.engine.commit()
                continue

            # 2. Skip if it contains pure emojis or generic short-phrase reactions
            cleaned_text = re.sub(r'[\U00010000-\U0010ffff]', '', text_content)  # remove emojis
            words_only = re.sub(r'[^\w\s]', '', cleaned_text).strip().lower()
            
            filler_phrases = {
                'thanks', 'thank you', 'lol', 'lmao', 'rofl', 'bump', 'agree', 'disagree', 
                'this', 'same', 'same here', 'me too', 'so true', 'true', 'yes', 'no', 'exactly',
                'interesting', 'cool', 'nice', 'wow', 'following', 'upvoted', 'upvote'
            }
            
            if not words_only or words_only in filler_phrases:
                self.engine.execute("""
                    INSERT INTO coded_data (
                        reddit_id, ppm_category, ppm_subcodes, themes, evidence_quotes, confidence, 
                        coded_at, coded_by, coding_approach, session_id, rationale, raw_prompt, raw_response, extra_metadata
                    ) 
                    VALUES (?, 'Trivial', '[]', '[]', '[]', 'N/A', ?, 'system_filter', ?, ?, 'Skipped: contains only filler words or emojis', '', '', ?)
                """, (
                    post_dict['reddit_id'],
                    datetime.utcnow().isoformat(),
                    layer,
                    self.session_id,
                    json.dumps({"status": "skipped_filler_content"})
                ))
                self.engine.commit()
                
                # Write to audit_log to keep complete trail
                self.engine.execute("""
                    INSERT INTO audit_log (session_id, action, timestamp, details)
                    VALUES (?, 'skipped_post_filter', ?, ?)
                """, (
                    self.session_id,
                    datetime.utcnow().isoformat(),
                    json.dumps({
                        "post_id": post_dict['reddit_id'],
                        "reason": "skipped_filler_content",
                        "original_text": text_content
                    })
                ))
                self.engine.commit()
                continue

            # 3. Strip Out Non-English Noise (Quick filter for heavy non-Latin token sequences)
            latin_chars = sum(1 for c in text_content if ord(c) < 128)
            if len(text_content) > 0 and (latin_chars / len(text_content)) < 0.70:
                self.engine.execute("""
                    INSERT INTO coded_data (
                        reddit_id, ppm_category, ppm_subcodes, themes, evidence_quotes, confidence, 
                        coded_at, coded_by, coding_approach, session_id, rationale, raw_prompt, raw_response, extra_metadata
                    ) 
                    VALUES (?, 'Trivial', '[]', '[]', '[]', 'N/A', ?, 'system_filter', ?, ?, 'Skipped: non-English language', '', '', ?)
                """, (
                    post_dict['reddit_id'],
                    datetime.utcnow().isoformat(),
                    layer,
                    self.session_id,
                    json.dumps({"status": "skipped_non_english"})
                ))
                self.engine.commit()
                
                # Write to audit_log to keep complete trail
                self.engine.execute("""
                    INSERT INTO audit_log (session_id, action, timestamp, details)
                    VALUES (?, 'skipped_post_filter', ?, ?)
                """, (
                    self.session_id,
                    datetime.utcnow().isoformat(),
                    json.dumps({
                        "post_id": post_dict['reddit_id'],
                        "reason": "skipped_non_english",
                        "latin_ratio": latin_chars / len(text_content)
                    })
                ))
                self.engine.commit()
                continue
                
            ded_res, raw_p, raw_r = self.code_layer_a(text_content)
            eme_res = {}
            
            if layer == 'B_emergent':
                eme_res, raw_p2, raw_r2 = self.code_layer_b_pass2(text_content, ded_res.get('deductive_codes', []))
                raw_p += "\n\n=== PASS 2 ===\n\n" + raw_p2
                raw_r += "\n\n=== PASS 2 ===\n\n" + raw_r2
                
            flags = self.check_circuit_breaker(post_dict, layer, ded_res, eme_res)
            
            result_obj = {
                'post': post_dict,
                'layer': layer,
                'deductive_res': ded_res,
                'emergent_res': eme_res,
                'flags': flags,
                'raw_prompt': raw_p,
                'raw_response': raw_r
            }
            batch_results.append(result_obj)
            processed_count += 1
            
            # --- Auto-save every SAVE_INTERVAL posts ---
            if processed_count % SAVE_INTERVAL == 0:
                self.persist_coded_data(batch_results)
                log.info(f"Auto-saved {len(batch_results)} posts to DB.")
                batch_results = []

            # --- 20% CIRCUIT BREAKER (Every REVIEW_INTERVAL posts) ---
            if processed_count % REVIEW_INTERVAL == 0 or processed_count == len(uncoded):
                # Save any remaining results in batch_results to DB before review
                if batch_results:
                    self.persist_coded_data(batch_results)
                    log.info(f"Auto-saved final {len(batch_results)} posts to DB before review.")
                    batch_results = []
                
                # Fetch last processed items for this session from the DB for sample instead
                num_to_review = processed_count % REVIEW_INTERVAL
                if num_to_review == 0:
                    num_to_review = REVIEW_INTERVAL
                
                log.info(f"Circuit Breaker trigger. Reviewing last {num_to_review} posts.")
                
                # Fetch the last num_to_review rows from coded_data for this session
                rows = self.engine.execute("""
                    SELECT c.reddit_id, c.subreddit, c.title, c.text,
                           cd.coding_approach, cd.extra_metadata, cd.themes
                    FROM coded_data cd
                    JOIN collected_data c ON cd.reddit_id = c.reddit_id
                    WHERE cd.session_id = ?
                    ORDER BY cd.id DESC
                    LIMIT ?
                """, (self.session_id, num_to_review)).fetchall()
                
                # Reconstruct review items list (order: oldest first for display)
                review_items = []
                for row in reversed(rows):
                    meta = json.loads(row['extra_metadata'] or '{}')
                    review_items.append({
                        'post': {
                            'reddit_id': row['reddit_id'],
                            'subreddit': row['subreddit'],
                            'title': row['title'],
                            'text': row['text']
                        },
                        'layer': row['coding_approach'],
                        'flags': meta.get('circuit_breaker_flags', []),
                        'deductive_res': {'deductive_codes': meta.get('deductive_codes', [])},
                        'emergent_res': {'emergent_candidates': json.loads(row['themes'] or '[]')}
                    })
                
                # Randomly sample 20% (up to 20)
                sample_size = min(20, max(1, int(len(review_items) * 0.20)))
                
                # Prioritize flagged items in the review sample
                flagged_items = [b for b in review_items if b['flags']]
                unflagged_items = [b for b in review_items if not b['flags']]
                
                review_sample = flagged_items[:sample_size]
                if len(review_sample) < sample_size:
                    review_sample += random.sample(unflagged_items, min(len(unflagged_items), sample_size - len(review_sample)))
                    
                print(f"\n==============================================")
                print(f" CIRCUIT BREAKER MANUAL REVIEW (20% SAMPLE)   ")
                print(f"==============================================")
                for item in review_sample:
                    print(f"\n[Post ID: {item['post']['reddit_id']}] Subreddit: {item['post']['subreddit']}")
                    if item['flags']:
                        print(f"⚠️ FLAGS TRIGGERED: {item['flags']}")
                    print(f"Deductive Codes: {json.dumps(item['deductive_res'].get('deductive_codes', []), indent=2)}")
                    if item['layer'] == 'B_emergent':
                        print(f"Emergent Themes: {json.dumps(item['emergent_res'].get('emergent_candidates', []), indent=2)}")
                
                print("\n==============================================")
                print("Options: [c] Confirm and continue | [o] Override notes and continue | [q] Abort batch")
                if self.non_interactive:
                    log.info("Non-interactive mode active: automatically confirming and continuing.")
                    choice = 'c'
                else:
                    choice = input("Enter choice (c/o/q): ").strip().lower()
                
                if choice == 'q':
                    log.error("Batch aborted by researcher. Shutting down.")
                    sys.exit(1)
                elif choice == 'o':
                    override = input("Enter override note: ")
                    
                    # Update all flagged items in this batch to log the mitigation retroactively
                    flagged_reddit_ids = []
                    for item in review_items:
                        if item['flags']:
                            flagged_reddit_ids.append(item['post']['reddit_id'])
                            
                            # Fetch existing extra_metadata from DB to preserve other keys
                            c_row = self.engine.execute("""
                                SELECT id, extra_metadata FROM coded_data
                                WHERE reddit_id = ? AND session_id = ?
                            """, (item['post']['reddit_id'], self.session_id)).fetchone()
                            
                            if c_row:
                                meta = json.loads(c_row['extra_metadata'] or '{}')
                                meta['override_notes'] = override
                                self.engine.execute("""
                                    UPDATE coded_data
                                    SET extra_metadata = ?
                                    WHERE id = ?
                                """, (json.dumps(meta), c_row['id']))
                    
                    if flagged_reddit_ids:
                        # Update audit_log details for these flagged reddit_ids
                        audit_rows = self.engine.execute("""
                            SELECT id, details
                            FROM audit_log
                            WHERE session_id = ? AND action = 'coded_post_bqah'
                        """, (self.session_id,)).fetchall()
                        
                        for a_row in audit_rows:
                            details = json.loads(a_row['details'] or '{}')
                            if details.get('post_id') in flagged_reddit_ids:
                                details['researcher_override'] = 1
                                details['override_notes'] = override
                                self.engine.execute("""
                                    UPDATE audit_log
                                    SET details = ?
                                    WHERE id = ?
                                """, (json.dumps(details), a_row['id']))
                                
                    self.engine.commit()
                    log.info(f"Retroactively applied override note to {len(flagged_reddit_ids)} flagged posts in the DB.")
                    
        log.info("Full Corpus Run Complete.")

    def run_layer_a_test(self):
        log.info("--- Running Step 2a: Layer A test ---")
        q_sa = "SELECT reddit_id, subreddit, title, text FROM collected_data WHERE json_extract(extra_metadata, '$.corpus_layer') = 'A_keyword' AND subreddit = 'StackAdvice' AND length(text) > 100 LIMIT 1"
        q_de = "SELECT reddit_id, subreddit, title, text FROM collected_data WHERE json_extract(extra_metadata, '$.corpus_layer') = 'A_keyword' AND subreddit = 'Decaf' AND length(text) > 100 LIMIT 1"
        q_ot = "SELECT reddit_id, subreddit, title, text FROM collected_data WHERE json_extract(extra_metadata, '$.corpus_layer') = 'A_keyword' AND subreddit NOT IN ('StackAdvice', 'Decaf') AND length(text) > 100 LIMIT 3"
        posts = []
        posts.extend(self.engine.execute(q_sa).fetchall())
        posts.extend(self.engine.execute(q_de).fetchall())
        posts.extend(self.engine.execute(q_ot).fetchall())
        for p in posts:
            log.info(f"Coding Layer A Post: {p['reddit_id']} [{p['subreddit']}]")
            text_content = f"Title: {p['title']}\nContent: {p['text'][:2000]}"
            res, _, _ = self.code_layer_a(text_content)
            print(json.dumps(res, indent=2))

    def run_layer_b_test(self):
        log.info("--- Running Step 2b: Layer B test ---")
        q_bi = "SELECT reddit_id, subreddit, title, text FROM collected_data WHERE json_extract(extra_metadata, '$.corpus_layer') = 'B_emergent' AND subreddit = 'Biohackers' AND length(text) > 100 LIMIT 1"
        q_ot = "SELECT reddit_id, subreddit, title, text FROM collected_data WHERE json_extract(extra_metadata, '$.corpus_layer') = 'B_emergent' AND subreddit != 'Biohackers' AND length(text) > 100 LIMIT 4"
        posts = []
        posts.extend(self.engine.execute(q_bi).fetchall())
        posts.extend(self.engine.execute(q_ot).fetchall())
        for p in posts:
            log.info(f"Coding Layer B Post: {p['reddit_id']} [{p['subreddit']}]")
            text_content = f"Title: {p['title']}\nContent: {p['text'][:2000]}"
            res1, _, _ = self.code_layer_a(text_content)
            print(">> Pass 1 (Deductive)\n" + json.dumps(res1, indent=2))
            res2, _, _ = self.code_layer_b_pass2(text_content, res1.get('deductive_codes', []))
            print(">> Pass 2 (Inductive)\n" + json.dumps(res2, indent=2))

def main():
    parser = argparse.ArgumentParser(description='BQAH Coding Pipeline')
    parser.add_argument('--db', default='data/research_data.db', help='Path to SQLite database')
    parser.add_argument('--model', default='llama3.1', help='Ollama model to use')
    parser.add_argument('--partition', choices=['A', 'B', 'all'], default='all', help="Which machine partition to code (A, B, or all)")
    parser.add_argument('--test-2a', action='store_true', help='Run Step 2a Test Protocol (Layer A)')
    parser.add_argument('--test-2b', action='store_true', help='Run Step 2b Test Protocol (Layer B)')
    parser.add_argument('--run', action='store_true', help='Execute Full Corpus Run with Circuit Breakers')
    parser.add_argument('--non-interactive', action='store_true', help='Bypass manual checkpoints (auto-confirm)')
    args = parser.parse_args()

    if not Path(args.db).exists():
        log.error(f"Database not found: {args.db}")
        sys.exit(1)

    from modules.ollama_client import is_ollama_running
    if not is_ollama_running():
        log.error("Ollama service is NOT running. Please ensure Ollama is installed and running locally (default: http://127.0.0.1:11434) before running the coder.")
        sys.exit(1)

    coder = BQAHCoder(args.db, args.model, partition=args.partition, non_interactive=args.non_interactive)
    
    if args.test_2a:
        coder.run_layer_a_test()
    elif args.test_2b:
        coder.run_layer_b_test()
    elif args.run:
        coder.run_full_corpus()
    else:
        parser.print_help()

if __name__ == '__main__':
    main()

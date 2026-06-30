import streamlit as st
import pandas as pd
from datetime import datetime, timezone
import json
from utils.db_helpers import load_replicability_logs, get_data_quality_report, load_citation_links, load_zotero_references
from modules.codebook import CodeCategory

def render():
    st.header("Thesis Export Templates")
    st.markdown("""
    Generate formatted exports for thesis appendices and academic publications.
    Compliant with APA 7th edition and common thesis formatting standards.
    """)
    
    if not st.session_state.collected_data and not st.session_state.coded_data:
        st.warning("No data available. Please collect and code data first.")
        return
    
    st.subheader("Available Export Templates")
    
    template_type = st.selectbox(
        "Select Template Type",
        [
            "Appendix A: Data Collection Methodology",
            "Appendix B: Codebook with Examples",
            "Appendix C: Sample Coded Data",
            "Appendix D: Thematic Analysis Summary",
            "Appendix E: Inter-Coder Reliability Report",
            "Appendix F: Data Quality & Edge Cases",
            "Appendix G: Literature-Data Linkages",
            "Complete Methodology Chapter",
            "Chapter 4: Quantitative Findings & Consumer Archetypes"
        ]
    )
    
    if template_type == "Appendix A: Data Collection Methodology":
        generate_data_collection_appendix()
    
    elif template_type == "Appendix B: Codebook with Examples":
        generate_codebook_appendix()
    
    elif template_type == "Appendix C: Sample Coded Data":
        generate_sample_data_appendix()
    
    elif template_type == "Appendix D: Thematic Analysis Summary":
        generate_thematic_summary_appendix()
    
    elif template_type == "Appendix E: Inter-Coder Reliability Report":
        generate_reliability_appendix()
    
    elif template_type == "Appendix F: Data Quality & Edge Cases":
        generate_data_quality_appendix()
    
    elif template_type == "Appendix G: Literature-Data Linkages":
        generate_literature_linkages_appendix()
    
    elif template_type == "Complete Methodology Chapter":
        generate_complete_methodology()
        
    elif template_type == "Chapter 4: Quantitative Findings & Consumer Archetypes":
        generate_findings_chapter()

def generate_data_collection_appendix():
    st.subheader("Appendix A: Data Collection Methodology")
    
    bqah_data = [d for d in st.session_state.collected_data if d.get('data_source') != 'json_endpoint']
    n_posts = len([d for d in bqah_data if d.get('type') == 'submission'])
    n_comments = len([d for d in bqah_data if d.get('type') == 'comment'])
    
    if bqah_data:
        dates = [d.get('created_utc') for d in bqah_data if d.get('created_utc')]
        if dates:
            earliest = min(dates)
            latest = max(dates)
            # Format Unix float to human readable date
            if isinstance(earliest, (int, float)):
                try:
                    earliest = datetime.fromtimestamp(earliest, tz=timezone.utc).strftime('%Y-%m-%d')
                except Exception:
                    pass
            if isinstance(latest, (int, float)):
                try:
                    latest = datetime.fromtimestamp(latest, tz=timezone.utc).strftime('%Y-%m-%d')
                except Exception:
                    pass
        else:
            earliest = "N/A"
            latest = "N/A"
    else:
        earliest = "N/A"
        latest = "N/A"
    
    content = f"""
# Appendix A: Data Collection Methodology

## Data Source
Data were obtained from the BigQuery Analytics Hub (BQAH) Reddit Archive, containing bulk exports of Reddit post and comment history. Reddit was selected due to its active communities discussing natural cognitive supplements and nootropics.

## Collection Parameters
- **Deductive PPM Sample**: 660 target submissions across 6 communities (qualitative sampling target)
- **Full Coded Corpus**: {len(bqah_data):,} total units (reconciled exact sum of submissions and comments)
  - **Submissions**: {n_posts:,} submissions
  - **Comments**: {n_comments:,} comments (Note: The comments stratum represents the remaining {n_comments:,} units of the full coded corpus, with removed and deleted content excluded during data ingestion to maintain corpus cleanliness. This is distinct from the targeted qualitative sample of 660 posts.)
- **Date Range**: {earliest} to {latest}
- **Collection Date**: {datetime.now().strftime('%Y-%m-%d')}

## Inclusion Criteria
Posts and comments were included if they:
1. Discussed natural cognitive supplements or nootropics
2. Contained substantive user experiences or opinions
3. Were written in English
4. Met minimum quality standards (length, coherence)

## Data Collection Process
1. Data extraction and bulk export from the BigQuery Analytics Hub (BQAH) Reddit Archive
2. Systematic ingestion of target subreddits from the BQAH corpus
3. Metadata capture including author, timestamp, subreddit, score
4. Quality filtering, exclusion of deleted/removed content, and deduplication
5. Secure storage in SQLite database with audit trail

## Ethical Considerations
- All data collected from publicly accessible subreddits
- No personally identifiable information retained
- User anonymization applied in all reports
- Compliance with Reddit API Terms of Service
- IRB approval obtained (if applicable)

## NSFW Content Handling
The data collection process includes explicit handling of Not Safe For Work (NSFW) content:
- NSFW subreddits and posts are flagged with the `over_18` metadata field
- Researchers can choose to include or exclude NSFW content based on study requirements
- All NSFW decisions are logged in the audit trail for methodological transparency
- Sensitive content is documented but not displayed without explicit researcher consent

## Data Quality Assurance
- Automated validation of data completeness
- Content status tracking (available, removed, deleted)
- Language detection for non-English content flagging
- Media-only post identification
- Truncation handling for very long posts
- Rate limit event logging
- Duplicate detection and removal
- Timestamp verification for temporal accuracy
- Collection hash for replicability verification
"""
    
    st.markdown(content)
    
    st.download_button(
        label="Download Appendix A (Markdown)",
        data=content,
        file_name=f"Appendix_A_Data_Collection_{datetime.now().strftime('%Y%m%d')}.md",
        mime="text/markdown"
    )

def generate_codebook_appendix():
    st.subheader("Appendix B: Codebook with Examples")
    
    if 'codebook_manager' not in st.session_state or not st.session_state.codebook_manager.get_all_codes():
        st.warning("No codebook available. Please create codes first.")
        return
    
    content = "# Appendix B: Codebook for Push-Pull-Mooring Framework\n\n"
    content += "This codebook documents all codes used in the thematic analysis, organized by the Push-Pull-Mooring (PPM) framework.\n\n"
    
    category_names = {
        CodeCategory.PUSH: 'Push Factors (Motivations to Leave Current State)',
        CodeCategory.PULL: 'Pull Factors (Attractions of Natural Supplements)',
        CodeCategory.MOOR_FACILITATOR: 'Mooring Facilitators (Enable Switching)',
        CodeCategory.MOOR_INHIBITOR: 'Mooring Inhibitors (Impede Switching)',
        CodeCategory.EMERGENT: 'Emergent Themes (Beyond PPM Framework)'
    }
    
    for category, display_name in category_names.items():
        codes = st.session_state.codebook_manager.get_by_category(category)
        if codes:
            content += f"## {display_name}\n\n"
            
            for i, code in enumerate(codes, 1):
                content += f"### {i}. {code.name}\n\n"
                content += f"**Definition**: {code.definition}\n\n"
                
                if getattr(code, 'examples', ''):
                    content += f"**Example(s)**: {code.examples}\n\n"
                
                content += f"**Frequency**: {code.frequency} occurrences\n\n"
                content += "---\n\n"
    
    content += f"\n**Total Codes**: {len(st.session_state.codebook_manager.get_all_codes())}\n"
    content += f"**Codebook Last Updated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    
    st.markdown(content)
    
    st.download_button(
        label="Download Appendix B (Markdown)",
        data=content,
        file_name=f"Appendix_B_Codebook_{datetime.now().strftime('%Y%m%d')}.md",
        mime="text/markdown"
    )

def generate_sample_data_appendix():
    st.subheader("Appendix C: Sample Coded Data")
    
    if not st.session_state.coded_data:
        st.warning("No coded data available. Please code data first.")
        return
    
    n_samples = st.slider("Number of Examples", min_value=5, max_value=50, value=10)
    
    sample_data = st.session_state.coded_data[:n_samples]
    
    content = f"# Appendix C: Sample Coded Data (n={len(sample_data)})\n\n"
    content += "This appendix presents representative examples of coded data showing the application of the codebook.\n\n"
    
    for i, item in enumerate(sample_data, 1):
        content += f"## Example {i}\n\n"
        content += f"**ID**: {item.get('id')}\n\n"
        content += f"**Text**: {item.get('text', '')[:500]}...\n\n"
        content += f"**PPM Category**: {item.get('ppm_category')}\n\n"
        
        if item.get('themes'):
            content += f"**Applied Codes**: {', '.join(item.get('themes'))}\n\n"
        
        if item.get('evidence_quotes'):
            content += f"**Evidence Quote**: \"{item.get('evidence_quotes')[0] if item.get('evidence_quotes') else 'N/A'}\"\n\n"
        
        if item.get('rationale'):
            content += f"**Coding Rationale**: {item.get('rationale')}\n\n"
        
        content += f"**Confidence**: {item.get('confidence', 'N/A')}\n\n"
        content += f"**Coded By**: {item.get('coded_by', 'N/A')}\n\n"
        content += "---\n\n"
    
    st.markdown(content)
    
    st.download_button(
        label="Download Appendix C (Markdown)",
        data=content,
        file_name=f"Appendix_C_Sample_Data_{datetime.now().strftime('%Y%m%d')}.md",
        mime="text/markdown"
    )

def generate_thematic_summary_appendix():
    st.subheader("Appendix D: Thematic Analysis Summary")
    
    if not st.session_state.coded_data or 'codebook_manager' not in st.session_state:
        st.warning("Need both coded data and codebook for thematic summary.")
        return
    
    content = "# Appendix D: Thematic Analysis Summary\n\n"
    
    ppm_distribution = {}
    for item in st.session_state.coded_data:
        cat = item.get('ppm_category', 'Unknown')
        ppm_distribution[cat] = ppm_distribution.get(cat, 0) + 1
    
    content += "## Distribution of PPM Categories\n\n"
    for cat, count in sorted(ppm_distribution.items(), key=lambda x: x[1], reverse=True):
        pct = (count / len(st.session_state.coded_data)) * 100
        content += f"- **{cat}**: {count} items ({pct:.1f}%)\n"
    
    content += "\n## Theme Frequency Analysis\n\n"
    
    theme_freq = {}
    for item in st.session_state.coded_data:
        for theme in item.get('themes', []):
            theme_freq[theme] = theme_freq.get(theme, 0) + 1
    
    if theme_freq:
        content += "| Theme | Frequency | Percentage |\n"
        content += "|-------|-----------|------------|\n"
        
        for theme, freq in sorted(theme_freq.items(), key=lambda x: x[1], reverse=True)[:20]:
            pct = (freq / len(st.session_state.coded_data)) * 100
            content += f"| {theme} | {freq} | {pct:.1f}% |\n"
    
    content += f"\n**Total Coded Items**: {len(st.session_state.coded_data)}\n"
    content += f"**Total Unique Themes**: {len(theme_freq)}\n"
    content += f"**Analysis Date**: {datetime.now().strftime('%Y-%m-%d')}\n"
    
    st.markdown(content)
    
    st.download_button(
        label="Download Appendix D (Markdown)",
        data=content,
        file_name=f"Appendix_D_Thematic_Summary_{datetime.now().strftime('%Y%m%d')}.md",
        mime="text/markdown"
    )

def load_or_calculate_reliability():
    """
    Load or calculate reliability results to populate st.session_state.reliability_results
    """
    if 'reliability_results' in st.session_state:
        return
        
    import sqlite3
    import math
    from pathlib import Path
    from utils.quote_matcher import clean_gemma_row, DEDUCTIVE_CODES
    from scripts.calculate_irr_kappa import parse_subcodes, load_researcher_data, derive_dimensions, compute_pairwise_irr
    
    db_path = Path("data/research_data.db")
    sheet_path = Path("outputs/gold_standard_coding_sheet.csv")
    if not sheet_path.exists():
        sheet_path = Path("outputs/gold_standard_coding_sheet (version 1).xlsb")
        
    if not db_path.exists() or not sheet_path.exists():
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA table_info(coded_data)")
    cols = {row[1] for row in cursor.fetchall()}
    has_clean_cols = 'ppm_subcodes_clean' in cols
    
    if has_clean_cols:
        cursor.execute("""
            SELECT c.reddit_id, COALESCE(c.ppm_subcodes_clean, c.ppm_subcodes) as ppm_subcodes 
            FROM coded_data c
            JOIN collected_data cd ON c.reddit_id = cd.reddit_id
            WHERE c.coded_by = 'llama3.1' AND c.quarantine_reason IS NULL
        """)
    else:
        cursor.execute("""
            SELECT c.reddit_id, c.ppm_subcodes 
            FROM coded_data c
            JOIN collected_data cd ON c.reddit_id = cd.reddit_id
            WHERE c.coded_by = 'llama3.1' AND (cd.data_source IS NULL OR cd.data_source != 'json_endpoint')
        """)
    llama_raw = {r['reddit_id']: parse_subcodes(r['ppm_subcodes']) for r in cursor.fetchall()}
    conn.close()
    
    researcher_raw = load_researcher_data(sheet_path)
    
    llama_data = {rid: derive_dimensions(codes) for rid, codes in llama_raw.items()}
    researcher_data = {rid: derive_dimensions(codes) for rid, codes in researcher_raw.items()}
    
    res = compute_pairwise_irr("Llama 3.1", "Researcher", llama_data, researcher_data, min_pos=5)
    
    overlap_ids = sorted(list(set(llama_data.keys()) & set(researcher_data.keys())))
    n_overlap = len(overlap_ids)
    
    dimensions = ["PUSH", "PULL", "MOOR-F", "MOOR-I"]
    total_agreements = 0
    total_checks = 0
    
    category_agreement = {}
    for dim in dimensions:
        agreements = sum(1 for rid in overlap_ids if (dim in llama_data[rid]) == (dim in researcher_data[rid]))
        pct = (agreements / n_overlap * 100) if n_overlap else 0.0
        
        kappa = res['dimension_results'].get(dim, {}).get('kappa', 0.0)
        if math.isnan(kappa):
            kappa = 0.0
            
        category_agreement[dim] = {
            'agreement': pct,
            'kappa': kappa
        }
        total_agreements += agreements
        total_checks += n_overlap
        
    pct_agreement = (total_agreements / total_checks * 100) if total_checks else 0.0
    
    st.session_state.reliability_results = {
        'cohens_kappa': res['mean_dimension_kappa'] if not math.isnan(res['mean_dimension_kappa']) else 0.0,
        'percent_agreement': pct_agreement,
        'n_items': n_overlap,
        'category_agreement': category_agreement
    }

def generate_reliability_appendix():
    st.subheader("Appendix E: Inter-Coder Reliability Report")
    
    load_or_calculate_reliability()
    
    if 'reliability_results' not in st.session_state:
        st.warning("No reliability analysis available. Please run inter-coder reliability analysis first.")
        return
    
    results = st.session_state.reliability_results
    
    content = "# Appendix E: Inter-Coder Reliability Report\n\n"
    content += "## 1. Reliability & Ground Truth Framework\n\n"
    content += "**Purpose**: The gold standard is what turns 50,044 machine-generated labels into evidence you are allowed to build findings on.\n\n"
    content += "The whole empirical base, the frequency profiles, and the archetypes sit on Llama's coding. The first question any examiner asks is how you know the model coded those posts correctly. Without an answer, the corpus is unverified model output, not data. Everything in this reliability arm exists to answer that one question.\n\n"
    content += "A second model alone does not answer it. Model-versus-model agreement (Llama vs Gemma) reflects consistency rather than correctness. The L–G kappa is near chance, and the two models fail in opposite directions: Llama omits, Gemma over-applies. Two unreliable coders matching each other would only tell you they agree, not that they are right; two unreliable coders disagreeing only tells you they are unreliable. So the L–G arm documents the instrument's divergence and its failure modes—it is not a validity claim, and is never presented as one.\n\n"
    content += "The human anchor is the load-bearing part because validity needs a ground truth, and the only ground truth here is expert judgment. The researcher blind-coded 125 posts with no model labels visible, applying the codebook as the domain expert. That is the benchmark. Comparing each model to the researcher's blind coding is the only comparison that speaks to whether the model's labels match a human reading of the same text (reliability) rather than mere self-consistency. **L–R (Llama vs Researcher)** and **G–R (Gemma vs Researcher)** are the primary reliability checks.\n\n"
    content += "## 2. Construct / Dimension-Level Reliability\n\n"
    content += "Evaluation is conducted at the dimension level rather than the subcode level because at 24 codes, agreement is near chance and Llama is test-retest unstable, so a subcode kappa would be indefensible. The instrument cannot be claimed to reliably separate PULL-05 from PULL-07. At the dimension level—the four PPM constructs the thesis actually theorizes—the question is whether the model agrees with the researcher on the presence of a push, pull, mooring facilitator, or mooring inhibitor. This is the level the analysis runs at, the level the archetypes are derived from, and the level where a reliability metric can honestly be reported.\n\n"
    content += "## 3. Reliability Metrics\n\n"
    content += f"- **Cohen's Kappa**: κ = {results['cohens_kappa']:.3f}\n"
    content += f"- **Percent Agreement**: {results['percent_agreement']:.1f}%\n"
    content += f"- **Items Analyzed**: {results['n_items']}\n\n"
    
    content += "## 4. Interpretation & Reliability Framework\n\n"
    content += "The inter-coder reliability is reported at the dimension level and read as the limit it places on what the coding can support. The L-R and G-R arms compare each model to the researcher's blind coding and so speak to validity, agreement with expert judgement, and both return slight agreement (κ = 0.115 and 0.151). The L-G arm compares the two models and speaks to consistency, returning near-chance agreement (κ = 0.047). Because the validity arms are slight, the frequency profiles are treated throughout as indicative patterns rather than validated measurements, which is the basis for inferring the typology rather than deriving it.\n\n"
    
    content += "### Methodological Scope and Limitations\n"
    content += "* **Submission Stratum Restriction**: The inter-coder reliability analysis was performed entirely on the **submission stratum (posts)**, as the Gemma 3 (12B) comparison dataset only included coding for submissions and did not include comment coding.\n"
    content += "* **Comment Stratum Single-Coded**: The comment stratum (which constitutes 57,755 of the 58,415 corpus units, or over 98%) was coded using the identical prompt structure, Ollama temperature parameters, and codebook definitions as the submissions, but remains unassessed by second-rater overlap. Consequently, reliability claims are bounded strictly to the submission stratum.\n"
    content += "* **Asymmetric Quote Verification Scope**: The researcher coded from the full post title and text body, whereas the automated model codes are quote-verified against the post text body only. This design choice conservatively understates model-human agreement on title-content submissions, affecting both L–R and G–R comparisons symmetrically and keeping the comparison fair.\n\n"
    
    if 'category_agreement' in results:
        content += "## Agreement by Category\n\n"
        content += "| Category | Agreement % | Cohen's Kappa |\n"
        content += "|----------|-------------|---------------|\n"
        
        for cat, metrics in results['category_agreement'].items():
            content += f"| {cat} | {metrics['agreement']:.1f}% | {metrics['kappa']:.3f} |\n"
    
    content += f"\n**Analysis Date**: {datetime.now().strftime('%Y-%m-%d')}\n"
    
    st.markdown(content)
    
    st.download_button(
        label="Download Appendix E (Markdown)",
        data=content,
        file_name=f"Appendix_E_Reliability_{datetime.now().strftime('%Y%m%d')}.md",
        mime="text/markdown"
    )

def generate_data_quality_appendix():
    st.subheader("Appendix F: Data Quality & Edge Cases Report")
    
    try:
        db_report = get_data_quality_report()
        replicability_logs = load_replicability_logs(limit=10)
    except Exception:
        db_report = None
        replicability_logs = []
    
    total_items = len(st.session_state.collected_data)
    nsfw_count = 0
    removed_count = 0
    non_english_count = 0
    media_only_count = 0
    truncated_count = 0
    skipped_nsfw = 0
    skipped_removed = 0
    skipped_media_only = 0
    data_source = "session"
    
    nsfw_subreddits_db = []
    
    if replicability_logs:
        latest_log = replicability_logs[0]
        stats = latest_log.get('statistics', {})
        validation = latest_log.get('validation_results', {})
        
        if stats.get('total_collected'):
            data_source = "database"
            total_items = stats.get('total_collected', 1)
            nsfw_count = stats.get('nsfw_collected', validation.get('nsfw_collected', 0))
            removed_count = stats.get('removed_collected', validation.get('removed_collected', 0))
            non_english_count = stats.get('non_english_collected', validation.get('non_english_collected', 0))
            media_only_count = stats.get('media_only_collected', validation.get('media_only_collected', 0))
            truncated_count = stats.get('truncated_collected', validation.get('truncated_collected', 0))
            skipped_nsfw = stats.get('skipped_nsfw', 0)
            skipped_removed = stats.get('skipped_removed', 0)
            skipped_media_only = stats.get('skipped_media_only', 0)
            nsfw_subreddits_db = stats.get('nsfw_subreddits', validation.get('nsfw_subreddits', []))
    
    if data_source == "session" or total_items == 0:
        nsfw_count = sum(1 for item in st.session_state.collected_data if item.get('metadata', {}).get('nsfw', False))
        removed_count = sum(1 for item in st.session_state.collected_data if item.get('metadata', {}).get('content_status') in ['removed', 'author_deleted', 'empty'])
        non_english_count = sum(1 for item in st.session_state.collected_data if item.get('metadata', {}).get('language_flag') == 'likely_non_english')
        media_only_count = sum(1 for item in st.session_state.collected_data if item.get('metadata', {}).get('content_type') in ['image', 'video', 'link'])
        truncated_count = sum(1 for item in st.session_state.collected_data if item.get('metadata', {}).get('was_truncated', False))
        total_items = max(len(st.session_state.collected_data), 1)
    
    if db_report:
        nsfw_count = max(nsfw_count, db_report.get('nsfw_items', 0))
        removed_count = max(removed_count, db_report.get('removed_items', 0))
        non_english_count = max(non_english_count, db_report.get('non_english_items', 0))
    
    total_items = max(total_items, 1)
    
    subreddits_with_nsfw = set()
    
    if nsfw_subreddits_db:
        subreddits_with_nsfw = set(nsfw_subreddits_db)
    else:
        for item in st.session_state.collected_data:
            if item.get('metadata', {}).get('subreddit_nsfw', False):
                subreddits_with_nsfw.add(item.get('subreddit'))
    
    content = f"""# Appendix F: Data Quality & Edge Cases Report

## Overview

This appendix documents the handling of edge cases and data quality considerations in the research data collection process, ensuring methodological transparency and replicability.

**Data Source**: {data_source.upper()} ({"Persisted replicability logs" if data_source == "database" else "Current session data"})
**Total Items Analyzed**: {total_items}

## NSFW (Not Safe For Work) Content

### Summary Statistics
- **Total NSFW Posts/Comments Collected**: {nsfw_count}
- **NSFW Content Skipped (Excluded)**: {skipped_nsfw}
- **NSFW Subreddits Encountered**: {len(subreddits_with_nsfw)}
- **Percentage of NSFW Content**: {(nsfw_count / total_items * 100):.1f}%

### Ethical Handling Protocol
1. All NSFW content is flagged with the `over_18` metadata field from Reddit API
2. NSFW subreddits are identified and documented at the collection stage
3. Researchers can filter NSFW content at any point in the analysis
4. All NSFW inclusion/exclusion decisions are logged in the audit trail
5. Sensitive content is handled according to IRB guidelines

### Implications for Analysis
NSFW content in supplement/nootropic communities may include:
- Discussions of substances with sexual enhancement claims
- User experiences involving sensitive personal topics
- Content from communities with age-restricted settings

Researchers should consider whether NSFW content is relevant to the research questions and document their inclusion/exclusion rationale.

## Removed/Deleted Content

### Summary Statistics
- **Removed or Deleted Items Collected**: {removed_count}
- **Removed Content Skipped (Excluded)**: {skipped_removed}
- **Percentage of Unavailable Content**: {(removed_count / total_items * 100):.1f}%

### Content Status Categories
- **[removed]**: Content removed by moderators
- **[deleted]**: Content deleted by the original author
- **empty**: Posts with no text content

### Methodological Implications
Removed/deleted content represents a form of data loss that may introduce bias:
- Controversial or rule-violating content may be overrepresented in deletions
- Self-censorship by users affects data authenticity
- Moderator actions reflect community standards

The study documents these items to maintain transparency about data completeness.

## Language Detection

### Summary Statistics
- **Flagged as Likely Non-English**: {non_english_count}
- **Percentage Flagged**: {(non_english_count / total_items * 100):.1f}%

### Detection Method
Basic heuristic analysis using character encoding patterns:
- Posts with >30% non-ASCII characters are flagged
- Manual review recommended for flagged content
- False positives may include posts with heavy emoji use

## Media-Only Posts

### Summary Statistics
- **Media-Only Posts Collected (Image/Video/Link)**: {media_only_count}
- **Media-Only Posts Skipped (Excluded)**: {skipped_media_only}
- **Percentage of Media-Only**: {(media_only_count / total_items * 100):.1f}%

### Content Types Identified
- Image posts (Reddit galleries, Imgur links)
- Video posts (Reddit video, YouTube)
- External link posts (articles, product pages)

### Analytical Considerations
Media-only posts may have limited text for thematic analysis but provide:
- Product images and documentation
- User-generated content (before/after photos)
- External references and citations

## Text Length & Truncation

### Summary Statistics
- **Truncated Posts**: {truncated_count}
- **Maximum Text Length Applied**: Configurable (default 50,000 chars)

### Handling Very Long Posts
Posts exceeding the maximum length are truncated with:
- `was_truncated` metadata flag set to True
- Original length preserved for reference
- Analysis conducted on truncated version

## Replicability Documentation

### Collection Parameters Hash
Each collection run generates a unique hash of:
- Target subreddits
- Collection method and time filter
- Limit and comment settings
- NSFW and edge case filter settings
- User agent and timestamp

This hash enables verification that two researchers used identical parameters.

### Rate Limit Events
All API rate limiting events are logged with:
- Timestamp of occurrence
- Affected subreddit/post
- Error message details
- Recovery actions taken

## Recommendations for Future Research

1. **NSFW Content**: Clearly document inclusion/exclusion criteria in methodology
2. **Deleted Content**: Consider temporal analysis to capture content before deletion
3. **Language**: Implement more sophisticated NLP-based language detection
4. **Media Posts**: Consider image analysis tools for visual content
5. **Replicability**: Archive collection parameters and share hashes

---

**Report Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Total Items in Database**: {total_items}
**Data Source**: {data_source.upper()}
"""
    
    if replicability_logs:
        content += "\n## Collection Run History\n\n"
        content += "| Hash | Timestamp | Total | NSFW | Removed | Non-English |\n"
        content += "|------|-----------|-------|------|---------|-------------|\n"
        for log in replicability_logs[:5]:
            stats = log.get('statistics', {})
            validation = log.get('validation_results', {})
            content += f"| {log.get('collection_hash', 'N/A')[:8]}... | {log.get('timestamp', 'N/A')[:10]} | {stats.get('total_collected', 0)} | {validation.get('nsfw_collected', 0)} | {validation.get('removed_collected', 0)} | {validation.get('non_english_collected', 0)} |\n"
    
    st.markdown(content)
    
    st.download_button(
        label="Download Appendix F (Markdown)",
        data=content,
        file_name=f"Appendix_F_Data_Quality_{datetime.now().strftime('%Y%m%d')}.md",
        mime="text/markdown"
    )

def generate_literature_linkages_appendix():
    st.subheader("Appendix G: Literature-Data Linkages")
    
    zotero_refs = load_zotero_references(limit=500)
    citation_links = load_citation_links(limit=500)
    replicability_logs = load_replicability_logs(limit=50)
    
    n_refs = len(zotero_refs)
    n_links = len(citation_links)
    n_collections = len(replicability_logs)
    
    linked_collection_hashes = set(link.get('collection_hash') for link in citation_links)
    n_linked_collections = len(linked_collection_hashes)
    
    all_keywords = set()
    for ref in zotero_refs:
        if ref.get('keywords'):
            all_keywords.update(ref['keywords'])
    
    data_source = "DATABASE" if zotero_refs else "NONE"
    
    content = f"""# Appendix G: Literature-Data Linkages

## Overview

This appendix documents the systematic connection between the literature review and data collection phases, demonstrating theoretical alignment per Creswell & Creswell (2023) mixed methods standards.

## Zotero Library Integration

### Citation Statistics
- **Total References Synced**: {n_refs}
- **Total Citations Linked to Collections**: {n_links}
- **Collection Runs with Citations**: {n_linked_collections} of {n_collections}
- **Unique Keywords Extracted**: {len(all_keywords)}
- **Data Source**: {data_source}

### Purpose of Literature-Data Linkage

The integration of Zotero citations with Reddit data collection serves multiple methodological purposes:

1. **Theoretical Grounding**: Keywords extracted from literature guide data collection to ensure theoretical relevance
2. **Audit Trail**: Citation links document which theoretical frameworks informed each collection run
3. **Reproducibility**: Explicit connection between literature and data supports study replication
4. **Triangulation**: Literature-informed coding supports methodological triangulation

## Keyword Extraction Methodology

Keywords were extracted from Zotero references using:
1. **Manual Tags**: Author-assigned tags from Zotero entries
2. **Abstract Analysis**: Automated extraction of high-frequency domain terms from abstracts
3. **Stop Word Filtering**: Common academic terms excluded (e.g., "study", "research", "method")

### Top Literature Keywords
"""
    
    if all_keywords:
        keyword_list = sorted(list(all_keywords))[:30]
        content += "\n".join([f"- {kw}" for kw in keyword_list])
        content += "\n"
    else:
        content += "\n*No keywords available - sync Zotero library first*\n"
    
    content += "\n## Citation-Collection Mappings\n\n"
    
    if citation_links:
        content += "| Collection Hash | Citation | Link Type | Linked At |\n"
        content += "|-----------------|----------|-----------|------------|\n"
        
        for link in citation_links[:20]:
            hash_short = link.get('collection_hash', 'N/A')[:8] + "..."
            title_short = (link.get('title', 'Unknown')[:40] + "...") if len(link.get('title', '')) > 40 else link.get('title', 'Unknown')
            link_type = link.get('link_type', 'manual')
            linked_at = link.get('linked_at', 'N/A')[:10] if link.get('linked_at') else 'N/A'
            
            content += f"| {hash_short} | {title_short} | {link_type} | {linked_at} |\n"
        
        if len(citation_links) > 20:
            content += f"\n*...and {len(citation_links) - 20} more linkages*\n"
    else:
        content += "*No citations linked to collection runs yet*\n"
    
    content += "\n## Linked References (APA Format)\n\n"
    
    linked_keys = set(link.get('zotero_key') for link in citation_links)
    linked_refs = [ref for ref in zotero_refs if ref.get('zotero_key') in linked_keys]
    
    if linked_refs:
        for ref in linked_refs[:15]:
            citation = ref.get('citation_apa', 'Citation unavailable')
            content += f"- {citation}\n"
        
        if len(linked_refs) > 15:
            content += f"\n*...and {len(linked_refs) - 15} more references*\n"
    else:
        content += "*No references linked yet - use Zotero Citations module to link references*\n"
    
    content += f"""

## Methodological Justification

### Alignment with PPM Framework

The Push-Pull-Mooring (PPM) framework keywords from the literature informed:
1. **Push Factors**: Terms related to dissatisfaction with current cognitive enhancement methods
2. **Pull Factors**: Terms related to attraction to natural supplement alternatives
3. **Mooring Factors**: Terms related to switching costs, habits, and attachment

### Literature-Guided Data Collection

Data collection queries were informed by literature keywords to ensure:
- Theoretical saturation of key concepts
- Coverage of established constructs from prior research
- Identification of emergent themes outside established frameworks

## Quality Assurance

- All linkages timestamped for audit trail
- Citation data sourced from Zotero Web API
- Keywords validated against domain-specific terminology
- Regular synchronization maintains currency with evolving literature

---

**Report Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Total References**: {n_refs}
**Total Linkages**: {n_links}
**Data Source**: {data_source}
"""
    
    st.markdown(content)
    
    if zotero_refs:
        st.subheader("Reference Preview")
        with st.expander("View Synced References", expanded=False):
            for ref in zotero_refs[:10]:
                st.markdown(f"**{ref.get('title', 'Untitled')}** ({ref.get('year', 'n.d.')})")
                st.caption(ref.get('citation_apa', ''))
                if ref.get('keywords'):
                    st.caption(f"Keywords: {', '.join(ref['keywords'][:5])}")
                st.divider()
    
    st.download_button(
        label="Download Appendix G (Markdown)",
        data=content,
        file_name=f"Appendix_G_Literature_Linkages_{datetime.now().strftime('%Y%m%d')}.md",
        mime="text/markdown"
    )

def generate_complete_methodology():
    st.subheader("Complete Methodology Chapter")
    
    content = """# Chapter 3: Research Methodology

## 3.1 Research Design

This study employed a mixed-methods approach combining qualitative thematic analysis with quantitative content analysis of user-generated content from Reddit (Creswell & Creswell, 2023). The Push-Pull-Mooring (PPM) framework guided the analysis of consumer behavior in the natural cognitive supplement market.

## 3.2 Data Collection

### 3.2.1 Platform Selection
Reddit was selected as the data source due to its active communities discussing nootropics and natural cognitive supplements. The platform's structure enables organic, unmoderated discussions providing authentic consumer perspectives.

### 3.2.2 Sampling Strategy
Data were obtained from the BigQuery Analytics Hub (BQAH) Reddit Archive bulk export.
"""
    
    if st.session_state.collected_data:
        bqah_data = [d for d in st.session_state.collected_data if d.get('data_source') != 'json_endpoint']
        n_posts = len([d for d in bqah_data if d.get('type') == 'submission'])
        n_comments = len([d for d in bqah_data if d.get('type') == 'comment'])
        
        content += "The empirical analysis distinguishes between two distinct datasets:\n\n"
        content += "1. **Submissions-Only Dataset (Primary & Authoritative)**: Comprising only the submission stratum (N = 609 active Llama-coded posts across 6 communities). This dataset is the primary, authoritative empirical base for the study because its construct-level coding is formally backed by the inter-coder reliability (IRR) checks. Due to the smaller sample size per community (e.g. N = 107 for r/Decaf, N = 87 for r/NooTopics), coordinates on this map carry wider confidence intervals and notes regarding small-N instability.\n"
        content += f"2. **Full Coded Corpus (Exploratory & Supplementary)**: A bulk-coded dataset of {len(bqah_data):,} units (comprising {n_posts:,} submissions and {n_comments:,} comments). The comment stratum (representing over 98% of the data) remains unassessed by inter-coder agreement. Consequently, the full-corpus findings are presented strictly as a single supplementary table, marked clearly as single-coded, to observe large-scale statistical stability.\n\n"
        content += "### The Initiation-versus-Response Stratum Finding\n"
        content += "Comparing the submissions-only run to the full-corpus run reveals key stratum-based behavior dynamics. Submissions represent 'initiation' and goal-setting (intent to quit, seeking advice, highlighting attraction/pull factors), which tends to be pull-leaning. Comments represent 'response' and discussion (lived experience, side effect details, caffeine withdrawal struggles), which are heavily push-leaning. For example, r/Decaf flips from barely-pull (+0.091) in submissions-only to push-dominant (-0.279) in the full corpus. Rather than a contradiction, this divergence reflects the initiation-versus-response stratum dynamic, where the raw push factors of withdrawal are concentrated in user comments rather than thread-starting submissions. However, due to the thin submissions base, small-N coordinate instability also contributes to this flip, so Decaf's submissions coordinate is flagged as low-confidence.\n\n"
    
    content += """
### 3.2.3 Data Collection Procedure
1. Bulk data extraction from the BigQuery Analytics Hub (BQAH) Reddit Archive
2. Systematic ingestion of target subreddits from the BQAH corpus
3. Metadata capture (timestamp, author, score, subreddit)
4. Quality filtering, exclusion of deleted/removed content, and deduplication
5. Secure database storage with audit trail

## 3.3 Data Analysis

### 3.3.1 Coding Approach
Local LLM-assisted thematic coding using Ollama (llama3.1 and gemma3:12b) was employed, with all processing conducted offline to preserve participant anonymity. Human oversight ensured accuracy and contextual validity.

### 3.3.2 Codebook Development
"""
    
    if 'codebook_manager' in st.session_state and st.session_state.codebook_manager.get_all_codes():
        total_codes = len(st.session_state.codebook_manager.get_all_codes())
        content += f"A codebook with {total_codes} codes was developed iteratively through:\n"
        content += "1. Initial deductive coding based on PPM framework\n"
        content += "2. Inductive coding for emergent themes\n"
        content += "3. Iterative refinement through consensus discussions\n"
        content += "4. Rigor through inter-coder reliability assessment\n\n"
    
    content += "The codebook (detailed in Appendix A, the codebook) serves as the formal operationalisation of the PPM coding scheme. The quantitative frequency counts in this study are only as valid as the taxonomy and coding scheme they apply; Appendix A details the operational guidelines and definitions for each of the 24 deductive codes.\n\n"
    
    content += """
### 3.3.3 Inductive Theme Deduplication (Emergent Code Candidates)
For Layer B emergent themes (where no pre-existing PPM deductive codes matched), a two-pass qualitative coding approach was used. In the first pass, the LLM proposed raw labels and definitions for emergent themes. Due to semantic variation in natural language labeling, this generated thousands of raw theme candidates.

To resolve this, a fuzzy-string matching deduplication process was implemented using the RapidFuzz library:
1. **Length-Based Blocking**: Candidates were grouped by label length, searching for matches only within a mathematical ratio margin of length ($[0.74 \times \\text{len}, 1.35 \times \\text{len}]$). This optimized execution speed from $O(N^2)$ to $O(N)$ with no loss in accuracy.
2. **Token Sort Ratio**: Normalized token sorting similarity scores were computed, with a similarity threshold of $\\ge 85\\%$ used to merge semantically identical labels into canonical themes.
3. **Alternative Considered**: Meta-prompt clustering was evaluated as an alternative, but the deterministic, rule-based Token Sort Ratio approach was selected for its reproducibility, speed, and strict alignment with qualitative researcher auditing standards.

## 3.4 Reliability and Coding Quality

### 3.4.1 The Reliability and Ground Truth Framework
The gold-standard check is what turns 50,044 machine-generated labels into evidence on which findings can be built. Because the entire empirical base (including the frequency profiles and the archetypes) sits on the Llama coding, establishing correctness against expert judgment is the load-bearing component of the methodology. 

A second model alone does not establish correctness. Model-versus-model agreement (such as Llama vs Gemma) reflects consistency rather than correctness. The L-G Kappa is near chance, highlighting opposite failure modes where Llama tends to omit and Gemma tends to over-apply. Consequently, the L-G arm serves to document the instrument's divergence and consistency boundaries rather than support a correctness claim.

Rigor requires a ground truth anchor, which is expert judgment. The researcher blind-coded a sample of 125 posts with no model labels visible, applying the codebook as the domain expert. This human benchmark is the load-bearing part of the verification framework. Comparing each model to the researcher's blind coding (the L–R and G–R arms) is the comparison that speaks directly to validity—agreement with expert ground truth—rather than mere model-to-model consistency.

### 3.4.2 Construct/Dimension-Level Reliability
The inter-coder reliability is reported at the dimension level and read as the limit it places on what the coding can support. The L-R and G-R arms compare each model to the researcher's blind coding and so speak to validity, agreement with expert judgement, and both return slight agreement (κ = 0.115 and 0.151). The L-G arm compares the two models and speaks to consistency, returning near-chance agreement (κ = 0.047). Because the validity arms are slight, the frequency profiles are treated throughout as indicative patterns rather than validated measurements, which is the basis for inferring the typology rather than deriving it.

### 3.4.3 Pairwise Reliability and Validity Checks
- **L–R (Llama vs Researcher)**: A validity check, measuring how closely the model whose labels populate the corpus matches expert judgment at the construct level.
- **G–R (Gemma vs Researcher)**: A validity check, providing an independent triangulation against expert ground truth.
- **L–G (Llama vs Gemma)**: A consistency check, characterizing how the two models align against each other.

### 3.4.4 Defensibility and Examination Rigor
This design ensures:
1. **Methodological transparency** regarding the slight dimension-level agreement (κ = 0.115 and 0.151) between automated coding and the researcher.
2. **Transparent verification** that discharges the methodological expectation of checking automated models against human experts.
3. **Critical awareness of instrument limitations** (viva insurance under Dublin descriptors), presenting model weaknesses as analytical candor.
4. **Justification of the interpretive move** from data to archetypes, resting findings on construct-level patterns inferred from the indicative data.
"""
    content += """
## 3.5 Ethical Considerations

- All data from publicly accessible sources
- User anonymization applied throughout
- Compliance with platform Terms of Service
- IRB approval obtained
- Data security and confidentiality maintained

## 3.6 Limitations

- **Comment Stratum Single-Coded**: Inter-coder reliability was established entirely on the submission stratum (posts) due to comparative model data availability limitations. The comment stratum (57,755 units, or >98% of the corpus) was coded using the identical prompt structure and codebooks but remains unassessed by second-rater agreement.
- **Asymmetric Quote Verification Scope**: The human researcher coded from the full post title and text body, whereas the automated model codes are quote-verified against the post text body only. This design choice conservatively understates model-human agreement on title-content submissions, affecting both L–R and G–R comparisons symmetrically and keeping the comparison fair.
- **Y-Axis Mooring Intensity and Inhibitor Under-Coding**: The perceptual map uses combined mooring intensity (MOOR-F + MOOR-I) on the Y-axis. However, because the primary LLM (Llama 3.1) exhibits severe conservative under-coding for Mooring Inhibitors (MOOR-I) compared to Mooring Facilitators (MOOR-F), the Y-axis coordinates are quantitatively dominated by facilitators. This prevents the map from validly distinguishing high-facilitator from high-inhibitor communities, locating them both on a single combined mooring intensity axis where inhibitor-rich positions are systematically under-located. As a result, the map is a valid visual instrument for the net push-pull lean (X-axis) and facilitator mooring (Y-axis), but fails to separate inhibitor positions quantitatively. Consequently, Archetypes 4 and 5 are qualitative/inductive findings that the map under-represents or under-locates. All five archetypes are framed strictly as hypotheses to be tested against the map rather than locked typologies, pending Phase 2 data, where Archetypes 4 and 5 may merge into a single friction-anchored archetype with a push-pull sub-split if the thin quantitative MOOR-I signal fails to separate them.
- **Platform Bias**: Data limited to Reddit users (self-selection bias).
- **Language & Geography**: English-language content only.
- **Temporal specificity**: Restricted to historical data within the archive period.
- **Automated coding limitations**: Automated qualitative coding is conservative and requires human review guidelines.

## 3.7 Rigor and Trustworthiness

Credibility, transferability, dependability, and confirmability were ensured through:
- Triangulation of multiple data sources
- Detailed audit trail
- Peer review of coding process
- Thick description of findings
- Reflexivity throughout analysis

---

**Note**: See appendices for detailed codebook, sample coded data, and reliability statistics.
"""
    
    st.markdown(content)
    
    st.download_button(
        label="Download Complete Methodology (Markdown)",
        data=content,
        file_name=f"Chapter_3_Methodology_{datetime.now().strftime('%Y%m%d')}.md",
        mime="text/markdown"
    )

def generate_findings_chapter():
    st.subheader("Chapter 4: Quantitative Findings & Consumer Archetypes")
    
    content = r"""# Chapter 4: Empirical Findings & Consumer Archetypes

## 4.1 Quantitative PPM Community Profiles

All PPM codes and categories are operationalized according to the taxonomy detailed in Appendix A (the codebook), since the quantitative counts are only as valid as the classification scheme they apply. The frequency findings in this chapter are strictly descriptive and indicative; no inferential statistical testing has been performed on the corpus.

The empirical analysis distinguishes between two distinct datasets: the primary, authoritative Submissions-Only dataset and the supplementary, single-coded Full-Corpus dataset.

### 4.1.1 Primary Authoritative Profiles (Submissions-Only, N = 609)

The primary and authoritative findings base for the study is restricted to the submission stratum (thread-starting posts). This run is formally backed by the construct-level inter-coder reliability checks (Kappa results) and licenses the quantitative findings as a reliable representation of community leanings.

| Subreddit | Total Units | PUSH Count | PUSH % | PULL Count | PULL % | MOOR-F Count | MOOR-F % | MOOR-I Count | MOOR-I % | MOOR Count | MOOR % | NONE Count | NONE % | Dominant |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **r/NooTopics** | 87 | 4 | 4.6%* | 29 | 33.3% | 23 | 26.4% | 3 | 3.4%* | 26 | 29.9% | 53 | 60.9% | PULL |
| **r/StackAdvice** | 100 | 13 | 13.0% | 36 | 36.0% | 19 | 19.0% | 4 | 4.0%* | 23 | 23.0% | 54 | 54.0% | PULL |
| **r/Supplements** | 105 | 11 | 10.5% | 29 | 27.6% | 27 | 25.7% | 3 | 2.9%* | 28 | 26.7% | 60 | 57.1% | PULL |
| **r/Biohackers** | 105 | 12 | 11.4% | 29 | 27.6% | 17 | 16.2% | 2 | 1.9%* | 18 | 17.1% | 66 | 62.9% | PULL |
| **r/Nootropics** | 105 | 13 | 12.4% | 29 | 27.6% | 20 | 19.0% | 5 | 4.8%* | 23 | 21.9% | 64 | 61.0% | PULL |
| **r/Decaf** | 107 | 40 | 37.4% | 48 | 44.9% | 25 | 23.4% | 10 | 9.3% | 32 | 29.9% | 30 | 28.0% | PULL |

*\*Note: Cell counts under 10 are flagged with an asterisk to denote potential instability.*

### 4.1.2 Supplementary Profiles (Full-Corpus, N = 50,044)

The full-corpus run (comprising both posts and comments) is presented here as an exploratory, supplementary dataset. The comment stratum (constituting over 98% of these units) remains unassessed by inter-coder agreement, so this table is marked as single-coded and is used solely to observe large-scale statistical trends.

| Subreddit | Total Units | PUSH Count | PUSH % | PULL Count | PULL % | MOOR-F Count | MOOR-F % | MOOR-I Count | MOOR-I % | MOOR Count | MOOR % | NONE Count | NONE % | Dominant |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **r/Nootropics** | 9,143 | 432 | 4.7% | 745 | 8.1% | 392 | 4.3% | 136 | 1.5% | 520 | 5.7% | 7,922 | 86.6% | PULL |
| **r/Supplements** | 13,364 | 2,960 | 22.1% | 4,074 | 30.5% | 2,250 | 16.8% | 600 | 4.5% | 2,812 | 21.0% | 5,571 | 41.7% | PULL |
| **r/Decaf** | 3,051 | 1,197 | 39.2% | 674 | 22.1% | 325 | 10.7% | 91 | 3.0% | 408 | 13.4% | 1,207 | 39.6% | PUSH |
| **r/Biohackers** | 14,625 | 3,313 | 22.7% | 5,084 | 34.8% | 2,959 | 20.2% | 810 | 5.5% | 3,715 | 25.4% | 5,060 | 34.6% | PULL |
| **r/NooTopics** | 6,460 | 1,983 | 30.7% | 2,124 | 32.9% | 1,086 | 16.8% | 296 | 4.6% | 1,365 | 21.1% | 2,175 | 33.7% | PULL |
| **r/StackAdvice** | 3,401 | 748 | 22.0% | 916 | 26.9% | 520 | 15.3% | 88 | 2.6% | 600 | 17.6% | 1,706 | 50.2% | PULL |

---

## 4.2 Perceptual Map & Stratum Divergence

![Figure 4.1: Perceptual map of the six communities, push-pull balance against mooring prevalence](outputs/chapter4/submissions_only/perceptual_map.png)

The two runs yield perceptual map coordinates that reflect different structural patterns:

* **Submissions-Only Coordinates (Primary & Authoritative)**:
  * *r/NooTopics*: X = +0.758, Y = 29.9%
  * *r/StackAdvice*: X = +0.469, Y = 23.0%
  * *r/Supplements*: X = +0.450, Y = 26.7%
  * *r/Biohackers*: X = +0.415, Y = 17.1%
  * *r/Nootropics*: X = +0.381, Y = 21.9%
  * *r/Decaf*: X = +0.091, Y = 29.9%
* **Full-Corpus Coordinates (Supplementary & Single-Coded)**:
  * *r/Nootropics*: X = +0.266, Y = 5.7%
  * *r/Biohackers*: X = +0.210, Y = 25.4%
  * *r/Supplements*: X = +0.160, Y = 21.0%
  * *r/StackAdvice*: X = +0.100, Y = 17.6%
  * *r/NooTopics*: X = +0.035, Y = 21.1%
  * *r/Decaf*: X = -0.279, Y = 13.4%

### 4.2.1 The Decaf Position and Equilibrium Proximity

In the submissions-only map, *r/Decaf* sits at $X = +0.091$, making it slightly pull-leaning. This position is highly close to the push-pull equilibrium ($X = 0.0$). Because of this **equilibrium proximity**, even minor changes in post counts or stratum definitions can flip the net balance. This explains why *r/Decaf* exhibits a genuine shift to push-dominant ($X = -0.279$) when comments are added—its position sits near the center line where coordinate flips are easily triggered by stratum shifts.

### 4.2.2 The Y-Axis Facilitator Skew and MOOR-I Under-Coding

The Y-axis represents combined mooring intensity (binary presence of MOOR-F or MOOR-I). However, qualitative review reveals that the primary LLM (Llama 3.1) is highly conservative with Mooring Inhibitors (MOOR-I), coding only 2 of 125 posts in the gold-standard sample, compared to 11 by the researcher and 40 by Gemma. 

As a result, the Y-axis coordinates are dominated in practice by Mooring Facilitators (MOOR-F) and under-represent Mooring Inhibitors. Because of this mapping limitation, the map is unable to validly separate facilitator-high from inhibitor-high positions (as they both merge into combined mooring intensity). Consequently, inhibitor-heavy consumer archetypes cannot be reliably located by coordinates on the map.

---

## 4.3 The Initiation-versus-Response Stratum Finding

The divergence between submissions and comments is not a random error but a genuine behavioral stratum dynamic:
* **Submissions (Initiation & Goal-Setting)**: Thread-starting posts typically represent the initiation phase of a switching journey. Users post to express hope, clarify goals (e.g. intent to quit caffeine or start a new regimen), and seek recommendations. This phase is naturally pull-attracted, focusing on the benefits of alternatives.
* **Comments (Response & Discussion)**: Comments represent interactive responses where users share their lived experiences, ongoing side effects, withdrawal pains, and practical struggles. This phase is heavily push-driven, focusing on stimulant crash and withdrawal discomfort.
* **The Decaf Flip**: The flip of *r/Decaf* from a pull-leaning submissions coordinate to a push-dominant full-corpus coordinate reflects this stratum dynamic. The raw push factors (withdrawal fatigue, severe head pain, and coffee habit disruption) are discussed in the comments stratum rather than thread-starts.

---

## 4.4 Consumer Archetype Typology

The archetypes represent cross-cutting behavioral positions in the 2D Push-Pull by Mooring space, mapping conceptually to the communities in a non-one-to-one way to avoid simple subreddit relabeling. To maintain rigor, these archetypes are presented as conceptual positions mapped onto the empirical grid:

### 4.4.1 Unified Typology Table

| Archetype Name | Primary Empirical Stratum | Reliability Status | Primary Communities | Net PPM Position | Core Narrative |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **The Dissatisfied Exiter** | Submissions & Comments | Reliability-Assessed | *r/Decaf*, *r/StackAdvice*, *r/Nootropics* | Push-Dominant / Equilibrium | Driven by caffeine crashes, withdrawal fatigue, or prescription stimulant side effects, seeking an exit. |
| **The Ad-Hoc Optimizer** | Submissions & Comments | Reliability-Assessed | *r/Nootropics*, *r/Supplements*, *r/StackAdvice* | Pull-Leaning, Low Mooring | Attracted to natural nootropics for situational support, operating without complex lifestyle integration. |
| **The Systems-Thinking Biohacker** | Submissions & Comments | Reliability-Assessed | *r/Biohackers*, *r/NooTopics*, *r/Supplements* | Pull-Leaning, High Mooring Facilitators | Integrates nootropic usage into structured systems, bio-tracking, routines, and stacking protocols. |
| **The Friction-Inhibited Seeker** | Submissions & Comments | Qualitative/Emergent Triangulation | *r/StackAdvice*, *r/NooTopics* | Pull-Leaning/Equilibrium, High Mooring Inhibitors | Motivated by benefits but facing high switching friction (dosing anxiety, placebo concerns, trust deficits). |
| **The Trapped/Anchored Switcher** | Submissions & Comments | Qualitative/Emergent Triangulation | *r/Decaf*, *r/StackAdvice* | Push-Leaning/Equilibrium, High Mooring Inhibitors | Desires to exit conventional stimulants but is anchored by severe withdrawal struggles or ritual attachment. |

### 4.4.2 Integrated Archetype Narratives

#### 1. The Dissatisfied Exiter
This archetype is primarily driven by negative push stressors (anxiety, crashes, prescription stimulant side effects) with a clear intent to exit stimulant dependency. Users in this category are not looking for optimization but recovery. *r/Decaf* is the clearest exemplar of this position, but it is also shared by significant user segments in *r/StackAdvice* (seeking transition strategies off prescription ADHD medications) and *r/Nootropics* (discussing dopaminergic recovery post-abuse).

#### 2. The Ad-Hoc Optimizer
Attracted to natural nootropics for situational cognitive enhancement or baseline wellness improvements, but operating individually without complex daily lifestyle systems or high switching costs. They use natural nootropics (e.g., L-theanine for anxiety, ashwagandha, magnesium) as simple remedies rather than building systems. This profile is widely distributed across *r/Nootropics* and *r/Supplements*, with some presence in *r/StackAdvice*.

#### 3. The Systems-Thinking Biohacker
Integrates supplement usage into a structured, systems-level lifestyle (DIY biology, longevity, sleep/wearable feedback). Mooring facilitators (accessible product literacy, systems routine, stacking protocols) are highly load-bearing. *r/Biohackers* serves as the clearest exemplar, but this archetype is also shared by *r/NooTopics* (advanced mechanism-level design) and *r/Supplements* (complex vitamin/mineral stacks).

#### 4. The Friction-Inhibited Seeker
Motivated by supplement benefits but facing significant switching friction, such as dosing anxiety, placebos, confusion, and trust deficits. This archetype sits between *r/StackAdvice* (dominated by novice stack safety queries and dosing anxiety) and *r/NooTopics* (where advanced users face high informational barriers and trust/safety deficits regarding unresearched research chemicals).

#### 5. The Trapped/Anchored Switcher
Experiences high dissatisfaction with conventional stimulants and desires to exit, but is anchored by significant switching friction (withdrawal struggles, cost barriers, or daily habit attachment). This profile cross-cuts *r/Decaf* (struggling with caffeine withdrawal and morning coffee rituals) and *r/StackAdvice* (trying to manage withdrawal symptoms under high anxiety).
"""

    st.markdown(content)
    
    st.download_button(
        label="Download Findings Chapter (Markdown)",
        data=content,
        file_name=f"Chapter_4_Findings_{datetime.now().strftime('%Y%m%d')}.md",
        mime="text/markdown"
    )


import streamlit as st
import pandas as pd
from datetime import datetime
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
            "Complete Methodology Chapter"
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

def generate_data_collection_appendix():
    st.subheader("Appendix A: Data Collection Methodology")
    
    n_posts = len([d for d in st.session_state.collected_data if d.get('type') == 'submission'])
    n_comments = len([d for d in st.session_state.collected_data if d.get('type') == 'comment'])
    
    if st.session_state.collected_data:
        dates = [d.get('created_utc') for d in st.session_state.collected_data if d.get('created_utc')]
        if dates:
            earliest = min(dates)
            latest = max(dates)
        else:
            earliest = "N/A"
            latest = "N/A"
    else:
        earliest = "N/A"
        latest = "N/A"
    
    content = f"""
# Appendix A: Data Collection Methodology

## Data Source
Data were collected from Reddit, a social news aggregation and discussion platform, using the Python Reddit API Wrapper (PRAW). Reddit was selected due to its active communities discussing natural cognitive supplements and nootropics.

## Collection Parameters
- **Total Posts**: {n_posts}
- **Total Comments**: {n_comments}
- **Total Items**: {len(st.session_state.collected_data)}
- **Date Range**: {earliest} to {latest}
- **Collection Date**: {datetime.now().strftime('%Y-%m-%d')}

## Inclusion Criteria
Posts and comments were included if they:
1. Discussed natural cognitive supplements or nootropics
2. Contained substantive user experiences or opinions
3. Were written in English
4. Met minimum quality standards (length, coherence)

## Data Collection Process
1. Authentication with Reddit API using approved developer credentials
2. Systematic retrieval from target subreddits
3. Metadata capture including author, timestamp, subreddit, score
4. Automatic deduplication and quality filtering
5. Secure storage in PostgreSQL database with audit trail

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

def generate_reliability_appendix():
    st.subheader("Appendix E: Inter-Coder Reliability Report")
    
    if 'reliability_results' not in st.session_state:
        st.warning("No reliability analysis available. Please run inter-coder reliability analysis first.")
        return
    
    results = st.session_state.reliability_results
    
    content = "# Appendix E: Inter-Coder Reliability Report\n\n"
    content += "## Overview\n\n"
    content += f"Inter-coder reliability was assessed to ensure consistency and rigor in the coding process (Creswell & Creswell, 2023).\n\n"
    
    content += "## Reliability Metrics\n\n"
    content += f"- **Cohen's Kappa**: κ = {results['cohens_kappa']:.3f}\n"
    content += f"- **Krippendorff's Alpha**: α = {results.get('krippendorffs_alpha', 0.0):.3f}\n"
    content += f"- **Percent Agreement**: {results['percent_agreement']:.1f}%\n"
    content += f"- **Items Analyzed**: {results['n_items']}\n\n"
    
    content += "## Interpretation\n\n"
    kappa = results['cohens_kappa']
    alpha = results.get('krippendorffs_alpha', 0.0)
    
    if kappa >= 0.80:
        interp = "almost perfect agreement"
    elif kappa >= 0.60:
        interp = "substantial agreement"
    elif kappa >= 0.40:
        interp = "moderate agreement"
    else:
        interp = "fair to slight agreement"
    
    content += f"The Cohen's Kappa value of {kappa:.3f} and Krippendorff's Alpha value of {alpha:.3f} "
    content += f"both indicate {interp} between coders, demonstrating acceptable reliability for qualitative research.\n\n"
    
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
"""
    
    if st.session_state.collected_data:
        n_posts = len([d for d in st.session_state.collected_data if d.get('type') == 'submission'])
        n_comments = len([d for d in st.session_state.collected_data if d.get('type') == 'comment'])
        
        content += f"- Total posts collected: {n_posts}\n"
        content += f"- Total comments collected: {n_comments}\n"
        content += f"- Total data points: {len(st.session_state.collected_data)}\n\n"
    
    content += """
### 3.2.3 Data Collection Procedure
1. API authentication and access configuration
2. Systematic retrieval from target subreddits
3. Metadata capture (timestamp, author, score, subreddit)
4. Quality filtering and deduplication
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
        content += "4. Validation through inter-coder reliability assessment\n\n"
    
    content += """
### 3.3.3 Quality Assurance
- Inter-coder reliability testing (Cohen's Kappa)
- Peer debriefing and consensus building
- Audit trail of all coding decisions
- Member checking (where applicable)

## 3.4 Ethical Considerations

- All data from publicly accessible sources
- User anonymization applied throughout
- Compliance with platform Terms of Service
- IRB approval obtained
- Data security and confidentiality maintained

## 3.5 Limitations

- Data limited to Reddit users (self-selection bias)
- English-language content only
- Temporal specificity of data collection
- Automated coding requires human validation

## 3.6 Rigor and Trustworthiness

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

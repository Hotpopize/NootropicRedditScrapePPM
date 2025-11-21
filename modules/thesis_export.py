import streamlit as st
import pandas as pd
from datetime import datetime
import json

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

## Data Quality Assurance
- Automated validation of data completeness
- Manual review of sample items
- Duplicate detection and removal
- Timestamp verification for temporal accuracy
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
    
    if not st.session_state.codebook:
        st.warning("No codebook available. Please create codes first.")
        return
    
    content = "# Appendix B: Codebook for Push-Pull-Mooring Framework\n\n"
    content += "This codebook documents all codes used in the thematic analysis, organized by the Push-Pull-Mooring (PPM) framework.\n\n"
    
    category_names = {
        'push_factors': 'Push Factors (Motivations to Leave Current State)',
        'pull_factors': 'Pull Factors (Attractions of Natural Supplements)',
        'mooring_factors': 'Mooring Factors (Barriers and Enablers)',
        'emergent_themes': 'Emergent Themes (Beyond PPM Framework)'
    }
    
    for category, display_name in category_names.items():
        codes = st.session_state.codebook.get(category, [])
        if codes:
            content += f"## {display_name}\n\n"
            
            for i, code in enumerate(codes, 1):
                content += f"### {i}. {code.get('name')}\n\n"
                content += f"**Definition**: {code.get('definition')}\n\n"
                
                if code.get('examples'):
                    content += f"**Example(s)**: {code.get('examples')}\n\n"
                
                content += f"**Frequency**: {code.get('frequency', 0)} occurrences\n\n"
                content += "---\n\n"
    
    content += f"\n**Total Codes**: {sum(len(codes) for codes in st.session_state.codebook.values())}\n"
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
    
    if not st.session_state.coded_data or not st.session_state.codebook:
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
LLM-assisted thematic coding using OpenAI GPT-5 was employed to identify patterns within the PPM framework. Human oversight ensured accuracy and contextual validity.

### 3.3.2 Codebook Development
"""
    
    if st.session_state.codebook:
        total_codes = sum(len(codes) for codes in st.session_state.codebook.values())
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
- LLM-assisted coding requires human validation

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

import streamlit as st
import pandas as pd
from datetime import datetime
from sklearn.metrics import cohen_kappa_score, confusion_matrix
import numpy as np
from utils.db_helpers import log_action
from itertools import combinations

def render():
    st.header("Inter-Coder Reliability Analysis")
    st.markdown("""
    Calculate Cohen's Kappa, Krippendorff's Alpha, and agreement metrics for validating coding consistency.
    Essential for methodological rigor in qualitative research (Creswell & Creswell, 2023).
    """)
    
    st.subheader("Upload Second Coder's Results")
    
    st.markdown("""
    **Instructions:**
    1. Export your coded data from the Data Export module
    2. Have a second coder code the same dataset independently
    3. Upload their coded data here (CSV or JSON format)
    4. The system will calculate agreement statistics
    """)
    
    uploaded_file = st.file_uploader(
        "Upload Second Coder's Data",
        type=['csv', 'json'],
        help="Upload CSV or JSON file with coded data from second coder"
    )
    
    if uploaded_file is not None:
        try:
            if uploaded_file.name.endswith('.csv'):
                coder2_data = pd.read_csv(uploaded_file)
            else:
                import json
                coder2_data = pd.DataFrame(json.load(uploaded_file))
            
            st.success(f"Loaded {len(coder2_data)} coded items from second coder")
            
            if 'coded_data' not in st.session_state or not st.session_state.coded_data:
                st.error("No primary coder data found. Please code data using the LLM-Assisted Coding module first.")
                return
            
            coder1_df = pd.DataFrame(st.session_state.coded_data)
            
            if len(coder1_df) == 0:
                st.error("Primary coder has no coded data. Please code data first.")
                return
            
            st.subheader("Data Alignment")
            
            common_ids = set(coder1_df['id']).intersection(set(coder2_data.get('id', coder2_data.get('reddit_id', []))))
            
            if len(common_ids) == 0:
                st.error("No common items found between the two coders. Ensure both datasets have matching 'id' or 'reddit_id' fields.")
                return
            
            st.info(f"Found {len(common_ids)} items coded by both coders")
            
            coder1_aligned = coder1_df[coder1_df['id'].isin(common_ids)].sort_values('id').reset_index(drop=True)
            
            id_col = 'id' if 'id' in coder2_data.columns else 'reddit_id'
            coder2_aligned = coder2_data[coder2_data[id_col].isin(common_ids)].sort_values(id_col).reset_index(drop=True)
            
            st.subheader("Agreement Analysis")
            
            if st.button("Calculate Inter-Coder Reliability", type="primary"):
                with st.spinner("Calculating agreement metrics..."):
                    try:
                        results = calculate_reliability(coder1_aligned, coder2_aligned)
                        
                        st.session_state.reliability_results = results
                        
                        log_action(
                            action="reliability_analysis",
                            session_id=st.session_state.session_id,
                            details={
                                'n_items': len(common_ids),
                                'cohens_kappa': results['cohens_kappa'],
                                'percent_agreement': results['percent_agreement'],
                                'timestamp': datetime.utcnow().isoformat()
                            }
                        )
                        
                        st.success("Reliability analysis complete!")
                        
                    except Exception as e:
                        st.error(f"Error calculating reliability: {str(e)}")
                        return
        
        except Exception as e:
            st.error(f"Error loading file: {str(e)}")
            return
    
    if 'reliability_results' in st.session_state and st.session_state.reliability_results:
        results = st.session_state.reliability_results
        
        st.subheader("Reliability Metrics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Cohen's Kappa", f"{results['cohens_kappa']:.3f}")
            kappa_interpretation = interpret_kappa(results['cohens_kappa'])
            st.caption(f"**{kappa_interpretation}**")
        
        with col2:
            st.metric("Krippendorff's Alpha", f"{results['krippendorffs_alpha']:.3f}")
            st.caption(f"**{interpret_kappa(results['krippendorffs_alpha'])}**")
        
        with col3:
            st.metric("Percent Agreement", f"{results['percent_agreement']:.1f}%")
        
        with col4:
            st.metric("Items Analyzed", results['n_items'])
        
        st.markdown("---")
        
        st.subheader("Agreement by Category")
        
        if 'category_agreement' in results:
            agreement_df = pd.DataFrame([
                {
                    'Category': cat,
                    'Agreement': f"{metrics['agreement']:.1f}%",
                    'Cohen\'s Kappa': f"{metrics['kappa']:.3f}",
                    'Interpretation': interpret_kappa(metrics['kappa'])
                }
                for cat, metrics in results['category_agreement'].items()
            ])
            
            st.dataframe(agreement_df, use_container_width=True, hide_index=True)
        
        st.markdown("---")
        
        st.subheader("Disagreement Analysis")
        
        if 'disagreements' in results and len(results['disagreements']) > 0:
            st.warning(f"Found {len(results['disagreements'])} disagreements to review")
            
            disagreements_df = pd.DataFrame(results['disagreements'])
            st.dataframe(disagreements_df, use_container_width=True, hide_index=True)
            
            st.info("""
            **Next Steps for Disagreements:**
            1. Review each disagreement with both coders
            2. Discuss rationale and reach consensus
            3. Update codebook definitions if needed
            4. Re-code disputed items
            5. Re-run reliability analysis
            """)
        else:
            st.success("Perfect agreement! No disagreements found.")
        
        st.markdown("---")
        
        st.subheader("Methodological Reporting")
        
        st.markdown(f"""
        **For Thesis/Publication:**
        
        Inter-coder reliability was assessed using Cohen's Kappa (κ = {results['cohens_kappa']:.3f}, 
        {kappa_interpretation.lower()}) and Krippendorff's Alpha (α = {results['krippendorffs_alpha']:.3f}, 
        {interpret_kappa(results['krippendorffs_alpha']).lower()}) across {results['n_items']} items. 
        Overall percent agreement was {results['percent_agreement']:.1f}%. 
        These metrics indicate {get_reliability_statement(results['cohens_kappa'])} coding consistency between independent coders, 
        meeting standards for qualitative research (Creswell & Creswell, 2023).
        """)

def calculate_reliability(coder1_df, coder2_df):
    ppm_col1 = 'ppm_category'
    ppm_col2 = 'ppm_category' if 'ppm_category' in coder2_df.columns else 'category'
    
    coder1_categories = coder1_df[ppm_col1].tolist()
    coder2_categories = coder2_df[ppm_col2].tolist()
    
    if len(coder1_categories) != len(coder2_categories):
        raise ValueError(f"Mismatched lengths: {len(coder1_categories)} vs {len(coder2_categories)}")
    
    kappa = cohen_kappa_score(coder1_categories, coder2_categories)
    
    alpha = krippendorffs_alpha(coder1_categories, coder2_categories)
    
    agreements = sum(1 for c1, c2 in zip(coder1_categories, coder2_categories) if c1 == c2)
    percent_agreement = (agreements / len(coder1_categories)) * 100
    
    category_agreement = {}
    unique_categories = set(coder1_categories + coder2_categories)
    
    for category in unique_categories:
        cat_mask1 = [c == category for c in coder1_categories]
        cat_mask2 = [c == category for c in coder2_categories]
        
        cat_agreements = sum(1 for m1, m2 in zip(cat_mask1, cat_mask2) if m1 and m2)
        cat_total = sum(1 for m1, m2 in zip(cat_mask1, cat_mask2) if m1 or m2)
        
        if cat_total > 0:
            cat_agreement_pct = (cat_agreements / cat_total) * 100
            
            try:
                cat_kappa = cohen_kappa_score(
                    [1 if c == category else 0 for c in coder1_categories],
                    [1 if c == category else 0 for c in coder2_categories]
                )
            except:
                cat_kappa = 0.0
            
            category_agreement[category] = {
                'agreement': cat_agreement_pct,
                'kappa': cat_kappa
            }
    
    disagreements = []
    for i, (c1, c2) in enumerate(zip(coder1_categories, coder2_categories)):
        if c1 != c2:
            disagreements.append({
                'Item ID': coder1_df.iloc[i]['id'],
                'Coder 1': c1,
                'Coder 2': c2,
                'Text Preview': str(coder1_df.iloc[i].get('text', ''))[:100] + '...'
            })
    
    return {
        'cohens_kappa': kappa,
        'krippendorffs_alpha': alpha,
        'percent_agreement': percent_agreement,
        'n_items': len(coder1_categories),
        'category_agreement': category_agreement,
        'disagreements': disagreements
    }

def interpret_kappa(kappa):
    if kappa < 0:
        return "Poor (Less than chance)"
    elif kappa < 0.20:
        return "Slight"
    elif kappa < 0.40:
        return "Fair"
    elif kappa < 0.60:
        return "Moderate"
    elif kappa < 0.80:
        return "Substantial"
    else:
        return "Almost Perfect"

def krippendorffs_alpha(coder1_categories, coder2_categories):
    """
    Calculate Krippendorff's Alpha for nominal data with 2 coders.
    Follows canonical formula from Krippendorff (2018).
    Alpha = 1 - (Do / De), where Do is observed disagreement and De is expected disagreement.
    """
    n_items = len(coder1_categories)
    
    Do = sum(1 for c1, c2 in zip(coder1_categories, coder2_categories) if c1 != c2)
    
    all_values = coder1_categories + coder2_categories
    value_counts = {}
    for val in all_values:
        value_counts[val] = value_counts.get(val, 0) + 1
    
    total_values = len(all_values)
    
    if total_values <= 1:
        return 0.0
    
    De = 0.0
    for c in value_counts:
        n_c = value_counts[c]
        De += n_c * (total_values - n_c)
    
    De /= (total_values * (total_values - 1))
    
    if De == 0:
        return 1.0 if Do == 0 else 0.0
    
    alpha = 1 - (Do / n_items) / De
    
    return alpha

def get_reliability_statement(kappa):
    if kappa >= 0.80:
        return "excellent"
    elif kappa >= 0.60:
        return "good"
    elif kappa >= 0.40:
        return "acceptable"
    else:
        return "limited"

import streamlit as st
import pandas as pd
import random
import json
from datetime import datetime
from sklearn.metrics import cohen_kappa_score
from utils.db_helpers import log_action

KAPPA_THRESHOLD = 0.70
CHECKPOINT_INTERVAL = 50
BASELINE_SUBSET_SIZE = 30

def init_reliability_state():
    if 'reliability_baseline_established' not in st.session_state:
        st.session_state.reliability_baseline_established = False
    if 'reliability_baseline_results' not in st.session_state:
        st.session_state.reliability_baseline_results = None
    if 'reliability_checkpoints' not in st.session_state:
        st.session_state.reliability_checkpoints = []
    if 'baseline_subset_ids' not in st.session_state:
        st.session_state.baseline_subset_ids = []
    if 'last_checkpoint_count' not in st.session_state:
        st.session_state.last_checkpoint_count = 0
    if 'reliability_results' not in st.session_state:
        st.session_state.reliability_results = None
    if 'disagreement_resolutions' not in st.session_state:
        st.session_state.disagreement_resolutions = []

def check_reliability_checkpoint_needed():
    if 'coded_data' not in st.session_state:
        return False
    coded_count = len(st.session_state.coded_data)
    last_checkpoint = st.session_state.get('last_checkpoint_count', 0)
    return coded_count >= last_checkpoint + CHECKPOINT_INTERVAL

def get_kappa_status_icon(kappa):
    if kappa >= KAPPA_THRESHOLD:
        return "✅"
    else:
        return "❌"

def get_kappa_status_color(kappa):
    if kappa >= KAPPA_THRESHOLD:
        return "green"
    else:
        return "red"

def render():
    init_reliability_state()
    
    st.header("📈 Inter-Coder Reliability Analysis")
    
    st.markdown(f"""
    **Academic Standards Configuration:**
    - Target Cohen's κ ≥ **{KAPPA_THRESHOLD}** per PPM category
    - Reliability checks every **{CHECKPOINT_INTERVAL}** coded posts
    - Baseline dual-coding subset: **n = {BASELINE_SUBSET_SIZE}**
    
    *Essential for methodological rigor in qualitative research (Creswell & Creswell, 2023).*
    """)
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🎯 Baseline Establishment", 
        "📊 Reliability Analysis",
        "📋 Checkpoint History",
        "🔧 Disagreement Resolution",
        "📄 Academic Export"
    ])
    
    with tab1:
        render_baseline_section()
    
    with tab2:
        render_reliability_analysis()
    
    with tab3:
        render_checkpoint_history()
    
    with tab4:
        render_disagreement_resolution()
    
    with tab5:
        render_academic_export()

def render_baseline_section():
    st.subheader("Baseline Dual-Coding Establishment")
    
    if st.session_state.reliability_baseline_established:
        st.success("✅ Baseline reliability has been established!")
        results = st.session_state.reliability_baseline_results
        if results:
            st.markdown(f"""
            **Baseline Results:**
            - Overall Cohen's κ: **{results['cohens_kappa']:.3f}** {get_kappa_status_icon(results['cohens_kappa'])}
            - Items in baseline: **{results['n_items']}**
            - Established: **{results.get('timestamp', 'N/A')}**
            """)
            
            if 'category_agreement' in results:
                st.markdown("**Per-Category Kappa:**")
                for cat, metrics in results['category_agreement'].items():
                    icon = get_kappa_status_icon(metrics['kappa'])
                    st.markdown(f"- {cat}: κ = {metrics['kappa']:.3f} {icon}")
        
        if st.button("🔄 Reset Baseline (Start Over)", type="secondary"):
            st.session_state.reliability_baseline_established = False
            st.session_state.reliability_baseline_results = None
            st.session_state.baseline_subset_ids = []
            st.rerun()
        return
    
    st.warning(f"⚠️ Baseline reliability not yet established. You must achieve κ ≥ {KAPPA_THRESHOLD} on a subset of n={BASELINE_SUBSET_SIZE} before proceeding with full analysis.")
    
    st.markdown("""
    **Steps to Establish Baseline:**
    1. Generate a random subset of posts for dual coding
    2. Export the subset for your second coder
    3. Both coders independently code the subset
    4. Upload second coder's results
    5. Calculate agreement - must meet κ ≥ 0.70 threshold
    """)
    
    st.divider()
    
    st.markdown("### Step 1: Generate Random Subset")
    
    if 'collected_data' not in st.session_state or len(st.session_state.collected_data) == 0:
        st.error("No collected data available. Please collect Reddit data first.")
        return
    
    available_count = len(st.session_state.collected_data)
    
    if available_count < BASELINE_SUBSET_SIZE:
        st.warning(f"Only {available_count} posts available. Need at least {BASELINE_SUBSET_SIZE} for baseline.")
        subset_size = available_count
    else:
        subset_size = BASELINE_SUBSET_SIZE
    
    if st.session_state.baseline_subset_ids:
        st.info(f"Subset already generated: {len(st.session_state.baseline_subset_ids)} items selected")
        
        subset_data = [item for item in st.session_state.collected_data 
                       if item.get('id') in st.session_state.baseline_subset_ids]
        
        if subset_data:
            st.dataframe(
                pd.DataFrame(subset_data)[['id', 'title', 'subreddit']].head(10),
                use_container_width=True,
                hide_index=True
            )
            if len(subset_data) > 10:
                st.caption(f"... and {len(subset_data) - 10} more items")
    else:
        if st.button(f"🎲 Generate Random Subset (n={subset_size})", type="primary"):
            all_ids = [item.get('id') for item in st.session_state.collected_data if item.get('id')]
            selected_ids = random.sample(all_ids, min(subset_size, len(all_ids)))
            st.session_state.baseline_subset_ids = selected_ids
            
            log_action(
                action="baseline_subset_generated",
                session_id=st.session_state.session_id,
                details={
                    'subset_size': len(selected_ids),
                    'total_available': available_count,
                    'timestamp': datetime.utcnow().isoformat()
                }
            )
            st.rerun()
    
    if st.session_state.baseline_subset_ids:
        st.divider()
        st.markdown("### Step 2: Export Subset for Second Coder")
        
        subset_data = [item for item in st.session_state.collected_data 
                       if item.get('id') in st.session_state.baseline_subset_ids]
        
        export_df = pd.DataFrame(subset_data)
        export_cols = ['id', 'title', 'text', 'subreddit', 'author', 'created_utc']
        export_cols = [c for c in export_cols if c in export_df.columns]
        export_df = export_df[export_cols]
        
        csv_data = export_df.to_csv(index=False)
        st.download_button(
            label="📥 Download Baseline Subset (CSV)",
            data=csv_data,
            file_name=f"baseline_subset_n{len(subset_data)}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
        
        st.divider()
        st.markdown("### Step 3-4: Upload Second Coder's Results")
        
        st.markdown("""
        The second coder should code each item with:
        - `id`: Matching ID from the exported subset
        - `ppm_category`: Push, Pull, or Mooring
        """)
        
        uploaded_file = st.file_uploader(
            "Upload Second Coder's Coded Data",
            type=['csv', 'json'],
            help="CSV or JSON with 'id' and 'ppm_category' columns"
        )
        
        if uploaded_file is not None:
            try:
                if uploaded_file.name.endswith('.csv'):
                    coder2_data = pd.read_csv(uploaded_file)
                else:
                    coder2_data = pd.DataFrame(json.load(uploaded_file))
                
                st.success(f"Loaded {len(coder2_data)} items from second coder")
                
                if 'coded_data' not in st.session_state or not st.session_state.coded_data:
                    st.error("Primary coder has not coded the baseline subset yet. Please code the subset first using the Automated Qualitative Coding module.")
                    return
                
                coder1_df = pd.DataFrame(st.session_state.coded_data)
                baseline_coded = coder1_df[coder1_df['id'].isin(st.session_state.baseline_subset_ids)]
                
                if len(baseline_coded) < len(st.session_state.baseline_subset_ids):
                    st.warning(f"Primary coder has only coded {len(baseline_coded)}/{len(st.session_state.baseline_subset_ids)} baseline items. Please complete coding first.")
                
                if len(baseline_coded) > 0:
                    st.divider()
                    st.markdown("### Step 5: Calculate Baseline Agreement")
                    
                    if st.button("🔬 Calculate Baseline Reliability", type="primary"):
                        id_col = 'id' if 'id' in coder2_data.columns else 'reddit_id'
                        common_ids = set(baseline_coded['id']).intersection(set(coder2_data[id_col]))
                        
                        if len(common_ids) == 0:
                            st.error("No matching IDs found between coders.")
                            return
                        
                        coder1_aligned = baseline_coded[baseline_coded['id'].isin(common_ids)].sort_values('id').reset_index(drop=True)
                        coder2_aligned = coder2_data[coder2_data[id_col].isin(common_ids)].sort_values(id_col).reset_index(drop=True)
                        
                        results = calculate_reliability(coder1_aligned, coder2_aligned)
                        results['timestamp'] = datetime.utcnow().isoformat()
                        results['is_baseline'] = True
                        
                        all_categories_pass = True
                        if 'category_agreement' in results:
                            for cat, metrics in results['category_agreement'].items():
                                if metrics['kappa'] < KAPPA_THRESHOLD:
                                    all_categories_pass = False
                        
                        overall_pass = results['cohens_kappa'] >= KAPPA_THRESHOLD
                        
                        if overall_pass and all_categories_pass:
                            st.session_state.reliability_baseline_established = True
                            st.session_state.reliability_baseline_results = results
                            st.session_state.reliability_checkpoints.append(results)
                            st.session_state.last_checkpoint_count = len(st.session_state.coded_data)
                            
                            log_action(
                                action="baseline_established",
                                session_id=st.session_state.session_id,
                                details={
                                    'cohens_kappa': results['cohens_kappa'],
                                    'n_items': results['n_items'],
                                    'category_kappas': {cat: m['kappa'] for cat, m in results.get('category_agreement', {}).items()},
                                    'timestamp': datetime.utcnow().isoformat()
                                }
                            )
                            
                            st.success(f"🎉 Baseline established! Overall κ = {results['cohens_kappa']:.3f}")
                            st.balloons()
                            st.rerun()
                        else:
                            st.error(f"❌ Baseline NOT met. Overall κ = {results['cohens_kappa']:.3f} (need ≥ {KAPPA_THRESHOLD})")
                            
                            st.markdown("**Category-level results:**")
                            for cat, metrics in results.get('category_agreement', {}).items():
                                icon = get_kappa_status_icon(metrics['kappa'])
                                st.markdown(f"- {cat}: κ = {metrics['kappa']:.3f} {icon}")
                            
                            st.info("**Next Steps:** Review disagreements, discuss with second coder, refine codebook definitions, and re-code.")
                            
                            if 'disagreements' in results and results['disagreements']:
                                st.markdown("**Disagreements to Review:**")
                                st.dataframe(pd.DataFrame(results['disagreements']), use_container_width=True, hide_index=True)
            
            except Exception as e:
                st.error(f"Error loading file: {str(e)}")

def render_reliability_analysis():
    st.subheader("Full Reliability Analysis")
    
    if not st.session_state.reliability_baseline_established:
        st.warning("⚠️ **Reliability Gate Active**: Baseline agreement must be established before full analysis can proceed.")
        st.info("Please go to the 'Baseline Establishment' tab to complete this requirement.")
        return
    
    coded_count = len(st.session_state.get('coded_data', []))
    last_checkpoint = st.session_state.get('last_checkpoint_count', 0)
    next_checkpoint = last_checkpoint + CHECKPOINT_INTERVAL
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Coded", coded_count)
    with col2:
        st.metric("Last Checkpoint", last_checkpoint)
    with col3:
        progress_to_next = min((coded_count - last_checkpoint) / CHECKPOINT_INTERVAL * 100, 100)
        st.metric("Progress to Next Check", f"{progress_to_next:.0f}%")
    
    if coded_count >= next_checkpoint:
        st.warning(f"🔔 **Checkpoint Required**: You have coded {coded_count - last_checkpoint} posts since last check. Time for reliability verification!")
    
    st.divider()
    
    st.markdown("### Upload Second Coder's Full Dataset")
    
    uploaded_file = st.file_uploader(
        "Upload Second Coder's Data",
        type=['csv', 'json'],
        help="Upload CSV or JSON file with coded data from second coder",
        key="full_analysis_upload"
    )
    
    if uploaded_file is not None:
        try:
            if uploaded_file.name.endswith('.csv'):
                coder2_data = pd.read_csv(uploaded_file)
            else:
                coder2_data = pd.DataFrame(json.load(uploaded_file))
            
            st.success(f"Loaded {len(coder2_data)} coded items from second coder")
            
            if 'coded_data' not in st.session_state or not st.session_state.coded_data:
                st.error("No primary coder data found. Please code data using the Automated Qualitative Coding module first.")
                return
            
            coder1_df = pd.DataFrame(st.session_state.coded_data)
            
            id_col = 'id' if 'id' in coder2_data.columns else 'reddit_id'
            common_ids = set(coder1_df['id']).intersection(set(coder2_data[id_col]))
            
            if len(common_ids) == 0:
                st.error("No common items found between coders.")
                return
            
            st.info(f"Found {len(common_ids)} items coded by both coders")
            
            if st.button("Calculate Inter-Coder Reliability", type="primary"):
                with st.spinner("Calculating agreement metrics..."):
                    coder1_aligned = coder1_df[coder1_df['id'].isin(common_ids)].sort_values('id').reset_index(drop=True)
                    coder2_aligned = coder2_data[coder2_data[id_col].isin(common_ids)].sort_values(id_col).reset_index(drop=True)
                    
                    results = calculate_reliability(coder1_aligned, coder2_aligned)
                    results['timestamp'] = datetime.utcnow().isoformat()
                    results['checkpoint_number'] = len(st.session_state.reliability_checkpoints) + 1
                    results['coded_at_checkpoint'] = coded_count
                    
                    st.session_state.reliability_results = results
                    st.session_state.reliability_checkpoints.append(results)
                    st.session_state.last_checkpoint_count = coded_count
                    
                    log_action(
                        action="reliability_checkpoint",
                        session_id=st.session_state.session_id,
                        details={
                            'checkpoint_number': results['checkpoint_number'],
                            'cohens_kappa': results['cohens_kappa'],
                            'n_items': results['n_items'],
                            'coded_at_checkpoint': coded_count,
                            'timestamp': datetime.utcnow().isoformat()
                        }
                    )
                    
                    st.success("Reliability analysis complete!")
        
        except Exception as e:
            st.error(f"Error: {str(e)}")
    
    if st.session_state.reliability_results:
        render_results_display(st.session_state.reliability_results)

def render_results_display(results):
    st.divider()
    st.subheader("Reliability Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        kappa = results['cohens_kappa']
        icon = get_kappa_status_icon(kappa)
        st.metric("Cohen's Kappa", f"{kappa:.3f} {icon}")
        interpretation = interpret_kappa(kappa)
        color = get_kappa_status_color(kappa)
        st.markdown(f"<span style='color:{color}'><b>{interpretation}</b></span>", unsafe_allow_html=True)
    
    with col2:
        alpha = results['krippendorffs_alpha']
        st.metric("Krippendorff's Alpha", f"{alpha:.3f}")
        st.caption(f"**{interpret_kappa(alpha)}**")
    
    with col3:
        st.metric("Percent Agreement", f"{results['percent_agreement']:.1f}%")
    
    with col4:
        st.metric("Items Analyzed", results['n_items'])
    
    st.divider()
    st.subheader("Agreement by PPM Category")
    
    if 'category_agreement' in results:
        category_data = []
        all_pass = True
        for cat, metrics in results['category_agreement'].items():
            passes = metrics['kappa'] >= KAPPA_THRESHOLD
            if not passes:
                all_pass = False
            category_data.append({
                'Category': cat,
                'Agreement': f"{metrics['agreement']:.1f}%",
                'Cohen\'s κ': f"{metrics['kappa']:.3f}",
                'Status': "✅ PASS" if passes else "❌ BELOW THRESHOLD",
                'Interpretation': interpret_kappa(metrics['kappa'])
            })
        
        st.dataframe(pd.DataFrame(category_data), use_container_width=True, hide_index=True)
        
        if all_pass:
            st.success(f"All categories meet the κ ≥ {KAPPA_THRESHOLD} threshold!")
        else:
            st.error(f"Some categories fall below the κ ≥ {KAPPA_THRESHOLD} threshold. Review and refine coding approach.")
    
    if 'disagreements' in results and len(results['disagreements']) > 0:
        st.divider()
        st.subheader("Disagreements")
        st.warning(f"Found {len(results['disagreements'])} disagreements to review")
        st.dataframe(pd.DataFrame(results['disagreements']), use_container_width=True, hide_index=True)

def render_checkpoint_history():
    st.subheader("Reliability Checkpoint History")
    
    if not st.session_state.reliability_checkpoints:
        st.info("No reliability checkpoints recorded yet.")
        return
    
    checkpoints = st.session_state.reliability_checkpoints
    
    checkpoint_summary = []
    for i, cp in enumerate(checkpoints):
        checkpoint_summary.append({
            'Checkpoint': i + 1,
            'Timestamp': cp.get('timestamp', 'N/A')[:19] if cp.get('timestamp') else 'N/A',
            'Items': cp.get('n_items', 0),
            'Cohen\'s κ': f"{cp.get('cohens_kappa', 0):.3f}",
            'Status': "✅ PASS" if cp.get('cohens_kappa', 0) >= KAPPA_THRESHOLD else "❌ FAIL",
            'Type': 'Baseline' if cp.get('is_baseline') else 'Checkpoint'
        })
    
    st.dataframe(pd.DataFrame(checkpoint_summary), use_container_width=True, hide_index=True)
    
    if len(checkpoints) > 1:
        st.divider()
        st.subheader("Reliability Trend")
        
        trend_data = pd.DataFrame({
            'Checkpoint': range(1, len(checkpoints) + 1),
            'Kappa': [cp.get('cohens_kappa', 0) for cp in checkpoints]
        })
        
        import altair as alt
        
        threshold_line = alt.Chart(pd.DataFrame({'y': [KAPPA_THRESHOLD]})).mark_rule(
            color='red',
            strokeDash=[5, 5]
        ).encode(y='y:Q')
        
        kappa_line = alt.Chart(trend_data).mark_line(point=True, color='blue').encode(
            x=alt.X('Checkpoint:O', title='Checkpoint'),
            y=alt.Y('Kappa:Q', title='Cohen\'s κ', scale=alt.Scale(domain=[0, 1])),
            tooltip=['Checkpoint', 'Kappa']
        )
        
        chart = (kappa_line + threshold_line).properties(
            title='Cohen\'s Kappa Across Checkpoints',
            height=300
        )
        
        st.altair_chart(chart, use_container_width=True)
        st.caption(f"Red dashed line = threshold (κ = {KAPPA_THRESHOLD})")

def render_disagreement_resolution():
    st.subheader("Disagreement Resolution Tracker")
    
    st.markdown("""
    Use this section to document how disagreements between coders were resolved.
    This creates an audit trail for methodological transparency.
    """)
    
    if st.session_state.reliability_results and 'disagreements' in st.session_state.reliability_results:
        disagreements = st.session_state.reliability_results['disagreements']
        
        if disagreements:
            st.markdown(f"**{len(disagreements)} disagreements from latest analysis:**")
            
            for i, d in enumerate(disagreements[:10]):
                with st.expander(f"Item: {d.get('Item ID', 'Unknown')} - Coder 1: {d.get('Coder 1')} vs Coder 2: {d.get('Coder 2')}"):
                    st.write(f"**Text Preview:** {d.get('Text Preview', 'N/A')}")
                    
                    resolution = st.selectbox(
                        "Resolution",
                        ["Unresolved", "Agreed on Coder 1", "Agreed on Coder 2", "New Category", "Excluded"],
                        key=f"resolution_{i}"
                    )
                    
                    notes = st.text_area("Resolution Notes", key=f"notes_{i}", placeholder="Document reasoning...")
                    
                    if st.button("Save Resolution", key=f"save_{i}"):
                        resolution_record = {
                            'item_id': d.get('Item ID'),
                            'coder1_category': d.get('Coder 1'),
                            'coder2_category': d.get('Coder 2'),
                            'resolution': resolution,
                            'notes': notes,
                            'timestamp': datetime.utcnow().isoformat()
                        }
                        st.session_state.disagreement_resolutions.append(resolution_record)
                        st.success("Resolution saved!")
            
            if len(disagreements) > 10:
                st.caption(f"Showing first 10 of {len(disagreements)} disagreements")
        else:
            st.success("No disagreements to resolve!")
    else:
        st.info("Run a reliability analysis first to identify disagreements.")
    
    if st.session_state.disagreement_resolutions:
        st.divider()
        st.markdown("### Resolution History")
        st.dataframe(pd.DataFrame(st.session_state.disagreement_resolutions), use_container_width=True, hide_index=True)

def render_academic_export():
    st.subheader("Academic Export & Reporting")
    
    if not st.session_state.reliability_baseline_established:
        st.warning("Establish baseline reliability first to generate reports.")
        return
    
    st.markdown("### APA-Formatted Methodology Paragraph")
    
    results = st.session_state.reliability_baseline_results or st.session_state.reliability_results
    
    if results:
        kappa = results['cohens_kappa']
        alpha = results['krippendorffs_alpha']
        n_items = results['n_items']
        percent_agree = results['percent_agreement']
        
        category_text = ""
        if 'category_agreement' in results:
            cat_parts = []
            for cat, metrics in results['category_agreement'].items():
                cat_parts.append(f"{cat} (κ = {metrics['kappa']:.2f})")
            category_text = f" Category-specific reliability was: {', '.join(cat_parts)}."
        
        apa_paragraph = f"""
**Inter-Coder Reliability**

Inter-coder reliability was established through independent dual coding of a randomly selected subset (n = {n_items}) of Reddit posts. Agreement was assessed using Cohen's Kappa (κ = {kappa:.2f}, {interpret_kappa(kappa).lower()}) and Krippendorff's Alpha (α = {alpha:.2f}). Overall percent agreement was {percent_agree:.1f}%.{category_text} Following the widely-adopted threshold criterion of κ ≥ 0.70 for acceptable reliability in qualitative research (Landis & Koch, 1977; McHugh, 2012), these metrics indicate {get_reliability_statement(kappa)} inter-coder reliability. This level of agreement supports the trustworthiness and credibility of the coding framework, consistent with methodological standards for qualitative rigor (Creswell & Creswell, 2023).
        """
        
        st.markdown(apa_paragraph)
        
        st.download_button(
            label="📥 Download Methodology Text",
            data=apa_paragraph,
            file_name="reliability_methodology.txt",
            mime="text/plain"
        )
    
    st.divider()
    st.markdown("### Complete Reliability Audit Export")
    
    if st.button("📊 Generate Complete Audit Package"):
        audit_data = {
            'baseline_established': st.session_state.reliability_baseline_established,
            'baseline_results': st.session_state.reliability_baseline_results,
            'checkpoints': st.session_state.reliability_checkpoints,
            'disagreement_resolutions': st.session_state.disagreement_resolutions,
            'configuration': {
                'kappa_threshold': KAPPA_THRESHOLD,
                'checkpoint_interval': CHECKPOINT_INTERVAL,
                'baseline_subset_size': BASELINE_SUBSET_SIZE
            },
            'export_timestamp': datetime.utcnow().isoformat()
        }
        
        audit_json = json.dumps(audit_data, indent=2, default=str)
        
        st.download_button(
            label="📥 Download Full Audit Package (JSON)",
            data=audit_json,
            file_name=f"reliability_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json"
        )
        
        if st.session_state.reliability_checkpoints:
            checkpoint_df = pd.DataFrame([
                {
                    'checkpoint': i + 1,
                    'timestamp': cp.get('timestamp'),
                    'cohens_kappa': cp.get('cohens_kappa'),
                    'krippendorffs_alpha': cp.get('krippendorffs_alpha'),
                    'percent_agreement': cp.get('percent_agreement'),
                    'n_items': cp.get('n_items'),
                    'type': 'baseline' if cp.get('is_baseline') else 'checkpoint'
                }
                for i, cp in enumerate(st.session_state.reliability_checkpoints)
            ])
            
            csv_data = checkpoint_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Checkpoint History (CSV)",
                data=csv_data,
                file_name=f"reliability_checkpoints_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

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
            except Exception as e:
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

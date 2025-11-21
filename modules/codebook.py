import streamlit as st
import json
import pandas as pd
from datetime import datetime

def render():
    st.header("📖 Codebook Management")
    
    st.info("""
    A **codebook** is essential for qualitative research transparency and replicability.
    It documents all codes, their definitions, and examples from your data.
    
    This codebook tracks both:
    - **Deductive codes** from the PPM framework
    - **Inductive codes** that emerge from your data
    """)
    
    st.divider()
    
    tab1, tab2, tab3 = st.tabs(["📚 View Codebook", "➕ Add/Edit Codes", "💾 Export Codebook"])
    
    with tab1:
        st.subheader("Current Codebook")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("### Push Factors")
            if st.session_state.codebook['push_factors']:
                for code in st.session_state.codebook['push_factors']:
                    with st.expander(f"🔴 {code['name']}"):
                        st.write(f"**Definition:** {code['definition']}")
                        st.write(f"**Examples:** {code.get('examples', 'No examples yet')}")
                        st.write(f"**Frequency:** {code.get('frequency', 0)} occurrences")
            else:
                st.caption("No push factor codes defined yet")
            
            st.write("### Pull Factors")
            if st.session_state.codebook['pull_factors']:
                for code in st.session_state.codebook['pull_factors']:
                    with st.expander(f"🟢 {code['name']}"):
                        st.write(f"**Definition:** {code['definition']}")
                        st.write(f"**Examples:** {code.get('examples', 'No examples yet')}")
                        st.write(f"**Frequency:** {code.get('frequency', 0)} occurrences")
            else:
                st.caption("No pull factor codes defined yet")
        
        with col2:
            st.write("### Mooring Factors")
            if st.session_state.codebook['mooring_factors']:
                for code in st.session_state.codebook['mooring_factors']:
                    with st.expander(f"🔵 {code['name']}"):
                        st.write(f"**Definition:** {code['definition']}")
                        st.write(f"**Examples:** {code.get('examples', 'No examples yet')}")
                        st.write(f"**Frequency:** {code.get('frequency', 0)} occurrences")
            else:
                st.caption("No mooring factor codes defined yet")
            
            st.write("### Emergent Themes")
            if st.session_state.codebook['emergent_themes']:
                for code in st.session_state.codebook['emergent_themes']:
                    with st.expander(f"⭐ {code['name']}"):
                        st.write(f"**Definition:** {code['definition']}")
                        st.write(f"**Examples:** {code.get('examples', 'No examples yet')}")
                        st.write(f"**Frequency:** {code.get('frequency', 0)} occurrences")
            else:
                st.caption("No emergent themes defined yet")
        
        if st.button("📊 Calculate Code Frequencies from Coded Data"):
            update_code_frequencies()
            st.success("✅ Code frequencies updated!")
            st.rerun()
    
    with tab2:
        st.subheader("Add or Edit Code")
        
        code_category = st.selectbox(
            "Code Category",
            ["Push Factors", "Pull Factors", "Mooring Factors", "Emergent Themes"]
        )
        
        code_name = st.text_input("Code Name", help="Short, descriptive name for the code")
        code_definition = st.text_area("Code Definition", help="Clear definition explaining when to apply this code")
        code_examples = st.text_area("Examples", help="Example quotes or scenarios from your data")
        
        if st.button("💾 Save Code", type="primary"):
            if code_name and code_definition:
                new_code = {
                    'name': code_name,
                    'definition': code_definition,
                    'examples': code_examples,
                    'added_at': datetime.now().isoformat(),
                    'frequency': 0
                }
                
                category_key = {
                    'Push Factors': 'push_factors',
                    'Pull Factors': 'pull_factors',
                    'Mooring Factors': 'mooring_factors',
                    'Emergent Themes': 'emergent_themes'
                }[code_category]
                
                existing_names = [c['name'] for c in st.session_state.codebook[category_key]]
                if code_name in existing_names:
                    idx = existing_names.index(code_name)
                    st.session_state.codebook[category_key][idx] = new_code
                    st.success(f"✅ Updated code '{code_name}' in {code_category}")
                else:
                    st.session_state.codebook[category_key].append(new_code)
                    st.success(f"✅ Added new code '{code_name}' to {code_category}")
                
                st.rerun()
            else:
                st.error("⚠️ Please provide both a code name and definition")
        
        st.divider()
        
        st.subheader("🗑️ Delete Code")
        delete_category = st.selectbox(
            "Category to delete from",
            ["Push Factors", "Pull Factors", "Mooring Factors", "Emergent Themes"],
            key="delete_cat"
        )
        
        category_key = {
            'Push Factors': 'push_factors',
            'Pull Factors': 'pull_factors',
            'Mooring Factors': 'mooring_factors',
            'Emergent Themes': 'emergent_themes'
        }[delete_category]
        
        if st.session_state.codebook[category_key]:
            code_to_delete = st.selectbox(
                "Select code to delete",
                [c['name'] for c in st.session_state.codebook[category_key]]
            )
            
            if st.button("🗑️ Delete Code", type="secondary"):
                st.session_state.codebook[category_key] = [
                    c for c in st.session_state.codebook[category_key] 
                    if c['name'] != code_to_delete
                ]
                st.success(f"✅ Deleted code '{code_to_delete}'")
                st.rerun()
        else:
            st.caption("No codes to delete in this category")
    
    with tab3:
        st.subheader("Export Codebook")
        
        st.write("Export your codebook for inclusion in your thesis appendix or for sharing with other coders.")
        
        export_format = st.selectbox("Export Format", ["JSON", "CSV", "Markdown"])
        
        if st.button("📥 Generate Export"):
            if export_format == "JSON":
                export_data = json.dumps(st.session_state.codebook, indent=2)
                st.download_button(
                    label="⬇️ Download Codebook (JSON)",
                    data=export_data,
                    file_name=f"codebook_{st.session_state.session_id}.json",
                    mime="application/json"
                )
            
            elif export_format == "CSV":
                rows = []
                for category, codes in st.session_state.codebook.items():
                    for code in codes:
                        rows.append({
                            'Category': category.replace('_', ' ').title(),
                            'Code Name': code['name'],
                            'Definition': code['definition'],
                            'Examples': code.get('examples', ''),
                            'Frequency': code.get('frequency', 0),
                            'Added At': code.get('added_at', '')
                        })
                
                if rows:
                    df = pd.DataFrame(rows)
                    csv_data = df.to_csv(index=False)
                    st.download_button(
                        label="⬇️ Download Codebook (CSV)",
                        data=csv_data,
                        file_name=f"codebook_{st.session_state.session_id}.csv",
                        mime="text/csv"
                    )
                else:
                    st.warning("No codes to export")
            
            elif export_format == "Markdown":
                md_content = "# Codebook: Natural Cognitive Supplement Market Segmentation\n\n"
                md_content += f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                md_content += f"**Session ID:** {st.session_state.session_id}\n\n"
                md_content += "---\n\n"
                
                for category, codes in st.session_state.codebook.items():
                    md_content += f"## {category.replace('_', ' ').title()}\n\n"
                    if codes:
                        for code in codes:
                            md_content += f"### {code['name']}\n\n"
                            md_content += f"**Definition:** {code['definition']}\n\n"
                            if code.get('examples'):
                                md_content += f"**Examples:**\n{code['examples']}\n\n"
                            md_content += f"**Frequency:** {code.get('frequency', 0)} occurrences\n\n"
                            md_content += "---\n\n"
                    else:
                        md_content += "*No codes defined yet*\n\n"
                
                st.download_button(
                    label="⬇️ Download Codebook (Markdown)",
                    data=md_content,
                    file_name=f"codebook_{st.session_state.session_id}.md",
                    mime="text/markdown"
                )
        
        st.divider()
        
        st.subheader("📄 Codebook Preview")
        
        preview_md = "## Codebook Summary\n\n"
        for category, codes in st.session_state.codebook.items():
            preview_md += f"**{category.replace('_', ' ').title()}:** {len(codes)} codes\n\n"
        
        st.markdown(preview_md)

def update_code_frequencies():
    for category_key in ['push_factors', 'pull_factors', 'mooring_factors', 'emergent_themes']:
        for code in st.session_state.codebook[category_key]:
            code['frequency'] = 0
    
    for item in st.session_state.coded_data:
        ppm_category = item.get('ppm_category', '').lower()
        
        if ppm_category == 'push':
            category_key = 'push_factors'
        elif ppm_category == 'pull':
            category_key = 'pull_factors'
        elif ppm_category == 'mooring':
            category_key = 'mooring_factors'
        else:
            category_key = None
        
        if category_key:
            for subcode in item.get('ppm_subcodes', []):
                for code in st.session_state.codebook[category_key]:
                    if code['name'].lower() in subcode.lower() or subcode.lower() in code['name'].lower():
                        code['frequency'] = code.get('frequency', 0) + 1
        
        for theme in item.get('themes', []):
            for code in st.session_state.codebook['emergent_themes']:
                if code['name'].lower() in theme.lower() or theme.lower() in code['name'].lower():
                    code['frequency'] = code.get('frequency', 0) + 1

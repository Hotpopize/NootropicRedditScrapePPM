import streamlit as st
import pandas as pd
import json
import os
from datetime import datetime

def render():
    st.header("💾 Data Export & Audit Trail")
    
    st.info("""
    Export your data for analysis in external tools like **NVivo**, **MAXQDA**, or **Python/R**.
    All actions are logged for methodological transparency and replicability per Creswell & Creswell (2023).
    """)
    
    st.divider()
    
    tab1, tab1b, tab2, tab3 = st.tabs(["📥 Export Data", "📂 Upload Dataset", "📜 Audit Trail", "🔄 Session Management"])
    
    with tab1:
        st.subheader("Export Research Data")
        
        export_type = st.selectbox(
            "Select Data to Export",
            ["Collected Data (Raw)", "Coded Data", "Both (Collected + Coded)", "Codebook"]
        )
        
        export_format = st.selectbox(
            "Export Format",
            ["CSV (Excel/SPSS compatible)", "JSON (NVivo/MAXQDA import)", "Excel (.xlsx)"]
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            include_metadata = st.checkbox("Include metadata fields", value=True, help="Author, score, timestamps, etc.")
        
        with col2:
            flatten_nested = st.checkbox("Flatten nested fields", value=True, help="Convert lists to comma-separated strings")
        
        if st.button("📥 Generate Export", type="primary"):
            if export_type == "Collected Data (Raw)":
                data_to_export = st.session_state.collected_data
                filename_prefix = "collected_data"
            elif export_type == "Coded Data":
                data_to_export = st.session_state.coded_data
                filename_prefix = "coded_data"
            elif export_type == "Both (Collected + Coded)":
                data_to_export = {
                    'collected': st.session_state.collected_data,
                    'coded': st.session_state.coded_data
                }
                filename_prefix = "research_data"
            else:
                data_to_export = st.session_state.codebook_manager.to_dict()
                filename_prefix = "codebook"
            
            if not data_to_export:
                st.warning("⚠️ No data available to export")
                return
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if export_format == "CSV (Excel/SPSS compatible)":
                if export_type == "Both (Collected + Coded)":
                    for key, data in data_to_export.items():
                        if data:
                            df = prepare_dataframe(data, flatten_nested, include_metadata)
                            csv_data = df.to_csv(index=False)
                            st.download_button(
                                label=f"⬇️ Download {key.title()} Data (CSV)",
                                data=csv_data,
                                file_name=f"{key}_{st.session_state.session_id}_{timestamp}.csv",
                                mime="text/csv"
                            )
                else:
                    df = prepare_dataframe(data_to_export, flatten_nested, include_metadata)
                    csv_data = df.to_csv(index=False)
                    st.download_button(
                        label="⬇️ Download CSV",
                        data=csv_data,
                        file_name=f"{filename_prefix}_{st.session_state.session_id}_{timestamp}.csv",
                        mime="text/csv"
                    )
            
            elif export_format == "JSON (NVivo/MAXQDA import)":
                json_data = json.dumps(data_to_export, indent=2, default=str)
                st.download_button(
                    label="⬇️ Download JSON",
                    data=json_data,
                    file_name=f"{filename_prefix}_{st.session_state.session_id}_{timestamp}.json",
                    mime="application/json"
                )
            
            elif export_format == "Excel (.xlsx)":
                try:
                    from io import BytesIO
                    if export_type == "Both (Collected + Coded)":
                        st.warning("Excel export with multiple sheets requires pandas ExcelWriter. Use CSV for now or export separately.")
                    else:
                        df = prepare_dataframe(data_to_export, flatten_nested, include_metadata)
                        excel_buffer = BytesIO()
                        df.to_excel(excel_buffer, index=False, engine='openpyxl')
                        excel_buffer.seek(0)
                        st.download_button(
                            label="⬇️ Download Excel",
                            data=excel_buffer,
                            file_name=f"{filename_prefix}_{st.session_state.session_id}_{timestamp}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                except Exception as e:
                    st.error(f"Excel export error: {str(e)}. Please use CSV format instead.")
            
            log_export_action(export_type, export_format)
            st.success("✅ Export generated successfully!")
        
        st.divider()
        
        st.subheader("📊 Data Summary")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Collected Items", len(st.session_state.collected_data))
        
        with col2:
            st.metric("Coded Items", len(st.session_state.coded_data))
        
        
        with col3:
            total_codes = len(st.session_state.codebook_manager.get_all())
            st.metric("Codebook Entries", total_codes)
    
    with tab1b:
        st.subheader("📂 Upload External Dataset")
        st.write("Upload a JSON file containing raw Reddit posts (e.g., mock data or previous exports) directly into your current session.")
        
        uploaded_dataset = st.file_uploader("Upload JSON Dataset", type=['json'], key="dataset_uploader")
        
        if uploaded_dataset is not None:
            try:
                new_data = json.load(uploaded_dataset)
                
                if isinstance(new_data, dict) and 'collected' in new_data:
                    # Handle "Both" export format
                    items_to_add = new_data['collected']
                elif isinstance(new_data, list):
                    # Handle raw list of posts
                    items_to_add = new_data
                else:
                    st.error("Invalid JSON structure. Expected a list of posts.")
                    items_to_add = []
                
                if items_to_add:
                    st.write(f"**Found {len(items_to_add)} items in the uploaded dataset.**")
                    
                    if st.button("📥 Import into Collected Data", type="primary"):
                        # Append to existing
                        if 'collected_data' not in st.session_state:
                            st.session_state.collected_data = []
                            
                        # Basic deduplication by ID
                        existing_ids = {item.get('id') for item in st.session_state.collected_data if item.get('id')}
                        added_count = 0
                        
                        for item in items_to_add:
                            item_id = item.get('id')
                            if not item_id or item_id not in existing_ids:
                                st.session_state.collected_data.append(item)
                                if item_id:
                                    existing_ids.add(item_id)
                                added_count += 1
                        
                        st.success(f"✅ Successfully imported {added_count} new items! (Skipped {len(items_to_add) - added_count} duplicates)")
                        st.rerun()
            except Exception as e:
                st.error(f"Error reading JSON file: {e}")
                
    with tab2:
        st.subheader("📜 Session Audit Trail")
        
        st.write("""
        The audit trail logs all data collection and coding activities for methodological transparency.
        This is essential for demonstrating rigor and replicability in qualitative research.
        """)
        
        audit_file = f'logs/audit_{st.session_state.session_id}.jsonl'
        
        if os.path.exists(audit_file):
            with open(audit_file, 'r') as f:
                audit_logs = [json.loads(line) for line in f]
            
            st.write(f"**Total Log Entries:** {len(audit_logs)}")
            
            filter_action = st.selectbox(
                "Filter by Action",
                ["All", "data_collection", "automated_coding_ollama", "export"]
            )
            
            if filter_action != "All":
                filtered_logs = [log for log in audit_logs if log.get('action') == filter_action]
            else:
                filtered_logs = audit_logs
            
            for log in filtered_logs[-20:]:
                with st.expander(f"{log.get('timestamp', 'N/A')} - {log.get('action', 'Unknown').upper()}"):
                    st.json(log)
            
            if st.button("📥 Download Complete Audit Trail"):
                audit_data = json.dumps(audit_logs, indent=2)
                st.download_button(
                    label="⬇️ Download Audit Log (JSON)",
                    data=audit_data,
                    file_name=f"audit_trail_{st.session_state.session_id}.json",
                    mime="application/json"
                )
        else:
            st.info("No audit trail yet. Start collecting or coding data to generate logs.")
    
    with tab3:
        st.subheader("🔄 Session Management")
        
        st.write(f"**Current Session ID:** `{st.session_state.session_id}`")
        st.write(f"**Session Started:** {st.session_state.session_id[:8]}")
        
        st.divider()
        
        st.subheader("💾 Save Session State")
        
        st.write("Save your current session to resume later or share with collaborators.")
        
        if st.button("💾 Save Session to File"):
            session_data = {
                'session_id': st.session_state.session_id,
                'saved_at': datetime.now().isoformat(),
                'collected_data': st.session_state.collected_data,
                'coded_data': st.session_state.coded_data,
                'codebook': st.session_state.codebook_manager.to_dict()
            }
            
            os.makedirs('exports', exist_ok=True)
            session_file = f'exports/session_{st.session_state.session_id}.json'
            
            with open(session_file, 'w') as f:
                json.dump(session_data, f, indent=2, default=str)
            
            st.success(f"✅ Session saved to {session_file}")
            
            with open(session_file, 'r') as f:
                st.download_button(
                    label="⬇️ Download Session File",
                    data=f.read(),
                    file_name=f"session_{st.session_state.session_id}.json",
                    mime="application/json"
                )
        
        st.divider()
        
        st.subheader("📂 Load Session State")
        
        uploaded_file = st.file_uploader("Upload a saved session file", type=['json'])
        
        if uploaded_file is not None:
            try:
                session_data = json.load(uploaded_file)
                
                st.write(f"**Session ID:** {session_data.get('session_id', 'Unknown')}")
                st.write(f"**Saved At:** {session_data.get('saved_at', 'Unknown')}")
                st.write(f"**Collected Items:** {len(session_data.get('collected_data', []))}")
                st.write(f"**Coded Items:** {len(session_data.get('coded_data', []))}")
                
                if st.button("🔄 Load This Session", type="primary"):
                    st.session_state.collected_data = session_data.get('collected_data', [])
                    st.session_state.coded_data = session_data.get('coded_data', [])
                    
                    # Load codebook
                    cb_data = session_data.get('codebook')
                    if cb_data:
                        # Handle both legacy dict format and new manager format
                        if 'codes' in cb_data:
                            from modules.codebook import CodebookManager
                            st.session_state.codebook_manager = CodebookManager.from_dict(cb_data)
                        else:
                            # Convert legacy dict to manager
                            from modules.codebook import CodebookManager, Code, CodeCategory, DEFAULT_CODES
                            st.session_state.codebook_manager = CodebookManager() # Fallback/Reset
                    else:
                        from modules.codebook import CodebookManager
                        st.session_state.codebook_manager = CodebookManager()
                    
                    st.success("✅ Session loaded successfully!")
                    st.rerun()
            
            except Exception as e:
                st.error(f"❌ Error loading session: {str(e)}")
        
        st.divider()
        
        st.subheader("🗑️ Clear Session Data")
        
        st.warning("⚠️ This will permanently delete all data in the current session. Make sure to export first!")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🗑️ Clear Collected Data"):
                st.session_state.collected_data = []
                st.success("✅ Collected data cleared")
                st.rerun()
        
        with col2:
            if st.button("🗑️ Clear Coded Data"):
                st.session_state.coded_data = []
                st.success("✅ Coded data cleared")
                st.rerun()
        
        if st.button("🗑️ Clear All Session Data", type="secondary"):
            st.session_state.collected_data = []
            st.session_state.coded_data = []
        if st.button("🗑️ Clear All Session Data", type="secondary"):
            st.session_state.collected_data = []
            st.session_state.coded_data = []
            from modules.codebook import CodebookManager
            st.session_state.codebook_manager = CodebookManager()
            st.success("✅ All session data cleared")
            st.rerun()

def prepare_dataframe(data, flatten_nested=True, include_metadata=True):
    if not data:
        return pd.DataFrame()
    
    if isinstance(data, dict) and not isinstance(data, list):
        rows = []
        for category, items in data.items():
            for item in items:
                row = {'category': category, **item}
                rows.append(row)
        df = pd.DataFrame(rows)
    else:
        df = pd.DataFrame(data)
    
    if flatten_nested:
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
    
    if not include_metadata:
        metadata_cols = ['id', 'author', 'created_utc', 'collected_at', 'coded_at', 'coded_by']
        df = df.drop(columns=[col for col in metadata_cols if col in df.columns], errors='ignore')
    
    return df

def log_export_action(export_type, export_format):
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'action': 'export',
        'export_type': export_type,
        'export_format': export_format,
        'session_id': st.session_state.session_id
    }
    
    os.makedirs('logs', exist_ok=True)
    with open(f'logs/audit_{st.session_state.session_id}.jsonl', 'a') as f:
        f.write(json.dumps(log_entry) + '\n')

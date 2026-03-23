# modules/data_manager.py
# =======================
# Data export, import, audit trail, and session management for the
# NootropicRedditScrapePPM thesis tool.
#
# Tabs
# ----
#   Export Data       — CSV / JSON / Excel download of collected, coded, or codebook data
#   Upload Dataset    — Import an external JSON file into the DB and session_state
#   Audit Trail       — Searchable view of all log_action records from the DB
#   Session Management— Save/load/clear session state; restore from file
#
# Key design decisions
# --------------------
# - All file downloads use in-memory bytes — no server-side files are created.
#   A previous version wrote session saves to disk (exports/ directory) which
#   left unmanaged files on the server.
# - Imported data (Upload Dataset, Load Session) is persisted to the DB via
#   save_collected_data() so it survives app restarts.
# - Codebook CSV/Excel export uses CodebookManager.to_csv_rows() — the dict
#   shape returned by to_dict() is not compatible with prepare_dataframe().
# - Audit filter action types are derived from live log records, not hardcoded,
#   so new action types from job_manager appear automatically.
#
# Exported symbols
# ----------------
#   render()            — Streamlit page entry point (called by app.py)
#   prepare_dataframe() — used by render(), testable standalone

import json
from datetime import datetime
from io import BytesIO

import pandas as pd
import streamlit as st

from modules.codebook import CodebookManager
from utils.db_helpers import load_audit_logs, log_action, save_collected_data


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _import_items_to_db(items: list) -> tuple[int, int]:
    """
    Persist a list of item dicts to the DB, deduplicating against existing DB IDs.

    Returns (added_count, skipped_count).
    Items without an 'id' field are always skipped — save_collected_data requires
    reddit_id and will silently skip them, so we surface the count here instead.

    Used by both the Upload Dataset tab and the Load Session tab so both paths
    get identical persistence behaviour.
    """
    from utils.db_helpers import get_all_collected_reddit_ids

    # Build set of BARE IDs (DB stores bare IDs, get_all returns t3_ prefixed)
    existing_raw = get_all_collected_reddit_ids()
    existing_ids = {fn[3:] for fn in existing_raw}  # strip t3_ prefix

    valid_items   = [it for it in items if it.get('id')]
    invalid_count = len(items) - len(valid_items)

    new_items = [it for it in valid_items if it.get('id') not in existing_ids]
    skipped   = len(valid_items) - len(new_items) + invalid_count

    if new_items:
        save_collected_data(new_items, st.session_state.get('session_id'))

    return len(new_items), skipped


# ---------------------------------------------------------------------------
# Page entry point
# ---------------------------------------------------------------------------

def render():
    st.header("💾 Data Export & Audit Trail")

    st.info(
        "Export your data for analysis in external tools such as NVivo, MAXQDA, "
        "or Python/R. All actions are logged for methodological transparency and "
        "replicability per Creswell & Creswell (2023)."
    )

    st.divider()

    tab_export, tab_upload, tab_audit, tab_session = st.tabs([
        "📥 Export Data",
        "📂 Upload Dataset",
        "📜 Audit Trail",
        "🔄 Session Management",
    ])

    # -----------------------------------------------------------------------
    # Tab 1: Export Data
    # -----------------------------------------------------------------------
    with tab_export:
        st.subheader("Export Research Data")

        export_type = st.selectbox(
            "Select Data to Export",
            ["Collected Data (Raw)", "Coded Data", "Both (Collected + Coded)", "Codebook"],
        )

        export_format = st.selectbox(
            "Export Format",
            ["CSV (Excel/SPSS compatible)", "JSON (NVivo/MAXQDA import)", "Excel (.xlsx)"],
        )

        col1, col2 = st.columns(2)
        with col1:
            include_metadata = st.checkbox(
                "Include metadata fields",
                value=True,
                help="Author, score, timestamps, etc.",
            )
        with col2:
            flatten_nested = st.checkbox(
                "Flatten nested fields",
                value=True,
                help="Convert lists/dicts to JSON strings for tabular formats.",
            )

        if st.button("📥 Generate Export", type="primary"):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            session_id = st.session_state.get('session_id', 'unknown')

            # Guard: codebook_manager must be initialised
            if 'codebook_manager' not in st.session_state:
                st.session_state.codebook_manager = CodebookManager()
            mgr = st.session_state.codebook_manager

            # --- Resolve data ---
            if export_type == "Collected Data (Raw)":
                data_to_export = st.session_state.collected_data
                filename_prefix = "collected_data"
            elif export_type == "Coded Data":
                data_to_export = st.session_state.coded_data
                filename_prefix = "coded_data"
            elif export_type == "Both (Collected + Coded)":
                data_to_export = {
                    'collected': st.session_state.collected_data,
                    'coded':     st.session_state.coded_data,
                }
                filename_prefix = "research_data"
            else:  # Codebook
                data_to_export = mgr.to_dict()
                filename_prefix = "codebook"

            if not data_to_export:
                st.warning("⚠️ No data available to export.")
                return

            # --- Build and offer download ---
            if export_format == "CSV (Excel/SPSS compatible)":
                if export_type == "Both (Collected + Coded)":
                    for key, data in data_to_export.items():
                        if data:
                            df = prepare_dataframe(data, flatten_nested, include_metadata)
                            st.download_button(
                                label=f"⬇️ Download {key.title()} Data (CSV)",
                                data=df.to_csv(index=False).encode('utf-8'),
                                file_name=f"{key}_{session_id}_{timestamp}.csv",
                                mime="text/csv",
                            )
                elif export_type == "Codebook":
                    # to_csv_rows() produces a flat list of dicts — compatible with DataFrame
                    df = pd.DataFrame(mgr.to_csv_rows())
                    st.download_button(
                        label="⬇️ Download Codebook (CSV)",
                        data=df.to_csv(index=False).encode('utf-8'),
                        file_name=f"codebook_{session_id}_{timestamp}.csv",
                        mime="text/csv",
                    )
                else:
                    df = prepare_dataframe(data_to_export, flatten_nested, include_metadata)
                    st.download_button(
                        label="⬇️ Download CSV",
                        data=df.to_csv(index=False).encode('utf-8'),
                        file_name=f"{filename_prefix}_{session_id}_{timestamp}.csv",
                        mime="text/csv",
                    )

            elif export_format == "JSON (NVivo/MAXQDA import)":
                json_bytes = json.dumps(data_to_export, indent=2, default=str).encode('utf-8')
                st.download_button(
                    label="⬇️ Download JSON",
                    data=json_bytes,
                    file_name=f"{filename_prefix}_{session_id}_{timestamp}.json",
                    mime="application/json",
                )

            elif export_format == "Excel (.xlsx)":
                try:
                    excel_buffer = BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                        if export_type == "Both (Collected + Coded)":
                            for key, data in data_to_export.items():
                                if data:
                                    df = prepare_dataframe(
                                        data, flatten_nested, include_metadata
                                    )
                                    df.to_excel(writer, sheet_name=key.title(), index=False)
                        elif export_type == "Codebook":
                            df = pd.DataFrame(mgr.to_csv_rows())
                            df.to_excel(writer, sheet_name="Codebook", index=False)
                        else:
                            df = prepare_dataframe(
                                data_to_export, flatten_nested, include_metadata
                            )
                            df.to_excel(writer, sheet_name="Data", index=False)

                    excel_buffer.seek(0)
                    st.download_button(
                        label="⬇️ Download Excel",
                        data=excel_buffer.getvalue(),
                        file_name=f"{filename_prefix}_{session_id}_{timestamp}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
                except Exception as e:
                    st.error(f"Excel export error: {e}. Use CSV format as a fallback.")

            log_action(
                action='export',
                session_id=session_id,
                details={'export_type': export_type, 'export_format': export_format},
            )
            st.success("✅ Export generated successfully.")

        st.divider()
        st.subheader("📊 Data Summary")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Collected Items", len(st.session_state.collected_data))
        with col2:
            st.metric("Coded Items", len(st.session_state.coded_data))
        with col3:
            if 'codebook_manager' in st.session_state:
                total_codes = len(st.session_state.codebook_manager.get_all())
            else:
                total_codes = 0
            st.metric("Codebook Entries", total_codes)

    # -----------------------------------------------------------------------
    # Tab 2: Upload Dataset
    # -----------------------------------------------------------------------
    with tab_upload:
        st.subheader("📂 Upload External Dataset")
        st.write(
            "Upload a JSON file containing raw Reddit posts (e.g., mock data or a "
            "previous export) to import into the current session and database."
        )

        uploaded_dataset = st.file_uploader(
            "Upload JSON Dataset",
            type=['json'],
            key="dataset_uploader",
        )

        if uploaded_dataset is not None:
            try:
                new_data = json.load(uploaded_dataset)

                if isinstance(new_data, dict) and 'collected' in new_data:
                    items_to_add = new_data['collected']
                elif isinstance(new_data, list):
                    items_to_add = new_data
                else:
                    st.error("Invalid JSON structure. Expected a list of posts or a dict with a 'collected' key.")
                    items_to_add = []

                if items_to_add:
                    # Validate: warn on items missing 'id'
                    invalid = [it for it in items_to_add if not it.get('id')]
                    if invalid:
                        st.warning(
                            f"⚠️ {len(invalid)} item(s) are missing an 'id' field and will be skipped on import."
                        )

                    st.write(f"**Found {len(items_to_add)} items** in the uploaded file.")

                    if st.button("📥 Import into Collected Data", type="primary"):
                        added, skipped = _import_items_to_db(items_to_add)

                        # Refresh session_state from DB so UI reflects new data
                        from utils.db_helpers import load_collected_data
                        st.session_state.collected_data = load_collected_data(
                            session_id=None, limit=10000
                        )

                        st.success(
                            f"✅ Imported {added} new item(s) to DB and session. "
                            f"({skipped} duplicate(s) or invalid item(s) skipped.)"
                        )
                        st.rerun()

            except Exception as e:
                st.error(f"Error reading JSON file: {e}")

    # -----------------------------------------------------------------------
    # Tab 3: Audit Trail
    # -----------------------------------------------------------------------
    with tab_audit:
        st.subheader("📜 Session Audit Trail")

        st.write(
            "The audit trail logs all collection and coding activities for "
            "methodological transparency — essential for demonstrating rigour "
            "and replicability in qualitative research."
        )

        audit_logs = load_audit_logs(st.session_state.get('session_id'), limit=1000)

        if audit_logs:
            st.write(f"**Total Log Entries:** {len(audit_logs)}")

            # Derive action types from live records — no hardcoded list
            action_types = sorted({log.get('action', '') for log in audit_logs if log.get('action')})
            filter_action = st.selectbox(
                "Filter by Action",
                ["All"] + action_types,
            )

            filtered_logs = (
                audit_logs if filter_action == "All"
                else [log for log in audit_logs if log.get('action') == filter_action]
            )

            for log in filtered_logs[:20]:
                ts    = log.get('timestamp', 'N/A')
                action = log.get('action', 'Unknown').upper()
                with st.expander(f"{ts} — {action}"):
                    st.json(log)

            if st.button("📥 Download Complete Audit Trail"):
                audit_bytes = json.dumps(audit_logs, indent=2, default=str).encode('utf-8')
                st.download_button(
                    label="⬇️ Download Audit Log (JSON)",
                    data=audit_bytes,
                    file_name=f"audit_trail_{st.session_state.get('session_id', 'unknown')}.json",
                    mime="application/json",
                )
        else:
            st.info("No audit entries yet. Collect or code data to generate logs.")

    # -----------------------------------------------------------------------
    # Tab 4: Session Management
    # -----------------------------------------------------------------------
    with tab_session:
        st.subheader("🔄 Session Management")

        session_id = st.session_state.get('session_id', 'unknown')
        st.write(f"**Current Session ID:** `{session_id}`")

        st.divider()

        # --- Save session ---
        st.subheader("💾 Save Session State")
        st.write("Save your current session to resume later or share with collaborators.")

        if st.button("💾 Save Session to File"):
            if 'codebook_manager' not in st.session_state:
                st.session_state.codebook_manager = CodebookManager()

            session_data = {
                'session_id':     session_id,
                'saved_at':       datetime.now().isoformat(),
                'collected_data': st.session_state.collected_data,
                'coded_data':     st.session_state.coded_data,
                'codebook':       st.session_state.codebook_manager.to_dict(),
            }

            # Build bytes in memory — no server-side file created
            session_bytes = json.dumps(session_data, indent=2, default=str).encode('utf-8')

            st.download_button(
                label="⬇️ Download Session File",
                data=session_bytes,
                file_name=f"session_{session_id}.json",
                mime="application/json",
            )
            st.success("✅ Session file ready for download.")

        st.divider()

        # --- Load session ---
        st.subheader("📂 Load Session State")

        uploaded_file = st.file_uploader(
            "Upload a saved session file",
            type=['json'],
            key="session_uploader",
        )

        if uploaded_file is not None:
            try:
                session_data = json.load(uploaded_file)

                st.write(f"**Session ID:** {session_data.get('session_id', 'Unknown')}")
                st.write(f"**Saved At:** {session_data.get('saved_at', 'Unknown')}")
                st.write(f"**Collected Items:** {len(session_data.get('collected_data', []))}")
                st.write(f"**Coded Items:** {len(session_data.get('coded_data', []))}")

                if st.button("🔄 Load This Session", type="primary"):
                    loaded_collected = session_data.get('collected_data', [])
                    loaded_coded     = session_data.get('coded_data', [])

                    # Persist loaded collected items to DB so they survive restarts
                    if loaded_collected:
                        added, skipped = _import_items_to_db(loaded_collected)
                        st.write(f"Persisted {added} collected item(s) to DB ({skipped} already present).")

                    # Refresh session_state from DB
                    from utils.db_helpers import load_collected_data, load_coded_data
                    st.session_state.collected_data = load_collected_data(
                        session_id=None, limit=10000
                    )
                    st.session_state.coded_data = loaded_coded  # coded_data stays as-is

                    # Restore codebook
                    cb_data = session_data.get('codebook')
                    if cb_data and 'codes' in cb_data:
                        st.session_state.codebook_manager = CodebookManager.from_dict(cb_data)
                    else:
                        st.session_state.codebook_manager = CodebookManager()

                    st.success("✅ Session loaded successfully.")
                    st.rerun()

            except Exception as e:
                st.error(f"❌ Error loading session: {e}")

        st.divider()

        # --- Clear session data ---
        st.subheader("🗑️ Clear Session Data")

        st.warning(
            "⚠️ Clearing session data removes it from memory only — "
            "the database is not affected. Export first if you need a backup."
        )

        col1, col2 = st.columns(2)
        with col1:
            if st.button("🗑️ Clear Collected Data"):
                st.session_state.collected_data = []
                st.success("✅ Collected data cleared from session.")
                st.rerun()
        with col2:
            if st.button("🗑️ Clear Coded Data"):
                st.session_state.coded_data = []
                st.success("✅ Coded data cleared from session.")
                st.rerun()

        if st.button("🗑️ Clear All Session Data", type="secondary"):
            st.session_state.collected_data = []
            st.session_state.coded_data     = []
            st.session_state.codebook_manager = CodebookManager()
            st.success("✅ All session data cleared.")
            st.rerun()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def prepare_dataframe(
    data: list | dict,
    flatten_nested: bool = True,
    include_metadata: bool = True,
) -> pd.DataFrame:
    """
    Convert a list of item dicts (or a category-keyed dict) to a DataFrame.

    NOT used for codebook export — use CodebookManager.to_csv_rows() for that.

    Parameters
    ----------
    data : list or dict
        List of item dicts, or {'category': [items]} dict (used by "Both" export).
    flatten_nested : bool
        If True, list/dict fields are serialised to JSON strings so the
        DataFrame is fully flat (required for CSV/Excel).
    include_metadata : bool
        If False, strips id/author/created_utc/collected_at/coded_at/coded_by.
    """
    if not data:
        return pd.DataFrame()

    if isinstance(data, dict):
        # Category-keyed dict: {'collected': [...], 'coded': [...]}
        # Flatten into a single DataFrame with a 'category' column.
        rows = []
        for category, items in data.items():
            if isinstance(items, list):
                for item in items:
                    rows.append({'category': category, **item})
        df = pd.DataFrame(rows)
    else:
        df = pd.DataFrame(data)

    if flatten_nested:
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].apply(
                    lambda x: json.dumps(x, default=str) if isinstance(x, (list, dict)) else x
                )

    if not include_metadata:
        metadata_cols = ['id', 'author', 'created_utc', 'collected_at', 'coded_at', 'coded_by']
        df = df.drop(
            columns=[c for c in metadata_cols if c in df.columns],
            errors='ignore',
        )

    return df

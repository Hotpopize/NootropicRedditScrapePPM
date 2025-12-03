import streamlit as st
from pyzotero import zotero
from datetime import datetime
import re


def extract_keywords_from_abstract(abstract):
    """Extract potential keywords from abstract text."""
    if not abstract:
        return []
    
    stop_words = {
        'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
        'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'been',
        'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
        'could', 'should', 'may', 'might', 'must', 'shall', 'can', 'need',
        'this', 'that', 'these', 'those', 'it', 'its', 'they', 'their',
        'we', 'our', 'you', 'your', 'he', 'she', 'his', 'her', 'which',
        'who', 'whom', 'what', 'when', 'where', 'why', 'how', 'all', 'each',
        'every', 'both', 'few', 'more', 'most', 'other', 'some', 'such',
        'only', 'same', 'than', 'very', 'just', 'also', 'into', 'over',
        'after', 'before', 'between', 'through', 'during', 'about', 'against',
        'study', 'studies', 'research', 'paper', 'article', 'results', 'findings',
        'method', 'methods', 'analysis', 'data', 'using', 'used', 'based'
    }
    
    words = re.findall(r'\b[a-zA-Z]{4,}\b', abstract.lower())
    word_freq = {}
    for word in words:
        if word not in stop_words:
            word_freq[word] = word_freq.get(word, 0) + 1
    
    sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
    return [word for word, freq in sorted_words[:15] if freq >= 2]


def format_authors(creators):
    """Format creator list into author string."""
    if not creators:
        return ""
    
    authors = []
    for creator in creators:
        if creator.get('creatorType') == 'author':
            name = f"{creator.get('lastName', '')}, {creator.get('firstName', '')}".strip(', ')
            if name:
                authors.append(name)
    
    if len(authors) == 0:
        return "Unknown"
    elif len(authors) == 1:
        return authors[0]
    elif len(authors) == 2:
        return f"{authors[0]} & {authors[1]}"
    else:
        return f"{authors[0]} et al."


def generate_apa_citation(item_data):
    """Generate APA-style citation from Zotero item data."""
    creators = item_data.get('creators', [])
    authors = format_authors(creators)
    year = item_data.get('date', '')[:4] if item_data.get('date') else 'n.d.'
    title = item_data.get('title', 'Untitled')
    
    item_type = item_data.get('itemType', '')
    
    if item_type == 'journalArticle':
        journal = item_data.get('publicationTitle', '')
        volume = item_data.get('volume', '')
        issue = item_data.get('issue', '')
        pages = item_data.get('pages', '')
        doi = item_data.get('DOI', '')
        
        citation = f"{authors} ({year}). {title}. "
        if journal:
            citation += f"*{journal}*"
            if volume:
                citation += f", *{volume}*"
                if issue:
                    citation += f"({issue})"
            if pages:
                citation += f", {pages}"
            citation += "."
        if doi:
            citation += f" https://doi.org/{doi}"
        return citation
    
    elif item_type == 'book':
        publisher = item_data.get('publisher', '')
        place = item_data.get('place', '')
        citation = f"{authors} ({year}). *{title}*."
        if place and publisher:
            citation += f" {place}: {publisher}."
        elif publisher:
            citation += f" {publisher}."
        return citation
    
    else:
        return f"{authors} ({year}). {title}."


def sync_zotero_library(api_key, library_id, library_type='user', collection_filter=None):
    """Sync items from Zotero library."""
    try:
        zot = zotero.Zotero(library_id, library_type, api_key)
        
        if collection_filter:
            items = zot.collection_items(collection_filter, limit=100)
        else:
            items = zot.top(limit=100)
        
        synced_items = []
        for item in items:
            item_data = item.get('data', {})
            if item_data.get('itemType') in ['attachment', 'note']:
                continue
            
            tags = [tag.get('tag', '') for tag in item_data.get('tags', [])]
            abstract = item_data.get('abstractNote', '')
            extracted_keywords = extract_keywords_from_abstract(abstract)
            
            synced_item = {
                'zotero_key': item_data.get('key'),
                'item_type': item_data.get('itemType'),
                'title': item_data.get('title', 'Untitled'),
                'authors': item_data.get('creators', []),
                'year': item_data.get('date', '')[:4] if item_data.get('date') else None,
                'abstract': abstract,
                'doi': item_data.get('DOI'),
                'url': item_data.get('url'),
                'tags': tags,
                'collections': item_data.get('collections', []),
                'keywords': list(set(tags + extracted_keywords)),
                'citation_apa': generate_apa_citation(item_data),
                'synced_at': datetime.utcnow().isoformat()
            }
            synced_items.append(synced_item)
        
        return synced_items, None
    
    except Exception as e:
        return [], str(e)


def get_zotero_collections(api_key, library_id, library_type='user'):
    """Get list of collections from Zotero library."""
    try:
        zot = zotero.Zotero(library_id, library_type, api_key)
        collections = zot.collections()
        
        return [
            {
                'key': col['data']['key'],
                'name': col['data']['name'],
                'parent': col['data'].get('parentCollection', None)
            }
            for col in collections
        ], None
    
    except Exception as e:
        return [], str(e)


def render():
    st.header("Zotero Citation Manager")
    st.markdown("""
    Connect your Zotero library to:
    - **Extract keywords** from your literature for guided Reddit data collection
    - **Link citations** to collection runs for audit trail documentation
    - **Export citations** alongside your research data for NVivo/MAXQDA
    """)
    
    if 'zotero_references' not in st.session_state:
        st.session_state.zotero_references = []
    if 'zotero_keywords' not in st.session_state:
        st.session_state.zotero_keywords = []
    if 'zotero_collections' not in st.session_state:
        st.session_state.zotero_collections = []
    
    st.subheader("Zotero API Configuration")
    
    with st.expander("How to get your Zotero API credentials", expanded=False):
        st.markdown("""
        1. Go to [Zotero Settings > Keys](https://www.zotero.org/settings/keys)
        2. Click **"Create new private key"**
        3. Name it (e.g., "Research Tool")
        4. Check **"Allow library access"** (read-only is sufficient)
        5. Copy the API key
        6. Your **Library ID** is the number shown on the keys page
        """)
    
    col1, col2 = st.columns(2)
    with col1:
        api_key = st.text_input(
            "Zotero API Key",
            type="password",
            value=st.session_state.get('zotero_api_key', ''),
            help="Your private API key from Zotero settings"
        )
    with col2:
        library_id = st.text_input(
            "Library ID",
            value=st.session_state.get('zotero_library_id', ''),
            help="Your numeric user ID from Zotero settings"
        )
    
    library_type = st.radio(
        "Library Type",
        options=['user', 'group'],
        horizontal=True,
        help="Select 'user' for personal library or 'group' for shared group libraries"
    )
    
    if api_key and library_id:
        st.session_state.zotero_api_key = api_key
        st.session_state.zotero_library_id = library_id
    
    st.divider()
    
    if api_key and library_id:
        st.subheader("Sync Zotero Library")
        
        col_sync1, col_sync2 = st.columns([2, 1])
        
        with col_sync1:
            if st.button("Load Collections", use_container_width=True):
                with st.spinner("Fetching collections..."):
                    collections, error = get_zotero_collections(api_key, library_id, library_type)
                    if error:
                        st.error(f"Error: {error}")
                    else:
                        st.session_state.zotero_collections = collections
                        st.success(f"Found {len(collections)} collections")
        
        collection_options = ["All Items"] + [c['name'] for c in st.session_state.zotero_collections]
        selected_collection = st.selectbox(
            "Filter by Collection (optional)",
            options=collection_options,
            help="Sync only items from a specific collection"
        )
        
        with col_sync2:
            if st.button("Sync References", type="primary", use_container_width=True):
                collection_key = None
                if selected_collection != "All Items":
                    for c in st.session_state.zotero_collections:
                        if c['name'] == selected_collection:
                            collection_key = c['key']
                            break
                
                with st.spinner("Syncing from Zotero..."):
                    items, error = sync_zotero_library(
                        api_key, library_id, library_type, collection_key
                    )
                    
                    if error:
                        st.error(f"Sync error: {error}")
                    else:
                        from utils.db_helpers import save_zotero_references
                        saved_count = save_zotero_references(items, st.session_state.session_id)
                        
                        st.session_state.zotero_references = items
                        
                        all_keywords = set()
                        for item in items:
                            all_keywords.update(item.get('keywords', []))
                        st.session_state.zotero_keywords = sorted(list(all_keywords))
                        
                        st.success(f"Synced {len(items)} references with {len(all_keywords)} unique keywords")
    
    st.divider()
    
    if st.session_state.zotero_references:
        st.subheader("Synced References")
        st.metric("Total References", len(st.session_state.zotero_references))
        
        with st.expander("View References", expanded=False):
            for ref in st.session_state.zotero_references[:20]:
                st.markdown(f"**{ref['title']}** ({ref.get('year', 'n.d.')})")
                st.caption(ref.get('citation_apa', ''))
                if ref.get('tags'):
                    st.caption(f"Tags: {', '.join(ref['tags'][:5])}")
                st.divider()
        
        st.subheader("Keyword Selection for Data Collection")
        st.markdown("Select keywords from your literature to guide Reddit data collection:")
        
        if st.session_state.zotero_keywords:
            keyword_columns = st.columns(4)
            selected_keywords = []
            
            for idx, keyword in enumerate(st.session_state.zotero_keywords[:40]):
                col_idx = idx % 4
                with keyword_columns[col_idx]:
                    if st.checkbox(keyword, key=f"kw_{keyword}"):
                        selected_keywords.append(keyword)
            
            if selected_keywords:
                st.session_state.selected_zotero_keywords = selected_keywords
                st.success(f"Selected {len(selected_keywords)} keywords: {', '.join(selected_keywords)}")
                
                st.markdown("**Suggested search terms for Reddit:**")
                search_terms = ' OR '.join(selected_keywords[:5])
                st.code(search_terms)
        
        st.divider()
        
        st.subheader("Link Citations to Collection Runs")
        st.markdown("Link relevant citations to your data collection runs for audit trail documentation.")
        
        if 'collection_runs' in st.session_state and st.session_state.collection_runs:
            collection_run_options = [
                f"{run.get('collection_hash', 'N/A')[:8]}... - {run.get('collection_started', 'Unknown')[:10]}"
                for run in st.session_state.collection_runs
            ]
            
            selected_run = st.selectbox(
                "Select Collection Run",
                options=collection_run_options
            )
            
            if selected_run:
                run_idx = collection_run_options.index(selected_run)
                run_hash = st.session_state.collection_runs[run_idx].get('collection_hash')
                
                st.markdown("**Select citations to link:**")
                selected_refs = st.multiselect(
                    "Citations",
                    options=[ref['title'] for ref in st.session_state.zotero_references],
                    help="Select citations relevant to this collection run"
                )
                
                if selected_refs and st.button("Link Citations"):
                    from utils.db_helpers import save_citation_links
                    
                    links = []
                    for title in selected_refs:
                        for ref in st.session_state.zotero_references:
                            if ref['title'] == title:
                                links.append({
                                    'collection_hash': run_hash,
                                    'zotero_key': ref['zotero_key'],
                                    'link_type': 'theoretical_framework',
                                    'session_id': st.session_state.session_id
                                })
                                break
                    
                    saved = save_citation_links(links, st.session_state.session_id)
                    st.success(f"Linked {saved} citations to collection run")
        else:
            st.info("No collection runs available. Collect Reddit data first to link citations.")
    
    else:
        st.info("Configure your Zotero API credentials and sync your library to get started.")
    
    st.divider()
    
    with st.expander("Zotero Integration Help"):
        st.markdown("""
        ### How Zotero Integration Works
        
        **1. Keyword-Guided Collection**
        - Tags and extracted keywords from your Zotero references are compiled
        - Use these keywords to inform your Reddit search queries
        - Ensures theoretical alignment between literature and data collection
        
        **2. Citation Linking**
        - Link relevant citations to each data collection run
        - Creates audit trail connecting literature to data
        - Supports methodological transparency per Creswell & Creswell (2023)
        
        **3. Export Integration**
        - Linked citations appear in thesis export templates
        - APA citations ready for methodology chapter
        - Compatible with NVivo and MAXQDA workflows
        
        ### Privacy & Security
        - API credentials are stored in session only (not persisted)
        - Read-only access is sufficient for all features
        - References are synced to your local database for offline access
        """)

import streamlit as st
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation, NMF
import pandas as pd
from datetime import datetime
from utils.db_helpers import log_action

def render():
    st.header("Topic Modeling & Validation")
    st.markdown("""
    Automated topic discovery using TF-IDF and LDA to validate manual coding and discover latent themes.
    """)
    
    if not st.session_state.collected_data:
        st.warning("No collected data available. Please collect data from Reddit first.")
        return
    
    st.subheader("Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        n_topics = st.slider("Number of Topics", min_value=2, max_value=20, value=5)
        method = st.selectbox("Method", ["LDA (Latent Dirichlet Allocation)", "NMF (Non-negative Matrix Factorization)"])
        
    with col2:
        max_features = st.slider("Max Features (Vocabulary Size)", min_value=100, max_value=5000, value=1000, step=100)
        top_words = st.slider("Top Words per Topic", min_value=5, max_value=30, value=10)
    
    min_df = st.slider("Minimum Document Frequency", min_value=1, max_value=10, value=2)
    max_df = st.slider("Maximum Document Frequency (% of documents)", min_value=0.1, max_value=1.0, value=0.9, step=0.05)
    
    if st.button("Run Topic Modeling", type="primary"):
        with st.spinner("Extracting topics..."):
            try:
                texts = []
                for item in st.session_state.collected_data:
                    text = item.get('title', '') + ' ' + item.get('text', '')
                    if text.strip():
                        texts.append(text.strip())
                
                if len(texts) < n_topics:
                    st.error(f"Not enough documents ({len(texts)}) for {n_topics} topics. Reduce number of topics or collect more data.")
                    return
                
                if method == "LDA (Latent Dirichlet Allocation)":
                    vectorizer = CountVectorizer(
                        max_features=max_features,
                        min_df=min_df,
                        max_df=max_df,
                        stop_words='english'
                    )
                    doc_term_matrix = vectorizer.fit_transform(texts)
                    
                    lda = LatentDirichletAllocation(
                        n_components=n_topics,
                        random_state=42,
                        max_iter=20
                    )
                    lda.fit(doc_term_matrix)
                    
                    feature_names = vectorizer.get_feature_names_out()
                    topics = []
                    
                    for topic_idx, topic in enumerate(lda.components_):
                        top_indices = topic.argsort()[-top_words:][::-1]
                        top_features = [feature_names[i] for i in top_indices]
                        topics.append({
                            'topic_id': topic_idx + 1,
                            'top_words': top_features,
                            'weights': [topic[i] for i in top_indices]
                        })
                    
                else:
                    vectorizer = TfidfVectorizer(
                        max_features=max_features,
                        min_df=min_df,
                        max_df=max_df,
                        stop_words='english'
                    )
                    tfidf_matrix = vectorizer.fit_transform(texts)
                    
                    nmf = NMF(
                        n_components=n_topics,
                        random_state=42,
                        max_iter=200
                    )
                    nmf.fit(tfidf_matrix)
                    
                    feature_names = vectorizer.get_feature_names_out()
                    topics = []
                    
                    for topic_idx, topic in enumerate(nmf.components_):
                        top_indices = topic.argsort()[-top_words:][::-1]
                        top_features = [feature_names[i] for i in top_indices]
                        topics.append({
                            'topic_id': topic_idx + 1,
                            'top_words': top_features,
                            'weights': [topic[i] for i in top_indices]
                        })
                
                st.session_state.discovered_topics = topics
                
                log_action(
                    action="topic_modeling",
                    session_id=st.session_state.session_id,
                    details={
                        'method': method,
                        'n_topics': n_topics,
                        'n_documents': len(texts),
                        'max_features': max_features,
                        'timestamp': datetime.utcnow().isoformat()
                    }
                )
                
                st.success(f"Discovered {len(topics)} topics from {len(texts)} documents!")
                
            except Exception as e:
                st.error(f"Error during topic modeling: {str(e)}")
                return
    
    if 'discovered_topics' in st.session_state and st.session_state.discovered_topics:
        st.subheader("Discovered Topics")
        
        for topic in st.session_state.discovered_topics:
            with st.expander(f"Topic {topic['topic_id']}: {', '.join(topic['top_words'][:5])}", expanded=True):
                st.markdown(f"**Top {len(topic['top_words'])} Words:**")
                
                words_df = pd.DataFrame({
                    'Word': topic['top_words'],
                    'Weight': [f"{w:.4f}" for w in topic['weights']]
                })
                st.dataframe(words_df, use_container_width=True, hide_index=True)
        
        st.subheader("Compare with Manual Codes")
        
        if 'codebook_manager' in st.session_state:
            st.markdown("**Your Current Codebook:**")
            
            all_codes = []
            for code in st.session_state.codebook_manager.get_all():
                all_codes.append({
                    'Category': code.category.value.title(),
                    'Code': code.name,
                    'Frequency': code.frequency
                })
            
            if all_codes:
                codes_df = pd.DataFrame(all_codes)
                st.dataframe(codes_df, use_container_width=True, hide_index=True)
                
                st.info("""
                **Analysis Guidance:**
                - Compare discovered topics with your manual codes
                - Topics with similar vocabulary may validate existing codes
                - Novel topic clusters may suggest new emergent themes
                - High-frequency words missing from your codebook warrant review
                """)
            else:
                st.warning("No codes in codebook yet. Run Automated Coding first to compare.")
        
        st.subheader("Export Topics")
        
        if st.button("Export Topics as JSON"):
            import json
            topics_json = json.dumps(st.session_state.discovered_topics, indent=2)
            st.download_button(
                label="Download Topics JSON",
                data=topics_json,
                file_name=f"discovered_topics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )

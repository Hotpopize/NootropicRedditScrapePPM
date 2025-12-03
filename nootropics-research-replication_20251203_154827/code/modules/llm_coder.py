import streamlit as st
import json
import os
from datetime import datetime
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from utils.db_helpers import save_coded_data, log_action

AI_INTEGRATIONS_OPENAI_API_KEY = os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY")
AI_INTEGRATIONS_OPENAI_BASE_URL = os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL")

if not AI_INTEGRATIONS_OPENAI_API_KEY or not AI_INTEGRATIONS_OPENAI_BASE_URL:
    openai_client = None
else:
    openai_client = OpenAI(
        api_key=AI_INTEGRATIONS_OPENAI_API_KEY,
        base_url=AI_INTEGRATIONS_OPENAI_BASE_URL
    )

def is_rate_limit_error(exception: BaseException) -> bool:
    error_msg = str(exception)
    return (
        "429" in error_msg
        or "RATELIMIT_EXCEEDED" in error_msg
        or "quota" in error_msg.lower()
        or "rate limit" in error_msg.lower()
        or (hasattr(exception, "status_code") and exception.status_code == 429)
    )

def render():
    st.header("🤖 LLM-Assisted Thematic Coding")
    
    st.info("""
    This module uses **OpenAI's LLM** (via Replit AI Integrations - no API key needed, billed to your credits) 
    to assist with thematic coding based on the **Push-Pull-Mooring (PPM) framework**.
    
    **Important for Academic Rigor:**
    - LLM suggestions should be reviewed and validated by the researcher
    - All coding decisions are logged for transparency and replicability
    - Results can be exported for inter-coder reliability analysis
    """)
    
    if not openai_client:
        st.error("⚠️ OpenAI integration not properly configured. Environment variables missing.")
        return
    
    if not st.session_state.collected_data:
        st.warning("⚠️ No data collected yet. Please collect data from the Reddit Data Collection module first.")
        return
    
    st.divider()
    
    st.subheader("⚙️ Coding Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        coding_approach = st.selectbox(
            "Coding Approach",
            ["Deductive (PPM Framework Only)", "Inductive (Emergent Themes Only)", "Mixed (PPM + Emergent Themes)"]
        )
        
        model_selection = st.selectbox(
            "LLM Model",
            ["gpt-5", "gpt-5-mini", "gpt-4.1", "gpt-4o"],
            help="gpt-5 is the newest and most capable model (released August 7, 2025)"
        )
    
    with col2:
        batch_size = st.number_input(
            "Batch Size for Coding",
            min_value=1,
            max_value=50,
            value=10,
            help="Number of items to code at once"
        )
        
        include_already_coded = st.checkbox(
            "Re-code already coded items",
            value=False,
            help="Check this to override previous coding"
        )
    
    st.divider()
    
    with st.expander("📖 View PPM Framework Definitions"):
        st.markdown("""
        ### Push Factors (Dissatisfaction with Current State)
        - Cognitive decline or perceived decline
        - Stress, anxiety, or mental fatigue
        - Poor focus or concentration
        - Memory issues
        - Academic or professional pressure
        - Dissatisfaction with pharmaceutical options
        
        ### Pull Factors (Attraction to Natural Nootropics)
        - Perceived cognitive benefits
        - Natural/organic ingredients preference
        - Positive testimonials or reviews
        - Scientific evidence or studies
        - Brand reputation
        - Specific ingredient efficacy
        
        ### Mooring Factors (Anchoring Elements)
        - Cost and affordability
        - Accessibility and availability
        - Trust in sources (brands, influencers, community)
        - Regulatory concerns or safety
        - Existing habits or routines
        - Side effects concerns
        """)
    
    if st.button("🚀 Start LLM-Assisted Coding", type="primary"):
        uncoded_data = [item for item in st.session_state.collected_data 
                       if include_already_coded or item.get('id') not in [c.get('id') for c in st.session_state.coded_data]]
        
        if not uncoded_data:
            st.info("All items have been coded. Enable 're-code already coded items' to code again.")
            return
        
        items_to_code = uncoded_data[:batch_size]
        
        st.info(f"🔄 Coding {len(items_to_code)} items using {model_selection}...")
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        coded_results = []
        
        for idx, item in enumerate(items_to_code):
            status_text.text(f"Coding item {idx + 1} of {len(items_to_code)}...")
            
            try:
                prompt = create_coding_prompt(item, coding_approach)
                
                @retry(
                    stop=stop_after_attempt(7),
                    wait=wait_exponential(multiplier=1, min=2, max=128),
                    retry=retry_if_exception(is_rate_limit_error),
                    reraise=True
                )
                def get_coding():
                    response = openai_client.chat.completions.create(
                        model=model_selection,
                        messages=[
                            {"role": "system", "content": "You are an academic research assistant specialized in qualitative thematic coding for consumer behavior research. Provide structured, evidence-based coding."},
                            {"role": "user", "content": prompt}
                        ],
                        response_format={"type": "json_object"},
                        max_completion_tokens=2000
                    )
                    return response.choices[0].message.content or "{}"
                
                coding_result = get_coding()
                coding_data = json.loads(coding_result)
                
                coded_item = {
                    **item,
                    'ppm_category': coding_data.get('ppm_category', 'Unknown'),
                    'ppm_subcodes': coding_data.get('ppm_subcodes', []),
                    'themes': coding_data.get('emergent_themes', []),
                    'evidence_quotes': coding_data.get('evidence_quotes', []),
                    'confidence': coding_data.get('confidence', 'Medium'),
                    'coded_at': datetime.now().isoformat(),
                    'coded_by': f"LLM-{model_selection}",
                    'coding_approach': coding_approach
                }
                
                coded_results.append(coded_item)
                
            except Exception as e:
                st.warning(f"⚠️ Error coding item {item.get('id')}: {str(e)}")
            
            progress_bar.progress((idx + 1) / len(items_to_code))
        
        if include_already_coded:
            st.session_state.coded_data = [c for c in st.session_state.coded_data 
                                          if c.get('id') not in [r.get('id') for r in coded_results]]
        
        st.session_state.coded_data.extend(coded_results)
        
        saved_count = save_coded_data(coded_results, st.session_state.session_id)
        
        log_action(
            action='llm_coding',
            session_id=st.session_state.session_id,
            details={
                'model': model_selection,
                'coding_approach': coding_approach,
                'items_coded': len(coded_results),
                'saved_to_db': saved_count
            }
        )
        
        status_text.empty()
        progress_bar.empty()
        
        st.success(f"✅ Successfully coded and saved {saved_count} items")
        
        st.subheader("📊 Coding Results Summary")
        
        ppm_dist = {}
        for item in coded_results:
            cat = item.get('ppm_category', 'Unknown')
            ppm_dist[cat] = ppm_dist.get(cat, 0) + 1
        
        st.write("**PPM Category Distribution:**")
        st.bar_chart(ppm_dist)
        
        all_themes = []
        for item in coded_results:
            all_themes.extend(item.get('themes', []))
        
        if all_themes:
            from collections import Counter
            theme_counts = Counter(all_themes)
            st.write("**Most Common Emergent Themes:**")
            st.write(theme_counts.most_common(10))
    
    st.divider()
    
    if st.session_state.coded_data:
        st.subheader("📋 Coded Data Preview")
        
        st.write(f"**Total Coded Items:** {len(st.session_state.coded_data)}")
        
        for item in st.session_state.coded_data[:5]:
            with st.expander(f"{item.get('type', 'item').upper()}: {item.get('title', 'No title')[:100]}..."):
                st.write(f"**Text:** {item.get('text', 'No text')[:500]}...")
                st.write(f"**PPM Category:** {item.get('ppm_category', 'N/A')}")
                st.write(f"**Subcodes:** {', '.join(item.get('ppm_subcodes', []))}")
                st.write(f"**Emergent Themes:** {', '.join(item.get('themes', []))}")
                st.write(f"**Evidence Quotes:** {item.get('evidence_quotes', [])}")
                st.write(f"**Confidence:** {item.get('confidence', 'N/A')}")
                st.write(f"**Coded By:** {item.get('coded_by', 'N/A')} at {item.get('coded_at', 'N/A')}")

def create_coding_prompt(item, approach):
    text_content = f"{item.get('title', '')} {item.get('text', '')}"
    
    if approach == "Deductive (PPM Framework Only)":
        prompt = f"""Analyze the following Reddit post/comment about cognitive supplements and code it using the Push-Pull-Mooring (PPM) framework.

TEXT:
{text_content}

TASK:
1. Identify the PRIMARY PPM category (Push, Pull, or Mooring)
2. Identify specific subcodes within that category
3. Extract evidence quotes that support your coding
4. Rate your confidence (High, Medium, Low)

PUSH FACTORS: Dissatisfaction (cognitive decline, stress, anxiety, poor focus, memory issues, pressure, dissatisfaction with pharmaceuticals)
PULL FACTORS: Attraction (perceived benefits, natural ingredients, testimonials, scientific evidence, brand reputation, ingredient efficacy)
MOORING FACTORS: Anchoring (cost, accessibility, trust, regulatory concerns, habits, side effects)

Respond in JSON format:
{{
  "ppm_category": "Push|Pull|Mooring",
  "ppm_subcodes": ["specific subcode 1", "specific subcode 2"],
  "evidence_quotes": ["quote 1", "quote 2"],
  "confidence": "High|Medium|Low",
  "rationale": "brief explanation"
}}"""
    
    elif approach == "Inductive (Emergent Themes Only)":
        prompt = f"""Analyze the following Reddit post/comment about cognitive supplements and identify emergent themes using open coding.

TEXT:
{text_content}

TASK:
1. Identify 2-5 emergent themes present in the text
2. Extract evidence quotes for each theme
3. Rate your confidence (High, Medium, Low)

Respond in JSON format:
{{
  "emergent_themes": ["theme 1", "theme 2", "theme 3"],
  "evidence_quotes": ["quote 1", "quote 2"],
  "confidence": "High|Medium|Low",
  "rationale": "brief explanation"
}}"""
    
    else:
        prompt = f"""Analyze the following Reddit post/comment about cognitive supplements using both the PPM framework (deductive) and emergent themes (inductive).

TEXT:
{text_content}

TASK:
1. Identify the PRIMARY PPM category and subcodes
2. Identify 2-5 emergent themes beyond the PPM framework
3. Extract evidence quotes supporting your coding
4. Rate your confidence (High, Medium, Low)

PPM FRAMEWORK:
- PUSH: Dissatisfaction (cognitive decline, stress, anxiety, poor focus, memory issues, pressure)
- PULL: Attraction (perceived benefits, natural ingredients, testimonials, evidence, brand, efficacy)
- MOORING: Anchoring (cost, accessibility, trust, regulations, habits, side effects)

Respond in JSON format:
{{
  "ppm_category": "Push|Pull|Mooring",
  "ppm_subcodes": ["specific subcode 1", "specific subcode 2"],
  "emergent_themes": ["theme 1", "theme 2"],
  "evidence_quotes": ["quote 1", "quote 2"],
  "confidence": "High|Medium|Low",
  "rationale": "brief explanation"
}}"""
    
    return prompt

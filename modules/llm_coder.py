import streamlit as st
import json
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.db_helpers import save_coded_data, log_action
from modules import ollama_client as ollama
from modules.codebook import CodebookManager, CodeCategory
from utils.db_helpers import load_codebook

def render():
    st.header("Automated Qualitative Coding 🤖")
    
    st.info("""
    This module utilizes **Local Computational Models via Ollama** to ensure academic replicability and privacy.
    
    **Requirements:**
    - Ollama must be running (`ollama serve`)
    - You must have at least one model pulled (e.g., `ollama pull llama3`)
    """)
    
    # Check Connection
    if not ollama.is_ollama_running():
        st.error("⚠️ **Ollama is not reachable.** Please ensure Ollama is running on localhost:11434.")
        st.code("ollama serve", language="bash")
        return
        
    # Get Models
    available_models = ollama.get_available_models()
    if not available_models:
        st.warning("⚠️ **No models found.** Please pull a model using your terminal.")
        st.code("ollama pull llama3", language="bash")
        return
    
    if not st.session_state.collected_data:
        st.warning("⚠️ No data collected yet. Please collect data from the Reddit Data Collection module first.")
        return
    
    # Initialize Codebook Manager
    if 'codebook_manager' not in st.session_state:
        saved = load_codebook(st.session_state.get('session_id'))
        if saved:
            st.session_state.codebook_manager = CodebookManager.from_dict(saved)
        else:
            st.session_state.codebook_manager = CodebookManager()

    
    st.divider()
    
    st.subheader("⚙️ Coding Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        coding_approach = st.selectbox(
            "Coding Approach",
            ["Deductive (PPM Framework Only)", "Inductive (Emergent Themes Only)", "Mixed (PPM + Emergent Themes)"]
        )
        
        # Prioritize "Packaged" models
        packaged_models = ["llama3.1", "gemma3:12b"]
        
        # Sort: Packaged models first, then others alphabetically
        available_models.sort(key=lambda x: (x not in packaged_models, x))
        
        # Find index of first packaged model available to set as default
        default_index = 0
        for idx, m in enumerate(available_models):
            if m in packaged_models:
                default_index = idx
                break

        model_selection = st.selectbox(
            "Local Computational Model",
            available_models,
            index=default_index if available_models else None,
            help="Select a model installed in Ollama. 'Packaged' models (llama3.1, gemma3:12b) are prioritized."
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
        
        # --- SAMPLING STRATEGY ---
        st.write("**Efficiency Mode**")
        use_sampling = st.checkbox("Enable Stratified Sampling", value=False, help="Process a random balanced subset instead of all data.")
        if use_sampling:
            sample_size_per_sub = st.number_input("Items per Subreddit", min_value=1, max_value=100, value=10)
    
    st.divider()
    
    with st.expander("📖 View PPM Framework Definitions"):
        # ... (Existing View Code - kept collapsed for brevity in this edit if possible, but replace tool needs context)
        # Actually, let's just keep the expander content as is or assume it's there. 
        # Using a targeted replace for the UI section first might be safer.
        pass

    # ... (skipping to start button logic)
    
    if st.button("🚀 Start Automated Coding", type="primary"):
        uncoded_data = [item for item in st.session_state.collected_data 
                       if include_already_coded or item.get('id') not in [c.get('id') for c in st.session_state.coded_data]]
        
        if not uncoded_data:
            st.info("All items have been coded. Enable 're-code already coded items' to code again.")
            return

        # --- SAMPLING LOGIC ---
        if use_sampling:
            st.info(f"🎲 Applying Stratified Sampling ({sample_size_per_sub} per subreddit)...")
            import random
            
            # Group by subreddit
            grouped = {}
            for item in uncoded_data:
                sub = item.get('subreddit', 'Unknown')
                if sub not in grouped:
                    grouped[sub] = []
                grouped[sub].append(item)
            
            sampled_items = []
            for sub, items in grouped.items():
                if len(items) > sample_size_per_sub:
                    sampled_items.extend(random.sample(items, sample_size_per_sub))
                else:
                    sampled_items.extend(items) # Take all if less than sample size
            
            # Shuffle the final list to mix subreddits in the batch
            random.shuffle(sampled_items)
            items_to_code = sampled_items[:batch_size] # Apply batch size to the *sample*
            st.write(f"selected {len(items_to_code)} items from the sample (Sample pool: {len(sampled_items)})")
        else:
            items_to_code = uncoded_data[:batch_size]
        
        st.info(f"🔄 Coding {len(items_to_code)} items using {model_selection}...")
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        coded_results = []
        consecutive_errors = 0
        MAX_CONSECUTIVE_ERRORS = 3
        
        for idx, item in enumerate(items_to_code):
            # Safe Stop if too many errors
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                st.error(f"🛑 **Process Stopped Early**: Encountered {MAX_CONSECUTIVE_ERRORS} consecutive errors. Stopping to prevent infinite retries/loops.")
                break

            status_text.text(f"Coding item {idx + 1} of {len(items_to_code)}...")
            
            try:
                prompt = create_coding_prompt(item, coding_approach)
                system_prompt = "You are an academic research assistant specialized in qualitative thematic coding. You effectively output only valid JSON without markdown formatting."
                
                # Call Ollama
                coding_result = ollama.generate_completion(model_selection, prompt, system_prompt)
                
                # Sanitize response
                if "```json" in coding_result:
                    coding_result = coding_result.split("```json")[1].split("```")[0]
                elif "```" in coding_result:
                    coding_result = coding_result.split("```")[1].split("```")[0]
                
                coding_data = json.loads(coding_result)
                
                is_relevant = coding_data.get('is_relevant', True) # Default to true if missing for backward compat
                
                if not is_relevant:
                     coded_item = {
                        **item,
                        'ppm_category': 'Excluded (Irrelevant)',
                        'ppm_subcodes': [],
                        'themes': [],
                        'evidence_quotes': [],
                        'confidence': 'High',
                        'coded_at': datetime.now().isoformat(),
                        'coded_by': f"Ollama-{model_selection}",
                        'coding_approach': coding_approach,
                        'rationale': coding_data.get('rationale', 'Does not meet inclusion criteria'),
                        'raw_prompt': prompt,
                        'raw_response': coding_result
                    }
                else:
                    coded_item = {
                        **item,
                        'ppm_category': coding_data.get('ppm_category', 'Unknown'),
                        'ppm_subcodes': coding_data.get('ppm_subcodes', []),
                        'themes': coding_data.get('emergent_themes', []),
                        'evidence_quotes': coding_data.get('evidence_quotes', []),
                        'confidence': coding_data.get('confidence', 'Medium'),
                        'coded_at': datetime.now().isoformat(),
                        'coded_by': f"Ollama-{model_selection}",
                        'coding_approach': coding_approach,
                         'rationale': coding_data.get('rationale', ''),
                         'raw_prompt': prompt,
                         'raw_response': coding_result
                    }
                
                coded_results.append(coded_item)
                consecutive_errors = 0 # Reset on success
                
            except Exception as e:
                consecutive_errors += 1
                st.warning(f"⚠️ Error coding item {item.get('id')}: {str(e)} (Consecutive Errors: {consecutive_errors}/{MAX_CONSECUTIVE_ERRORS})")
                # Continue process to next item check
            
            progress_bar.progress((idx + 1) / len(items_to_code))
        
        if include_already_coded:
            st.session_state.coded_data = [c for c in st.session_state.coded_data 
                                          if c.get('id') not in [r.get('id') for r in coded_results]]
        
        st.session_state.coded_data.extend(coded_results)
        
        saved_count = save_coded_data(coded_results, st.session_state.session_id)
        
        log_action(
            action='automated_coding_ollama',
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
    text_content = f"Title: {item.get('title', '')}\nContent: {item.get('text', '')}"
    
    mgr = st.session_state.codebook_manager

    # --- Helper to format codebook sections ---
    def format_codebook_section(category_enum):
        codes = mgr.get_by_category(category_enum)
        if not codes:
            return "- No codes defined."
            
        formatted = []
        for c in codes:
            if c.name.startswith("[Reserved"):
                continue
            entry = f"{c.id}: {c.name} - {c.definition}"
            if c.include:
                 entry += f" (INCLUDE: {c.include})"
            if c.exclude:
                 entry += f" (EXCLUDE: {c.exclude})"
            formatted.append(entry)
            
        return "\n".join(formatted)

    # --- Build Prompts ---
    push_section = format_codebook_section(CodeCategory.PUSH)
    pull_section = format_codebook_section(CodeCategory.PULL)
    
    moor_f = format_codebook_section(CodeCategory.MOOR_FACILITATOR)
    moor_i = format_codebook_section(CodeCategory.MOOR_INHIBITOR)
    moor_section = f"FACILITATORS (Enable switching):\n{moor_f}\n\nINHIBITORS (Impede switching):\n{moor_i}"
    
    base_instructions = f"""You are an expert qualitative research assistant applying the Push-Pull-Mooring (PPM) framework to analyze Reddit posts about nootropics and cognitive enhancement.

REDDIT POST:
{text_content}

YOUR TASK:
1. Determine if the text is relevant. It is relevant only if the post describes or compares switching from conventional stimulants to natural nootropics, or vice versa — not relevant if it discusses only one category in isolation.
2. If relevant, classify the text into ONE OR MORE of the following subcodes using EXACT ID MATCHING.

--- CODEBOOK DEFINITIONS ---
PUSH FACTORS (Dissatisfaction with conventional enhancers):
{push_section}

PULL FACTORS (Attraction to natural nootropics):
{pull_section}

MOORING FACTORS:
{moor_section}
----------------------------
"""

    if approach == "Deductive (PPM Framework Only)":
        prompt = base_instructions + """
Respond in strict JSON format matching exactly this schema:
{
  "is_relevant": true|false,
  "ppm_category": "Push|Pull|Mooring|Mixed", 
  "ppm_subcodes": ["PUSH-01", "MOOR-I02"], 
  "evidence_quotes": ["quote from text mapping to PUSH-01", "quote from text mapping to MOOR-I02"],
  "confidence": "High|Medium|Low",
  "rationale": "Briefly explain why these specific subcodes were chosen"
}"""
    
    elif approach == "Inductive (Emergent Themes Only)":
        prompt = f"""Analyze the text for emergent themes NOT covered by the standard PPM theory. Look for novel patterns, unexpected user behaviors, or distinct narrative elements.

TEXT:
{text_content}

Respond in strict JSON format matching exactly this schema:
{
  "is_relevant": true|false,
  "emergent_themes": ["Clear Name of New Theme 1", "New Theme 2"],
  "evidence_quotes": ["quote 1"],
  "confidence": "High|Medium|Low",
  "rationale": "Brief explanation of these new themes"
}"""
    
    else: # Mixed
        prompt = base_instructions + """
You must ALSO identify "Emergent Themes": key themes that appear but do NOT perfectly fit into the deductive PPM codes provided above.

Respond in strict JSON format matching exactly this schema:
{
  "is_relevant": true|false,
  "ppm_category": "Push|Pull|Mooring|Mixed", 
  "ppm_subcodes": ["PUSH-01", "PULL-03"],
  "emergent_themes": ["Clear Name of New Theme 1"],
  "evidence_quotes": ["quote supporting code", "quote supporting theme"],
  "confidence": "High|Medium|Low",
  "rationale": "Explanation of your deductive coding choices AND any emergent themes found"
}"""
    
    return prompt

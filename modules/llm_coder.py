# modules/llm_coder.py
# ====================
# Automated qualitative coding module for the NootropicRedditScrapePPM thesis tool.
#
# Purpose
# -------
# Applies the Push-Pull-Mooring (PPM) framework to collected Reddit posts via
# local LLM inference (Ollama). Supports three coding approaches:
#   - Deductive (PPM Framework Only): assigns PUSH/PULL/MOOR codes from the
#     31-code codebook
#   - Inductive (Emergent Themes Only): identifies novel themes not covered by PPM
#   - Mixed (PPM + Emergent Themes): both passes in a single prompt
#
# Pipeline
# --------
#   session_state.collected_data
#     → uncoded_data filter (set-based, O(n))
#     → optional stratified sampling by subreddit
#     → create_coding_prompt() per item
#     → ollama.generate_completion() → raw JSON string
#     → json.loads() → coded_item dict
#     → save_coded_data() (DB upsert)
#     → session_state.coded_data update
#
# Ollama notes
# ------------
# ollama_client forces format='json' on all generate requests. This instructs
# the model to output raw JSON with no markdown fences. The json.loads() call
# therefore operates directly on the model output — no stripping required.
#
# Error handling
# --------------
# json.JSONDecodeError (model returned malformed JSON) is caught separately from
# general Exception (Ollama down, network failure). Parse failures do NOT
# increment the consecutive_errors counter — the LLM service is still running
# and the next item may succeed. Connectivity failures DO increment it. The
# consecutive_errors guard stops the run after MAX_CONSECUTIVE_ERRORS connectivity
# failures to prevent a stalled loop.
#
# MOOR_EMERGENT codes (EMER-01 to EMER-05)
# -----------------------------------------
# These are placeholder slots in the 31-code codebook. Their names are [TBD] or
# [Reserved] — they have no definitions yet and are intentionally excluded from
# the LLM prompt. The Mixed coding approach discovers free-text emergent themes
# inductively; the researcher then populates EMER slots based on those findings.
#
# Methodological note — relevance gate
# -------------------------------------
# By default (strict_relevance=False), any post expressing a PPM factor is
# considered relevant, regardless of whether switching intent is explicit.
# Enabling the strict gate (strict_relevance=True) limits relevance to posts
# that explicitly describe or compare switching between stimulant categories.
# See thesis methodology Chapter 3 for the rationale for the default setting.
#
# Exported symbols
# ----------------
#   render()                — Streamlit page entry point (called by app.py)
#   create_coding_prompt()  — prompt builder (used by render(), testable standalone)

import json
import random

import streamlit as st
from datetime import datetime

from modules import ollama_client as ollama
from modules.codebook import CodebookManager, CodeCategory
from utils.db_helpers import load_codebook, log_action, save_coded_data


# ---------------------------------------------------------------------------
# Page entry point
# ---------------------------------------------------------------------------

def render():
    st.header("Automated Qualitative Coding 🤖")

    st.info(
        "This module uses **local computational models via Ollama** to ensure "
        "academic replicability and data privacy.\n\n"
        "**Requirements:** Ollama must be running (`ollama serve`) with at least "
        "one model pulled (e.g. `ollama pull llama3.1`)."
    )

    # --- Connectivity checks ---
    if not ollama.is_ollama_running():
        st.error(
            "⚠️ **Ollama is not reachable.** "
            "Please ensure Ollama is running on localhost:11434."
        )
        st.code("ollama serve", language="bash")
        return

    available_models = ollama.get_available_models()
    if not available_models:
        st.warning("⚠️ **No models found.** Pull a model from your terminal first.")
        st.code("ollama pull llama3.1", language="bash")
        return

    if not st.session_state.collected_data:
        st.warning(
            "⚠️ No collected data found. "
            "Run the Data Collection module first."
        )
        return

    # --- Codebook initialisation ---
    if 'codebook_manager' not in st.session_state:
        saved = load_codebook(st.session_state.get('session_id'))
        if saved:
            st.session_state.codebook_manager = CodebookManager.from_dict(saved)
        else:
            st.session_state.codebook_manager = CodebookManager()

    st.divider()

    # -----------------------------------------------------------------------
    # Coding configuration
    # -----------------------------------------------------------------------
    st.subheader("⚙️ Coding Configuration")

    col1, col2 = st.columns(2)

    with col1:
        coding_approach = st.selectbox(
            "Coding Approach",
            [
                "Deductive (PPM Framework Only)",
                "Inductive (Emergent Themes Only)",
                "Mixed (PPM + Emergent Themes)",
            ],
        )

        # Preferred thesis models first, then everything else alphabetically
        packaged_models = ["llama3.1", "gemma3:12b"]
        available_models.sort(key=lambda x: (x not in packaged_models, x))
        default_index = next(
            (i for i, m in enumerate(available_models) if m in packaged_models), 0
        )

        model_selection = st.selectbox(
            "Local Computational Model",
            available_models,
            index=default_index,
            help=(
                "Models shown in order of preference for this thesis. "
                "llama3.1 and gemma3:12b are the validated thesis models."
            ),
        )

    with col2:
        batch_size = st.number_input(
            "Batch Size",
            min_value=1,
            max_value=120,
            value=25,
            help="Number of items to code in this run.",
        )

        include_already_coded = st.checkbox(
            "Re-code already coded items",
            value=False,
            help="Override existing coding for items that have already been processed.",
        )

        st.write("**Efficiency Mode**")
        use_sampling = st.checkbox(
            "Enable Stratified Sampling",
            value=False,
            help=(
                "Process a balanced random subset rather than the first N items. "
                "Samples evenly across subreddits."
            ),
        )
        if use_sampling:
            sample_size_per_sub = st.number_input(
                "Items per Subreddit",
                min_value=1,
                max_value=100,
                value=10,
            )

        # METHODOLOGICAL NOTE: the relevance gate controls what the LLM considers
        # in-scope. When False (default), any post expressing a PPM factor is coded.
        # When True, only posts explicitly describing switching intent are coded.
        # See module docstring and thesis methodology Chapter 3 for rationale.
        strict_relevance = st.checkbox(
            "Strict relevance gate",
            value=False,
            help=(
                "When enabled, only posts that explicitly describe switching between "
                "stimulant categories are considered relevant. Default (off) codes any "
                "post expressing push, pull, or mooring factors. "
                "See thesis methodology Chapter 3."
            ),
        )

    st.divider()

    # -----------------------------------------------------------------------
    # PPM Framework Definitions expander — sourced from live codebook
    # -----------------------------------------------------------------------
    with st.expander("📖 View PPM Framework Definitions"):
        mgr = st.session_state.codebook_manager
        category_map = {
            "PUSH Factors": CodeCategory.PUSH,
            "PULL Factors": CodeCategory.PULL,
            "MOOR Facilitators": CodeCategory.MOOR_FACILITATOR,
            "MOOR Inhibitors":   CodeCategory.MOOR_INHIBITOR,
        }
        for label, cat in category_map.items():
            codes = mgr.get_by_category(cat)
            st.markdown(f"**{label}**")
            for c in codes:
                if c.name.startswith("[Reserved") or c.name.startswith("[TBD"):
                    continue
                st.markdown(f"- **{c.id} — {c.name}:** {c.definition}")
            st.write("")

    # -----------------------------------------------------------------------
    # Start coding
    # -----------------------------------------------------------------------
    if st.button("🚀 Start Automated Coding", type="primary"):

        # Build O(n) lookup set — avoids O(n²) list comprehension per item
        coded_ids: set = {c.get('id') for c in st.session_state.coded_data}

        uncoded_data = [
            item for item in st.session_state.collected_data
            if include_already_coded or item.get('id') not in coded_ids
        ]

        if not uncoded_data:
            st.info(
                "All items have already been coded. "
                "Enable 'Re-code already coded items' to run again."
            )
            return

        # --- Sampling ---
        if use_sampling:
            st.info(
                f"🎲 Applying Stratified Sampling "
                f"({sample_size_per_sub} per subreddit)..."
            )
            grouped: dict[str, list] = {}
            for item in uncoded_data:
                sub = item.get('subreddit', 'Unknown')
                grouped.setdefault(sub, []).append(item)

            sampled_items: list = []
            for sub, items in grouped.items():
                sampled_items.extend(
                    random.sample(items, sample_size_per_sub)
                    if len(items) > sample_size_per_sub
                    else items
                )
            random.shuffle(sampled_items)
            items_to_code = sampled_items[:batch_size]
            st.write(
                f"Selected {len(items_to_code)} items "
                f"(sample pool: {len(sampled_items)})."
            )
        else:
            items_to_code = uncoded_data[:batch_size]

        st.info(
            f"🔄 Coding {len(items_to_code)} items using **{model_selection}**..."
        )

        progress_bar    = st.progress(0)
        status_text     = st.empty()

        coded_results        : list = []
        consecutive_errors   : int  = 0   # connectivity / runtime failures only
        parse_failures       : int  = 0   # json.JSONDecodeError — model output issues
        MAX_CONSECUTIVE_ERRORS = 3

        for idx, item in enumerate(items_to_code):

            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                st.error(
                    f"🛑 **Stopped early**: {MAX_CONSECUTIVE_ERRORS} consecutive "
                    "connectivity errors. Check that Ollama is running and the "
                    "selected model is loaded."
                )
                break

            status_text.text(f"Coding item {idx + 1} of {len(items_to_code)}...")

            try:
                prompt = create_coding_prompt(item, coding_approach, strict_relevance)
                system_prompt = (
                    "You are an academic research assistant specialised in qualitative "
                    "thematic coding. Output only valid JSON with no markdown formatting."
                )

                raw_response = ollama.generate_completion(
                    model_selection, prompt, system_prompt
                )

                # ollama_client forces format='json' — model output is raw JSON.
                # Markdown-fence stripping is therefore not needed, but we keep
                # a minimal fallback for edge cases where the model ignores the format hint.
                clean_response = raw_response
                if "```json" in raw_response:
                    clean_response = raw_response.split("```json")[1].split("```")[0]
                elif "```" in raw_response:
                    clean_response = raw_response.split("```")[1].split("```")[0]

                # json.JSONDecodeError is caught separately below — see module docstring.
                coding_data = json.loads(clean_response)

                is_relevant = coding_data.get('is_relevant', True)

                # Capture decision logic for audit
                coded_item_meta = {
                    'coded_at':        datetime.now().isoformat(),
                    'coded_by':        f"Ollama-{model_selection}",
                    'coding_approach': coding_approach,
                    'raw_prompt':      prompt,
                    'raw_response':    raw_response,
                    'rationale':       coding_data.get('rationale', ''),
                }

                if not is_relevant:
                    coded_item = {
                        **item,
                        **coded_item_meta,
                        'ppm_category':    'Excluded (Irrelevant)',
                        'ppm_subcodes':    [],
                        'themes':          [],
                        'evidence_quotes': [],
                        'confidence':      'High',
                    }
                else:
                    coded_item = {
                        **item,
                        **coded_item_meta,
                        'ppm_category':    coding_data.get('ppm_category', 'Unknown'),
                        'ppm_subcodes':    coding_data.get('ppm_subcodes', []),
                        'themes':          coding_data.get('emergent_themes', []),
                        'evidence_quotes': coding_data.get('evidence_quotes', []),
                        'confidence':      coding_data.get('confidence', 'Medium'),
                    }

                coded_results.append(coded_item)
                consecutive_errors = 0  # reset on any successful response

            except json.JSONDecodeError as e:
                # The LLM responded but output was not valid JSON.
                # This is a model/prompt quality issue, not a connectivity failure —
                # do NOT increment consecutive_errors; the next item may succeed.
                parse_failures += 1
                st.warning(
                    f"⚠️ Item {item.get('id')}: model returned malformed JSON "
                    f"(parse error: {e}). Skipping item. "
                    f"Total parse failures this run: {parse_failures}."
                )

            except Exception as e:
                # Ollama unreachable, model not loaded, or unexpected runtime error.
                # This type of failure is likely persistent — increment the guard.
                consecutive_errors += 1
                st.warning(
                    f"⚠️ Item {item.get('id')}: connectivity/runtime error "
                    f"({e}). "
                    f"Consecutive errors: {consecutive_errors}/{MAX_CONSECUTIVE_ERRORS}."
                )

            progress_bar.progress((idx + 1) / len(items_to_code))

        # --- Merge results into session state ---
        if include_already_coded and coded_results:
            recoded_ids = {r.get('id') for r in coded_results}
            st.session_state.coded_data = [
                c for c in st.session_state.coded_data
                if c.get('id') not in recoded_ids
            ]

        st.session_state.coded_data.extend(coded_results)

        # --- Update codebook frequency counters ---
        # Must run before log_action so the audit record reflects updated counts.
        if coded_results:
            from modules.codebook import update_codebook_frequencies
            update_codebook_frequencies(coded_results, st.session_state.session_id)

        # --- Persist to DB ---
        saved_count = save_coded_data(coded_results, st.session_state.session_id)

        log_action(
            action='automated_coding_ollama',
            session_id=st.session_state.session_id,
            details={
                'model':           model_selection,
                'coding_approach': coding_approach,
                'items_coded':     len(coded_results),
                'parse_failures':  parse_failures,
                'saved_to_db':     saved_count,
                'strict_relevance': strict_relevance,
            },
        )

        status_text.empty()
        progress_bar.empty()

        st.success(f"✅ Coded and saved **{saved_count}** items.")
        st.info(
            "**Next step →** Review the results in the **📊 Dashboard** or "
            "go to **💾 Data Export & Audit** to generate your thesis files."
        )

        if parse_failures:
            st.warning(
                f"⚠️ {parse_failures} item(s) were skipped due to malformed JSON "
                "output from the model. Consider switching to a more capable model "
                "or reviewing the prompt structure."
            )

        # --- Results summary ---
        st.subheader("📊 Coding Results Summary")

        ppm_dist: dict = {}
        for item in coded_results:
            cat = item.get('ppm_category', 'Unknown')
            ppm_dist[cat] = ppm_dist.get(cat, 0) + 1

        if ppm_dist:
            st.write("**PPM Category Distribution (this run):**")
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

    # -----------------------------------------------------------------------
    # Coded data preview
    # -----------------------------------------------------------------------
    if st.session_state.coded_data:
        st.subheader("📋 Coded Data Preview")
        st.write(f"**Total Coded Items:** {len(st.session_state.coded_data)}")

        for item in st.session_state.coded_data[:5]:
            title_preview = (item.get('title') or 'No title')[:100]
            with st.expander(
                f"{item.get('type', 'item').upper()}: {title_preview}..."
            ):
                st.write(f"**Text:** {(item.get('text') or '')[:500]}...")
                st.write(f"**PPM Category:** {item.get('ppm_category', 'N/A')}")
                st.write(f"**Subcodes:** {', '.join(item.get('ppm_subcodes', []))}")
                st.write(f"**Emergent Themes:** {', '.join(item.get('themes', []))}")
                st.write(f"**Evidence Quotes:** {item.get('evidence_quotes', [])}")
                st.write(f"**Confidence:** {item.get('confidence', 'N/A')}")
                st.write(
                    f"**Coded By:** {item.get('coded_by', 'N/A')} "
                    f"at {item.get('coded_at', 'N/A')}"
                )


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

def create_coding_prompt(item: dict, approach: str, strict_relevance: bool = False) -> str:
    """
    Build the LLM coding prompt for a single collected item.

    Parameters
    ----------
    item : dict
        A collected_data row. Reads 'title' and 'text'.
    approach : str
        One of the three coding approach strings from the UI selectbox.
    strict_relevance : bool
        When True, the LLM is instructed to mark posts as irrelevant unless they
        explicitly describe switching between stimulant categories. When False
        (default), any post expressing a PPM factor is considered relevant.
        See module docstring for methodological rationale.

    Returns
    -------
    str
        Formatted prompt string ready to pass to ollama.generate_completion().
    """
    text_content = (
        f"Title: {item.get('title', '')}\n"
        f"Content: {item.get('text', '')}"
    )

    mgr = st.session_state.codebook_manager

    def format_codebook_section(category_enum: CodeCategory) -> str:
        codes = mgr.get_by_category(category_enum)
        if not codes:
            return "- No codes defined."
        formatted = []
        for c in codes:
            if c.name.startswith("[Reserved") or c.name.startswith("[TBD"):
                continue
            entry = f"{c.id}: {c.name} — {c.definition}"
            if c.include:
                entry += f" (INCLUDE: {c.include})"
            if c.exclude:
                entry += f" (EXCLUDE: {c.exclude})"
            formatted.append(entry)
        return "\n".join(formatted) if formatted else "- No codes defined."

    push_section = format_codebook_section(CodeCategory.PUSH)
    pull_section  = format_codebook_section(CodeCategory.PULL)
    moor_f        = format_codebook_section(CodeCategory.MOOR_FACILITATOR)
    moor_i        = format_codebook_section(CodeCategory.MOOR_INHIBITOR)
    moor_section  = (
        f"FACILITATORS (enable switching):\n{moor_f}\n\n"
        f"INHIBITORS (impede switching):\n{moor_i}"
    )

    # METHODOLOGICAL NOTE: relevance gate wording changes with strict_relevance.
    # Default (False): relevant = any post expressing a PPM factor.
    # Strict (True):   relevant = post explicitly describes switching behaviour.
    # The default is recommended for this thesis to avoid excluding valid push/pull
    # evidence that does not frame itself as a switching narrative.
    if strict_relevance:
        relevance_instruction = (
            "1. Determine if the text is relevant. It is relevant ONLY if the post "
            "explicitly describes or compares switching from conventional stimulants "
            "(caffeine, Adderall, Ritalin, modafinil) to natural nootropics, or vice versa. "
            "Posts discussing only one category in isolation are NOT relevant."
        )
    else:
        relevance_instruction = (
            "1. Determine if the text is relevant. It is relevant if the post expresses "
            "ANY push factor (dissatisfaction with a current stimulant), pull factor "
            "(attraction to an alternative), or mooring factor (barrier or facilitator "
            "of switching). The post does NOT need to explicitly describe switching — "
            "expressions of dissatisfaction or curiosity about alternatives are sufficient."
        )

    base_instructions = f"""You are an expert qualitative research assistant applying the Push-Pull-Mooring (PPM) framework to analyse Reddit posts about nootropics and cognitive enhancement.

REDDIT POST:
{text_content}

YOUR TASK:
{relevance_instruction}
2. If relevant, classify the text into ONE OR MORE of the following subcodes using EXACT ID MATCHING.

--- CODEBOOK DEFINITIONS ---
PUSH FACTORS (dissatisfaction with conventional enhancers):
{push_section}

PULL FACTORS (attraction to natural nootropics):
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
  "ppm_subcodes": ["PUSH-01", "MOOR-I-02"],
  "evidence_quotes": ["exact quote from text supporting PUSH-01", "exact quote supporting MOOR-I-02"],
  "confidence": "High|Medium|Low",
  "rationale": "Brief explanation of why these specific subcodes were chosen"
}"""

    elif approach == "Inductive (Emergent Themes Only)":
        prompt = f"""You are an expert qualitative research assistant. Analyse the text below for emergent themes NOT covered by standard Push-Pull-Mooring (PPM) theory. Look for novel patterns, unexpected user behaviours, or distinct narrative elements.

TEXT:
{text_content}

Respond in strict JSON format matching exactly this schema:
{{
  "is_relevant": true|false,
  "emergent_themes": ["Descriptive Theme Name 1", "Descriptive Theme Name 2"],
  "evidence_quotes": ["exact quote supporting Theme 1"],
  "confidence": "High|Medium|Low",
  "rationale": "Brief explanation of these emergent themes and why they are novel"
}}"""

    else:  # Mixed
        prompt = base_instructions + """
You must ALSO identify any "Emergent Themes": patterns that appear in the text but do NOT fit neatly into the deductive PPM codes above. Name each theme descriptively.

Respond in strict JSON format matching exactly this schema:
{
  "is_relevant": true|false,
  "ppm_category": "Push|Pull|Mooring|Mixed",
  "ppm_subcodes": ["PUSH-01", "PULL-03"],
  "emergent_themes": ["Descriptive Theme Name 1"],
  "evidence_quotes": ["exact quote supporting a deductive code", "exact quote supporting an emergent theme"],
  "confidence": "High|Medium|Low",
  "rationale": "Explanation of deductive coding choices AND any emergent themes identified"
}"""

    return prompt

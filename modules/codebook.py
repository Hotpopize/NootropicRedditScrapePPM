# modules/codebook.py
# ====================
# PPM codebook data model, manager, and Streamlit UI for the
# NootropicRedditScrapePPM thesis tool.
#
# Purpose
# -------
# Defines the 31-code deductive codebook for netnographic analysis of Reddit
# communities using the Push-Pull-Mooring (PPM) framework. Provides CRUD
# operations, serialisation, emergent theme support, and LLM prompt export.
#
# Code ID format
# --------------
# All code IDs follow the pattern CATEGORY-NN: PUSH-01, PULL-01, EMER-01.
# MOOR codes include a type segment: MOOR-F-01 (facilitator), MOOR-I-01 (inhibitor).
# IMPORTANT: Earlier versions of this file used MOOR-F01 / MOOR-I01 (no dash
# before the number). If the database contains rows with the old format, run a
# one-time UPDATE collected_data SET ... to normalise stored ppm_subcodes values.
# The dashed format is canonical from this version forward.
#
# Compliance note — code count
# ----------------------------
# The thesis methodology specifies 31 deductive codes:
#   PUSH-01..07 (7), PULL-01..07 (7), MOOR-F-01..06 (6),
#   MOOR-I-01..06 (6), EMER-01..05 (5) = 31.
# MOOR-F-05 and MOOR-F-06 are currently reserved placeholders pending
# researcher definition during the analysis phase. The thesis methodology
# chapter should document whether these are populated inductively or remain
# as reserved slots.
#
# Exported symbols
# ----------------
#   CodeCategory                     — Enum for code categories
#   Code                             — Dataclass for single codebook entry
#   CodebookManager                  — Business logic class
#   DEFAULT_CODES                    — 31-entry canonical code list
#   get_ppm_keywords()               — Keyword dict derived from include fields
#   update_codebook_frequencies()    — Called by llm_coder.py after coding runs
#   render()                         — Streamlit page entry point

import copy
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Optional

import pandas as pd
import streamlit as st

from utils.db_helpers import load_codebook, log_action, save_codebook


# =============================================================================
# DATA MODEL
# =============================================================================

class CodeCategory(Enum):
    PUSH             = "push"
    PULL             = "pull"
    MOOR_FACILITATOR = "mooring_facilitator"
    MOOR_INHIBITOR   = "mooring_inhibitor"
    EMERGENT         = "emergent"


@dataclass
class Code:
    """Single codebook entry."""
    id:                    str
    category:              CodeCategory
    name:                  str
    definition:            str
    include:               str  = ""
    exclude:               str  = ""
    examples:              str  = ""
    source:                str  = ""
    frequency:             int  = 0
    created_at:            str  = field(default_factory=lambda: datetime.now().isoformat())
    is_emergent_candidate: bool = False

    def to_dict(self) -> dict:
        d = asdict(self)
        d['category'] = self.category.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'Code':
        # Copy to avoid mutating the caller's dict — d['category'] assignment
        # would otherwise modify whatever data structure was passed in.
        d = d.copy()
        # category may arrive as a string (from JSON/DB) or as a CodeCategory
        # instance (from asdict). Both are handled safely.
        if isinstance(d['category'], str):
            d['category'] = CodeCategory(d['category'])
        return cls(**d)


# =============================================================================
# DEFAULT CODEBOOK (31 Deductive Codes)
# =============================================================================

DEFAULT_CODES = [
    # --- PUSH FACTORS (7) ---
    Code(
        id="PUSH-01", category=CodeCategory.PUSH,
        name="Acute Side Effects",
        definition="Immediate negative reactions to caffeine or prescription stimulants",
        include="Jitters, anxiety, palpitations, sleep disruption, energy crashes",
        exclude="Long-term health concerns (→ PUSH-04)",
        source="Fuentes et al. (2024); Dresler et al. (2019); Saiz Garcia et al. (2017)"
    ),
    Code(
        id="PUSH-02", category=CodeCategory.PUSH,
        name="Tolerance",
        definition="Diminishing effectiveness requiring dose escalation",
        include="\"Doesn't work anymore,\" needing higher doses, reduced response over time",
        exclude="Dependency language (→ PUSH-03)",
        source="Sharif et al. (2021); Dresler et al. (2019); Cappelletti et al. (2015)"
    ),
    Code(
        id="PUSH-03", category=CodeCategory.PUSH,
        name="Dependency/Withdrawal",
        definition="Concern about physical or psychological dependence on conventional enhancers",
        include="Withdrawal symptoms, feeling \"hooked,\" inability to function without substance",
        exclude="General efficacy doubt (→ PUSH-05)",
        source="Cappelletti et al. (2015); Schifano et al. (2022)"
    ),
    Code(
        id="PUSH-04", category=CodeCategory.PUSH,
        name="Health Risk Perception",
        definition="Concern about long-term health consequences of conventional enhancers",
        include="Cardiovascular fears, neurological damage concerns, unsustainability",
        exclude="Acute side effects (→ PUSH-01)",
        source="Urban & Gao (2014); Saiz Garcia et al. (2017); Schifano et al. (2022)"
    ),
    Code(
        id="PUSH-05", category=CodeCategory.PUSH,
        name="Efficacy Uncertainty",
        definition="Doubt that conventional enhancers actually improve cognitive performance",
        include="Placebo suspicion, \"doesn't really help,\" variable or inconsistent response",
        exclude="Tolerance-related decline (→ PUSH-02)",
        source="Dresler et al. (2019); Bowman et al. (2023)"
    ),
    Code(
        id="PUSH-06", category=CodeCategory.PUSH,
        name="Cost/Access Barriers",
        definition="Financial or legal barriers to obtaining conventional enhancers",
        include="Prescription requirements, high expense, legality concerns",
        exclude="Natural nootropic costs (→ MOOR-I-02)",
        source="Cavaco et al. (2022); Jones & Newton (2024)"
    ),
    Code(
        id="PUSH-07", category=CodeCategory.PUSH,
        name="Ethical Objections",
        definition="Moral discomfort with pharmaceutical use or authenticity concerns",
        include="Big pharma distrust, \"unnatural\" framing, cheating/fairness concerns",
        exclude="General enhancement ethics (→ MOOR-I-06)",
        source="Keary et al. (2023); Schelle et al. (2014); Franke et al. (2012)"
    ),

    # --- PULL FACTORS (7) ---
    Code(
        id="PULL-01", category=CodeCategory.PULL,
        name="Naturalness",
        definition="Valuing products framed as \"natural,\" \"plant-based,\" or \"clean\"",
        include="Botanical preference, non-synthetic emphasis, organic sourcing",
        exclude="Safety claims without naturalness framing (→ PULL-02)",
        source="Roe & Venkataraman (2021); Chiba & Tanemura (2022)"
    ),
    Code(
        id="PULL-02", category=CodeCategory.PULL,
        name="Perceived Safety",
        definition="Belief that natural nootropics have a safer risk profile",
        include="\"Fewer side effects,\" gentler action, non-toxic perception",
        exclude="Efficacy claims (→ PULL-03)",
        source="O'Hara et al. (2023); Schifano et al. (2022)"
    ),
    Code(
        id="PULL-03", category=CodeCategory.PULL,
        name="Sustainable Benefits",
        definition="Expectation of smooth, sustained cognitive effects without crash cycles",
        include="\"No crash,\" cumulative benefits, lasting effects over time",
        exclude="Immediate energy boost claims",
        source="Grebow (2022)"
    ),
    Code(
        id="PULL-04", category=CodeCategory.PULL,
        name="Holistic Integration",
        definition="Linking cognitive enhancement to broader wellness outcomes",
        include="Sleep quality, mood support, stress management, adaptogenic benefits, wellness and betterment",
        exclude="Single-function cognitive claims (→ PULL-07)",
        source="Roe & Venkataraman (2021); Pop et al. (2024)"
    ),
    Code(
        id="PULL-05", category=CodeCategory.PULL,
        name="Community Endorsement",
        definition="Peer recommendations and community validation driving attraction",
        include="\"Everyone recommends,\" stack sharing, trusted user reviews",
        exclude="Scientific citation or research references",
        source="Cox & Piatkowski (2024); Catalani et al. (2021)"
    ),
    Code(
        id="PULL-06", category=CodeCategory.PULL,
        name="Neuroprotection",
        definition="Long-term brain health maintenance and cognitive decline prevention",
        include="Protective effects, longevity focus, preventive framing",
        exclude="Immediate performance enhancement (→ PULL-07)",
        source="Possemis et al. (2024); Malík & Tlustoš (2022)"
    ),
    Code(
        id="PULL-07", category=CodeCategory.PULL,
        name="Cognitive Specificity",
        definition="Targeting specific cognitive domains rather than general enhancement",
        include="\"For focus,\" memory enhancement, creativity support, domain-specific claims",
        exclude="General \"brain boost\" framing (→ PULL-01)",
        source="Suliman et al. (2016); Fuentes et al. (2024)"
    ),

    # --- MOORING FACILITATORS (6) ---
    # NOTE: MOOR-F-05 and MOOR-F-06 are reserved pending researcher definition.
    # See module docstring compliance note.
    Code(
        id="MOOR-F-01", category=CodeCategory.MOOR_FACILITATOR,
        name="Community Information",
        definition="Online communities reduce uncertainty through shared knowledge",
        include="\"Found answers here,\" protocols, harm reduction guidance, FAQ references",
        exclude="",
        # Bouzoubaa year corrected to 2024 (earlier versions erroneously cited 2023)
        source="Cox & Piatkowski (2024); Bouzoubaa et al. (2024); Krohn & Weninger (2022)"
    ),
    Code(
        id="MOOR-F-02", category=CodeCategory.MOOR_FACILITATOR,
        name="Over the Counter Accessibility",
        definition="Easy access to natural nootropics facilitates switching behaviour",
        include="No prescription required, online ordering ease, retail availability",
        exclude="",
        source="Fuentes et al. (2024); Turnock & Gibbs (2023)"
    ),
    Code(
        id="MOOR-F-03", category=CodeCategory.MOOR_FACILITATOR,
        name="Health Consciousness",
        definition="Proactive health orientation drives openness to alternatives",
        include="Biohacking identity, wellness pursuit, self-optimisation framing",
        exclude="",
        source="Roe & Venkataraman (2021); Hamilton (2018)"
    ),
    Code(
        id="MOOR-F-04", category=CodeCategory.MOOR_FACILITATOR,
        name="Low Switching Costs",
        definition="Transition to natural nootropics perceived as easy or reversible",
        include="\"Easy to try,\" low commitment, reversibility emphasis",
        exclude="",
        source="Wu et al. (2017); Marx (2025)"
    ),
    # COMPLIANCE NOTE: MOOR-F-05 and MOOR-F-06 are reserved slots.
    # The thesis methodology specifies 6 mooring facilitator codes.
    # Define these during analysis when sufficient inductive evidence exists,
    # or document them as intentionally consolidated into MOOR-F-01..04.
    Code(
        id="MOOR-F-05", category=CodeCategory.MOOR_FACILITATOR,
        name="[Reserved]",
        definition="To be defined during analysis — mooring facilitator slot 5",
        include="", exclude="", source="[To be defined]"
    ),
    Code(
        id="MOOR-F-06", category=CodeCategory.MOOR_FACILITATOR,
        name="[Reserved]",
        definition="To be defined during analysis — mooring facilitator slot 6",
        include="", exclude="", source="[To be defined]"
    ),

    # --- MOORING INHIBITORS (6) ---
    Code(
        id="MOOR-I-01", category=CodeCategory.MOOR_INHIBITOR,
        name="Habit/Inertia",
        definition="Established routines anchor users to conventional enhancers",
        include="Coffee ritual attachment, \"hard to change,\" routine dependency",
        exclude="",
        source="Marx (2025); Krishnan & Raghuram (2024)"
    ),
    Code(
        id="MOOR-I-02", category=CodeCategory.MOOR_INHIBITOR,
        name="Financial Costs",
        definition="Financial barriers to natural nootropic alternatives",
        include="Supplement expense, \"too expensive,\" cost comparison concerns",
        exclude="",
        source="Marx (2025)"
    ),
    Code(
        id="MOOR-I-03", category=CodeCategory.MOOR_INHIBITOR,
        name="Learning Costs",
        definition="Cognitive effort required to understand new substances",
        include="Complexity overwhelms, research burden, indicator overload",
        exclude="",
        source="Bansal et al. (2005); Cox & Piatkowski (2024)"
    ),
    Code(
        id="MOOR-I-04", category=CodeCategory.MOOR_INHIBITOR,
        name="Information Asymmetry",
        definition="Doubt arising from unverified claims or conflicting information",
        include="\"Don't know what to trust,\" lack of research, conflicting advice",
        exclude="",
        source="Fuentes et al. (2024); Napoletano et al. (2020); Nugroho & Wang (2023)"
    ),
    Code(
        id="MOOR-I-05", category=CodeCategory.MOOR_INHIBITOR,
        name="Social Stigma",
        definition="Peer scepticism or professional stigma around cognitive enhancement",
        include="\"People think it's weird,\" workplace disclosure concerns",
        exclude="",
        source="Champagne et al. (2019); Jones & Newton (2024); Janssen et al. (2018)"
    ),
    Code(
        id="MOOR-I-06", category=CodeCategory.MOOR_INHIBITOR,
        name="Ethical Concerns",
        definition="Moral hesitation about cognitive enhancement in general",
        include="\"Is this cheating?,\" fairness concerns, authenticity doubt",
        exclude="",
        source="Keary et al. (2023); Bárd (2023); Schelle et al. (2014)"
    ),

    # --- EMERGENT (5 Placeholders) ---
    # These slots are populated inductively during analysis.
    # EMER-01 is TBD; EMER-02..05 are reserved. All are excluded from LLM prompts
    # until populated — see the filter in CodebookManager.to_llm_prompt().
    Code(
        id="EMER-01", category=CodeCategory.EMERGENT,
        name="[TBD]",
        definition="[To be developed inductively during analysis]",
        include="[TBD]", exclude="", source="[TBD]"
    ),
    Code(id="EMER-02", category=CodeCategory.EMERGENT, name="[Reserved]",
         definition="To be defined during analysis", include="", exclude="", source="Inductive"),
    Code(id="EMER-03", category=CodeCategory.EMERGENT, name="[Reserved]",
         definition="To be defined during analysis", include="", exclude="", source="Inductive"),
    Code(id="EMER-04", category=CodeCategory.EMERGENT, name="[Reserved]",
         definition="To be defined during analysis", include="", exclude="", source="Inductive"),
    Code(id="EMER-05", category=CodeCategory.EMERGENT, name="[Reserved]",
         definition="To be defined during analysis", include="", exclude="", source="Inductive"),
]


def get_ppm_keywords() -> dict:
    """
    Generate the PPM keyword dict from include fields in DEFAULT_CODES.
    Used by reddit_service.get_ppm_tags() for auto-tagging collected posts.
    Placeholder codes (names starting with '[') are excluded.
    """
    keywords: dict[str, list[str]] = {'Push': [], 'Pull': [], 'Mooring': []}

    for code in DEFAULT_CODES:
        if not code.include or code.name.startswith("["):
            continue
        phrases = [p.strip().lower().replace("\"", "") for p in code.include.split(",")]
        if code.category == CodeCategory.PUSH:
            keywords['Push'].extend(phrases)
        elif code.category == CodeCategory.PULL:
            keywords['Pull'].extend(phrases)
        elif code.category in (CodeCategory.MOOR_FACILITATOR, CodeCategory.MOOR_INHIBITOR):
            keywords['Mooring'].extend(phrases)

    return {k: list(dict.fromkeys(v)) for k, v in keywords.items()}


# =============================================================================
# CODEBOOK MANAGER
# =============================================================================

class CodebookManager:
    """
    Business logic for codebook operations. Separates data from UI.

    Instantiation
    -------------
    CodebookManager()           — loads DEFAULT_CODES (deep copy, enum-safe)
    CodebookManager(codes=[..]) — loads provided Code instances
    CodebookManager.from_dict() — restores from DB-serialised dict
    """

    def __init__(self, codes: list[Code] = None):
        # Deep copy DEFAULT_CODES to avoid shared mutable state across instances.
        # copy.deepcopy is used rather than [Code(**asdict(c)) ...] for clarity,
        # though both are functionally equivalent (asdict preserves enum instances).
        self.codes = codes if codes is not None else copy.deepcopy(DEFAULT_CODES)
        self._emergent_counter = self._get_max_emergent_id() + 1

    def _get_max_emergent_id(self) -> int:
        """Find highest EMER-NN number for auto-increment."""
        max_id = 0
        for code in self.codes:
            if code.category == CodeCategory.EMERGENT and code.id.startswith("EMER-"):
                try:
                    num = int(code.id.split("-")[1])
                    max_id = max(max_id, num)
                except (ValueError, IndexError):
                    pass
        return max_id

    # --- CRUD ---

    def get_all(self) -> list[Code]:
        return self.codes

    def get_by_id(self, code_id: str) -> Optional[Code]:
        return next((c for c in self.codes if c.id == code_id), None)

    def get_by_category(self, category: CodeCategory) -> list[Code]:
        return [c for c in self.codes if c.category == category]

    def add_code(self, code: Code) -> None:
        if self.get_by_id(code.id):
            raise ValueError(f"Code {code.id} already exists")
        self.codes.append(code)

    def update_code(self, code_id: str, **kwargs) -> None:
        code = self.get_by_id(code_id)
        if not code:
            raise ValueError(f"Code {code_id} not found")
        for k, v in kwargs.items():
            if hasattr(code, k):
                setattr(code, k, v)

    def delete_code(self, code_id: str) -> None:
        self.codes = [c for c in self.codes if c.id != code_id]

    def increment_frequency(self, code_id: str) -> None:
        """Increment usage counter for a code. No-op if code_id not found."""
        code = self.get_by_id(code_id)
        if code:
            code.frequency += 1

    # --- Emergent Theme Support ---

    def create_emergent_code(self, name: str, definition: str,
                              examples: str = "", source: str = "Inductive") -> Code:
        """Create a new emergent code with auto-incremented ID."""
        new_id = f"EMER-{self._emergent_counter:02d}"
        self._emergent_counter += 1
        new_code = Code(
            id=new_id, category=CodeCategory.EMERGENT,
            name=name, definition=definition, examples=examples, source=source,
        )
        self.codes.append(new_code)
        return new_code

    def flag_emergent_candidate(self, text: str, suggested_theme: str) -> dict:
        """
        Return a candidate object for the researcher review queue.
        The candidate is NOT added to the codebook automatically — the
        researcher approves or rejects it in the Emergent Queue tab.
        """
        return {
            "text":            text,
            "suggested_theme": suggested_theme,
            "flagged_at":      datetime.now().isoformat(),
            "status":          "pending",
            "assigned_code":   None,
        }

    # --- LLM Prompt Export ---

    def to_llm_prompt(self) -> str:
        """
        Export codebook as a standalone LLM coding prompt.

        NOTE: This method is NOT used by the main coding pipeline.
        modules/llm_coder.py builds its own prompt via create_coding_prompt()
        using format_codebook_section() calls. This method produces a
        self-contained prompt for standalone Ollama experiments or manual
        verification. Its output schema matches the llm_coder.py format.

        Placeholder codes (names starting with '[') are excluded so the
        LLM is not shown undefined or reserved entries.
        """
        prompt = (
            "You are an expert qualitative research assistant applying the "
            "Push-Pull-Mooring (PPM) framework to analyse Reddit posts about "
            "nootropics and cognitive enhancement.\n\n"
            "## CODEBOOK\n\n"
            "Assign ONE OR MORE codes from the categories below. "
            "If no code fits, set is_relevant=false.\n\n"
        )

        categories = [
            (CodeCategory.PUSH,             "PUSH FACTORS (Dissatisfaction with conventional enhancers)"),
            (CodeCategory.PULL,             "PULL FACTORS (Attraction to natural nootropics)"),
            (CodeCategory.MOOR_FACILITATOR, "MOORING FACILITATORS (Enable switching)"),
            (CodeCategory.MOOR_INHIBITOR,   "MOORING INHIBITORS (Impede switching)"),
        ]
        # EMERGENT codes are excluded — placeholder slots are not shown to the LLM.

        for cat, title in categories:
            codes = self.get_by_category(cat)
            prompt += f"### {title}\n"
            for code in codes:
                # Exclude ALL placeholder entries — names starting with '['
                # covers both [Reserved] and [TBD] variants.
                if code.name.startswith("["):
                    continue
                prompt += f"- **{code.id}** {code.name}: {code.definition}\n"
                if code.include:
                    prompt += f"  - Include: {code.include}\n"
            prompt += "\n"

        prompt += (
            "## OUTPUT FORMAT\n"
            "Return valid JSON only, no markdown fences:\n"
            "{\n"
            "  \"is_relevant\": true|false,\n"
            "  \"ppm_category\": \"Push|Pull|Mooring|Mixed\",\n"
            "  \"ppm_subcodes\": [\"PUSH-01\", \"MOOR-I-02\"],\n"
            "  \"evidence_quotes\": [\"exact quote supporting PUSH-01\"],\n"
            "  \"confidence\": \"High|Medium|Low\",\n"
            "  \"rationale\": \"Brief explanation of coding choices\"\n"
            "}\n"
        )
        return prompt

    def to_compact_prompt(self) -> str:
        """Minimal prompt for token-constrained contexts."""
        lines = ["PPM CODES:"]
        for code in self.codes:
            if not code.name.startswith("["):
                lines.append(f"{code.id}: {code.name}")
        lines.append(
            "\nReturn JSON: "
            "{ppm_subcodes: [...], confidence: High|Medium|Low, rationale: str}"
        )
        return "\n".join(lines)

    # --- Serialisation ---

    def to_dict(self) -> dict:
        """Serialise for database storage via save_codebook()."""
        return {
            "codes":       [c.to_dict() for c in self.codes],
            "exported_at": datetime.now().isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'CodebookManager':
        """Restore from a dict produced by to_dict(). Used by load_codebook()."""
        codes = [Code.from_dict(c) for c in data.get("codes", [])]
        return cls(codes=codes)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    def to_csv_rows(self) -> list[dict]:
        """Export for CSV/DataFrame (used by data_manager.py)."""
        return [
            {
                "ID":         c.id,
                "Category":   c.category.value,
                "Name":       c.name,
                "Definition": c.definition,
                "Include":    c.include,
                "Exclude":    c.exclude,
                "Source":     c.source,
                "Frequency":  c.frequency,
            }
            for c in self.codes
        ]

    def get_statistics(self) -> dict:
        stats = {cat.value: {"count": 0, "total_freq": 0} for cat in CodeCategory}
        for code in self.codes:
            stats[code.category.value]["count"]      += 1
            stats[code.category.value]["total_freq"] += code.frequency
        stats["total_codes"]  = len(self.codes)
        stats["total_coded"]  = sum(c.frequency for c in self.codes)
        return stats


# =============================================================================
# FREQUENCY UPDATE HELPER (called from llm_coder.py)
# =============================================================================

def update_codebook_frequencies(coded_results: list[dict], session_id: str) -> None:
    """
    Increment code frequency counters based on a completed coding run.

    Called by modules/llm_coder.py after each batch coding run — after
    coded_results are confirmed non-empty and before log_action().

    Reads codebook_manager from session_state, increments frequency for each
    subcode in every coded item, then persists the updated codebook to DB.

    No-op if session_state has no codebook_manager or coded_results is empty.
    """
    if not coded_results:
        return

    mgr: Optional[CodebookManager] = st.session_state.get('codebook_manager')
    if mgr is None:
        return

    for item in coded_results:
        for code_id in item.get('ppm_subcodes', []):
            mgr.increment_frequency(code_id)

    save_codebook(mgr.to_dict(), session_id)


# =============================================================================
# STREAMLIT UI
# =============================================================================

def render() -> None:
    """Streamlit page entry point — called by app.py routing."""
    st.header("📖 Codebook: PPM Framework")

    # Initialise manager from session state or database
    if 'codebook_manager' not in st.session_state:
        saved = load_codebook(st.session_state.get('session_id'))
        if saved:
            st.session_state.codebook_manager = CodebookManager.from_dict(saved)
        else:
            st.session_state.codebook_manager = CodebookManager()

    mgr = st.session_state.codebook_manager

    deductive_count = len([c for c in mgr.codes if c.category != CodeCategory.EMERGENT])
    emergent_count  = len(mgr.get_by_category(CodeCategory.EMERGENT))
    total_freq      = sum(c.frequency for c in mgr.codes)

    st.info(
        f"**Deductive Codes:** {deductive_count} | "
        f"**Emergent Codes:** {emergent_count} | "
        f"**Total Coded:** {total_freq}"
    )

    tab_view, tab_edit, tab_emergent, tab_export = st.tabs([
        "📚 View", "➕ Add/Edit", "🔬 Emergent Queue", "💾 Export"
    ])

    with tab_view:
        _render_view_tab(mgr)

    with tab_edit:
        _render_edit_tab(mgr)

    with tab_emergent:
        _render_emergent_tab(mgr)

    with tab_export:
        _render_export_tab(mgr)


def _render_view_tab(mgr: CodebookManager) -> None:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Push Factors")
        for code in mgr.get_by_category(CodeCategory.PUSH):
            with st.expander(f"**[{code.id}]** {code.name} ({code.frequency})"):
                st.write(f"**Definition:** {code.definition}")
                st.caption(f"Include: {code.include}")
                st.caption(f"Source: {code.source}")

        st.subheader("Pull Factors")
        for code in mgr.get_by_category(CodeCategory.PULL):
            with st.expander(f"**[{code.id}]** {code.name} ({code.frequency})"):
                st.write(f"**Definition:** {code.definition}")
                st.caption(f"Include: {code.include}")
                st.caption(f"Source: {code.source}")

    with col2:
        st.subheader("Mooring Facilitators")
        for code in mgr.get_by_category(CodeCategory.MOOR_FACILITATOR):
            with st.expander(f"**[{code.id}]** {code.name} ({code.frequency})"):
                st.write(f"**Definition:** {code.definition}")
                st.caption(f"Include: {code.include}")

        st.subheader("Mooring Inhibitors")
        for code in mgr.get_by_category(CodeCategory.MOOR_INHIBITOR):
            with st.expander(f"**[{code.id}]** {code.name} ({code.frequency})"):
                st.write(f"**Definition:** {code.definition}")
                st.caption(f"Include: {code.include}")

        st.subheader("Emergent Themes")
        emergent = [
            c for c in mgr.get_by_category(CodeCategory.EMERGENT)
            if not c.name.startswith("[")
        ]
        if emergent:
            for code in emergent:
                with st.expander(f"**[{code.id}]** {code.name} ({code.frequency})"):
                    st.write(f"**Definition:** {code.definition}")
        else:
            st.caption("No emergent themes defined yet.")


def _render_edit_tab(mgr: CodebookManager) -> None:
    st.subheader("Add or Edit Code")

    category   = st.selectbox("Category", [c.value for c in CodeCategory])
    code_id    = st.text_input("Code ID", placeholder="e.g., PUSH-01 or MOOR-F-01")
    name       = st.text_input("Name")
    definition = st.text_area("Definition")
    include    = st.text_input("Include Criteria")
    exclude    = st.text_input("Exclude Criteria")
    source     = st.text_input("Source")

    if st.button("Save Code", type="primary"):
        if not code_id.strip():
            st.error("Code ID is required. Use the format PUSH-01 or MOOR-F-01.")
            return
        if not name.strip() or not definition.strip():
            st.error("Name and Definition are required.")
            return

        existing = mgr.get_by_id(code_id)
        if existing:
            mgr.update_code(
                code_id, name=name, definition=definition,
                include=include, exclude=exclude, source=source,
            )
            st.success(f"Updated {code_id}")
        else:
            new_code = Code(
                id=code_id, category=CodeCategory(category),
                name=name, definition=definition,
                include=include, exclude=exclude, source=source,
            )
            mgr.add_code(new_code)
            st.success(f"Added {code_id}")

        save_codebook(mgr.to_dict(), st.session_state.get('session_id'))
        st.rerun()


def _render_emergent_tab(mgr: CodebookManager) -> None:
    st.subheader("🔬 Emergent Theme Management")
    st.write("Create new inductive codes discovered during analysis.")

    with st.form("emergent_form"):
        name       = st.text_input("Theme Name")
        definition = st.text_area("Definition")
        examples   = st.text_area("Example Excerpts")

        if st.form_submit_button("Create Emergent Code"):
            if name and definition:
                new_code = mgr.create_emergent_code(name, definition, examples)
                save_codebook(mgr.to_dict(), st.session_state.get('session_id'))
                st.success(f"Created {new_code.id}: {new_code.name}")
                st.rerun()

    st.divider()
    st.subheader("Flagged Candidates")

    queue = st.session_state.get('emergent_queue', [])
    if queue:
        for i, candidate in enumerate(queue):
            with st.expander(f"Candidate: {candidate['suggested_theme']}"):
                st.write(f"**Text:** {candidate['text'][:200]}...")
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Approve", key=f"approve_{i}"):
                        mgr.create_emergent_code(
                            candidate['suggested_theme'],
                            "User-approved emergent theme",
                            candidate['text'][:200],
                        )
                        # pop(i) is safe here: st.rerun() fires immediately after
                        # this block, so the loop does not continue to the next
                        # iteration with a stale index. The queue is re-read fresh
                        # on the next render cycle.
                        st.session_state.emergent_queue.pop(i)
                        save_codebook(mgr.to_dict(), st.session_state.get('session_id'))
                        st.rerun()
                with col2:
                    if st.button("Reject", key=f"reject_{i}"):
                        st.session_state.emergent_queue.pop(i)
                        st.rerun()
    else:
        st.caption(
            "No flagged candidates. The LLM coder will flag text that does not "
            "fit existing codes when using the Mixed coding approach."
        )


def _render_export_tab(mgr: CodebookManager) -> None:
    st.subheader("Export Options")

    col1, col2, col3 = st.columns(3)

    with col1:
        df = pd.DataFrame(mgr.to_csv_rows())
        st.download_button(
            "📄 Download CSV",
            data=df.to_csv(index=False).encode('utf-8'),
            file_name="codebook.csv",
            mime="text/csv",
        )

    with col2:
        st.download_button(
            "📋 Download JSON",
            data=mgr.to_json().encode('utf-8'),
            file_name="codebook.json",
            mime="application/json",
        )

    with col3:
        prompt = mgr.to_llm_prompt()
        st.download_button(
            "🤖 Download Coding Prompt",
            data=prompt.encode('utf-8'),
            file_name="ppm_coding_prompt.txt",
            mime="text/plain",
        )
        with st.expander("Preview Prompt"):
            st.code(prompt[:1000] + "...")

    st.divider()
    st.subheader("Statistics")
    st.json(mgr.get_statistics())


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    mgr = CodebookManager()
    print(f"Total codes: {len(mgr.codes)}")
    print(mgr.to_llm_prompt())

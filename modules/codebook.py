"""
Codebook Module: PPM Framework for Nootropics Discourse Analysis
Refactored for NootropicPPMRedditScrape

Supports:
- 31 deductive codes (PUSH, PULL, MOOR-F, MOOR-I, EMER)
- Emergent theme flagging during analysis
- Computational prompt export for local model integration
"""

import json
from dataclasses import dataclass, field, asdict
from typing import Optional
from datetime import datetime
from enum import Enum

# =============================================================================
# DATA MODEL
# =============================================================================


class CodeCategory(Enum):
    PUSH = "push"
    PULL = "pull"
    MOOR_FACILITATOR = "mooring_facilitator"
    MOOR_INHIBITOR = "mooring_inhibitor"
    EMERGENT = "emergent"


@dataclass
class Code:
    """Single codebook entry."""
    id: str
    category: CodeCategory
    name: str
    definition: str
    include: str = ""
    exclude: str = ""
    examples: str = ""
    source: str = ""
    frequency: int = 0
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    is_emergent_candidate: bool = False  # Flagged during analysis

    def to_dict(self) -> dict:
        d = asdict(self)
        d['category'] = self.category.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'Code':
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
        exclude="Natural nootropic costs (→ MOOR-I02)",
        source="Cavaco et al. (2022); Jones & Newton (2024)"
    ),
    Code(
        id="PUSH-07", category=CodeCategory.PUSH,
        name="Ethical Objections",
        definition="Moral discomfort with pharmaceutical use or authenticity concerns",
        include="Big pharma distrust, \"unnatural\" framing, cheating/fairness concerns",
        exclude="General enhancement ethics (→ MOOR-I06)",
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

    # --- MOORING FACILITATORS (4) ---
    Code(
        id="MOOR-F01", category=CodeCategory.MOOR_FACILITATOR,
        name="Community Information",
        definition="Online communities reduce uncertainty through shared knowledge",
        include="\"Found answers here,\" protocols, harm reduction guidance, FAQ references",
        exclude="",
        source="Cox & Piatkowski (2024); Bouzoubaa et al. (2023); Krohn & Weninger (2022)"
    ),
    Code(
        id="MOOR-F02", category=CodeCategory.MOOR_FACILITATOR,
        name="Over the counter Accessibility",
        definition="Easy access to natural nootropics facilitates switching behaviour",
        include="No prescription required, online ordering ease, retail availability",
        exclude="",
        source="Fuentes et al. (2024); Turnock & Gibbs (2023)"
    ),
    Code(
        id="MOOR-F03", category=CodeCategory.MOOR_FACILITATOR,
        name="Health Consciousness",
        definition="Proactive health orientation drives openness to alternatives",
        include="Biohacking identity, wellness pursuit, self-optimisation framing",
        exclude="",
        source="Roe & Venkataraman (2021); Hamilton (2018)"
    ),
    Code(
        id="MOOR-F04", category=CodeCategory.MOOR_FACILITATOR,
        name="Low Switching Costs",
        definition="Transition to natural nootropics perceived as easy or reversible",
        include="\"Easy to try,\" low commitment, reversibility emphasis",
        exclude="",
        source="Wu et al. (2017); Marx (2025)"
    ),

    # --- MOORING INHIBITORS (6) ---
    Code(
        id="MOOR-I01", category=CodeCategory.MOOR_INHIBITOR,
        name="Habit/Inertia",
        definition="Established routines anchor users to conventional enhancers",
        include="Coffee ritual attachment, \"hard to change,\" routine dependency",
        exclude="",
        source="Marx (2025); Krishnan & Raghuram (2024)"
    ),
    Code(
        id="MOOR-I02", category=CodeCategory.MOOR_INHIBITOR,
        name="Financial Costs",
        definition="Financial barriers to natural nootropic alternatives",
        include="Supplement expense, \"too expensive,\" cost comparison concerns",
        exclude="",
        source="Marx (2025)"
    ),
    Code(
        id="MOOR-I03", category=CodeCategory.MOOR_INHIBITOR,
        name="Learning Costs",
        definition="Cognitive effort required to understand new substances",
        include="Complexity overwhelms, research burden, information overload",
        exclude="",
        source="Bansal et al. (2005); Cox & Piatkowski (2024)"
    ),
    Code(
        id="MOOR-I04", category=CodeCategory.MOOR_INHIBITOR,
        name="Information Asymmetry",
        definition="Doubt arising from unverified claims or conflicting information",
        include="\"Don't know what to trust,\" lack of research, conflicting advice",
        exclude="",
        source="Fuentes et al. (2024); Napoletano et al. (2020); Nugroho & Wang (2023)"
    ),
    Code(
        id="MOOR-I05", category=CodeCategory.MOOR_INHIBITOR,
        name="Social Stigma",
        definition="Peer scepticism or professional stigma around cognitive enhancement",
        include="\"People think it's weird,\" workplace disclosure concerns",
        exclude="",
        source="Champagne et al. (2019); Jones & Newton (2024); Janssen et al. (2018)"
    ),
    Code(
        id="MOOR-I06", category=CodeCategory.MOOR_INHIBITOR,
        name="Ethical Concerns",
        definition="Moral hesitation about cognitive enhancement in general",
        include="\"Is this cheating?,\" fairness concerns, authenticity doubt",
        exclude="",
        source="Keary et al. (2023); Bárd (2023); Schelle et al. (2014)"
    ),

    # --- EMERGENT (5 Placeholders) ---
    Code(id="EMER-01", category=CodeCategory.EMERGENT, name="[TBD]", definition="[To be developed inductively during analysis]", include="[TBD]", exclude="", source="[TBD]"),
    Code(id="EMER-02", category=CodeCategory.EMERGENT, name="[Reserved]", definition="To be defined during analysis", include="", exclude="", source="Inductive"),
    Code(id="EMER-03", category=CodeCategory.EMERGENT, name="[Reserved]", definition="To be defined during analysis", include="", exclude="", source="Inductive"),
    Code(id="EMER-04", category=CodeCategory.EMERGENT, name="[Reserved]", definition="To be defined during analysis", include="", exclude="", source="Inductive"),
    Code(id="EMER-05", category=CodeCategory.EMERGENT, name="[Reserved]", definition="To be defined during analysis", include="", exclude="", source="Inductive"),
]


def get_ppm_keywords() -> dict:
    """
    Dynamically generates the PPM keyword dictionary from the DEFAULT_CODES.
    This extracts the `include` string and splits it by commas for pure text tagging.
    """
    keywords = {
        'Push': [],
        'Pull': [],
        'Mooring': []
    }

    for code in DEFAULT_CODES:
        if not code.include or code.name.startswith("["):
            continue

        # Extract individual phrases/words by splitting on commas
        phrases = [p.strip().lower().replace("\"", "") for p in code.include.split(",")]

        if code.category == CodeCategory.PUSH:
            keywords['Push'].extend(phrases)
        elif code.category == CodeCategory.PULL:
            keywords['Pull'].extend(phrases)
        elif code.category in [CodeCategory.MOOR_FACILITATOR, CodeCategory.MOOR_INHIBITOR]:
            keywords['Mooring'].extend(phrases)

    # Deduplicate while preserving general order
    return {k: list(dict.fromkeys(v)) for k, v in keywords.items()}

# =============================================================================
# CODEBOOK MANAGER
# =============================================================================

class CodebookManager:
    """
    Manages PPM codebook operations.
    Separates business logic from UI layer.
    """

    def __init__(self, codes: list[Code] = None):
        self.codes = codes if codes else [Code(**asdict(c)) for c in DEFAULT_CODES]
        self._emergent_counter = self._get_max_emergent_id() + 1

    def _get_max_emergent_id(self) -> int:
        """Find highest EMER-XX number for auto-increment."""
        max_id = 0
        for code in self.codes:
            if code.category == CodeCategory.EMERGENT and code.id.startswith("EMER-"):
                try:
                    num = int(code.id.split("-")[1])
                    max_id = max(max_id, num)
                except ValueError:
                    pass
        return max_id

    # --- CRUD Operations ---

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
        code = self.get_by_id(code_id)
        if code:
            code.frequency += 1

    # --- Emergent Theme Support ---

    def create_emergent_code(self, name: str, definition: str,
                              examples: str = "", source: str = "Inductive") -> Code:
        """Create new emergent code with auto-incremented ID."""
        new_id = f"EMER-{self._emergent_counter:02d}"
        self._emergent_counter += 1

        new_code = Code(
            id=new_id,
            category=CodeCategory.EMERGENT,
            name=name,
            definition=definition,
            examples=examples,
            source=source
        )
        self.codes.append(new_code)
        return new_code

    def flag_emergent_candidate(self, text: str, suggested_theme: str) -> dict:
        """
        Flag text as potential emergent theme for researcher review.
        Returns candidate object for review queue.
        """
        return {
            "text": text,
            "suggested_theme": suggested_theme,
            "flagged_at": datetime.now().isoformat(),
            "status": "pending",  # pending | approved | rejected
            "assigned_code": None
        }

    # --- LLM Prompt Export ---

    def to_llm_prompt(self) -> str:
        """
        Export codebook as structured prompt for Ollama coding.
        Format optimised for llama3.1/gemma3:12b comprehension.
        """
        prompt = """You are a qualitative research assistant applying the Push-Pull-Mooring (PPM) framework to analyse Reddit posts about nootropics and cognitive enhancement.

## CODEBOOK

Apply ONE OR MORE codes from the following categories. If no code fits, flag as EMERGENT_CANDIDATE.

"""
        categories = [
            (CodeCategory.PUSH, "PUSH FACTORS (Dissatisfaction with conventional enhancers)"),
            (CodeCategory.PULL, "PULL FACTORS (Attraction to natural nootropics)"),
            (CodeCategory.MOOR_FACILITATOR, "MOORING FACILITATORS (Enable switching)"),
            (CodeCategory.MOOR_INHIBITOR, "MOORING INHIBITORS (Impede switching)"),
            (CodeCategory.EMERGENT, "EMERGENT (Inductive themes)")
        ]

        for cat, title in categories:
            codes = self.get_by_category(cat)
            prompt += f"### {title}\n"
            for code in codes:
                if code.name.startswith("[Reserved"):
                    continue
                prompt += f"- **{code.id}** {code.name}: {code.definition}\n"
                if code.include:
                    prompt += f"  - Include: {code.include}\n"
            prompt += "\n"

        prompt += """## OUTPUT FORMAT
Return JSON:
{
  "codes": ["CODE-ID", ...],
  "confidence": 0.0-1.0,
  "emergent_flag": true/false,
  "suggested_theme": "theme name if emergent_flag is true, else null",
  "rationale": "brief justification"
}
"""
        return prompt

    def to_compact_prompt(self) -> str:
        """Minimal prompt for token-constrained contexts."""
        lines = ["PPM CODES:"]
        for code in self.codes:
            if not code.name.startswith("[Reserved"):
                lines.append(f"{code.id}: {code.name}")
        lines.append("\nReturn JSON: {codes: [...], confidence: 0-1, emergent_flag: bool}")
        return "\n".join(lines)

    # --- Serialisation ---

    def to_dict(self) -> dict:
        """Export for database storage."""
        return {
            "codes": [c.to_dict() for c in self.codes],
            "exported_at": datetime.now().isoformat()
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'CodebookManager':
        codes = [Code.from_dict(c) for c in data.get("codes", [])]
        return cls(codes=codes)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    def to_csv_rows(self) -> list[dict]:
        """Export for CSV/DataFrame."""
        return [{
            "ID": c.id,
            "Category": c.category.value,
            "Name": c.name,
            "Definition": c.definition,
            "Include": c.include,
            "Exclude": c.exclude,
            "Source": c.source,
            "Frequency": c.frequency
        } for c in self.codes]

    # --- Statistics ---

    def get_statistics(self) -> dict:
        stats = {cat.value: {"count": 0, "total_freq": 0} for cat in CodeCategory}
        for code in self.codes:
            stats[code.category.value]["count"] += 1
            stats[code.category.value]["total_freq"] += code.frequency
        stats["total_codes"] = len(self.codes)
        stats["total_coded"] = sum(c.frequency for c in self.codes)
        return stats


# =============================================================================
# STREAMLIT UI (Separated from logic)
# =============================================================================

def render():
    """Streamlit UI for codebook management."""
    import streamlit as st
    import pandas as pd
    from utils.db_helpers import save_codebook, load_codebook

    st.header("📖 Codebook: PPM Framework")

    # Initialise manager from session state or database
    if 'codebook_manager' not in st.session_state:
        saved = load_codebook(st.session_state.get('session_id'))
        if saved:
            st.session_state.codebook_manager = CodebookManager.from_dict(saved)
        else:
            st.session_state.codebook_manager = CodebookManager()

    mgr = st.session_state.codebook_manager

    # --- Info Banner ---
    st.info(f"""
    **Deductive Codes:** {len([c for c in mgr.codes if c.category != CodeCategory.EMERGENT])} |
    **Emergent Codes:** {len(mgr.get_by_category(CodeCategory.EMERGENT))} |
    **Total Coded:** {sum(c.frequency for c in mgr.codes)}
    """)

    # --- Tabs ---
    tab1, tab2, tab3, tab4 = st.tabs(["📚 View", "➕ Add/Edit", "🔬 Emergent Queue", "💾 Export"])

    with tab1:
        _render_view_tab(mgr)

    with tab2:
        _render_edit_tab(mgr)

    with tab3:
        _render_emergent_tab(mgr)

    with tab4:
        _render_export_tab(mgr)


def _render_view_tab(mgr: CodebookManager):
    import streamlit as st

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
        emergent = mgr.get_by_category(CodeCategory.EMERGENT)
        if emergent:
            for code in emergent:
                if not code.name.startswith("[Reserved"):
                    with st.expander(f"**[{code.id}]** {code.name} ({code.frequency})"):
                        st.write(f"**Definition:** {code.definition}")
        else:
            st.caption("No emergent themes yet.")


def _render_edit_tab(mgr: CodebookManager):
    import streamlit as st
    from utils.db_helpers import save_codebook

    st.subheader("Add or Edit Code")

    category = st.selectbox("Category", [c.value for c in CodeCategory])
    code_id = st.text_input("Code ID", placeholder="e.g., PUSH-01")
    name = st.text_input("Name")
    definition = st.text_area("Definition")
    include = st.text_input("Include Criteria")
    exclude = st.text_input("Exclude Criteria")
    source = st.text_input("Source")

    if st.button("Save Code", type="primary"):
        if name and definition:
            existing = mgr.get_by_id(code_id)
            if existing:
                mgr.update_code(code_id, name=name, definition=definition,
                                include=include, exclude=exclude, source=source)
                st.success(f"Updated {code_id}")
            else:
                new_code = Code(
                    id=code_id, category=CodeCategory(category),
                    name=name, definition=definition,
                    include=include, exclude=exclude, source=source
                )
                mgr.add_code(new_code)
                st.success(f"Added {code_id}")
            save_codebook(mgr.to_dict(), st.session_state.session_id)
            st.rerun()


def _render_emergent_tab(mgr: CodebookManager):
    import streamlit as st
    from utils.db_helpers import save_codebook

    st.subheader("🔬 Emergent Theme Management")
    st.write("Create new inductive codes discovered during analysis.")

    with st.form("emergent_form"):
        name = st.text_input("Theme Name")
        definition = st.text_area("Definition")
        examples = st.text_area("Example Excerpts")

        if st.form_submit_button("Create Emergent Code"):
            if name and definition:
                new_code = mgr.create_emergent_code(name, definition, examples)
                save_codebook(mgr.to_dict(), st.session_state.session_id)
                st.success(f"Created {new_code.id}: {new_code.name}")
                st.rerun()

    # Review queue placeholder
    st.divider()
    st.subheader("Flagged Candidates")
    if 'emergent_queue' in st.session_state and st.session_state.emergent_queue:
        for i, candidate in enumerate(st.session_state.emergent_queue):
            with st.expander(f"Candidate: {candidate['suggested_theme']}"):
                st.write(f"**Text:** {candidate['text'][:200]}...")
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Approve", key=f"approve_{i}"):
                        # Promote to emergent code
                        mgr.create_emergent_code(
                            candidate['suggested_theme'],
                            "User-approved emergent theme",
                            candidate['text'][:200]
                        )
                        st.session_state.emergent_queue.pop(i)
                        save_codebook(mgr.to_dict(), st.session_state.session_id)
                        st.rerun()
                with col2:
                    if st.button("Reject", key=f"reject_{i}"):
                        st.session_state.emergent_queue.pop(i)
                        st.rerun()
    else:
        st.caption("No flagged candidates. The computational model will flag text that doesn't fit existing codes.")


def _render_export_tab(mgr: CodebookManager):
    import streamlit as st
    import pandas as pd

    st.subheader("Export Options")

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("📄 CSV"):
            df = pd.DataFrame(mgr.to_csv_rows())
            st.download_button("Download CSV", df.to_csv(index=False), "codebook.csv")

    with col2:
        if st.button("📋 JSON"):
            st.download_button("Download JSON", mgr.to_json(), "codebook.json")

    with col3:
        if st.button("🤖 Export Prompt"):
            prompt = mgr.to_llm_prompt()
            st.download_button("Download Prompt", prompt, "computational_prompt.txt")
            with st.expander("Preview"):
                st.code(prompt[:1000] + "...")

    st.divider()
    st.subheader("Statistics")
    stats = mgr.get_statistics()
    st.json(stats)


# =============================================================================
# CLI / Direct Execution
# =============================================================================

if __name__ == "__main__":
    # Generate LLM prompt for Ollama integration
    mgr = CodebookManager()
    print(mgr.to_llm_prompt()) # compare with the current codebook

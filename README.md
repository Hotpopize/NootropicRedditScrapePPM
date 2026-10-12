# NootropicRedditScrapePPM

**Why people move from caffeine and stimulants to nootropics, read from six Reddit communities with a local-LLM coding pipeline.**

MSc Management thesis, Modul University Vienna, 2026 (graded 91/100). Supervisor: Dr. Lyndon Nixon.
Author: Vladislav Dolgov.

![Perceptual map of six nootropics communities](outputs/chapter4/submissions_only/perceptual_map.png)

---

## In one paragraph

The thesis applies the Push-Pull-Mooring (PPM) migration framework to consumer switching in the nootropics market. Push factors are bad experiences with the incumbent (caffeine crashes, stimulant side effects), pull factors are the draw of the alternative (focus, clarity, "natural"), and mooring factors are what eases or blocks the move (dosing know-how, safety worries, cost). Reddit data from 2020 to 2025 was extracted under Reddit's Research Data Addendum, coded locally by an LLM against a PPM codebook, and aggregated into a perceptual map that places each community by its push-pull balance and its mooring prevalence. Three community archetypes come out of the map.

## Results

### Certified corpus

| Stage | Units |
|---|---|
| Certified submissions | 609 |
| Certified comments | 49,435 |
| **Certified corpus** | **50,044** |

Communities: r/Nootropics, r/Supplements, r/Decaf, r/Biohackers, r/NooTopics, r/StackAdvice.

### Perceptual map (submissions stratum, N = 609)

X = push-pull balance, (PULL - PUSH) / (PULL + PUSH). Y = mooring prevalence, share of submissions with any mooring code present.

| Community | N | Push-pull balance | Mooring prevalence (%) | Archetype |
|---|---|---|---|---|
| r/NooTopics | 87 | +0.758 | 29.9 | Facilitator-Embedded Wellness Seeker |
| r/Supplements | 105 | +0.450 | 26.7 | Facilitator-Embedded Wellness Seeker |
| r/StackAdvice | 100 | +0.469 | 23.0 | Self-Directed Natural Optimiser |
| r/Biohackers | 105 | +0.415 | 17.1 | Self-Directed Natural Optimiser |
| r/Nootropics | 105 | +0.381 | 21.9 | Self-Directed Natural Optimiser |
| r/Decaf | 107 | +0.091 | 29.9 | Caffeine/Stimulant Escapee |

What the map shows:

- **All six communities read net-pull in the coded submissions.** Discussion is driven more by what the alternative offers than by complaints about the incumbent.
- **r/Decaf sits closest to equilibrium.** Push talk (40 codes) nearly matches pull talk (48), which fits a community organised around leaving caffeine rather than choosing a product.
- **Mooring separates the rest.** r/NooTopics and r/Supplements carry the most facilitating and inhibiting talk (how to dose, what is safe, where to buy). r/Biohackers carries the least, reading as more self-directed.

### Reliability caveat

Agreement between coders was slight: Cohen's kappa of 0.115 (Llama vs researcher), 0.151 (Gemma vs researcher) and 0.047 (Llama vs Gemma). The map and the archetypes are therefore an interpretive reading of community-level discourse, not a measurement of individual users. All claims are at community level.

## Method

```
BigQuery extraction ──► SQLite import ──► LLM coding ──► aggregation ──► perceptual map
 (Reddit Research       (bqah_import.py)   (bqah_coder.py,   (module3_       (perceptual_map.py)
  Data Addendum)                            Llama 3.1 via     aggregation.py)
                                            Ollama)
                                   └──► reliability check (calculate_irr_kappa.py, Gemma 3 12B second rater)
```

1. **Collection.** Six subreddits, 1 Jan 2020 to 31 Dec 2025, extracted in BigQuery under the Reddit Research Data Addendum. Quality gates on score and body length; a keyword layer for switching, push, pull and mooring terms plus a keyword-free layer; tertile-stratified sampling to 660 posts with their comment threads. The full coded set was then analysed as a census.
2. **Coding.** Every unit coded locally by Llama 3.1 (Ollama, 16k context, six parallel workers) against a PPM codebook. No data leaves the machine.
3. **Reliability.** A researcher-coded subsample, double-coded by Gemma 3 12B, scored with Cohen's kappa.
4. **Aggregation and freeze.** Counts per community frozen to `outputs/frozen_*.json`; every figure in the thesis traces back to these files.
5. **Map.** Coordinates computed from the frozen counts and rendered with matplotlib.

Technical extraction parameters: [`docs/bqah_extraction_reference.md`](docs/bqah_extraction_reference.md). Architecture: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Repository layout

| Path | What it is |
|---|---|
| `scripts/bqah_import.py` | Imports the BigQuery CSV exports into SQLite |
| `scripts/bqah_coder.py` | Partitioned LLM coding run with test protocols and circuit breakers |
| `scripts/calculate_irr_kappa.py` | Inter-rater reliability |
| `scripts/export_coded_data.py` | Exports coded data for analysis |
| `modules/codebook.py` | PPM codebook |
| `modules/module3_aggregation.py` | Community-level aggregation |
| `perceptual_map.py` | Renders the perceptual map |
| `outputs/frozen_*.json` | Frozen figures behind every number above |
| `outputs/chapter4/` | Rendered maps (submissions and full corpus) |
| `samples/` | Fully synthetic demo data |
| `app.py` | Streamlit research dashboard |

## Run it on synthetic data

The research data cannot be redistributed, so the repo ships a generator for a fully synthetic dataset.

```bash
pip install -e .                          # Python 3.11+
python scripts/verify_install.py          # environment and Ollama health check
python scripts/generate_mock_ppm_data.py  # load synthetic data
streamlit run app.py                      # open the dashboard
```

Local LLM coding needs Ollama; see [`SETUP.md`](SETUP.md).

## Data and ethics

Public code, private data. No Reddit content or user data is in this repository. Data was accessed under the Reddit Research Data Addendum (executed 2026-04-01); author fields are kept out of the analysis pipeline. Clause-by-clause mapping of the agreement to the code: [`COMPLIANCE.md`](COMPLIANCE.md).

## Citation

```bibtex
@software{dolgov2026caffeine,
  author      = {Dolgov, Vladislav},
  title       = {Caffeine to Brain Boosts: A Qualitative Research Instrument for Nootropic Discourse Analysis},
  institution = {Modul University Vienna},
  year        = {2026},
  url         = {https://github.com/Hotpopize/NootropicRedditScrapePPM}
}
```

MIT licence. Contact: [LinkedIn / email here]

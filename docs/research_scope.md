# Project Research Scope

**Instrument:** NootropicRedditScrapePPM  
**Academic Context:** MSc Management / MBA Thesis, Modul University Vienna  
**Status:** Operational (Authenticated PRAW Collection)

---

## 1. Research Question
The project investigates aggregate consumer discourse regarding cognitive supplement usage patterns. Specifically:
> *How do consumers talk about switching between caffeine, prescription stimulants, and natural nootropics?*

The research is framed within the **Push-Pull-Mooring (PPM)** migration framework, treating the transition between supplement categories as a "migration" process driven by specific stressors and attractors.

## 2. Target Communities (Subreddits)
Data collection is restricted to a fixed set of topic-relevant subreddits where consumer discourse on cognitive enhancement is primary:
- `r/Nootropics`
- `r/StackAdvice`
- `r/Supplements`
- `r/Decaf`
- `r/Biohackers` (Jul. 2013 | 643k | 17k)
- `r/NooTopics`

**Note on r/Biohackers:** Integrated PULL + MOOR-F — supplements embedded within broader lifestyle systems; most sophisticated consumer profile. The Systems-Thinking Self-Optimiser. Situates nootropic use within DIY biology, longevity, sleep, and wearables discourse. Distinguishes standalone-supplement from systems-level orientation. Anchors P2 and P4.

## 3. Keywords & Thematic Filters
The collection and subsequent LLM coding pipeline focus on the following core entities:
- **Push Factors (Dissatisfaction with current state):** `anxiety`, `crash`, `jitters`, `tolerance`, `side effects`, `withdrawal`.
- **Pull Factors (Attraction to alternatives):** `focus`, `baseline`, `natural`, `longevity`, `sustainable`, `clear head`.
- **Product Categories:** `caffeine`, `adderall`, `ritalin`, `modafinil`, `vyvanse`, `l-theanine`, `bacopa`, `lion's mane`.

## 4. Volume & Velocity Parameters
To ensure academic focus and respect platform limits, the following constraints are operationalized in `core/schemas.py`:
- **Target Corpus Size:** ~150–200 high-quality submissions and associated comment threads.
- **Collection Depth:** Limited to the top 50 posts per subreddit per run.
- **Rate Limiting:** Managed via a token-bucket `RateLimiter` in `services/reddit_service.py`, set to a conservative 50 requests per minute.

## 5. Exclusions
- **Personal Health Information:** Individual diagnostic or clinical treatment claims are excluded from analysis.
- **Commercial Content:** Affiliate links or promotional posts are filtered out during the coding phase.
- **Non-English Content:** Detected and flagged automatically for exclusion.

---
*For technical implementation details, see [ARCHITECTURE.md](ARCHITECTURE.md). For compliance mapping, see [COMPLIANCE.md](../COMPLIANCE.md).*

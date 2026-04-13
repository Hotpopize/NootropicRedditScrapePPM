#!/usr/bin/env python3
"""
scripts/generate_mock_ppm_data.py
=================================
Generates an entirely fabricated synthetic sample dataset for end-to-end
testing of the NootropicRedditScrapePPM analysis pipeline.

Why this exists
---------------
The tool's analysis pipeline (Push-Pull-Mooring coding, topic modeling,
dashboard, codebook management) must be testable *without* any live Reddit
access and *without* any real user content. This script produces a committed
sample CSV that any developer — including reviewers who do not have Reddit
API credentials — can load via `scripts/import_external_data.py` to exercise
the full pipeline on a self-contained, clearly-fabricated dataset.

Compliance posture
------------------
Every post in this file is written by the thesis author. No text is scraped,
paraphrased, or derived from any real Reddit post, user, comment, or
community. All usernames are synthetic (`synth_user_NN`). All permalinks
point at the reserved `.example` TLD (RFC 2606) which cannot resolve to any
real resource. Every row in the output CSV carries a `SYNTHETIC=TRUE` flag
in the first column. This file contains NO real Reddit user content of any
kind and is safe to commit to a public repository under the Reddit4Researcher
Agreement, which prohibits redistribution of real Reddit data but places no
restriction on synthetic test fixtures written by the researcher.

Design notes
------------
  - Posts are hand-written to deliberately exercise the Push-Pull-Mooring
    (PPM) migration framework from Moon (1995), adapted for consumer-product
    switching contexts. Each post is tagged with a `ppm_hint` column that
    acts as a weak ground-truth label for evaluating the LLM coder.
  - Distribution: ~6 push, ~9 pull, ~10 mooring, ~3 mixed. Slight overweight
    on mooring reflects the PPM literature's finding that switching barriers
    dominate consumer narratives around habitual products.
  - IDs are deterministic (`synth_001` .. `synth_028`) so the generated file
    is reproducible and diffable in git.
  - Timestamps are spread across 2023-01-01 → 2024-12-31 using deterministic
    offsets so re-running the script produces byte-identical output.
  - Output is a flat CSV (not nested JSON) so a reviewer can open it in a
    spreadsheet and audit every row in under a minute.

Usage
-----
  python scripts/generate_mock_ppm_data.py                 # default output path
  python scripts/generate_mock_ppm_data.py --out PATH.csv  # custom output path

Consumers
---------
  scripts/import_external_data.py — ingests this CSV into the local research
    database for analysis pipeline testing.
  docs/quickstart.md — references this file in the "Quickstart with sample
    data" walkthrough for developers and reviewers without Reddit credentials.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Synthetic post corpus — hand-written by the thesis author.
# No text below is scraped, paraphrased, or derived from any real source.
# ---------------------------------------------------------------------------

SYNTHETIC_POSTS: list[dict] = [
    # ---- PUSH: dissatisfaction with current/synthetic/prescription options ----
    {
        "title": "Finally quit Adderall after 4 years. Burnout is real.",
        "text": (
            "Been on 30mg XR since undergrad and honestly it stopped working somewhere around "
            "year 2 but I kept pushing the dose up. Last semester I hit a wall — constant chest "
            "tightness, couldn't sleep even on weekends, and my emotions just flattened out to "
            "nothing. My GP wanted to add a second med to 'manage the side effects' which felt "
            "insane. Tapered off over 6 weeks and I feel like a person again, but my focus is "
            "genuinely worse than before I ever started. Trying to figure out what to do now "
            "because I still have finals coming up. Anyone been through this and come out the "
            "other side?"
        ),
        "subreddit": "Nootropics",
        "ppm_hint": "push",
    },
    {
        "title": "3 cups of coffee used to feel normal. Now 1 wrecks me.",
        "text": (
            "Something changed in my 30s. I used to slam espresso all day in my 20s with zero "
            "issues, and now even a single flat white puts my heart rate up in the 90s and I "
            "get this weird doom feeling for hours. I love coffee — it's my morning ritual, "
            "it's my social thing with coworkers, it's how I start work — but my body is "
            "clearly telling me something. I don't want to quit entirely, but the cost-benefit "
            "has shifted and I hate that. Anyone figured out a way to keep the ritual without "
            "the physical fallout?"
        ),
        "subreddit": "Nootropics",
        "ppm_hint": "push",
    },
    {
        "title": "Modafinil stopped working and I'm scared of going higher",
        "text": (
            "I started at 100mg and it felt like a superpower. Focus, mood, motivation, "
            "everything locked in. Six months later I'm at 200mg and it feels like nothing, "
            "maybe a slight lift. I know the obvious answer is to take breaks but my work "
            "schedule doesn't allow that — I have deadlines every week and I can't afford to "
            "be a fog for three days of tapering. Reading the long-term studies is not "
            "reassuring either. Feels like I stumbled into a dependency I didn't sign up for."
        ),
        "subreddit": "StackAdvice",
        "ppm_hint": "push",
    },
    {
        "title": "Doctors here treat ADHD meds like I'm asking for heroin",
        "text": (
            "I moved to a new city and spent 4 months trying to get my prescription "
            "transferred. Three different psychiatrists, each wanted a full re-evaluation, "
            "each charged out of pocket because my insurance changed. One literally asked if "
            "I'd 'tried meditation.' I have a diagnosis from 2019 and a paper trail longer "
            "than my arm. I understand why the guardrails exist but the system is punishing "
            "the people who actually need it. Seriously considering just going full natural "
            "stack because at least I can buy lion's mane without begging."
        ),
        "subreddit": "Nootropics",
        "ppm_hint": "push",
    },
    {
        "title": "Energy drinks stole my sleep for 5 years and I didn't notice",
        "text": (
            "Drank 2-3 energy drinks a day from age 22 to 27. Thought I was just 'a bad "
            "sleeper.' Cut them out two months ago after a physical showed resting HR of 92 "
            "and I genuinely cannot believe how much better I feel. Sleeping 7 solid hours, "
            "dreaming again, not waking up at 4am anxious. The crazy part is I had convinced "
            "myself they helped me focus, but turns out sleeping properly is the actual "
            "productivity hack. Now looking for something gentler for the afternoon dip "
            "because coffee alone isn't cutting it."
        ),
        "subreddit": "decidingtobebetter",
        "ppm_hint": "push",
    },
    {
        "title": "The 2pm coffee crash is ruining my work",
        "text": (
            "Every single day. Morning coffee is great, 10am I'm crushing it, then between "
            "1:30 and 2:30 I turn into a zombie and can barely read a sentence. More coffee "
            "just delays the crash and wrecks my sleep. Tried skipping lunch, eating lunch, "
            "protein-heavy, carb-heavy, nothing moves the needle. It's the caffeine cycle, I "
            "know it is, but I don't know how to get off it without losing the morning focus "
            "I actually need. Feels like I'm stuck in a trap I built myself."
        ),
        "subreddit": "productivity",
        "ppm_hint": "push",
    },

    # ---- PULL: attraction to natural nootropic alternatives ----
    {
        "title": "Lion's mane actually works but it took way longer than I expected",
        "text": (
            "Read all the hype, tried it for 4 weeks, felt nothing, gave up. Picked it up "
            "again six months later out of curiosity and stuck with it for 10 weeks this "
            "time. Somewhere around week 7 I noticed I was finishing writing tasks without "
            "the usual twenty tab-switching breaks. Not euphoric, not stimulating, just… my "
            "brain stays on track longer. I think the first time I quit too early. Dose is "
            "1g dual-extract morning and evening. Not a miracle but it's the first thing "
            "I've taken that feels like it's doing something sustainable rather than borrowed."
        ),
        "subreddit": "Nootropics",
        "ppm_hint": "pull",
    },
    {
        "title": "Rhodiola is the only thing that touched my work stress",
        "text": (
            "I'm not someone who believes in supplements generally. I'm an engineer, I want "
            "data. But after six months of grinding on a brutal project my partner basically "
            "forced me to try rhodiola. Took 200mg mornings for three weeks. The thing I "
            "noticed wasn't that I felt 'energized' — it's that the small stuff stopped "
            "derailing me. Slack pings, surprise meetings, a bug in prod — I could just… "
            "handle them. Still stressful work but the edge was off. Caveat: I also started "
            "walking at lunch so maybe that's half of it. But I'm keeping the rhodiola."
        ),
        "subreddit": "Nootropics",
        "ppm_hint": "pull",
    },
    {
        "title": "Bacopa: subtle, slow, but the most real thing I've tried",
        "text": (
            "Three months in on 300mg standardized bacopa with breakfast. First month was "
            "nothing. Second month I noticed I was remembering names better — which sounds "
            "small but I'm terrible with names and always have been. Third month my partner "
            "commented I was 'less scatterbrained.' I don't feel anything taking it, there's "
            "no acute effect at all, but the baseline has moved. I think this is what people "
            "mean when they say nootropics are boring — the good ones don't feel like "
            "anything, they just quietly make you a bit better over months. Patience required."
        ),
        "subreddit": "StackAdvice",
        "ppm_hint": "pull",
    },
    {
        "title": "Ashwagandha fixed my sleep and my focus came back as a side effect",
        "text": (
            "I started ashwagandha for anxiety, not for focus. KSM-66, 600mg before bed. "
            "Within two weeks I was sleeping through the night for the first time in maybe "
            "five years. What I didn't expect is what happened to my work performance — "
            "turns out when you sleep properly, you don't need half the stimulants you "
            "thought you did. My coffee intake dropped from 4 cups to 1.5 without me trying. "
            "Moral of the story: sometimes the 'cognitive enhancer' you need is just actual "
            "sleep."
        ),
        "subreddit": "Supplements",
        "ppm_hint": "pull",
    },
    {
        "title": "Matcha + L-theanine is the coffee replacement I didn't know I needed",
        "text": (
            "Made the switch three months ago. Ceremonial grade matcha in the morning, 200mg "
            "L-theanine on the side. The caffeine hit is lower and slower, but there's zero "
            "jitters, zero crash, and I feel calm-alert instead of wired-anxious. It's more "
            "expensive per cup than coffee, and it takes 3 extra minutes to prep which I've "
            "come to actually enjoy as a ritual. Not for everyone — if you love the hit of "
            "espresso this will feel weak. But for anyone whose coffee stopped feeling good, "
            "worth trying."
        ),
        "subreddit": "Nootropics",
        "ppm_hint": "pull",
    },
    {
        "title": "Creatine for the brain, not just the gym",
        "text": (
            "I've taken creatine for lifting since I was 19. Recently read some newer studies "
            "suggesting cognitive benefits especially under sleep deprivation and thought, "
            "wait, I already take this, why haven't I tested it that way? Kept the 5g daily "
            "but started paying attention on bad sleep days. Noticeable difference in how "
            "foggy I am after a short night — still tired, but I can still think. It's "
            "stupidly cheap, extremely well-studied, and if you already lift you're leaving "
            "cognition gains on the table by not paying attention."
        ),
        "subreddit": "Nootropics",
        "ppm_hint": "pull",
    },
    {
        "title": "Omega-3 quietly changed how I think",
        "text": (
            "Started taking 2g EPA/DHA per day about 8 months ago because my doctor flagged "
            "my blood work. Wasn't expecting anything cognitive. Around month 3 my partner "
            "asked if I'd changed my meds because I 'seemed more present.' I hadn't. I think "
            "the sustained intake finally did something to my baseline — I find meetings "
            "less draining, I can follow long conversations without zoning out, and my "
            "afternoon mood is dramatically more stable. It's the least sexy supplement in "
            "the world and possibly the most underrated."
        ),
        "subreddit": "Supplements",
        "ppm_hint": "pull",
    },
    {
        "title": "Mushroom coffee blend — surprised I'm sticking with it",
        "text": (
            "Tried one of those lion's mane plus chaga plus coffee blends as a joke after "
            "seeing an ad. Thought it'd taste awful. It's actually fine, tastes like slightly "
            "earthy coffee. Three weeks in and I'm not going back to straight coffee — I get "
            "a gentler lift, no afternoon crash, and I drink less of it because I don't feel "
            "the need to chain cups. I don't know if it's the mushrooms or just that the "
            "blend is lower caffeine, but whatever it is, it works for me."
        ),
        "subreddit": "Nootropics",
        "ppm_hint": "pull",
    },
    {
        "title": "Ginkgo for the afternoon fog — worth another look",
        "text": (
            "Everyone treats ginkgo like it's a 1990s joke but I circled back to it last "
            "month and I'm impressed. 120mg standardized extract at lunch and the 2-4pm fog "
            "that's been wrecking my afternoons for years is noticeably lighter. Not "
            "coffee-level alertness, just the absence of that heavy-headed feeling. The "
            "research is mixed, I know, but at a few cents per day I don't need it to be a "
            "miracle. Would be curious if anyone else has given it a second chance recently."
        ),
        "subreddit": "StackAdvice",
        "ppm_hint": "pull",
    },

    # ---- MOORING: switching costs, habits, social norms, uncertainty ----
    {
        "title": "I know coffee is hurting me but I can't give up the ritual",
        "text": (
            "My whole morning is built around it. Grind the beans, pull the shot, sit at the "
            "kitchen window for 10 minutes before the kids wake up. It's not about the "
            "caffeine really, it's the ritual. I've tried replacing it with tea, with "
            "nothing, with mushroom coffee — all of it feels like losing something I actually "
            "care about. Willing to switch in theory but in practice every morning I'm back "
            "at the espresso machine. Is anyone else in this weirdly emotional situation "
            "with their caffeine?"
        ),
        "subreddit": "decidingtobebetter",
        "ppm_hint": "mooring",
    },
    {
        "title": "The 'cheap' supplement stack isn't actually cheap",
        "text": (
            "Sat down and added up what a 'basic' stack from this sub costs per month: "
            "lion's mane, bacopa, rhodiola, omega-3, magnesium, b-complex, creatine. Even "
            "buying the cheapest reputable brands I could find, I was looking at nearly "
            "90 euro per month. That's a lot for 'might help with focus.' Meanwhile my 4 "
            "euro bag of coffee lasts two weeks and definitely works. I want to try this "
            "stuff but I can't justify the monthly cost against uncertain benefit, "
            "especially as a grad student."
        ),
        "subreddit": "Nootropics",
        "ppm_hint": "mooring",
    },
    {
        "title": "How do you actually trust a supplement brand in Europe?",
        "text": (
            "Spent two hours trying to pick a lion's mane extract and gave up. Every brand "
            "claims 30% beta-glucans, every brand says 'dual extract,' every brand has "
            "glowing reviews that might be fake. The EU doesn't regulate these like "
            "medicines so there's no real check on the label claims. Third-party testing "
            "certificates are either missing or from labs I've never heard of. I want to "
            "try nootropics but I can't even get past picking the first product. How does "
            "anyone navigate this without getting scammed?"
        ),
        "subreddit": "Supplements",
        "ppm_hint": "mooring",
    },
    {
        "title": "Bringing herbal tea to a coffee-culture office feels weird",
        "text": (
            "My workplace runs on espresso. The machine is basically the heart of the "
            "office — people gather around it, deals happen there, it's where you bond with "
            "new hires. I decided to cut back for health reasons and started bringing my "
            "own herbal tea. I'm not kidding, it's affected my relationships at work. "
            "People make jokes, I get less face time with my manager who's a coffee guy, "
            "and I feel like a weirdo. Did not expect 'give up coffee' to have a social "
            "cost this high."
        ),
        "subreddit": "decidingtobebetter",
        "ppm_hint": "mooring",
    },
    {
        "title": "Every source gives a different dose. How do you even pick?",
        "text": (
            "Want to try rhodiola. One reference site says 288-680mg. This subreddit says "
            "start at 100mg. The bottle says 500mg. A YouTube video says 'cycle 5 days on 2 "
            "days off.' A paper I found used 576mg. I'm a careful person and I like to "
            "understand what I'm putting in my body, and this level of variance for "
            "something that's supposed to be well-studied is making me not trust any of it. "
            "Is there actually a right answer or is everyone just guessing?"
        ),
        "subreddit": "StackAdvice",
        "ppm_hint": "mooring",
    },
    {
        "title": "Is it working or am I just believing it's working?",
        "text": (
            "Two months on a simple stack (bacopa, lion's mane, fish oil). I THINK I feel "
            "better. More focused, steadier mood, less afternoon slump. But I don't know if "
            "that's the supplements, or the placebo effect from wanting them to work, or "
            "the fact that I also started going to bed earlier, or just that summer is "
            "over. I can't run a blinded trial on myself. How do people here convince "
            "themselves the effects are real? Or do you just accept the uncertainty?"
        ),
        "subreddit": "Nootropics",
        "ppm_hint": "mooring",
    },
    {
        "title": "My GP laughed when I asked about nootropics",
        "text": (
            "Brought up lion's mane and bacopa at my annual physical, asked if there was "
            "anything I should watch out for with my current meds. My doctor literally "
            "chuckled and said 'there's no evidence for any of that, save your money.' I'm "
            "not naive, I know the evidence base is thin, but the total dismissal made me "
            "feel stupid for asking and now I'm second-guessing even trying. How much "
            "weight should I give to a doctor who clearly hasn't read any of the newer "
            "studies versus people here who've actually used this stuff?"
        ),
        "subreddit": "Nootropics",
        "ppm_hint": "mooring",
    },
    {
        "title": "Using nootropics for finals feels like cheating and I don't know why",
        "text": (
            "It's legal, it's over the counter, it's not that different from coffee — and "
            "yet every time I take my morning stack during exam period I feel a weird guilt "
            "about it. Like I'm 'gaming' the exam instead of earning the grade. My roommate "
            "takes prescribed Adderall and doesn't feel this way. I think it's because I "
            "know the playing field isn't even — not every student can afford this stack. "
            "Curious if anyone else has worked through this or if I'm overthinking it."
        ),
        "subreddit": "Nootropics",
        "ppm_hint": "mooring",
    },
    {
        "title": "Terrified to cut caffeine because of the withdrawal stories",
        "text": (
            "Everything I read says the first 3-5 days are brutal — migraines, brain fog, "
            "mood crashes. I've got kids, a job, deadlines. I literally cannot afford to be "
            "non-functional for a week. I know intellectually it would pass and I'd feel "
            "better on the other side, but the short-term cost is too high for me to "
            "actually commit. So I'm stuck in a loop where I know caffeine isn't great for "
            "me anymore but the off-ramp is worse than the status quo. Has anyone tapered "
            "super slowly and avoided the worst of it?"
        ),
        "subreddit": "decidingtobebetter",
        "ppm_hint": "mooring",
    },
    {
        "title": "Tried adaptogens once, felt nothing, can't bring myself to try again",
        "text": (
            "Two years ago I did a full 8-week trial of ashwagandha, rhodiola, and "
            "L-theanine based on a popular stack guide. Felt absolutely nothing the entire "
            "time. 60 euro in the bin. Now whenever someone here posts about how 'it took "
            "3 months to notice,' I just think — or maybe it doesn't work and you're in a "
            "sunk cost fallacy. I'm skeptical by nature and my one experiment made that "
            "worse. Curious if there's anything that would actually change my mind or if I "
            "should just accept these aren't for me."
        ),
        "subreddit": "Supplements",
        "ppm_hint": "mooring",
    },

    # ---- MIXED: posts that deliberately span two or more PPM categories ----
    {
        "title": "Six months post-Adderall on a natural stack. Honest update.",
        "text": (
            "Came off Adderall last October after 3 years because the comedowns were "
            "destroying my evenings with my family. Replaced it with: lion's mane 1g, "
            "rhodiola 200mg, omega-3 2g, and an embarrassing amount of green tea. Honest "
            "assessment — my peak focus is lower. On Adderall I had 4-hour laser sessions, "
            "now my best is maybe 90 minutes. But my baseline is higher. I'm present with "
            "my kids, I sleep properly, my weekends don't feel like recovery. If your job "
            "demands peak output you might regret this switch. If your life demands "
            "sustainability, it's been worth it for me."
        ),
        "subreddit": "Nootropics",
        "ppm_hint": "mixed",
    },
    {
        "title": "Want to try nootropics but I don't even know where to start",
        "text": (
            "I'm interested. I've read the wiki, I've read threads, and honestly I just "
            "feel more confused than when I started. Which brand? Which dose? Extract or "
            "whole powder? Cycled or daily? With food or without? And how do I even tell "
            "if it's working? I don't want to spend 80 euro on a stack and waste it because "
            "I didn't know what I was doing. Part of me thinks I should just start with "
            "the simplest possible thing — maybe just lion's mane alone — but even picking "
            "a single product feels overwhelming. Anyone have advice for a cautious "
            "beginner?"
        ),
        "subreddit": "Nootropics",
        "ppm_hint": "mixed",
    },
    {
        "title": "One year in: what I've learned moving from coffee and Adderall to a natural stack",
        "text": (
            "Quick summary of my journey for anyone considering this. Started because the "
            "Adderall crashes and coffee jitters were wrecking my evenings and sleep. Got "
            "curious about lion's mane and rhodiola after reading about long-term use "
            "versus stimulant tolerance. Hardest part wasn't the switch itself — it was "
            "everything around it: figuring out trustworthy brands in the EU market, "
            "affording the stack on a grad student budget, explaining my new morning "
            "routine to a coffee-obsessed team, and doubting myself every time I didn't "
            "feel an acute effect. A year later I'm mostly on the natural side, still have "
            "one coffee a day as ritual, and my life is dramatically calmer. Not saying "
            "this path is for everyone — it required a lot of research, a budget I didn't "
            "expect, and a willingness to feel subtler effects instead of the obvious hit "
            "of a stimulant. But I don't regret it."
        ),
        "subreddit": "Nootropics",
        "ppm_hint": "mixed",
    },
    # ---- NOOTOPICS: Science-heavy / Specific compounds ----
    {
        "title": "The shift from generic stacks to high-affinity ligands — my experience with NooTopics",
        "text": (
             "I spent three years on the standard r/Nootropics 'beginner stack' and never felt "
             "much besides a slight reduction in anxiety from Ashwagandha. Moving over to "
             "r/NooTopics was a wake-up call regarding the actual science of receptor affinity. "
             "The move to more targeted compounds has been a game-changer for my cognitive "
             "endurance. It's not about 'feeling' a buzz, it's about the measurable reduction "
             "in mental fatigue during 10-hour coding sessions. The barrier for entry is "
             "definitely higher — you have to actually read the papers — but the pull of "
             "legitimate, non-placebo cognitive enhancement is hard to ignore once you see "
             "the data."
        ),
        "subreddit": "NooTopics",
        "ppm_hint": "pull",
    },
    {
        "title": "Is the complexity of advanced nootropics a 'mooring' factor?",
        "text": (
            "I love the deep dives on r/NooTopics, but sometimes I wonder if the sheer "
            "technical density of the discussions acts as a barrier for people wanting to "
            "switch from caffeine. I've been researching bromantane and TAK-653 for months, "
            "and I still feel like I'm barely scratching the surface of the pharmacology. "
            "The cost of 'learning' the stack is much higher than just grabbing a pre-made "
            "Alpha Brain or whatever. For me, the fascination with the brain's mechanics is "
            "part of the pull, but for most people, the learning curve is probably a massive "
            "mooring factor that keeps them stuck on traditional stimulants."
        ),
        "subreddit": "NooTopics",
        "ppm_hint": "mixed",
    },
]


# ---------------------------------------------------------------------------
# CSV schema — flat columns for maximum auditability.
# A reviewer should be able to open this file in any spreadsheet tool and
# verify every field of every row without needing a JSON parser.
# ---------------------------------------------------------------------------

CSV_FIELDNAMES: list[str] = [
    "SYNTHETIC",         # Always "TRUE" — the headline flag column
    "id",                # Deterministic synthetic id: synth_001..synth_028
    "type",              # Always "submission"
    "subreddit",         # Target subreddit name (no r/ prefix)
    "title",             # Post title
    "text",              # Post body (selftext)
    "author",            # Synthetic author: synth_user_01..synth_user_28
    "score",             # Fabricated upvote count
    "num_comments",      # Fabricated comment count
    "created_utc",       # Unix timestamp — deterministic
    "created_date_iso",  # Human-readable ISO date — for spreadsheet audit
    "url",               # Points at .example (RFC 2606 reserved TLD)
    "permalink",         # Same — cannot resolve to real Reddit content
    "data_source",       # Always "synthetic_sample"
    "content_type",      # Always "text"
    "language_flag",     # Always "english"
    "word_count",        # Computed from text
    "ppm_hint",          # Ground-truth label: push | pull | mooring | mixed
    "collected_at",      # ISO generation timestamp (script run time)
]


def build_record(idx: int, post: dict, base_time: datetime, run_time: str) -> dict:
    """
    Convert a bare post dict into a full CSV record matching CSV_FIELDNAMES.

    Deterministic fields (id, author, timestamps, permalinks) are derived from
    `idx` so re-running the script produces byte-identical output. The only
    nondeterministic field is `collected_at`, which captures the generation
    time for provenance — downstream tests that need byte-identical output
    should freeze or ignore this column.
    """
    synth_id = f"synth_{idx:03d}"
    synth_author = f"synth_user_{idx:02d}"

    # Spread 28 posts across 2023-01-01 -> 2024-12-31 with ~25-day offsets.
    # This gives a believable temporal distribution for time-series testing
    # without requiring any real dates.
    created_dt = base_time + timedelta(days=(idx - 1) * 25)
    created_utc = created_dt.timestamp()
    created_date_iso = created_dt.strftime("%Y-%m-%d")

    # RFC 2606 reserves `.example` as a non-resolvable TLD. Using it here
    # guarantees these URLs can never accidentally point at real content,
    # even if a downstream tool tries to fetch them.
    subreddit = post["subreddit"]
    permalink = f"https://reddit.example/r/{subreddit}/comments/{synth_id}/"
    url = permalink

    word_count = len(post["text"].split())

    return {
        "SYNTHETIC":        "TRUE",
        "id":               synth_id,
        "type":             "submission",
        "subreddit":        subreddit,
        "title":            post["title"],
        "text":             post["text"],
        "author":           synth_author,
        "score":            _deterministic_score(idx),
        "num_comments":     _deterministic_comment_count(idx),
        "created_utc":      f"{created_utc:.1f}",
        "created_date_iso": created_date_iso,
        "url":              url,
        "permalink":        permalink,
        "data_source":      "synthetic_sample",
        "content_type":     "text",
        "language_flag":    "english",
        "word_count":       word_count,
        "ppm_hint":         post["ppm_hint"],
        "collected_at":     run_time,
    }


def _deterministic_score(idx: int) -> int:
    """
    Produce a plausible-looking but entirely fabricated upvote count.
    Uses a simple hash-free formula so the output is reproducible without
    importing `random`. Values range roughly 12..340.
    """
    return 12 + ((idx * 37) % 329)


def _deterministic_comment_count(idx: int) -> int:
    """
    Produce a plausible-looking but entirely fabricated comment count.
    Values range roughly 3..87.
    """
    return 3 + ((idx * 19) % 85)


def summarize_distribution(posts: list[dict]) -> dict[str, int]:
    """
    Count posts per PPM category for the post-generation summary log line.
    """
    counts: dict[str, int] = {}
    for p in posts:
        counts[p["ppm_hint"]] = counts.get(p["ppm_hint"], 0) + 1
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate the synthetic PPM sample CSV.",
    )
    parser.add_argument(
        "--out",
        default="samples/synthetic_nootropics_sample.csv",
        help="Output CSV path (default: %(default)s)",
    )
    args = parser.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Use timezone-aware datetime
    base_time = datetime(2023, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
    run_time = datetime.now(timezone.utc).isoformat(timespec="seconds")

    records = [
        build_record(idx, post, base_time, run_time)
        for idx, post in enumerate(SYNTHETIC_POSTS, start=1)
    ]

    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(records)

    dist = summarize_distribution(SYNTHETIC_POSTS)
    print(f"Wrote {len(records)} synthetic posts to {out_path}")
    print(f"PPM distribution: {dist}")
    print("All rows flagged SYNTHETIC=TRUE. No real Reddit content included.")


if __name__ == "__main__":
    main()

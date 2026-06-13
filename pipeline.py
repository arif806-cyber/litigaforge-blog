"""
LitigaForge AI — Autonomous Content Pipeline
=============================================
Quota-balanced topic engine → AI article → GitHub commit → IndexNow

Free stack:
  - Gemini 2.5 Flash    (free tier)
  - Groq Llama 3.3 70B  (14,400 req/day free)
  - GitHub Contents API (free)
  - IndexNow API        (free)

Runs every 2 hours (12 stateless runs/day). Each run reads the already-published
markdown to decide what is still owed for *today* (Asia/Kolkata), then publishes
at most PER_RUN_MAX fresh, deduplicated, balanced articles until the daily target
is met.

Controls (env):
  GENERATION_ENABLED   FAIL-CLOSED kill switch. Only "true"/"1"/"yes"/"on" runs;
                       anything else (incl. missing) keeps the pipeline PAUSED.
  DRY_RUN              "true"/"1"               -> generate 5 samples, write nothing
  DAILY_TARGET         default 7
  MAX_ARTICLES         per-run cap, default 2
  MIN_WORDS            article body floor, default 1500
"""

import os
import json
import base64
import asyncio
import httpx
import re
from datetime import datetime, timezone, timedelta

# ─── CONFIG FROM ENVIRONMENT ─────────────────────────────────────────────────
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GROQ_API_KEY   = os.environ.get("GROQ_API_KEY", "")
GITHUB_TOKEN   = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO    = os.environ.get("GITHUB_REPO", "arif806-cyber/litigaforge-blog")
INDEXNOW_KEY   = os.environ.get("INDEXNOW_KEY", "")
BLOG_DOMAIN    = os.environ.get("BLOG_DOMAIN", "blog.litigaforge.com")
SITE_DOMAIN    = os.environ.get("SITE_DOMAIN", "litigaforge.com")

DAILY_TARGET = int(os.environ.get("DAILY_TARGET", "7"))
PER_RUN_MAX  = int(os.environ.get("MAX_ARTICLES", "2"))
MIN_WORDS    = int(os.environ.get("MIN_WORDS", "1500"))
# A full 1700-2000+ word article serializes to ~9-12k output tokens; give plenty
# of headroom so the JSON is never truncated mid-string (which is unparseable).
ARTICLE_MAX_TOKENS = int(os.environ.get("ARTICLE_MAX_TOKENS", "24000"))

# Kill switch — FAIL CLOSED. Generation only runs when GENERATION_ENABLED is
# explicitly truthy. A missing/empty/unknown value keeps the pipeline PAUSED so
# a scheduled, manual, or VM-dispatched run can never publish unapproved content.
# Production go-live = set the repo variable GENERATION_ENABLED=true.
_gen = os.environ.get("GENERATION_ENABLED", "false").strip().lower()
GENERATION_ENABLED = _gen in ("1", "true", "yes", "on")
DRY_RUN = os.environ.get("DRY_RUN", "").strip().lower() in ("1", "true", "yes", "on")

CONTENT_DIR    = os.path.join("src", "content", "blog")
PUBLISHED_FILE = "published_slugs.json"

# India Standard Time has no DST — a fixed +5:30 offset needs no tzdata.
IST = timezone(timedelta(hours=5, minutes=30))

# ─── COUNTRY QUOTA & CATEGORY ROTATION ───────────────────────────────────────
# Daily target = 7 articles: 6 fixed + 1 rotating slot. India leads (it is the
# core market); the four established markets get one each; one of the emerging
# markets rotates in daily so all three get steady coverage over time.
COUNTRY_QUOTA = {
    "India": 2,
    "USA":   1,
    "UK":    1,
    "UAE":   1,
    "Germany": 1,
}
ROTATING_COUNTRIES = ["Australia", "Canada", "Singapore"]  # 1 slot/day, rotates

CATEGORIES = [
    "Employment Law",
    "Family Law",
    "Tenant & Property Rights",
    "Consumer Rights",
    "Criminal Law",
    "Immigration Law",
    "Business & Startup Law",
    "Tax & Finance Law",
    "Intellectual Property",
    "Civil Rights",
]

# Seed *angles* (not full titles) used only to steer the model toward a concrete,
# searchable subtopic. The model proposes the actual title and must avoid topics
# that already exist.
CATEGORY_SEEDS = {
    "Employment Law": ["unfair dismissal claim", "recovering unpaid wages", "notice period & resignation rights", "workplace harassment complaint", "redundancy / severance pay"],
    "Family Law": ["divorce grounds & process", "child custody & visitation", "spousal maintenance / alimony", "domestic violence protection order", "validity of a prenuptial agreement"],
    "Tenant & Property Rights": ["security deposit refund", "protection from illegal eviction", "rent increase rules", "landlord repair obligations", "breaking a lease early"],
    "Consumer Rights": ["refund for a defective product", "e-commerce return rights", "service deficiency complaint", "misleading advertising claim", "enforcing a product warranty"],
    "Criminal Law": ["filing a police complaint / FIR", "bail process & rights", "your rights on arrest", "reporting online fraud / cybercrime", "responding to a defamation case"],
    "Immigration Law": ["work-visa rights & employer violations", "permanent residency pathway", "consequences of a visa overstay", "family / dependent visa sponsorship", "appealing a visa refusal"],
    "Business & Startup Law": ["registering a company", "founder / shareholder agreement", "contractor vs employee classification", "tax / GST registration steps", "winding up a company"],
    "Tax & Finance Law": ["responding to a tax notice", "claiming deductions & exemptions", "GST/VAT dispute resolution", "penalties for late filing", "appealing a tax assessment"],
    "Intellectual Property": ["registering a trademark", "remedies for copyright infringement", "patent filing basics", "protecting IP when using contractors", "enforcing an NDA / trade secret"],
    "Civil Rights": ["filing a right-to-information request", "data privacy & protection rights", "anti-discrimination protections", "public grievance redressal", "enforcing fundamental rights"],
}

# Stopwords + generic legal/geo terms stripped when normalizing a topic for
# duplicate detection. Country is keyed separately, so country names are stripped.
_STOP = set("the a an and or of to in on for with your you our we how what when where why which who is are be can do does this that these those at by from as it its will may must should not into about after before your their his her i".split())
_GENERIC = set("law laws legal rights guide rules act explained complete need know 2024 2025 2026 india usa us uk uae germany australia canada singapore american british".split())


# ─── FRONT-MATTER + EXISTING-ARTICLE INDEX ───────────────────────────────────
def _read_frontmatter(path: str) -> dict:
    """Minimal front-matter reader. Values are JSON-encoded (see build_markdown),
    so we json.loads each value and fall back to a stripped string."""
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read()
    except OSError:
        return {}
    if not text.startswith("---"):
        return {}
    end = text.find("\n---", 3)
    block = text[3:end] if end != -1 else text[3:]
    data = {}
    for line in block.splitlines():
        if ":" not in line:
            continue
        key, _, val = line.partition(":")
        val = val.strip()
        try:
            data[key.strip()] = json.loads(val)
        except (ValueError, json.JSONDecodeError):
            data[key.strip()] = val.strip('"').strip("'")
    return data


def _normalize_topic(title: str) -> str:
    """Stable dedup key: lowercase, drop punctuation, stopwords, generic/geo
    words, then sort the remaining significant tokens so word order doesn't
    matter ('tenant rights india' == 'india rights tenant')."""
    words = re.findall(r"[a-z0-9]+", (title or "").lower())
    sig = [w for w in words if w not in _STOP and w not in _GENERIC and len(w) > 2]
    return " ".join(sorted(set(sig)))


def _map_area(area: str) -> str:
    """Best-effort map a legacy legalArea string to one of the 10 categories
    (only used to estimate category breadth for older articles)."""
    a = (area or "").lower()
    if "employ" in a or "dismiss" in a or "termination" in a or "salary" in a or "wage" in a:
        return "Employment Law"
    if "family" in a or "divorce" in a or "custody" in a or "alimony" in a or "marriage" in a:
        return "Family Law"
    if "tenan" in a or "rent" in a or "propert" in a or "landlord" in a or "evict" in a:
        return "Tenant & Property Rights"
    if "consumer" in a or "refund" in a or "product" in a:
        return "Consumer Rights"
    if "criminal" in a or "police" in a or "fir" in a or "bail" in a or "arrest" in a:
        return "Criminal Law"
    if "immigr" in a or "visa" in a or "h1b" in a or "h-1b" in a or "residency" in a:
        return "Immigration Law"
    if "corporate" in a or "startup" in a or "business" in a or "contract" in a or "company" in a or "ip" in a or "intellect" in a:
        return "Business & Startup Law"
    if "tax" in a or "finance" in a or "gst" in a:
        return "Tax & Finance Law"
    return "Civil Rights"


def _parse_date_ist(s: str):
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(IST).date()
    except (ValueError, TypeError):
        return None


def _load_existing_articles() -> list[dict]:
    """Index every published markdown file with the fields the engine needs."""
    out = []
    try:
        names = sorted(os.listdir(CONTENT_DIR))
    except FileNotFoundError:
        return out
    for name in names:
        if not name.endswith(".md"):
            continue
        fm = _read_frontmatter(os.path.join(CONTENT_DIR, name))
        title = fm.get("title", "")
        category = fm.get("category") or _map_area(fm.get("legalArea", ""))
        out.append({
            "slug":            name[:-3],
            "title":           title,
            "country":         fm.get("country", ""),
            "category":        category,
            "legalArea":       fm.get("legalArea", ""),
            "normalizedTopic": fm.get("normalizedTopic") or _normalize_topic(title),
            "date_ist":        _parse_date_ist(fm.get("date", "")),
            "date_raw":        fm.get("date", ""),
        })
    return out


# ─── SLUGS ───────────────────────────────────────────────────────────────────
def _clean_slug(text: str, maxlen: int = 70) -> str:
    """Lowercase hyphenated slug, truncated at a word boundary (never mid-word)."""
    s = re.sub(r"[^a-z0-9\s-]", "", (text or "").lower())
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    if len(s) <= maxlen:
        return s
    cut = s[:maxlen]
    if "-" in cut:
        cut = cut[:cut.rfind("-")]
    return cut.strip("-")


def _dedupe_slug(slug: str, taken: set) -> str:
    if slug not in taken:
        return slug
    i = 2
    while f"{slug}-{i}" in taken:
        i += 1
    return f"{slug}-{i}"


# ─── QUOTA / ROTATION ENGINE ─────────────────────────────────────────────────
def todays_quota(today) -> dict:
    """Per-country quota for `today` — 6 fixed + 1 rotating = 7."""
    q = dict(COUNTRY_QUOTA)
    rc = ROTATING_COUNTRIES[today.toordinal() % len(ROTATING_COUNTRIES)]
    q[rc] = q.get(rc, 0) + 1
    return q


def _violates_consecutive(recent: list, country: str, category: str) -> bool:
    """No 3rd consecutive article of the same category, nor same country, in
    today's chronological publish order."""
    tail_cat = [c for (_, c) in recent[-2:]]
    if len(tail_cat) == 2 and tail_cat[0] == tail_cat[1] == category:
        return True
    tail_ctry = [c for (c, _) in recent[-2:]]
    if len(tail_ctry) == 2 and tail_ctry[0] == tail_ctry[1] == country:
        return True
    return False


def _pick_slot(quota, by_country, cat_by_country, recent):
    """Pick the most under-quota country, then its least-covered category that
    doesn't break the consecutive rule."""
    avail = [(c, quota[c] - by_country.get(c, 0)) for c in quota if quota[c] - by_country.get(c, 0) > 0]
    if not avail:
        return None
    avail.sort(key=lambda x: -x[1])
    for country, _ in avail:
        cats = sorted(CATEGORIES, key=lambda cat: (cat_by_country.get((country, cat), 0), CATEGORIES.index(cat)))
        for cat in cats:
            if _violates_consecutive(recent, country, cat):
                continue
            return {"country": country, "category": cat}
    # Everything left would break the consecutive rule — relax it.
    country = avail[0][0]
    cats = sorted(CATEGORIES, key=lambda cat: (cat_by_country.get((country, cat), 0), CATEGORIES.index(cat)))
    return {"country": country, "category": cats[0]}


def decide_slots(per_run_max: int, existing: list[dict]) -> list[dict]:
    """Return up to per_run_max (country, category) slots still owed today."""
    today = datetime.now(IST).date()
    todays = [a for a in existing if a["date_ist"] == today]
    if len(todays) >= DAILY_TARGET:
        print(f"  ✓ Daily target already met ({len(todays)}/{DAILY_TARGET}) — nothing to publish")
        return []

    quota = todays_quota(today)
    by_country = {}
    for a in todays:
        by_country[a["country"]] = by_country.get(a["country"], 0) + 1

    cat_by_country = {}
    for a in existing:
        key = (a["country"], a["category"])
        cat_by_country[key] = cat_by_country.get(key, 0) + 1

    recent = [(a["country"], a["category"]) for a in sorted(todays, key=lambda x: x["date_raw"])]

    remaining = DAILY_TARGET - len(todays)
    n = min(per_run_max, remaining)
    chosen = []
    for _ in range(n):
        slot = _pick_slot(quota, by_country, cat_by_country, recent)
        if not slot:
            break
        chosen.append(slot)
        by_country[slot["country"]] = by_country.get(slot["country"], 0) + 1
        cat_by_country[(slot["country"], slot["category"])] = cat_by_country.get((slot["country"], slot["category"]), 0) + 1
        recent.append((slot["country"], slot["category"]))
    return chosen


# ─── AI CALLS ────────────────────────────────────────────────────────────────
async def call_gemini(prompt: str, max_tokens: int = 8192) -> str:
    """Gemini 2.5 Flash — fall back to 2.0 if rate-limited.

    On 2.5 models we set thinkingBudget=0: 'thinking' tokens are billed against
    the output budget and, for a pure-generation JSON task, can starve the actual
    answer (truncated/empty responses). We don't need reasoning tokens here."""
    if not GEMINI_API_KEY:
        raise ValueError("No GEMINI_API_KEY")
    for model in ("gemini-2.5-flash", "gemini-2.0-flash"):
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}"
        gen = {"temperature": 0.7, "maxOutputTokens": max_tokens}
        if model.startswith("gemini-2.5"):
            gen["thinkingConfig"] = {"thinkingBudget": 0}
        payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": gen}
        async with httpx.AsyncClient(timeout=180) as client:
            r = await client.post(url, json=payload)
            data = r.json()
            if "error" in data:
                err = data["error"]
                if err.get("code") == 429:
                    print(f"  ⚠ Gemini {model} rate-limited, trying next model...")
                    await asyncio.sleep(2)
                    continue
                raise ValueError(f"Gemini {model} error: {err.get('message', err)}")
            cand = (data.get("candidates") or [{}])[0]
            parts = cand.get("content", {}).get("parts", []) or []
            text = "".join(p.get("text", "") for p in parts)
            if not text:
                fr = cand.get("finishReason")
                print(f"  ⚠ Gemini {model} returned no text (finishReason={fr})")
                continue
            return text
    raise ValueError("All Gemini models failed/rate-limited")


async def call_groq(prompt: str, max_tokens: int = 8000) -> str:
    """Groq Llama 3.3 70B — 14,400 req/day free."""
    if not GROQ_API_KEY:
        raise ValueError("No GROQ_API_KEY")
    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.7,
                "max_tokens": max_tokens,
            },
        )
        data = r.json()
        if "error" in data:
            raise ValueError(f"Groq error: {data['error']['message']}")
        return data["choices"][0]["message"]["content"]


# ─── TITLE PROPOSAL (dedup-aware) ────────────────────────────────────────────
TITLE_PROMPT = """You are an SEO editor for LitigaForge AI. Propose ONE specific, search-optimized blog article title.

Country: {country}
Category: {category}
Example angles for inspiration (do NOT copy verbatim): {seeds}

These topics already exist — your title MUST cover a clearly DIFFERENT angle or subtopic:
{existing}

Rules:
- Be specific: name a concrete situation, right, or statute.
- Include the country name and the year 2026.
- 50-75 characters is ideal.
- No clickbait, no quotes around the title.

Return ONLY the title text on a single line, nothing else."""


async def propose_title(country: str, category: str, existing_titles: list[str]) -> str | None:
    seeds = ", ".join(CATEGORY_SEEDS.get(category, []))
    existing = "\n".join(f"- {t}" for t in existing_titles[:40]) or "- (none yet)"
    prompt = TITLE_PROMPT.format(country=country, category=category, seeds=seeds, existing=existing)
    raw = None
    try:
        raw = await call_groq(prompt, max_tokens=120)
    except Exception:
        try:
            raw = await call_gemini(prompt, max_tokens=120)
        except Exception as e:
            print(f"  ⚠ Title proposal failed: {e}")
            return None
    if not raw:
        return None
    line = raw.strip().splitlines()[0].strip().strip('"').strip("#").strip()
    return line[:120] if line else None


# ─── ARTICLE GENERATION ──────────────────────────────────────────────────────
ARTICLE_PROMPT = """You are a senior legal content writer for LitigaForge AI (litigaforge.com), an AI legal platform operating in India, USA, UK, UAE, Germany, Australia, Canada and Singapore.

Write a comprehensive, original, SEO-optimized legal article.
Topic: "{title}"
Country: {country}
Category: {category}

Return ONLY valid JSON — no markdown fences, no preamble, no explanation:

{{
  "title": "Specific, descriptive SEO H1 — include the country and the year 2026 (aim 50-75 characters)",
  "metaDescription": "Meta description, max 155 chars, include the primary keyword",
  "readTime": "X min read",
  "intro": "2-3 sentence hook that directly answers the core question",
  "sections": [
    {{"h2": "Section heading", "body": "At least 350 words of authoritative, specific content: name real statutes and section numbers, penalties, timelines, and numbered practical steps. Plain language for a layperson.", "keyTakeaway": "One actionable sentence"}}
  ],
  "faq": [{{"q": "Common question", "a": "Concise answer under 80 words"}}],
  "cta": "One sentence call-to-action to try LitigaForge AI free at litigaforge.com",
  "tags": ["tag1", "tag2", "tag3", "tag4", "tag5"]
}}

Requirements:
- AT LEAST 6 sections, each AT LEAST 350 words. The combined body MUST exceed 1700 words.
- EXACTLY 5 FAQ entries.
- Use real {country} statutes, acts and section numbers relevant to {category}.
- No fluff — every sentence must add value.
- Optimized for Google Featured Snippets (direct answers, numbered lists)."""

STRENGTHEN = "\n\nIMPORTANT: A previous draft was too short. Write substantially MORE: at least 8 sections, each 400+ words, total body well over 2000 words. Add detailed examples, step-by-step procedures and statute citations."


def _parse_article_json(raw: str) -> dict | None:
    clean = raw.strip()
    if clean.startswith("```json"):
        clean = clean[7:].strip()
    if clean.startswith("```"):
        clean = clean[3:].strip()
    if clean.endswith("```"):
        clean = clean[:-3].strip()
    if clean and clean[-1] != "}":
        last = clean.rfind("}")
        if last > 0:
            clean = clean[:last + 1].strip()
    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        m = re.search(r"\{[\s\S]*\}", clean)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                return None
    return None


async def generate_article(title: str, country: str, category: str, strengthen: bool = False) -> dict | None:
    """Generate + parse an article. Gemini is primary (large output ceiling needed
    for a full long-form article); Groq is the fallback. Each provider's output is
    parsed independently, so a truncated/garbled response from one falls through
    to the other instead of failing the whole slot."""
    prompt = ARTICLE_PROMPT.format(title=title, country=country, category=category)
    if strengthen:
        prompt += STRENGTHEN
    providers = [
        ("Gemini", lambda: call_gemini(prompt, max_tokens=ARTICLE_MAX_TOKENS)),
        ("Groq", lambda: call_groq(prompt, max_tokens=8000)),
    ]
    for name, fn in providers:
        try:
            raw = await fn()
        except Exception as e:
            print(f"  ⚠ {name} failed: {e}")
            continue
        article = _parse_article_json(raw)
        if article:
            print(f"  ✓ Draft via {name}")
            return article
        print(f"  ⚠ {name} returned unparseable/truncated JSON ({len(raw)} chars)")
    return None


def _article_wordcount(article: dict) -> int:
    """Word count of the substantive body (intro + section bodies), excluding
    FAQ and CTA boilerplate, so padding FAQs can't fake the floor."""
    text = article.get("intro", "") + " " + " ".join(
        s.get("body", "") for s in article.get("sections", [])
    )
    return len(re.findall(r"\b\w+\b", text))


async def generate_with_minwords(title: str, country: str, category: str) -> dict | None:
    """Generate, enforcing MIN_WORDS with one strengthened retry, else discard."""
    for attempt in range(2):
        article = await generate_article(title, country, category, strengthen=(attempt > 0))
        if not article:
            continue
        wc = _article_wordcount(article)
        if wc >= MIN_WORDS:
            return article
        action = "discarding" if attempt else "retrying with stronger prompt"
        print(f"  ⚠ Body only {wc} words (< {MIN_WORDS}) — {action}")
    return None


# ─── INTERNAL LINKING ────────────────────────────────────────────────────────
def _related_candidates(existing: list[dict], country: str, category: str, limit: int = 8) -> list[dict]:
    """Rank link targets: same country (+2) and/or category (+1) first, then fall
    back to any other published article so even a brand-new country/category still
    gets the full 3-5 internal links. Stable sort keeps deterministic ordering."""
    scored = []
    for a in existing:
        score = 0
        if a["country"].lower() == country.lower():
            score += 2
        if a["category"].lower() == category.lower():
            score += 1
        scored.append((score, a))
    scored.sort(key=lambda x: -x[0])
    return [a for _, a in scored[:limit]]


def _anchor_keywords(cand: dict) -> list[str]:
    """Ordered candidate anchor phrases from a related title (specific first)."""
    words = re.findall(r"[A-Za-z][A-Za-z-]+", cand.get("title", ""))
    sig = [w for w in words if w.lower() not in _STOP and w.lower() not in _GENERIC and len(w) > 3]
    out = [f"{sig[i]} {sig[i + 1]}" for i in range(len(sig) - 1)]      # bigrams
    out += sorted(set(sig), key=len, reverse=True)                     # then long unigrams
    return out


def insert_internal_links(article: dict, related: list[dict], target: int = 4, max_links: int = 5) -> int:
    """Insert 3-5 contextual /blog/<slug> links into section bodies. First tries
    to wrap a matching keyword; falls back to a natural 'see also' sentence."""
    sections = article.get("sections", [])
    if not sections:
        return 0
    used = set()
    count = 0
    for cand in related:
        if count >= max_links:
            break
        if cand["slug"] in used:
            continue
        placed = False
        for kw in _anchor_keywords(cand):
            pat = re.compile(r"(?<![\w/\[\]])(" + re.escape(kw) + r")(?![\w\]])", re.IGNORECASE)
            for s in sections:
                body = s.get("body", "")
                if body.count("](/blog/") >= 2:
                    continue
                if f"](/blog/{cand['slug']})" in body:
                    placed = True
                    break
                m = pat.search(body)
                if m:
                    anchor = m.group(1)
                    s["body"] = body[:m.start()] + f"[{anchor}](/blog/{cand['slug']})" + body[m.end():]
                    used.add(cand["slug"])
                    count += 1
                    placed = True
                    break
            if placed:
                break
    # Fallback: natural 'see also' sentences spread across mid sections.
    if count < target:
        idx = max(0, len(sections) // 2 - 1)
        for cand in related:
            if count >= target:
                break
            if cand["slug"] in used:
                continue
            s = sections[min(idx, len(sections) - 1)]
            s["body"] = s.get("body", "").rstrip() + f" For related guidance, see [{cand['title']}](/blog/{cand['slug']})."
            used.add(cand["slug"])
            count += 1
            idx += 1
    return count


# ─── PRODUCE ONE ARTICLE (shared by live + dry-run) ──────────────────────────
async def produce_article(country: str, category: str, existing: list[dict]) -> dict | None:
    existing_titles = [a["title"] for a in existing if a["country"].lower() == country.lower()]
    existing_norm = {(a["country"].lower(), a["normalizedTopic"]) for a in existing if a["normalizedTopic"]}
    existing_slugs = {a["slug"] for a in existing}

    # 1. Propose a fresh, non-duplicate title.
    title = None
    for _ in range(4):
        t = await propose_title(country, category, existing_titles)
        if not t:
            continue
        if (country.lower(), _normalize_topic(t)) in existing_norm:
            existing_titles.append(t)  # exclude on next attempt
            continue
        title = t
        break
    if not title:
        print(f"  ⏭ No fresh title for {country} · {category} — skipping")
        return None
    norm = _normalize_topic(title)

    # 2. Generate with the word-count floor enforced.
    article = await generate_with_minwords(title, country, category)
    if not article:
        return None
    article["title"] = article.get("title") or title

    # 3. Slug (word-boundary truncation + collision-safe).
    slug = _dedupe_slug(_clean_slug(article["title"]), existing_slugs)
    article["slug"] = slug
    article["country"] = country
    article["category"] = category
    article["legalArea"] = category

    # 4. Internal links.
    related = _related_candidates(existing, country, category)
    nlinks = insert_internal_links(article, related)

    wc = _article_wordcount(article)
    return {
        "article": article,
        "slug": slug,
        "country": country,
        "category": category,
        "normalizedTopic": norm,
        "wordcount": wc,
        "links": nlinks,
    }


# ─── BUILD MARKDOWN ──────────────────────────────────────────────────────────
def build_markdown(meta: dict) -> str:
    article = meta["article"]
    now = datetime.now(timezone.utc).isoformat()
    slug = meta["slug"]

    sections_md = "\n\n".join(
        f"## {s.get('h2', '')}\n\n{s.get('body', '')}\n\n> **Key takeaway:** {s.get('keyTakeaway', '')}"
        for s in article.get("sections", [])
    )
    faq_md = "\n\n".join(f"### {f['q']}\n\n{f['a']}" for f in article.get("faq", []))

    # Front-matter values are JSON-encoded — JSON is valid YAML, so this is robust
    # against quotes, colons and unicode in titles / FAQ answers without PyYAML.
    fm = {
        "title": article["title"],
        "description": article.get("metaDescription", ""),
        "slug": slug,
        "date": now,
        "dateModified": now,
        "country": meta["country"],
        "legalArea": meta["category"],
        "category": meta["category"],
        "tags": article.get("tags", []),
        "readTime": article.get("readTime", "7 min read"),
        "wordCount": meta["wordcount"],
        "normalizedTopic": meta["normalizedTopic"],
        "author": "LitigaForge AI Editorial Team",
        "authorUrl": f"https://{SITE_DOMAIN}/about",
        "canonicalUrl": f"https://{SITE_DOMAIN}/blog/{slug}",
        "schema": "FAQPage",
        "faq": article.get("faq", []),
    }
    front = "---\n" + "\n".join(f"{k}: {json.dumps(v, ensure_ascii=False)}" for k, v in fm.items()) + "\n---"

    return f"""{front}

# {article['title']}

{article.get('intro', '')}

{sections_md}

---

## Frequently Asked Questions

{faq_md}

---

*{article.get('cta', 'Get your free AI legal analysis at litigaforge.com.')}*

**Try it free:** [LitigaForge AI Legal Analysis](https://{SITE_DOMAIN})

<!-- auto-published by LitigaForge Content Pipeline -->
<!-- {meta['country']} · {meta['category']} · {meta['wordcount']} words · {meta['links']} internal links -->
<!-- generated: {now} -->
"""


# ─── GITHUB + INDEXNOW ───────────────────────────────────────────────────────
async def push_to_github(slug: str, content: str) -> bool:
    path = f"src/content/blog/{slug}.md"
    if not GITHUB_TOKEN:
        print("  ⚠ No GITHUB_TOKEN — writing locally")
        _write_local(path, content)
        return False

    encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    auth_prefix = "Bearer" if GITHUB_TOKEN.startswith(("ghs_", "ghp_")) else "token"
    headers = {
        "Authorization": f"{auth_prefix} {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "LitigaForgeBot/1.0",
    }
    async with httpx.AsyncClient(timeout=30) as client:
        sha = None
        check = await client.get(api_url, headers=headers)
        if check.status_code == 200:
            sha = check.json().get("sha")
        payload = {
            "message": f"content: auto-publish '{slug}'",
            "content": encoded,
            "committer": {"name": "LitigaForge Bot", "email": "bot@litigaforge.com"},
        }
        if sha:
            payload["sha"] = sha
        r = await client.put(api_url, headers=headers, json=payload)
        if r.status_code in (200, 201):
            print(f"  ✓ GitHub commit: {path}")
            return True
        err = r.json().get("message", "Unknown error")
        print(f"  ✗ GitHub error {r.status_code}: {err} — writing locally")
        _write_local(path, content)
        return False


def _write_local(path: str, content: str):
    full = os.path.join(".", path)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"  ✓ Local write: {path}")


async def submit_indexnow(slug: str) -> bool:
    if not INDEXNOW_KEY:
        return False
    live_url = f"https://{SITE_DOMAIN}/blog/{slug}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                "https://api.indexnow.org/indexnow",
                json={
                    "host": SITE_DOMAIN,
                    "key": INDEXNOW_KEY,
                    "keyLocation": f"https://{SITE_DOMAIN}/{INDEXNOW_KEY}.txt",
                    "urlList": [live_url],
                },
                headers={"Content-Type": "application/json"},
            )
        if r.status_code in (200, 202):
            print(f"  ✓ IndexNow submitted: {live_url}")
            return True
        print(f"  ⚠ IndexNow returned {r.status_code}")
    except Exception as e:
        print(f"  ⚠ IndexNow error: {e}")
    return False


# ─── PUBLISHED-SLUG CACHE (workflow artifact compatibility) ──────────────────
def load_published() -> set:
    try:
        with open(PUBLISHED_FILE) as f:
            return set(json.load(f))
    except Exception:
        return set()


def save_published(slugs: set):
    try:
        with open(PUBLISHED_FILE, "w") as f:
            json.dump(sorted(slugs), f)
    except OSError:
        pass


def _to_index(meta: dict) -> dict:
    """Convert a freshly produced article into the existing-index shape so later
    articles in the same run dedup/relate against it."""
    return {
        "slug": meta["slug"],
        "title": meta["article"]["title"],
        "country": meta["country"],
        "category": meta["category"],
        "legalArea": meta["category"],
        "normalizedTopic": meta["normalizedTopic"],
        "date_ist": datetime.now(IST).date(),
        "date_raw": datetime.now(timezone.utc).isoformat(),
    }


# ─── DRY RUN (item 16 — generate 5 samples, write nothing) ───────────────────
DRY_RUN_SLOTS = [
    ("India", "Criminal Law"),
    ("Canada", "Immigration Law"),
    ("Singapore", "Business & Startup Law"),
    ("UAE", "Family Law"),
    ("Germany", "Tax & Finance Law"),
]


async def dry_run():
    print("\n" + "=" * 60)
    print("  DRY RUN — generating 5 sample articles")
    print("  NOTHING will be saved, committed, or deployed")
    print("=" * 60)
    existing = _load_existing_articles()
    in_run = []
    results = []
    for i, (country, category) in enumerate(DRY_RUN_SLOTS, 1):
        print(f"\n[{i}/5] {country} · {category} ...")
        meta = await produce_article(country, category, existing + in_run)
        if not meta:
            print("  ✗ generation failed for this slot")
            continue
        results.append(meta)
        in_run.append(_to_index(meta))
        await asyncio.sleep(2)

    print("\n" + "=" * 60)
    print(f"  DRY-RUN REPORT — {len(results)}/5 articles produced")
    print("=" * 60)
    for i, m in enumerate(results, 1):
        a = m["article"]
        ok = "✓" if m["wordcount"] >= MIN_WORDS else "✗"
        print(f"\n── Article {i} ─────────────────────────────────────────────")
        print(f"  Title          : {a['title']}")
        print(f"  Slug           : {m['slug']}  ({len(m['slug'])} chars)")
        print(f"  Country        : {m['country']}")
        print(f"  Category       : {m['category']}")
        print(f"  Word count     : {m['wordcount']}  (min {MIN_WORDS} {ok})")
        print(f"  Internal links : {m['links']}")
        print(f"  Featured image : skipped (per spec — no images)")
        print(f"  Meta desc      : {a.get('metaDescription', '')}")
        faqs = a.get("faq", [])[:5]
        print(f"  FAQ ({len(faqs)}):")
        for q in faqs:
            print(f"    • {q.get('q', '')}")
    print("\n" + "=" * 60)
    print("  End of dry run. Review above, then approve to go live.")
    print("=" * 60 + "\n")


# ─── MAIN (live) ─────────────────────────────────────────────────────────────
async def main():
    print("\n" + "=" * 55)
    print("  LitigaForge Content Pipeline")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 55)

    if not GEMINI_API_KEY and not GROQ_API_KEY:
        print("❌ FATAL: set GEMINI_API_KEY or GROQ_API_KEY")
        return

    if DRY_RUN:
        await dry_run()
        return

    if not GENERATION_ENABLED:
        print("⏸  GENERATION_ENABLED is false — pipeline paused, no articles generated.")
        return

    existing = _load_existing_articles()
    print(f"📚 Published to date: {len(existing)} articles")

    slots = decide_slots(PER_RUN_MAX, existing)
    if not slots:
        print("Nothing to publish this run.")
        return
    print(f"🧭 Slots this run: " + ", ".join(f"{s['country']}·{s['category']}" for s in slots))

    in_run = []
    published = 0
    for slot in slots:
        print(f"\n{'─' * 55}\n🚀 {slot['country']} · {slot['category']}")
        meta = await produce_article(slot["country"], slot["category"], existing + in_run)
        if not meta:
            continue
        md = build_markdown(meta)
        print(f"  📄 Markdown: {len(md)} chars · {meta['wordcount']} words · {meta['links']} links")
        await push_to_github(meta["slug"], md)
        await submit_indexnow(meta["slug"])
        in_run.append(_to_index(meta))
        published += 1
        print(f"  ✅ https://{SITE_DOMAIN}/blog/{meta['slug']}")
        if published < len(slots):
            await asyncio.sleep(5)

    if in_run:
        save_published({a["slug"] for a in existing} | {m["slug"] for m in in_run})

    print(f"\n{'=' * 55}")
    print(f"  Pipeline complete — {published} article(s) published")
    print(f"  Total to date: {len(existing) + published}")
    print("=" * 55 + "\n")


if __name__ == "__main__":
    asyncio.run(main())

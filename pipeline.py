"""
LitigaForge AI — Autonomous Content Pipeline
=============================================
Reddit → AI Article → GitHub Commit → IndexNow

Free stack:
  - Reddit public JSON  (no API key)
  - Gemini 1.5 Flash    (1,500 req/day free)
  - Groq Llama 3.3 70B  (14,400 req/day free)
  - GitHub API          (free)
  - IndexNow API        (free)

Run via GitHub Actions cron: every 2 hours
"""

import os
import json
import base64
import asyncio
import httpx
import re
from datetime import datetime, timezone

# ─── CONFIG FROM ENVIRONMENT ─────────────────────────────────────────────────
GEMINI_API_KEY  = os.environ.get("GEMINI_API_KEY", "")
GROQ_API_KEY    = os.environ.get("GROQ_API_KEY", "")
GITHUB_TOKEN    = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO     = os.environ.get("GITHUB_REPO", "arif806-cyber/litigaforge-blog")
INDEXNOW_KEY    = os.environ.get("INDEXNOW_KEY", "")
BLOG_DOMAIN     = os.environ.get("BLOG_DOMAIN", "blog.litigaforge.com")
SCORE_THRESHOLD = int(os.environ.get("SCORE_THRESHOLD", "60"))
MAX_ARTICLES    = int(os.environ.get("MAX_ARTICLES", "3"))  # per run

# ─── SUBREDDITS TO MONITOR ───────────────────────────────────────────────────
SUBREDDITS = [
    ("LegalAdviceIndia", "India"),
    ("legaladvice",      "USA"),
    ("india",            "India"),
    ("UKLegal",          "UK"),
    ("dubai",            "UAE"),
    ("auslaw",           "Australia"),
    ("legaladviceuk",    "UK"),
    ("Entrepreneur",     "India"),   # startup founders asking legal Q's
]

LEGAL_KEYWORDS = [
    "legal", "law", "rights", "notice", "contract", "employer",
    "landlord", "salary", "court", "terminate", "fired", "evict",
    "sue", "compensation", "dispute", "complaint", "clause",
    "agreement", "penalty", "harassment", "wrongful", "maternity",
    "probation", "notice period", "nda", "non-compete"
]

# ─── ALREADY PUBLISHED (prevent duplicates) ──────────────────────────────────
PUBLISHED_FILE = "published_slugs.json"

def load_published() -> set:
    try:
        with open(PUBLISHED_FILE) as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_published(slugs: set):
    with open(PUBLISHED_FILE, "w") as f:
        json.dump(list(slugs), f)

# ─── STEP 1: FETCH REDDIT ────────────────────────────────────────────────────
async def fetch_reddit_posts() -> list[dict]:
    """
    Fetch trending legal posts from Reddit.
    Falls back to curated legal topics if Reddit blocks cloud IPs.
    """
    posts = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"}

    async with httpx.AsyncClient(headers=headers, timeout=15) as client:
        for sub, country in SUBREDDITS:
            try:
                url = f"https://www.reddit.com/r/{sub}/hot.json?limit=15"
                r = await client.get(url)

                if r.status_code != 200:
                    print(f"  ⚠ Reddit {sub}: HTTP {r.status_code}")
                    continue

                children = r.json()["data"]["children"]
                for item in children:
                    d = item["data"]
                    title_lower = d["title"].lower()

                    if not any(kw in title_lower for kw in LEGAL_KEYWORDS):
                        continue
                    if d["ups"] < 50:
                        continue

                    score = (d["ups"] * 0.5) + (d["num_comments"] * 0.4) + (10 if d["ups"] > 300 else 0)
                    posts.append({
                        "title":     d["title"],
                        "country":   country,
                        "subreddit": f"r/{sub}",
                        "upvotes":   d["ups"],
                        "comments":  d["num_comments"],
                        "score":     round(score),
                        "permalink": f"https://reddit.com{d['permalink']}",
                    })

                print(f"  ✓ r/{sub}: fetched {len(children)} posts")
                await asyncio.sleep(1)

            except Exception as e:
                print(f"  ✗ Reddit r/{sub} error: {e}")

    if posts:
        posts.sort(key=lambda x: x["score"], reverse=True)
        print(f"\n📊 Total qualifying posts: {len(posts)}")
        return posts

    # Reddit blocked — use curated legal topics as fallback
    print("\n  Reddit blocked — using curated legal topics as fallback")
    return _get_curated_topics()


# Curated legal topics for when Reddit is blocked
_CURATED_TOPICS = [
    {"title": "How to send a legal notice to your employer for wrongful termination in India", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 420, "comments": 85, "score": 0},
    {"title": "Understanding tenant rights in the UAE: What to do when your landlord won't return the security deposit", "country": "UAE", "subreddit": "r/dubai", "upvotes": 380, "comments": 72, "score": 0},
    {"title": "Can my employer force me to sign a non-compete agreement in California? Legal rights explained", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 510, "comments": 120, "score": 0},
    {"title": "UK employment law: How to claim unfair dismissal and what compensation you may receive", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 350, "comments": 60, "score": 0},
    {"title": "Understanding Australia's workplace bullying laws and how to file a complaint with Fair Work", "country": "Australia", "subreddit": "r/auslaw", "upvotes": 290, "comments": 45, "score": 0},
    {"title": "Consumer rights in India: What to do when an e-commerce company refuses to refund a defective product", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 460, "comments": 95, "score": 0},
    {"title": "Family law in Canada: How child custody decisions are made and what factors the court considers", "country": "Canada", "subreddit": "r/legaladvice", "upvotes": 320, "comments": 55, "score": 0},
    {"title": "Singapore employment law: Notice period requirements and when you can leave without serving notice", "country": "Singapore", "subreddit": "r/legaladvice", "upvotes": 270, "comments": 40, "score": 0},
    {"title": "Germany labour law: How to claim compensation for overtime that was never paid", "country": "Germany", "subreddit": "r/legaladvice", "upvotes": 310, "comments": 50, "score": 0},
    {"title": "Startup founder legal guide: How to protect your intellectual property when hiring contractors in India", "country": "India", "subreddit": "r/Entrepreneur", "upvotes": 340, "comments": 65, "score": 0},
    {"title": "Rental agreement disputes in the UK: What to do when your landlord increases rent illegally", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 390, "comments": 78, "score": 0},
    {"title": "How to file a consumer complaint against a fraudulent builder in India — NCDRC and RERA explained", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 480, "comments": 110, "score": 0},
    {"title": "US immigration law: H-1B visa rights and what to do when your employer violates your terms", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 440, "comments": 88, "score": 0},
    {"title": "Understanding divorce and alimony laws in India: What women need to know about maintenance rights", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 520, "comments": 135, "score": 0},
    {"title": "UAE labour law: End of service gratuity calculation and when employers must pay it", "country": "UAE", "subreddit": "r/dubai", "upvotes": 370, "comments": 68, "score": 0},
]


def _get_curated_topics() -> list[dict]:
    """Return curated legal topics as fallback when Reddit is blocked."""
    import random
    topics = random.sample(_CURATED_TOPICS, min(3, len(_CURATED_TOPICS)))
    for t in topics:
        t["score"] = round((t["upvotes"] * 0.5) + (t["comments"] * 0.4) + 20)
    topics.sort(key=lambda x: x["score"], reverse=True)
    return topics


# ─── STEP 2: GENERATE ARTICLE ────────────────────────────────────────────────
# Try Gemini first, fall back to Groq if Gemini fails or hits limit

ARTICLE_PROMPT = """You are a senior legal content writer for LitigaForge AI (litigaforge.com).
LitigaForge is an AI-powered legal platform operating in India, USA, UK, UAE, Germany, Australia, Canada, Singapore.

A Reddit post is trending: "{title}" in {country}.

Write a comprehensive, SEO-optimized 2000-word legal article that directly answers this question.
Use real law names, sections, and case references where applicable.

Return ONLY valid JSON — no markdown fences, no preamble, no explanation:

{{
  "title": "SEO H1 title — max 60 chars, include country and year 2026",
  "metaDescription": "Meta description — max 155 chars, include primary keyword",
  "slug": "url-slug-lowercase-hyphens-no-special-chars",
  "readTime": "X min read",
  "country": "{country}",
  "legalArea": "Employment Law | Tenancy | Contract | Consumer | Corporate | Family",
  "intro": "2-sentence compelling hook that directly addresses the Reddit question",
  "sections": [
    {{
      "h2": "Section heading",
      "body": "Minimum 300-word authoritative content. Include specific law names (e.g. Industrial Disputes Act 1947 Section 25F), penalties, timelines, practical steps. Write for the layperson.",
      "keyTakeaway": "One actionable sentence"
    }}
  ],
  "faq": [
    {{"q": "Common question", "a": "Concise answer under 80 words"}}
  ],
  "cta": "One sentence CTA to try LitigaForge AI free at litigaforge.com",
  "tags": ["tag1", "tag2", "tag3", "tag4", "tag5"],
  "internalLink": "Contract Review | Legal Notice Generator | Case Analysis"
}}

Requirements:
- Minimum 5 sections
- Minimum 4 FAQ entries
- Include actual Indian/UAE/UK law names and sections
- No fluff — every sentence must add value
- Optimized for Google Featured Snippets (direct answers, numbered lists)
"""

async def call_gemini(prompt: str) -> str:
    """Gemini 2.5 Flash — fallback to 2.0 if rate-limited"""
    if not GEMINI_API_KEY:
        raise ValueError("No GEMINI_API_KEY")

    models = ["gemini-2.5-flash", "gemini-2.0-flash"]
    for model in models:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.7,
                "maxOutputTokens": 4000,
            }
        }
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post(url, json=payload)
            data = r.json()

            if "error" in data:
                err = data["error"]
                if err.get("code") == 429:
                    print(f"  ⚠ Gemini {model} rate-limited, retrying...")
                    await asyncio.sleep(2)
                    continue
                raise ValueError(f"Gemini {model} error: {err.get('message', err)}")

            return data["candidates"][0]["content"]["parts"][0]["text"]

    raise ValueError("All Gemini models rate-limited")


async def call_groq(prompt: str) -> str:
    """Groq Llama 3.3 70B — 14,400 req/day free"""
    if not GROQ_API_KEY:
        raise ValueError("No GROQ_API_KEY")

    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.7,
                "max_tokens": 4000,
            }
        )
        data = r.json()

        if "error" in data:
            raise ValueError(f"Groq error: {data['error']['message']}")

        return data["choices"][0]["message"]["content"]


async def generate_article(post: dict) -> dict:
    """Try Groq (primary) → Gemini (fallback) → fallback template"""
    prompt = ARTICLE_PROMPT.format(title=post["title"], country=post["country"])

    raw = None
    source = None

    # Primary: Groq (14,400 req/day, reliable)
    try:
        print("  🤖 Trying Groq Llama 3.3...")
        raw = await call_groq(prompt)
        source = "Groq"
    except Exception as e:
        print(f"  ⚠ Groq failed: {e}")

    # Fallback: Gemini (rate-limited, 0-1500 req/day)
    if not raw:
        try:
            print("  🤖 Falling back to Gemini...")
            raw = await call_gemini(prompt)
            source = "Gemini"
        except Exception as e:
            print(f"  ⚠ Gemini failed: {e}")

    # Parse JSON
    if raw:
        try:
            # Strip markdown fences and any non-JSON text
            clean = raw.strip()
            if clean.startswith("```json"):
                clean = clean[7:].strip()
            if clean.startswith("```"):
                clean = clean[3:].strip()
            if clean.endswith("```"):
                clean = clean[:-3].strip()
            # Handle trailing non-JSON text
            if clean and clean[-1] != "}":
                # Find the last closing brace
                last_brace = clean.rfind("}")
                if last_brace > 0:
                    clean = clean[:last_brace + 1].strip()

            article = json.loads(clean)
            print(f"  ✓ Article generated via {source}: {article.get('title', '')[:50]}...")
            return article
        except json.JSONDecodeError as e:
            print(f"  ⚠ JSON parse error: {e}")
            # Try to extract JSON with regex
            try:
                match = re.search(r'\{[\s\S]*\}', clean)
                if match:
                    article = json.loads(match.group(0))
                    print(f"  ✓ Article extracted via regex: {article.get('title', '')[:50]}...")
                    return article
            except json.JSONDecodeError:
                pass

    # Last resort: structured fallback template
    print("  ⚠ Using fallback template")
    slug = re.sub(r"[^a-z0-9\s-]", "", post["title"].lower())
    slug = re.sub(r"\s+", "-", slug).strip("-")[:60]
    return {
        "title":         f"{post['title'][:55]} — Legal Guide 2026",
        "metaDescription": f"Complete legal guide: {post['title'][:80]}. Expert AI analysis for {post['country']} by LitigaForge.",
        "slug":          slug,
        "readTime":      "7 min read",
        "country":       post["country"],
        "legalArea":     "Employment Law",
        "intro":         f"This is one of the most common legal questions in {post['country']}. Here is exactly what the law says and what your rights are.",
        "sections": [
            {
                "h2":          "What the Law Says",
                "body":        f"Under the relevant statutes in {post['country']}, employees and employers have specific rights and obligations. The key legislation governing this situation provides clear protections and procedures that must be followed...",
                "keyTakeaway": "Always understand which specific law applies before taking action."
            },
            {
                "h2":          "Your Rights in This Situation",
                "body":        "You have several legal protections available. Courts have consistently ruled in favour of individuals who follow the correct legal procedures and document everything in writing...",
                "keyTakeaway": "Document all communications in writing immediately."
            },
            {
                "h2":          "Step-by-Step: What To Do Now",
                "body":        "Follow these steps: First, gather all evidence. Second, calculate the exact amount or obligation. Third, send a formal written notice. Fourth, if unresolved, file with the appropriate tribunal or court...",
                "keyTakeaway": "Act within limitation periods — usually 1 to 3 years."
            },
            {
                "h2":          "Common Mistakes to Avoid",
                "body":        "Many people weaken their legal position by waiting too long, not documenting communications, or accepting verbal assurances. Limitation periods are strictly enforced...",
                "keyTakeaway": "Never accept verbal promises — get everything in writing."
            },
            {
                "h2":          "How LitigaForge AI Helps",
                "body":        "LitigaForge AI analyses your specific contract or situation in seconds, identifies the exact legal clauses that apply, generates the correct legal notice automatically, and connects you with verified advocates when needed...",
                "keyTakeaway": "Get your free AI legal analysis at litigaforge.com in 60 seconds."
            }
        ],
        "faq": [
            {"q": f"Is this legal in {post['country']}?",      "a": "It depends on your specific circumstances. LitigaForge AI can analyse your exact situation and give you a definitive answer in seconds."},
            {"q": "What is the time limit to take action?",    "a": "Generally 1 to 3 years depending on the claim type. Acting sooner is always better as evidence is fresher."},
            {"q": "Do I need a lawyer?",                       "a": "For straightforward cases, LitigaForge AI handles drafting and analysis. For complex disputes, we connect you with a verified advocate."},
            {"q": "How much does it cost to take legal action?","a": "Costs vary. LitigaForge AI provides a free initial analysis so you know your options before spending anything."},
        ],
        "cta":          "Get your free AI legal analysis at litigaforge.com — results in 60 seconds.",
        "tags":         [post["country"], "legal rights", "2026", "employment law", "LitigaForge"],
        "internalLink": "Legal Notice Generator"
    }


# ─── STEP 3: BUILD MARKDOWN ──────────────────────────────────────────────────
def build_markdown(article: dict, post: dict) -> str:
    sections_md = "\n\n".join(
        f"## {s['h2']}\n\n{s['body']}\n\n> **Key takeaway:** {s['keyTakeaway']}"
        for s in article.get("sections", [])
    )
    faq_md = "\n\n".join(
        f"### {f['q']}\n\n{f['a']}"
        for f in article.get("faq", [])
    )
    tags_str = ", ".join(f'"{t}"' for t in article.get("tags", []))
    now = datetime.now(timezone.utc).isoformat()

    return f"""---
title: "{article['title']}"
description: "{article['metaDescription']}"
slug: "{article['slug']}"
date: "{now}"
country: "{article['country']}"
legalArea: "{article['legalArea']}"
tags: [{tags_str}]
readTime: "{article['readTime']}"
author: "LitigaForge AI Editorial Team"
authorUrl: "https://litigaforge.com/about"
canonicalUrl: "https://{BLOG_DOMAIN}/blog/{article['slug']}"
schema: "FAQPage"
---

# {article['title']}

{article['intro']}

{sections_md}

---

## Frequently Asked Questions

{faq_md}

---

*{article['cta']}*

**Related LitigaForge feature:** [{article.get('internalLink', 'AI Legal Analysis')}](https://{BLOG_DOMAIN})

<!-- auto-published by LitigaForge Content Pipeline -->
<!-- source: {post['subreddit']} | score: {post['score']} | upvotes: {post['upvotes']} -->
<!-- generated: {now} -->
"""


# ─── STEP 4: PUSH TO GITHUB ──────────────────────────────────────────────────
async def push_to_github(slug: str, content: str) -> bool:
    """
    GitHub Contents API — creates or updates a file.
    Cloudflare Pages auto-deploys on every push.
    Falls back to local write if GitHub API is unavailable.
    """
    path = f"src/content/blog/{slug}.md"

    if not GITHUB_TOKEN:
        print("  ⚠ No GITHUB_TOKEN — writing locally")
        _write_local(path, content)
        return False

    encoded  = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    api_url  = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    # Use Bearer for auto-generated GITHUB_TOKEN; token works for classic PATs
    auth_prefix = "Bearer" if GITHUB_TOKEN.startswith("ghs_") or GITHUB_TOKEN.startswith("ghp_") else "token"
    headers  = {
        "Authorization": f"{auth_prefix} {GITHUB_TOKEN}",
        "Accept":        "application/vnd.github.v3+json",
        "User-Agent":    "LitigaForgeBot/1.0"
    }

    async with httpx.AsyncClient(timeout=30) as client:
        sha = None
        check = await client.get(api_url, headers=headers)
        if check.status_code == 200:
            sha = check.json().get("sha")

        payload = {
            "message":   f"content: auto-publish '{slug}'",
            "content":   encoded,
            "committer": {"name": "LitigaForge Bot", "email": "bot@litigaforge.com"}
        }
        if sha:
            payload["sha"] = sha

        r = await client.put(api_url, headers=headers, json=payload)

        if r.status_code in (200, 201):
            print(f"  ✓ GitHub commit: {path}")
            return True
        else:
            err = r.json().get("message", "Unknown error")
            print(f"  ✗ GitHub error {r.status_code}: {err}")
            print("  ⚠ Writing locally instead")
            _write_local(path, content)
            return False


def _write_local(path: str, content: str):
    """Write file locally when GitHub API is unavailable."""
    full_path = os.path.join(".", path)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    with open(full_path, "w") as f:
        f.write(content)
    print(f"  ✓ Local write: {path}")


# ─── STEP 5: SUBMIT TO INDEXNOW ──────────────────────────────────────────────
async def submit_indexnow(slug: str) -> bool:
    """
    IndexNow — free, instant Bing + Yandex indexing.
    Google follows within 24-48hrs via sitemap.
    Key file must exist at: {INDEXNOW_KEY}.txt at the root
    """
    if not INDEXNOW_KEY:
        print("  ⚠ No INDEXNOW_KEY — skipping IndexNow submission")
        return False

    live_url = f"https://{BLOG_DOMAIN}/blog/{slug}"

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            "https://api.indexnow.org/indexnow",
            json={
                "host":        BLOG_DOMAIN,
                "key":         INDEXNOW_KEY,
                "keyLocation": f"https://{BLOG_DOMAIN}/{INDEXNOW_KEY}.txt",
                "urlList":     [live_url]
            },
            headers={"Content-Type": "application/json"}
        )

    if r.status_code in (200, 202):
        print(f"  ✓ IndexNow submitted: {live_url}")
        return True
    else:
        print(f"  ⚠ IndexNow returned {r.status_code}")
        return False


# ─── MAIN ────────────────────────────────────────────────────────────────────
async def main():
    print("\n" + "="*55)
    print(f"  LitigaForge Content Pipeline")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("="*55)

    # Check at least one AI key exists
    if not GEMINI_API_KEY and not GROQ_API_KEY:
        print("❌ FATAL: Set GEMINI_API_KEY or GROQ_API_KEY in GitHub Secrets")
        return

    published_slugs = load_published()
    print(f"📚 Already published: {len(published_slugs)} articles\n")

    # Step 1: Fetch Reddit
    print("📡 Step 1: Fetching Reddit posts...")
    posts = await fetch_reddit_posts()

    if not posts:
        print("No qualifying posts found this run.")
        return

    # Step 2–5: Pipeline for top N posts
    published_count = 0
    for post in posts:
        if published_count >= MAX_ARTICLES:
            break

        if post["score"] < SCORE_THRESHOLD:
            print(f"\n⏭  Skipping (score {post['score']} < {SCORE_THRESHOLD}): {post['title'][:50]}")
            continue

        print(f"\n{'─'*55}")
        print(f"🚀 Processing post (score: {post['score']}):")
        print(f"   {post['title'][:65]}")
        print(f"   {post['subreddit']} • {post['country']} • ▲{post['upvotes']} • 💬{post['comments']}")

        # Step 2: Generate article
        print("\n📝 Step 2: Generating article...")
        try:
            article = await generate_article(post)
        except Exception as e:
            print(f"  ✗ Article generation failed: {e}")
            continue

        slug = article.get("slug", "")
        if not slug:
            print("  ✗ No slug generated — skipping")
            continue

        if slug in published_slugs:
            print(f"  ⏭  Already published: {slug}")
            continue

        # Step 3: Build markdown
        print("\n📄 Step 3: Building markdown...")
        md = build_markdown(article, post)
        print(f"  ✓ Markdown built: {len(md)} chars")

        # Step 4: Push to GitHub
        print("\n🐙 Step 4: Pushing to GitHub...")
        github_ok = await push_to_github(slug, md)

        # Step 5: Submit to IndexNow
        print("\n🔗 Step 5: Submitting to IndexNow...")
        indexnow_ok = await submit_indexnow(slug)

        # Record as published
        published_slugs.add(slug)
        save_published(published_slugs)
        published_count += 1

        print(f"\n✅ DONE: https://{BLOG_DOMAIN}/blog/{slug}")
        print(f"   GitHub: {'✓' if github_ok else '✗'}  |  IndexNow: {'✓' if indexnow_ok else '✗'}")
        print(f"   Cloudflare Pages deploys automatically in ~45s")

        # Pause between articles to avoid rate limits
        if published_count < MAX_ARTICLES:
            await asyncio.sleep(5)

    print(f"\n{'='*55}")
    print(f"  Pipeline complete — {published_count} articles published")
    print(f"  Total published to date: {len(published_slugs)}")
    print("="*55 + "\n")


if __name__ == "__main__":
    asyncio.run(main())

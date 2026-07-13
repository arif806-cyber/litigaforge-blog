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
GITHUB_TOKEN    = (
    os.environ.get("GITHUB_TOKEN")
    or os.environ.get("GITHUB_PERSONAL_ACCESS_TOKEN")
    or os.environ.get("GITHUB_PERSONAL_ACCESS_TOKEN_NOEXPIRE")
    or ""
)
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


# ─── TOPIC SATURATION CHECK ──────────────────────────────────────────────────
_STOP_WORDS = {
    "a","an","the","and","or","but","in","on","at","to","for","of","with",
    "by","from","as","is","was","are","were","be","been","being","have",
    "has","had","do","does","did","will","would","could","should","may",
    "might","shall","can","how","what","when","where","who","why","which",
    "your","my","our","their","its","this","that","these","those","not",
    "no","vs","2026","guide","complete","explained","understanding",
    "navigating","rights","law","legal","india","uae","uk","usa","germany",
    "australia","canada","singapore",
}

def _slug_keywords(text: str) -> set:
    """Extract meaningful keywords from a title or slug."""
    words = re.sub(r"[^a-z0-9\s]", " ", text.lower()).split()
    return {w for w in words if len(w) >= 4 and w not in _STOP_WORDS}

def _topic_already_covered(title: str, published_slugs: set, threshold: int = 4) -> bool:
    """
    Returns True if 'threshold' or more published slugs share
    2+ keywords with this title — meaning this topic area is saturated.
    """
    candidate_kw = _slug_keywords(title)
    if not candidate_kw:
        return False
    hits = 0
    for slug in published_slugs:
        slug_kw = _slug_keywords(slug.replace("-", " "))
        if len(candidate_kw & slug_kw) >= 2:
            hits += 1
            if hits >= threshold:
                return True
    return False


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


# ─── CURATED LEGAL TOPICS (200+ unique topics, 8 countries × 10 legal areas) ─
_CURATED_TOPICS = [
    # ── INDIA — Employment ──
    {"title": "PF withdrawal process in India: how to claim your EPF online and offline 2026", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 410, "comments": 88},
    {"title": "Gratuity calculation formula India: eligibility, maximum limit and how to claim", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 390, "comments": 75},
    {"title": "Can my employer deduct salary for leaves in India? rules under Payment of Wages Act", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 370, "comments": 68},
    {"title": "Maternity benefit in India: 26 weeks paid leave rules and employer obligations 2026", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 450, "comments": 102},
    {"title": "How to claim unpaid overtime in India: Industrial Disputes Act and Labour Court process", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 340, "comments": 60},
    {"title": "Wrongful termination without notice period India: legal steps and compensation rights", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 480, "comments": 115},
    {"title": "ESIC benefits India: which employees qualify and how to claim medical reimbursement", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 320, "comments": 55},
    {"title": "Can employer force you to work weekends in India? overtime pay rules explained", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 360, "comments": 70},
    {"title": "Sexual harassment at workplace India: POSH Act complaint procedure step by step", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 510, "comments": 130},
    {"title": "Provident Fund withdrawal before 5 years: tax implications and penalty explained", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 355, "comments": 65},

    # ── INDIA — Consumer ──
    {"title": "How to file RTI application in India: step by step guide for citizens 2026", "country": "India", "subreddit": "r/india", "upvotes": 430, "comments": 95},
    {"title": "MahaRERA complaint against builder: how to file and what compensation you can get", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 470, "comments": 108},
    {"title": "Credit card fraud in India: how to dispute charges and get refund from bank", "country": "India", "subreddit": "r/india", "upvotes": 395, "comments": 82},
    {"title": "How to file complaint against insurance company in India: IRDAI ombudsman process", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 350, "comments": 63},
    {"title": "National Consumer Disputes Redressal Commission: filing a complaint for amounts over 1 crore", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 310, "comments": 50},
    {"title": "Food safety complaint in India: how to report FSSAI violations and get action taken", "country": "India", "subreddit": "r/india", "upvotes": 290, "comments": 45},
    {"title": "Online fraud recovery India: cybercrime portal complaint and police FIR process", "country": "India", "subreddit": "r/india", "upvotes": 520, "comments": 140},
    {"title": "Zomato Swiggy delivery dispute: consumer rights and platform liability in India", "country": "India", "subreddit": "r/india", "upvotes": 380, "comments": 78},

    # ── INDIA — Property & Tenancy ──
    {"title": "How to evict a tenant in India legally: notice period and court procedure 2026", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 420, "comments": 90},
    {"title": "Stamp duty and registration charges for property in India: state-wise guide 2026", "country": "India", "subreddit": "r/india", "upvotes": 375, "comments": 70},
    {"title": "Can landlord increase rent arbitrarily in India? Rent Control Act protections explained", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 410, "comments": 85},
    {"title": "Society maintenance charges dispute in India: which authority to complain to", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 340, "comments": 58},
    {"title": "Property inheritance without a will in India: Hindu Succession Act rules explained", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 460, "comments": 100},
    {"title": "How to get electricity connection disconnected to force tenant out legally in India", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 285, "comments": 42},
    {"title": "Flat purchase agreement checklist India: clauses to verify before signing 2026", "country": "India", "subreddit": "r/india", "upvotes": 390, "comments": 80},

    # ── INDIA — Family Law ──
    {"title": "Child custody after divorce in India: factors courts consider under Hindu Marriage Act", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 490, "comments": 120},
    {"title": "Maintenance rights for wife in India: Section 125 CrPC amount and how to apply", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 470, "comments": 110},
    {"title": "How to get legal separation in India without divorce: judicial separation procedure", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 360, "comments": 68},
    {"title": "Domestic violence protection order India: how to file under DV Act and get shelter", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 530, "comments": 145},
    {"title": "NRI divorce in India: jurisdiction, property rights and enforcement of foreign decree", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 440, "comments": 95},
    {"title": "Dowry harassment case in India: Section 498A IPC, bail and evidence requirements", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 500, "comments": 128},
    {"title": "Muslim divorce laws in India: Triple Talaq ban, Khula and Mubarat explained 2026", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 385, "comments": 75},

    # ── INDIA — Startup & IP ──
    {"title": "How to register a trademark in India: step by step process, cost and timeline 2026", "country": "India", "subreddit": "r/Entrepreneur", "upvotes": 420, "comments": 90},
    {"title": "Patent filing for Indian startups: provisional vs complete specification and cost", "country": "India", "subreddit": "r/Entrepreneur", "upvotes": 375, "comments": 72},
    {"title": "DPIIT startup recognition India: benefits, eligibility and step by step application", "country": "India", "subreddit": "r/Entrepreneur", "upvotes": 400, "comments": 82},
    {"title": "Founder agreement checklist for Indian startups: equity, vesting and exit clauses", "country": "India", "subreddit": "r/Entrepreneur", "upvotes": 355, "comments": 65},
    {"title": "GST registration for freelancers and consultants in India: threshold and process 2026", "country": "India", "subreddit": "r/Entrepreneur", "upvotes": 410, "comments": 88},
    {"title": "Angel tax exemption for Indian startups under Section 56(2)(viib): how to qualify", "country": "India", "subreddit": "r/Entrepreneur", "upvotes": 330, "comments": 55},

    # ── INDIA — Criminal & Civil ──
    {"title": "FIR registration refused by police in India: how to approach magistrate under Section 156(3)", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 495, "comments": 125},
    {"title": "Anticipatory bail in India: how to apply, grounds and duration under Section 438 CrPC", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 430, "comments": 92},
    {"title": "Section 138 cheque dishonour case: timeline, penalty and settlement process in India", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 465, "comments": 105},
    {"title": "Defamation law in India: civil vs criminal defamation, Section 499 IPC explained", "country": "India", "subreddit": "r/LegalAdviceIndia", "upvotes": 370, "comments": 70},
    {"title": "Cybercrime laws in India: Section 66 IT Act offences, penalties and how to report", "country": "India", "subreddit": "r/india", "upvotes": 490, "comments": 118},

    # ── INDIA — Taxation ──
    {"title": "Income tax notice response in India: how to reply to Section 148 reassessment notice", "country": "India", "subreddit": "r/india", "upvotes": 415, "comments": 88},
    {"title": "Capital gains tax on property sale in India: calculation, exemptions and reporting 2026", "country": "India", "subreddit": "r/india", "upvotes": 440, "comments": 98},
    {"title": "HRA exemption calculation in India: rules for salaried employees and self-employed 2026", "country": "India", "subreddit": "r/india", "upvotes": 380, "comments": 72},
    {"title": "Tax evasion vs tax avoidance in India: GAAR, SAAR and consequences of non-compliance", "country": "India", "subreddit": "r/india", "upvotes": 320, "comments": 52},
    {"title": "Section 80C deductions in India: complete list of eligible investments and limits 2026", "country": "India", "subreddit": "r/india", "upvotes": 460, "comments": 105},

    # ── UAE — Employment ──
    {"title": "Limited vs unlimited contract UAE: which is better and termination rules 2026", "country": "UAE", "subreddit": "r/dubai", "upvotes": 400, "comments": 82},
    {"title": "MOHRE complaint against employer UAE: unpaid salary process step by step 2026", "country": "UAE", "subreddit": "r/dubai", "upvotes": 430, "comments": 95},
    {"title": "UAE 6 month ban after resignation: when it applies and how to get it waived 2026", "country": "UAE", "subreddit": "r/dubai", "upvotes": 510, "comments": 132},
    {"title": "Annual leave encashment UAE: calculation formula and when employer must pay 2026", "country": "UAE", "subreddit": "r/dubai", "upvotes": 360, "comments": 65},
    {"title": "UAE sick leave rules: paid and unpaid entitlement, employer obligations and abuse", "country": "UAE", "subreddit": "r/dubai", "upvotes": 340, "comments": 60},
    {"title": "Non-compete clause enforceability in UAE: Federal Labour Law Article 10 explained", "country": "UAE", "subreddit": "r/dubai", "upvotes": 370, "comments": 70},
    {"title": "Arbitrary dismissal compensation UAE: how to calculate and claim under Labour Law", "country": "UAE", "subreddit": "r/dubai", "upvotes": 440, "comments": 98},

    # ── UAE — Tenancy ──
    {"title": "Eviction notice rules Dubai: RERA forms, 12-month notice and tenant defences 2026", "country": "UAE", "subreddit": "r/dubai", "upvotes": 420, "comments": 88},
    {"title": "RERA rental dispute settlement centre Dubai: how to file complaint and process 2026", "country": "UAE", "subreddit": "r/dubai", "upvotes": 395, "comments": 78},
    {"title": "Rent increase limits Dubai 2026: RERA calculator and how to dispute above-cap hike", "country": "UAE", "subreddit": "r/dubai", "upvotes": 480, "comments": 118},
    {"title": "Ejari registration Dubai: why it matters, how to register and what happens if you don't", "country": "UAE", "subreddit": "r/dubai", "upvotes": 350, "comments": 63},
    {"title": "Maintenance responsibility for tenants vs landlords in UAE: which repairs must landlord fix", "country": "UAE", "subreddit": "r/dubai", "upvotes": 330, "comments": 56},
    {"title": "Abu Dhabi tenancy law 2026: rent dispute authority and eviction grounds explained", "country": "UAE", "subreddit": "r/dubai", "upvotes": 310, "comments": 50},

    # ── UAE — Visa & Immigration ──
    {"title": "UAE golden visa eligibility 2026: categories, required documents and application steps", "country": "UAE", "subreddit": "r/dubai", "upvotes": 540, "comments": 145},
    {"title": "UAE residence visa cancellation: what happens to your status and 30 day grace period", "country": "UAE", "subreddit": "r/dubai", "upvotes": 460, "comments": 108},
    {"title": "Visit visa to residence visa conversion UAE: rules and employer sponsorship process 2026", "country": "UAE", "subreddit": "r/dubai", "upvotes": 410, "comments": 85},
    {"title": "Freelance permit UAE 2026: how to get it, cost and which free zones allow it", "country": "UAE", "subreddit": "r/dubai", "upvotes": 390, "comments": 78},
    {"title": "UAE overstay fines 2026: calculation per day, amnesty options and how to clear fines", "country": "UAE", "subreddit": "r/dubai", "upvotes": 470, "comments": 112},

    # ── UAE — Family & Personal ──
    {"title": "Divorce for expats in UAE: court process, jurisdiction and asset division 2026", "country": "UAE", "subreddit": "r/dubai", "upvotes": 420, "comments": 90},
    {"title": "Child custody for non-Muslims in UAE: personal status courts and Sharia alternatives", "country": "UAE", "subreddit": "r/dubai", "upvotes": 405, "comments": 83},
    {"title": "UAE inheritance law for expats: application of home country law and UAE courts", "country": "UAE", "subreddit": "r/dubai", "upvotes": 375, "comments": 70},
    {"title": "Bounced cheque criminal charges UAE: Federal Law No. 14 2022 changes and your rights", "country": "UAE", "subreddit": "r/dubai", "upvotes": 450, "comments": 100},

    # ── UK — Employment ──
    {"title": "Constructive dismissal UK: what it is, how to prove it and compensation you can claim", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 490, "comments": 122},
    {"title": "Zero hours contract rights UK 2026: holiday pay, minimum wage and unfair treatment", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 460, "comments": 108},
    {"title": "UK statutory sick pay 2026: entitlement, employer obligations and what to do if refused", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 380, "comments": 74},
    {"title": "TUPE transfer UK: employee rights when business changes hands in 2026", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 340, "comments": 60},
    {"title": "Whistleblowing protection UK: Public Interest Disclosure Act rights and detriment claims", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 420, "comments": 90},
    {"title": "Workplace discrimination claims UK: Equality Act 2010 protected characteristics and ET1 form", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 510, "comments": 132},
    {"title": "UK redundancy consultation rules 2026: collective and individual process, selection criteria", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 395, "comments": 80},
    {"title": "Employment tribunal claim UK 2026: step by step process, fees and time limits", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 470, "comments": 115},

    # ── UK — Tenancy ──
    {"title": "Section 8 eviction UK: grounds, notice periods and defending at court 2026", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 440, "comments": 98},
    {"title": "Renters Reform Bill UK 2026: what tenants must know about new protections", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 520, "comments": 140},
    {"title": "UK deposit dispute resolution: Tenancy Deposit Scheme claim process step by step", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 410, "comments": 85},
    {"title": "Damp and mould in UK rental: landlord legal obligation and how to force repairs", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 490, "comments": 120},
    {"title": "Houses in Multiple Occupation UK 2026: HMO licence rules and tenant rights", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 350, "comments": 62},
    {"title": "Short-term let and Airbnb rules UK 2026: permitted development and council restrictions", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 330, "comments": 55},

    # ── UK — Consumer & Financial ──
    {"title": "Section 75 claim UK: how to get credit card refund for faulty goods or services", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 480, "comments": 118},
    {"title": "PPI mis-selling claims UK 2026: can you still claim after Plevin ruling deadline", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 370, "comments": 68},
    {"title": "UK financial ombudsman complaint 2026: how to escalate bank dispute and win", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 430, "comments": 92},
    {"title": "County Court Judgement CCJ UK: how it affects credit and how to get it set aside", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 400, "comments": 82},
    {"title": "UK small claims court 2026: how to file, what it costs and what you can claim for", "country": "UK", "suburdit": "r/legaladviceuk", "upvotes": 460, "comments": 108},

    # ── UK — Immigration ──
    {"title": "UK skilled worker visa 2026: eligibility, salary threshold and switching from student visa", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 510, "comments": 135},
    {"title": "UK indefinite leave to remain 2026: 5 year route requirements and application checklist", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 480, "comments": 120},
    {"title": "UK spouse visa refusal appeal 2026: grounds, evidence and success rates explained", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 450, "comments": 105},
    {"title": "Graduate visa UK 2026: how to switch from student visa, working rights and duration", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 420, "comments": 88},
    {"title": "UK naturalisation as British citizen 2026: eligibility, good character test and application", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 400, "comments": 80},

    # ── UK — Family & Estate ──
    {"title": "How to contest a will in UK 2026: undue influence, lack of capacity and Inheritance Act claims", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 430, "comments": 93},
    {"title": "Cohabitation agreement UK 2026: what unmarried couples need to protect their assets", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 395, "comments": 78},
    {"title": "Child maintenance UK 2026: CMS calculation, enforcement and variation requests", "country": "UK", "subreddit": "r/legaladviceuk", "upvotes": 465, "comments": 110},

    # ── USA — Employment ──
    {"title": "At-will employment US: exceptions, wrongful termination and what employees can do", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 510, "comments": 135},
    {"title": "FMLA leave US 2026: 12-week entitlement, employer retaliation and how to file complaint", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 480, "comments": 120},
    {"title": "Unpaid internship legality US 2026: DOL seven-factor test and your wage claim rights", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 390, "comments": 78},
    {"title": "EEOC complaint process US 2026: workplace discrimination charge and right to sue letter", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 450, "comments": 102},
    {"title": "Wage theft laws US 2026: how to file a DOL complaint and recover unpaid wages", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 470, "comments": 112},
    {"title": "Independent contractor vs employee misclassification US: IRS 20-factor test and damages", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 430, "comments": 95},
    {"title": "Severance package negotiation US 2026: what is standard and what can you push back on", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 415, "comments": 88},

    # ── USA — Immigration ──
    {"title": "Green card through employer sponsorship US 2026: PERM labor certification step by step", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 540, "comments": 148},
    {"title": "OPT to H-1B cap-gap US 2026: maintaining status during lottery wait and what happens if rejected", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 510, "comments": 135},
    {"title": "L-1 visa for intracompany transfer US 2026: requirements, specialised knowledge and extensions", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 420, "comments": 88},
    {"title": "TN visa for Canadians and Mexicans US 2026: USMCA professions, process and renewal", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 395, "comments": 80},
    {"title": "Asylum application US 2026: affirmative vs defensive asylum and what happens at interview", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 475, "comments": 115},
    {"title": "DACA renewal US 2026: latest court status, eligibility and how to renew without a lawyer", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 490, "comments": 125},

    # ── USA — Consumer & Civil ──
    {"title": "Small claims court US 2026: state limits, how to sue without a lawyer and collection", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 450, "comments": 100},
    {"title": "FDCPA debt collector harassment US: illegal practices and how to sue for $1,000 per violation", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 420, "comments": 90},
    {"title": "FCRA credit report dispute US 2026: how to remove errors and sue credit bureaus", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 440, "comments": 98},
    {"title": "Class action lawsuit participation US: what it means for your rights and individual claims", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 380, "comments": 72},
    {"title": "Slip and fall personal injury claim US 2026: premises liability elements and settlement process", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 410, "comments": 85},

    # ── USA — Family & Estate ──
    {"title": "Divorce with prenuptial agreement US 2026: enforceability and how courts review them", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 460, "comments": 108},
    {"title": "Alimony calculation US 2026: factors judges consider and how long payments last", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 430, "comments": 95},
    {"title": "Adoption law US 2026: stepparent adoption process and terminating parental rights", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 390, "comments": 78},
    {"title": "Living trust vs will US 2026: which is better for avoiding probate and protecting assets", "country": "USA", "subreddit": "r/legaladvice", "upvotes": 470, "comments": 115},

    # ── Germany — Employment ──
    {"title": "Kurzarbeit short-time work Germany 2026: employer obligations and employee rights", "country": "Germany", "subreddit": "r/germany", "upvotes": 360, "comments": 65},
    {"title": "Termination protection Germany KSchG: who is protected, notice periods and severance", "country": "Germany", "subreddit": "r/germany", "upvotes": 395, "comments": 78},
    {"title": "Works council rights Germany Betriebsrat: co-determination and dismissal approval 2026", "country": "Germany", "subreddit": "r/germany", "upvotes": 340, "comments": 58},
    {"title": "Elternzeit parental leave Germany 2026: duration, Elterngeld amount and employer duties", "country": "Germany", "subreddit": "r/germany", "upvotes": 420, "comments": 90},
    {"title": "German employment contract review: mandatory clauses and common traps for foreigners 2026", "country": "Germany", "subreddit": "r/germany", "upvotes": 380, "comments": 72},
    {"title": "Unfair dismissal Germany: Abfindung severance calculation and Arbeitsgericht filing", "country": "Germany", "subreddit": "r/germany", "upvotes": 410, "comments": 85},

    # ── Germany — Tenancy ──
    {"title": "Mietpreisbremse rent brake Germany 2026: how to use it and claim back excess rent", "country": "Germany", "subreddit": "r/germany", "upvotes": 445, "comments": 100},
    {"title": "Eigenbedarfskündigung Germany: landlord personal use eviction rights and tenant protections", "country": "Germany", "subreddit": "r/germany", "upvotes": 420, "comments": 88},
    {"title": "Kaution deposit rules Germany 2026: maximum amount, return timeline and withholding disputes", "country": "Germany", "subreddit": "r/germany", "upvotes": 390, "comments": 80},
    {"title": "Nebenkostenabrechnung utility billing dispute Germany: how to check and challenge errors", "country": "Germany", "subreddit": "r/germany", "upvotes": 365, "comments": 68},
    {"title": "Mietminderung rent reduction Germany: defects that qualify and how to calculate the amount", "country": "Germany", "subreddit": "r/germany", "upvotes": 405, "comments": 84},

    # ── Germany — Immigration & Taxation ──
    {"title": "Blue Card Germany 2026: salary threshold, job requirements and permanent residency path", "country": "Germany", "subreddit": "r/germany", "upvotes": 480, "comments": 118},
    {"title": "Niederlassungserlaubnis permanent residency Germany 2026: 5-year vs 3-year route explained", "country": "Germany", "subreddit": "r/germany", "upvotes": 450, "comments": 105},
    {"title": "Steuererklärung German tax return 2026: ELSTER submission, deductions and refund timeline", "country": "Germany", "subreddit": "r/germany", "upvotes": 420, "comments": 90},
    {"title": "Kirchensteuer church tax Germany: how to leave church officially and stop the deduction", "country": "Germany", "subreddit": "r/germany", "upvotes": 380, "comments": 72},
    {"title": "Self-employed Freiberufler vs Gewerbe Germany: tax differences and registration process 2026", "country": "Germany", "subreddit": "r/germany", "upvotes": 395, "comments": 80},

    # ── Australia — Employment ──
    {"title": "Unfair dismissal claim Australia Fair Work 2026: 21-day deadline, process and compensation cap", "country": "Australia", "subreddit": "r/auslaw", "upvotes": 450, "comments": 102},
    {"title": "General protections claim Australia: adverse action, dismissal and underpayment rights 2026", "country": "Australia", "subreddit": "r/auslaw", "upvotes": 420, "comments": 90},
    {"title": "Long service leave Australia 2026: state-by-state entitlements and how to claim on termination", "country": "Australia", "subreddit": "r/auslaw", "upvotes": 385, "comments": 75},
    {"title": "Superannuation underpayment recovery Australia 2026: ATO complaint process and employer penalties", "country": "Australia", "subreddit": "r/auslaw", "upvotes": 440, "comments": 98},
    {"title": "Sham contracting Australia 2026: how Fair Work detects it and worker rights to back-pay", "country": "Australia", "subreddit": "r/auslaw", "upvotes": 370, "comments": 68},
    {"title": "Redundancy pay calculation Australia 2026: service years, Small Business Fair Dismissal Code", "country": "Australia", "subreddit": "r/auslaw", "upvotes": 410, "comments": 85},

    # ── Australia — Tenancy ──
    {"title": "Residential tenancy dispute NSW 2026: NCAT application process, fees and what orders you can get", "country": "Australia", "subreddit": "r/auslaw", "upvotes": 395, "comments": 78},
    {"title": "Bond refusal dispute Victoria VCAT 2026: how to apply and evidence you need to win", "country": "Australia", "subreddit": "r/auslaw", "upvotes": 365, "comments": 65},
    {"title": "Queensland rental reforms 2026: minimum housing standards and tenant remedy orders", "country": "Australia", "subreddit": "r/auslaw", "upvotes": 375, "comments": 70},
    {"title": "Landlord entry rules Australia 2026: notice required, tenant refusal and breach penalties", "country": "Australia", "subreddit": "r/auslaw", "upvotes": 350, "comments": 62},
    {"title": "Pet approval in Australian rentals 2026: state laws on landlord refusal and bonds", "country": "Australia", "subreddit": "r/auslaw", "upvotes": 430, "comments": 95},

    # ── Australia — Consumer & Migration ──
    {"title": "Australian Consumer Law guarantee claims 2026: major failure vs minor failure remedies", "country": "Australia", "subreddit": "r/auslaw", "upvotes": 440, "comments": 100},
    {"title": "Skilled migration subclass 482 visa Australia 2026: sponsor obligations and pathway to PR", "country": "Australia", "subreddit": "r/auslaw", "upvotes": 480, "comments": 118},
    {"title": "Partner visa Australia 2026: stage 1 and stage 2, evidence requirements and waiting times", "country": "Australia", "subreddit": "r/auslaw", "upvotes": 510, "comments": 135},
    {"title": "Student visa to permanent residency Australia 2026: best pathways and state nomination", "country": "Australia", "subreddit": "r/auslaw", "upvotes": 490, "comments": 125},
    {"title": "Character test visa cancellation Australia: section 501 grounds and merits review", "country": "Australia", "subreddit": "r/auslaw", "upvotes": 400, "comments": 82},

    # ── Canada — Employment & Family ──
    {"title": "Employment insurance EI Canada 2026: eligibility, insurable hours and how to apply", "country": "Canada", "subreddit": "r/legaladvice", "upvotes": 440, "comments": 98},
    {"title": "Wrongful dismissal Canada 2026: reasonable notice period calculation and severance claims", "country": "Canada", "subreddit": "r/legaladvice", "upvotes": 460, "comments": 108},
    {"title": "Human rights complaint Canada 2026: provincial vs federal tribunal process and remedies", "country": "Canada", "subreddit": "r/legaladvice", "upvotes": 415, "comments": 88},
    {"title": "Spousal support Canada 2026: SSAG calculation, variation and enforcement in provinces", "country": "Canada", "subreddit": "r/legaladvice", "upvotes": 390, "comments": 78},
    {"title": "Division of property on separation Canada 2026: net family property equalization explained", "country": "Canada", "subreddit": "r/legaladvice", "upvotes": 410, "comments": 85},

    # ── Canada — Immigration ──
    {"title": "Express Entry CRS score boost Canada 2026: provincial nomination and job offer points", "country": "Canada", "subreddit": "r/legaladvice", "upvotes": 530, "comments": 145},
    {"title": "PGWP post-graduation work permit Canada 2026: eligibility, duration and renewal", "country": "Canada", "subreddit": "r/legaladvice", "upvotes": 490, "comments": 125},
    {"title": "LMIA process Canada 2026: employer advertising requirements and refused application appeal", "country": "Canada", "subreddit": "r/legaladvice", "upvotes": 415, "comments": 88},
    {"title": "Spousal sponsorship Canada 2026: minimum income requirement, processing time and refusal", "country": "Canada", "subreddit": "r/legaladvice", "upvotes": 480, "comments": 118},
    {"title": "Canadian citizenship test 2026: eligibility, physical presence days and oath ceremony", "country": "Canada", "subreddit": "r/legaladvice", "upvotes": 450, "comments": 102},

    # ── Singapore — Employment & Corporate ──
    {"title": "Tripartite grievance resolution Singapore 2026: how to file MOM complaint for unfair dismissal", "country": "Singapore", "subreddit": "r/legaladvice", "upvotes": 380, "comments": 72},
    {"title": "Employment Act coverage Singapore 2026: which employees qualify and key entitlements", "country": "Singapore", "subreddit": "r/legaladvice", "upvotes": 360, "comments": 65},
    {"title": "CPF contributions Singapore 2026: employer and employee rates, voluntary top-ups and schemes", "country": "Singapore", "subreddit": "r/legaladvice", "upvotes": 415, "comments": 88},
    {"title": "Starting a company in Singapore 2026: Pte Ltd registration, ACRA process and costs", "country": "Singapore", "subreddit": "r/legaladvice", "upvotes": 440, "comments": 98},
    {"title": "Employment pass EP Singapore 2026: COMPASS framework, salary criteria and rejection appeal", "country": "Singapore", "subreddit": "r/legaladvice", "upvotes": 500, "comments": 130},
    {"title": "Restraint of trade clause Singapore: enforceability test and how courts assess reasonableness", "country": "Singapore", "subreddit": "r/legaladvice", "upvotes": 355, "comments": 63},

    # ── Singapore — Tenancy & Consumer ──
    {"title": "HDB subletting rules Singapore 2026: who can sublet, approval process and penalties", "country": "Singapore", "subreddit": "r/legaladvice", "upvotes": 390, "comments": 78},
    {"title": "Security deposit dispute Singapore tenancy 2026: small claims tribunal application process", "country": "Singapore", "subreddit": "r/legaladvice", "upvotes": 365, "comments": 67},
    {"title": "PDPA personal data breach Singapore 2026: reporting obligations and PDPC enforcement", "country": "Singapore", "subreddit": "r/legaladvice", "upvotes": 400, "comments": 82},
    {"title": "Online purchase dispute Singapore consumer rights 2026: Consumers Association and chargebacks", "country": "Singapore", "subreddit": "r/legaladvice", "upvotes": 375, "comments": 70},
]


def _get_curated_topics(published_slugs: set | None = None) -> list[dict]:
    """
    Return fresh curated legal topics, skipping any whose theme is already
    well-represented in the published_slugs cache.
    """
    import random

    available = list(_CURATED_TOPICS)

    if published_slugs:
        # Filter out topics that are already saturated in the slug cache
        fresh = [t for t in available if not _topic_already_covered(t["title"], published_slugs)]
        print(f"  Topic pool: {len(available)} total, {len(fresh)} fresh (not yet saturated)")
        if fresh:
            available = fresh
        else:
            print("  ⚠ All curated topics saturated — using full pool with random selection")

    random.shuffle(available)
    topics = available[:min(6, len(available))]  # candidate pool of 6, pipeline picks top 3

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
    path = f"src/content/blog/{slug}.md"

    if not GITHUB_TOKEN:
        print("  ⚠ No GITHUB_TOKEN — writing locally")
        _write_local(path, content)
        return False

    encoded  = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    api_url  = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
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
            _write_local(path, content)
            return False


def _write_local(path: str, content: str):
    full_path = os.path.join(".", path)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    with open(full_path, "w") as f:
        f.write(content)
    print(f"  ✓ Local write: {path}")


# ─── STEP 5: SUBMIT TO INDEXNOW ──────────────────────────────────────────────
async def submit_indexnow(slug: str) -> bool:
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

    if not GEMINI_API_KEY and not GROQ_API_KEY:
        print("❌ FATAL: Set GEMINI_API_KEY or GROQ_API_KEY in GitHub Secrets")
        return

    published_slugs = load_published()
    print(f"📚 Already published: {len(published_slugs)} articles\n")

    # Step 1: Fetch Reddit (falls back to curated topics with saturation check)
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

        if published_count < MAX_ARTICLES:
            await asyncio.sleep(5)

    print(f"\n{'='*55}")
    print(f"  Pipeline complete — {published_count} articles published")
    print(f"  Total published to date: {len(published_slugs)}")
    print("="*55 + "\n")


# Reddit is blocked in cloud/CI — patch fetch_reddit_posts to pass published_slugs
_original_fetch = fetch_reddit_posts

async def fetch_reddit_posts() -> list[dict]:
    posts = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

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
                        "title": d["title"], "country": country,
                        "subreddit": f"r/{sub}", "upvotes": d["ups"],
                        "comments": d["num_comments"], "score": round(score),
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

    print("\n  Reddit blocked — using curated legal topics as fallback")
    # Load published slugs for saturation check
    published_slugs = load_published()
    return _get_curated_topics(published_slugs)


if __name__ == "__main__":
    asyncio.run(main())

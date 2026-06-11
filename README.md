# LitigaForge Content Pipeline 🚀
### Reddit → AI Article → GitHub → Cloudflare Pages → IndexNow
### Total cost: ₹0/month

---

## How It Works

```
Every 2 hours (GitHub Actions cron):

1. Fetches hot posts from 8 legal subreddits
2. Scores posts by upvotes + comments
3. Gemini 1.5 Flash writes a 2000-word article
   (falls back to Groq Llama 3.3 if Gemini fails)
4. Commits markdown file to this GitHub repo
5. Cloudflare Pages auto-deploys in ~45 seconds
6. IndexNow submits URL to Bing + Yandex instantly
7. Google picks it up via sitemap within 24-48 hrs
```

---

## Setup — 4 Steps, ~30 minutes total

### Step 1 — Get Free API Keys (10 min)

| Key | Where | Free Limit |
|-----|-------|-----------|
| `GEMINI_API_KEY` | [aistudio.google.com](https://aistudio.google.com) | 1,500 req/day |
| `GROQ_API_KEY`   | [console.groq.com](https://console.groq.com)       | 14,400 req/day |
| `INDEXNOW_KEY`   | Any random 32-char string                           | Unlimited |

### Step 2 — Add GitHub Secrets (5 min)

Go to: `github.com/arif806-cyber/litigaforge-blog`
→ Settings → Secrets and variables → Actions → New repository secret

Add these secrets:
```
GEMINI_API_KEY   = AIza...
GROQ_API_KEY     = gsk_...
INDEXNOW_KEY     = abc123def456...  (your random key)
BLOG_DOMAIN      = litigaforge.com
```

`GITHUB_TOKEN` is automatic — no need to add it.

### Step 3 — Set Up IndexNow Key File (2 min)

1. Rename `public/indexnow-key.txt` to `public/{YOUR_KEY}.txt`
2. File content = just your key, nothing else
3. After deploy, verify: `https://litigaforge.com/{YOUR_KEY}.txt`
4. Update `INDEXNOW_KEY` in pipeline.py to match

### Step 4 — Connect Cloudflare Pages (10 min)

1. Go to [Cloudflare Dashboard](https://dash.cloudflare.com) → Pages
2. Create a project → Connect to GitHub → select this repo
3. Build settings:
   - Framework: Astro
   - Build command: `npm run build`
   - Output directory: `dist`
4. Add environment variable: `NODE_VERSION = 18`
5. Deploy — your blog is live!
6. Add custom domain: `litigaforge.com` or `blog.litigaforge.com`

---

## File Structure

```
litigaforge-blog/
├── .github/
│   └── workflows/
│       └── pipeline.yml        ← GitHub Actions cron
├── src/
│   ├── content/
│   │   ├── config.ts           ← Content collection schema
│   │   └── blog/               ← Auto-published .md files land here
│   └── pages/
│       └── blog/
│           ├── index.astro     ← Blog listing page
│           └── [slug].astro    ← Individual article pages
├── public/
│   └── {INDEXNOW_KEY}.txt      ← IndexNow verification file
├── pipeline.py                 ← Main automation script
├── published_slugs.json        ← Tracks published articles (auto-created)
├── astro.config.mjs            ← Astro + sitemap config
└── package.json
```

---

## Manual Run

Trigger the pipeline manually anytime:
1. Go to GitHub → Actions → LitigaForge Content Pipeline
2. Click "Run workflow"
3. Set max_articles (default: 3)
4. Click "Run workflow" button

---

## Monitoring

- **GitHub Actions tab** → see every run, logs, errors
- **Cloudflare Pages dashboard** → deploy status
- **published_slugs.json** → list of all published articles
- **litigaforge.com/blog** → live articles

---

## Scaling Up

To publish more articles per day, edit `pipeline.yml`:
```yaml
# Change cron to run every hour:
- cron: '0 * * * *'

# Change max articles per run:
MAX_ARTICLES: "5"
```

At 5 articles/run × 12 runs/day = **60 articles/day**
Still within GitHub free tier (2,000 min/month).

---

## Cost Breakdown

| Component | Cost |
|-----------|------|
| GitHub Actions | ₹0 (2,000 min/month free) |
| Gemini 1.5 Flash | ₹0 (1,500 req/day free) |
| Groq Llama 3.3 | ₹0 (14,400 req/day free) |
| Cloudflare Pages | ₹0 (unlimited deploys) |
| IndexNow | ₹0 (unlimited submissions) |
| **TOTAL** | **₹0/month** |

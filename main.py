"""
Flavour Founders Research Bot
- Daily quick-hit digest (7am)
- Weekly deep-dive digest (Monday 8am)
- Runs as always-on FastAPI server on Railway
- Output POSTed to webhook URL (Slack / Notion / email relay)
"""

import os
import json
import logging
import httpx
import anthropic

from datetime import datetime
from fastapi import FastAPI, BackgroundTasks
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config from environment ────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
OUTPUT_WEBHOOK_URL = os.environ["OUTPUT_WEBHOOK_URL"]   # Slack / Notion / email relay
MANUAL_TRIGGER_TOKEN = os.environ.get("MANUAL_TRIGGER_TOKEN", "")  # optional auth
APIFY_API_TOKEN = os.environ.get("APIFY_API_TOKEN", "")  # for Instagram scraping
RESEARCH_WEBHOOK_URL = os.environ.get("RESEARCH_WEBHOOK_URL", "")  # content bot ingest

# ── Creator handles to scrape ─────────────────────────────────────────────
CREATOR_HANDLES = [
    "thomas_straker",
    "rollboysldn",
    "sourdoughsophia",
    "cantoast.ldn",
    "jb_copeland",
]

# ── Load CLAUDE.md config ──────────────────────────────────────────────────
def load_config() -> dict:
    """Read CLAUDE.md and parse config sections."""
    config = {
        "brand_context": "",
        "competitors": [],
        "focus_topics": [],
        "target_audience": "",
        "tone": "professional but warm",
    }
    try:
        with open("CLAUDE.md", "r") as f:
            content = f.read()
        config["_raw"] = content
        log.info("CLAUDE.md loaded successfully")
    except FileNotFoundError:
        log.warning("CLAUDE.md not found — using defaults")
    return config

# ── Anthropic client ───────────────────────────────────────────────────────
client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


# ── Apify Instagram scraper ──────────────────────────────────────────────
async def scrape_instagram_creators() -> str:
    """Use Apify to scrape recent posts from watched creators."""
    if not APIFY_API_TOKEN:
        log.warning("No APIFY_API_TOKEN set — skipping Instagram scrape")
        return "No Instagram data available (Apify not configured)."

    import asyncio
    log.info(f"Apify token present: {bool(APIFY_API_TOKEN)} (length: {len(APIFY_API_TOKEN)})")
    all_posts = []

    try:
        log.info(f"Scraping {len(CREATOR_HANDLES)} Instagram creators via Apify...")
        async with httpx.AsyncClient(timeout=600) as http:

            # Step 1: Start the run
            start_resp = await http.post(
                "https://api.apify.com/v2/acts/apify~instagram-profile-scraper/runs",
                params={"token": APIFY_API_TOKEN},
                json={
                    "usernames": CREATOR_HANDLES,
                    "resultsLimit": 5,
                },
            )
            log.info(f"Apify start response: {start_resp.status_code}")

            if start_resp.status_code not in (200, 201):
                log.error(f"Apify start failed: {start_resp.status_code} - {start_resp.text[:500]}")
                return f"Instagram scrape failed to start (HTTP {start_resp.status_code})."

            run_data = start_resp.json().get("data", {})
            run_id = run_data.get("id")
            dataset_id = run_data.get("defaultDatasetId")
            log.info(f"Apify run started: run_id={run_id}, dataset_id={dataset_id}")

            if not run_id:
                return "Instagram scrape failed: no run ID returned."

            # Step 2: Poll until the run finishes (max 5 minutes)
            for attempt in range(30):
                await asyncio.sleep(10)
                status_resp = await http.get(
                    f"https://api.apify.com/v2/actor-runs/{run_id}",
                    params={"token": APIFY_API_TOKEN},
                )
                run_status = status_resp.json().get("data", {}).get("status")
                log.info(f"Apify run status (attempt {attempt+1}): {run_status}")

                if run_status == "SUCCEEDED":
                    break
                elif run_status in ("FAILED", "ABORTED", "TIMED-OUT"):
                    return f"Instagram scrape failed: run status {run_status}."
            else:
                return "Instagram scrape timed out waiting for Apify."

            # Step 3: Fetch the dataset
            dataset_resp = await http.get(
                f"https://api.apify.com/v2/datasets/{dataset_id}/items",
                params={"token": APIFY_API_TOKEN},
            )
            log.info(f"Apify dataset response: {dataset_resp.status_code}")

            if dataset_resp.status_code != 200:
                return f"Instagram scrape failed to fetch results (HTTP {dataset_resp.status_code})."

            data = dataset_resp.json()
            log.info(f"Apify returned {len(data)} profiles")

            if not data:
                return "Instagram scrape returned no data."

            for profile in data:
                username = profile.get("username", "unknown")
                followers = profile.get("followersCount", "?")
                posts = profile.get("latestPosts", [])

                creator_summary = f"\n@{username} ({followers} followers) — recent posts:"
                for post in posts[:5]:
                    caption = (post.get("caption") or "")[:150]
                    likes = post.get("likesCount", 0)
                    comments = post.get("commentsCount", 0)
                    post_type = post.get("type", "unknown")
                    creator_summary += f"\n  - [{post_type}] {likes} likes, {comments} comments: \"{caption}\""

                all_posts.append(creator_summary)

    except Exception as e:
        log.error(f"Apify scrape failed: {type(e).__name__}: {e}")
        return f"Instagram scrape failed: {type(e).__name__}: {e}"

    if not all_posts:
        return "Instagram scrape returned no data."

    result = "REAL INSTAGRAM DATA FROM WATCHED CREATORS:\n" + "\n".join(all_posts)
    log.info(f"Instagram scrape complete: {len(all_posts)} creators scraped")
    return result

# ── Research prompts ───────────────────────────────────────────────────────
BRAND_CONTEXT = """
WHO YOU ARE RESEARCHING FOR:
A UK food entrepreneur who has built TWO 7-figure food businesses (bakery/café, multi-site).
Currently scaling a group with a restaurant launching. Real experience, hard lessons, no theory.

PERSONAL BRAND PILLARS:
1. Food & Drink Business (PRIMARY) — bakery/café growth, profit, margins, labour, systems, scaling
2. Care Less (SECONDARY) — life perspective, freedom, time, YOLO, not overvaluing seriousness

TARGET AUDIENCE: Bakery, cafe, coffee shop, and restaurant owners/founders. People running or starting their own food business.

CONTENT STYLE REFERENCE — @mrfourtoeight (Blake Rocha):
This is the #1 brand inspiration. Study his approach:
- Personal transformation storytelling — "broke to multi-millionaire", real journey, real struggles
- Faith + wealth narrative — purpose-driven entrepreneurship, not just money
- Raw, honest, direct — zero corporate polish, zero guru energy
- Storytelling posts massively outperform advice posts (19K vs 500 likes)
- Conversation starters that pull engagement ("Do you agree? 👇")
- Mixes personal story with practical insight — never one without the other
When researching, always filter through: "Would this fit the @mrfourtoeight style of content — personal, story-driven, faith-to-wealth, real?"

YOUR JOB: Research and intelligence ONLY. Do NOT generate reel ideas, hooks, or content briefs — a separate content bot handles that.
Focus on ENTREPRENEURSHIP in food & drink — not macro industry reports, alt-protein regulation, or manufacturing stats. Everything must be relevant to someone running a small-to-medium food business.
"""


def build_daily_prompt(config: dict, instagram_data: str = "") -> str:
    competitors = config.get("_raw", "")
    today = datetime.now().strftime("%A %d %B %Y")
    return f"""You are a research intelligence assistant for a UK food entrepreneur personal brand called Flavour Founders.

{BRAND_CONTEXT}

Brand config:
{competitors}

{instagram_data}

Today is {today}.

Run a DAILY RESEARCH digest. Use web search for news. Use the REAL INSTAGRAM DATA above for creator intelligence.

Output EXACTLY in this format — nothing else:

---
⚡ DAILY RESEARCH DIGEST — {datetime.now().strftime("%d %b %Y")}

📰 ONE THING HAPPENING TODAY
[Single most relevant thing for someone running a bakery, cafe, coffee shop, or restaurant. NOT macro industry news — find real stories about food entrepreneurs, small operators, or trends that directly affect someone running their own food business. One sentence on what happened. One sentence on why it matters.]

🔍 CREATOR INTELLIGENCE
Based on the real Instagram data above, analyse what the watched creators are doing:

📊 What's hitting — [Which posts got the highest engagement? What topics/formats performed best? Include actual numbers.]

🔁 Pattern winning right now — [What content pattern or format is consistently outperforming across these creators? Be specific — e.g. "behind-the-scenes kitchen footage", "text-led controversy hooks", "personal storytelling carousels".]

🕳️ The gap — [What are NONE of these creators talking about that Flavour Founders could own? What topic or angle is missing from their content?]

🎬 Steal this — [One specific thing a creator did this week that worked really well. What was it, why did it work, and how could Flavour Founders adapt it?]

---

RULES:
- This is RESEARCH ONLY. Do NOT generate reel ideas, hooks, or content briefs.
- Creator intelligence must reference ACTUAL posts from the Instagram data — not guesses.
- Keep the whole digest under 250 words.
- No motivational fluff. Facts, data, insight only."""


def build_weekly_prompt(config: dict, instagram_data: str = "") -> str:
    competitors = config.get("_raw", "")
    week_start = datetime.now().strftime("%d %b %Y")
    return f"""You are a research intelligence assistant for a UK food entrepreneur personal brand called Flavour Founders.

{BRAND_CONTEXT}

Brand config:
{competitors}

{instagram_data}

Week of {week_start}.

Run a WEEKLY DEEP-DIVE research report. Use web search extensively for market intel. Use the REAL INSTAGRAM DATA above for creator analysis.

Output EXACTLY in this format — nothing else:

---
📋 WEEKLY RESEARCH DIGEST — w/c {week_start}

📰 3 THINGS RELEVANT TO BAKERY/CAFE/RESTAURANT OWNERS THIS WEEK
Focus on entrepreneurship in food & drink — NOT macro industry news. Look for:
- Real stories of bakery/cafe/coffee shop/restaurant owners (openings, closings, scaling, struggles, wins)
- Trends affecting small food business operators (labour, costs, margins, footfall, delivery, social media)
- Anything a bakery owner scrolling Instagram would think "that's me"
- [Insight 1 — what happened + why it matters to someone running a bakery/cafe/restaurant]
- [Insight 2]
- [Insight 3]

🔍 CREATOR INTELLIGENCE (WEEKLY DEEP DIVE)
Based on the real Instagram data above:

📊 What's hitting — [Across all watched creators this week — which posts got the highest engagement? What topics dominate? Include actual numbers and compare across creators.]

🔁 Pattern winning right now — [What content format or approach is consistently outperforming? Has anything shifted from last week? Be specific.]

🕳️ The gap — [What are NONE of these creators covering that Flavour Founders could own? What conversations are happening in the industry that no one is making content about?]

🎬 Steal this — [2-3 specific things creators did this week that worked. For each: what was it, why did it work, how could Flavour Founders adapt it?]

📈 STRATEGIC DIRECTION
[Based on all research above — what should Flavour Founders focus on this week and why? One clear recommendation backed by the data.]

---

RULES:
- This is RESEARCH ONLY. Do NOT generate reel ideas, hooks, captions, or content briefs.
- Creator intelligence must reference ACTUAL posts from the Instagram data — not guesses.
- Every insight must be backed by data or specific observations.
- Max 400 words total."""


# ── Core research runner ───────────────────────────────────────────────────
async def run_research(digest_type: str) -> str:
    """Call Claude with web search to generate a research digest."""
    config = load_config()

    # Scrape real Instagram data before building the prompt
    instagram_data = await scrape_instagram_creators()

    prompt = (
        build_daily_prompt(config, instagram_data)
        if digest_type == "daily"
        else build_weekly_prompt(config, instagram_data)
    )

    log.info(f"Running {digest_type} research digest...")

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2000,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        messages=[{"role": "user", "content": prompt}]
    )

    # Extract all text blocks from response (may include tool use blocks)
    output_parts = []
    for block in response.content:
        if block.type == "text":
            output_parts.append(block.text)

    result = "\n\n".join(output_parts) if output_parts else "No output generated."
    log.info(f"{digest_type} digest complete ({len(result)} chars)")
    return result


# ── Webhook delivery ───────────────────────────────────────────────────────
async def deliver_digest(digest_type: str, content: str):
    """POST the digest to the configured output webhook."""
    payload = {
        "digest_type": digest_type,
        "generated_at": datetime.now().isoformat(),
        "content": content,
        # Slack-compatible text field
        "text": f"*Flavour Founders Research Bot*\n{content}"
    }

    async with httpx.AsyncClient(timeout=30) as http:
        try:
            resp = await http.post(OUTPUT_WEBHOOK_URL, json=payload)
            resp.raise_for_status()
            log.info(f"Digest delivered to Slack → {resp.status_code}")
        except Exception as e:
            log.error(f"Slack webhook delivery failed: {e}")

        # Forward to content bot
        if RESEARCH_WEBHOOK_URL:
            try:
                resp2 = await http.post(RESEARCH_WEBHOOK_URL, json=payload)
                resp2.raise_for_status()
                log.info(f"Digest delivered to content bot → {resp2.status_code}")
            except Exception as e:
                log.error(f"Content bot delivery failed: {e}")


# ── Scheduled jobs ─────────────────────────────────────────────────────────
async def daily_job():
    log.info("⏰ Daily digest triggered")
    content = await run_research("daily")
    await deliver_digest("daily", content)


async def weekly_job():
    log.info("⏰ Weekly digest triggered")
    content = await run_research("weekly")
    await deliver_digest("weekly", content)


# ── FastAPI app ────────────────────────────────────────────────────────────
app = FastAPI(title="Flavour Founders Research Bot")
scheduler = AsyncIOScheduler(timezone="Europe/London")


@app.on_event("startup")
async def startup():
    # Daily quick hits — every day at 7:00am London time
    scheduler.add_job(daily_job, CronTrigger(hour=7, minute=0))
    # Weekly deep dive — every Monday at 8:00am London time
    scheduler.add_job(weekly_job, CronTrigger(day_of_week="mon", hour=8, minute=0))
    scheduler.start()
    log.info("Scheduler started — daily 07:00, weekly Mon 08:00 (Europe/London)")


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


@app.get("/")
async def health():
    jobs = [
        {"id": job.id, "next_run": str(job.next_run_time)}
        for job in scheduler.get_jobs()
    ]
    return {"status": "running", "bot": "Flavour Founders Research Bot", "scheduled_jobs": jobs}


@app.get("/debug/scrape")
async def debug_scrape(token: str = ""):
    """Test the Apify scraper independently."""
    if MANUAL_TRIGGER_TOKEN and token != MANUAL_TRIGGER_TOKEN:
        return {"error": "Unauthorised"}
    result = await scrape_instagram_creators()
    return {"apify_token_set": bool(APIFY_API_TOKEN), "result_length": len(result), "preview": result[:1000]}


@app.post("/trigger/{digest_type}")
async def manual_trigger(digest_type: str, background_tasks: BackgroundTasks, token: str = ""):
    """Manually trigger a digest. digest_type = 'daily' or 'weekly'."""
    if MANUAL_TRIGGER_TOKEN and token != MANUAL_TRIGGER_TOKEN:
        return {"error": "Unauthorised"}, 401

    if digest_type not in ("daily", "weekly"):
        return {"error": "digest_type must be 'daily' or 'weekly'"}

    async def _run():
        content = await run_research(digest_type)
        await deliver_digest(digest_type, content)

    background_tasks.add_task(_run)
    return {"status": "triggered", "digest_type": digest_type}

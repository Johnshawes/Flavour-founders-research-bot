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

# ── Research prompts ───────────────────────────────────────────────────────
def build_daily_prompt(config: dict) -> str:
    raw = config.get("_raw", "No config loaded.")
    return f"""You are a research assistant for Flavour Founders, a UK food and hospitality personal brand.

Brand config:
{raw}

Today is {datetime.now().strftime("%A %d %B %Y")}.

Run a DAILY QUICK-HIT research digest. Use web search to find TODAY'S most relevant information.

Structure your output EXACTLY like this (markdown):

## ⚡ Daily Quick Hits — {datetime.now().strftime("%d %b %Y")}

### 🔥 Trending Now (Food & Hospitality UK)
- 3–5 bullet points on what's trending today in UK food, hospitality, or creator/personal brand space

### 👀 Competitor Pulse
- 2–3 bullet points: anything notable from competitors or similar brands (new content, launches, campaigns)

### 💡 Content Idea of the Day
- 1 strong content idea for Flavour Founders based on what's trending
- Include: suggested format (Reel / carousel / story), hook line, why it's timely

### 📊 Audience Insight
- 1 insight about the Flavour Founders target audience — behaviour, sentiment, or conversation happening right now

Keep it punchy. Max 300 words total. Every point must be actionable or immediately useful."""


def build_weekly_prompt(config: dict) -> str:
    raw = config.get("_raw", "No config loaded.")
    week_start = datetime.now().strftime("%d %b %Y")
    return f"""You are a research assistant for Flavour Founders, a UK food and hospitality personal brand.

Brand config:
{raw}

Week of {week_start}.

Run a WEEKLY DEEP-DIVE research report. Use web search extensively to build a comprehensive picture.

Structure your output EXACTLY like this (markdown):

## 📋 Weekly Deep Dive — w/c {week_start}

### 🏆 Market & Industry Overview
- 4–6 bullet points on key UK food & hospitality market developments this week
- Include any data, statistics, or notable news

### 🕵️ Competitor Analysis
- For each major competitor/similar brand: what content performed well, any new moves, gaps you spotted
- Identify 1–2 opportunities Flavour Founders could capitalise on

### 📈 Trending Topics & Hashtags
- Top 5 topics/hashtags in the food creator space right now
- For each: why it's trending + how Flavour Founders could authentically join the conversation

### 🎯 Audience Deep Dive
- What is the Flavour Founders target audience talking about, worried about, excited by this week?
- Any shifts in sentiment or behaviour worth noting?

### 📅 Content Brief — Next 7 Days
Provide 5 content ideas for the coming week:
For each idea include:
- **Format:** (Reel / Carousel / Story / Static post)
- **Hook:** (opening line or visual concept)
- **Topic:** (what it covers)
- **Why now:** (why this week specifically)

### 🔮 One Big Opportunity
- The single biggest strategic opportunity for Flavour Founders right now, based on everything above
- Be specific and actionable

Max 600 words. Every section must contain fresh, this-week intelligence — not generic advice."""


# ── Core research runner ───────────────────────────────────────────────────
async def run_research(digest_type: str) -> str:
    """Call Claude with web search to generate a research digest."""
    config = load_config()

    prompt = (
        build_daily_prompt(config)
        if digest_type == "daily"
        else build_weekly_prompt(config)
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
            log.info(f"Digest delivered → {resp.status_code}")
        except Exception as e:
            log.error(f"Webhook delivery failed: {e}")


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

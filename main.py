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

    all_posts = []

    async with httpx.AsyncClient(timeout=120) as http:
        for handle in CREATOR_HANDLES:
            try:
                log.info(f"Scraping Instagram: @{handle}")

                # Run the Instagram Profile Scraper actor synchronously
                run_resp = await http.post(
                    "https://api.apify.com/v2/acts/apify~instagram-profile-scraper/run-sync-get-dataset-items",
                    params={"token": APIFY_API_TOKEN},
                    json={
                        "usernames": [handle],
                        "resultsLimit": 5,
                    },
                    timeout=120,
                )

                if run_resp.status_code != 200:
                    log.error(f"Apify error for @{handle}: {run_resp.status_code} - {run_resp.text[:200]}")
                    continue

                data = run_resp.json()
                if not data:
                    log.info(f"No data returned for @{handle}")
                    continue

                # Extract profile and recent posts
                for profile in data:
                    username = profile.get("username", handle)
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
                log.error(f"Failed to scrape @{handle}: {type(e).__name__}: {e}")
                continue

    if not all_posts:
        return "Instagram scrape returned no data."

    result = "REAL INSTAGRAM DATA FROM WATCHED CREATORS:\n" + "\n".join(all_posts)
    log.info(f"Instagram scrape complete: {len(all_posts)} creators scraped")
    return result

# ── Research prompts ───────────────────────────────────────────────────────
BRAND_CONTEXT = """
WHO YOU ARE WRITING FOR:
A UK food entrepreneur who has built TWO 7-figure food businesses (bakery/café, multi-site).
Currently scaling a group with a restaurant launching. Real experience, hard lessons, no theory.

PERSONAL BRAND PILLARS:
1. Food & Drink Business (PRIMARY) — bakery/café growth, profit, margins, labour, systems, scaling
2. Care Less (SECONDARY) — life perspective, freedom, time, YOLO, not overvaluing seriousness

BRAND VOICE:
- Direct, honest, slightly confrontational, insight-led
- NO fluff, NO generic motivation, NO basic advice, NO surface-level content
- Always ask: "Would this make a bakery owner feel called out?" If not — it's not good enough.

REEL FORMAT (6–9 seconds, text-led, no talking):
- Hook style: "Why [positive]... but [negative reality]" — contradiction hooks perform best
- Best topics: profit, cash, margins, systems, time/freedom
- Structure: Hook → Curiosity line → 1–2 punchy insight lines

CONTENT ROTATION:
💰 Money (profit, margins, cash)
🧠 Control (systems, chaos, structure)
⏱ Time (freedom, burnout, stepping away)
🌍 Care Less (occasional — life perspective)
"""


def build_daily_prompt(config: dict, instagram_data: str = "") -> str:
    competitors = config.get("_raw", "")
    today = datetime.now().strftime("%A %d %B %Y")
    return f"""You are a high-level content strategist and research assistant for a UK food entrepreneur personal brand called Flavour Founders.

{BRAND_CONTEXT}

Competitor/watch list from config:
{competitors}

{instagram_data}

Today is {today}.

Run a DAILY INSPIRATION digest. Use web search to find what's happening TODAY that's relevant.

Output EXACTLY in this format — clean, no waffle:

---
⚡ DAILY DIGEST — {datetime.now().strftime("%d %b %Y")}

📰 ONE THING HAPPENING TODAY
[Single most relevant news item/trend for a UK food business owner. One sentence. Why it matters to them.]

🎬 3 REEL IDEAS FOR TODAY

IDEA 1 — [💰/🧠/⏱/🌍] [ANGLE NAME]
Hook: "Why [positive]... but [negative reality]"
Angle: [What the reel reveals — one sharp sentence]
Why it works: [Why a bakery owner will feel called out]

IDEA 2 — [💰/🧠/⏱/🌍] [ANGLE NAME]
Hook: "Why [positive]... but [negative reality]"
Angle: [What the reel reveals — one sharp sentence]
Why it works: [Why a bakery owner will feel called out]

IDEA 3 — [💰/🧠/⏱/🌍] [ANGLE NAME]
Hook: "Why [positive]... but [negative reality]"
Angle: [What the reel reveals — one sharp sentence]
Why it works: [Why a bakery owner will feel called out]

👀 CREATOR WATCH
[Based on the REAL INSTAGRAM DATA above — what are the watched creators posting? What's getting engagement? Any formats or hooks worth stealing? 2-3 lines max.]

---

RULES:
- No generic ideas. Every reel idea must feel personal to a food business owner.
- Hooks must follow the contradiction format — no exceptions.
- Keep the whole digest under 250 words.
- No motivational fluff. Facts, tension, insight only."""


def build_weekly_prompt(config: dict, instagram_data: str = "") -> str:
    competitors = config.get("_raw", "")
    week_start = datetime.now().strftime("%d %b %Y")
    return f"""You are a high-level content strategist and research assistant for a UK food entrepreneur personal brand called Flavour Founders.

{BRAND_CONTEXT}

Competitor/watch list from config:
{competitors}

{instagram_data}

Week of {week_start}.

Run a WEEKLY STRATEGIC DIGEST. Use web search extensively. Every insight must be actionable.

Output EXACTLY in this format:

---
📋 WEEKLY STRATEGY DIGEST — w/c {week_start}

🏆 3 THINGS SHAPING UK FOOD BUSINESS THIS WEEK
- [Insight 1 — one punchy line + why it matters to a food entrepreneur]
- [Insight 2]
- [Insight 3]

🕵️ CREATOR MOVES (based on REAL Instagram data above)
[Analyse the actual posts from watched creators. What content is getting the most engagement? What formats are working? What hooks are they using? What gaps are they missing that Flavour Founders can own?]

📈 WHAT TO DOUBLE DOWN ON THIS WEEK
[Based on trends — which of the 3 content angles (Money / Control / Time) has the most momentum right now and why]

📅 5 REEL IDEAS FOR THE WEEK

IDEA 1 — [💰/🧠/⏱/🌍] [ANGLE]
Hook: "Why [positive]... but [negative reality]"
Angle: [Sharp one-liner]
Why now: [What makes this week specifically the right time]

IDEA 2 — [💰/🧠/⏱/🌍] [ANGLE]
Hook: "Why [positive]... but [negative reality]"
Angle: [Sharp one-liner]
Why now: [Timing rationale]

IDEA 3 — [💰/🧠/⏱/🌍] [ANGLE]
Hook: "Why [positive]... but [negative reality]"
Angle: [Sharp one-liner]
Why now: [Timing rationale]

IDEA 4 — [💰/🧠/⏱/🌍] [ANGLE]
Hook: "Why [positive]... but [negative reality]"
Angle: [Sharp one-liner]
Why now: [Timing rationale]

IDEA 5 — [💰/🧠/⏱/🌍] [ANGLE]
Hook: "Why [positive]... but [negative reality]"
Angle: [Sharp one-liner]
Why now: [Timing rationale]

🔮 THE ONE BIG OPPORTUNITY THIS WEEK
[Single most important strategic move for Flavour Founders right now. Specific. Actionable. No fluff.]

---

RULES:
- Every reel idea must make a bakery/café owner feel called out.
- All hooks must follow the contradiction format.
- No generic content strategy advice — this must be specific to food entrepreneurship.
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

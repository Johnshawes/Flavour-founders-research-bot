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
import hashlib
from pathlib import Path
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

# ── Ideas store (received from Slack #idea channel via workflow webhook) ──
ideas_store: list[dict] = []  # {"text": "...", "received_at": "..."}


def drain_ideas() -> str:
    """Pop all pending ideas and return as formatted string. Clears the store."""
    if not ideas_store:
        return ""
    lines = []
    for i, idea in enumerate(ideas_store, 1):
        lines.append(f"{i}. {idea['text']}  (submitted {idea['received_at'][:16]})")
    ideas_store.clear()
    return "\n".join(lines)

# ── Your handle (for daily reel review) ───────────────────────────────────
OWN_HANDLE = "john_s_hawes"

# ── Creator handles to scrape ─────────────────────────────────────────────
CREATOR_HANDLES = [
    "thomas_straker",
    "rollboysldn",
    "sourdoughsophia",
    "cantoast.ldn",
    "jb_copeland",
    "mrfourtoeight",
]

# ── Discovery pool (weekly rotation — profile scraper) ────────────────────
# Food/bakery/hospitality creators to monitor for trends + inspiration.
# Rotated weekly — each week scrapes a different batch of 5.
# Add or remove names as you like — no code changes needed elsewhere.
DISCOVERY_POOL = [
    # Bakery / bread creators
    "breadaheadbakery",       # ~424K — London sourdough & education
    "lilsbakerybrighton",     # Brighton bakery brand
    "breadbybike",            # ~12K — indie bakery
    "dustyknucklebakery",     # popular bakery brand
    "jolenerestaurant",       # bakery-restaurant hybrid
    # Cafe / coffee / hospitality
    "departmentofcoffee",     # coffee culture
    "watchhousecoffee",       # London specialty coffee
    "dishamorita",            # hospitality entrepreneur
    "maxhalley",              # sandwich king, food creator
    "ravneetgill",            # baker & food writer
    # Food business entrepreneurs
    "miguelbarclaysart",      # budget food creator
    "fitwaffle",              # baking creator
    "clembalfour",            # food business
    "thefoodboss_",           # food entrepreneur content
    "ellafreedman_",          # bakery / food business
]
DISCOVERY_MIN_FOLLOWERS = 10_000
DISCOVERY_MAX_FOLLOWERS = 500_000

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


# ── Apify helper: run actor, poll, return results ────────────────────────
async def _run_apify_actor(http, actor_id: str, input_json: dict, label: str) -> list:
    """Start an Apify actor, poll until done, return dataset items."""
    import asyncio

    start_resp = await http.post(
        f"https://api.apify.com/v2/acts/{actor_id}/runs",
        params={"token": APIFY_API_TOKEN},
        json=input_json,
    )
    log.info(f"[{label}] start: {start_resp.status_code}")

    if start_resp.status_code not in (200, 201):
        log.error(f"[{label}] start failed: {start_resp.status_code} - {start_resp.text[:500]}")
        return []

    run_data = start_resp.json().get("data", {})
    run_id = run_data.get("id")
    dataset_id = run_data.get("defaultDatasetId")
    log.info(f"[{label}] run_id={run_id}, dataset_id={dataset_id}")

    if not run_id:
        return []

    for attempt in range(48):
        await asyncio.sleep(10)
        status_resp = await http.get(
            f"https://api.apify.com/v2/actor-runs/{run_id}",
            params={"token": APIFY_API_TOKEN},
        )
        run_status = status_resp.json().get("data", {}).get("status")
        log.info(f"[{label}] status (attempt {attempt+1}): {run_status}")
        if run_status == "SUCCEEDED":
            break
        elif run_status in ("FAILED", "ABORTED", "TIMED-OUT"):
            log.error(f"[{label}] failed: {run_status}")
            return []
    else:
        log.error(f"[{label}] timed out")
        return []

    dataset_resp = await http.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items",
        params={"token": APIFY_API_TOKEN},
    )
    if dataset_resp.status_code != 200:
        log.error(f"[{label}] dataset fetch failed: {dataset_resp.status_code}")
        return []

    data = dataset_resp.json()
    log.info(f"[{label}] returned {len(data)} items")
    return data


# ── Format profile-only data (daily) ─────────────────────────────────────
def _format_profile_data(profile_data: list) -> str:
    """Light format — profile scraper data only (used for daily digest)."""
    sections = []
    for profile in profile_data:
        username = profile.get("username", "unknown")
        followers = profile.get("followersCount", "?")
        posts = profile.get("latestPosts", [])

        section = f"\n@{username} ({followers} followers) — recent posts:"
        for post in posts[:5]:
            caption = (post.get("caption") or "")[:150]
            likes = post.get("likesCount", 0)
            comments = post.get("commentsCount", 0)
            post_type = post.get("type", "unknown")
            section += f"\n  - [{post_type}] {likes} likes, {comments} comments: \"{caption}\""
        sections.append(section)

    if not sections:
        return "No Instagram profile data available."
    return "REAL INSTAGRAM DATA FROM WATCHED CREATORS:\n" + "\n".join(sections)


# ── Format own reels (daily review) ───────────────────────────────────────
def _format_own_reels(post_data: list) -> str:
    """Format John's own recent reels with full engagement data for daily review."""
    if not post_data:
        return "YOUR RECENT REELS:\n  No reel data available."

    from datetime import datetime, timedelta, timezone
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)

    # Filter: only posts from last 48 hours, exclude pinned
    recent = []
    for p in post_data:
        # Skip pinned posts
        if p.get("isPinned") or p.get("pinned"):
            continue
        # Check timestamp
        ts = p.get("timestamp") or p.get("takenAtTimestamp") or ""
        if ts:
            try:
                if isinstance(ts, (int, float)):
                    post_time = datetime.fromtimestamp(ts, tz=timezone.utc)
                else:
                    post_time = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                if post_time < cutoff:
                    continue
            except (ValueError, TypeError):
                pass  # Include if we can't parse the timestamp
        recent.append(p)

    if not recent:
        return "YOUR RECENT REELS:\n  No reels found from the last 7 days (pinned posts excluded)."

    # Sort by engagement
    for p in recent:
        p["_engagement"] = (p.get("likesCount", 0) or 0) + (p.get("commentsCount", 0) or 0)
    recent.sort(key=lambda p: p["_engagement"], reverse=True)

    result = "YOUR RECENT REELS (@john_s_hawes) — last 7 days, pinned excluded, sorted by engagement:\n"
    for p in recent[:5]:
        post_type = p.get("type", "unknown")
        likes = p.get("likesCount", 0) or 0
        comments = p.get("commentsCount", 0) or 0
        views = p.get("videoPlayCount") or p.get("videoViewCount") or p.get("playCount") or 0
        url = p.get("url") or ""
        full_caption = (p.get("caption") or "").strip()
        hook_line = full_caption.split("\n")[0][:100] if full_caption else ""

        result += f"\n  [{post_type.upper()}] {likes:,} likes | {comments:,} comments | {views:,} views"
        if url:
            result += f"\n  URL: {url}"
        result += f"\n  HOOK: \"{hook_line}\""
        result += f"\n  FULL CAPTION: \"{full_caption[:500]}\"\n"

    return result


# ── Format deep post data (weekly) ───────────────────────────────────────
def _format_deep_data(profile_data: list, post_data: list, discovery_data: list) -> str:
    """Rich format — profiles + detailed posts + discovered creators (weekly)."""

    # Build profile lookup
    profile_map = {}
    for profile in profile_data:
        username = profile.get("username", "unknown")
        profile_map[username] = {
            "followers": profile.get("followersCount", "?"),
            "bio": (profile.get("biography") or "")[:200],
        }

    # Group posts by creator
    posts_by_creator = {}
    for post in post_data:
        owner = post.get("ownerUsername") or post.get("owner", {}).get("username", "unknown")
        posts_by_creator.setdefault(owner, []).append(post)

    # ── Core creators section ────────────────────────────────────────────
    sections = []
    for handle in CREATOR_HANDLES:
        info = profile_map.get(handle, {})
        followers = info.get("followers", "?")
        bio = info.get("bio", "")

        section = f"\n@{handle} ({followers} followers)"
        if bio:
            section += f"\nBio: {bio}"

        creator_posts = posts_by_creator.get(handle, [])
        if not creator_posts:
            section += "\n  No detailed post data."
            sections.append(section)
            continue

        # Sort by engagement
        for p in creator_posts:
            p["_engagement"] = (p.get("likesCount", 0) or 0) + (p.get("commentsCount", 0) or 0)
        creator_posts.sort(key=lambda p: p["_engagement"], reverse=True)

        section += f"\n  Posts ({len(creator_posts)} scraped, sorted by engagement):"
        for p in creator_posts[:5]:
            post_type = p.get("type", "unknown")
            likes = p.get("likesCount", 0) or 0
            comments = p.get("commentsCount", 0) or 0
            views = p.get("videoPlayCount") or p.get("videoViewCount") or p.get("playCount") or 0
            url = p.get("url") or ""
            full_caption = (p.get("caption") or "").strip()
            hook_line = full_caption.split("\n")[0][:100] if full_caption else ""

            section += f"\n\n  [{post_type.upper()}] {likes:,} likes | {comments:,} comments | {views:,} views"
            if url:
                section += f"\n  URL: {url}"
            section += f"\n  HOOK: \"{hook_line}\""
            section += f"\n  FULL CAPTION: \"{full_caption[:500]}\""

        sections.append(section)

    result = "REAL INSTAGRAM DATA FROM WATCHED CREATORS (DETAILED):\n" + "\n".join(sections)

    # ── Discovery section (from profile scraper — rotated pool) ────────
    if discovery_data:
        log.info(f"Discovery returned {len(discovery_data)} profiles")
        existing = set(CREATOR_HANDLES)
        discovered = []
        for profile in discovery_data:
            username = profile.get("username", "unknown")
            if username in existing:
                continue
            followers = profile.get("followersCount", 0) or 0
            if not (DISCOVERY_MIN_FOLLOWERS <= followers <= DISCOVERY_MAX_FOLLOWERS):
                continue
            discovered.append(profile)

        # Sort by followers
        discovered.sort(key=lambda p: p.get("followersCount", 0) or 0, reverse=True)

        if discovered:
            result += "\n\n🔎 DISCOVERED CREATORS (from weekly rotation pool, 10K-500K followers):\n"
            for profile in discovered:
                username = profile.get("username", "unknown")
                followers = profile.get("followersCount", 0) or 0
                bio = (profile.get("biography") or "")[:200]
                posts = profile.get("latestPosts", [])

                result += f"\n  @{username} ({followers:,} followers)"
                if bio:
                    result += f"\n  Bio: {bio}"

                # Show top posts if available
                if posts:
                    # Sort by engagement
                    for p in posts:
                        p["_eng"] = (p.get("likesCount", 0) or 0) + (p.get("commentsCount", 0) or 0)
                    posts.sort(key=lambda p: p["_eng"], reverse=True)

                    for p in posts[:3]:
                        likes = p.get("likesCount", 0) or 0
                        comments = p.get("commentsCount", 0) or 0
                        post_type = p.get("type", "unknown")
                        caption = (p.get("caption") or "").strip()
                        hook_line = caption.split("\n")[0][:100] if caption else ""
                        result += f"\n    [{post_type.upper()}] {likes:,} likes | {comments:,} comments — \"{hook_line}\""
                result += "\n"

    return result


# ── Main scraper entry point ─────────────────────────────────────────────
async def scrape_instagram_creators(digest_type: str = "daily") -> str:
    """Scrape Instagram data. Daily = profile only. Weekly = profile + posts + discovery."""
    if not APIFY_API_TOKEN:
        log.warning("No APIFY_API_TOKEN set — skipping Instagram scrape")
        return "No Instagram data available (Apify not configured)."

    import asyncio
    log.info(f"Scraping creators for {digest_type} digest...")

    async with httpx.AsyncClient(timeout=600) as http:
        try:
            if digest_type == "daily":
                # Daily: profile scraper only (reel review runs separately at 8pm)
                profile_data = await _run_apify_actor(
                    http, "apify~instagram-profile-scraper",
                    {"usernames": CREATOR_HANDLES, "resultsLimit": 5},
                    "DailyProfiles",
                )
                return _format_profile_data(profile_data)

            else:
                # Weekly: profile + post + discovery scrapers in parallel
                profile_task = _run_apify_actor(
                    http, "apify~instagram-profile-scraper",
                    {"usernames": CREATOR_HANDLES, "resultsLimit": 10},
                    "WeeklyProfiles",
                )
                # Post scraper takes usernames, not URLs
                post_task = _run_apify_actor(
                    http, "apify~instagram-post-scraper",
                    {"username": CREATOR_HANDLES, "resultsLimit": 5},
                    "WeeklyPosts",
                )
                # Discovery: rotate through pool — 5 creators per week
                week_num = int(datetime.now().strftime("%U"))  # week of year
                batch_size = 5
                start = (week_num * batch_size) % len(DISCOVERY_POOL)
                discovery_batch = (DISCOVERY_POOL + DISCOVERY_POOL)[start:start + batch_size]
                log.info(f"Discovery batch (week {week_num}): {discovery_batch}")

                discovery_task = _run_apify_actor(
                    http, "apify~instagram-profile-scraper",
                    {"usernames": discovery_batch, "resultsLimit": 10},
                    "Discovery",
                )

                profile_data, post_data, discovery_data = await asyncio.gather(
                    profile_task, post_task, discovery_task
                )
                return _format_deep_data(profile_data, post_data, discovery_data)

        except Exception as e:
            log.error(f"Apify scrape failed: {type(e).__name__}: {e}")
            return f"Instagram scrape failed: {type(e).__name__}: {e}"

# ── Load shared founder profile ──────────────────────────────────────────────
_PROFILE_PATH = Path(__file__).parent / "founder-profile.txt"
try:
    _FOUNDER_PROFILE = _PROFILE_PATH.read_text(encoding="utf-8").strip()
except FileNotFoundError:
    _FOUNDER_PROFILE = "John Hawes — UK food and hospitality entrepreneur running KNEAD, Watermoor Meat Supply Ltd, and Flavour Founders."
    log.warning("founder-profile.txt not found — using fallback bio")
# ─────────────────────────────────────────────────────────────────────────────

# ── Research prompts ───────────────────────────────────────────────────────
BRAND_CONTEXT = f"""
WHO YOU ARE RESEARCHING FOR:
{_FOUNDER_PROFILE}

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


def build_daily_prompt(config: dict, instagram_data: str = "", ideas: str = "") -> str:
    competitors = config.get("_raw", "")
    today = datetime.now().strftime("%A %d %B %Y")

    ideas_block = ""
    if ideas.strip():
        ideas_block = f"""
JOHN'S IDEAS (dropped into #idea channel in the last 24 hours):
{ideas}

IMPORTANT: These are raw content ideas from John himself. They should be treated as HIGH PRIORITY.
- Research and validate each idea — find supporting data, trends, or angles that make it stronger.
- Include relevant creator intelligence or news that connects to these ideas.
- If an idea is strong, say so and explain why. If it needs sharpening, suggest how.
"""

    return f"""You are a research intelligence assistant for a UK food entrepreneur personal brand called Flavour Founders.

{BRAND_CONTEXT}

Brand config:
{competitors}

{instagram_data}

{ideas_block}

Today is {today}.

CRITICAL — RECENCY RULES:
- You MUST use web search before writing anything. Do NOT rely on your training data for news.
- Only cite news, stories, or events from the LAST 48 HOURS. Nothing older.
- If your web search returns nothing from the last 48 hours, say "No relevant news found today" — do NOT fall back to older stories.
- Include the source and date for every news item cited.

Run a DAILY RESEARCH digest. Use web search for news. Use the REAL INSTAGRAM DATA above for creator intelligence.

Output EXACTLY in this format — nothing else:

---
⚡ DAILY RESEARCH DIGEST — {datetime.now().strftime("%d %b %Y")}

📰 ONE THING HAPPENING TODAY
[One entrepreneurship lesson from today's news (last 48 hours ONLY) relevant to bakery/café/coffee shop/restaurant owners. Focus exclusively on scaling, profit, margins, or systems. No macro industry news, trade events, or wage statistics. One sentence on the lesson. One sentence on why it matters to food entrepreneurs. Include source and date.]

🔍 CREATOR INTELLIGENCE
Based on the real Instagram data above, analyse what the watched creators are doing:

📊 What's hitting — [Which posts got the highest engagement? What topics/formats performed best? Include actual numbers.]

🔁 Pattern winning right now — [What content pattern or format is consistently outperforming across these creators? Be specific — e.g. "behind-the-scenes kitchen footage", "text-led controversy hooks", "personal storytelling carousels".]

🕳️ The gap — [What are NONE of these creators talking about that Flavour Founders could own? What topic or angle is missing from their content?]

🎬 Steal this — [One specific thing a creator did this week that worked really well. What was it, why did it work, and how could Flavour Founders adapt it?]

💡 JOHN'S IDEAS — VALIDATED
[If John dropped ideas in the #idea channel, validate each one here. For each idea:
- The idea (as John wrote it)
- Supporting data or trend that backs it up
- Suggested angle or sharpening to make it hit harder
- Verdict: 🟢 Strong / 🟡 Needs work / 🔴 Skip
If no ideas were submitted, omit this section entirely.]

---

RULES:
- This is RESEARCH ONLY. Do NOT generate reel ideas, hooks, or content briefs.
- Creator intelligence must reference ACTUAL posts from the Instagram data — not guesses.
- Keep the whole digest under 250 words.
- No motivational fluff. Facts, data, insight only.
- NEVER cite news older than 48 hours. If nothing recent exists, say so."""


def build_weekly_prompt(config: dict, instagram_data: str = "") -> str:
    competitors = config.get("_raw", "")
    week_start = datetime.now().strftime("%d %b %Y")
    return f"""You are a research intelligence assistant for a UK food entrepreneur personal brand called Flavour Founders.

{BRAND_CONTEXT}

Brand config:
{competitors}

{instagram_data}

Week of {week_start}.

CRITICAL — RECENCY RULES:
- You MUST use web search extensively before writing anything. Do NOT rely on your training data for news.
- Only cite news, stories, or events from the LAST 7 DAYS. Nothing older.
- If your web search returns nothing from the last 7 days for a section, say "No relevant news found this week" — do NOT fall back to older stories.
- Include the source and date for every news item cited.

Run a WEEKLY DEEP-DIVE research report. Use web search extensively for market intel. Use the REAL INSTAGRAM DATA above for creator analysis.

Output EXACTLY in this format — nothing else:

---
📋 WEEKLY RESEARCH DIGEST — w/c {week_start}

📰 3 THINGS RELEVANT TO BAKERY/CAFE/RESTAURANT OWNERS THIS WEEK (last 7 days ONLY)
Focus on entrepreneurship in food & drink — NOT macro industry news. Look for:
- Real stories of bakery/cafe/coffee shop/restaurant owners (openings, closings, scaling, struggles, wins)
- Trends affecting small food business operators (labour, costs, margins, footfall, delivery, social media)
- Anything a bakery owner scrolling Instagram would think "that's me"
- [Insight 1 — what happened + why it matters to someone running a bakery/cafe/restaurant. Source + date.]
- [Insight 2. Source + date.]
- [Insight 3. Source + date.]

🔍 CREATOR INTELLIGENCE (WEEKLY DEEP DIVE)
Based on the real Instagram data above:

📊 What's hitting — [Across all watched creators this week — which posts got the highest engagement? What topics dominate? Include actual numbers and compare across creators.]

🔁 Pattern winning right now — [What content format or approach is consistently outperforming? Has anything shifted from last week? Be specific.]

🕳️ The gap — [What are NONE of these creators covering that Flavour Founders could own? What conversations are happening in the industry that no one is making content about?]

🎬 Steal this — [2-3 specific things creators did this week that worked. For each: what was it, why did it work, how could Flavour Founders adapt it?]

🔎 NEW CREATORS SPOTTED
The Instagram data above includes discovered creators from hashtag searches (10K-500K followers).
[For each discovered creator worth watching: who they are, what they post, why they're relevant to Flavour Founders, and whether they should be added to the permanent watchlist. Max 3 creators.]

📈 STRATEGIC DIRECTION
[Based on all research above — what should Flavour Founders focus on this week and why? One clear recommendation backed by the data.]

---

RULES:
- This is RESEARCH ONLY. Do NOT generate reel ideas, hooks, captions, or content briefs.
- Creator intelligence must reference ACTUAL posts from the Instagram data — not guesses. Use view counts, hook text, and full captions to analyse what's working.
- Every insight must be backed by data or specific observations.
- NEVER cite news older than 7 days. If nothing recent exists, say so.
- Max 500 words total."""


# ── Core research runner ───────────────────────────────────────────────────
async def run_research(digest_type: str) -> str:
    """Call Claude with web search to generate a research digest."""
    config = load_config()

    # Scrape real Instagram data — daily = profiles only, weekly = profiles + posts + discovery
    instagram_data = await scrape_instagram_creators(digest_type)

    # Drain pending ideas for daily digest
    ideas = drain_ideas() if digest_type == "daily" else ""
    if ideas:
        log.info(f"Including {ideas.count(chr(10)) + 1} ideas from #idea channel")

    prompt = (
        build_daily_prompt(config, instagram_data, ideas)
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

        # Forward to content bot (daily only — weekly is Slack-only)
        if RESEARCH_WEBHOOK_URL and digest_type == "daily":
            try:
                resp2 = await http.post(RESEARCH_WEBHOOK_URL, json=payload)
                resp2.raise_for_status()
                log.info(f"Digest delivered to content bot → {resp2.status_code}")
            except Exception as e:
                log.error(f"Content bot delivery failed: {e}")


# ── Evening reel review ────────────────────────────────────────────────────
REEL_REVIEW_PROMPT = """You are analysing Instagram reel performance for @john_s_hawes (Flavour Founders).

Below is the REAL data from the last 3 days of reels posted to the main grid (pinned posts excluded).

{reel_data}

Analyse these reels and output EXACTLY in this format:

---
📹 EVENING REEL REVIEW — {date}

🏆 WINNER
[Which reel performed best? State the hook, views, likes, comments. Why did this one outperform the others? Be specific about what made the hook work.]

📉 UNDERPERFORMER
[Which reel got the least engagement? What was the hook? Why did it underperform compared to the winner? Be honest and direct.]

💡 ONE THING TO CHANGE TOMORROW
[Based on the data, one specific actionable change for tomorrow's reels. Be direct — e.g. "Your top reel used a contradiction hook, your worst used a statement. Lead with contradiction tomorrow."]

📊 NUMBERS AT A GLANCE
[Quick table: each reel's hook (first line), views, likes, comments — so you can see the comparison at a glance]

---

RULES:
- Reference ACTUAL reel data only — not guesses.
- Be brutally honest about what's not working.
- Keep it under 200 words.
- No motivational fluff. Data and insight only."""


async def run_reel_review() -> str:
    """Scrape own reels and generate evening review."""
    if not APIFY_API_TOKEN:
        return "No Apify token — skipping reel review."

    import asyncio
    async with httpx.AsyncClient(timeout=600) as http:
        own_posts = await _run_apify_actor(
            http, "apify~instagram-post-scraper",
            {"username": [OWN_HANDLE], "resultsLimit": 15},
            "EveningReels",
        )

    reel_data = _format_own_reels(own_posts)

    if "No reels found" in reel_data:
        return reel_data

    prompt = REEL_REVIEW_PROMPT.format(
        reel_data=reel_data,
        date=datetime.now().strftime("%d %b %Y"),
    )

    log.info("Running evening reel review...")
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1500,
        messages=[{"role": "user", "content": prompt}],
    )

    output_parts = [block.text for block in response.content if block.type == "text"]
    result = "\n\n".join(output_parts) if output_parts else "No output generated."
    log.info(f"Reel review complete ({len(result)} chars)")
    return result


async def deliver_reel_review(content: str):
    """POST reel review to Command Centre (and Slack as fallback)."""
    payload = {
        "content": content,
        "generated_at": datetime.now().isoformat(),
        "text": f"*Flavour Founders — Evening Reel Review*\n{content}",
    }
    async with httpx.AsyncClient(timeout=30) as http:
        # Post to Command Centre
        reel_webhook = os.environ.get("REEL_REVIEW_WEBHOOK_URL", "")
        if reel_webhook:
            try:
                resp = await http.post(reel_webhook, json=payload)
                resp.raise_for_status()
                log.info(f"Reel review delivered to Command Centre → {resp.status_code}")
            except Exception as e:
                log.error(f"Command Centre reel review delivery failed: {e}")

        # Also post to main webhook (Slack/Command Centre)
        try:
            resp = await http.post(OUTPUT_WEBHOOK_URL, json=payload)
            resp.raise_for_status()
            log.info(f"Reel review delivered to output webhook → {resp.status_code}")
        except Exception as e:
            log.error(f"Output webhook reel review delivery failed: {e}")


# ── Scheduled jobs ─────────────────────────────────────────────────────────
async def daily_job():
    log.info("⏰ Daily digest triggered")
    content = await run_research("daily")
    await deliver_digest("daily", content)


async def weekly_job():
    log.info("⏰ Weekly digest triggered")
    content = await run_research("weekly")
    await deliver_digest("weekly", content)


async def evening_reel_job():
    log.info("⏰ Evening reel review triggered")
    content = await run_reel_review()
    await deliver_reel_review(content)


# ── FastAPI app ────────────────────────────────────────────────────────────
app = FastAPI(title="Flavour Founders Research Bot")
scheduler = AsyncIOScheduler(timezone="Europe/London")


@app.on_event("startup")
async def startup():
    # Daily quick hits — every day at 7:00am London time
    scheduler.add_job(daily_job, CronTrigger(hour=7, minute=0))
    # Weekly deep dive — every Monday at 8:00am London time
    scheduler.add_job(weekly_job, CronTrigger(day_of_week="mon", hour=8, minute=0))
    # Evening reel review — every day at 8:00pm London time
    scheduler.add_job(evening_reel_job, CronTrigger(hour=20, minute=0))
    scheduler.start()
    log.info("Scheduler started — daily 07:00, weekly Mon 08:00, reel review 20:00 (Europe/London)")


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


@app.get("/")
async def health():
    jobs = [
        {"id": job.id, "next_run": str(job.next_run_time)}
        for job in scheduler.get_jobs()
    ]
    return {
        "status": "running",
        "bot": "Flavour Founders Research Bot",
        "scheduled_jobs": jobs,
        "pending_ideas": len(ideas_store),
    }


@app.post("/ideas")
async def receive_idea(payload: dict):
    """Receive a content idea from Slack Workflow webhook."""
    text = payload.get("text", "").strip()
    if not text:
        return {"error": "No text provided"}

    ideas_store.append({
        "text": text,
        "received_at": datetime.now().isoformat(),
    })
    log.info(f"Idea received: {text[:80]}... ({len(ideas_store)} pending)")
    return {"status": "stored", "pending_ideas": len(ideas_store)}


@app.get("/ideas")
async def list_ideas(token: str = ""):
    """View pending ideas (debug)."""
    if MANUAL_TRIGGER_TOKEN and token != MANUAL_TRIGGER_TOKEN:
        return {"error": "Unauthorised"}
    return {"pending_ideas": len(ideas_store), "ideas": ideas_store}


@app.get("/debug/scrape")
async def debug_scrape(token: str = "", mode: str = "daily"):
    """Test the Apify scraper independently. mode = 'daily' or 'weekly'."""
    if MANUAL_TRIGGER_TOKEN and token != MANUAL_TRIGGER_TOKEN:
        return {"error": "Unauthorised"}
    result = await scrape_instagram_creators(mode)
    return {"apify_token_set": bool(APIFY_API_TOKEN), "mode": mode, "result_length": len(result), "preview": result[:2000]}


@app.get("/debug/discovery")
async def debug_discovery(token: str = ""):
    """Test discovery pool scraper."""
    import asyncio
    if MANUAL_TRIGGER_TOKEN and token != MANUAL_TRIGGER_TOKEN:
        return {"error": "Unauthorised"}

    week_num = int(datetime.now().strftime("%U"))
    batch_size = 5
    start = (week_num * batch_size) % len(DISCOVERY_POOL)
    batch = (DISCOVERY_POOL + DISCOVERY_POOL)[start:start + batch_size]

    async with httpx.AsyncClient(timeout=600) as http:
        data = await _run_apify_actor(
            http, "apify~instagram-profile-scraper",
            {"usernames": batch, "resultsLimit": 10},
            "DebugDiscovery",
        )
    if not data:
        return {"status": "no data", "items": 0, "batch": batch}
    return {
        "items": len(data),
        "batch": batch,
        "profiles": [
            {"username": p.get("username"), "followers": p.get("followersCount"), "bio": (p.get("biography") or "")[:100]}
            for p in data
        ],
    }


@app.get("/debug/own-reels")
async def debug_own_reels(token: str = ""):
    """Debug: show raw post data for own handle."""
    import asyncio
    if MANUAL_TRIGGER_TOKEN and token != MANUAL_TRIGGER_TOKEN:
        return {"error": "Unauthorised"}
    async with httpx.AsyncClient(timeout=600) as http:
        data = await _run_apify_actor(
            http, "apify~instagram-post-scraper",
            {"username": [OWN_HANDLE], "resultsLimit": 5},
            "DebugOwnReels",
        )
    if not data:
        return {"status": "no data", "items": 0}
    return {
        "items": len(data),
        "posts": [
            {
                "type": p.get("type"),
                "timestamp": p.get("timestamp"),
                "takenAtTimestamp": p.get("takenAtTimestamp"),
                "isPinned": p.get("isPinned"),
                "pinned": p.get("pinned"),
                "likesCount": p.get("likesCount"),
                "commentsCount": p.get("commentsCount"),
                "videoPlayCount": p.get("videoPlayCount"),
                "caption": (p.get("caption") or "")[:100],
                "all_keys": list(p.keys()),
            }
            for p in data[:5]
        ],
    }


@app.post("/trigger/{digest_type}")
async def manual_trigger(digest_type: str, background_tasks: BackgroundTasks, token: str = ""):
    """Manually trigger a digest. digest_type = 'daily' or 'weekly'."""
    if MANUAL_TRIGGER_TOKEN and token != MANUAL_TRIGGER_TOKEN:
        return {"error": "Unauthorised"}, 401

    if digest_type not in ("daily", "weekly"):
        return {"error": "digest_type must be 'daily' or 'weekly'"}

    async def _run():
        try:
            content = await run_research(digest_type)
            await deliver_digest(digest_type, content)
        except Exception as e:
            log.error(f"Background task failed: {type(e).__name__}: {e}")

    background_tasks.add_task(_run)
    return {"status": "triggered", "digest_type": digest_type}


@app.post("/debug/trigger/{digest_type}")
async def debug_trigger(digest_type: str, token: str = ""):
    """Foreground trigger — returns result or error directly."""
    if MANUAL_TRIGGER_TOKEN and token != MANUAL_TRIGGER_TOKEN:
        return {"error": "Unauthorised"}
    if digest_type not in ("daily", "weekly", "reels"):
        return {"error": "digest_type must be 'daily', 'weekly', or 'reels'"}
    try:
        if digest_type == "reels":
            content = await run_reel_review()
            await deliver_reel_review(content)
        else:
            content = await run_research(digest_type)
            await deliver_digest(digest_type, content)
        return {"status": "success", "digest_type": digest_type, "content_length": len(content), "preview": content[:500]}
    except Exception as e:
        return {"status": "error", "error": f"{type(e).__name__}: {e}"}

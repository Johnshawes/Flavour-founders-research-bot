# Flavour Founders — Research Bot

Always-on research digest bot. Runs on Railway. Same architecture as the Instagram DM bot.

## What It Does

| Schedule | Time | Output |
|---|---|---|
| Daily digest | 07:00 London | Trending topics, competitor pulse, 1 content idea, 1 audience insight |
| Weekly deep dive | Mon 08:00 London | Full market report, competitor analysis, 7-day content brief |

Results are POSTed as JSON to your `OUTPUT_WEBHOOK_URL` — works with Slack, Notion, or any relay.

---

## Environment Variables (set in Railway)

| Variable | Required | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | ✅ | Your Anthropic API key |
| `OUTPUT_WEBHOOK_URL` | ✅ | Where digests are sent (Slack webhook, Notion API, email relay etc) |
| `MANUAL_TRIGGER_TOKEN` | Optional | Secret token to protect the manual trigger endpoint |

---

## File Structure

```
research-bot/
├── main.py            ← Entire bot (FastAPI + scheduler + Claude calls)
├── CLAUDE.md          ← Config: competitors, topics, audience, tone (edit freely)
├── requirements.txt   ← Python dependencies
├── Procfile           ← Railway start command
└── README.md
```

---

## Manual Trigger

Hit this endpoint to run a digest on demand (useful for testing):

```
POST /trigger/daily?token=YOUR_TOKEN
POST /trigger/weekly?token=YOUR_TOKEN
```

## Health Check

```
GET /
```
Returns status + next scheduled run times.

---

## Deploy to Railway

1. Push this folder to a GitHub repo
2. Connect repo to Railway
3. Set the 3 environment variables
4. Railway auto-deploys — bot starts immediately

---

## Customising Research Focus

Edit `CLAUDE.md` — no code changes needed:
- Add competitors to watch
- Adjust focus topics
- Change tone/style preferences
- Railway will pick up changes on next deploy

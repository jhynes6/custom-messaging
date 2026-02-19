# Custom Messaging Pipeline

An async pipeline that takes a list of prospect companies, collects intelligence from their website and LinkedIn profile, and uses that context to generate tailored go-to-market messaging variables via LLM.

Simply modify promps/custom_messaging.txt to get your desired custom message. 

Ex: if you are selling SEO, you could use a prompt that generates a sample content calendar based on the context gathered from the client's website + LI company profile. 

## BASE CASE EXAMPLE IN CURRENT PROMPTS (`prompts/custom_messaging.txt`)

### PROBLEM CONTEXT: 

- Email outreach to B2B service companies selling outbound GTM services
- Desired personalization: a bullet-point list of intent signals relevant to the prospect's service offering

### SAMPLE USAGE: 

(The app only generates the values for the custom variables, so you'll need to adjust the prompts accourdingly) 

"""
<...EMAIL INTRO...>

If you we're trying to find high-intent buyers for your {custom_message_output_1}, we might start by looking at accounts with

{custom_message_output_3} 

<...EMAIL SIGN-OFF...>
"""

### SAMPLE OUTPUT: 

"""
<...EMAIL INTRO...>

If you we're trying to find high-intent buyers for your {email & SMS marketing} services, we might start by looking at accounts with

{
- Search intent topics like "Klaviyo flow setup" or "SMS abandoned cart".
- Job posts for CRM Lifecycle Manager and leadership changes in Marketing.
- Declining organic traffic and rising paid search share in Similarweb/Semrush.
- Low review recency and increased complaints about support or repeat purchases.
}

<...EMAIL SIGN-OFF...>

"""

---

## How It Works

The pipeline runs in two phases for each prospect.

### Phase 1 — Data Collection

The pipeline gathers raw context about each prospect from two sources:

**LinkedIn (via BrightData)**
Fetches the company's LinkedIn profile — description, industry, headcount, specialties, and recent posts.

**Website**
Scrapes the company's website in three steps:
1. Fetches the **homepage** content.
2. Parses the **sitemap** to discover all URLs.
3. Uses an LLM (`prompts/sitemap_analysis.txt`) to classify URLs into three buckets — services/products, markets/industries, and case studies — then scrapes the most relevant pages from each.

### Phase 2 — Brief + Messaging Generation

With the raw data collected, the pipeline runs three sequential LLM steps:

**Step 1 — Prospect Brief** (`prompts/prospect_brief.txt`)
Synthesizes all gathered data into a structured brief covering:
- Services and products offered
- Markets and industries served
- Business problems and pain points addressed or purpose of the product / service
- Case studies with quantifiable results

> If no pain points or purpose are found, a fallback KPI research step (`prompts/kpi_research.txt`) infers relevant metrics from the company's services.

**Step 2.1 — Custom Messaging** (`prompts/custom_messaging.txt`)

Uses the prospect brief as context to generate three messaging variables designed for cold outreach:

| Output Variable | Description |
|---|---|
| `custom_message_output_1` | **Selected Service** — the most relevant service the prospect offers |
| `custom_message_output_2` | **Problem Solved** — the specific problem that service addresses |
| `custom_message_output_3` | **Intent Signals** — four observable, data-driven triggers indicating a prospect needs this service (e.g. job posts, search intent keywords, declining web metrics) |

Results are cached in Supabase so previously processed companies are returned instantly without re-scraping.

---

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in your API keys.

---

## Input CSV

| Column | Required | Description |
|---|---|---|
| `company_name` | Yes | Name of the prospect company |
| `company_website` | Yes | Company website (bare domain like `acme.com` is fine) |
| `company_linkedin_url` | Yes | LinkedIn company page URL |

All other columns are preserved in the output.

---

## Commands

```bash
# Full run
python main.py --input data/prospects.csv

# Specify output path
python main.py -i data/prospects.csv -o data/results.csv

# Dry run (first 5 rows only)
python main.py -i data/prospects.csv --dry-run

# Reprocess with updated prompts (skips scraping, re-runs LLM steps)
python main.py -i data/prospects.csv --reprocess

# Override messaging model
python main.py -i data/prospects.csv --model gpt-4o

# Override concurrency
python main.py -i data/prospects.csv --concurrency 10
```

---

## Output CSV

All input columns are preserved, plus:

| Column | Description |
|---|---|
| `prospect_brief` | JSON string of the structured brief |
| `custom_messaging` | Full raw LLM messaging output |
| `custom_message_output_1` | Selected Service |
| `custom_message_output_2` | Problem Solved |
| `custom_message_output_3` | Intent Signals (bulleted list) |

A companion `_errors.csv` is written alongside the output for any failed rows.

---

## Prompts

| File | Purpose |
|---|---|
| `prompts/sitemap_analysis.txt` | Classifies sitemap URLs into services, markets, and case study categories |
| `prompts/prospect_brief.txt` | Synthesizes scraped data into a structured prospect brief |
| `prompts/kpi_research.txt` | Fallback — infers pain points from services when none are found on the site |
| `prompts/custom_messaging.txt` | Generates the three final messaging output variables from the brief |

---

## Jobs: LinkedIn Job Scraping

Standalone scripts in `jobs/` for scraping LinkedIn job data via BrightData.

### Input CSV columns

| Column | Required | Description |
|---|---|---|
| `url` | Yes | LinkedIn job search URL or individual job post URL |

### Commands

**Search scrape** (synchronous) — provide LinkedIn job search URLs, get back all discovered job posts:

```bash
python jobs/linkedin_jobs.py search --input data/job_searches.csv --output data/job_posts.csv
```

Example search URLs:
- `https://www.linkedin.com/jobs/search?keywords=Software&location=Tel%20Aviv`
- `https://www.linkedin.com/jobs/semrush-jobs?f_C=2821922`

**Direct scrape** (async trigger/poll) — provide individual LinkedIn job post URLs, get back full job details:

```bash
python jobs/linkedin_jobs.py direct --input data/job_urls.csv --output data/job_details.csv
```

Example job URLs:
- `https://www.linkedin.com/jobs/view/software-engineer-at-epic-3986111804`
- `https://www.linkedin.com/jobs/view/software-engineer-at-pave-4310512612`

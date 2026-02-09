# Custom Messaging Pipeline — Design Plan

## Is an Agentic System Necessary?

**No.** An agentic system (LLM decides which tools to call, loops until done) adds latency, unpredictability, and cost — all undesirable at 10,000-row scale. This workload is a **deterministic pipeline**: every prospect follows the exact same steps in the exact same order. There are no branching decisions an LLM needs to make at the orchestration level.

What we need instead is a **structured async pipeline** where the LLM is used as a processing step (sitemap analysis, brief generation, messaging generation) — not as an orchestrator.

Benefits over an agentic approach:
- **Predictable cost** — fixed number of LLM calls per row (3-4)
- **Faster** — no agent reasoning overhead, direct async I/O
- **Reliable** — no tool-selection failures or infinite loops
- **Debuggable** — each stage can be tested and logged independently

---

## High-Level Architecture

```
┌──────────┐
│  CSV In   │
└────┬─────┘
     │
     ▼
┌──────────────────────────────────────────────────┐
│           ASYNC PIPELINE (per prospect)          │
│                                                  │
│  ┌─────────────────┐   ┌──────────────────────┐  │
│  │  LinkedIn Data   │   │   Website Data        │  │
│  │  (BrightData     │   │   1. Fetch homepage   │  │
│  │   API)           │   │   2. Fetch sitemap    │  │
│  │                  │   │   3. LLM: pick URLs   │  │
│  │                  │   │   4. Scrape pages      │  │
│  └───────┬─────────┘   └──────────┬───────────┘  │
│          │                        │               │
│          └──────────┬─────────────┘               │
│                     ▼                             │
│          ┌────────────────────┐                   │
│          │  BRIEF GENERATOR   │                   │
│          │  (gpt-4o-mini)     │                   │
│          └─────────┬──────────┘                   │
│                    ▼                              │
│          ┌────────────────────┐                   │
│          │ MESSAGING GENERATOR│                   │
│          │  (gpt-4o-mini)     │                   │
│          └─────────┬──────────┘                   │
│                    │                              │
└────────────────────┼──────────────────────────────┘
                     ▼
              ┌──────────┐
              │  CSV Out  │
              │ (+custom_ │
              │ messaging)│
              └──────────┘
```

---

## Step 1: Data Gathering

### 1a. LinkedIn Company Profile (BrightData API)

- Call BrightData's dataset API to trigger a snapshot for the LinkedIn company URL.
- BrightData returns structured JSON with company description, specialties, industry, size, etc.
- **Async**: all LinkedIn requests fire concurrently (with a semaphore for rate limiting).

### 1b. Website Scraping

For each prospect's company website:

1. **Fetch homepage** — extract text content.
2. **Fetch `/sitemap.xml`** (or `/sitemap_index.xml`) — parse all listed URLs.
3. **LLM call (gpt-4o-mini)** — given the sitemap URL list, identify URLs likely related to:
   - Services / Products
   - Markets / Industries
   - Case Studies / Customer Stories
   Return a filtered list (capped at ~10 URLs to control cost and latency).
4. **Scrape identified pages** — fetch and extract text from each.

All HTTP fetches use `httpx.AsyncClient` for concurrency.

---

## Step 2: Brief Generation (gpt-4o-mini)

Feed all gathered data (LinkedIn profile + scraped page content) into a single LLM call with a structured output prompt. The prompt file is loaded from `prompts/prospect_brief.txt`.

### Prospect Brief Data Structure

```python
class CaseStudy(BaseModel):
    case_study_company: str
    case_study_industry: str
    case_study_results: str
    case_study_services: str

class ProspectBrief(BaseModel):
    company_name: str
    services_products: list[str]          # Bulleted items
    markets_industries: list[str]         # Bulleted items
    problems_pain_points: list[str]       # Detailed bulleted items
    case_studies: list[CaseStudy]         # Structured case studies
```

Using OpenAI's **structured output** (`response_format`) to guarantee the response parses into this Pydantic model every time. This eliminates JSON parsing failures at scale.

---

## Step 3: Custom Messaging Generation

Feed the serialized `ProspectBrief` into another LLM call. Prompt loaded from `prompts/custom_messaging.txt`.

### Model Recommendation for Step 2

**Start with `gpt-4o-mini`.**

Rationale:
- By the time we reach this step, the brief already contains structured, distilled information. The LLM's job is moderate: pick a service, identify the problem, and generate 4 aligned intent signals.
- At 10,000 rows, cost matters. gpt-4o-mini is ~17x cheaper than gpt-4o.
- The prompt is externalized, so if quality isn't sufficient on early test batches, swapping to `gpt-4o` is a one-line config change — no code changes needed.

| Model | Input Cost | Output Cost | 10K rows est. |
|-------|-----------|-------------|---------------|
| gpt-4o-mini | $0.15/1M tokens | $0.60/1M tokens | ~$2-5 |
| gpt-4o | $2.50/1M tokens | $10.00/1M tokens | ~$40-80 |

The output is appended as a `custom_messaging` column in the output CSV.

---

## Concurrency & Throughput Design

```
CONCURRENCY CONTROLS (configurable in config.py):
├── MAX_CONCURRENT_PROSPECTS = 20     # Prospects processed in parallel
├── MAX_CONCURRENT_HTTP = 50          # Total concurrent HTTP requests
├── MAX_CONCURRENT_LLM = 20          # Total concurrent OpenAI calls
├── MAX_PAGES_PER_SITE = 10          # Cap on scraped pages per prospect
└── HTTP_TIMEOUT = 30s               # Per-request timeout
```

- `asyncio.Semaphore` gates each resource pool.
- Prospects processed via `asyncio.gather` in batches.
- `tqdm` progress bar for real-time tracking.
- Failed prospects are logged and skipped (not fatal) — a `_errors.csv` is written alongside the output with failure details.

---

## Prompt Management

All LLM prompts live in `prompts/` as plain text files, loaded at runtime:

```
prompts/
├── sitemap_analysis.txt      # "Given this sitemap, pick relevant URLs..."
├── prospect_brief.txt        # "Generate a prospect brief from this data..."
└── custom_messaging.txt      # "Using the prospect brief below..."
```

Editing a prompt requires zero code changes — just edit the file and re-run.

---

## Error Handling & Resilience

| Failure | Handling |
|---------|----------|
| Website unreachable / no sitemap | Proceed with homepage only + LinkedIn data |
| BrightData API error | Proceed with website data only, log warning |
| LLM call fails | Retry up to 3x with exponential backoff |
| All data sources fail | Skip prospect, log to `_errors.csv` |
| Malformed LLM response | Retry with stricter prompt (structured output minimizes this) |

---

## Database Requirement

**Not needed for the core pipeline.** The system is stateless: CSV in → CSV out. However, if we want any of the following later, Supabase would be useful:

- **Caching**: Store scraped data so re-runs don't re-fetch the same companies.
- **Checkpointing**: Resume a 10K-row job from where it left off after a failure.
- **Audit trail**: Store briefs and messaging for review/analysis.

**Recommendation**: Build V1 without a database. Add Supabase caching in V2 if re-processing the same companies becomes common. The data model is clean enough that adding persistence later is straightforward.

---

## Project Structure

```
custom-messaging/
├── main.py                        # CLI entry point
├── config.py                      # Settings from .env + defaults
├── models.py                      # Pydantic models (ProspectBrief, CaseStudy, etc.)
├── pipeline/
│   ├── __init__.py
│   ├── csv_handler.py             # Read input CSV, write output CSV + errors
│   ├── linkedin_scraper.py        # BrightData API client for LinkedIn profiles
│   ├── website_scraper.py         # Homepage, sitemap, page scraping
│   ├── brief_generator.py         # LLM call to produce ProspectBrief
│   └── messaging_generator.py     # LLM call to produce custom messaging
├── prompts/
│   ├── sitemap_analysis.txt
│   ├── prospect_brief.txt
│   └── custom_messaging.txt
├── data/                          # Input/output CSVs (gitignored)
├── requirements.txt
├── .env
├── .gitignore
└── DESIGN_PLAN.md
```

---

## Input CSV Expected Columns

At minimum, the input CSV must contain:

| Column | Description |
|--------|-------------|
| `company_name` | Name of the prospect company |
| `company_website` | Full URL (e.g. `https://acme.com`) |
| `company_linkedin_url` | LinkedIn company page URL |

All other columns are preserved in the output CSV. The pipeline appends:
- `prospect_brief` — JSON string of the structured brief
- `custom_messaging` — the generated messaging text

---

## Execution

```bash
# Activate venv
source .venv/bin/activate

# Run pipeline
python main.py --input data/prospects.csv --output data/prospects_output.csv
```

Optional flags:
- `--concurrency 20` — override MAX_CONCURRENT_PROSPECTS
- `--model gpt-4o` — override messaging model
- `--dry-run` — process first 5 rows only (for testing)

---

## Summary of LLM Calls Per Prospect

| Call | Model | Purpose |
|------|-------|---------|
| 1 | gpt-4o-mini | Analyze sitemap → pick relevant URLs |
| 2 | gpt-4o-mini | Generate structured ProspectBrief |
| 3 | gpt-4o-mini | Generate custom messaging from brief |

**Total: 3 LLM calls per prospect, ~4 calls if sitemap requires pagination.**

At 10,000 prospects with gpt-4o-mini across all calls: **estimated cost $5–15** depending on page content length.

"""
Custom Messaging Pipeline
=========================
Process prospect CSVs → gather data → generate briefs → produce messaging.

Usage:
    python main.py --input data/prospects.csv
    python main.py --input data/prospects.csv --output data/out.csv --dry-run
    python main.py -i data/prospects.csv -c 10 --model gpt-4o
"""

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
from tqdm.asyncio import tqdm_asyncio

from config import settings
from models import ProspectInput
from pipeline.csv_handler import (
    read_input_csv,
    write_output_csv,
    write_errors_csv,
)
from pipeline.linkedin_scraper import LinkedInScraper
from pipeline.website_scraper import WebsiteScraper
from pipeline.brief_generator import BriefGenerator
from pipeline.kpi_researcher import KPIResearcher
from pipeline.messaging_generator import MessagingGenerator
from pipeline.supabase_client import SupabaseCache
from utils import normalize_url

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log"),
    ],
)
logger = logging.getLogger("custom_messaging")


# ---------------------------------------------------------------------------
# Per-prospect processing
# ---------------------------------------------------------------------------
async def process_prospect(
    prospect: ProspectInput,
    http_client: httpx.AsyncClient,
    website_scraper: WebsiteScraper,
    linkedin_data_map: dict[str, dict],
    brief_generator: BriefGenerator,
    kpi_researcher: KPIResearcher,
    messaging_generator: MessagingGenerator,
    cache: SupabaseCache,
    prospect_semaphore: asyncio.Semaphore,
    reprocess: bool = False,
) -> dict:
    """Run the full pipeline for a single prospect.

    Args:
        reprocess: If True, skip the cache short-circuit and re-run LLM
                   steps.  Cached scraped data (LinkedIn + website) is
                   reused when available so we don't re-scrape.
    """
    website = normalize_url(prospect.company_website)
    result: dict = {
        "company_name": prospect.company_name,
        "company_website": website,
        "brief": None,
        "messaging": None,
        "custom_message_output_1": "",
        "custom_message_output_2": "",
        "custom_message_output_3": "",
        "error": None,
    }

    async with prospect_semaphore:
        try:
            # ---- Cache check ----
            cached = await asyncio.to_thread(
                cache.get_cached_prospect, website
            )

            if (
                not reprocess
                and cached
                and cached.get("processing_status") == "completed"
            ):
                result["brief"] = cached.get("prospect_brief")
                result["messaging"] = cached.get("custom_messaging")
                result["custom_message_output_1"] = cached.get(
                    "custom_message_output_1", ""
                ) or ""
                result["custom_message_output_2"] = cached.get(
                    "custom_message_output_2", ""
                ) or ""
                result["custom_message_output_3"] = cached.get(
                    "custom_message_output_3", ""
                ) or ""
                logger.info(f"Cache hit: {prospect.company_name}")
                return result

            # ---- DATA GATHERING ----
            # Reuse cached scraped data when reprocessing so we skip
            # the expensive LinkedIn + website fetches.
            if reprocess and cached and cached.get("linkedin_data"):
                linkedin_data = cached["linkedin_data"]
            else:
                linkedin_data = linkedin_data_map.get(
                    prospect.company_linkedin_url, {}
                )

            if reprocess and cached and cached.get("website_data"):
                website_data = cached["website_data"]
            else:
                website_data = await website_scraper.scrape_company(
                    http_client, website, prospect.company_name
                )

            await asyncio.to_thread(
                cache.upsert_prospect,
                {
                    "company_name": prospect.company_name,
                    "company_website": website,
                    "company_linkedin_url": prospect.company_linkedin_url,
                    "linkedin_data": linkedin_data or None,
                    "website_data": website_data or None,
                    "processing_status": "data_gathered",
                },
            )

            # ---- BRIEF GENERATION ----
            brief = await brief_generator.generate(
                prospect.company_name, linkedin_data, website_data
            )

            # ---- KPI RESEARCH (fallback when no pain points) ----
            if not brief.problems_pain_points and brief.services_products:
                logger.info(
                    f"No pain points for {prospect.company_name} — "
                    f"researching KPIs for {len(brief.services_products[:5])} services"
                )
                pain_points = await kpi_researcher.research_pain_points(
                    brief.services_products
                )
                brief.problems_pain_points = pain_points

            brief_dict = brief.model_dump()

            await asyncio.to_thread(
                cache.upsert_prospect,
                {
                    "company_name": prospect.company_name,
                    "company_website": website,
                    "company_linkedin_url": prospect.company_linkedin_url,
                    "prospect_brief": brief_dict,
                    "processing_status": "brief_generated",
                },
            )

            # ---- CUSTOM MESSAGING ----
            msg = await messaging_generator.generate(brief)

            await asyncio.to_thread(
                cache.upsert_prospect,
                {
                    "company_name": prospect.company_name,
                    "company_website": website,
                    "company_linkedin_url": prospect.company_linkedin_url,
                    "custom_messaging": msg.raw,
                    "custom_message_output_1": msg.selected_service,
                    "custom_message_output_2": msg.problem_solved,
                    "custom_message_output_3": msg.intent_signals,
                    "processing_status": "completed",
                },
            )

            result["brief"] = brief_dict
            result["messaging"] = msg.raw
            result["custom_message_output_1"] = msg.selected_service
            result["custom_message_output_2"] = msg.problem_solved
            result["custom_message_output_3"] = msg.intent_signals
            logger.info(f"Completed: {prospect.company_name}")

        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"
            result["error"] = error_msg
            logger.error(f"Failed: {prospect.company_name} — {error_msg}")

            await asyncio.to_thread(
                cache.upsert_prospect,
                {
                    "company_name": prospect.company_name,
                    "company_website": website,
                    "processing_status": "failed",
                    "error_message": error_msg,
                },
            )

    return result


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------
async def run_pipeline(
    input_path: str,
    output_path: str,
    concurrency: int | None = None,
    model: str | None = None,
    dry_run: bool = False,
    reprocess: bool = False,
) -> None:
    """Main entry point: CSV in → process → CSV out."""

    if concurrency:
        settings.max_concurrent_prospects = concurrency
    if model:
        settings.messaging_model = model

    # ---- Read CSV ----
    logger.info(f"Reading input: {input_path}")
    prospects, df = read_input_csv(input_path)

    if dry_run:
        prospects = prospects[:5]
        logger.info("DRY RUN — processing first 5 prospects only")

    logger.info(f"Loaded {len(prospects)} prospects")

    # ---- Initialise components ----
    cache = SupabaseCache()
    http_sem = asyncio.Semaphore(settings.max_concurrent_http)
    llm_sem = asyncio.Semaphore(settings.max_concurrent_llm)
    prospect_sem = asyncio.Semaphore(settings.max_concurrent_prospects)

    linkedin_scraper = LinkedInScraper(http_sem)
    website_scraper = WebsiteScraper(http_sem, llm_sem)
    brief_generator = BriefGenerator(llm_sem)
    kpi_researcher = KPIResearcher(llm_sem)
    messaging_generator = MessagingGenerator(llm_sem)

    # ---- Pipeline run record ----
    run_id = await asyncio.to_thread(
        cache.create_pipeline_run, input_path, len(prospects)
    )
    logger.info(f"Pipeline run ID: {run_id}")

    # ==================================================================
    # PHASE 1 — Batch fetch LinkedIn data
    # ==================================================================
    linkedin_data_map: dict[str, dict] = {}

    if reprocess:
        logger.info(
            "Phase 1: REPROCESS mode — skipping LinkedIn scraping "
            "(using cached data)"
        )
    else:
        logger.info("Phase 1: Fetching LinkedIn profiles via BrightData …")

        linkedin_companies = [
            {"url": p.company_linkedin_url, "company_name": p.company_name}
            for p in prospects
            if p.company_linkedin_url
            and p.company_linkedin_url.strip().lower()
            not in ("nan", "", "none")
        ]

        if linkedin_companies:
            async with httpx.AsyncClient() as client:
                try:
                    linkedin_data_map = (
                        await linkedin_scraper.scrape_companies(
                            client, linkedin_companies
                        )
                    )
                except Exception as e:
                    logger.error(f"LinkedIn scraping failed: {e}")

    # ==================================================================
    # PHASE 2 — Per-prospect processing
    # ==================================================================
    logger.info(
        "Phase 2: Website scraping → brief → messaging …"
    )

    async with httpx.AsyncClient() as client:
        tasks = [
            process_prospect(
                prospect=p,
                http_client=client,
                website_scraper=website_scraper,
                linkedin_data_map=linkedin_data_map,
                brief_generator=brief_generator,
                kpi_researcher=kpi_researcher,
                messaging_generator=messaging_generator,
                cache=cache,
                prospect_semaphore=prospect_sem,
                reprocess=reprocess,
            )
            for p in prospects
        ]

        results = await tqdm_asyncio.gather(
            *tasks, desc="Processing prospects"
        )

    # ==================================================================
    # PHASE 3 — Write output
    # ==================================================================
    logger.info("Phase 3: Writing output CSV …")

    completed = sum(1 for r in results if r["error"] is None)
    failed = sum(1 for r in results if r["error"] is not None)
    errors = [
        {
            "company_name": r["company_name"],
            "company_website": r["company_website"],
            "error": r["error"],
        }
        for r in results
        if r["error"]
    ]

    write_output_csv(df, results, output_path)

    if errors:
        write_errors_csv(errors, output_path)

    # Update pipeline run
    await asyncio.to_thread(
        cache.update_pipeline_run,
        run_id,
        completed=completed,
        failed=failed,
        status="completed",
        completed_at=datetime.now(timezone.utc).isoformat(),
    )

    logger.info(
        f"Pipeline complete: {completed} succeeded, {failed} failed "
        f"out of {len(prospects)}"
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Custom Messaging Pipeline"
    )
    parser.add_argument(
        "--input", "-i", required=True, help="Path to input CSV"
    )
    parser.add_argument(
        "--output", "-o", default=None, help="Path to output CSV"
    )
    parser.add_argument(
        "--concurrency", "-c", type=int, default=None,
        help="Override max concurrent prospects",
    )
    parser.add_argument(
        "--model", "-m", default=None,
        help="Override messaging model (e.g. gpt-4o)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Process first 5 rows only",
    )
    parser.add_argument(
        "--reprocess", action="store_true",
        help="Re-run brief + messaging with current prompts "
             "(reuses cached scraped data, skips LinkedIn re-fetch)",
    )

    args = parser.parse_args()

    output_path = args.output
    if not output_path:
        p = Path(args.input)
        output_path = str(p.parent / f"{p.stem}_output{p.suffix}")

    asyncio.run(
        run_pipeline(
            args.input,
            output_path,
            args.concurrency,
            args.model,
            args.dry_run,
            args.reprocess,
        )
    )


if __name__ == "__main__":
    main()

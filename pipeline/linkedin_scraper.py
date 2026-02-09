"""BrightData Datasets API client for LinkedIn company profiles.

Mirrors the proven patterns from context/BrightData.py (batch-of-50 triggers,
snapshot-level polling, rate-limit guard, exponential-backoff retries) while
using async httpx for throughput.
"""

import asyncio
import logging
from typing import Optional

import httpx

from config import settings

logger = logging.getLogger("custom_messaging")

BRIGHTDATA_API_BASE = "https://api.brightdata.com/datasets/v3"
BATCH_SIZE = 50
MAX_TRIGGER_RETRIES = 3
TRIGGER_BASE_DELAY = 2  # seconds, doubles each retry
INTER_BATCH_DELAY = 0.5  # seconds between trigger calls
SNAPSHOT_POLL_INTERVAL = 30  # seconds between status checks
MAX_SNAPSHOT_WAIT = 600  # 10 minutes per snapshot
MAX_RUNNING_SNAPSHOTS = 99  # pause triggering above this


class LinkedInScraper:
    """Batch-trigger LinkedIn company profile collection via BrightData.

    Flow (matches reference client):
      1. Chunk URLs into batches of 50
      2. Trigger each batch → collect snapshot IDs
         - Check running-snapshot count; sleep if >= 99
         - Retry each trigger up to 3× with exponential backoff
      3. Wait for ALL snapshots to reach ``ready`` or ``failed``
      4. Download results from ready snapshots
      5. Map results back by LinkedIn URL
    """

    def __init__(self, http_semaphore: asyncio.Semaphore):
        self.semaphore = http_semaphore
        self.api_key = settings.brightdata_api_key
        self.dataset_id = settings.brightdata_linkedin_company_dataset_id
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _chunk(items: list, size: int):
        """Yield successive chunks of ``size`` from ``items``."""
        for i in range(0, len(items), size):
            yield items[i : i + size]

    # ------------------------------------------------------------------
    # API calls
    # ------------------------------------------------------------------

    async def _get_running_snapshot_count(
        self, client: httpx.AsyncClient
    ) -> int:
        """Return the number of currently-running snapshots in the account."""
        try:
            async with self.semaphore:
                resp = await client.get(
                    "https://api.brightdata.com/datasets/v3/snapshots/",
                    params={"status": "running"},
                    headers=self.headers,
                    timeout=30,
                )
                resp.raise_for_status()
                return len(resp.json())
        except Exception as e:
            logger.warning(f"Could not check running snapshots: {e}")
            return 0

    async def _trigger_snapshot(
        self, client: httpx.AsyncClient, batch: list[dict]
    ) -> str:
        """POST /trigger with retry + exponential backoff.  Returns snapshot_id."""
        url = f"{BRIGHTDATA_API_BASE}/trigger"
        params = {
            "dataset_id": self.dataset_id,
            "include_errors": "true",
        }

        for attempt in range(MAX_TRIGGER_RETRIES):
            try:
                async with self.semaphore:
                    resp = await client.post(
                        url,
                        params=params,
                        headers=self.headers,
                        json=batch,
                        timeout=30,
                    )
                    resp.raise_for_status()
                    return resp.json()["snapshot_id"]
            except Exception as e:
                if attempt == MAX_TRIGGER_RETRIES - 1:
                    raise RuntimeError(
                        f"Trigger failed after {MAX_TRIGGER_RETRIES} attempts: {e}"
                    ) from e
                delay = TRIGGER_BASE_DELAY * (2**attempt)
                logger.warning(
                    f"Trigger attempt {attempt + 1} failed, "
                    f"retrying in {delay}s … ({e})"
                )
                await asyncio.sleep(delay)

        # unreachable, but keeps type-checkers happy
        raise RuntimeError("Trigger failed")

    async def _check_snapshot_status(
        self, client: httpx.AsyncClient, snapshot_id: str
    ) -> dict:
        """GET /progress/{snapshot_id} → status dict."""
        async with self.semaphore:
            resp = await client.get(
                f"{BRIGHTDATA_API_BASE}/progress/{snapshot_id}",
                headers=self.headers,
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()

    async def _wait_on_snapshot(
        self, client: httpx.AsyncClient, snapshot_id: str
    ) -> str:
        """Poll until snapshot is ``ready`` or ``failed``.  Returns final status."""
        elapsed = 0
        while elapsed < MAX_SNAPSHOT_WAIT:
            progress = await self._check_snapshot_status(client, snapshot_id)
            status = progress.get("status", "unknown")

            if status in ("ready", "failed"):
                return status

            logger.debug(
                f"Snapshot {snapshot_id}: {status} "
                f"(elapsed {elapsed}s / {MAX_SNAPSHOT_WAIT}s)"
            )
            await asyncio.sleep(SNAPSHOT_POLL_INTERVAL)
            elapsed += SNAPSHOT_POLL_INTERVAL

        logger.warning(f"Snapshot {snapshot_id} timed out after {MAX_SNAPSHOT_WAIT}s")
        return "timeout"

    async def _download_snapshot(
        self, client: httpx.AsyncClient, snapshot_id: str
    ) -> list[dict]:
        """GET /snapshot/{id}?format=json → list of result dicts."""
        async with self.semaphore:
            resp = await client.get(
                f"{BRIGHTDATA_API_BASE}/snapshot/{snapshot_id}",
                params={"format": "json"},
                headers=self.headers,
                timeout=60,
            )
            if resp.status_code == 200:
                return resp.json()
            logger.error(
                f"Snapshot {snapshot_id} download returned {resp.status_code}"
            )
            return []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def scrape_companies(
        self, client: httpx.AsyncClient, companies: list[dict]
    ) -> dict[str, dict]:
        """Scrape LinkedIn company profiles for a list of companies.

        Args:
            companies: List of dicts, each with ``url`` and ``company_name``.

        Returns:
            Mapping of LinkedIn URL → profile data dict.
        """
        if not companies:
            return {}

        urls = [c["url"] for c in companies]

        # ---- Step 1: Trigger all batches, collect snapshot IDs ----
        snapshot_ids: list[str] = []

        for batch_urls in self._chunk(urls, BATCH_SIZE):
            # Rate-limit guard: pause if too many snapshots already running
            running = await self._get_running_snapshot_count(client)
            if running >= MAX_RUNNING_SNAPSHOTS:
                logger.info(
                    f"Reached {running} running snapshots — "
                    f"sleeping 120s before next trigger"
                )
                await asyncio.sleep(120)

            batch_payload = [{"url": u} for u in batch_urls]
            try:
                sid = await self._trigger_snapshot(client, batch_payload)
                snapshot_ids.append(sid)
                logger.info(
                    f"Triggered snapshot {sid} "
                    f"({len(snapshot_ids) * BATCH_SIZE} / {len(urls)} queued)"
                )
            except Exception as e:
                logger.error(f"Failed to trigger batch: {e}")

            # Small delay between triggers to avoid rate-limiting
            await asyncio.sleep(INTER_BATCH_DELAY)

        if not snapshot_ids:
            return {}

        # ---- Step 2: Wait for ALL snapshots to finish ----
        logger.info(
            f"Waiting on {len(snapshot_ids)} snapshot(s) to complete …"
        )
        statuses = await asyncio.gather(
            *[self._wait_on_snapshot(client, sid) for sid in snapshot_ids]
        )

        ready_count = sum(1 for s in statuses if s == "ready")
        failed_count = sum(1 for s in statuses if s != "ready")
        logger.info(
            f"Snapshots done: {ready_count} ready, {failed_count} failed/timed-out"
        )

        # ---- Step 3: Download results from ready snapshots ----
        all_results: list[dict] = []
        for sid, status in zip(snapshot_ids, statuses):
            if status == "ready":
                data = await self._download_snapshot(client, sid)
                all_results.extend(data)

        # ---- Step 4: Map by LinkedIn URL ----
        mapped: dict[str, dict] = {}
        for item in all_results:
            url = item.get("url") or item.get("input", {}).get("url", "")
            if url:
                mapped[url] = item

        logger.info(
            f"LinkedIn data retrieved for {len(mapped)} / {len(urls)} companies"
        )
        return mapped

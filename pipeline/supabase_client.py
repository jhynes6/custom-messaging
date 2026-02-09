"""Supabase caching and pipeline-run tracking."""

import logging
from datetime import datetime, timezone
from typing import Optional

from supabase import create_client, Client

from config import settings

logger = logging.getLogger("custom_messaging")


class SupabaseCache:
    """Thin wrapper around Supabase for prospect caching and run tracking."""

    def __init__(self):
        self.client: Client = create_client(
            settings.supabase_url, settings.supabase_key
        )

    # ------------------------------------------------------------------
    # Prospect cache
    # ------------------------------------------------------------------

    def get_cached_prospect(self, company_website: str) -> Optional[dict]:
        """Return cached row for a company, or None."""
        try:
            result = (
                self.client.table("prospect_cache")
                .select("*")
                .eq("company_website", company_website)
                .execute()
            )
            if result.data:
                return result.data[0]
        except Exception as e:
            logger.warning(f"Cache lookup failed for {company_website}: {e}")
        return None

    def upsert_prospect(self, data: dict) -> None:
        """Insert or update a prospect_cache row (keyed on company_website)."""
        data["updated_at"] = datetime.now(timezone.utc).isoformat()
        try:
            self.client.table("prospect_cache").upsert(
                data, on_conflict="company_website"
            ).execute()
        except Exception as e:
            logger.warning(f"Cache upsert failed: {e}")

    # ------------------------------------------------------------------
    # Pipeline runs
    # ------------------------------------------------------------------

    def create_pipeline_run(self, input_file: str, total_prospects: int) -> str:
        """Create a pipeline_runs row and return its UUID."""
        result = (
            self.client.table("pipeline_runs")
            .insert(
                {
                    "input_file": input_file,
                    "total_prospects": total_prospects,
                }
            )
            .execute()
        )
        return result.data[0]["id"]

    def update_pipeline_run(self, run_id: str, **kwargs) -> None:
        """Update arbitrary fields on a pipeline run."""
        try:
            self.client.table("pipeline_runs").update(kwargs).eq(
                "id", run_id
            ).execute()
        except Exception as e:
            logger.warning(f"Pipeline run update failed: {e}")

    def increment_pipeline_counter(self, run_id: str, field: str) -> None:
        """Increment the 'completed' or 'failed' counter by 1."""
        try:
            result = (
                self.client.table("pipeline_runs")
                .select(field)
                .eq("id", run_id)
                .execute()
            )
            if result.data:
                current = result.data[0][field]
                self.client.table("pipeline_runs").update(
                    {field: current + 1}
                ).eq("id", run_id).execute()
        except Exception as e:
            logger.warning(f"Counter increment failed: {e}")

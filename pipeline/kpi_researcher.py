"""Web-search KPI researcher — used when no pain points are found in the brief.

Uses OpenAI Responses API with web_search_preview to search for
"KPIs related to [service]" and summarise findings.
"""

import asyncio
import logging

from openai import AsyncOpenAI

from config import settings
from utils import load_prompt

logger = logging.getLogger("custom_messaging")


class KPIResearcher:
    """Search the web for KPIs related to each service (up to 5)."""

    def __init__(self, llm_semaphore: asyncio.Semaphore):
        self.llm_sem = llm_semaphore
        self.openai = AsyncOpenAI(api_key=settings.openai_api_key)
        self.prompt = load_prompt("prompts/kpi_research.txt")

    async def research_kpis_for_service(self, service: str) -> list[str]:
        """Web search + summarise KPIs for a single service."""
        try:
            async with self.llm_sem:
                response = await self.openai.responses.create(
                    model="gpt-4o-mini",
                    tools=[{"type": "web_search_preview"}],
                    instructions=self.prompt,
                    input=f"KPIs related to {service}",
                )

                text = response.output_text
                if text:
                    kpis: list[str] = []
                    for line in text.strip().split("\n"):
                        line = line.strip().lstrip("-•*").strip()
                        if line:
                            kpis.append(line)
                    return kpis if kpis else [text.strip()]

        except Exception as e:
            logger.warning(f"KPI web search failed for '{service}': {e}")

        return []

    async def research_pain_points(self, services: list[str]) -> list[str]:
        """Research KPIs for up to 5 services; return as pain-point bullets."""
        to_research = services[:5]

        results = await asyncio.gather(
            *[self.research_kpis_for_service(s) for s in to_research],
            return_exceptions=True,
        )

        pain_points: list[str] = []
        for service, result in zip(to_research, results):
            if isinstance(result, Exception):
                logger.warning(f"KPI research failed for '{service}': {result}")
                continue
            for kpi in result:
                pain_points.append(f"[{service}] {kpi}")

        return pain_points

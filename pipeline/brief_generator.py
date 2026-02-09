"""Generate a structured ProspectBrief from gathered data via OpenAI."""

import asyncio
import logging

from openai import AsyncOpenAI

from config import settings
from models import ProspectBrief
from utils import load_prompt

logger = logging.getLogger("custom_messaging")


class BriefGenerator:
    """Produces a ProspectBrief using structured output from gpt-4o-mini."""

    def __init__(self, llm_semaphore: asyncio.Semaphore):
        self.llm_sem = llm_semaphore
        self.openai = AsyncOpenAI(api_key=settings.openai_api_key)
        self.prompt = load_prompt("prompts/prospect_brief.txt")

    # ------------------------------------------------------------------
    # Formatting
    # ------------------------------------------------------------------

    @staticmethod
    def _format_input(
        company_name: str,
        linkedin_data: dict,
        website_data: dict,
    ) -> str:
        """Combine all gathered data into a single context string."""
        parts: list[str] = [f"# Company: {company_name}\n"]

        # LinkedIn profile — only feed "about" and "description" to the LLM
        # (full data is stored in Supabase; neither field is guaranteed)
        if linkedin_data:
            li_about = linkedin_data.get("about") or ""
            li_description = linkedin_data.get("description") or ""
            if li_about or li_description:
                parts.append("## LinkedIn Company Profile")
                if li_about:
                    parts.append(f"About: {li_about}")
                if li_description:
                    parts.append(f"Description: {li_description}")
                parts.append("")

        # Website pages — organised by category
        if website_data:
            homepage = website_data.get("homepage", "")
            if homepage:
                parts.append("## Homepage Content")
                parts.append(homepage[:8000])
                parts.append("")

            category_labels = {
                "services_products_pages": "Services / Products",
                "markets_industries_pages": "Markets / Industries",
                "case_studies_pages": "Case Studies",
            }
            for key, label in category_labels.items():
                pages = website_data.get(key, {})
                if pages:
                    parts.append(f"## {label}")
                    for url, content in pages.items():
                        parts.append(f"### {url}")
                        parts.append(content[:5000])
                        parts.append("")

        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Generation
    # ------------------------------------------------------------------

    async def generate(
        self,
        company_name: str,
        linkedin_data: dict,
        website_data: dict,
    ) -> ProspectBrief:
        """Generate a structured ProspectBrief, with retries."""
        user_content = self._format_input(
            company_name, linkedin_data, website_data
        )

        for attempt in range(settings.llm_retry_attempts):
            try:
                async with self.llm_sem:
                    response = await self.openai.chat.completions.parse(
                        model=settings.brief_model,
                        messages=[
                            {"role": "system", "content": self.prompt},
                            {"role": "user", "content": user_content},
                        ],
                        response_format=ProspectBrief,
                    )

                parsed = response.choices[0].message.parsed
                if parsed:
                    return parsed

                refusal = response.choices[0].message.refusal
                logger.warning(
                    f"Brief refused for {company_name}: {refusal}"
                )
            except Exception as e:
                logger.warning(
                    f"Brief attempt {attempt + 1} failed for "
                    f"{company_name}: {e}"
                )
                if attempt < settings.llm_retry_attempts - 1:
                    await asyncio.sleep(2**attempt)

        # Fallback: empty brief rather than crash
        return ProspectBrief(
            company_name=company_name,
            services_products=[],
            markets_industries=[],
            problems_pain_points=[],
            case_studies=[],
        )

"""Generate custom messaging from a ProspectBrief via OpenAI."""

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Optional

from openai import AsyncOpenAI

from config import settings
from models import ProspectBrief
from utils import load_prompt

logger = logging.getLogger("custom_messaging")


@dataclass
class MessagingResult:
    """Parsed custom messaging output."""
    raw: str                         # Full LLM output
    selected_service: str = ""       # custom_message_output_1
    problem_solved: str = ""         # custom_message_output_2
    intent_signals: str = ""         # custom_message_output_3


class MessagingGenerator:
    """Produce intent-signal messaging from a structured brief."""

    def __init__(self, llm_semaphore: asyncio.Semaphore):
        self.llm_sem = llm_semaphore
        self.openai = AsyncOpenAI(api_key=settings.openai_api_key)
        self.prompt = load_prompt("prompts/custom_messaging.txt")

    # ------------------------------------------------------------------
    # Formatting
    # ------------------------------------------------------------------

    @staticmethod
    def _format_brief(brief: ProspectBrief) -> str:
        """Serialize the brief into a readable string for the LLM."""
        parts: list[str] = [f"Company: {brief.company_name}\n"]

        parts.append("Services/Products:")
        for item in brief.services_products:
            parts.append(f"  - {item}")

        parts.append("\nMarkets/Industries:")
        for item in brief.markets_industries:
            parts.append(f"  - {item}")

        parts.append("\nProblems/Pain Points:")
        for item in brief.problems_pain_points:
            parts.append(f"  - {item}")

        if brief.case_studies:
            parts.append("\nCase Studies:")
            for cs in brief.case_studies:
                parts.append(f"  - Company: {cs.case_study_company}")
                parts.append(f"    Industry: {cs.case_study_industry}")
                parts.append(f"    Results: {cs.case_study_results}")
                parts.append(f"    Services: {cs.case_study_services}")

        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    @staticmethod
    def parse_messaging(text: str) -> MessagingResult:
        """Extract the three output fields from the LLM response.

        Expected format (bullet list):
          - **Selected Service**: ...
          - **Problem Solved**: ...
          - **Intent Signals**:
            - signal 1
            - signal 2
            ...
        """
        result = MessagingResult(raw=text)

        # Selected Service
        m = re.search(
            r"\*\*Selected Service\*\*\s*:\s*(.+)",
            text,
            re.IGNORECASE,
        )
        if m:
            result.selected_service = m.group(1).strip().strip("*")

        # Problem Solved
        m = re.search(
            r"\*\*Problem Solved\*\*\s*:\s*(.+)",
            text,
            re.IGNORECASE,
        )
        if m:
            result.problem_solved = m.group(1).strip().strip("*")

        # Intent Signals — everything after the header until end of text
        m = re.search(
            r"\*\*Intent Signals\*\*\s*:\s*\n?([\s\S]+)",
            text,
            re.IGNORECASE,
        )
        if m:
            signals_block = m.group(1).strip()
            # Collect bullet lines
            lines: list[str] = []
            for line in signals_block.split("\n"):
                line = line.strip().lstrip("-•*").strip()
                if line:
                    lines.append(line)
            result.intent_signals = "\n".join(f"- {l}" for l in lines)

        return result

    # ------------------------------------------------------------------
    # Generation
    # ------------------------------------------------------------------

    async def generate(self, brief: ProspectBrief) -> MessagingResult:
        """Return parsed MessagingResult (empty fields on total failure)."""
        brief_text = self._format_brief(brief)

        for attempt in range(settings.llm_retry_attempts):
            try:
                async with self.llm_sem:
                    response = await self.openai.chat.completions.create(
                        model=settings.messaging_model,
                        reasoning_effort="none",
                        messages=[
                            {"role": "system", "content": self.prompt},
                            {"role": "user", "content": brief_text},
                        ],
                    )

                content = response.choices[0].message.content
                if content:
                    return self.parse_messaging(content.strip())

            except Exception as e:
                logger.warning(
                    f"Messaging attempt {attempt + 1} failed for "
                    f"{brief.company_name}: {e}"
                )
                if attempt < settings.llm_retry_attempts - 1:
                    await asyncio.sleep(2**attempt)

        return MessagingResult(raw="")

"""Website scraping: homepage, sitemap analysis, and targeted page extraction."""

import asyncio
import logging
import re
from xml.etree import ElementTree

import httpx
from openai import AsyncOpenAI

from config import settings
from models import SitemapAnalysis
from utils import extract_text_from_html, normalize_url, load_prompt

logger = logging.getLogger("custom_messaging")

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


class WebsiteScraper:
    """Scrape a company website: homepage + LLM-selected sitemap pages."""

    def __init__(
        self,
        http_semaphore: asyncio.Semaphore,
        llm_semaphore: asyncio.Semaphore,
    ):
        self.http_sem = http_semaphore
        self.llm_sem = llm_semaphore
        self.openai = AsyncOpenAI(api_key=settings.openai_api_key)
        self.sitemap_prompt = load_prompt("prompts/sitemap_analysis.txt")

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    async def fetch_page(
        self, client: httpx.AsyncClient, url: str
    ) -> str:
        """GET a page and return its extracted text (empty string on failure)."""
        async with self.http_sem:
            try:
                resp = await client.get(
                    url,
                    timeout=settings.http_timeout,
                    follow_redirects=True,
                    headers={"User-Agent": USER_AGENT},
                )
                resp.raise_for_status()
                return extract_text_from_html(resp.text)
            except Exception as e:
                logger.debug(f"Failed to fetch {url}: {e}")
                return ""

    async def fetch_sitemap(
        self, client: httpx.AsyncClient, base_url: str
    ) -> list[str]:
        """Try common sitemap paths; return all <loc> URLs found."""
        candidates = [
            f"{base_url}/sitemap.xml",
            f"{base_url}/sitemap_index.xml",
            f"{base_url}/sitemap-index.xml",
        ]
        for sitemap_url in candidates:
            async with self.http_sem:
                try:
                    resp = await client.get(
                        sitemap_url,
                        timeout=settings.http_timeout,
                        follow_redirects=True,
                        headers={"User-Agent": USER_AGENT},
                    )
                    if resp.status_code == 200 and resp.text.strip():
                        urls = self._parse_sitemap(resp.text)
                        if urls:
                            return urls
                except Exception:
                    continue
        return []

    @staticmethod
    def _parse_sitemap(xml_content: str) -> list[str]:
        """Extract URLs from a sitemap XML document."""
        urls: list[str] = []
        try:
            root = ElementTree.fromstring(xml_content)
            ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            for loc in root.findall(".//ns:loc", ns):
                if loc.text:
                    urls.append(loc.text.strip())
        except ElementTree.ParseError:
            # Regex fallback for malformed XML
            urls = re.findall(r"<loc>(.*?)</loc>", xml_content)
        return urls

    # ------------------------------------------------------------------
    # LLM sitemap analysis
    # ------------------------------------------------------------------

    async def identify_relevant_urls(
        self, sitemap_urls: list[str], company_name: str
    ) -> dict[str, list[str]]:
        """Ask the LLM to classify sitemap URLs into categories.

        Returns:
            {
                "services_products": [url, ...],
                "markets_industries": [url, ...],
                "case_studies": [url, ...],
            }
        Each list is capped to its per-category config limit.
        """
        empty: dict[str, list[str]] = {
            "services_products": [],
            "markets_industries": [],
            "case_studies": [],
        }
        if not sitemap_urls:
            return empty

        # Cap the list sent to the LLM to avoid token overflow
        url_list = "\n".join(sitemap_urls[:500])

        async with self.llm_sem:
            try:
                response = await self.openai.chat.completions.parse(
                    model=settings.sitemap_model,
                    messages=[
                        {"role": "system", "content": self.sitemap_prompt},
                        {
                            "role": "user",
                            "content": (
                                f"Company: {company_name}\n\n"
                                f"Sitemap URLs:\n{url_list}"
                            ),
                        },
                    ],
                    response_format=SitemapAnalysis,
                )
                parsed = response.choices[0].message.parsed
                if parsed:
                    return {
                        "services_products": parsed.services_products_urls[
                            : settings.max_services_pages
                        ],
                        "markets_industries": parsed.markets_industries_urls[
                            : settings.max_markets_pages
                        ],
                        "case_studies": parsed.case_studies_urls[
                            : settings.max_case_study_pages
                        ],
                    }
            except Exception as e:
                logger.warning(
                    f"Sitemap analysis failed for {company_name}: {e}"
                )

        return empty

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def scrape_company(
        self,
        client: httpx.AsyncClient,
        website_url: str,
        company_name: str,
    ) -> dict:
        """Scrape a company website.

        Returns:
            {
                "homepage": str,
                "services_products_pages": {url: content, ...},
                "markets_industries_pages": {url: content, ...},
                "case_studies_pages": {url: content, ...},
                "sitemap_urls_found": int,
            }
        """
        base_url = normalize_url(website_url)

        # 1. Fetch homepage + sitemap concurrently
        homepage_content, sitemap_urls = await asyncio.gather(
            self.fetch_page(client, base_url),
            self.fetch_sitemap(client, base_url),
        )

        # 2. LLM classifies sitemap URLs into categories
        categorized = await self.identify_relevant_urls(
            sitemap_urls, company_name
        )

        # 3. Scrape all selected pages concurrently
        all_urls: list[str] = []
        url_to_category: dict[str, str] = {}
        for category, urls in categorized.items():
            for url in urls:
                all_urls.append(url)
                url_to_category[url] = category

        result_pages: dict[str, dict[str, str]] = {
            "services_products": {},
            "markets_industries": {},
            "case_studies": {},
        }

        if all_urls:
            page_contents = await asyncio.gather(
                *[self.fetch_page(client, url) for url in all_urls]
            )
            for url, content in zip(all_urls, page_contents):
                if content.strip():
                    cat = url_to_category[url]
                    result_pages[cat][url] = content

        return {
            "homepage": homepage_content,
            "services_products_pages": result_pages["services_products"],
            "markets_industries_pages": result_pages["markets_industries"],
            "case_studies_pages": result_pages["case_studies"],
            "sitemap_urls_found": len(sitemap_urls),
        }

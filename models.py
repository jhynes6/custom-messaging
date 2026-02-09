"""Pydantic models for the custom messaging pipeline."""

from pydantic import BaseModel
from typing import Optional


class CaseStudy(BaseModel):
    case_study_company: str
    case_study_industry: str
    case_study_results: str
    case_study_services: str


class ProspectBrief(BaseModel):
    company_name: str
    services_products: list[str]
    markets_industries: list[str]
    problems_pain_points: list[str]
    case_studies: list[CaseStudy]


class SitemapAnalysis(BaseModel):
    """LLM output for sitemap URL classification — one list per category."""
    services_products_urls: list[str]
    markets_industries_urls: list[str]
    case_studies_urls: list[str]


class ProspectInput(BaseModel):
    """A single row from the input CSV."""
    company_name: str
    company_website: str
    company_linkedin_url: str

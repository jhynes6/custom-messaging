"""Application configuration loaded from .env"""

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # API Keys
    openai_api_key: str
    brightdata_api_key: str
    brightdata_linkedin_company_dataset_id: str

    # Supabase
    supabase_url: str = Field(validation_alias="SUPABASE_CUSTOM_MESSAGING_PROJECT_URL")
    supabase_key: str = Field(validation_alias="SUPABASE_CUSTOM_MESSAGING_ANON_KEY")

    # Pipeline concurrency
    max_concurrent_prospects: int = 20
    max_concurrent_http: int = 50
    max_concurrent_llm: int = 20
    http_timeout: int = 30
    llm_retry_attempts: int = 3

    # Per-category page limits (homepage is always 1, these are additional)
    max_services_pages: int = 3
    max_markets_pages: int = 3
    max_case_study_pages: int = 5

    # Models
    brief_model: str = "gpt-4o-mini"
    messaging_model: str = "gpt-5.2"
    sitemap_model: str = "gpt-4o-mini"

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()

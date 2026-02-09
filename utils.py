"""Shared utility functions."""

import logging
import re

logger = logging.getLogger("custom_messaging")


def extract_text_from_html(html: str, max_length: int = 15000) -> str:
    """Extract readable text from HTML, stripping scripts, styles, and tags."""
    # Remove script, style, and noscript blocks
    html = re.sub(
        r"<(script|style|noscript)[^>]*>.*?</\1>",
        "",
        html,
        flags=re.DOTALL | re.IGNORECASE,
    )
    # Remove HTML comments
    html = re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)
    # Replace block-level elements with newlines
    html = re.sub(
        r"<(br|p|div|h[1-6]|li|tr|section|article|header|footer)[^>]*>",
        "\n",
        html,
        flags=re.IGNORECASE,
    )
    # Strip remaining tags
    text = re.sub(r"<[^>]+>", " ", html)
    # Decode common HTML entities
    for entity, char in [
        ("&amp;", "&"),
        ("&lt;", "<"),
        ("&gt;", ">"),
        ("&quot;", '"'),
        ("&#39;", "'"),
        ("&nbsp;", " "),
    ]:
        text = text.replace(entity, char)
    # Collapse whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n", "\n\n", text)
    text = text.strip()
    # Truncate
    if len(text) > max_length:
        text = text[:max_length] + "\n...[truncated]"
    return text


def normalize_url(url: str) -> str:
    """Normalise any plausible website input into a clean ``https://`` URL.

    Handles bare domains (``acme.com``), ``www.`` prefixes,
    ``http://`` schemes, trailing slashes/paths, whitespace, and
    accidental surrounding quotes.
    """
    url = url.strip().strip("\"'")

    # Remove leading garbage like "mailto:", "ftp://", etc.
    url = re.sub(r"^(mailto:|ftp://)", "", url, flags=re.IGNORECASE)

    # Strip any existing scheme so we can re-add https consistently
    url = re.sub(r"^https?://", "", url, flags=re.IGNORECASE)

    # Remove trailing slashes, query strings, and fragments
    url = url.split("?")[0].split("#")[0].rstrip("/")

    # Drop a trailing port-only pattern left by copy-paste (e.g. ":443")
    url = re.sub(r":443$", "", url)

    # Ensure www. prefix is not doubled
    url = re.sub(r"^(www\.)+", "www.", url, flags=re.IGNORECASE)

    if not url:
        return ""

    return f"https://{url}"


def load_prompt(path: str) -> str:
    """Load a prompt template from a file."""
    with open(path, "r") as f:
        return f.read()

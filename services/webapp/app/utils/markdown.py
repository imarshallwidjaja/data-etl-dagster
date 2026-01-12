"""Markdown rendering with XSS sanitization."""

import re

import bleach
import markdown as md

ALLOWED_TAGS = [
    "p",
    "strong",
    "em",
    "a",
    "ul",
    "ol",
    "li",
    "code",
    "pre",
    "blockquote",
    "br",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
]
ALLOWED_ATTRIBUTES = {"a": ["href"]}
ALLOWED_PROTOCOLS = ["http", "https", "mailto"]


def render_markdown(text: str) -> str:
    """
    Render markdown to HTML with XSS sanitization.

    Args:
        text: Raw markdown text

    Returns:
        Sanitized HTML string (safe for |safe in Jinja)
    """
    if not text:
        return ""

    html = md.markdown(text, extensions=["fenced_code"])

    html = bleach.clean(
        html,
        tags=ALLOWED_TAGS,
        attributes=ALLOWED_ATTRIBUTES,
        protocols=ALLOWED_PROTOCOLS,
        strip=True,
    )

    html = re.sub(
        r'<a href="',
        '<a target="_blank" rel="noopener noreferrer" href="',
        html,
    )

    return html

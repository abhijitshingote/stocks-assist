"""Convert market brief markdown to a PDF matching the dark formatted view."""

from __future__ import annotations

import markdown
from weasyprint import CSS, HTML

# Mirrors frontend/templates/base.html (:root dark) + market_brief.html (.markdown-body)
PDF_CSS = """
@page {
    size: letter;
    margin: 0.65in 0.6in;
    background: #0d1117;
}
html, body {
    background: #0d1117;
}
body {
    font-family: "Outfit", -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    font-size: 10.5pt;
    line-height: 1.8;
    color: #e6edf3;
    margin: 0;
    padding: 0;
}
.markdown-body {
    font-size: 1rem;
    line-height: 1.8;
}
.markdown-body h1 {
    font-size: 1.6rem;
    margin-top: 0;
    margin-bottom: 0.75rem;
    color: #3fb950;
    border-bottom: 1px solid #30363d;
    padding-bottom: 0.75rem;
    page-break-after: avoid;
}
.markdown-body h2 {
    font-size: 1.3rem;
    margin-top: 1.75rem;
    margin-bottom: 0.75rem;
    color: #58a6ff;
    font-weight: 600;
    page-break-after: avoid;
}
.markdown-body h3 {
    font-size: 1.1rem;
    margin-top: 1.25rem;
    margin-bottom: 0.5rem;
    color: #e6edf3;
    font-weight: 600;
    page-break-after: avoid;
}
.markdown-body p {
    margin-bottom: 1rem;
    letter-spacing: 0.3px;
    orphans: 3;
    widows: 3;
}
.markdown-body ul {
    padding-left: 1.75rem;
    margin-bottom: 1rem;
}
.markdown-body li {
    margin-bottom: 0.5rem;
    line-height: 1.8;
}
.markdown-body strong {
    color: #e6edf3;
    font-weight: 600;
}
.markdown-body em {
    color: #6e7681;
    font-style: italic;
}
.markdown-body code {
    font-family: "JetBrains Mono", "SF Mono", Menlo, Consolas, monospace;
    font-size: 0.9rem;
    background: #0d1117;
    padding: 0.15rem 0.4rem;
    border-radius: 3px;
    letter-spacing: 0.3px;
}
.markdown-body hr {
    border: none;
    border-top: 1px solid #30363d;
    margin: 1rem 0;
}
.markdown-body table {
    width: 100%;
    border-collapse: collapse;
    margin: 0.75rem 0 1rem;
    font-size: 0.7rem;
    font-family: "JetBrains Mono", "SF Mono", Menlo, Consolas, monospace;
    page-break-inside: avoid;
}
.markdown-body th,
.markdown-body td {
    padding: 0.35rem 0.5rem;
    text-align: left;
    border-bottom: 1px solid #30363d;
    vertical-align: top;
    color: #e6edf3;
}
.markdown-body th {
    color: #6e7681;
    font-weight: 600;
}
.markdown-body a {
    color: #58a6ff;
    text-decoration: none;
}
"""


def markdown_to_pdf_bytes(md: str, *, title: str) -> bytes:
    """Render markdown string to PDF bytes (dark theme, same structure as Formatted view)."""
    body_html = markdown.markdown(
        md,
        extensions=["tables", "fenced_code", "nl2br", "sane_lists"],
    )
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>{_escape_html(title)}</title>
</head>
<body>
  <div class="markdown-body">
  {body_html}
  </div>
</body>
</html>"""
    return HTML(string=html).write_pdf(stylesheets=[CSS(string=PDF_CSS)])


def _escape_html(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )

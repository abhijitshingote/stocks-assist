"""Static checks that the frontend remains mobile-friendly."""

from __future__ import annotations

import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATES_DIR = REPO_ROOT / "frontend" / "templates"
STATIC_RESPONSIVE = REPO_ROOT / "frontend" / "static" / "css" / "responsive.css"
BASE_TEMPLATE = TEMPLATES_DIR / "base.html"
FRONTEND_APP = REPO_ROOT / "frontend" / "app.py"

# Templates that intentionally use wide tables with horizontal scroll (no page @media required).
_WIDE_TABLE_ALLOWLIST: set[str] = set()

_MEDIA_QUERY_RE = re.compile(r"@media\s*\(")
_MIN_WIDTH_RE = re.compile(r"min-width:\s*(\d+)px")
_VIEWPORT_RE = re.compile(
    r'<meta\s+name=["\']viewport["\']\s+content=["\']width=device-width',
    re.IGNORECASE,
)
_RESPONSIVE_CSS_LINK_RE = re.compile(r"css/responsive\.css")


class TestFrontendResponsive(unittest.TestCase):
    def test_base_has_viewport_meta(self) -> None:
        content = BASE_TEMPLATE.read_text(encoding="utf-8")
        self.assertRegex(content, _VIEWPORT_RE, "base.html must include a mobile viewport meta tag")

    def test_base_links_shared_responsive_stylesheet(self) -> None:
        content = BASE_TEMPLATE.read_text(encoding="utf-8")
        self.assertRegex(
            content,
            _RESPONSIVE_CSS_LINK_RE,
            "base.html must link frontend/static/css/responsive.css",
        )
        self.assertTrue(STATIC_RESPONSIVE.is_file(), "responsive.css must exist on disk")

    def test_responsive_stylesheet_has_mobile_breakpoints(self) -> None:
        css = STATIC_RESPONSIVE.read_text(encoding="utf-8")
        self.assertIn("@media (max-width: 768px)", css)
        self.assertIn("@media (max-width: 480px)", css)

    def test_all_feature_templates_extend_base(self) -> None:
        for path in sorted(TEMPLATES_DIR.glob("*.html")):
            if path.name == "base.html":
                continue
            content = path.read_text(encoding="utf-8")
            self.assertIn(
                'extends "base.html"',
                content,
                f"{path.name} should extend base.html for shared nav and mobile CSS",
            )

    def test_templates_without_page_media_have_shared_css_or_allowlist(self) -> None:
        """Pages with large min-width layouts should have @media rules or shared responsive.css."""
        responsive_css = STATIC_RESPONSIVE.read_text(encoding="utf-8")
        missing: list[str] = []
        for path in sorted(TEMPLATES_DIR.glob("*.html")):
            if path.name in ("base.html",):
                continue
            content = path.read_text(encoding="utf-8")
            has_media = bool(_MEDIA_QUERY_RE.search(content))
            if has_media or path.name in _WIDE_TABLE_ALLOWLIST:
                continue
            # Heuristic: fixed min-width columns without any responsive strategy
            widths = [int(m.group(1)) for m in _MIN_WIDTH_RE.finditer(content)]
            if any(w >= 300 for w in widths):
                missing.append(path.name)
        self.assertEqual(
            [],
            missing,
            "Templates with min-width >= 300px need @media rules or an allowlist entry; missing: "
            + ", ".join(missing),
        )

    def test_rendered_routes_return_html_with_viewport(self) -> None:
        """Smoke-test key pages through Flask test client (no live server)."""
        import sys

        try:
            import flask  # noqa: F401
        except ImportError:
            self.skipTest("flask not installed in this environment")

        sys.path.insert(0, str(REPO_ROOT))
        from frontend.app import app  # noqa: WPS433

        client = app.test_client()
        routes = [
            "/",
            "/daily-review",
            "/volspike-gapper-90d",
            "/strong-stocks",
            "/daily-shortlist",
            "/daily-themes",
            "/main-view",
            "/rs-screener",
            "/market-brief",
            "/abi-dislikes",
            "/themes",
            "/stock/AAPL",
        ]
        for route in routes:
            resp = client.get(route)
            self.assertEqual(resp.status_code, 200, f"GET {route} should return 200")
            body = resp.get_data(as_text=True)
            self.assertRegex(
                body,
                _VIEWPORT_RE,
                f"GET {route} HTML should include viewport meta",
            )
            self.assertIn(
                "responsive.css",
                body,
                f"GET {route} HTML should link responsive.css",
            )


if __name__ == "__main__":
    unittest.main()

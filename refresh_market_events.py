import os
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BACKUP_DIR = Path(os.getenv("USER_DATA_DIR", "user_data"))
BACKUP_DIR.mkdir(exist_ok=True)
CACHE_FILE = BACKUP_DIR / "market_events.html"


def fetch_market_events_html() -> str:
    url = "https://api.perplexity.ai/chat/completions"
    headers = {
        "Authorization": f"Bearer {os.getenv('PERPLEXITY_API_KEY')}",
        "Content-Type": "application/json",
    }

    prompt = (
        "Identify the most significant upcoming market-moving events over the next "
        "1–2 weeks that could impact US and global equities. Prioritize central bank "
        "decisions (FOMC, ECB), key macro prints (CPI, PPI, jobs, GDP, PMIs), major "
        "earnings that can move indices or sectors, large options expirations, notable "
        "government actions, geopolitical events, and major index rebalances.\n\n"
        "Return event headings with date and time with very short descriptions "
        "if needed. Example: Aug 31 8:30 AM ET: FOMC Rate Decision\n\n"
        "Output format: a single <ul> with one <li> per event heading. No preamble, "
        "no code fences, no additional text. Limit to the top 12–16 events."
    )

    data = {
        "model": "sonar-pro",
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
    }

    resp = requests.post(url, headers=headers, json=data, timeout=60)
    resp.raise_for_status()
    result = resp.json()
    content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
    return content or "<p>No events available.</p>"


def write_cache(html: str) -> None:
    CACHE_FILE.write_text(html, encoding="utf-8")
    print(f"Wrote cached events to {CACHE_FILE}")


def main():
    html = fetch_market_events_html()
    write_cache(html)


if __name__ == "__main__":
    main()



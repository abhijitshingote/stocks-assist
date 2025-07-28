import json
import os
import requests
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import logging

# --------------------------------------------------
# Configuration & Setup
# --------------------------------------------------
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY")
if not PERPLEXITY_API_KEY:
    raise EnvironmentError("PERPLEXITY_API_KEY not found in environment variables.")

# Model to use – keep in sync with app.py default (sonar)
PERPLEXITY_MODEL = os.getenv("PERPLEXITY_MODEL", "sonar")

# Input / Output paths
DATA_PATH = Path(__file__).resolve().parent / "seachange-fixed.json"
OUTPUT_PATH = Path(__file__).resolve().parent / "seachange_perplexity_output.txt"

# --------------------------------------------------
# Helper: Perplexity API call (replicated from app.py)
# --------------------------------------------------
def call_perplexity(prompt: str) -> str:
    """Send a prompt to Perplexity.ai and return the response content (no streaming)."""
    url = "https://api.perplexity.ai/chat/completions"
    headers = {
        "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
        "Content-Type": "application/json",
    }
    request_data = {
        "model": PERPLEXITY_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
    }

    logger.debug("Querying Perplexity with payload: %s", request_data)
    response = requests.post(url, headers=headers, json=request_data, timeout=60)
    response.raise_for_status()
    data = response.json()

    # Basic validation
    try:
        return data["choices"][0]["message"]["content"]
    except (KeyError, IndexError) as err:
        logger.error("Unexpected response format from Perplexity: %s", data)
        raise RuntimeError("Invalid Perplexity response") from err

# --------------------------------------------------
# Build prompt for each ticker
# --------------------------------------------------
PROMPT_TEMPLATE = (
    "Provide a description of fundamental events that help explain why "
    "{ticker} ({company_name}) showed significant change in price on the following dates: {dates}. "
    "Focus exclusively on fundamental news such as earnings, guidance, product launches, regulatory actions, "
    "or macro factors. Avoid technical analysis or momentum commentary."\
)


def build_prompt(ticker: str, company_name: str, sea_change_dates: list[str]) -> str:
    date_list = ", ".join(sea_change_dates)
    return PROMPT_TEMPLATE.format(ticker=ticker, company_name=company_name, dates=date_list)


# --------------------------------------------------
# Main execution
# --------------------------------------------------

def main():
    logger.info("Loading sea-change dataset from %s", DATA_PATH)

    with DATA_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    logger.info("Loaded %d ticker records", len(data))

    # Prepare output file
    with OUTPUT_PATH.open("w", encoding="utf-8") as out_file:
        header = (
            "Sea-Change Perplexity Analysis\n"
            "Generated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
            "Model: " + PERPLEXITY_MODEL + "\n"
            + "=" * 80 + "\n\n"
        )
        out_file.write(header)

        for idx, entry in enumerate(data[:2], start=1):
            ticker = entry.get("ticker")
            company_name = entry.get("company_name", "N/A")
            sea_dates = entry.get("sea_change_dates", [])

            if not ticker or not sea_dates:
                logger.warning("Skipping invalid record at index %d: %s", idx - 1, entry)
                continue

            logger.info("(%d/%d) Processing %s", idx, len(data), ticker)

            prompt = build_prompt(ticker, company_name, sea_dates)

            try:
                response_text = call_perplexity(prompt)
            except Exception as e:
                logger.error("Failed to fetch analysis for %s: %s", ticker, e)
                response_text = f"Error: {e}"

            # Write to output text file
            out_file.write(f"# {ticker} – {company_name}\n")
            out_file.write("Sea-Change Dates: " + ", ".join(sea_dates) + "\n\n")
            out_file.write(response_text.strip() + "\n\n")
            out_file.write("-" * 80 + "\n\n")

    logger.info("Analysis complete. Output written to %s", OUTPUT_PATH)


if __name__ == "__main__":
    main() 
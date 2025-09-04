import os
import csv
import sys
import time
import argparse
from typing import List, Dict, Tuple

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv


def load_api_key() -> str:
    """Load FMP API key from environment; raise with clear message if missing."""
    load_dotenv()
    api_key = os.getenv("FMP_API_KEY")
    if not api_key:
        raise ValueError(
            "FMP_API_KEY not found in environment variables. Please add it to your .env file."
        )
    return api_key

def ensure_data_directory() -> str:
    """Ensure data directory exists and return its path."""
    data_dir = os.path.join("db_scripts", "data")
    os.makedirs(data_dir, exist_ok=True)
    return data_dir


def fetch_with_retry(url: str, retries: int = 2, delay_seconds: float = 1.0) -> Tuple[bool, List[Dict]]:
    """Simple GET with a couple of retries. Returns (ok, data)."""
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, timeout=20)
            if resp.ok:
                try:
                    return True, resp.json()
                except Exception:
                    # Non-JSON
                    print(f"Non-JSON response from {url[:80]}... status={resp.status_code}")
                    print(resp.text[:300])
                    return False, []
            else:
                print(f"HTTP {resp.status_code} from {url}")
                snippet = (resp.text or "")[:200]
                if snippet:
                    print(f"Body: {snippet}")
            # Non-200 response
            time.sleep(delay_seconds)
        except Exception:
            print(f"Request error on {url}, retrying...")
            time.sleep(delay_seconds)
    return False, []


def get_html(url: str, timeout: int = 25) -> str:
    headers = {
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    resp = requests.get(url, headers=headers, timeout=timeout)
    if not resp.ok:
        print(f"HTTP {resp.status_code} getting {url}")
        return ""
    return resp.text


def fetch_sp500_constituents(api_key: str) -> List[Dict[str, str]]:
    base = "https://financialmodelingprep.com/api/v3"
    url = f"{base}/sp500_constituent?apikey={api_key}"
    ok, data = fetch_with_retry(url)
    if ok and isinstance(data, list) and data:
        print(f"FMP S&P 500 returned {len(data)} rows")
        return [{"symbol": row.get("symbol", "").strip(), "name": row.get("name", "").strip()} for row in data]
    else:
        if ok:
            print(f"FMP S&P 500 response type: {type(data)} keys: {list(data[0].keys()) if isinstance(data, list) and data else 'n/a'}")
        print("Falling back to Wikipedia (S&P 500)...")
    # Fallbacks
    rows = fetch_sp500_wiki_bs4()
    if rows:
        return rows
    print("Falling back to SlickCharts (S&P 500)...")
    return fetch_sp500_slickcharts()


def fetch_nasdaq100_constituents(api_key: str) -> List[Dict[str, str]]:
    base = "https://financialmodelingprep.com/api/v3"
    # FMP has nasdaq_constituent for Nasdaq 100
    url = f"{base}/nasdaq_constituent?apikey={api_key}"
    ok, data = fetch_with_retry(url)
    if ok and isinstance(data, list) and data:
        print(f"FMP Nasdaq 100 returned {len(data)} rows")
        return [{"symbol": row.get("symbol", "").strip(), "name": row.get("name", "").strip()} for row in data]
    else:
        if ok:
            print(f"FMP Nasdaq 100 response type: {type(data)} keys: {list(data[0].keys()) if isinstance(data, list) and data else 'n/a'}")
        print("Falling back to Wikipedia (Nasdaq 100)...")
    # Fallbacks
    rows = fetch_nasdaq100_wiki_bs4()
    if rows:
        return rows
    print("Falling back to SlickCharts (Nasdaq 100)...")
    return fetch_nasdaq100_slickcharts()


def fetch_sp500_wiki_bs4() -> List[Dict[str, str]]:
    # Try the REST HTML first (more stable markup)
    rest_url = "https://en.wikipedia.org/api/rest_v1/page/html/List_of_S%26P_500_companies"
    try:
        resp = requests.get(rest_url, timeout=25, headers={"accept": "text/html", "user-agent": "Mozilla/5.0"})
        if resp.ok:
            soup = BeautifulSoup(resp.text, "html.parser")
            table = None
            # Prefer table with id-like attribute containing 'constituent'
            for t in soup.find_all("table"):
                header_text = " ".join([th.get_text(strip=True) for th in t.find_all("th")]).lower()
                if "symbol" in header_text and ("security" in header_text or "company" in header_text):
                    table = t
                    break
            if table is not None:
                rows = []
                for tr in table.find_all("tr"):
                    tds = tr.find_all("td")
                    if len(tds) < 2:
                        continue
                    symbol = tds[0].get_text(strip=True)
                    name = tds[1].get_text(strip=True)
                    if symbol and symbol.lower() != "symbol":
                        rows.append({"symbol": symbol, "name": name})
                if rows:
                    return rows
    except Exception:
        pass

    # Fallback to classic HTML
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        resp = requests.get(url, timeout=25, headers={"user-agent": "Mozilla/5.0"})
        if not resp.ok:
            raise RuntimeError("wiki resp not ok")
        soup = BeautifulSoup(resp.text, "html.parser")
        table = None
        for t in soup.select("table.wikitable"):
            headers = [th.get_text(strip=True).lower() for th in (t.select("thead th") or t.select("tr th"))]
            if "symbol" in headers and ("security" in headers or "company" in headers):
                table = t
                break
        if table:
            rows = []
            for tr in table.select("tbody tr"):
                cells = [td.get_text(strip=True) for td in tr.select("td")]
                if len(cells) >= 2 and cells[0] and cells[0].lower() != "symbol":
                    rows.append({"symbol": cells[0], "name": cells[1]})
            if rows:
                return rows
    except Exception:
        pass

    # Last-resort: pandas read_html
    try:
        import pandas as pd
        tables = pd.read_html(url)
        if tables:
            df = tables[0]
            if "Symbol" in df.columns and ("Security" in df.columns or "Company" in df.columns):
                name_col = "Security" if "Security" in df.columns else "Company"
                return [
                    {"symbol": str(sym).strip(), "name": str(nm).strip()}
                    for sym, nm in zip(df["Symbol"], df[name_col])
                    if str(sym) != "nan"
                ]
    except Exception:
        pass
    return []


def fetch_nasdaq100_wiki_bs4() -> List[Dict[str, str]]:
    # Try REST HTML first
    rest_url = "https://en.wikipedia.org/api/rest_v1/page/html/Nasdaq-100"
    try:
        resp = requests.get(rest_url, timeout=25, headers={"accept": "text/html", "user-agent": "Mozilla/5.0"})
        if resp.ok:
            soup = BeautifulSoup(resp.text, "html.parser")
            table = None
            for t in soup.find_all("table"):
                header_text = " ".join([th.get_text(strip=True) for th in t.find_all("th")]).lower()
                if "ticker" in header_text or "symbol" in header_text:
                    table = t
                    break
            if table is not None:
                # Infer indices
                headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
                def index_of(options):
                    for i, h in enumerate(headers):
                        if h in options:
                            return i
                    return None
                sym_idx = index_of({"ticker", "symbol"})
                name_idx = index_of({"company", "name"})
                rows = []
                for tr in table.find_all("tr"):
                    tds = tr.find_all("td")
                    if not tds:
                        continue
                    sym = tds[sym_idx].get_text(strip=True) if sym_idx is not None and sym_idx < len(tds) else ""
                    name = tds[name_idx].get_text(strip=True) if name_idx is not None and name_idx < len(tds) else ""
                    if sym:
                        rows.append({"symbol": sym, "name": name})
                if rows:
                    return rows
    except Exception:
        pass

    # Fallback classic HTML
    url = "https://en.wikipedia.org/wiki/Nasdaq-100"
    try:
        resp = requests.get(url, timeout=25, headers={"user-agent": "Mozilla/5.0"})
        if not resp.ok:
            raise RuntimeError("wiki resp not ok")
        soup = BeautifulSoup(resp.text, "html.parser")
        table = None
        for t in soup.select("table.wikitable"):
            headers = [th.get_text(strip=True).lower() for th in (t.select("thead th") or t.select("tr th"))]
            if any(h in ("ticker", "symbol") for h in headers):
                table = t
                break
        if table:
            header_cells = [th.get_text(strip=True).lower() for th in table.select("tr th")]
            def idx_of(options):
                for i, h in enumerate(header_cells):
                    if h in options:
                        return i
                return None
            sym_idx = idx_of({"ticker", "symbol"})
            name_idx = idx_of({"company", "name"})
            rows = []
            for tr in table.select("tbody tr"):
                tds = tr.select("td")
                if not tds:
                    continue
                sym = tds[sym_idx].get_text(strip=True) if sym_idx is not None and sym_idx < len(tds) else ""
                name = tds[name_idx].get_text(strip=True) if name_idx is not None and name_idx < len(tds) else ""
                if sym:
                    rows.append({"symbol": sym, "name": name})
            if rows:
                return rows
    except Exception:
        pass

    # Last-resort: pandas read_html
    try:
        import pandas as pd
        tables = pd.read_html(url)
        if tables:
            df = tables[0]
            sym_col = "Ticker" if "Ticker" in df.columns else ("Symbol" if "Symbol" in df.columns else None)
            name_col = "Company" if "Company" in df.columns else ("Name" if "Name" in df.columns else None)
            if sym_col:
                return [
                    {"symbol": str(sym).strip(), "name": str(nm).strip() if name_col else ""}
                    for sym, nm in zip(df[sym_col], df[name_col] if name_col else df[sym_col])
                    if str(sym) != "nan"
                ]
    except Exception:
        pass
    return []


# Additional fallback: SlickCharts HTML (commonly accessible)
def fetch_sp500_slickcharts() -> List[Dict[str, str]]:
    html = get_html("https://www.slickcharts.com/sp500")
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []
    headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
    try:
        sym_idx = headers.index("symbol")
    except ValueError:
        sym_idx = None
    name_idx = None
    for label in ("company", "name"):
        if label in headers:
            name_idx = headers.index(label)
            break
    rows: List[Dict[str, str]] = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        sym = (tds[sym_idx].get_text(strip=True) if sym_idx is not None and sym_idx < len(tds) else "").strip()
        nm = (tds[name_idx].get_text(strip=True) if name_idx is not None and name_idx < len(tds) else "").strip()
        if sym:
            rows.append({"symbol": sym, "name": nm})
    return rows


def fetch_nasdaq100_slickcharts() -> List[Dict[str, str]]:
    html = get_html("https://www.slickcharts.com/nasdaq100")
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []
    headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
    try:
        sym_idx = headers.index("symbol")
    except ValueError:
        sym_idx = None
    name_idx = None
    for label in ("company", "name"):
        if label in headers:
            name_idx = headers.index(label)
            break
    rows: List[Dict[str, str]] = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        sym = (tds[sym_idx].get_text(strip=True) if sym_idx is not None and sym_idx < len(tds) else "").strip()
        nm = (tds[name_idx].get_text(strip=True) if name_idx is not None and name_idx < len(tds) else "").strip()
        if sym:
            rows.append({"symbol": sym, "name": nm})
    return rows


def write_csv(path: str, rows: List[Dict[str, str]]) -> int:
    if not rows:
        return 0
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["symbol", "name"])
        writer.writeheader()
        for r in rows:
            writer.writerow({"symbol": r.get("symbol", ""), "name": r.get("name", "")})
    return len(rows)


def main():
    data_dir = ensure_data_directory()
    default_sp500 = os.path.join(data_dir, "sp500_list.csv")
    default_nasdaq100 = os.path.join(data_dir, "nasdaq100_list.csv")

    parser = argparse.ArgumentParser(description="Build S&P 500 and Nasdaq 100 stock lists (CSV) using FMP API with Wikipedia fallback.")
    parser.add_argument("--sp500-out", default=default_sp500, help=f"Output CSV path for S&P 500 (default: {default_sp500})")
    parser.add_argument("--nasdaq100-out", default=default_nasdaq100, help=f"Output CSV path for Nasdaq 100 (default: {default_nasdaq100})")
    args = parser.parse_args()

    try:
        api_key = load_api_key()
    except Exception as e:
        print(str(e))
        sys.exit(1)

    print("Fetching S&P 500 constituents...")
    sp = fetch_sp500_constituents(api_key)
    print(f"Found {len(sp)} S&P 500 constituents")
    n_sp = write_csv(args.sp500_out, sp)
    print(f"Wrote {n_sp} rows to {args.sp500_out}")

    print("\nFetching Nasdaq 100 constituents...")
    nq = fetch_nasdaq100_constituents(api_key)
    print(f"Found {len(nq)} Nasdaq 100 constituents")
    n_nq = write_csv(args.nasdaq100_out, nq)
    print(f"Wrote {n_nq} rows to {args.nasdaq100_out}")

    if n_sp == 0 or n_nq == 0:
        print("\nWarning: One or more lists are empty. Check API key, network, or fallback parsing.")


if __name__ == "__main__":
    main()



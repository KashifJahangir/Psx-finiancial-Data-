#!/usr/bin/env python3
"""
PSX Fundamental Scraper -> JSON   (ALL listed securities version)

Adapted from the KSE-100 scraper. The parsing logic is unchanged; what's new:

* Symbols are read from psx_all_stocks.json (570 securities) instead of a
  hardcoded KSE-100 list. Each result keeps the `in_kse100` flag.
* --resume       : reuse an existing output file, only re-scrape missing/failed symbols
* --kse100-only  : scrape just the 100 KSE-100 stocks (old behaviour)
* --equity-only  : skip rights, preference shares, ETFs, REITs, GEM-board etc.
* --symbols A B  : scrape only specific symbols (handy for a quick test)
* Progress is saved to disk every SAVE_EVERY stocks, so a crash / Ctrl+C loses nothing.
* 404s are not retried (delisted / dead symbols fail fast).

Usage
-----
    pip install requests beautifulsoup4 lxml
    python psx_all_scraper.py --symbols HBL LUCK OGDC     # quick test first
    python psx_all_scraper.py                             # whole market
    python psx_all_scraper.py --resume                    # continue after an interruption
"""

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

# ------------------------------------------------------------------
# config
# ------------------------------------------------------------------
INPUT_JSON = "psx_all_stocks.json"
OUTPUT_JSON = "PSX_All_Fundamentals.json"
MAX_WORKERS = 5
REQUEST_DELAY = 1.0
TIMEOUT = 30
RETRIES = 3
SAVE_EVERY = 25

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

NUM_RE = re.compile(r"^[\d,]+(?:\.\d+)?$")
YEAR_RE = re.compile(r"\b(?:19|20)\d{2}\b")
QUARTER_RE = re.compile(r"\bQ[1-4]\b", re.I)

# names that indicate a non-ordinary-share security
NON_EQUITY_RE = re.compile(
    r"\(right\)|\bright\b|\(pref\)|\bpref\b|preference|\bconvt\b|convertible|"
    r"\betf\b|exchange traded fund|\breit\b|\(gem\)|\(prs\)|\bspac-|"
    r"non-voting|\(c\)$",
    re.I,
)


class NotFound(Exception):
    """Raised on HTTP 404 so we don't waste retries."""


# ------------------------------------------------------------------
# helpers (unchanged from your fixed version)
# ------------------------------------------------------------------
def norm(s):
    """Collapse all whitespace (incl. non-breaking) to single spaces."""
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def clean_number(text):
    """'1,234.5' -> 1234.5 ; '(18.71)' -> -18.71 ; '-' / '' -> None"""
    text = norm(text)
    if text in ("", "-", "—", "–", "N/A", "n/a", "NA"):
        return None
    neg = text.startswith("(") and text.endswith(")")
    text = text.replace(",", "").replace("(", "").replace(")", "").replace("%", "").strip()
    try:
        val = float(text)
    except ValueError:
        return None
    return -val if neg else val


def table_kind(table):
    """Return ('annual'|'quarterly', periods) based on the header row, else None."""
    rows = table.find_all("tr")
    if len(rows) < 2:
        return None
    header = [norm(c.get_text(" ", strip=True)) for c in rows[0].find_all(["th", "td"])]
    periods = header[1:]
    if not periods or not any(periods):
        return None
    if all(QUARTER_RE.search(p) and YEAR_RE.search(p) for p in periods if p):
        return "quarterly", periods
    if all(YEAR_RE.search(p) and not QUARTER_RE.search(p) for p in periods if p):
        return "annual", periods
    return None


def map_metric(label):
    m = norm(label).lower()
    if "gross profit margin" in m:
        return "Gross_Profit_Margin"
    if "net profit margin" in m:
        return "Net_Profit_Margin"
    if "eps growth" in m:
        return "EPS_Growth"
    if m.startswith("peg"):
        return "PEG"
    if re.match(r"^(eps|earnings per share)\b", m):
        return "EPS"
    if "profit after" in m:
        return "PAT"
    if any(x in m for x in ("sales", "total income", "mark-up earned", "markup earned",
                            "revenue", "premium")):
        return "Sales"
    return None


def parse_table(table, periods):
    """Return {period: {metric: value}} for one financial / ratio table."""
    out = {p: {} for p in periods if p}
    rows = table.find_all("tr")[1:]
    first_row_done = False
    for row in rows:
        cells = [norm(c.get_text(" ", strip=True)) for c in row.find_all(["th", "td"])]
        if len(cells) < 2:
            continue
        key = map_metric(cells[0])
        # First row of the Financials table is always revenue, even when a
        # bank / insurer labels it differently (e.g. "Mark-up Earned").
        if key is None and not first_row_done:
            key = "Sales"
        first_row_done = True
        if not key:
            continue
        for i, p in enumerate(periods):
            if p and i < len(cells) - 1:
                out[p][key] = clean_number(cells[i + 1])
    return out


def parse_financials(soup):
    annual, quarterly = {}, {}
    for tbl in soup.find_all("table"):
        info = table_kind(tbl)
        if not info:
            continue
        kind, periods = info
        data = parse_table(tbl, periods)
        target = annual if kind == "annual" else quarterly
        for period, metrics in data.items():
            target.setdefault(period, {}).update(metrics)
    return annual, quarterly


def first_number_after(soup, label_regex, max_nodes=8):
    """DOM fallback: find a label and return the first numeric text node after it."""
    node = soup.find(string=re.compile(label_regex, re.I))
    if not node:
        return None
    for s in node.find_all_next(string=True, limit=max_nodes):
        t = norm(s)
        if t and NUM_RE.match(t):
            return clean_number(t)
    return None


def parse_equity(soup, text, result):
    m = re.search(r"Market\s*Cap\s*\(\s*000.{0,3}\)\s*([\d,]+(?:\.\d+)?)", text, re.I)
    result["market_cap_000s"] = (clean_number(m.group(1)) if m
                                 else first_number_after(soup, r"Market\s*Cap"))

    m = re.search(r"(?:^|\n)\s*Shares\s*([\d,]+)", text)
    result["total_shares"] = (clean_number(m.group(1)) if m
                              else first_number_after(soup, r"^\s*Shares\s*$"))

    m = re.search(r"Free\s*Float\s*([\d,]+)\s*Free\s*Float\s*([\d.]+)\s*%", text, re.I)
    if m:
        result["free_float"] = clean_number(m.group(1))
        result["free_float_pct"] = clean_number(m.group(2))

    m = re.search(r"Fiscal\s*Year\s*End\s*([A-Za-z]+)", text, re.I)
    if m:
        result["fiscal_year_end"] = m.group(1).strip()


def parse_profile(soup, result):
    desc = soup.find(string=re.compile(r"BUSINESS DESCRIPTION", re.I))
    if desc:
        parent = desc.find_parent()
        nxt = parent.find_next_sibling() if parent else None
        if nxt:
            result["company_profile"] = norm(nxt.get_text(" ", strip=True))[:2000]

    key_section = soup.find(string=re.compile(r"KEY PEOPLE", re.I))
    if key_section:
        table = key_section.find_parent().find_next("table")
        if table:
            people = []
            for tr in table.find_all("tr"):
                cols = [norm(td.get_text(strip=True)) for td in tr.find_all("td")]
                if len(cols) >= 2:
                    people.append({"name": cols[0], "designation": cols[1]})
            result["key_people"] = people


# ------------------------------------------------------------------
# scrape one company
# ------------------------------------------------------------------
def scrape_one(stock):
    symbol, name = stock["symbol"], stock["name"]
    url = f"https://dps.psx.com.pk/company/{symbol}"
    result = {
        "symbol": symbol, "name": name, "url": url,
        "in_kse100": stock.get("in_kse100", False),
        "market_cap_000s": None, "total_shares": None,
        "free_float": None, "free_float_pct": None,
        "fiscal_year_end": None, "company_profile": None, "key_people": None,
        "annual": {}, "quarterly": {}, "error": None,
    }
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if resp.status_code == 404:
                raise NotFound("HTTP 404")
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
            text = soup.get_text("\n", strip=True)

            parse_equity(soup, text, result)
            parse_profile(soup, result)
            result["annual"], result["quarterly"] = parse_financials(soup)
            last_err = None
            break
        except NotFound as e:
            last_err = str(e)
            break  # no point retrying
        except Exception as e:  # network / parse problem -> retry
            last_err = str(e)[:150]
            time.sleep(2 * attempt)
    result["error"] = last_err
    time.sleep(REQUEST_DELAY)
    return result


# ------------------------------------------------------------------
# input / output
# ------------------------------------------------------------------
def load_stocks(args):
    with open(INPUT_JSON, encoding="utf-8") as f:
        stocks = json.load(f)["stocks"]

    if args.kse100_only:
        stocks = [s for s in stocks if s.get("in_kse100")]
    if args.equity_only:
        stocks = [s for s in stocks
                  if s.get("in_kse100") or not NON_EQUITY_RE.search(s["name"])]
    if args.symbols:
        wanted = {x.upper() for x in args.symbols}
        stocks = [s for s in stocks if s["symbol"].upper() in wanted]
    return stocks


def save(results, order, total):
    results_sorted = sorted(results.values(), key=lambda x: order.get(x["symbol"], 99999))
    product = {
        "index": "PSX-ALL",
        "source": "PSX Data Portal (dps.psx.com.pk)",
        "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(results_sorted),
        "target_count": total,
        "stocks": results_sorted,
    }
    tmp = OUTPUT_JSON + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(product, f, indent=2, ensure_ascii=False)
    os.replace(tmp, OUTPUT_JSON)  # atomic: never leaves a half-written file


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--resume", action="store_true", help="skip symbols already scraped OK")
    ap.add_argument("--kse100-only", action="store_true")
    ap.add_argument("--equity-only", action="store_true")
    ap.add_argument("--symbols", nargs="+")
    args = ap.parse_args()

    if not os.path.exists(INPUT_JSON):
        sys.exit(f"Missing {INPUT_JSON} - put it in the same folder as this script.")

    stocks = load_stocks(args)
    order = {s["symbol"]: i for i, s in enumerate(stocks)}

    results = {}
    if args.resume and os.path.exists(OUTPUT_JSON):
        with open(OUTPUT_JSON, encoding="utf-8") as f:
            for r in json.load(f).get("stocks", []):
                if not r.get("error"):  # retry only the failed ones
                    results[r["symbol"]] = r
        print(f"Resuming: {len(results)} already done.")

    todo = [s for s in stocks if s["symbol"] not in results]
    print(f"Scraping {len(todo)} of {len(stocks)} securities...\n")

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(scrape_one, s): s for s in todo}
            for i, fut in enumerate(as_completed(futures), 1):
                res = fut.result()
                results[res["symbol"]] = res
                status = "OK" if not res["error"] else f"ERR -> {res['error']}"
                print(f"[{i:3d}/{len(todo)}] {res['symbol']:10s} {status}")
                if i % SAVE_EVERY == 0:
                    save(results, order, len(stocks))
    except KeyboardInterrupt:
        print("\nInterrupted - saving progress. Re-run with --resume to continue.")
    finally:
        save(results, order, len(stocks))

    rows = list(results.values())

    def missing(field):
        return [r["symbol"] for r in rows if not r[field]]

    no_eps = [r["symbol"] for r in rows
              if not any("EPS" in m for m in r["annual"].values())]
    print(f"\nDone -> {OUTPUT_JSON}  ({len(rows)} securities)")
    print("No market cap :", len(missing("market_cap_000s")), "symbols")
    print("No annual EPS :", len(no_eps), "symbols")
    print("Fetch errors  :", [r["symbol"] for r in rows if r["error"]] or "none")


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
PSX Fundamental Scraper -> JSON (Enhanced with Balance Sheet, Cash Flow, 52-Wk Range & Yahoo Backup)

Features:
- Scrapes dps.psx.com.pk for all listed securities
- Extracts Balance Sheet (Assets, Debt, Equity, ROE, ROA, Cash Flow)
- Extracts 52-Week High & Low prices
- Yahoo Finance backup fallback if PSX table is missing specific fields
- Multi-threaded, atomic saves, resume support
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

try:
    import yfinance as yf
except ImportError:
    yf = None  # Optional fallback

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
INPUT_JSON = "psx_all_stocks.json"
OUTPUT_JSON = "PSX_All_Fundamentals.json"
MAX_WORKERS = 5
REQUEST_DELAY = 0.8
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

NON_EQUITY_RE = re.compile(
    r"\(right\)|\bright\b|\(pref\)|\bpref\b|preference|\bconvt\b|convertible|"
    r"\betf\b|exchange traded fund|\breit\b|\(gem\)|\(prs\)|\bspac-|"
    r"non-voting|\(c\)$",
    re.I,
)


class NotFound(Exception):
    """Raised on HTTP 404 so we don't waste retries."""


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def norm(s):
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def clean_number(text):
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
    """Maps table row labels to standard JSON financial keys."""
    m = norm(label).lower()
    
    # Income Statement
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
    if any(x in m for x in ("profit after tax", "net profit", "pat")):
        return "PAT"
    if any(x in m for x in ("sales", "total income", "mark-up earned", "markup earned", "revenue", "premium")):
        return "Sales"

    # Balance Sheet & Cash Flow (NEW)
    if any(x in m for x in ("total asset", "total assets")):
        return "Total_Assets"
    if any(x in m for x in ("total debt", "total liabilities & debt", "debt")):
        return "Total_Debt"
    if "free cash flow" in m:
        return "Free_Cash_Flow"
    if "operating cash flow" in m or "cash from operations" in m:
        return "Operating_Cash_Flow"
    if "debt to equity" in m or "debt/equity" in m or "d/e" in m:
        return "Debt_To_Equity"
    if "return on equity" in m or "roe" in m:
        return "Return_On_Equity"
    if "return on assets" in m or "roa" in m:
        return "Return_On_Assets"
    if "current ratio" in m:
        return "Current_Ratio"

    return None


def parse_table(table, periods):
    out = {p: {} for p in periods if p}
    rows = table.find_all("tr")[1:]
    first_row_done = False
    for row in rows:
        cells = [norm(c.get_text(" ", strip=True)) for c in row.find_all(["th", "td"])]
        if len(cells) < 2:
            continue
        key = map_metric(cells[0])
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

    # Parse 52-Week High & Low (NEW)
    m_high = re.search(r"52\s*Week\s*High\s*:?\s*([\d,]+(?:\.\d+)?)", text, re.I)
    result["52_Week_High"] = (clean_number(m_high.group(1)) if m_high
                              else first_number_after(soup, r"52\s*Week\s*High"))

    m_low = re.search(r"52\s*Week\s*Low\s*:?\s*([\d,]+(?:\.\d+)?)", text, re.I)
    result["52_Week_Low"] = (clean_number(m_low.group(1)) if m_low
                             else first_number_after(soup, r"52\s*Week\s*Low"))

    m = re.search(r"Fiscal\s*Year\s*End\s*([A-Za-z]+)", text, re.I)
    if m:
        result["fiscal_year_end"] = m.group(1).strip()


def enrich_with_yahoo_backup(symbol, result):
    """Yahoo Finance Backup Fallback for missing 52-week range or balance sheet data."""
    if not yf:
        return
    try:
        ticker = yf.Ticker(f"{symbol}.KA")
        info = ticker.info
        if not result.get("52_Week_High"):
            result["52_Week_High"] = info.get("fiftyTwoWeekHigh")
        if not result.get("52_Week_Low"):
            result["52_Week_Low"] = info.get("fiftyTwoWeekLow")

        # Fill annual balance sheet if missing
        latest_year = list(result["annual"].keys())[0] if result["annual"] else "2024"
        ann = result["annual"].setdefault(latest_year, {})

        bs = ticker.balance_sheet
        if not bs.empty and "Total Assets" not in ann:
            latest_bs = bs.iloc[:, 0]
            ann["Total_Assets"] = float(latest_bs.get("Total Assets", 0) or 0) / 1000.0
            ann["Total_Debt"] = float(latest_bs.get("Total Debt", 0) or 0) / 1000.0

        cf = ticker.cashflow
        if not cf.empty and "Free_Cash_Flow" not in ann:
            ann["Free_Cash_Flow"] = float(cf.iloc[:, 0].get("Free Cash Flow", 0) or 0) / 1000.0
    except Exception:
        pass


# ------------------------------------------------------------------
# Scrape One Stock
# ------------------------------------------------------------------
def scrape_one(stock):
    symbol, name = stock["symbol"], stock["name"]
    url = f"https://dps.psx.com.pk/company/{symbol}"
    result = {
        "symbol": symbol, "name": name, "url": url,
        "in_kse100": stock.get("in_kse100", False),
        "market_cap_000s": None, "total_shares": None,
        "free_float": None, "free_float_pct": None,
        "52_Week_High": None, "52_Week_Low": None,
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
            result["annual"], result["quarterly"] = parse_financials(soup)
            
            # Use Yahoo Finance backup for any missing metrics
            enrich_with_yahoo_backup(symbol, result)
            
            last_err = None
            break
        except NotFound as e:
            last_err = str(e)
            break
        except Exception as e:
            last_err = str(e)[:150]
            time.sleep(2 * attempt)
            
    result["error"] = last_err
    time.sleep(REQUEST_DELAY)
    return result


# ------------------------------------------------------------------
# Execution & Save
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
    os.replace(tmp, OUTPUT_JSON)


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
                if not r.get("error"):
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
    print(f"\nDone -> {OUTPUT_JSON} ({len(rows)} securities)")


if __name__ == "__main__":
    main()

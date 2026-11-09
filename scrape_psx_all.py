#!/usr/bin/env python3
"""
PSX Fundamental Scraper -> JSON

Per company it collects:
  * PSX (dps.psx.com.pk): market cap, shares, free float, 52-week range,
    fiscal year end, business description, key people, annual/quarterly
    Sales / PAT / EPS / ratios
  * stockanalysis.com (fallback: Yahoo Finance): full balance sheet,
    cash flow statement and income statement (annual, values in millions PKR)

NOTE: dps.psx.com.pk does NOT publish balance sheet / cash flow tables,
so those come from a second source.

Usage:
  python psx_scraper.py --symbols AABS          # test one stock first
  python psx_scraper.py --resume
  python psx_scraper.py --no-statements         # PSX page only (fast)
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
    yf = None

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

STATEMENT_PAGES = {
    "balance_sheet": "balance-sheet",
    "cash_flow": "cash-flow-statement",
    "income_statement": "",  # base /financials/ page
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
    pass


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def norm(s):
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def clean_number(text):
    text = norm(str(text) if text is not None else "")
    if text in ("", "-", "—", "–", "N/A", "n/a", "NA", "Upgrade"):
        return None
    neg = (text.startswith("(") and text.endswith(")")) or text.startswith("-")
    text = (text.replace(",", "").replace("(", "").replace(")", "")
            .replace("%", "").replace("-", "").strip())
    try:
        val = float(text)
    except ValueError:
        return None
    return -val if neg else val


def snake(label):
    s = re.sub(r"[^A-Za-z0-9]+", "_", norm(label)).strip("_")
    return s


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
    if any(x in m for x in ("profit after tax", "net profit", "pat")):
        return "PAT"
    if any(x in m for x in ("sales", "total income", "mark-up earned",
                            "markup earned", "revenue", "premium")):
        return "Sales"
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


# ------------------------------------------------------------------
# PSX page: equity info, 52-week range, profile, key people
# ------------------------------------------------------------------
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

    # 52-week range: PSX shows "52-WEEK RANGE  low  —  high"
    m = re.search(
        r"52[\s-]*WEEK\s*RANGE\D{0,15}?([\d,]+(?:\.\d+)?)\D{1,15}?([\d,]+(?:\.\d+)?)",
        text, re.I)
    if m:
        lo, hi = clean_number(m.group(1)), clean_number(m.group(2))
        if lo is not None and hi is not None:
            result["52_Week_Low"], result["52_Week_High"] = min(lo, hi), max(lo, hi)

    m = re.search(r"Fiscal\s*Year\s*End\s*([A-Za-z]+)", text, re.I)
    if m:
        result["fiscal_year_end"] = m.group(1).strip()


def parse_profile(soup, text, result):
    m = re.search(r"BUSINESS\s*DESCRIPTION\s*(.*?)\s*KEY\s*PEOPLE", text, re.S | re.I)
    if m:
        desc = norm(m.group(1))
        result["company_profile"] = desc or None

    node = soup.find(string=re.compile(r"^\s*KEY\s*PEOPLE\s*$", re.I))
    if node:
        tbl = node.find_next("table")
        people = []
        if tbl:
            for row in tbl.find_all("tr"):
                cells = [norm(c.get_text(" ", strip=True)) for c in row.find_all(["th", "td"])]
                if len(cells) >= 2 and cells[0].lower() not in ("name", ""):
                    people.append({"name": cells[0], "position": cells[1]})
        result["key_people"] = people or None


# ------------------------------------------------------------------
# Dividends: DPS, Dividend_Yield, Payout_Ratio (per fiscal year)
# ------------------------------------------------------------------
# NOTE: the Payouts table on the PSX company page is loaded by JavaScript,
# so it is EMPTY in the HTML that requests downloads. We call the same
# endpoint the page's JavaScript calls. If PSX changes it, find the new URL
# in your browser: open the company page -> F12 -> Network -> Fetch/XHR ->
# reload -> click the request that returns the payouts table, then put its
# URL / method / form fields into PAYOUT_ENDPOINTS below.
PAYOUT_ENDPOINTS = [
    # (method, url, form-field name that carries the symbol)
    ("POST", "https://dps.psx.com.pk/company/payouts", "symbol"),
    ("POST", "https://dps.psx.com.pk/payouts", "symbol"),
]

PAYOUT_RE = re.compile(r"([\d,]+(?:\.\d+)?)\s*%\s*((?:\(\s*[A-Za-z]+\s*\)\s*)+)")
PERIOD_RE = re.compile(r"\d{1,2}/\d{1,2}/(?:19|20)\d{2}\s*\([^)]*\)")   # 31/03/2024(IIIQ)
DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/((?:19|20)\d{2})")
MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"], 1)}


def fetch_payouts_soup(symbol):
    """Calls PSX's payouts endpoint (what the page's JavaScript does)."""
    hdrs = {**HEADERS, "X-Requested-With": "XMLHttpRequest",
            "Referer": f"https://dps.psx.com.pk/company/{symbol}"}
    for method, url, field in PAYOUT_ENDPOINTS:
        try:
            if method == "POST":
                r = requests.post(url, data={field: symbol}, headers=hdrs, timeout=TIMEOUT)
            else:
                r = requests.get(url, params={field: symbol}, headers=hdrs, timeout=TIMEOUT)
            if r.status_code == 200 and "<tr" in r.text.lower():
                return BeautifulSoup(r.text, "lxml")
        except Exception:
            continue
    return None


def extract_payout_rows(soup):
    """Content-based: a payout row has a 'dd/mm/yyyy(IIIQ)' cell and a '25%(iii) (D)' cell."""
    rows = []
    for tr in soup.find_all("tr"):
        cells = [norm(c.get_text(" ", strip=True)) for c in tr.find_all(["th", "td"])]
        period = next((c for c in cells if PERIOD_RE.search(c)), None)
        details = next((c for c in cells if PAYOUT_RE.search(c) or re.search(r"\([DBR]\)", c)), None)
        if period and details and period != details:
            rows.append((cells[0], PERIOD_RE.search(period).group(0), details))
    return rows


def fiscal_year_of(period, fye_month):
    """Period end date -> fiscal year label (matches PSX annual columns)."""
    m = DATE_RE.search(period)
    if not m:
        return None
    month, year = int(m.group(2)), int(m.group(3))
    if fye_month and month > fye_month:
        year += 1          # e.g. June FY: 31/12/2025 belongs to FY2026
    return str(year)


def parse_payouts(soup, text, result, symbol):
    """
    DPS per fiscal year = sum of all cash (D) payouts for that year, where
    percent is of face value (Rs 10 unless the page states otherwise).
    (B) bonus and (R) right issues are ignored.
    """
    m = re.search(r"Face\s*Value\s*:?\s*(?:Rs\.?\s*)?([\d.]+)", text, re.I)
    face_value = clean_number(m.group(1)) if m and clean_number(m.group(1)) else 10.0
    fye_month = MONTHS.get((result.get("fiscal_year_end") or "").lower())

    source = "static"
    raw_rows = extract_payout_rows(soup)
    if not raw_rows:
        ajax = fetch_payouts_soup(symbol)
        raw_rows = extract_payout_rows(ajax) if ajax else []
        source = "ajax" if raw_rows else None

    payouts = []
    for announced, period, details in raw_rows:
        cash_pct = 0.0
        for pm in PAYOUT_RE.finditer(details):
            tags = re.findall(r"\(\s*([A-Za-z]+)\s*\)", pm.group(2))
            if any(t.upper() == "D" for t in tags):
                cash_pct += clean_number(pm.group(1)) or 0.0
        payouts.append({
            "announced": announced,
            "period": period,
            "details": details,
            "fiscal_year": fiscal_year_of(period, fye_month),
            "cash_dividend_pct": cash_pct,
            "DPS": round(cash_pct / 100.0 * face_value, 4),
        })

    result["payouts"] = payouts
    result["payouts_source"] = source

    dps_by_year = {}
    for p in payouts:
        if p["fiscal_year"]:
            dps_by_year[p["fiscal_year"]] = dps_by_year.get(p["fiscal_year"], 0.0) + p["DPS"]

    price = None
    if result.get("market_cap_000s") and result.get("total_shares"):
        price = result["market_cap_000s"] * 1000.0 / result["total_shares"]
    result["current_price_implied"] = round(price, 2) if price else None

    for y in sorted(set(result["annual"]) | set(dps_by_year)):
        ann = result["annual"].setdefault(y, {})
        dps = round(dps_by_year.get(y, 0.0), 4)
        eps = ann.get("EPS")
        ann["DPS"] = dps
        # yield is against the CURRENT price (PSX page has no historical prices)
        ann["Dividend_Yield"] = round(dps / price * 100.0, 2) if price else None
        if eps is not None and eps > 0:
            ann["Payout_Ratio"] = round(dps / eps * 100.0, 2)
        else:
            ann["Payout_Ratio"] = 0.0 if (dps == 0 and eps is not None) else None


# ------------------------------------------------------------------
# Balance sheet / cash flow / income statement
# ------------------------------------------------------------------
def fetch_statement_stockanalysis(symbol, page):
    """Returns {period: {metric: value}} (annual, millions PKR) or {}."""
    base = f"https://stockanalysis.com/quote/psx/{symbol}/financials/"
    url = base + (page + "/" if page else "")
    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    if resp.status_code != 200:
        return {}
    soup = BeautifulSoup(resp.text, "lxml")
    tbl = soup.find("table")
    if not tbl:
        return {}
    rows = tbl.find_all("tr")
    if len(rows) < 3:
        return {}
    header = [norm(c.get_text(" ", strip=True)) for c in rows[0].find_all(["th", "td"])]
    periods = header[1:]
    out = {p: {} for p in periods if p}
    for row in rows[1:]:
        cells = [norm(c.get_text(" ", strip=True)) for c in row.find_all(["th", "td"])]
        if len(cells) < 2:
            continue
        key = snake(cells[0])
        if not key:
            continue
        for i, p in enumerate(periods):
            if p and i + 1 < len(cells):
                raw = cells[i + 1]
                # keep period-end dates as text, everything else numeric
                out[p][key] = raw if key.lower().startswith("period") else clean_number(raw)
    return out


def fetch_statements_yahoo(symbol):
    """Fallback via yfinance. Raw PKR -> millions PKR."""
    if not yf:
        return {}
    out = {}
    try:
        t = yf.Ticker(f"{symbol}.KA")
        for key, df in (("balance_sheet", t.balance_sheet),
                        ("cash_flow", t.cashflow),
                        ("income_statement", t.financials)):
            if df is None or df.empty:
                continue
            d = {}
            for col in df.columns:
                year = str(col.year)
                d[year] = {}
                for label, val in df[col].items():
                    try:
                        d[year][snake(label)] = None if val != val else float(val) / 1e6
                    except (TypeError, ValueError):
                        d[year][snake(label)] = None
            out[key] = d
    except Exception:
        pass
    return out


def enrich_statements(symbol, result):
    for key, page in STATEMENT_PAGES.items():
        data = {}
        try:
            data = fetch_statement_stockanalysis(symbol, page)
        except Exception:
            pass
        result[key] = data
        time.sleep(0.4)

    # fallback for anything still empty
    if not all(result.get(k) for k in STATEMENT_PAGES):
        yb = fetch_statements_yahoo(symbol)
        for k in STATEMENT_PAGES:
            if not result.get(k) and yb.get(k):
                result[k] = yb[k]
                result.setdefault("statement_source", {})[k] = "yahoo"

    # 52-week fallback from Yahoo
    if yf and (result.get("52_Week_High") is None or result.get("52_Week_Low") is None):
        try:
            info = yf.Ticker(f"{symbol}.KA").info
            if result.get("52_Week_High") is None:
                result["52_Week_High"] = info.get("fiftyTwoWeekHigh")
            if result.get("52_Week_Low") is None:
                result["52_Week_Low"] = info.get("fiftyTwoWeekLow")
        except Exception:
            pass


# ------------------------------------------------------------------
# Scrape one stock
# ------------------------------------------------------------------
def scrape_one(stock, with_statements=True):
    symbol, name = stock["symbol"], stock["name"]
    url = f"https://dps.psx.com.pk/company/{symbol}"
    result = {
        "symbol": symbol, "name": name, "url": url,
        "in_kse100": stock.get("in_kse100", False),
        "market_cap_000s": None, "total_shares": None,
        "free_float": None, "free_float_pct": None,
        "52_Week_High": None, "52_Week_Low": None,
        "fiscal_year_end": None, "company_profile": None, "key_people": None,
        "annual": {}, "quarterly": {},
        "payouts": [], "payouts_source": None, "current_price_implied": None,
        "balance_sheet": {}, "cash_flow": {}, "income_statement": {},
        "error": None,
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
            parse_profile(soup, text, result)
            result["annual"], result["quarterly"] = parse_financials(soup)
            parse_payouts(soup, text, result, symbol)

            if with_statements:
                enrich_statements(symbol, result)

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
# Execution & save
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
        "source": "PSX Data Portal (dps.psx.com.pk) + stockanalysis.com / Yahoo Finance",
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
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--kse100-only", action="store_true")
    ap.add_argument("--equity-only", action="store_true")
    ap.add_argument("--no-statements", action="store_true",
                    help="skip balance sheet / cash flow / income statement")
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
            futures = {ex.submit(scrape_one, s, not args.no_statements): s for s in todo}
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

    print(f"\nDone -> {OUTPUT_JSON} ({len(results)} securities)")


if __name__ == "__main__":
    main()

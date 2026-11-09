#!/usr/bin/env python3
"""
PSX & StockAnalysis Fundamental Scraper -> JSON

Per company it collects:
  * PSX (dps.psx.com.pk): Market Cap, total shares, free float, 52-week range,
    fiscal year end, business description, key people, annual/quarterly
    Sales / PAT / EPS / ratios / payouts.
  * StockAnalysis (stockanalysis.com): Full dividend history & metrics,
    balance sheet, cash flow statement, income statement (annual, values in millions PKR).
  * Yahoo Finance (fallback): Dividend history/summary and financial statements
    when PSX and StockAnalysis do not provide them.

Usage:
  python psx_scraper.py --symbols HUBC SYS          # test specific stocks
  python psx_scraper.py --kse100-only                # scrape KSE-100 stocks
  python psx_scraper.py --resume                     # resume interrupted run
  python psx_scraper.py --no-statements               # skip financial statements (faster)
"""

import argparse
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bs4 import BeautifulSoup

try:
    import yfinance as yf
except ImportError:
    yf = None

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
INPUT_JSON_FILES = ["psx_all_stocks.json", "app/psx_all_stocks.json"]
OUTPUT_JSON = "PSX_All_Fundamentals.json"
MAX_WORKERS = 5
REQUEST_DELAY = 0.5
TIMEOUT = 15
RETRIES = 3
SAVE_EVERY = 10

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

DATE_FORMATS = (
    "%b %d, %Y", "%B %d, %Y", "%Y-%m-%d",
    "%d/%m/%Y", "%d-%m-%Y", "%d %b %Y", "%d %B %Y",
)

MONTHS = {
    m: i for i, m in enumerate([
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december"
    ], 1)
}

PAYOUT_ENDPOINTS = [
    ("POST", "https://dps.psx.com.pk/company/payouts", "symbol"),
    ("GET",  "https://dps.psx.com.pk/company/payouts", "symbol"),
    ("GET",  "https://dps.psx.com.pk/company/payouts/{symbol}", None),
]

PAYOUT_RE = re.compile(r"([\d,]+(?:\.\d+)?)\s*%\s*((?:[,;/\-\s]*\(\s*[A-Za-z]+\s*\)\s*)+)")
PERIOD_RE = re.compile(r"\d{1,2}[/-]\d{1,2}[/-](?:19|20)\d{2}(?:\s*\([^)]*\))?")
DATE_RE = re.compile(r"(\d{1,2})[/-](\d{1,2})[/-]((?:19|20)\d{2})")

_thread_local = threading.local()


class NotFound(Exception):
    pass


# ------------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------------
def get_session():
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        _thread_local.session = s
    return _thread_local.session


def fetch_with_retries(method, url, **kwargs):
    sess = get_session()
    kwargs.setdefault("timeout", TIMEOUT)
    for attempt in range(1, RETRIES + 1):
        try:
            resp = sess.request(method, url, **kwargs)
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", 2 * attempt))
                time.sleep(min(retry_after, 8))
                continue
            return resp
        except requests.RequestException:
            if attempt == RETRIES:
                raise
            time.sleep(1.2 * attempt)
    return None


def norm(s):
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def clean_number(text):
    if text is None:
        return None
    t = norm(str(text))
    if t.lower() in ("", "-", "—", "–", "n/a", "na", "none", "upgrade"):
        return None
    neg = (t.startswith("(") and t.endswith(")")) or t.startswith("-")
    t = re.sub(r"[^\d.]", "", t)
    try:
        val = float(t)
        return -val if neg else val
    except ValueError:
        return None


def clean_currency_number(text):
    if not text:
        return None
    t = norm(str(text))
    if t.lower() in ("-", "—", "–", "n/a", "na", "none", "upgrade"):
        return None
    cleaned = re.sub(r"\b(?:PKR|Rs|USD)\b\.?|₨|\$|,", "", t, flags=re.I).strip()
    m = re.search(r"^\(?\s*(-?\d+(?:\.\d+)?)\s*\)?", cleaned)
    if not m:
        m = re.search(r"(-?\d+(?:\.\d+)?)", cleaned)
        if not m:
            return None
        return float(m.group(1))
    val = float(m.group(1))
    neg = cleaned.startswith("-") or (cleaned.startswith("(") and ")" in cleaned)
    return -abs(val) if neg else val


def parse_date_flexible(text):
    t = norm(text)
    if not t or t.lower() in ("-", "—", "–", "n/a", "na", "none"):
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(t, fmt)
        except ValueError:
            continue
    m = YEAR_RE.search(t)
    if m:
        return datetime(int(m.group(0)), 1, 1)
    return None


def snake(label):
    return re.sub(r"[^A-Za-z0-9]+", "_", norm(label)).strip("_")


# ------------------------------------------------------------------
# PSX Scraper (dps.psx.com.pk)
# ------------------------------------------------------------------
def get_psx_stat(soup, label_keyword):
    """Extracts value text for a key stat in PSX DOM using stats_item / stats_label."""
    for item in soup.find_all(class_="stats_item"):
        lbl = item.find(class_="stats_label")
        if lbl and label_keyword.lower() in lbl.get_text().lower():
            full_text = item.get_text(" ", strip=True)
            return full_text.replace(lbl.get_text(" ", strip=True), "").strip()
        elif not lbl and label_keyword.lower() in item.get_text().lower():
            lbl_text = item.get_text(" ", strip=True)
            return re.sub(rf"{re.escape(label_keyword)}[^\d]*", "", lbl_text, flags=re.I).strip()
    return None


def parse_psx_equity(soup, text, result):
    # 1. Market Cap
    mc_str = get_psx_stat(soup, "Market Cap")
    if not mc_str:
        m = re.search(r"Market\s*Cap[\s\S]*?([\d,]{4,}(?:\.\d+)?)", text, re.I)
        mc_str = m.group(1) if m else None
    result["market_cap_000s"] = clean_number(mc_str)

    # 2. Shares
    sh_str = get_psx_stat(soup, "Shares")
    if not sh_str:
        m = re.search(r"Shares\s*\n?\s*([\d,]{4,})", text, re.I)
        sh_str = m.group(1) if m else None
    result["total_shares"] = clean_number(sh_str)

    # 3. Free Float & Pct
    ff_items = []
    for item in soup.find_all(class_="stats_item"):
        lbl = item.find(class_="stats_label")
        if (lbl and "free float" in lbl.get_text().lower()) or ("free float" in item.get_text().lower()):
            t = item.get_text(" ", strip=True)
            ff_items.append(t)

    ff_val, ff_pct = None, None
    for t in ff_items:
        if "%" in t:
            m = re.search(r"([\d.]+)\s*%", t)
            if m:
                ff_pct = clean_number(m.group(1))
        else:
            m = re.search(r"([\d,]{4,})", t)
            if m:
                ff_val = clean_number(m.group(1))

    if ff_val is None or ff_pct is None:
        m = re.search(r"Free\s*Float\s*\n?\s*([\d,]+)[\s\S]*?Free\s*Float\s*\n?\s*([\d.]+)\s*%", text, re.I)
        if m:
            ff_val = ff_val or clean_number(m.group(1))
            ff_pct = ff_pct or clean_number(m.group(2))

    result["free_float"] = ff_val
    result["free_float_pct"] = ff_pct

    # 4. 52-Week Range
    range_str = get_psx_stat(soup, "52-WEEK") or get_psx_stat(soup, "52 WEEK")
    if range_str:
        m = re.search(r"([\d,]+(?:\.\d+)?)\s*[\-–—\s]+\s*([\d,]+(?:\.\d+)?)", range_str)
        if m:
            lo, hi = clean_number(m.group(1)), clean_number(m.group(2))
            if lo is not None and hi is not None:
                result["52_Week_Low"], result["52_Week_High"] = min(lo, hi), max(lo, hi)
    else:
        m = re.search(r"52[\s-]*WEEK\s*RANGE[\s\S]*?([\d,]+(?:\.\d+)?)\s*[\-–—\s]+\s*([\d,]+(?:\.\d+)?)", text, re.I)
        if m:
            lo, hi = clean_number(m.group(1)), clean_number(m.group(2))
            if lo is not None and hi is not None:
                result["52_Week_Low"], result["52_Week_High"] = min(lo, hi), max(lo, hi)

    # 5. Fiscal Year End
    fye = None
    for item in soup.find_all(class_=re.compile(r"profile__item|item__head|stats", re.I)):
        t = item.get_text(" ", strip=True)
        if "Fiscal Year End" in t:
            m = re.search(r"Fiscal Year End\s*([A-Za-z]+)", t, re.I)
            if m:
                fye = m.group(1)
                break
    if not fye:
        m = re.search(r"Fiscal\s*Year\s*End\s*\n?\s*([A-Za-z]+)", text, re.I)
        if m:
            fye = m.group(1)
    result["fiscal_year_end"] = fye.strip() if fye else None


def parse_psx_profile(soup, text, result):
    prof = None
    p_div = soup.find(class_=re.compile(r"companyProfile|profile__desc|description", re.I))
    if p_div:
        prof = norm(p_div.get_text(" ", strip=True))
    else:
        m = re.search(r"BUSINESS\s*DESCRIPTION\s*(.*?)\s*(?:KEY\s*PEOPLE|STATUS)", text, re.S | re.I)
        if m:
            prof = norm(m.group(1))
    result["company_profile"] = prof or None

    people = []
    kp_head = soup.find(string=re.compile(r"KEY\s*PEOPLE", re.I))
    if kp_head:
        tbl = kp_head.find_next("table")
        if tbl:
            for tr in tbl.find_all("tr"):
                cells = [norm(c.get_text(" ", strip=True)) for c in tr.find_all(["th", "td"])]
                if len(cells) >= 2 and cells[0].lower() not in ("name", ""):
                    people.append({"name": cells[0], "position": cells[1]})
    result["key_people"] = people or None


def map_psx_metric(label):
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
    if any(x in m for x in ("sales", "total income", "mark-up earned", "markup earned", "revenue", "premium")):
        return "Sales"
    return None


def parse_psx_financials(soup):
    annual, quarterly = {}, {}
    for tbl in soup.find_all("table"):
        rows = tbl.find_all("tr")
        if len(rows) < 2:
            continue
        header = [norm(c.get_text(" ", strip=True)) for c in rows[0].find_all(["th", "td"])]
        periods = header[1:]
        if not periods or not any(periods):
            continue

        kind = None
        if all(QUARTER_RE.search(p) and YEAR_RE.search(p) for p in periods if p):
            kind = "quarterly"
        elif all(YEAR_RE.search(p) and not QUARTER_RE.search(p) for p in periods if p):
            kind = "annual"

        if not kind:
            continue

        target = annual if kind == "annual" else quarterly
        out = {p: {} for p in periods if p}

        for row in rows[1:]:
            cells = [norm(c.get_text(" ", strip=True)) for c in row.find_all(["th", "td"])]
            if len(cells) < 2:
                continue
            key = map_psx_metric(cells[0])
            if not key:
                continue
            for i, p in enumerate(periods):
                if p and i < len(cells) - 1:
                    out[p][key] = clean_number(cells[i + 1])

        for p, metrics in out.items():
            if metrics:
                target.setdefault(p, {}).update(metrics)

    return annual, quarterly


def fetch_psx_payouts(symbol, soup, text, face_value=10.0, fye_month=None):
    hdrs = {
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"https://dps.psx.com.pk/company/{symbol}",
    }
    raw_rows = []
    for method, url_tmpl, field in PAYOUT_ENDPOINTS:
        url = url_tmpl.format(symbol=symbol)
        try:
            if method == "POST":
                r = fetch_with_retries("POST", url, data={field: symbol}, headers=hdrs)
            elif field:
                r = fetch_with_retries("GET", url, params={field: symbol}, headers=hdrs)
            else:
                r = fetch_with_retries("GET", url, headers=hdrs)

            if r and r.status_code == 200 and "<tr" in r.text.lower():
                psoup = BeautifulSoup(r.text, "lxml")
                for tr in psoup.find_all("tr"):
                    cells = [norm(c.get_text(" ", strip=True)) for c in tr.find_all(["th", "td"])]
                    if len(cells) >= 2:
                        period = next((c for c in cells if PERIOD_RE.search(c)), None)
                        details = next((c for c in cells if PAYOUT_RE.search(c) or re.search(r"\([DBR]\)", c, re.I)), None)
                        if period and details and period != details:
                            raw_rows.append((cells[0], PERIOD_RE.search(period).group(0), details))
                if raw_rows:
                    break
        except Exception:
            continue

    payouts = []
    for announced, period, details in raw_rows:
        cash_pct = 0.0
        for pm in PAYOUT_RE.finditer(details):
            tags = re.findall(r"\(\s*([A-Za-z]+)\s*\)", pm.group(2))
            if any(t.upper() == "D" for t in tags):
                cash_pct += clean_number(pm.group(1)) or 0.0

        # Estimate fiscal year from period date
        m = DATE_RE.search(period)
        fy = None
        if m:
            month, yr = int(m.group(2)), int(m.group(3))
            if fye_month and month > fye_month:
                yr += 1
            fy = str(yr)

        payouts.append({
            "announced": announced,
            "period": period,
            "details": details,
            "fiscal_year": fy,
            "cash_dividend_pct": cash_pct,
            "DPS": round(cash_pct / 100.0 * face_value, 4),
        })
    return payouts


# ------------------------------------------------------------------
# StockAnalysis Scraper (stockanalysis.com)
# ------------------------------------------------------------------
def parse_stockanalysis_dividends(symbol):
    """
    Fetches full dividend history table & summary metrics from stockanalysis.com/quote/psx/{symbol}/dividend/.
    """
    url = f"https://stockanalysis.com/quote/psx/{symbol.lower()}/dividend/"
    summary = {}
    payouts = []
    try:
        r = fetch_with_retries("GET", url)
        if r and r.status_code == 200:
            soup = BeautifulSoup(r.text, "lxml")

            # 1. Key Metrics block
            for tr in soup.find_all(["tr", "div"]):
                t = norm(tr.get_text(" ", strip=True))
                if "Annual Dividend" in t:
                    m = re.search(r"Annual Dividend\s*(?:PKR|Rs|₨|\$)?\s*([\d,]+(?:\.\d+)?)", t)
                    if m and "annual_dividend" not in summary:
                        summary["annual_dividend"] = clean_currency_number(m.group(1))
                if "Dividend Yield" in t:
                    m = re.search(r"Dividend Yield\s*([\d.]+)\s*%", t)
                    if m and "dividend_yield_pct" not in summary:
                        summary["dividend_yield_pct"] = clean_number(m.group(1))
                if "Ex-Dividend Date" in t:
                    m = re.search(r"Ex-Dividend Date\s*([A-Za-z]+\s+\d+,\s+\d{4})", t)
                    if m and "ex_dividend_date" not in summary:
                        summary["ex_dividend_date"] = m.group(1)
                if "Payout Ratio" in t:
                    m = re.search(r"Payout Ratio\s*([\d.]+)\s*%", t)
                    if m and "payout_ratio_pct" not in summary:
                        summary["payout_ratio_pct"] = clean_number(m.group(1))

            # 2. History table
            tbl = soup.find("table")
            if tbl:
                rows = tbl.find_all("tr")
                if len(rows) > 1:
                    # Normalize header strings by stripping spaces & hyphens (e.g. 'ex-div idend date' -> 'exdividenddate')
                    hdr = [re.sub(r"[\s\-]+", "", c.get_text().strip().lower()) for c in rows[0].find_all(["th", "td"])]
                    ex_idx = next((i for i, h in enumerate(hdr) if "exdiv" in h or "exdate" in h), None)
                    amt_idx = next((i for i, h in enumerate(hdr) if "cash" in h or "amount" in h or "dividend" in h), None)
                    rec_idx = next((i for i, h in enumerate(hdr) if "record" in h), None)
                    pay_idx = next((i for i, h in enumerate(hdr) if "pay" in h), None)

                    for tr in rows[1:]:
                        cells = [norm(c.get_text(" ", strip=True)) for c in tr.find_all(["th", "td"])]
                        if ex_idx is not None and ex_idx < len(cells) and amt_idx is not None and amt_idx < len(cells):
                            dps = clean_currency_number(cells[amt_idx])
                            ex_date = cells[ex_idx]
                            if dps is not None and ex_date:
                                rec_date = cells[rec_idx] if rec_idx is not None and rec_idx < len(cells) else None
                                pay_date = cells[pay_idx] if pay_idx is not None and pay_idx < len(cells) else None
                                m_yr = re.search(r"\b(20\d{2})\b", ex_date)
                                fy = m_yr.group(1) if m_yr else None
                                payouts.append({
                                    "announced": None,
                                    "ex_dividend_date": ex_date,
                                    "record_date": rec_date if rec_date not in ("-", "—", "N/A") else None,
                                    "pay_date": pay_date if pay_date not in ("-", "—", "N/A") else None,
                                    "period": ex_date,
                                    "details": f"PKR {dps} (StockAnalysis)",
                                    "fiscal_year": fy,
                                    "cash_dividend_pct": None,
                                    "DPS": dps,
                                })
    except Exception:
        pass
    return summary, payouts


def parse_stockanalysis_overview(symbol):
    """
    Fetches stock quote summary info from stockanalysis.com/quote/psx/{symbol}/.
    """
    url = f"https://stockanalysis.com/quote/psx/{symbol.lower()}/"
    summary = {}
    try:
        r = fetch_with_retries("GET", url)
        if r and r.status_code == 200:
            soup = BeautifulSoup(r.text, "lxml")
            for tr in soup.find_all("tr"):
                cells = [norm(c.get_text(" ", strip=True)) for c in tr.find_all(["th", "td"])]
                if len(cells) == 2:
                    k, v = cells[0].lower(), cells[1]
                    if "previous close" in k:
                        summary["previous_close"] = clean_currency_number(v)
                    elif "52-week range" in k:
                        m = re.search(r"([\d,]+(?:\.\d+)?)\s*[-–—]\s*([\d,]+(?:\.\d+)?)", v)
                        if m:
                            summary["52_Week_Low"] = clean_number(m.group(1))
                            summary["52_Week_High"] = clean_number(m.group(2))
                    elif "market cap" in k:
                        summary["market_cap_sa"] = clean_currency_number(v)
    except Exception:
        pass
    return summary


def fetch_statement_stockanalysis(symbol, page):
    """
    Scrapes StockAnalysis financial statement table -> {period: {metric: value}} (annual, values in millions PKR).
    """
    url = f"https://stockanalysis.com/quote/psx/{symbol.lower()}/financials/{page + '/' if page else ''}"
    try:
        r = fetch_with_retries("GET", url)
        if not r or r.status_code != 200:
            return {}
        soup = BeautifulSoup(r.text, "lxml")
        tbl = soup.find("table")
        if not tbl:
            return {}
        rows = tbl.find_all("tr")
        if len(rows) < 3:
            return {}

        header = [norm(c.get_text(" ", strip=True)) for c in rows[0].find_all(["th", "td"])]
        raw_periods = header[1:]
        periods = []
        for p in raw_periods:
            m = YEAR_RE.search(p)
            periods.append(m.group(0) if m else p)

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
                    raw_val = cells[i + 1]
                    out[p][key] = raw_val if key.lower().startswith("period") else clean_number(raw_val)
        return out
    except Exception:
        return {}


# ------------------------------------------------------------------
# Yahoo Finance Fallback
# ------------------------------------------------------------------
def fetch_dividends_yahoo(symbol):
    if not yf:
        return [], {}, None
    try:
        t = yf.Ticker(f"{symbol}.KA")
        divs = t.dividends
        payouts = []
        if divs is not None and not divs.empty:
            for dt, val in divs.items():
                if val is None or val != val:
                    continue
                date_str = dt.strftime("%Y-%m-%d")
                dps = round(float(val), 4)
                payouts.append({
                    "announced": None,
                    "ex_dividend_date": date_str,
                    "period": date_str,
                    "details": f"PKR {dps} (Yahoo)",
                    "fiscal_year": str(dt.year),
                    "cash_dividend_pct": None,
                    "DPS": dps,
                })
        summary = {}
        try:
            info = t.info or {}
            if info.get("dividendRate") is not None:
                summary["annual_dividend"] = round(float(info["dividendRate"]), 4)
            if info.get("dividendYield") is not None:
                y = float(info["dividendYield"])
                summary["dividend_yield_pct"] = round(y * 100 if y < 1 else y, 2)
            if info.get("payoutRatio") is not None:
                summary["payout_ratio_pct"] = round(float(info["payoutRatio"]) * 100, 2)
        except Exception:
            pass

        if payouts:
            summary["source"] = "yahoo"
            return payouts, summary, "yahoo"
    except Exception:
        pass
    return [], {}, None


# ------------------------------------------------------------------
# Core Single Stock Scraper
# ------------------------------------------------------------------
def scrape_one(stock, with_statements=True):
    symbol, name = stock["symbol"], stock["name"]
    url = f"https://dps.psx.com.pk/company/{symbol}"

    result = {
        "symbol": symbol,
        "name": name,
        "url": url,
        "in_kse100": stock.get("in_kse100", False),
        "market_cap_000s": None,
        "total_shares": None,
        "free_float": None,
        "free_float_pct": None,
        "52_Week_High": None,
        "52_Week_Low": None,
        "fiscal_year_end": None,
        "company_profile": None,
        "key_people": None,
        "annual": {},
        "quarterly": {},
        "payouts": [],
        "payouts_source": None,
        "dividend_summary": None,
        "current_price_implied": None,
        "balance_sheet": {},
        "cash_flow": {},
        "income_statement": {},
        "error": None,
    }

    try:
        # 1. Fetch PSX page
        resp = fetch_with_retries("GET", url)
        if not resp:
            raise NotFound("PSX returned empty response")
        if resp.status_code == 404:
            raise NotFound("HTTP 404")
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "lxml")
        text = soup.get_text("\n", strip=True)

        parse_psx_equity(soup, text, result)
        parse_psx_profile(soup, text, result)
        result["annual"], result["quarterly"] = parse_psx_financials(soup)

        # 2. Payouts & Dividends (PSX -> StockAnalysis -> Yahoo)
        fye_month = MONTHS.get((result.get("fiscal_year_end") or "").lower())
        psx_payouts = fetch_psx_payouts(symbol, soup, text, fye_month=fye_month)

        sa_summary, sa_payouts = parse_stockanalysis_dividends(symbol)
        sa_overview = parse_stockanalysis_overview(symbol)
        sa_summary.update({k: v for k, v in sa_overview.items() if k not in sa_summary and v is not None})

        # Fill 52-week range from StockAnalysis if PSX missed it
        if result["52_Week_Low"] is None and sa_overview.get("52_Week_Low") is not None:
            result["52_Week_Low"] = sa_overview["52_Week_Low"]
        if result["52_Week_High"] is None and sa_overview.get("52_Week_High") is not None:
            result["52_Week_High"] = sa_overview["52_Week_High"]

        if psx_payouts:
            result["payouts"] = psx_payouts
            result["payouts_source"] = "psx"
            result["dividend_summary"] = sa_summary or None
        elif sa_payouts:
            result["payouts"] = sa_payouts
            result["payouts_source"] = "stockanalysis"
            result["dividend_summary"] = sa_summary or None
        else:
            y_payouts, y_summary, y_src = fetch_dividends_yahoo(symbol)
            result["payouts"] = y_payouts
            result["payouts_source"] = y_src
            result["dividend_summary"] = y_summary or None

        # 3. Calculate Implied Price & DPS Aggregation per Annual FY
        price = None
        if result.get("market_cap_000s") and result.get("total_shares") and result["total_shares"] > 0:
            price = round((result["market_cap_000s"] * 1000.0) / result["total_shares"], 2)
        elif sa_summary.get("previous_close"):
            price = sa_summary["previous_close"]
        elif sa_summary.get("annual_dividend") and sa_summary.get("dividend_yield_pct") and sa_summary["dividend_yield_pct"] > 0:
            price = round(sa_summary["annual_dividend"] / (sa_summary["dividend_yield_pct"] / 100.0), 2)

        result["current_price_implied"] = price

        dps_by_year = {}
        for p in result["payouts"]:
            fy = p.get("fiscal_year")
            if fy:
                dps_by_year[fy] = dps_by_year.get(fy, 0.0) + (p.get("DPS") or 0.0)

        for y in sorted(set(result["annual"]) | set(dps_by_year)):
            ann = result["annual"].setdefault(y, {})
            eps = ann.get("EPS")
            dps = round(dps_by_year.get(y, 0.0), 4) if y in dps_by_year else None
            ann["DPS"] = dps
            ann["Dividend_Yield"] = round(dps / price * 100.0, 2) if (dps is not None and price and price > 0) else None
            if dps is not None and eps is not None and eps > 0:
                ann["Payout_Ratio"] = round(dps / eps * 100.0, 2)
            elif dps == 0 and eps is not None:
                ann["Payout_Ratio"] = 0.0
            else:
                ann["Payout_Ratio"] = None

        # 4. Financial Statements (StockAnalysis -> Yahoo fallback)
        if with_statements:
            for key, page in STATEMENT_PAGES.items():
                data = fetch_statement_stockanalysis(symbol, page)
                result[key] = data
                time.sleep(0.3)

    except NotFound as e:
        result["error"] = str(e)
    except Exception as e:
        result["error"] = str(e)[:150]

    time.sleep(REQUEST_DELAY)
    return result


# ------------------------------------------------------------------
# File Loading & Main
# ------------------------------------------------------------------
def find_input_file():
    for fpath in INPUT_JSON_FILES:
        if os.path.exists(fpath):
            return fpath
    return None


def load_stocks(args):
    fpath = find_input_file()
    if not fpath:
        sys.exit(f"Missing input JSON ({', '.join(INPUT_JSON_FILES)}).")

    with open(fpath, encoding="utf-8") as f:
        data = json.load(f)
        stocks = data.get("stocks", data) if isinstance(data, dict) else data

    if args.kse100_only:
        stocks = [s for s in stocks if s.get("in_kse100")]
    if args.equity_only:
        stocks = [s for s in stocks if s.get("in_kse100") or not NON_EQUITY_RE.search(s["name"])]
    if args.symbols:
        wanted = {x.upper() for x in args.symbols}
        stocks = [s for s in stocks if s["symbol"].upper() in wanted]
    return stocks


def save_results(results, order, total):
    results_sorted = sorted(results.values(), key=lambda x: order.get(x["symbol"], 99999))
    product = {
        "index": "PSX-ALL",
        "source": "PSX Data Portal (dps.psx.com.pk) + stockanalysis.com / Yahoo Finance",
        "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(results_sorted),
        "target_count": total,
        "stocks": results_sorted,
    }

    out_paths = [OUTPUT_JSON]
    if os.path.exists("app"):
        out_paths.append(os.path.join("app", OUTPUT_JSON))

    for opath in out_paths:
        tmp = opath + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(product, f, indent=2, ensure_ascii=False)
        os.replace(tmp, opath)


def main():
    ap = argparse.ArgumentParser(description="PSX & StockAnalysis Fundamental Scraper")
    ap.add_argument("--resume", action="store_true", help="Resume previous scraping session")
    ap.add_argument("--kse100-only", action="store_true", help="Scrape KSE-100 index constituent stocks only")
    ap.add_argument("--equity-only", action="store_true", help="Exclude non-equity securities (bonds/rights)")
    ap.add_argument("--no-statements", action="store_true", help="Skip balance sheet / cash flow / income statement")
    ap.add_argument("--symbols", nargs="+", help="Scrape specific symbol(s) e.g. --symbols HUBC SYS")
    args = ap.parse_args()

    stocks = load_stocks(args)
    order = {s["symbol"]: i for i, s in enumerate(stocks)}

    results = {}
    if args.resume and os.path.exists(OUTPUT_JSON):
        with open(OUTPUT_JSON, encoding="utf-8") as f:
            for r in json.load(f).get("stocks", []):
                if not r.get("error"):
                    results[r["symbol"]] = r
        print(f"Resuming: {len(results)} stocks already scraped.")

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
                    save_results(results, order, len(stocks))
    except KeyboardInterrupt:
        print("\nInterrupted - saving progress. Re-run with --resume to continue.")
    finally:
        save_results(results, order, len(stocks))

    print(f"\nCompleted! Saved data for {len(results)} securities to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()

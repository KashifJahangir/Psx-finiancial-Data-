#!/usr/bin/env python3
"""
PSX Fundamental Scraper -> JSON

Per company it collects:
  * PSX (dps.psx.com.pk): market cap, shares, free float, 52-week range,
    fiscal year end, business description, key people, annual/quarterly
    Sales / PAT / EPS / ratios / payouts
  * stockanalysis.com (fallback: Yahoo Finance): full balance sheet,
    cash flow statement, income statement (annual, values in millions PKR),
    and dividend history/summary when PSX does not provide payout data.

Usage:
  python psx_scraper.py --symbols HUBC          # test one stock first
  python psx_scraper.py --resume
  python psx_scraper.py --no-statements         # skip financial statements (fast)
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

DATE_FORMATS = (
    "%b %d, %Y", "%B %d, %Y", "%Y-%m-%d",
    "%d/%m/%Y", "%d-%m-%Y", "%d %b %Y", "%d %B %Y",
)

DIV_PAIR_RE = re.compile(r"([\d,]+(?:\.\d+)?)\s*\(\s*([\d,]+(?:\.\d+)?)\s*%\s*\)")

_thread_local = threading.local()


class NotFound(Exception):
    pass


# ------------------------------------------------------------------
# Helpers
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
                time.sleep(min(retry_after, 10))
                continue
            return resp
        except requests.RequestException:
            if attempt == RETRIES:
                raise
            time.sleep(1.5 * attempt)
    return None


def norm(s):
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def clean_number(text):
    text = norm(str(text) if text is not None else "")
    if text.lower() in ("", "-", "—", "–", "n/a", "na", "none", "upgrade"):
        return None
    neg = (text.startswith("(") and text.endswith(")")) or text.startswith("-")
    text = (text.replace(",", "").replace("(", "").replace(")", "")
            .replace("%", "").replace("-", "").strip())
    try:
        val = float(text)
    except ValueError:
        return None
    return -val if neg else val


def clean_currency_number(text):
    text = norm(str(text) if text is not None else "")
    if not text or text.lower() in ("-", "—", "–", "n/a", "na", "none", "upgrade"):
        return None
    cleaned = re.sub(r"\b(?:PKR|Rs|USD)\b\.?|₨|\$|,", "", text, flags=re.I).strip()
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
# Dividends: PSX + stockanalysis.com fallback + Yahoo Finance fallback
# ------------------------------------------------------------------
PAYOUT_ENDPOINTS = [
    ("POST", "https://dps.psx.com.pk/company/payouts", "symbol"),
    ("GET",  "https://dps.psx.com.pk/company/payouts", "symbol"),
    ("GET",  "https://dps.psx.com.pk/company/payouts/{symbol}", None),
    ("POST", "https://dps.psx.com.pk/payouts", "symbol"),
    ("GET",  "https://dps.psx.com.pk/payouts", "symbol"),
]

PAYOUT_RE = re.compile(
    r"([\d,]+(?:\.\d+)?)\s*%\s*((?:[,;/\-\s]*\(\s*[A-Za-z]+\s*\)\s*)+)"
)
PERIOD_RE = re.compile(r"\d{1,2}[/-]\d{1,2}[/-](?:19|20)\d{2}(?:\s*\([^)]*\))?")
DATE_RE = re.compile(r"(\d{1,2})[/-](\d{1,2})[/-]((?:19|20)\d{2})")
MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"], 1)}


def fetch_payouts_soup(symbol):
    """Calls PSX's payouts endpoint using the active session."""
    hdrs = {
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"https://dps.psx.com.pk/company/{symbol}",
    }
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
                return BeautifulSoup(r.text, "lxml")
        except Exception:
            continue
    return None


def extract_payout_rows(soup):
    """Content-based: a payout row has a date/period cell and a payout %/(D)(B)(R) cell."""
    if not soup:
        return []
    rows = []
    for tr in soup.find_all("tr"):
        cells = [norm(c.get_text(" ", strip=True)) for c in tr.find_all(["th", "td"])]
        if len(cells) < 2:
            continue
        period = next((c for c in cells if PERIOD_RE.search(c)), None)
        details = next(
            (c for c in cells if PAYOUT_RE.search(c) or re.search(r"\([DBR]\)", c, re.I)),
            None,
        )
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
        year += 1  # e.g. June FY: 31/12/2025 belongs to FY2026
    return str(year)


def _extract_sa_kv_pairs(soup):
    """Extracts label -> raw value text from StockAnalysis tables and summary blocks."""
    kv = {}
    for tr in soup.find_all("tr"):
        cells = [norm(c.get_text(" ", strip=True)) for c in tr.find_all(["th", "td"])]
        if len(cells) == 2 and cells[0]:
            kv.setdefault(cells[0].lower(), cells[1])

    summary_labels = (
        "Dividend Yield",
        "Annual Dividend",
        "Ex-Dividend Date",
        "Payout Frequency",
        "Payout Ratio",
        "Dividend Growth",
        "Dividend",
        "Previous Close",
        "52-Week Range",
    )
    label_set = {x.lower() for x in summary_labels}
    for label in summary_labels:
        key = label.lower()
        if key in kv:
            continue
        node = soup.find(string=re.compile(rf"^\s*{re.escape(label)}\s*$", re.I))
        if not node:
            continue
        for nxt in node.find_all_next(string=True, limit=4):
            t = norm(nxt)
            if not t:
                continue
            if t.lower() in label_set:
                break
            kv[key] = t
            break
    return kv


def parse_stockanalysis_overview(symbol):
    """Scrapes summary dividend & quote info from https://stockanalysis.com/quote/psx/{symbol}/."""
    url = f"https://stockanalysis.com/quote/psx/{symbol.upper()}/"
    try:
        resp = fetch_with_retries("GET", url)
    except Exception:
        return {}
    if not resp or resp.status_code != 200:
        return {}

    soup = BeautifulSoup(resp.text, "lxml")
    kv = _extract_sa_kv_pairs(soup)
    summary = {}

    div_raw = kv.get("dividend")
    if div_raw:
        m = DIV_PAIR_RE.search(div_raw)
        if m:
            summary["annual_dividend"] = clean_number(m.group(1))
            summary["dividend_yield_pct"] = clean_number(m.group(2))
        else:
            summary["annual_dividend"] = clean_currency_number(div_raw)

    if summary.get("dividend_yield_pct") is None and kv.get("dividend yield"):
        summary["dividend_yield_pct"] = clean_number(kv["dividend yield"])

    if kv.get("ex-dividend date") and kv["ex-dividend date"].lower() not in ("n/a", "-", "—"):
        summary["ex_dividend_date"] = kv["ex-dividend date"]

    if kv.get("payout ratio"):
        summary["payout_ratio_pct"] = clean_number(kv["payout ratio"])

    if kv.get("previous close"):
        summary["previous_close"] = clean_currency_number(kv["previous close"])

    if kv.get("52-week range"):
        m = re.search(r"([\d,]+(?:\.\d+)?)\s*[-–—]\s*([\d,]+(?:\.\d+)?)", kv["52-week range"])
        if m:
            lo, hi = clean_number(m.group(1)), clean_number(m.group(2))
            if lo is not None and hi is not None:
                summary["52_Week_Low"] = min(lo, hi)
                summary["52_Week_High"] = max(lo, hi)

    return summary


def parse_stockanalysis_dividend_page(symbol):
    """Scrapes summary + dividend history table from .../quote/psx/{symbol}/dividend/."""
    url = f"https://stockanalysis.com/quote/psx/{symbol.upper()}/dividend/"
    try:
        resp = fetch_with_retries("GET", url)
    except Exception:
        return {}, []
    if not resp or resp.status_code != 200:
        return {}, []

    soup = BeautifulSoup(resp.text, "lxml")
    kv = _extract_sa_kv_pairs(soup)
    summary = {}

    if kv.get("annual dividend"):
        summary["annual_dividend"] = clean_currency_number(kv["annual dividend"])
    if kv.get("dividend yield"):
        summary["dividend_yield_pct"] = clean_number(kv["dividend yield"])
    if kv.get("ex-dividend date") and kv["ex-dividend date"].lower() not in ("n/a", "-", "—"):
        summary["ex_dividend_date"] = kv["ex-dividend date"]
    if kv.get("payout frequency") and kv["payout frequency"].lower() not in ("n/a", "-", "—"):
        summary["payout_frequency"] = kv["payout frequency"]
    if kv.get("payout ratio"):
        summary["payout_ratio_pct"] = clean_number(kv["payout ratio"])
    if kv.get("dividend growth"):
        summary["dividend_growth_pct"] = clean_number(kv["dividend growth"])

    payouts = []
    for tbl in soup.find_all("table"):
        rows = tbl.find_all("tr")
        if len(rows) < 2:
            continue
        headers = [norm(c.get_text(" ", strip=True)).lower()
                   for c in rows[0].find_all(["th", "td"])]
        ex_idx = next((i for i, h in enumerate(headers)
                       if "ex-dividend" in h or "ex date" in h), None)
        amt_idx = next((i for i, h in enumerate(headers)
                        if "cash amount" in h or "amount" in h or h == "dividend"), None)
        if ex_idx is None or amt_idx is None:
            continue

        rec_idx = next((i for i, h in enumerate(headers) if "record" in h), None)
        pay_idx = next((i for i, h in enumerate(headers) if "pay" in h), None)
        type_idx = next((i for i, h in enumerate(headers) if "type" in h), None)

        for row in rows[1:]:
            cells = [norm(c.get_text(" ", strip=True)) for c in row.find_all(["th", "td"])]
            if max(ex_idx, amt_idx) >= len(cells):
                continue
            dps = clean_currency_number(cells[amt_idx])
            if dps is None:
                continue
            ex_date_str = cells[ex_idx]
            dt = parse_date_flexible(ex_date_str)
            rec_date = cells[rec_idx] if rec_idx is not None and rec_idx < len(cells) else None
            pay_date = cells[pay_idx] if pay_idx is not None and pay_idx < len(cells) else None
            div_type = cells[type_idx] if type_idx is not None and type_idx < len(cells) else "Cash"
            payouts.append({
                "announced": None,
                "ex_dividend_date": ex_date_str,
                "record_date": None if not rec_date or rec_date in ("-", "—", "n/a") else rec_date,
                "pay_date": None if not pay_date or pay_date in ("-", "—", "n/a") else pay_date,
                "period": ex_date_str,
                "details": f"PKR {dps} ({div_type or 'Cash'})",
                "fiscal_year": str(dt.year) if dt else None,
                "cash_dividend_pct": None,
                "DPS": round(dps, 4),
            })
        if payouts:
            break

    return summary, payouts


def fetch_dividend_stockanalysis(symbol):
    """
    Checks StockAnalysis /dividend/ first for full history, then /quote/psx/{symbol}/
    to fill missing summary metrics, price, or overview-only dividend info.
    """
    summary, payouts = parse_stockanalysis_dividend_page(symbol)
    need_overview = (
        not payouts
        or summary.get("annual_dividend") is None
        or summary.get("dividend_yield_pct") is None
        or summary.get("ex_dividend_date") is None
    )
    if need_overview:
        ov = parse_stockanalysis_overview(symbol)
        for k, v in ov.items():
            if summary.get(k) is None and v is not None:
                summary[k] = v

    summary = {k: v for k, v in summary.items() if v is not None}
    if payouts:
        summary["source"] = "stockanalysis"
        return payouts, summary, "stockanalysis"
    if any(k in summary for k in ("annual_dividend", "dividend_yield_pct", "ex_dividend_date")):
        summary["source"] = "stockanalysis_overview"
        return [], summary, "stockanalysis_overview"
    return [], summary, None


def fetch_dividends_yahoo(symbol):
    """Last-resort dividend fallback via yfinance."""
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
        if summary:
            summary["source"] = "yahoo_overview"
            return [], summary, "yahoo_overview"
    except Exception:
        pass
    return [], {}, None


def parse_payouts(soup, text, result, symbol):
    """
    Collects dividends from PSX first. If PSX returns no payout rows,
    falls back to stockanalysis.com (/dividend/ and overview page) and then Yahoo Finance.
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
    if raw_rows:
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
        sa_summary = {}
    else:
        payouts, sa_summary, source = fetch_dividend_stockanalysis(symbol)
        # Fill 52-week range from StockAnalysis overview if PSX missed it
        if result.get("52_Week_Low") is None and sa_summary.get("52_Week_Low") is not None:
            result["52_Week_Low"] = sa_summary.pop("52_Week_Low")
        else:
            sa_summary.pop("52_Week_Low", None)
        if result.get("52_Week_High") is None and sa_summary.get("52_Week_High") is not None:
            result["52_Week_High"] = sa_summary.pop("52_Week_High")
        else:
            sa_summary.pop("52_Week_High", None)

        if not payouts and source is None:
            payouts, y_summary, source = fetch_dividends_yahoo(symbol)
            sa_summary.update(y_summary)

    result["payouts"] = payouts
    result["payouts_source"] = source
    result["dividend_summary"] = sa_summary or None

    dps_by_year = {}
    for p in payouts:
        if p.get("fiscal_year"):
            fy = p["fiscal_year"]
            dps_by_year[fy] = dps_by_year.get(fy, 0.0) + (p.get("DPS") or 0.0)

    price = None
    if result.get("market_cap_000s") and result.get("total_shares"):
        price = result["market_cap_000s"] * 1000.0 / result["total_shares"]
    elif sa_summary.get("previous_close"):
        price = sa_summary["previous_close"]
    elif sa_summary.get("annual_dividend") and sa_summary.get("dividend_yield_pct"):
        if sa_summary["dividend_yield_pct"] > 0:
            price = sa_summary["annual_dividend"] / (sa_summary["dividend_yield_pct"] / 100.0)
    result["current_price_implied"] = round(price, 2) if price else None

    min_hist_year = min((int(y) for y in dps_by_year if y.isdigit()), default=None)

    for y in sorted(set(result["annual"]) | set(dps_by_year)):
        ann = result["annual"].setdefault(y, {})
        eps = ann.get("EPS")

        if source in ("static", "ajax"):
            dps = round(dps_by_year.get(y, 0.0), 4)
        elif source in ("stockanalysis", "yahoo") and dps_by_year:
            in_window = min_hist_year is not None and y.isdigit() and int(y) >= min_hist_year
            dps = round(dps_by_year.get(y, 0.0), 4) if (y in dps_by_year or in_window) else None
        else:
            dps = None

        ann["DPS"] = dps
        ann["Dividend_Yield"] = round(dps / price * 100.0, 2) if (dps is not None and price) else None
        if dps is not None and eps is not None and eps > 0:
            ann["Payout_Ratio"] = round(dps / eps * 100.0, 2)
        elif dps == 0 and eps is not None:
            ann["Payout_Ratio"] = 0.0
        else:
            ann["Payout_Ratio"] = None


# ------------------------------------------------------------------
# Balance sheet / cash flow / income statement
# ------------------------------------------------------------------
def fetch_statement_stockanalysis(symbol, page):
    """Returns {period: {metric: value}} (annual, millions PKR) or {}."""
    base = f"https://stockanalysis.com/quote/psx/{symbol.upper()}/financials/"
    url = base + (page + "/" if page else "")
    resp = fetch_with_retries("GET", url)
    if not resp or resp.status_code != 200:
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
        "payouts": [], "payouts_source": None, "dividend_summary": None,
        "current_price_implied": None,
        "balance_sheet": {}, "cash_flow": {}, "income_statement": {},
        "error": None,
    }
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            resp = fetch_with_retries("GET", url)
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

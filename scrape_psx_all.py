#!/usr/bin/env python3
"""
PSX Fundamental Scraper -> JSON   (v4: v3 data + units map)

What v4 adds
------------
* Every metric scraped from stockanalysis now has a unit recorded in a
  separate "units" map (the value structure is unchanged, so existing app
  code keeps working):
      PKR_million     money rows (Revenue, Net Income, Total Assets ...)
      percent         rows ending in "Growth", margins, yields, payout ...
      PKR_per_share   EPS, DPS, book value per share ...
      million_shares  share counts
      ratio / multiple / days / date
* "units_note" explains the PSX-page vs stockanalysis units.
* SCHEMA_VERSION = 4, so --resume re-scrapes anything made by v3.

Usage:
  python psx_scraper.py --symbols HUBC        # ALWAYS test one stock first
  python psx_scraper.py --resume
  python psx_scraper.py --no-statements       # PSX page only (fast)
"""

import argparse
import json
import os
import re
import sys
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
SCHEMA_VERSION = 4
MAX_WORKERS = 2            # stockanalysis rate-limits; keep this low
REQUEST_DELAY = 0.8
SA_DELAY = 1.2             # pause between stockanalysis requests
TIMEOUT = 30
RETRIES = 3
SAVE_EVERY = 25
MIN_STATEMENT_ROWS = 10    # fewer rows than this -> save raw HTML for debugging

SA_BASE = "https://stockanalysis.com/quote/psx"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

# output key -> stockanalysis path under /financials/
# (each one is fetched twice: annual and ?p=quarterly)
STATEMENT_PAGES = {
    "income_statement": "income-statement",
    "balance_sheet": "balance-sheet",
    "cash_flow": "cash-flow-statement",
    "ratios": "ratios",
    "kpis": "metrics",     # not every stock has it; a 404 is simply ignored
}
# the pages that must be non-empty, otherwise Yahoo is used as a fallback
CORE_STATEMENTS = ("income_statement", "balance_sheet", "cash_flow")

UNITS_NOTE = ("stockanalysis statements: PKR millions unless the units map says "
              "percent / PKR_per_share / million_shares / ratio / multiple / days / date. "
              "PSX-page 'annual'/'quarterly' Sales & PAT: PKR thousands.")

NUM_RE = re.compile(r"^[\d,]+(?:\.\d+)?$")
YEAR_RE = re.compile(r"\b(?:19|20)\d{2}\b")
QUARTER_RE = re.compile(r"\bQ[1-4]\b", re.I)
DATE_IN_TEXT_RE = re.compile(r"[A-Z][a-z]{2,8} \d{1,2}, \d{4}")
DASHES = ("", "-", "—", "–", "N/A", "n/a", "NA", "Upgrade")

MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun",
     "jul", "aug", "sep", "oct", "nov", "dec"], 1)}

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
    if text in DASHES:
        return None
    neg = (text.startswith("(") and text.endswith(")")) or text.startswith("-")
    text = (text.replace(",", "").replace("(", "").replace(")", "")
            .replace("%", "").replace("-", "").rstrip("xX").strip())
    try:
        val = float(text)
    except ValueError:
        return None
    return -val if neg else val


def snake(label):
    return re.sub(r"[^A-Za-z0-9]+", "_", norm(label)).strip("_")


def infer_unit(label, raw=""):
    """Unit of one stockanalysis row. The cell text wins (%, x); the label is the fallback."""
    raw, lab = norm(raw), norm(label).lower()
    if lab.startswith("period ending"):
        return "date"
    if raw.endswith("%"):
        return "percent"
    if re.search(r"\d\s*[xX]$", raw):
        return "multiple"
    if (lab.endswith("growth") or "margin" in lab or "yield" in lab
            or "payout" in lab or "return on" in lab or lab.endswith(" rate")):
        return "percent"
    if re.search(r"per share|\beps\b|\bdps\b|last close price", lab):
        return "PKR_per_share"
    if "shares" in lab:
        return "million_shares"
    if "days" in lab:
        return "days"
    if re.search(r"ratio|turnover|/", lab):
        return "ratio"
    return "PKR_million"


def norm_period(p):
    """'FY 2024' -> '2024', 'Q2 2025' -> 'Q2 2025', 'TTM' stays."""
    p = norm(p)
    if p.upper() == "TTM":
        return "TTM"
    m = re.search(r"Q([1-4])\D{0,3}((?:19|20)\d{2})", p, re.I)
    if m:
        return f"Q{m.group(1)} {m.group(2)}"
    m = YEAR_RE.search(p)
    if m:
        return m.group(0)
    return p


def month_from_text(txt):
    if not txt:
        return None
    return MONTHS.get(txt.strip()[:3].lower())


def fiscal_year_of(d, fye_month):
    """datetime -> fiscal year label (a June FYE: Dec-2025 belongs to FY2026)."""
    y = d.year
    if fye_month and d.month > fye_month:
        y += 1
    return str(y)


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
# PSX page parsing (unchanged logic)
# ------------------------------------------------------------------
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
    first_row_done = False
    for row in table.find_all("tr")[1:]:
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
        result["company_profile"] = norm(m.group(1)) or None

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
# stockanalysis.com helpers
# ------------------------------------------------------------------
SA_RETRIES = 6


def sa_get(session, url, log=None):
    """GET a stockanalysis page -> BeautifulSoup or None. 404 = page really
    does not exist (fine). Anything else that never succeeds is logged as
    BLOCKED so the stock is marked incomplete and re-scraped on --resume."""
    last = None
    for attempt in range(1, SA_RETRIES + 1):
        try:
            r = session.get(url, headers=HEADERS, timeout=TIMEOUT)
            last = r.status_code
            if log is not None:
                log.append(f"{r.status_code} {len(r.text)}B {url}")
            if r.status_code == 200:
                time.sleep(SA_DELAY)
                return BeautifulSoup(r.text, "lxml")
            if r.status_code == 404:
                return None
            # 429 / 403 / 5xx: wait longer each time, then retry
            time.sleep(20 * attempt)
        except requests.RequestException as ex:
            last = "EXC"
            if log is not None:
                log.append(f"EXC {str(ex)[:80]} {url}")
            time.sleep(5 * attempt)
    if log is not None:
        log.append(f"BLOCKED ({last}) {url}")
    return None


def _cells(tr):
    """Direct <th>/<td> children only (so nested markup can't shift columns)."""
    return tr.find_all(["th", "td"], recursive=False)


def _label(cell):
    """Row label = first non-empty text chunk of the first cell. Avoids the
    'Revenue Revenue Growth' style merged labels from hidden/nested spans."""
    for s in cell.stripped_strings:
        s = norm(s)
        if s:
            return s
    return ""


def _is_period_header(cells):
    vals = [norm(c.get_text(" ", strip=True)) for c in cells[1:]]
    vals = [v for v in vals if v]
    if not vals:
        return False
    return all(v.upper() == "TTM" or YEAR_RE.search(v) for v in vals)


def parse_one_sa_table(tbl):
    """One stockanalysis data table -> ({period: {metric: value}}, {metric: unit})."""
    trs = tbl.find_all("tr")
    hdr_idx = None
    for i, tr in enumerate(trs):
        cells = _cells(tr)
        if len(cells) >= 2 and _is_period_header(cells):
            hdr_idx = i
            break
    if hdr_idx is None:
        return {}, {}

    header = [norm(c.get_text(" ", strip=True)) for c in _cells(trs[hdr_idx])]
    periods = [norm_period(p) for p in header[1:]]
    out = {p: {} for p in periods if p}
    units = {}
    label_count = {}

    for tr in trs[hdr_idx + 1:]:
        cells = _cells(tr)
        if len(cells) < 2:
            continue
        label = _label(cells[0])
        base = snake(label)
        if not base:
            continue
        label_count[base] = label_count.get(base, 0) + 1
        key = base if label_count[base] == 1 else f"{base}_{label_count[base]}"

        vals = cells[1:]
        if len(vals) > len(periods):               # extra leading columns -> align right
            vals = vals[-len(periods):]
        sample = ""                                # first non-empty raw cell decides the unit
        for p, cell in zip(periods, vals):
            if not p:
                continue
            raw = norm(cell.get_text(" ", strip=True))
            if not sample and raw not in DASHES:
                sample = raw
            if key.lower().startswith("period_ending"):
                m = DATE_IN_TEXT_RE.search(raw)
                out[p][key] = m.group(0) if m else raw
                continue
            num = clean_number(raw)
            if num is not None:
                out[p][key] = num
            elif raw in DASHES:
                out[p][key] = None
            else:
                out[p][key] = raw                  # keep text values instead of losing them
        units[key] = infer_unit(label, sample)
    return out, units


def parse_sa_financial_table(soup):
    """ALL tables on a /financials/ page (main table + 'Additional Metrics' ...)
    merged into ({period: {metric: value}}, {metric: unit})."""
    if soup is None:
        return {}, {}
    merged, units = {}, {}
    for tbl in soup.find_all("table"):
        data, u = parse_one_sa_table(tbl)
        for period, metrics in data.items():
            tgt = merged.setdefault(period, {})
            for k, v in metrics.items():
                kk, n = k, 2
                while kk in tgt:                   # same label in a later table
                    kk = f"{k}_{n}"
                    n += 1
                tgt[kk] = v
                if k in u:
                    units.setdefault(kk, u[k])
    return merged, units


def parse_label_value_pairs(soup):
    """Every 2-cell table row on a page -> {label: raw value string}.
    Used for the Statistics and Company pages (many small tables)."""
    pairs = {}
    if soup is None:
        return pairs
    for tr in soup.find_all("tr"):
        cells = [norm(c.get_text(" ", strip=True)) for c in tr.find_all(["th", "td"])]
        if len(cells) == 2 and cells[0] and cells[1] and cells[1] != "Upgrade":
            pairs.setdefault(cells[0], cells[1])
    return pairs


def parse_sa_company(soup):
    out = parse_label_value_pairs(soup)
    if soup is not None:
        node = soup.find(string=re.compile(r"Company Description", re.I))
        if node:
            p = node.find_next("p")
            if p:
                out["Company Description"] = norm(p.get_text(" ", strip=True))
    return out


def count_rows(statement):
    return max((len(v) for v in statement.values()), default=0)


# ------------------------------------------------------------------
# Dividends (stockanalysis /dividend page)
# ------------------------------------------------------------------
DATE_FORMATS = ("%b %d, %Y", "%B %d, %Y", "%Y-%m-%d", "%d/%m/%Y",
                "%m/%d/%Y", "%d %b %Y", "%b %d %Y", "%Y/%m/%d")


def clean_text(t):
    """normalise unicode hyphens / spaces so matching is reliable"""
    t = norm(t)
    for ch in ("\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2212"):
        t = t.replace(ch, "-")
    return t


def parse_date(t):
    t = clean_text(t)
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(t, fmt)
        except ValueError:
            pass
    return None


def parse_amount(t):
    t = clean_text(t)
    if parse_date(t):
        return None
    m = re.match(r"^(?:PKR|Rs\.?|\$)?\s*([\d,]+(?:\.\d+)?)\s*(?:PKR|Rs\.?)?$", t, re.I)
    return clean_number(m.group(1)) if m else None


def extract_dividend_rows(soup):
    """Return [(ex_datetime, amount, pay_text)] using header positions when the
    header is recognisable, otherwise a content-based guess per row."""
    rows_out = []
    for tbl in soup.find_all("table"):
        trs = tbl.find_all("tr")
        if len(trs) < 2:
            continue
        head = [clean_text(c.get_text(" ", strip=True)).lower()
                for c in trs[0].find_all(["th", "td"])]
        joined = " ".join(head)
        if "date" not in joined or not ("amount" in joined or "dividend" in joined):
            continue
        ex_i = next((i for i, h in enumerate(head) if "ex" in h and "date" in h), 0)
        amt_i = next((i for i, h in enumerate(head) if "amount" in h), 1)
        pay_i = next((i for i, h in enumerate(head) if "pay" in h), None)
        for tr in trs[1:]:
            cells = [clean_text(c.get_text(" ", strip=True)) for c in tr.find_all(["th", "td"])]
            ex = parse_date(cells[ex_i]) if ex_i < len(cells) else None
            amt = parse_amount(cells[amt_i]) if amt_i < len(cells) else None
            if ex is None or amt is None:
                # content-based fallback for this row
                dates = [parse_date(c) for c in cells]
                ex = next((d for d in dates if d), None)
                amt = next((parse_amount(c) for c in cells if parse_amount(c) is not None), None)
            if ex is None or amt is None:
                continue
            pay = cells[pay_i] if (pay_i is not None and pay_i < len(cells)) else None
            rows_out.append((ex, amt, pay))
        if rows_out:
            return rows_out

    # last resort: any row in any table with a date cell and a "... PKR" cell
    for tr in soup.find_all("tr"):
        cells = [clean_text(c.get_text(" ", strip=True)) for c in tr.find_all(["th", "td"])]
        ex = next((parse_date(c) for c in cells if parse_date(c)), None)
        amt = next((parse_amount(c) for c in cells
                    if re.search(r"PKR|Rs", c, re.I) and parse_amount(c) is not None), None)
        if ex and amt is not None:
            rows_out.append((ex, amt, None))
    return rows_out


def clean_text_block(t):
    for ch in ("\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2212", "\xa0"):
        t = t.replace(ch, "-" if ch != "\xa0" else " ")
    return t


def parse_dividends(soup, fye_month):
    out = {"summary": {}, "history": [], "by_fiscal_year": {}, "by_calendar_year": {}}
    if soup is None:
        return out
    text = clean_text_block(soup.get_text("\n", strip=True))

    for label, key in [
        (r"Dividend Yield", "dividend_yield_pct"),
        (r"Annual Dividend", "annual_dividend"),
        (r"Ex-Dividend Date", "last_ex_dividend_date"),
        (r"Payout Frequency", "payout_frequency"),
        (r"Payout Ratio", "payout_ratio_pct"),
        (r"Dividend Growth[^\n]*", "dividend_growth_pct"),
    ]:
        m = re.search(label + r"\n([^\n]+)", text)
        if m:
            raw = m.group(1).strip()
            numeric = key in ("dividend_yield_pct", "annual_dividend",
                              "payout_ratio_pct", "dividend_growth_pct")
            out["summary"][key] = clean_number(raw.split()[0]) if numeric else raw

    seen = set()
    for ex, amt, pay in extract_dividend_rows(soup):
        k = (ex.strftime("%Y-%m-%d"), amt)
        if k in seen:
            continue
        seen.add(k)
        out["history"].append({
            "ex_date": ex.strftime("%Y-%m-%d"),
            "amount": amt,
            "pay_date": pay,
            "fiscal_year": fiscal_year_of(ex, fye_month),
        })

    for h in out["history"]:
        fy, cy = h["fiscal_year"], h["ex_date"][:4]
        out["by_fiscal_year"][fy] = round(out["by_fiscal_year"].get(fy, 0.0) + h["amount"], 4)
        out["by_calendar_year"][cy] = round(out["by_calendar_year"].get(cy, 0.0) + h["amount"], 4)
    return out


def apply_dividends_to_annual(result):
    """Put DPS / Dividend_Yield / Payout_Ratio into each year of result['annual']."""
    div = result.get("dividends") or {}
    dps_by_fy = div.get("by_fiscal_year", {})
    ratios_annual = (result.get("ratios") or {})

    price = None
    if result.get("market_cap_000s") and result.get("total_shares"):
        price = result["market_cap_000s"] * 1000.0 / result["total_shares"]
    result["current_price_implied"] = round(price, 2) if price else None

    years = set(result["annual"]) | set(dps_by_fy)
    for y in sorted(years):
        ann = result["annual"].setdefault(y, {})
        dps = dps_by_fy.get(y, 0.0)
        ann["DPS"] = round(dps, 4)

        # prefer stockanalysis' own historical yield / payout (uses year-end price)
        r = ratios_annual.get(y, {}) if isinstance(ratios_annual, dict) else {}
        sa_yield = r.get("Dividend_Yield")
        sa_payout = r.get("Payout_Ratio")

        if isinstance(sa_yield, (int, float)):
            ann["Dividend_Yield"] = sa_yield
        else:
            ann["Dividend_Yield"] = round(dps / price * 100.0, 2) if price else None
            ann["Dividend_Yield_basis"] = "current_price"

        eps = ann.get("EPS")
        if isinstance(sa_payout, (int, float)):
            ann["Payout_Ratio"] = sa_payout
        elif eps is not None and eps > 0:
            ann["Payout_Ratio"] = round(dps / eps * 100.0, 2)
        else:
            ann["Payout_Ratio"] = 0.0 if (dps == 0 and eps is not None) else None


# ------------------------------------------------------------------
# Yahoo fallback (annual statements only)
# ------------------------------------------------------------------
def fetch_statements_yahoo(symbol):
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


# ------------------------------------------------------------------
# Everything from stockanalysis for one symbol
# ------------------------------------------------------------------
def save_debug_html(symbol, name, soup, log, why):
    fn = f"debug_{symbol}_{name}.html"
    try:
        with open(fn, "w", encoding="utf-8") as f:
            f.write(str(soup))
        log.append(f"{why} - raw page saved to {fn}")
    except OSError:
        pass


def fetch_statement(session, symbol, key, url, log):
    """-> (data, units)"""
    soup = sa_get(session, url, log)
    data, units = parse_sa_financial_table(soup)
    if (soup is not None and key in CORE_STATEMENTS
            and count_rows(data) < MIN_STATEMENT_ROWS and "?p=" not in url):
        save_debug_html(symbol, key, soup, log,
                        f"{key}: only {count_rows(data)} rows parsed")
    return data, units


def enrich_from_stockanalysis(symbol, result):
    s = requests.Session()
    log = result.setdefault("sa_debug", [])
    fye_month = month_from_text(result.get("fiscal_year_end"))

    # 1) dividends (page is cheap and has the whole history)
    div_soup = sa_get(s, f"{SA_BASE}/{symbol}/dividend/", log)
    result["dividends"] = parse_dividends(div_soup, fye_month)
    if (div_soup is not None and not result["dividends"]["history"]
            and "Cash Amount" in div_soup.get_text(" ")):
        save_debug_html(symbol, "dividend", div_soup, log, "no dividend rows parsed")

    # 2) statistics + company page
    result["statistics"] = parse_label_value_pairs(sa_get(s, f"{SA_BASE}/{symbol}/statistics/", log))
    result["sa_company"] = parse_sa_company(sa_get(s, f"{SA_BASE}/{symbol}/company/", log))

    # 3) statements + ratios + KPIs, annual and quarterly
    for key, page in STATEMENT_PAGES.items():
        base = f"{SA_BASE}/{symbol}/financials/" + (page + "/" if page else "")
        result[key], u1 = fetch_statement(s, symbol, key, base, log)
        result[key + "_quarterly"], u2 = fetch_statement(s, symbol, key, base + "?p=quarterly", log)
        result["units"][key] = {**u2, **u1}      # same units for annual + quarterly

    result["incomplete"] = any(l.startswith("BLOCKED") for l in log)
    healthy = (not result["incomplete"] and result["dividends"]["history"]
               and count_rows(result.get("income_statement", {})) >= MIN_STATEMENT_ROWS)
    if healthy:
        result.pop("sa_debug", None)      # keep the JSON small when all went fine

    # 4) Yahoo fallback for any empty ANNUAL statement
    if not all(result.get(k) for k in CORE_STATEMENTS):
        yb = fetch_statements_yahoo(symbol)
        for k in CORE_STATEMENTS:
            if not result.get(k) and yb.get(k):
                result[k] = yb[k]
                result.setdefault("statement_source", {})[k] = "yahoo"

    # 5) 52-week fallback
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
        "schema_version": SCHEMA_VERSION,
        "symbol": symbol, "name": name, "url": url,
        "in_kse100": stock.get("in_kse100", False),
        "market_cap_000s": None, "total_shares": None,
        "free_float": None, "free_float_pct": None,
        "52_Week_High": None, "52_Week_Low": None,
        "fiscal_year_end": None, "company_profile": None, "key_people": None,
        "annual": {}, "quarterly": {},
        "current_price_implied": None,
        "dividends": {}, "statistics": {}, "sa_company": {},
        "income_statement": {}, "balance_sheet": {}, "cash_flow": {}, "ratios": {}, "kpis": {},
        "income_statement_quarterly": {}, "balance_sheet_quarterly": {},
        "cash_flow_quarterly": {}, "ratios_quarterly": {}, "kpis_quarterly": {},
        "units": {},
        "units_note": UNITS_NOTE,
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

            if with_statements:
                enrich_from_stockanalysis(symbol, result)
            apply_dividends_to_annual(result)

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
        "schema_version": SCHEMA_VERSION,
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
                    help="skip everything from stockanalysis.com")
    ap.add_argument("--symbols", nargs="+")
    args = ap.parse_args()

    if not os.path.exists(INPUT_JSON):
        sys.exit(f"Missing {INPUT_JSON} - put it in the same folder as this script.")

    stocks = load_stocks(args)
    if not stocks:
        with open(INPUT_JSON, encoding="utf-8") as f:
            raw = json.load(f)
        allst = raw.get("stocks", []) if isinstance(raw, dict) else raw
        print(f"No stocks matched. {INPUT_JSON} has {len(allst)} entries.")
        print("Top-level keys:", list(raw.keys()) if isinstance(raw, dict) else "(a list)")
        print("First 5 entries:", allst[:5])
        sys.exit("Nothing to scrape - check the symbol spelling / file format above. "
                 "Your output file was NOT touched.")
    order = {s["symbol"]: i for i, s in enumerate(stocks)}

    results = {}
    if args.resume and os.path.exists(OUTPUT_JSON):
        with open(OUTPUT_JSON, encoding="utf-8") as f:
            for r in json.load(f).get("stocks", []):
                # keep only complete records made by THIS schema version;
                # anything older (v2/v3, no units map) is re-scraped
                if (r.get("schema_version") == SCHEMA_VERSION and not r.get("error")
                        and "dividends" in r and not r.get("incomplete")):
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
                n_div = len((res.get("dividends") or {}).get("history", []))
                n_is = count_rows(res.get("income_statement", {}))
                n_bs = count_rows(res.get("balance_sheet", {}))
                n_cf = count_rows(res.get("cash_flow", {}))
                status = "OK" if not res["error"] else f"ERR -> {res['error']}"
                if res.get("incomplete"):
                    status += "  [INCOMPLETE - rate limited, will retry on --resume]"
                print(f"[{i:3d}/{len(todo)}] {res['symbol']:10s} {status}  "
                      f"(dividends: {n_div}, rows IS/BS/CF: {n_is}/{n_bs}/{n_cf})")
                if res.get("sa_debug") and (n_div == 0 or n_is < MIN_STATEMENT_ROWS):
                    for line in res["sa_debug"][:8]:
                        print("      ", line)
                if i % SAVE_EVERY == 0:
                    save(results, order, len(stocks))
    except KeyboardInterrupt:
        print("\nInterrupted - saving progress. Re-run with --resume to continue.")
    finally:
        if results:
            save(results, order, len(stocks))

    print(f"\nDone -> {OUTPUT_JSON} ({len(results)} securities)")


if __name__ == "__main__":
    main()

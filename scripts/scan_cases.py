#!/usr/bin/env python3
"""Weekly scanner for NYC felony-assault DA press releases.

Reads cases-2026.json, scrapes each DA's press-release listing page for
releases posted since the last scan, asks Claude Haiku to classify and
extract fields, and appends qualifying cases to cases-2026.json.

Idempotent: keys on source_url. A URL already in the file is skipped.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin

import requests
from anthropic import Anthropic
from bs4 import BeautifulSoup

HERE = Path(__file__).resolve().parent.parent
CASES_PATH = HERE / "cases-2026.json"
YEAR = 2026
# Only look back this many days if the file has no last_scan_date.
DEFAULT_LOOKBACK_DAYS = 14
# Hard cap on releases fetched per DA per run to avoid runaway API usage.
MAX_RELEASES_PER_DA = 30

USER_AGENT = (
    "NYC-Assault-Tracker/1.0 (+https://vitalcity-nyc.github.io/nyc-assault-tracker/)"
)


@dataclass
class Release:
    url: str
    title: str
    posted: date | None
    borough_hint: str  # DA borough, used as a default if Haiku can't tell


# ---------- Per-DA listing scrapers ----------


def fetch(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
    r.raise_for_status()
    return r.text


def parse_date(s: str) -> date | None:
    s = s.strip()
    # Common shapes: "January 2, 2026", "Jan 2, 2026", "2026-01-02", "01/02/2026"
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y", "%B %d %Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def scrape_manhattan() -> list[Release]:
    url = "https://manhattanda.org/category/news/press-release/"
    out: list[Release] = []
    try:
        html = fetch(url)
    except Exception as e:
        print(f"[manhattan] fetch failed: {e}", file=sys.stderr)
        return out
    soup = BeautifulSoup(html, "lxml")
    for art in soup.select("article")[:MAX_RELEASES_PER_DA]:
        a = art.select_one("h2 a, h3 a, a.entry-title-link")
        if not a:
            continue
        href = a.get("href")
        title = a.get_text(strip=True)
        time_el = art.select_one("time")
        posted = parse_date(time_el.get("datetime", "").split("T")[0]) if time_el else None
        if not posted and time_el:
            posted = parse_date(time_el.get_text())
        if href and title:
            out.append(Release(url=href, title=title, posted=posted, borough_hint="Manhattan"))
    return out


def scrape_brooklyn() -> list[Release]:
    url = "https://www.brooklynda.org/press-releases/"
    out: list[Release] = []
    try:
        html = fetch(url)
    except Exception as e:
        print(f"[brooklyn] fetch failed: {e}", file=sys.stderr)
        return out
    soup = BeautifulSoup(html, "lxml")
    for art in soup.select("article, .post, .news-item")[:MAX_RELEASES_PER_DA]:
        a = art.find("a", href=True)
        if not a:
            continue
        href = urljoin(url, a["href"])
        title = a.get_text(strip=True) or art.get_text(" ", strip=True)[:160]
        date_text = ""
        date_el = art.select_one("time, .date, .post-date")
        if date_el:
            date_text = date_el.get("datetime", "") or date_el.get_text()
        posted = parse_date(date_text.split("T")[0] if date_text else "")
        # Brooklyn URLs often embed the date: /2026/01/23/...
        if posted is None:
            m = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", href)
            if m:
                y, mo, d = map(int, m.groups())
                try:
                    posted = date(y, mo, d)
                except ValueError:
                    posted = None
        if href and title and "/press-releases/" not in href.rstrip("/").split("/")[-1]:
            out.append(Release(url=href, title=title, posted=posted, borough_hint="Brooklyn"))
    return out


def scrape_queens() -> list[Release]:
    url = "https://queensda.org/press_releases/"
    out: list[Release] = []
    try:
        html = fetch(url)
    except Exception as e:
        print(f"[queens] fetch failed: {e}", file=sys.stderr)
        return out
    soup = BeautifulSoup(html, "lxml")
    for a in soup.select("h2 a, h3 a, a.press-release-title, .entry-title a")[:MAX_RELEASES_PER_DA]:
        href = a.get("href")
        title = a.get_text(strip=True)
        # Walk upward to find a date sibling
        container = a.find_parent(["article", "div", "li"]) or a
        date_text = ""
        d_el = container.find(string=re.compile(r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\b"))
        if d_el:
            m = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}", d_el)
            if m:
                date_text = m.group(0)
        posted = parse_date(date_text)
        if href and title:
            out.append(Release(url=href, title=title, posted=posted, borough_hint="Queens"))
    return out


def scrape_bronx() -> list[Release]:
    url = "https://www.bronxda.nyc.gov/html/newsroom/press-releases.shtml"
    out: list[Release] = []
    try:
        html = fetch(url)
    except Exception as e:
        print(f"[bronx] fetch failed: {e}", file=sys.stderr)
        return out
    soup = BeautifulSoup(html, "lxml")
    # Bronx DA is static HTML. Links point to PDF press releases dated like pr-2026-01-02-name.pdf
    for a in soup.select("a[href$='.pdf'], a[href*='press-release']")[:MAX_RELEASES_PER_DA]:
        href = urljoin(url, a.get("href"))
        title = a.get_text(strip=True)
        # Try to pull a date from URL pattern
        posted = None
        m = re.search(r"(20\d{2})[-_/](\d{1,2})[-_/](\d{1,2})", href)
        if m:
            y, mo, d = map(int, m.groups())
            try:
                posted = date(y, mo, d)
            except ValueError:
                pass
        if href and title:
            out.append(Release(url=href, title=title, posted=posted, borough_hint="Bronx"))
    return out


def scrape_staten_island() -> list[Release]:
    url = "https://statenislandda.org/press_releases/"
    out: list[Release] = []
    try:
        html = fetch(url)
    except Exception as e:
        print(f"[staten_island] fetch failed: {e}", file=sys.stderr)
        return out
    soup = BeautifulSoup(html, "lxml")
    for a in soup.select("h2 a, h3 a, .entry-title a")[:MAX_RELEASES_PER_DA]:
        href = a.get("href")
        title = a.get_text(strip=True)
        container = a.find_parent(["article", "div", "li"]) or a
        date_text = ""
        d_el = container.find(string=re.compile(r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\b"))
        if d_el:
            m = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}", d_el)
            if m:
                date_text = m.group(0)
        posted = parse_date(date_text)
        if href and title:
            out.append(Release(url=href, title=title, posted=posted, borough_hint="Staten Island"))
    return out


SCRAPERS = [
    scrape_manhattan,
    scrape_brooklyn,
    scrape_queens,
    scrape_bronx,
    scrape_staten_island,
]


# ---------- Release body fetch ----------


def fetch_release_body(url: str) -> str:
    if url.lower().endswith(".pdf"):
        # Skip PDFs: pulling text would require pypdf; low-yield, high-complexity. Log and skip.
        return ""
    try:
        html = fetch(url)
    except Exception as e:
        print(f"[body] fetch failed for {url}: {e}", file=sys.stderr)
        return ""
    soup = BeautifulSoup(html, "lxml")
    # Drop nav/footer/script
    for tag in soup(["script", "style", "nav", "footer", "header", "aside", "form"]):
        tag.decompose()
    main = soup.select_one("article, main, .entry-content, .post-content, #content")
    text = (main or soup).get_text("\n", strip=True)
    # Compact
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text[:8000]  # cap for token budget


# ---------- Haiku classifier / extractor ----------


CLASSIFIER_PROMPT = """You extract felony-assault case records from New York City District Attorney press releases and named newsroom reporting for a data-journalism tracker.

A "qualifying" case means the alleged offense in the press release meets a felony-assault definition under New York Penal Law Article 120 (first, second, or aggravated-weapons third degree). This includes:
- Stabbings, shootings, severe beatings, subway pushings
- Hate-crime assaults elevated to felony
- Attacks on police/transit workers with serious physical injury
- Sex-crime cases with felony-assault counts
- Murder/attempted-murder cases where felony assault is a charged count (indictments, convictions, sentencings all count)

EXCLUDE:
- Misdemeanor-assault-only cases
- Narcotics, fraud, theft, weapon-possession-only cases (no assault count)
- Cases where the conduct happened outside NYC
- Organized-crime/racketeering press releases where no violent assault is described

Borough is one of: Manhattan, Brooklyn, Queens, Bronx, Staten Island.

Input: title, URL, posted date (if known), borough hint (the DA's borough, use unless the conduct clearly occurred elsewhere in NYC), and release body text.

Output: a JSON object exactly matching this schema, and nothing else:

{
  "qualifies": true | false,
  "reason_if_not": "short explanation if qualifies=false",
  "name": "primary defendant name(s), comma-separated; use a descriptor like '17-year-old defendant (name withheld)' if unnamed",
  "date": "YYYY-MM-DD — the date of the news event described (arrest, indictment, conviction, sentencing)",
  "borough": "Manhattan|Brooklyn|Queens|Bronx|Staten Island",
  "summary": "one to three plain-English sentences, ending in a period, describing the alleged conduct — match the voice of short wire-service style"
}

If qualifies=false, the other fields may be empty strings.

Return ONLY the JSON object, no preamble, no markdown fences.
"""


def extract_json(text: str) -> dict | None:
    """Pull the first JSON object out of a model response."""
    # Strip common fences
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    # Find outermost braces
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return None


def classify_and_extract(client: Anthropic, release: Release, body: str) -> dict | None:
    if not body:
        return None
    posted_str = release.posted.isoformat() if release.posted else ""
    user = (
        f"TITLE: {release.title}\n"
        f"URL: {release.url}\n"
        f"POSTED DATE: {posted_str}\n"
        f"DA BOROUGH HINT: {release.borough_hint}\n\n"
        f"BODY:\n{body}"
    )
    try:
        resp = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=500,
            system=CLASSIFIER_PROMPT,
            messages=[{"role": "user", "content": user}],
        )
    except Exception as e:
        print(f"[haiku] API error for {release.url}: {e}", file=sys.stderr)
        return None
    text = "".join(b.text for b in resp.content if hasattr(b, "text"))
    parsed = extract_json(text)
    if parsed is None:
        print(f"[haiku] unparseable response for {release.url}:\n{text[:300]}", file=sys.stderr)
    return parsed


# ---------- Main orchestration ----------


def load_cases() -> dict:
    with CASES_PATH.open() as f:
        return json.load(f)


def save_cases(obj: dict) -> None:
    with CASES_PATH.open("w") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
        f.write("\n")


def main() -> int:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ANTHROPIC_API_KEY not set — aborting", file=sys.stderr)
        return 1

    client = Anthropic(api_key=api_key)
    data = load_cases()
    cases = data.get("cases", [])
    existing_urls = {c.get("source_url") for c in cases if c.get("source_url")}

    # Determine cutoff — default to 14 days back if no prior scan
    last_scan = data.get("last_scan_date")
    cutoff = date.today() - timedelta(days=DEFAULT_LOOKBACK_DAYS)
    if last_scan:
        try:
            cutoff = datetime.strptime(last_scan, "%Y-%m-%d").date() - timedelta(days=2)  # small safety overlap
        except ValueError:
            pass
    print(f"Scanning releases posted on or after {cutoff.isoformat()}")

    all_releases: list[Release] = []
    for scraper in SCRAPERS:
        rels = scraper()
        print(f"  {scraper.__name__}: {len(rels)} links")
        all_releases.extend(rels)

    # Filter to this year, past cutoff, and not already in our file
    candidates: list[Release] = []
    for r in all_releases:
        if r.url in existing_urls:
            continue
        if r.posted and r.posted.year != YEAR:
            continue
        if r.posted and r.posted < cutoff:
            continue
        candidates.append(r)
    print(f"Candidates after dedup/cutoff: {len(candidates)}")

    added = 0
    for rel in candidates:
        body = fetch_release_body(rel.url)
        if not body:
            continue
        extracted = classify_and_extract(client, rel, body)
        time.sleep(0.5)  # polite to the API
        if not extracted or not extracted.get("qualifies"):
            continue
        # Determine case date — fall back to posted date if Haiku didn't supply one
        case_date = extracted.get("date") or (rel.posted.isoformat() if rel.posted else None)
        if not case_date or not case_date.startswith(str(YEAR)):
            continue
        borough = extracted.get("borough") or rel.borough_hint
        if borough not in {"Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"}:
            borough = rel.borough_hint
        entry = {
            "n": 0,  # renumbered below
            "name": extracted.get("name", "").strip() or "Defendant (name withheld)",
            "date": case_date,
            "borough": borough,
            "summary": extracted.get("summary", "").strip(),
            "source_url": rel.url,
        }
        if not entry["summary"]:
            continue
        cases.append(entry)
        existing_urls.add(rel.url)
        added += 1
        print(f"  + {entry['date']} {entry['borough']} — {entry['name']}")

    # If nothing new, don't rewrite the file — avoids noisy weekly "0 cases" commits.
    # last_scan_date advances only when the file is actually updated; the cutoff logic
    # will still look back far enough because DEFAULT_LOOKBACK_DAYS gives a floor.
    if added == 0:
        print(f"Done. No new cases. Total still {len(cases)}.")
        return 0

    # Sort chronologically, then renumber
    cases.sort(key=lambda c: (c["date"], c["name"]))
    for i, c in enumerate(cases, start=1):
        c["n"] = i

    data["cases"] = cases
    data["last_scan_date"] = date.today().isoformat()
    data["last_updated"] = date.today().isoformat()

    save_cases(data)
    print(f"Done. {added} new case(s) appended. Total now {len(cases)}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

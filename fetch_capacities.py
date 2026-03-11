"""
Fetches stadium capacities from Wikipedia infoboxes and writes them
back into stadiums_20232024.csv (in-place, preserving all other fields).

Usage:
    python fetch_capacities.py
"""

import csv
import os
import re
import time

import requests

WIKI_API   = "https://en.wikipedia.org/w/api.php"
CSV_PATH   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stadiums_20232024.csv")
DELAY      = 0.5   # seconds between requests (be polite to Wikipedia)
USER_AGENT = "StadiumCapacityFetcher/1.0 (educational project; python-requests)"
SESSION    = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})


def wiki_get(params: dict, retries: int = 3) -> dict | None:
    """GET the Wikipedia API with automatic retry on timeout."""
    for attempt in range(retries):
        try:
            resp = SESSION.get(WIKI_API, params=params, timeout=30)
            if resp.status_code != 200:
                return None
            return resp.json()
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                wait = 5 * (attempt + 1)
                print(f"(timeout, retrying in {wait}s)", end=" ", flush=True)
                time.sleep(wait)
            else:
                print("(timeout, giving up)", end=" ", flush=True)
                return None
        except requests.exceptions.RequestException:
            return None
    return None


def get_capacity_from_page(title: str) -> int | None:
    """Fetch wikitext for a Wikipedia title and parse the capacity field."""
    data = wiki_get({
        "action":    "query",
        "prop":      "revisions",
        "rvprop":    "content",
        "titles":    title,
        "redirects": 1,
        "format":    "json",
    })
    if data is None:
        return None

    pages = data.get("query", {}).get("pages", {})
    for pid, page in pages.items():
        if pid == "-1":
            continue
        revs = page.get("revisions", [])
        if not revs:
            continue
        # Support both old (revisions[0]["*"]) and new slot format
        wikitext = revs[0].get("*", "") or revs[0].get("slots", {}).get("main", {}).get("*", "")

        # Matches forms like:
        #   | capacity = 60,704
        #   | capacity = {{formatnum:60704}}
        #   | capacity = 60.704   (European decimal separator)
        match = re.search(
            r'\|\s*capacity\s*=\s*(?:\{\{[Ff]ormatnum:)?([\d][\d,\.\s]*[\d])',
            wikitext,
            re.IGNORECASE,
        )
        if match:
            raw = match.group(1).replace(",", "").replace(".", "")
            try:
                val = int(raw)
                if 1_000 < val < 300_000:   # basic sanity check
                    return val
            except ValueError:
                pass
    return None


def search_capacity(name: str, location: str = "") -> int | None:
    """Search Wikipedia for a stadium and return its capacity, or None."""
    # Build a list of queries from most to least specific
    queries = [name]
    # Try appending the city extracted from the location string
    city = location.split()[-1] if location else ""
    if city and city not in name:
        queries.append(f"{name} {city}")
    queries.append(f"{name} stadium")

    for query in queries:
        data = wiki_get({
            "action":   "query",
            "list":     "search",
            "srsearch": query,
            "srlimit":  3,
            "format":   "json",
        })
        if data is None:
            continue

        hits = data.get("query", {}).get("search", [])
        for hit in hits:
            cap = get_capacity_from_page(hit["title"])
            if cap:
                return cap
            time.sleep(DELAY)

    return None


def main() -> None:
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print("CSV is empty.")
        return

    fieldnames = list(rows[0].keys())
    total = len(rows)

    for i, row in enumerate(rows, 1):
        if row.get("capacity", "").strip():
            print(f"[{i:>3}/{total}] {row['name']}: already set ({row['capacity']})")
            continue

        print(f"[{i:>3}/{total}] Searching: {row['name']} ...", end=" ", flush=True)
        cap = search_capacity(row["name"], row.get("location", ""))
        if cap:
            row["capacity"] = cap
            print(f"→ {cap:,}")
            # Save progress after every found capacity so a crash doesn't lose work
            with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
        else:
            print("→ not found")
        time.sleep(DELAY)

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nDone. Updated {CSV_PATH}")


if __name__ == "__main__":
    main()

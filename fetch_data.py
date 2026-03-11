"""
Fetches the current season data from football-data.org for the Top 5
European leagues and writes CSV files matching the existing schema.

Output files (written to the same directory as this script):
  leagues_new.csv, teams_new.csv, stadiums_new.csv,
  matches_new.csv, scores_new.csv, standings_new.csv
"""

import csv
import time
import requests

API_KEY  = "4350e0a10bda4aa89da74b064ebe1e01"
BASE_URL = "https://api.football-data.org/v4"
DELAY    = 6.5           # seconds between requests (free tier: 10 req/min)

while True:
    try:
        SEASON = int(input("Enter season start year (e.g. 2024 for 2024/25): ").strip())
        break
    except ValueError:
        print("Please enter a valid 4-digit year.")

SEASON_SUFFIX = f"{SEASON}{SEASON + 1}"  # e.g. 20242025

HEADERS = {"X-Auth-Token": API_KEY}

# local_id -> (football-data competition code, country, country_id,
#              cl_spots, uel_spots, relegation_spots)
LEAGUES = {
    1: ("PL",  "England", 1, 4, 6, 3),
    2: ("SA",  "Italy",   2, 4, 5, 3),
    3: ("PD",  "Spain",   3, 4, 5, 3),
    4: ("BL1", "Germany", 4, 4, 5, 3),
    5: ("FL1", "France",  5, 3, 4, 3),
}

def get(path, params=None):
    url = f"{BASE_URL}{path}"
    resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

# ── holders ────────────────────────────────────────────────────────────────
leagues_rows   = []
teams_rows     = []
stadiums_rows  = []
matches_rows   = []
scores_rows    = []
standings_rows = []

# local id counters for junction tables
standing_id = 1
score_id    = 1
stadium_id  = 1
coach_id    = 1

# dedup maps  (fd_team_id -> local team_id, etc.)
team_id_map   = {}   # fd team id  -> local team id
stadium_map   = {}   # venue name  -> local stadium id
coach_map     = {}   # coach name  -> local coach id

# ── leagues ────────────────────────────────────────────────────────────────
for local_lid, (code, country, country_id, cl, uel, rel) in LEAGUES.items():
    icon = f"https://crests.football-data.org/{code}.png"
    leagues_rows.append({
        "league_id":       local_lid,
        "name":            "",          # filled below from API
        "country":         country,
        "country_id":      country_id,
        "icon_url":        icon,
        "cl_spot":         cl,
        "uel_spot":        uel,
        "relegation_spot": rel,
    })

# ── per-league data ─────────────────────────────────────────────────────────
for local_lid, (code, country, country_id, cl, uel, rel) in LEAGUES.items():

    print(f"\n{'='*55}\nLeague {local_lid}: {code}")

    # ---------- teams ----------
    print(f"  Fetching teams …")
    t_data = get(f"/competitions/{code}/teams", {"season": SEASON})
    time.sleep(DELAY)

    # update competition name in leagues_rows
    comp_name = t_data.get("competition", {}).get("name", code)
    for row in leagues_rows:
        if row["league_id"] == local_lid:
            row["name"] = comp_name

    for team in t_data.get("teams", []):
        fd_id   = team["id"]
        if fd_id in team_id_map:
            continue                # already added (shouldn't happen per-league)
        local_tid = len(team_id_map) + 1
        team_id_map[fd_id] = local_tid

        # ---- venue ----
        venue_name = (team.get("venue") or "").strip() or f"Stadium of {team['name']}"
        if venue_name not in stadium_map:
            stadium_map[venue_name] = stadium_id
            stadiums_rows.append({
                "stadium_id": stadium_id,
                "name":       venue_name,
                "location":   f"{team.get('address', '')}".strip(),
                "capacity":   "",
            })
            stadium_id += 1
        local_sid = stadium_map[venue_name]

        # ---- coach ----
        coach_data = team.get("coach") or {}
        coach_name = (coach_data.get("name") or "").strip()
        if coach_name and coach_name not in coach_map:
            coach_map[coach_name] = coach_id
            coach_id += 1
        local_cid = coach_map.get(coach_name, "")

        teams_rows.append({
            "team_id":      local_tid,
            "name":         team.get("name", ""),
            "founded_year": team.get("founded", ""),
            "stadium_id":   local_sid,
            "league_id":    local_lid,
            "coach_id":     local_cid,
            "cresturl":     team.get("crest", ""),
        })

    # ---------- standings ----------
    print(f"  Fetching standings …")
    s_data = get(f"/competitions/{code}/standings", {"season": SEASON})
    time.sleep(DELAY)

    season_year = s_data.get("season", {}).get("startDate", f"{SEASON}-01-01")[:4]
    local_season_id = local_lid   # simple 1:1 mapping (one season per league)

    for standing_table in s_data.get("standings", []):
        if standing_table.get("type") != "TOTAL":
            continue
        for entry in standing_table.get("table", []):
            fd_tid = entry["team"]["id"]
            local_tid = team_id_map.get(fd_tid, fd_tid)
            form_raw = entry.get("form") or ""
            # convert "W,D,L,W,W" → "['W', 'D', 'L', 'W', 'W']"
            if form_raw:
                form_list = [f"'{c}'" for c in form_raw.split(",") if c]
                form_str  = "[" + ", ".join(form_list) + "]"
            else:
                form_str = ""

            standings_rows.append({
                "standing_id":     standing_id,
                "season_id":       local_season_id,
                "league_id":       local_lid,
                "position":        entry.get("position", ""),
                "team_id":         local_tid,
                "played_games":    entry.get("playedGames", ""),
                "won":             entry.get("won", ""),
                "draw":            entry.get("draw", ""),
                "lost":            entry.get("lost", ""),
                "points":          entry.get("points", ""),
                "goals_for":       entry.get("goalsFor", ""),
                "goals_against":   entry.get("goalsAgainst", ""),
                "goal_difference": entry.get("goalDifference", ""),
                "form":            form_str,
            })
            standing_id += 1

    # ---------- matches ----------
    print(f"  Fetching matches …")
    m_data = get(f"/competitions/{code}/matches", {"season": SEASON})
    time.sleep(DELAY)

    local_season_id = local_lid

    for match in m_data.get("matches", []):
        fd_mid = match["id"]
        home_fd = match["homeTeam"]["id"]
        away_fd = match["awayTeam"]["id"]
        home_local = team_id_map.get(home_fd, home_fd)
        away_local = team_id_map.get(away_fd, away_fd)

        outcome = match.get("score", {}).get("winner") or ""

        matches_rows.append({
            "match_id":      fd_mid,
            "season_id":     local_season_id,
            "league_id":     local_lid,
            "matchday":      match.get("matchday", ""),
            "home_team_id":  home_local,
            "away_team_id":  away_local,
            "winner":        outcome,
            "utc_date":      (match.get("utcDate") or "")[:10],
        })

        ht = match.get("score", {}).get("halfTime") or {}
        ft = match.get("score", {}).get("fullTime")  or {}
        scores_rows.append({
            "score_id":        score_id,
            "match_id":        fd_mid,
            "full_time_home":  ft.get("home", ""),
            "full_time_away":  ft.get("away", ""),
            "half_time_home":  ht.get("home", ""),
            "half_time_away":  ht.get("away", ""),
        })
        score_id += 1

print("\n\nAll data fetched. Writing CSVs …")

# ── write helpers ────────────────────────────────────────────────────────────
BASE = r"c:\Users\roptant\.projects\Python2026\Python2026"

def write_csv(filename, fieldnames, rows):
    path = f"{BASE}\\{filename}"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  Wrote {len(rows):>5} rows -> {filename}")

write_csv(f"leagues_{SEASON_SUFFIX}.csv",
    ["league_id","name","country","country_id","icon_url","cl_spot","uel_spot","relegation_spot"],
    leagues_rows)

write_csv(f"teams_{SEASON_SUFFIX}.csv",
    ["team_id","name","founded_year","stadium_id","league_id","coach_id","cresturl"],
    teams_rows)

write_csv(f"stadiums_{SEASON_SUFFIX}.csv",
    ["stadium_id","name","location","capacity"],
    stadiums_rows)

write_csv(f"matches_{SEASON_SUFFIX}.csv",
    ["match_id","season_id","league_id","matchday","home_team_id","away_team_id","winner","utc_date"],
    matches_rows)

write_csv(f"scores_{SEASON_SUFFIX}.csv",
    ["score_id","match_id","full_time_home","full_time_away","half_time_home","half_time_away"],
    scores_rows)

write_csv(f"standings_{SEASON_SUFFIX}.csv",
    ["standing_id","season_id","league_id","position","team_id","played_games","won","draw",
     "lost","points","goals_for","goals_against","goal_difference","form"],
    standings_rows)

print("\nDone.")

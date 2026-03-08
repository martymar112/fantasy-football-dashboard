"""
fetch_espn_data.py
==================
Connects to ESPN Fantasy Football API and pulls all historical
data into a local SQLite database.

HOW CHAMPION DETECTION WORKS:
  We use a manual override table (champion_overrides) as the
  primary source of truth for champions. This is more reliable
  than trying to detect the champion from ESPN's API, which
  doesn't have a direct champion field and whose playoff data
  is inconsistent for older seasons.

  The override table stores the owner name for each year's
  champion. When writing season summaries, we look up the
  override first. If no override exists, we fall back to
  auto-detection from the final week scores.

  To add or correct a champion, update the CHAMPION_OVERRIDES
  dict below - no database changes needed.
"""

import os
import sqlite3
import logging
from espn_api.football import League


# ============================================================
# SECTION 1 - CHAMPION OVERRIDES
# ============================================================
# This is your verified champion list - the ground truth.
# Format: year (int) -> owner display name (string)
#
# The owner name must match exactly what ESPN returns for that
# owner. If a champion isn't showing up correctly, check the
# owner name in the teams table:
#   SELECT DISTINCT owner FROM teams WHERE year=2014;

CHAMPION_OVERRIDES = {
    # Format: year -> ESPN username (must match owner field in teams table exactly)
    # To find usernames: SELECT DISTINCT owner FROM teams WHERE year=XXXX;
    #
    # 2013: unknown - add when confirmed e.g. 2013: "espn_username",
    2014: "HoosierGuy8229",     # Nicholas Walker - Reggie's Bushleague
    2015: "michwolverines163",  # Bobby Denofre - Hill's Gladiators
    2016: "kunkel33",           # William Kunkel - Theokoles; The Shadow of Death
    2017: "JCinxcess",          # Jesse Wright - Trump's Micropenis
    2018: "RussianTron14",      # Kyle Coggins - The People's Champ
    2019: "mikedime89",         # Michael Diamond - Make America Mahomes again
    2020: "benjamin 55",        # Ben Bultema - Show me your TD's
    2021: "martinga2va",        # Michael Martin - Dirty Mike and The Boys
    2022: "benjamin 55",        # Ben Bultema - Protect CMC
    2023: "RussianTron14",      # Kyle Coggins - Acknowledge Me!
    2024: "kyle10051989",       # Kyle Bradshaw - The Stimmy PK Playa
    2025: "kunkel33",           # William Kunkel - An Officer And A Jeantyman
}

# NOTE: 2013 champion unknown - add when confirmed:


# ============================================================
# SECTION 2 - LOGGING SETUP
# ============================================================
os.makedirs("/app/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/fetch.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ============================================================
# SECTION 3 - LOAD ENVIRONMENT VARIABLES
# ============================================================
# Reads settings from your .env file.
#
# Required:
#   LEAGUE_ID      : your ESPN league ID
#   SWID           : ESPN auth cookie (never changes)
#   ESPN_S2        : ESPN auth cookie (current seasons)
#   YEARS          : comma-separated e.g. 2013,2014,...,2024
#
# Optional:
#   ESPN_S2_LEGACY : ESPN auth cookie for pre-2019 seasons
#   DB_PATH        : path to database file

def get_env_int(key):
    """Read an env var and convert to int, with clear error messages."""
    val = os.getenv(key)
    if val is None:
        raise EnvironmentError(f"Missing required env variable: {key}")
    try:
        return int(val)
    except ValueError:
        raise EnvironmentError(f"{key} must be a number, got: {val!r}")


league_id      = get_env_int("LEAGUE_ID")
swid           = os.getenv("SWID")
espn_s2        = os.getenv("ESPN_S2")
espn_s2_legacy = os.getenv("ESPN_S2_LEGACY")
db_path        = os.getenv("DB_PATH", "/app/data/fantasy.db")

# Build years list from YEARS=2013,2014,...,2024
# Falls back to single YEAR=2024 if YEARS not set
years_env = os.getenv("YEARS")
if years_env:
    years = [int(y.strip()) for y in years_env.split(",")]
else:
    single_year = os.getenv("YEAR")
    if not single_year:
        raise EnvironmentError("Set YEARS=2013,...,2024 or YEAR=2024 in .env")
    years = [int(single_year)]

if not swid or not espn_s2:
    raise EnvironmentError("SWID and ESPN_S2 must be set in .env")

log.info(f"Config loaded: league={league_id} | years={years}")
log.info(f"Legacy token: {'YES' if espn_s2_legacy else 'NO'}")
log.info(f"Champion overrides loaded for years: {sorted(CHAMPION_OVERRIDES.keys())}")


# ============================================================
# SECTION 4 - DATABASE SETUP
# ============================================================
# Three tables:
#
#   teams    : one row per team per year
#   scores   : one row per team per week per year
#   seasons  : season summary per team per year
#              (total points, playoffs, champion)

def setup_database(cursor):
    """Create tables if they don't exist. Safe to run every time."""

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS teams (
            team_id   INTEGER,
            year      INTEGER,
            owner     TEXT,
            team_name TEXT,
            wins      INTEGER,
            losses    INTEGER,
            PRIMARY KEY (team_id, year)
        )
    """)

    # UNIQUE(year, week, team_id) prevents duplicate rows on re-sync.
    # INSERT OR REPLACE will update existing rows instead of duplicating.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scores (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            year            INTEGER,
            week            INTEGER,
            team_id         INTEGER,
            score           REAL,
            opponent_id     INTEGER,
            projected_score REAL,
            UNIQUE(year, week, team_id)
        )
    """)

    # made_playoffs and champion use 1=yes, 0=no
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS seasons (
            year          INTEGER,
            team_id       INTEGER,
            owner         TEXT,
            team_name     TEXT,
            wins          INTEGER,
            losses        INTEGER,
            total_points  REAL,
            made_playoffs INTEGER DEFAULT 0,
            champion      INTEGER DEFAULT 0,
            PRIMARY KEY (year, team_id)
        )
    """)


# ============================================================
# SECTION 5 - HELPER FUNCTIONS
# ============================================================

def get_owner_name(team):
    """
    Safely extract owner display name from ESPN team object.
    ESPN returns owners as a list of dicts like:
        [{"displayName": "martinga2va", "id": "..."}]
    """
    if hasattr(team, "owners") and team.owners:
        owner_info = team.owners[0]
        if isinstance(owner_info, dict):
            return owner_info.get("displayName", "Unknown")
        return str(owner_info)
    return "Unknown"


def safe_team_id(team_or_int):
    """
    ESPN inconsistently returns team objects, raw ints, or None.
    This handles all three cases:
        Team object -> team.team_id
        Raw int     -> the int itself
        None        -> None (bye week - caller handles it)
    """
    if team_or_int is None:
        return None
    if isinstance(team_or_int, int):
        return team_or_int
    return getattr(team_or_int, "team_id", None)


def try_connect(year):
    """
    Connect to ESPN for a given year, trying two tokens.

    WHY TWO TOKENS:
        ESPN uses different auth for older seasons (pre-2019).
        ESPN_S2 works for recent years. ESPN_S2_LEGACY is a
        cookie grabbed from your browser on an older season page.

    HOW TO GET ESPN_S2_LEGACY:
        1. Go to fantasy.espn.com/football/team?leagueId=27635&seasonId=2018
        2. F12 -> Application -> Cookies -> fantasy.espn.com
        3. Copy ESPN_S2 value
        4. Add to .env: ESPN_S2_LEGACY=that_value

    Returns League object if successful, None if both tokens fail.
    """
    # Attempt 1: current token
    try:
        league = League(league_id=league_id, year=year,
                        swid=swid, espn_s2=espn_s2)
        if league.teams:
            log.info(f"  Connected with current token ({len(league.teams)} teams)")
            return league
        log.warning(f"  Current token: connected but 0 teams for {year}")
    except Exception as e:
        log.warning(f"  Current token failed for {year}: {e}")

    # Attempt 2: legacy token
    if espn_s2_legacy:
        log.info(f"  Trying legacy token for {year}...")
        try:
            league = League(league_id=league_id, year=year,
                            swid=swid, espn_s2=espn_s2_legacy)
            if league.teams:
                log.info(f"  Connected with legacy token ({len(league.teams)} teams)")
                return league
            log.warning(f"  Legacy token: connected but 0 teams for {year}")
        except Exception as e:
            log.warning(f"  Legacy token failed for {year}: {e}")
    else:
        log.warning(f"  No legacy token set. Add ESPN_S2_LEGACY to .env for pre-2019 seasons.")

    return None


def get_champion_id(year, league, cursor):
    """
    Find the champion team_id for a given year.

    PRIORITY ORDER:
        1. Manual override (CHAMPION_OVERRIDES dict above)
           - Most reliable, you control this directly
           - Looks up the owner name, finds their team_id in DB
        2. Auto-detection from final week scores
           - Fallback only - finds winner of highest-scoring game
           - Less reliable for older seasons

    This two-step approach means:
        - Known champions are always correct
        - Unknown years still get a best-guess
        - Easy to fix: just add the year to CHAMPION_OVERRIDES
    """

    # Step 1: check manual override
    if year in CHAMPION_OVERRIDES:
        override_owner = CHAMPION_OVERRIDES[year]
        log.info(f"  Champion override set for {year}: {override_owner}")

        # Find this owner's team_id in the teams table
        # We look for a partial match in case display names differ slightly
        row = cursor.execute("""
            SELECT team_id, owner, team_name
            FROM teams
            WHERE year = ?
            AND (owner = ? OR owner LIKE ?)
            LIMIT 1
        """, (year, override_owner, f"%{override_owner}%")).fetchone()

        if row:
            log.info(f"  Champion matched: {row[1]} | {row[2]} | team_id={row[0]}")
            return row[0]
        else:
            log.warning(
                f"  Override owner '{override_owner}' not found in teams table for {year}.\n"
                f"  Run this to see actual owner names:\n"
                f"  SELECT owner, team_name FROM teams WHERE year={year};"
            )

    # Step 2: auto-detect from final week
    log.info(f"  No override for {year} - auto-detecting champion...")
    try:
        box_scores  = league.box_scores(league.current_week)
        best_score  = -1
        champion_id = None

        for matchup in box_scores:
            home_id = safe_team_id(matchup.home_team)
            away_id = safe_team_id(matchup.away_team)
            if away_id is None:
                continue
            game_high = max(matchup.home_score, matchup.away_score)
            if game_high > best_score:
                best_score  = game_high
                champion_id = (home_id if matchup.home_score > matchup.away_score
                               else away_id)

        log.info(f"  Auto-detected champion: team_id={champion_id} score={best_score}")
        return champion_id

    except Exception as e:
        log.warning(f"  Champion auto-detection failed for {year}: {e}")
        return None


# ============================================================
# SECTION 6 - PULL ONE YEAR
# ============================================================

def pull_year(cursor, year):
    """
    Fetch one full season from ESPN and write to database.

    Steps:
        1. Connect to ESPN (current token first, legacy fallback)
        2. Detect playoff teams
        3. Sync team records to teams table
        4. Sync weekly scores to scores table
        5. Determine champion (override first, auto-detect fallback)
        6. Write season summary to seasons table
    """
    log.info(f"\n{'=' * 55}")
    log.info(f"Fetching {year} season...")

    league = try_connect(year)
    if not league:
        log.error(f"  SKIPPED {year} - could not connect with any token")
        log.error(f"  For pre-2019 seasons: add ESPN_S2_LEGACY to .env")
        return

    log.info(f"  League : {league.settings.name}")
    log.info(f"  Teams  : {len(league.teams)}")
    log.info(f"  Weeks  : {league.current_week}")

    # --- Detect playoff teams ---
    # Try ESPN's playoff_pct field first (100 = clinched playoffs)
    # Fall back to top 4 by wins if that field isn't available
    try:
        playoff_teams = {
            t.team_id for t in league.teams
            if hasattr(t, "playoff_pct") and t.playoff_pct == 100
        }
        if not playoff_teams:
            sorted_teams  = sorted(league.teams,
                                   key=lambda t: (t.wins, t.points_for),
                                   reverse=True)
            playoff_teams = {t.team_id for t in sorted_teams[:4]}
    except Exception:
        sorted_teams  = sorted(league.teams, key=lambda t: t.wins, reverse=True)
        playoff_teams = {t.team_id for t in sorted_teams[:4]}

    log.info(f"  Playoff teams: {playoff_teams}")

    # --- Sync teams ---
    log.info(f"  Syncing teams...")
    for team in league.teams:
        owner = get_owner_name(team)
        cursor.execute("""
            INSERT OR REPLACE INTO teams
                (team_id, year, owner, team_name, wins, losses)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (team.team_id, year, owner, team.team_name, team.wins, team.losses))
        log.info(f"    {team.team_name} | {owner} | {team.wins}W-{team.losses}L")

    # --- Sync weekly scores ---
    # One row per team per week.
    # opponent_id allows us to pair matchups in SQL queries later.
    log.info(f"  Syncing weeks 1-{league.current_week}...")
    skipped_weeks = []

    for week in range(1, league.current_week + 1):
        try:
            box_scores = league.box_scores(week)
        except Exception as e:
            log.warning(f"    Week {week}: fetch failed - {e}")
            skipped_weeks.append(week)
            continue

        count = 0
        for matchup in box_scores:
            home_id = safe_team_id(matchup.home_team)
            away_id = safe_team_id(matchup.away_team)

            if home_id is None:
                continue

            # Store home team score
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO scores
                        (year, week, team_id, score, opponent_id, projected_score)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (year, week, home_id,
                      matchup.home_score, away_id, matchup.home_projected))
            except Exception as e:
                log.warning(f"    Week {week} home insert failed: {e}")

            # Bye week - no away team to store
            if away_id is None:
                log.info(f"    Week {week}: team {home_id} has bye")
                continue

            # Store away team score
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO scores
                        (year, week, team_id, score, opponent_id, projected_score)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (year, week, away_id,
                      matchup.away_score, home_id, matchup.away_projected))
                count += 1
            except Exception as e:
                log.warning(f"    Week {week} away insert failed: {e}")

        log.info(f"    Week {week}: {count} matchups synced")

    if skipped_weeks:
        log.warning(f"  Weeks skipped: {skipped_weeks}")

    # --- Determine champion ---
    # Uses override dict first, auto-detect as fallback
    champion_id = get_champion_id(year, league, cursor)

    # --- Season summary ---
    # One row per team with total points, playoff status, champion flag
    log.info(f"  Writing season summaries...")
    for team in league.teams:
        owner = get_owner_name(team)
        total_pts = cursor.execute(
            "SELECT ROUND(SUM(score), 2) FROM scores WHERE year=? AND team_id=?",
            (year, team.team_id)
        ).fetchone()[0] or 0.0

        cursor.execute("""
            INSERT OR REPLACE INTO seasons
                (year, team_id, owner, team_name, wins, losses,
                 total_points, made_playoffs, champion)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            year, team.team_id, owner, team.team_name,
            team.wins, team.losses, total_pts,
            1 if team.team_id in playoff_teams else 0,
            1 if team.team_id == champion_id else 0
        ))

    log.info(f"  Year {year} complete!")


# ============================================================
# SECTION 7 - PULL ALL YEARS
# ============================================================

def pull_all_years():
    """
    Main entry point. Loops all years and calls pull_year() each time.
    Commits after each year so partial progress is saved if a crash occurs.
    """
    log.info(f"Starting sync for {len(years)} seasons: {years}")

    conn   = sqlite3.connect(db_path)
    cursor = conn.cursor()
    setup_database(cursor)

    failed  = []
    success = []

    for year in years:
        try:
            pull_year(cursor, year)
            conn.commit()    # save this year to disk immediately
            success.append(year)
        except Exception as e:
            log.error(f"Unexpected error on {year}: {e}")
            conn.rollback()  # undo partial writes for this year
            failed.append(year)

    conn.close()

    log.info(f"\n{'=' * 55}")
    log.info(f"SYNC COMPLETE")
    log.info(f"  Succeeded : {success}")
    if failed:
        log.warning(f"  Failed    : {failed}")
        log.warning(f"  Check logs/fetch.log for details")
    log.info(f"{'=' * 55}")


if __name__ == "__main__":
    pull_all_years()

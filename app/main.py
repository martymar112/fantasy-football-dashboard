from flask import Flask, render_template, abort
import sqlite3

app = Flask(__name__)
DB_PATH = "/app/data/fantasy.db"

# ============================================================
# REAL NAMES LOOKUP
# ============================================================
# Maps ESPN usernames to real names for display everywhere.
# If a username isn't in this dict, the username is shown as fallback.
# To add a new mapping: "espn_username": "Real Name"

REAL_NAMES = {
    "RussianTron14":      "Kyle Coggins",
    "martinga2va":        "Michael Martin",
    "benjamin 55":        "Ben Bultema",
    "mikedime89":         "Michael Diamond",
    "kunkel33":           "William Kunkel",
    "kyle10051989":       "Kyle Bradshaw",
    "HoosierGuy8229":     "Nicholas Walker",
    "JCinxcess":          "Jesse Wright",
    "michwolverines163":  "Bobby Denofre",
    "DHow557":            "David Howard",
    "cport2621":          "Chase Porterfield",
    "Quealman":           "Taylor Mar'Queal",
    "nucleusofchaos":     "Al St Victor",
    "Vince_324":          "Vincent Stephens",
    "SUPADAVE62":         "Dave Tillman",
    "Jordan77816":        "Jordan Wood",
    "Doclepx":            "Marc Tyler",
    # Unknown — will show username as fallback
    # "Insanelytorn":     "???",
    # "NoR_CaL2006":      "???",
    # "espn16924623":     "???",
}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # Register real_name() as a custom SQL function so we can
    # use it directly in queries: real_name(owner) -> "Kyle Coggins"
    conn.create_function("real_name", 1,
                         lambda u: REAL_NAMES.get(u, u) if u else u)
    return conn


# ============================================================
# HOME - All-Time Standings
# ============================================================
@app.route("/")
def standings():
    conn = get_db()
    teams = conn.execute("""
        SELECT
            real_name(owner)          AS display_name,
            owner                     AS username,
            SUM(wins)                 AS total_wins,
            SUM(losses)               AS total_losses,
            COUNT(DISTINCT year)      AS seasons_played,
            ROUND(SUM(total_points),2) AS total_points,
            SUM(made_playoffs)        AS playoff_appearances,
            SUM(champion)             AS championships
        FROM seasons
        GROUP BY owner
        ORDER BY championships DESC, total_wins DESC, total_points DESC
    """).fetchall()
    years = conn.execute(
        "SELECT DISTINCT year FROM seasons ORDER BY year"
    ).fetchall()
    conn.close()
    return render_template("standings.html",
                           teams=teams,
                           years=[r["year"] for r in years])


# ============================================================
# SEASON VIEW
# ============================================================
@app.route("/season/<int:year>")
def season(year):
    conn = get_db()
    teams = conn.execute("""
        SELECT
            team_name,
            real_name(owner)       AS display_name,
            owner                  AS username,
            wins,
            losses,
            ROUND(total_points,2)  AS total_points,
            made_playoffs,
            champion
        FROM seasons
        WHERE year = ?
        ORDER BY wins DESC, total_points DESC
    """, (year,)).fetchall()
    weeks = conn.execute(
        "SELECT DISTINCT week FROM scores WHERE year=? ORDER BY week",
        (year,)
    ).fetchall()
    conn.close()
    if not teams:
        abort(404)
    return render_template("season.html",
                           teams=teams, year=year,
                           weeks=[r["week"] for r in weeks])


# ============================================================
# WEEKLY SCORES
# ============================================================
@app.route("/season/<int:year>/week/<int:week>")
def weekly_scores(year, week):
    conn = get_db()
    scores = conn.execute("""
        SELECT
            t1.team_name            AS home_team,
            real_name(t1.owner)     AS home_owner,
            s1.score                AS home_score,
            t2.team_name            AS away_team,
            real_name(t2.owner)     AS away_owner,
            s2.score                AS away_score
        FROM scores s1
        JOIN scores s2 ON s1.opponent_id = s2.team_id
                       AND s1.week = s2.week
                       AND s1.year = s2.year
        JOIN teams t1 ON s1.team_id = t1.team_id AND t1.year = s1.year
        JOIN teams t2 ON s2.team_id = t2.team_id AND t2.year = s2.year
        WHERE s1.year = ? AND s1.week = ?
          AND s1.team_id < s2.team_id
        ORDER BY s1.score DESC
    """, (year, week)).fetchall()
    conn.close()
    return render_template("week.html", scores=scores, week=week, year=year)


# ============================================================
# RECORDS
# ============================================================
@app.route("/records")
def records():
    conn = get_db()

    highest = conn.execute("""
        SELECT
            real_name(t.owner) AS display_name,
            t.team_name,
            s.score,
            s.week,
            s.year
        FROM scores s
        JOIN teams t ON s.team_id = t.team_id AND t.year = s.year
        WHERE s.score > 0
        ORDER BY s.score DESC
        LIMIT 10
    """).fetchall()

    lowest = conn.execute("""
        SELECT
            real_name(t.owner) AS display_name,
            t.team_name,
            s.score,
            s.week,
            s.year
        FROM scores s
        JOIN teams t ON s.team_id = t.team_id AND t.year = s.year
        WHERE s.score > 0
        ORDER BY s.score ASC
        LIMIT 10
    """).fetchall()

    best_season = conn.execute("""
        SELECT
            real_name(owner)      AS display_name,
            team_name,
            year,
            wins,
            losses,
            ROUND(total_points,2) AS total_points
        FROM seasons
        ORDER BY wins DESC, total_points DESC
        LIMIT 10
    """).fetchall()

    worst_season = conn.execute("""
        SELECT
            real_name(owner)      AS display_name,
            team_name,
            year,
            wins,
            losses,
            ROUND(total_points,2) AS total_points
        FROM seasons
        WHERE total_points > 0
        ORDER BY wins ASC, total_points ASC
        LIMIT 10
    """).fetchall()

    conn.close()
    return render_template("records.html",
                           highest=highest, lowest=lowest,
                           best_season=best_season,
                           worst_season=worst_season)


# ============================================================
# CHAMPIONSHIPS
# ============================================================
@app.route("/championships")
def championships():
    conn = get_db()

    champs = conn.execute("""
        SELECT
            real_name(owner)      AS display_name,
            team_name,
            year,
            wins,
            losses,
            ROUND(total_points,2) AS total_points
        FROM seasons
        WHERE champion = 1
        ORDER BY year DESC
    """).fetchall()

    playoff_counts = conn.execute("""
        SELECT
            real_name(owner)       AS display_name,
            owner                  AS username,
            SUM(made_playoffs)     AS appearances,
            SUM(champion)          AS titles,
            COUNT(DISTINCT year)   AS seasons
        FROM seasons
        GROUP BY owner
        ORDER BY titles DESC, appearances DESC
    """).fetchall()

    conn.close()
    return render_template("championships.html",
                           champs=champs,
                           playoff_counts=playoff_counts)


# ============================================================
# HEAD TO HEAD
# ============================================================
# Current league members (2025 roster)
# Update this list each season when membership changes
CURRENT_MEMBERS = {
    "DHow557", "HoosierGuy8229", "Quealman", "RussianTron14",
    "benjamin 55", "cport2621", "espn16924623", "kunkel33",
    "kyle10051989", "martinga2va", "mikedime89", "nucleusofchaos"
}


def build_h2h_matrix(conn, current_only=False):
    """
    Build a head-to-head W/L matrix for all owners or current members only.

    HOW IT WORKS:
    - Fetch every matchup from the scores table
    - For each matchup, record a win for the higher scorer
      and a loss for the lower scorer in a nested dictionary
    - The matrix is keyed by real name so it displays correctly

    current_only=True filters both the owner list AND the matchups
    to only include the 12 current league members.
    """
    # Get owner list — filtered or full
    if current_only:
        owners_raw = conn.execute("""
            SELECT DISTINCT owner FROM seasons
            WHERE owner IN ({})
            ORDER BY owner
        """.format(",".join("?" * len(CURRENT_MEMBERS))),
        list(CURRENT_MEMBERS)).fetchall()
    else:
        owners_raw = conn.execute(
            "SELECT DISTINCT owner FROM seasons ORDER BY owner"
        ).fetchall()

    usernames = [r["owner"] for r in owners_raw]
    owners    = [REAL_NAMES.get(u, u) for u in usernames]

    # Fetch all matchups — filter to current members if needed
    if current_only:
        placeholders = ",".join("?" * len(CURRENT_MEMBERS))
        matchups = conn.execute(f"""
            SELECT
                real_name(t1.owner) AS owner_a,
                real_name(t2.owner) AS owner_b,
                s1.score            AS score_a,
                s2.score            AS score_b
            FROM scores s1
            JOIN scores s2 ON s1.opponent_id = s2.team_id
                           AND s1.week = s2.week
                           AND s1.year = s2.year
            JOIN teams t1 ON s1.team_id = t1.team_id AND t1.year = s1.year
            JOIN teams t2 ON s2.team_id = t2.team_id AND t2.year = s2.year
            WHERE s1.team_id < s2.team_id
              AND t1.owner IN ({placeholders})
              AND t2.owner IN ({placeholders})
        """, list(CURRENT_MEMBERS) * 2).fetchall()
    else:
        matchups = conn.execute("""
            SELECT
                real_name(t1.owner) AS owner_a,
                real_name(t2.owner) AS owner_b,
                s1.score            AS score_a,
                s2.score            AS score_b
            FROM scores s1
            JOIN scores s2 ON s1.opponent_id = s2.team_id
                           AND s1.week = s2.week
                           AND s1.year = s2.year
            JOIN teams t1 ON s1.team_id = t1.team_id AND t1.year = s1.year
            JOIN teams t2 ON s2.team_id = t2.team_id AND t2.year = s2.year
            WHERE s1.team_id < s2.team_id
        """).fetchall()

    # Build nested W/L matrix
    matrix = {o: {o2: {"wins": 0, "losses": 0, "pf": 0.0, "pa": 0.0}
                  for o2 in owners} for o in owners}

    for m in matchups:
        a, b   = m["owner_a"], m["owner_b"]
        sa, sb = m["score_a"], m["score_b"]
        if a not in matrix or b not in matrix:
            continue
        matrix[a][b]["pf"] += sa
        matrix[a][b]["pa"] += sb
        matrix[b][a]["pf"] += sb
        matrix[b][a]["pa"] += sa
        if sa > sb:
            matrix[a][b]["wins"]   += 1
            matrix[b][a]["losses"] += 1
        else:
            matrix[b][a]["wins"]   += 1
            matrix[a][b]["losses"] += 1

    return owners, matrix


@app.route("/head-to-head")
def head_to_head():
    conn   = get_db()
    owners, matrix = build_h2h_matrix(conn, current_only=False)
    conn.close()
    return render_template("head_to_head.html",
                           owners=owners, matrix=matrix,
                           current_only=False)


@app.route("/head-to-head/current")
def head_to_head_current():
    conn   = get_db()
    owners, matrix = build_h2h_matrix(conn, current_only=True)
    conn.close()
    return render_template("head_to_head.html",
                           owners=owners, matrix=matrix,
                           current_only=True)


# ============================================================
# DRAFT HISTORY
# ============================================================
@app.route("/draft")
@app.route("/draft/<int:year>")
def draft(year=None):
    conn = get_db()

    years = [r["year"] for r in conn.execute(
        "SELECT DISTINCT year FROM draft_picks ORDER BY year"
    ).fetchall()]

    if not years:
        conn.close()
        return render_template("draft.html", years=[], selected_year=None,
                               rounds=[], selected_round=None,
                               picks=[], owner_summary=[])

    # Default to most recent year
    if year is None or year not in years:
        year = years[-1]

    # Get available rounds for this year
    rounds = [r["round"] for r in conn.execute(
        "SELECT DISTINCT round FROM draft_picks WHERE year=? ORDER BY round", (year,)
    ).fetchall()]

    # Selected round from query string, default to round 1
    from flask import request
    selected_round = int(request.args.get("round", rounds[0] if rounds else 1))

    # Picks for selected round
    picks_raw = conn.execute("""
        SELECT pick, player, position, real_name(owner) AS display_name
        FROM draft_picks
        WHERE year=? AND round=?
        ORDER BY pick
    """, (year, selected_round)).fetchall()
    picks = [dict(p) for p in picks_raw]

    # Full draft for owner summary cards
    all_picks_raw = conn.execute("""
        SELECT round, pick, player, position, owner, real_name(owner) AS display_name
        FROM draft_picks
        WHERE year=?
        ORDER BY owner, round, pick
    """, (year,)).fetchall()

    # Group by owner
    owner_dict = {}
    for p in all_picks_raw:
        name = p["display_name"]
        if name not in owner_dict:
            owner_dict[name] = {"display_name": name, "picks": []}
        owner_dict[name]["picks"].append(dict(p))

    owner_summary = sorted(owner_dict.values(), key=lambda x: x["display_name"])

    conn.close()
    return render_template("draft.html",
                           years=years,
                           selected_year=year,
                           rounds=rounds,
                           selected_round=selected_round,
                           picks=picks,
                           owner_summary=owner_summary)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

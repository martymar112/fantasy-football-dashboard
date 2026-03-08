"""
import_draft.py
Run once to import draft history from LOEG_Draft_Report.xlsx into fantasy.db
Usage: python import_draft.py
"""
import openpyxl, sqlite3, os, logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger()

XLSX_PATH = "/app/data/LOEG_Draft_Report.xlsx"
DB_PATH   = os.getenv("DB_PATH", "/app/data/fantasy.db")

NAME_TO_USERNAME = {
    "Kyle Coggins":      "RussianTron14",
    "Michael Martin":    "martinga2va",
    "Ben Bultema":       "benjamin 55",
    "Ben bultema":       "benjamin 55",
    "Michael Diamond":   "mikedime89",
    "William Kunkel":    "kunkel33",
    "Kyle Bradshaw":     "kyle10051989",
    "Nicholas Walker":   "HoosierGuy8229",
    "Jesse Wright":      "JCinxcess",
    "Bobby Denofre":     "michwolverines163",
    "David Howard":      "DHow557",
    "Chase Porterfield": "cport2621",
    "Mar'Queal Taylor":  "Quealman",
    "Alain St.Victor":   "nucleusofchaos",
    "Alain St. Victor":  "nucleusofchaos",
    "Vince Stephens":    "Vince_324",
    "Dave Tillman":      "SUPADAVE62",
    "Jordan Wood":       "Jordan77816",
    "Eric Tyler":        "Doclepx",
    "Tyson Meyer":       "Insanelytorn",
    "Ian MacMillan":     "NoR_CaL2006",
    "Dak Up Mahomies":   "espn16924623",
}

def run():
    if not os.path.exists(XLSX_PATH):
        log.error(f"File not found: {XLSX_PATH}")
        log.error("Copy LOEG_Draft_Report.xlsx to /mnt/nvme/fantasy-football-dashboard/data/")
        return

    log.info(f"Loading {XLSX_PATH}...")
    wb = openpyxl.load_workbook(XLSX_PATH, data_only=True)
    ws = wb['Draft Data']

    conn = sqlite3.connect(DB_PATH)
    conn.execute("DROP TABLE IF EXISTS draft_picks")
    conn.execute("""
        CREATE TABLE draft_picks (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            year     INTEGER,
            round    INTEGER,
            pick     INTEGER,
            player   TEXT,
            position TEXT,
            owner    TEXT,
            UNIQUE(year, round, pick)
        )
    """)

    rows = list(ws.iter_rows(values_only=True))
    imported = skipped = 0
    unknown = set()

    for row in rows[1:]:
        year, rnd, pick, player, position, manager = row
        if not all([year, rnd, pick, player, manager]):
            skipped += 1
            continue
        player   = str(player).replace('\xa0', ' ').strip()
        position = str(position).replace('\xa0', ' ').strip()
        owner    = NAME_TO_USERNAME.get(str(manager).strip())
        if not owner:
            unknown.add(manager)
            skipped += 1
            continue
        conn.execute("INSERT OR IGNORE INTO draft_picks VALUES (NULL,?,?,?,?,?,?)",
                     (year, rnd, pick, player, position, owner))
        imported += 1

    conn.commit()
    log.info(f"Imported: {imported} picks | Skipped: {skipped}")
    if unknown:
        log.warning(f"Unknown managers (not imported): {unknown}")

    for r in conn.execute("SELECT year, MAX(round), COUNT(*) FROM draft_picks GROUP BY year ORDER BY year").fetchall():
        log.info(f"  {r[0]}: {r[1]} rounds | {r[2]} picks")

    conn.close()
    log.info("Done!")

if __name__ == "__main__":
    run()

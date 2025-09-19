import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv
import os

# --------------------
# Konfiguration
# --------------------
load_dotenv()

ROW_LIMIT = int(os.getenv("ROW_LIMIT", "10000"))
DB_PATH = os.getenv("DB_PATH_ELT", "warehouse_elt.db")

ACS_PATH = os.getenv("DEMO_ACS_PATH", "data/raw/acs_community.csv")
MAP_PATH = os.getenv("DEMO_MAP_PATH", "data/raw/Boundaries_Community_Areas.csv")

Path(DB_PATH).unlink(missing_ok=True)  # frische DB für Tests

# --------------------
# Extract
# --------------------
def extract_311(limit=ROW_LIMIT):
    url = f"https://data.cityofchicago.org/resource/v6vf-nfxy.csv?$order=created_date+DESC&$limit={limit}"
    logger.info(f"Fetching Chicago 311 CSV: {url}")
    df = pd.read_csv(
        url,
        parse_dates=["created_date", "closed_date", "last_modified_date"]
    )

    # 🔧 Fix: datetime → string für SQLite
    for col in ["created_date", "closed_date", "last_modified_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"Fetched {len(df):,} rows (311)")
    return df


def _robust_read_csv_local(path: str) -> pd.DataFrame:
    import pandas as pd
    try:
        df = pd.read_csv(path)
        if df.shape[1] == 1:  # evtl. Semikolon
            df = pd.read_csv(path, sep=";", engine="python")
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding="latin-1")
        if df.shape[1] == 1:
            df = pd.read_csv(path, sep=";", engine="python", encoding="latin-1")
    return df

def _norm(s: str) -> str:
    # trim + lowercase + Mehrfach-Spaces zu einem Space
    return " ".join(str(s).strip().lower().split())

def extract_acs_and_mapping():
    logger.info(f"Reading ACS (local): {ACS_PATH}")
    acs = _robust_read_csv_local(ACS_PATH)

    logger.info(f"Reading Boundaries Mapping (local): {MAP_PATH}")
    mapping = _robust_read_csv_local(MAP_PATH)

    logger.info(f"Mapping rows: {len(mapping)} (sollte ~77 sein)")

    # ---- ACS (Demografie) wie gehabt ----
    acs_small = acs[[
        "Community Area Number",
        "COMMUNITY AREA NAME",
        "PER CAPITA INCOME ",
        "HARDSHIP INDEX"
    ]].rename(columns={
        "Community Area Number": "community_area_int",
        "COMMUNITY AREA NAME": "community_area_name",
        "PER CAPITA INCOME ": "per_capita_income",
        "HARDSHIP INDEX": "hardship_index"
    })

    # ---- Mapping robust finden (ID + Name), egal wie die Spalten heißen ----
    cols = list(mapping.columns)
    lookup = {_norm(c): c for c in cols}

    # Kandidaten für die ID-Spalte (Community Area Number in Boundaries-Datei)
    id_col = None
    for key in ("area_numbe", "area numbe", "area_num", "area num", "area_num_1", "area num 1",
                "community area number", "community area num", "community_area_number"):
        if key in lookup:
            id_col = lookup[key]; break

    # Kandidaten für die Name-Spalte
    name_col = None
    for key in ("community", "community area", "community_area_name", "community area name", "name"):
        if key in lookup:
            name_col = lookup[key]; break

    if not id_col or not name_col:
        raise ValueError(
            "Konnte Mapping-Spalten nicht erkennen.\n"
            f"Vorhandene Spalten: {cols}\n"
            "Erwartet etwas wie 'AREA_NUMBE' (ID) und 'COMMUNITY' (Name)."
        )

    df_map = (
        mapping[[id_col, name_col]].copy()
        .rename(columns={id_col: "community_area_int", name_col: "community_area_name"})
    )

    # normalisieren
    df_map["community_area_int"] = pd.to_numeric(df_map["community_area_int"], errors="coerce").astype("Int64")

    df_map["community_area_name"] = (df_map["community_area_name"].astype(str).str.strip().str.upper())
    acs_small["community_area_name"] = (acs_small["community_area_name"].astype(str).str.strip().str.upper())


    return acs_small, df_map

# --------------------
# Load Staging
# --------------------
def stage(df, name, conn):
    logger.info(f"Staging complete: {name} replaced")
    df.to_sql(name, conn, if_exists="replace", index=False)

# --------------------
# Views (ELT)
# --------------------
def ensure_schema_and_views(conn):
    cur = conn.cursor()

    # Drop alte Views
    cur.executescript("""
    DROP VIEW IF EXISTS fact_requests_v;
    DROP VIEW IF EXISTS dim_area_extended_v;
    """)

    # fact_requests_v: KPIs in SQL
    cur.executescript("""
    CREATE VIEW fact_requests_v AS
    SELECT
        sr_number,
        sr_type AS request_type_name,
        owner_department AS owner_dept_name,
        status,
        created_date,
        closed_date,
        ward AS ward_int,
        community_area AS community_area_int,
        CAST((JULIANDAY(closed_date) - JULIANDAY(created_date)) * 24 AS REAL) AS resolution_time_h,
        72 AS sla_target_h,
        CASE
            WHEN UPPER(status) = 'COMPLETED' AND (JULIANDAY(closed_date) - JULIANDAY(created_date)) * 24 <= 72
                THEN 1 ELSE 0
        END AS sla_met,
        CASE WHEN UPPER(status) = 'COMPLETED' THEN 0 ELSE 1 END AS open_flag
    FROM stg_requests;
    """)

    # dim_area_extended_v: Join ACS + Mapping
    cur.executescript("""
    CREATE VIEW dim_area_extended_v AS
    SELECT
        m.community_area_int,
        m.community_area_name,
        a.per_capita_income,
        a.hardship_index
    FROM stg_area_map m
    LEFT JOIN stg_demographics a
        ON m.community_area_int = a.community_area_int
       AND UPPER(m.community_area_name) = UPPER(a.community_area_name);
    """)

    conn.commit()
    logger.info("Views created: fact_requests_v, dim_area_extended_v")

# --------------------
# Health Check
# --------------------
def health_check(conn):
    n1 = conn.execute("SELECT COUNT(*) FROM stg_requests;").fetchone()[0]
    n2 = conn.execute("SELECT COUNT(*) FROM fact_requests_v;").fetchone()[0]
    n3 = conn.execute("SELECT COUNT(*) FROM dim_area_extended_v;").fetchone()[0]
    logger.info(f"HealthCheck: stg_requests={n1:,}, fact_requests_v={n2:,}, dim_area_extended_v={n3:,}")

# --------------------
# Orchestrierung
# --------------------
def run():
    t0 = datetime.utcnow()
    logger.add("pipeline_elt.log", rotation="1 MB", retention=5)
    logger.info("Pipeline start (ELT mode)")

    df = extract_311(ROW_LIMIT)
    acs_small, df_map = extract_acs_and_mapping()

    conn = sqlite3.connect(DB_PATH)
    try:
        stage(df, "stg_requests", conn)
        stage(acs_small, "stg_demographics", conn)
        stage(df_map, "stg_area_map", conn)

        ensure_schema_and_views(conn)

        health_check(conn)
        logger.info("Pipeline success (ELT)")
    except Exception as e:
        logger.exception(f"Pipeline failed (ELT): {e}")
        raise
    finally:
        conn.close()
        logger.info(f"Pipeline end (UTC) in {(datetime.utcnow()-t0).total_seconds():.1f}s")

if __name__ == "__main__":
    run()

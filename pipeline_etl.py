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
SLA_HOURS = int(os.getenv("SLA_HOURS", "72"))
DB_PATH = os.getenv("DB_PATH_ETL", "warehouse_etl.db")

ACS_PATH = os.getenv("DEMO_ACS_PATH", "data/raw/acs_community.csv")
MAP_PATH = os.getenv("DEMO_MAP_PATH", "data/raw/Boundaries_Community_Areas.csv")

Path(DB_PATH).unlink(missing_ok=True)  # nur beim ersten Mal, frische DB

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
    logger.info(f"Fetched {len(df):,} rows (311)")
    return df


def _robust_read_csv_local(path: str) -> pd.DataFrame:
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
# Transform (Python)
# --------------------
def transform_311(df, sla_hours=SLA_HOURS):
    logger.info("Transforming 311 data in Python (ETL mode)")
    df = df.copy()

    # sicherstellen: echte Datetimes
    df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")
    df["closed_date"]  = pd.to_datetime(df["closed_date"], errors="coerce")

    # KPI: Resolution & Flags
    df["resolution_time_h"] = (df["closed_date"] - df["created_date"]).dt.total_seconds() / 3600
    df["open_flag"] = df["status"].str.upper().ne("COMPLETED").astype(int)
    df["sla_met"] = ((df["status"].str.upper() == "COMPLETED") &
                     (df["resolution_time_h"] <= sla_hours)).astype(int)
    df["sla_target_h"] = sla_hours

    # Datum/Zeit-Splits (created)
    df["created_date_key"] = df["created_date"].dt.strftime("%Y%m%d").astype("Int64")
    df["created_date_only"] = df["created_date"].dt.date.astype("string")
    df["created_time"] = df["created_date"].dt.strftime("%H:%M:%S")
    df["created_hour"] = df["created_date"].dt.hour.astype("Int64")
    df["created_dow"]  = df["created_date"].dt.weekday.astype("Int64")  # 0=Mon … 6=Sun
    df["created_month"] = df["created_date"].dt.month.astype("Int64")

    # Datum/Zeit-Splits (closed)
    df["closed_date_key"] = df["closed_date"].dt.strftime("%Y%m%d").astype("Int64")
    df["closed_date_only"] = df["closed_date"].dt.date.astype("string")
    df["closed_time"] = df["closed_date"].dt.strftime("%H:%M:%S")
    df["closed_hour"] = df["closed_date"].dt.hour.astype("Int64")
    df["closed_dow"]  = df["closed_date"].dt.weekday.astype("Int64")
    df["closed_month"] = df["closed_date"].dt.month.astype("Int64")

    return df

# --------------------
# Load (Warehouse)
# --------------------
def ensure_schema(conn):
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS fact_requests;")
    cur.execute("DROP TABLE IF EXISTS dim_area_extended;")

    cur.execute("""
                

    CREATE TABLE fact_requests (
        sr_number TEXT PRIMARY KEY,
        request_type_name TEXT,
        owner_dept_name TEXT,
        status TEXT,

        -- originale Timestamps (als TEXT in ISO für SQLite)
        created_date TEXT,
        closed_date  TEXT,

        -- Splits & Keys
        created_date_key INTEGER,
        created_date_only TEXT,
        created_time TEXT,
        created_hour INTEGER,
        created_dow  INTEGER,
        created_month INTEGER,

        closed_date_key INTEGER,
        closed_date_only TEXT,
        closed_time TEXT,
        closed_hour INTEGER,
        closed_dow  INTEGER,
        closed_month INTEGER,

        ward_int INTEGER,
        community_area_int INTEGER,

        resolution_time_h REAL,
        sla_target_h REAL,
        open_flag INTEGER,
        sla_met INTEGER
        );
    """)

    cur.execute("""
    CREATE TABLE dim_area_extended (
        community_area_int INTEGER PRIMARY KEY,
        community_area_name TEXT,
        per_capita_income REAL,
        hardship_index REAL
    );
    """)

    conn.commit()
    logger.info("Warehouse schema ensured (fact_requests, dim_area_extended)")


def load_fact(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    """
    Lädt die transformierten 311-Daten (ETL) in die Tabelle fact_requests.
    Erwartet, dass transform_311() bereits alle KPI- und Split-Felder erzeugt hat.
    Konvertiert Datetime-Spalten in ISO-Strings für SQLite.
    """
    logger.info("Loading fact_requests")

    # defensive copy
    d = df.copy()

    # Datetime -> ISO-Strings (SQLite speichert TEXT)
    for col in ["created_date", "closed_date"]:
        if col in d.columns:
            d[col] = pd.to_datetime(d[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    # Spalten in der Reihenfolge, wie sie in der DDL definiert sind
    cols = [
        "sr_number",
        "request_type_name",   # aus sr_type umbenennen
        "owner_dept_name",     # aus owner_department umbenennen
        "status",

        "created_date",
        "closed_date",

        "created_date_key",
        "created_date_only",
        "created_time",
        "created_hour",
        "created_dow",
        "created_month",

        "closed_date_key",
        "closed_date_only",
        "closed_time",
        "closed_hour",
        "closed_dow",
        "closed_month",

        "ward_int",            # aus ward
        "community_area_int",  # aus community_area

        "resolution_time_h",
        "sla_target_h",
        "open_flag",
        "sla_met",
    ]

    # Umbenennen aus Rohspalten -> Zielschema
    d = d.rename(columns={
        "sr_type": "request_type_name",
        "owner_department": "owner_dept_name",
        "ward": "ward_int",
        "community_area": "community_area_int",
    })

    # Sicherstellen, dass alle Zielspalten existieren (falls manche im DF fehlen -> anlegen)
    for c in cols:
        if c not in d.columns:
            d[c] = pd.NA

    # Records bauen
    rows = d[cols].to_dict(orient="records")

    placeholders = ",".join(["?"] * len(cols))
    col_list = ",".join(cols)

    conn.executemany(f"""
        INSERT OR REPLACE INTO fact_requests
        ({col_list})
        VALUES ({placeholders});
    """, [tuple(r[c] for c in cols) for r in rows])

    conn.commit()
    logger.info(f"Loaded {len(rows):,} rows into fact_requests")


def load_dim_area_extended(conn, acs_small, df_map):
    logger.info("Loading dim_area_extended")

    ext = df_map.merge(
        acs_small,
        on=["community_area_int", "community_area_name"],
        how="left"
    )

    rows = ext.to_dict(orient="records")

    conn.executemany("""
    INSERT OR REPLACE INTO dim_area_extended
    (community_area_int, community_area_name, per_capita_income, hardship_index)
    VALUES (:community_area_int, :community_area_name, :per_capita_income, :hardship_index);
    """, rows)

    conn.commit()
    logger.info(f"Loaded {len(rows):,} rows into dim_area_extended")

# --------------------
# Health Check
# --------------------
def health_check(conn):
    n1 = conn.execute("SELECT COUNT(*) FROM fact_requests;").fetchone()[0]
    n2 = conn.execute("SELECT COUNT(*) FROM dim_area_extended;").fetchone()[0]
    logger.info(f"HealthCheck: fact_requests={n1:,}, dim_area_extended={n2:,}")

# --------------------
# Orchestrierung
# --------------------
def run():
    t0 = datetime.utcnow()
    logger.add("pipeline_etl.log", rotation="1 MB", retention=5)
    logger.info("Pipeline start (ETL mode)")

    df = extract_311(ROW_LIMIT)
    acs_small, df_map = extract_acs_and_mapping()

    tf = transform_311(df, SLA_HOURS)

    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_schema(conn)
        load_fact(conn, tf)
        load_dim_area_extended(conn, acs_small, df_map)

        health_check(conn)
        logger.info("Pipeline success (ETL)")
    except Exception as e:
        logger.exception(f"Pipeline failed (ETL): {e}")
        raise
    finally:
        conn.close()
        logger.info(f"Pipeline end (UTC) in {(datetime.utcnow()-t0).total_seconds():.1f}s")

if __name__ == "__main__":
    run()

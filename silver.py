"""
silver.py - build the silver layer from the bronze tables.

Bronze  = validated copy of the provider's files (METRO_BRONZE_DB.MYSTERY_SHOPPER)
Silver  = clean, typed, de-duplicated tables with the KPI  (METRO_SILVER_DB.MYSTERY_SHOPPER)

What silver does to every table listed in silver_config.json
  - column names are cleaned:  "Job ID" -> JOB_ID
  - text is trimmed and empty text becomes NULL
  - columns under "types" are converted (DATE, BOOLEAN, NUMBER(18,4), PERCENT for '94.87%')
    Values that cannot be converted become NULL and are counted in the log.
  - KPI column:   KPI_SCORE_PCT = points / possible points * 100
                  (NULL when possible points is empty or 0)
  - one row per "key": if a key appears more than once, the newest bronze row wins
  - bronze audit columns are kept as BRONZE_SOURCE_FILE / BRONZE_LOADED_AT;
    SILVER_LOADED_AT records when the row was written to silver

Load behaviour
  - FULL load    : first run (silver table does not exist yet) or  `python silver.py full`
                   The table is rebuilt from all of bronze.
  - INCREMENTAL  : every later run. Only bronze rows loaded after the newest
                   BRONZE_LOADED_AT already in silver are MERGEd in (new keys are
                   inserted, existing keys are updated).

Run directly:
    python silver.py          # full on first run, incremental afterwards
    python silver.py full     # force a full rebuild
"""

import json
import logging
import os
import re
import sys
from collections import namedtuple
from pathlib import Path

from connection import SF_DATABASE, SF_SCHEMA, get_snowflake
from validator import lit, q

SILVER_DATABASE = os.environ.get("SF_SILVER_DATABASE", "METRO_SILVER_DB")
SILVER_SCHEMA = os.environ.get("SF_SILVER_SCHEMA", SF_SCHEMA)
CONFIG_FILE = Path(__file__).parent / "silver_config.json"

# bronze audit column -> (name in silver, sql type)
AUDIT_COLUMNS = {
    "_SOURCE_FILE": ("BRONZE_SOURCE_FILE", "VARCHAR"),
    "_LOADED_AT": ("BRONZE_LOADED_AT", "TIMESTAMP_NTZ"),
}
PERCENT_TYPE = "NUMBER(9,4)"
TYPE_PATTERN = re.compile(r"^(PERCENT|[A-Z]+(\(\s*\d+\s*(,\s*\d+\s*)?\))?)$")

Col = namedtuple("Col", "name expr source dtype sql_type")


# ----------------------------------------------------------------- config ----
def normalize(name: str) -> str:
    """'Job ID' -> JOB_ID"""
    return re.sub(r"[^A-Za-z0-9]+", "_", str(name)).strip("_").upper()


def load_config(path=CONFIG_FILE) -> dict:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    config = {}
    for table, raw in data.items():
        if table.startswith("_"):
            continue
        types = {normalize(c): str(t).strip().upper() for c, t in raw.get("types", {}).items()}
        for c, t in types.items():
            if not TYPE_PATTERN.match(t):
                raise ValueError(f"{table}: type '{t}' for {c} is not valid")
        key = [normalize(c) for c in raw.get("key", [])]
        if not key:
            raise ValueError(f"{table}: 'key' is required in silver_config.json")
        kpi = None
        if raw.get("kpi"):
            k = raw["kpi"]
            kpi = {
                "name": normalize(k.get("name", "KPI_SCORE_PCT")),
                "numerator": normalize(k["numerator"]),
                "denominator": normalize(k["denominator"]),
                "decimals": int(k.get("decimals", 2)),
                "compare_to": normalize(k["compare_to"]) if k.get("compare_to") else None,
            }
        config[table.upper()] = {"key": key, "types": types, "kpi": kpi}
    return config


# ------------------------------------------------------------- SQL pieces ----
def cast_expr(column_sql: str, dtype) -> str:
    base = f"NULLIF(TRIM(TO_VARCHAR({column_sql})), '')"
    if not dtype:
        return base
    if dtype == "PERCENT":  # '94.87%' -> 94.87
        return f"TRY_CAST(TRIM(REPLACE({base}, '%', '')) AS {PERCENT_TYPE})"
    return f"TRY_CAST({base} AS {dtype})"


def bronze_fq(table: str) -> str:
    return f"{SF_DATABASE}.{SF_SCHEMA}.{table}"


def silver_fq(table: str) -> str:
    return f"{SILVER_DATABASE}.{SILVER_SCHEMA}.{table}"


def bronze_columns(cur, table: str) -> list:
    cur.execute(
        f"SELECT COLUMN_NAME FROM {SF_DATABASE}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = {lit(SF_SCHEMA.upper())} AND TABLE_NAME = {lit(table)} "
        "ORDER BY ORDINAL_POSITION"
    )
    return [r[0] for r in cur.fetchall()]


def silver_columns(cur, table: str) -> list:
    cur.execute(
        f"SELECT COLUMN_NAME FROM {SILVER_DATABASE}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = {lit(SILVER_SCHEMA.upper())} AND TABLE_NAME = {lit(table)} "
        "ORDER BY ORDINAL_POSITION"
    )
    return [r[0] for r in cur.fetchall()]


def plan(table: str, cfg: dict, columns: list) -> list:
    """One Col per bronze column (cleaned name, expression, type)."""
    result, seen = [], {}
    for c in columns:
        if c.upper() in AUDIT_COLUMNS:
            name, sql_type = AUDIT_COLUMNS[c.upper()]
            col = Col(name, q(c), c, None, sql_type)
        else:
            name = normalize(c)
            dtype = cfg["types"].get(name)
            sql_type = PERCENT_TYPE if dtype == "PERCENT" else (dtype or "VARCHAR")
            col = Col(name, cast_expr(q(c), dtype), c, dtype, sql_type)
        if name in seen:
            raise ValueError(f"{table}: columns '{seen[name]}' and '{c}' both become {name}")
        seen[name] = c
        result.append(col)

    by_name = {c.name: c for c in result}
    for k in cfg["key"]:
        if k not in by_name:
            raise ValueError(f"{table}: key column {k} not found. Columns: {', '.join(sorted(by_name))}")
    for t in cfg["types"]:
        if t not in by_name:
            raise ValueError(f"{table}: typed column {t} not found. Columns: {', '.join(sorted(by_name))}")

    kpi = cfg["kpi"]
    if kpi:
        for role in ("numerator", "denominator"):
            col = by_name.get(kpi[role])
            if col is None:
                raise ValueError(f"{table}: KPI {role} {kpi[role]} not found. Columns: {', '.join(sorted(by_name))}")
            if not (col.dtype or "").startswith("NUMBER"):
                raise ValueError(f"{table}: KPI {role} {kpi[role]} must have a NUMBER type in 'types'")
        if kpi["name"] in by_name:
            raise ValueError(f"{table}: KPI name {kpi['name']} is already a column")
        if kpi["compare_to"] and kpi["compare_to"] not in by_name:
            raise ValueError(f"{table}: compare_to {kpi['compare_to']} not found")
    return result


def kpi_expr(cfg: dict, cols: list) -> str:
    kpi = cfg["kpi"]
    by_name = {c.name: c for c in cols}
    num, den = by_name[kpi["numerator"]].expr, by_name[kpi["denominator"]].expr
    return (
        f"IFF({num} IS NOT NULL AND {den} IS NOT NULL AND {den} <> 0, "
        f"ROUND({num} / {den} * 100, {kpi['decimals']}), NULL)"
    )


def select_sql(table: str, cfg: dict, cols: list, only_new: bool) -> str:
    """The cleaned, de-duplicated rows. only_new=True limits it to rows newer than silver."""
    by_name = {c.name: c for c in cols}
    items = [f"{c.expr} AS {q(c.name)}" for c in cols]
    if cfg["kpi"]:
        items.append(f"{kpi_expr(cfg, cols)} AS {q(cfg['kpi']['name'])}")
    items.append(f"SYSDATE() AS {q('SILVER_LOADED_AT')}")

    key_exprs = [by_name[k].expr for k in cfg["key"]]
    where = " AND ".join(f"{e} IS NOT NULL" for e in key_exprs)
    bronze_cols = {c.source.upper() for c in cols}
    if only_new:
        where += (
            f' AND "_LOADED_AT" > (SELECT COALESCE(MAX("BRONZE_LOADED_AT"), '
            f"'1900-01-01'::TIMESTAMP_NTZ) FROM {silver_fq(table)})"
        )
    order = [f"{q(c)} DESC NULLS LAST" for c in ("_LOADED_AT", "_SOURCE_FILE") if c in bronze_cols]
    order_by = f"ORDER BY {', '.join(order)}" if order else ""
    return (
        f"SELECT {', '.join(items)} FROM {bronze_fq(table)} WHERE {where} "
        f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {', '.join(key_exprs)} {order_by}) = 1"
    )


def full_load_sql(table: str, cfg: dict, cols: list) -> str:
    return f"CREATE OR REPLACE TABLE {silver_fq(table)} AS {select_sql(table, cfg, cols, only_new=False)}"


def merge_sql(table: str, cfg: dict, cols: list) -> str:
    names = [c.name for c in cols]
    if cfg["kpi"]:
        names.append(cfg["kpi"]["name"])
    names.append("SILVER_LOADED_AT")
    on = " AND ".join(f"t.{q(k)} = s.{q(k)}" for k in cfg["key"])
    sets = ", ".join(f"t.{q(n)} = s.{q(n)}" for n in names if n not in cfg["key"])
    cols_sql = ", ".join(q(n) for n in names)
    vals_sql = ", ".join(f"s.{q(n)}" for n in names)
    return (
        f"MERGE INTO {silver_fq(table)} t USING ({select_sql(table, cfg, cols, only_new=True)}) s ON {on} "
        f"WHEN MATCHED THEN UPDATE SET {sets} "
        f"WHEN NOT MATCHED THEN INSERT ({cols_sql}) VALUES ({vals_sql})"
    )


# ----------------------------------------------------------------- build -----
def count(cur, sql: str) -> int:
    cur.execute(sql)
    return cur.fetchone()[0]


def add_missing_columns(cur, table: str, cfg: dict, cols: list, existing: list) -> None:
    """If bronze (or the KPI config) gained a column, add it to silver before MERGE."""
    have = {c.upper() for c in existing}
    wanted = [(c.name, c.sql_type) for c in cols]
    if cfg["kpi"]:
        wanted.append((cfg["kpi"]["name"], f"NUMBER(18,{cfg['kpi']['decimals']})"))
    wanted.append(("SILVER_LOADED_AT", "TIMESTAMP_NTZ"))
    for name, sql_type in wanted:
        if name not in have:
            logging.info("Silver %s: adding new column %s", table, name)
            cur.execute(f"ALTER TABLE {silver_fq(table)} ADD COLUMN {q(name)} {sql_type}")


def report_cast_failures(cur, table: str, cols: list) -> None:
    for c in cols:
        if c.dtype:
            raw = f"NULLIF(TRIM(TO_VARCHAR({q(c.source)})), '')"
            bad = count(cur, f"SELECT COUNT(*) FROM {bronze_fq(table)} WHERE {raw} IS NOT NULL "
                             f"AND {cast_expr(q(c.source), c.dtype)} IS NULL")
            if bad:
                logging.warning("Silver %s.%s: %s values could not be converted to %s (now NULL)",
                                table, c.name, bad, c.dtype)


def check_kpi(cur, table: str, cfg: dict) -> None:
    """Log how many rows differ from the provider's own score (difference above 0.01)."""
    kpi = cfg["kpi"]
    if not kpi or not kpi["compare_to"]:
        return
    k, p = q(kpi["name"]), q(kpi["compare_to"])
    total = count(cur, f"SELECT COUNT(*) FROM {silver_fq(table)} WHERE {k} IS NOT NULL AND {p} IS NOT NULL")
    diff = count(cur, f"SELECT COUNT(*) FROM {silver_fq(table)} WHERE {k} IS NOT NULL AND {p} IS NOT NULL "
                      f"AND ABS({k} - {p}) > 0.01")
    level = logging.warning if diff else logging.info
    level("Silver %s: %s KPI differs from provider %s in %s of %s rows",
          table, kpi["name"], kpi["compare_to"], diff, total)


def build_table(cur, table: str, cfg: dict, force_full: bool = False) -> None:
    columns = bronze_columns(cur, table)
    if not columns:
        logging.warning("Silver %s: bronze table not found, skipped", table)
        return
    cols = plan(table, cfg, columns)
    existing = silver_columns(cur, table)
    has_watermark = any(c.upper() == "_LOADED_AT" for c in columns)

    full = force_full or not existing or not has_watermark
    if full:
        reason = "forced" if force_full else ("first run" if not existing else "bronze has no _LOADED_AT")
        logging.info("Silver %s: FULL load (%s)", table, reason)
        report_cast_failures(cur, table, cols)
        cur.execute(full_load_sql(table, cfg, cols))
    else:
        add_missing_columns(cur, table, cfg, cols, existing)
        cur.execute(merge_sql(table, cfg, cols))
        row = cur.fetchone()
        logging.info("Silver %s: INCREMENTAL merge, %s rows inserted, %s rows updated", table, row[0], row[1])

    bronze_rows = count(cur, f"SELECT COUNT(*) FROM {bronze_fq(table)}")
    silver_rows = count(cur, f"SELECT COUNT(*) FROM {silver_fq(table)}")
    logging.info("Silver %s: %s bronze rows, %s silver rows", table, bronze_rows, silver_rows)
    check_kpi(cur, table, cfg)


def run_silver(full_refresh: bool = False) -> None:
    config = load_config()
    conn = get_snowflake()
    try:
        cur = conn.cursor()
        for table, cfg in config.items():
            build_table(cur, table, cfg, force_full=full_refresh)
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_silver(full_refresh=len(sys.argv) > 1 and sys.argv[1] == "full")
    logging.info("Silver finished")

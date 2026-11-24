"""
silver.py - build the silver layer from the bronze tables.

Bronze  = validated copy of the provider's files (METRO_BRONZE_DB.MYSTERY_SHOPPER)
Silver  = clean, typed, de-duplicated tables     (METRO_SILVER_DB.MYSTERY_SHOPPER)

For every table listed in silver_config.json:
  - column names are cleaned:  "Job ID" -> JOB_ID
  - text is trimmed and empty text becomes NULL
  - columns listed under "types" are converted (DATE, NUMBER(18,4), BOOLEAN ...);
    values that cannot be converted become NULL and are counted in the log
  - one row per "key": if the same key appears more than once in bronze, the row
    from the newest load wins
  - bronze audit columns are kept as BRONZE_SOURCE_FILE / BRONZE_LOADED_AT and
    SILVER_LOADED_AT records when silver was built

Silver tables are rebuilt from bronze on every run (CREATE OR REPLACE), which is
simple and safe for this data size. Run directly:   python silver.py
"""

import json
import logging
import os
import re
from pathlib import Path

from connection import SF_DATABASE, SF_SCHEMA, get_snowflake
from validator import lit, q

SILVER_DATABASE = os.environ.get("SF_SILVER_DATABASE", "METRO_SILVER_DB")
SILVER_SCHEMA = os.environ.get("SF_SILVER_SCHEMA", SF_SCHEMA)
CONFIG_FILE = Path(__file__).parent / "silver_config.json"

# bronze audit column -> name in silver
AUDIT_RENAME = {"_SOURCE_FILE": "BRONZE_SOURCE_FILE", "_LOADED_AT": "BRONZE_LOADED_AT"}
TYPE_PATTERN = re.compile(r"^[A-Z]+(\(\s*\d+\s*(,\s*\d+\s*)?\))?$")


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
        config[table.upper()] = {"key": key, "types": types}
    return config


def bronze_columns(cur, table: str) -> list:
    cur.execute(
        f"SELECT COLUMN_NAME FROM {SF_DATABASE}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = {lit(SF_SCHEMA.upper())} AND TABLE_NAME = {lit(table)} "
        "ORDER BY ORDINAL_POSITION"
    )
    return [r[0] for r in cur.fetchall()]


def plan(table: str, cfg: dict, columns: list) -> list:
    """Return [(silver_name, expression, bronze_column, type_or_None)] for every bronze column."""
    result, seen = [], {}
    for c in columns:
        if c.upper() in AUDIT_RENAME:
            name, expr, dtype = AUDIT_RENAME[c.upper()], q(c), None
        else:
            name = normalize(c)
            dtype = cfg["types"].get(name)
            expr = f"NULLIF(TRIM(TO_VARCHAR({q(c)})), '')"
            if dtype:
                expr = f"TRY_CAST({expr} AS {dtype})"
        if name in seen:
            raise ValueError(f"{table}: columns '{seen[name]}' and '{c}' both become {name}")
        seen[name] = c
        result.append((name, expr, c, dtype))
    names = {n for n, _, _, _ in result}
    for k in cfg["key"]:
        if k not in names:
            raise ValueError(f"{table}: key column {k} not found. Columns: {', '.join(sorted(names))}")
    for t in cfg["types"]:
        if t not in names:
            raise ValueError(f"{table}: typed column {t} not found. Columns: {', '.join(sorted(names))}")
    return result


def silver_sql(table: str, cfg: dict, cols: list) -> str:
    by_name = {n: (e, c) for n, e, c, _ in cols}
    select = ", ".join(f"{e} AS {q(n)}" for n, e, _, _ in cols)
    key_exprs = [by_name[k][0] for k in cfg["key"]]
    not_null = " AND ".join(f"{e} IS NOT NULL" for e in key_exprs)
    bronze_cols = {c.upper() for _, _, c, _ in cols}
    order = [f'{q(c)} DESC NULLS LAST' for c in ("_LOADED_AT", "_SOURCE_FILE") if c in bronze_cols]
    order_by = f"ORDER BY {', '.join(order)}" if order else ""
    return (
        f"CREATE OR REPLACE TABLE {SILVER_DATABASE}.{SILVER_SCHEMA}.{table} AS "
        f"SELECT {select}, SYSDATE() AS SILVER_LOADED_AT "
        f"FROM {SF_DATABASE}.{SF_SCHEMA}.{table} "
        f"WHERE {not_null} "
        f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {', '.join(key_exprs)} {order_by}) = 1"
    )


def count(cur, sql: str) -> int:
    cur.execute(sql)
    return cur.fetchone()[0]


def build_table(cur, table: str, cfg: dict) -> None:
    columns = bronze_columns(cur, table)
    if not columns:
        logging.warning("Silver %s: bronze table not found, skipped", table)
        return
    cols = plan(table, cfg, columns)
    bronze_fq = f"{SF_DATABASE}.{SF_SCHEMA}.{table}"

    # Values that exist in bronze but cannot be converted to the wanted type.
    for name, _, c, dtype in cols:
        if dtype:
            raw = f"NULLIF(TRIM(TO_VARCHAR({q(c)})), '')"
            bad = count(cur, f"SELECT COUNT(*) FROM {bronze_fq} WHERE {raw} IS NOT NULL "
                             f"AND TRY_CAST({raw} AS {dtype}) IS NULL")
            if bad:
                logging.warning("Silver %s.%s: %s values could not be converted to %s (now NULL)",
                                table, name, bad, dtype)

    cur.execute(silver_sql(table, cfg, cols))
    bronze_rows = count(cur, f"SELECT COUNT(*) FROM {bronze_fq}")
    silver_rows = count(cur, f"SELECT COUNT(*) FROM {SILVER_DATABASE}.{SILVER_SCHEMA}.{table}")
    logging.info("Silver %s: %s bronze rows -> %s silver rows (%s dropped: duplicate or empty key)",
                 table, bronze_rows, silver_rows, bronze_rows - silver_rows)


def run_silver() -> None:
    config = load_config()
    conn = get_snowflake()
    try:
        cur = conn.cursor()
        for table, cfg in config.items():
            build_table(cur, table, cfg)
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_silver()
    logging.info("Silver finished")

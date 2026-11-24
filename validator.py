"""
validator.py - data-quality checks for the SFTP -> Snowflake load.

For every new file:
  1. the file is loaded into a staging table  (<TABLE>_STG)
  2. each row is checked against the rules in validation_rules.json:
       - not_null         columns that must never be empty
       - primary_key      column(s) that must be unique (and are never empty)
       - accepted_values  columns that may only hold the listed values
  3. rows that pass go into the real table; rows that fail are written to
     <TABLE>_REJECTS together with the file name and the reason
  4. the result is recorded in FILE_AUDIT, so a file is never processed twice

Rules per table (all optional):
  "primary_key":      ["Job ID"]
  "not_null":         ["Job ID", "Job Date"]
  "accepted_values":  {"Wave": ["Wave 1", "Wave 2"]}      (compared as text)
  "existing_key":     "reject" | "update" | "allow"       (default "reject")
        what to do when a row's primary key is already in the real table
        reject = reject the row, update = overwrite the existing row (MERGE),
        allow  = do not check against the real table
  "on_failure":       "quarantine_rows" | "reject_file" | "report_only"
        quarantine_rows = load the good rows, reject the bad ones (default)
        reject_file     = if any row fails, load nothing from that file
        report_only     = load every row, but still record the violations

Column names in the rules are matched to the real column names ignoring upper/lower
case, spaces and underscores, so JOB_ID, "Job ID" and "job id" all find the column "Job ID".
"""

import json
import logging
import re
from pathlib import Path

COMPLETED = ("LOADED", "LOADED_WITH_REJECTS", "LOADED_WITH_WARNINGS", "SEEDED")
ON_FAILURE = ("quarantine_rows", "reject_file", "report_only")
EXISTING_KEY = ("reject", "update", "allow")
LOADED_AT = "_LOADED_AT"  # filled automatically with the load time if the table has it
SOURCE_FILE = "_SOURCE_FILE"  # filled with the CSV name if the table has it
AUDIT_COLUMNS = ((SOURCE_FILE, "VARCHAR"), (LOADED_AT, "TIMESTAMP_NTZ"))  # added to every bronze table

STAGE = "@sftp_stage"
FILE_FORMAT = "mystry_shopper_csv"  # must match sql/setup/02_file_format_stage.sql
SQL_DIR = Path(__file__).parent / "sql"


class RulesError(Exception):
    """The rules file or the rules for a table are not valid."""


# ----------------------------------------------------------------- helpers --
def q(name: str) -> str:
    """Quote a column name exactly as it is in the table."""
    return '"' + name.replace('"', '""') + '"'


def lit(value) -> str:
    """Snowflake string literal."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def loose(name: str) -> str:
    """Compare column names ignoring case, spaces and underscores."""
    return re.sub(r"[\s_]+", "", str(name)).upper()


def as_text(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


# ------------------------------------------------------------------- rules --
def default_rules() -> dict:
    return {
        "primary_key": [],
        "not_null": [],
        "accepted_values": {},
        "existing_key": "reject",
        "on_failure": "quarantine_rows",
    }


def load_rules(path) -> dict:
    """Read validation_rules.json -> {TABLE_NAME: rules}."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    rules = {}
    for table, raw in data.items():
        if table.startswith("_"):
            continue  # comments / notes
        merged = default_rules()
        for key, value in raw.items():
            if key.startswith("_"):
                continue
            if key not in merged:
                raise RulesError(f"{table}: unknown setting '{key}'")
            merged[key] = value
        if merged["on_failure"] not in ON_FAILURE:
            raise RulesError(f"{table}: on_failure must be one of {ON_FAILURE}")
        if merged["existing_key"] not in EXISTING_KEY:
            raise RulesError(f"{table}: existing_key must be one of {EXISTING_KEY}")
        rules[table.upper()] = merged
    return rules


def resolve_rules(table: str, rules: dict, actual_columns: list) -> dict:
    """Match the column names in the rules to the real column names."""
    by_upper = {c.upper(): c for c in actual_columns}
    by_loose = {}
    for c in actual_columns:
        by_loose.setdefault(loose(c), []).append(c)

    def fix(name):
        name = str(name).strip()
        if name.upper() in by_upper:
            return by_upper[name.upper()]
        matches = by_loose.get(loose(name), [])
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise RulesError(f"{table}: column '{name}' matches several columns: {', '.join(matches)}")
        raise RulesError(
            f"{table}: column '{name}' from validation_rules.json does not exist. "
            f"Columns in the table: {', '.join(actual_columns)}"
        )

    resolved = dict(rules)
    pk = [fix(c) for c in rules["primary_key"]]
    resolved["primary_key"] = pk
    not_null = [fix(c) for c in rules["not_null"]]
    for c in pk:  # a primary key is never empty
        if c not in not_null:
            not_null.append(c)
    resolved["not_null"] = not_null
    resolved["accepted_values"] = {
        fix(c): list(values) for c, values in rules["accepted_values"].items()
    }
    return resolved


# --------------------------------------------------------------- SQL pieces --
def reason_sql(table: str, rules: dict) -> tuple:
    """Return (join_sql, reason_expression) for the check query."""
    parts, join = [], ""

    for c in rules["not_null"]:
        parts.append(f"IFF(s.{q(c)} IS NULL, {lit('NULL:' + c)}, NULL)")

    for c, values in rules["accepted_values"].items():
        allowed = ", ".join(lit(as_text(v)) for v in values)
        parts.append(
            f"IFF(s.{q(c)} IS NOT NULL AND TO_VARCHAR(s.{q(c)}) NOT IN ({allowed}), "
            f"{lit('NOT_ACCEPTED:' + c)}, NULL)"
        )

    pk = rules["primary_key"]
    if pk:
        keys_present = " AND ".join(f"s.{q(c)} IS NOT NULL" for c in pk)
        partition = ", ".join(f"s.{q(c)}" for c in pk)
        parts.append(
            f"IFF({keys_present} AND COUNT(*) OVER (PARTITION BY {partition}) > 1, "
            f"'DUPLICATE_KEY', NULL)"
        )
        if rules["existing_key"] == "reject":
            key_cols = ", ".join(q(c) for c in pk)
            on = " AND ".join(f"s.{q(c)} = k.{q(c)}" for c in pk)
            join = f"LEFT JOIN (SELECT DISTINCT {key_cols}, 1 AS DQ_HIT FROM {table}) k ON {on}"
            parts.append("IFF(k.DQ_HIT IS NOT NULL, 'KEY_EXISTS_IN_TARGET', NULL)")

    if not parts:
        return join, "''"
    return join, "ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT(" + ", ".join(parts) + "), '; ')"


def check_sql(table: str, rules: dict) -> str:
    join, reason = reason_sql(table, rules)
    return (
        f"CREATE OR REPLACE TEMPORARY TABLE {table}_CHK AS "
        f"SELECT s.*, {reason} AS DQ_REASON FROM {table}_STG s {join}"
    ).strip()


OK = "COALESCE(DQ_REASON, '') = ''"


def value_expr(column: str) -> str:
    return "SYSDATE()" if column.upper() == LOADED_AT else q(column)


def insert_sql(table: str, columns: list, where: str) -> str:
    cols = ", ".join(q(c) for c in columns)
    exprs = ", ".join(value_expr(c) for c in columns)
    return f"INSERT INTO {table} ({cols}) SELECT {exprs} FROM {table}_CHK WHERE {where}"


def merge_sql(table: str, columns: list, key: list) -> str:
    cols = ", ".join(q(c) for c in columns)
    source = ", ".join(f"{value_expr(c)} AS {q(c)}" for c in columns)
    on = " AND ".join(f"t.{q(c)} = s.{q(c)}" for c in key)
    updates = ", ".join(f"t.{q(c)} = s.{q(c)}" for c in columns if c not in key)
    values = ", ".join(f"s.{q(c)}" for c in columns)
    update_clause = f"WHEN MATCHED THEN UPDATE SET {updates} " if updates else ""
    return (
        f"MERGE INTO {table} t USING "
        f"(SELECT {source} FROM {table}_CHK WHERE {OK}) s ON {on} "
        f"{update_clause}WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({values})"
    )


def rejects_sql(table: str, columns: list, file_name: str) -> str:
    cols = ", ".join(q(c) for c in columns)
    return (
        f"INSERT INTO {table}_REJECTS ({cols}, DQ_FILE_NAME, DQ_REASON, DQ_REJECTED_AT) "
        f"SELECT {cols}, {lit(file_name)}, DQ_REASON, SYSDATE() "
        f"FROM {table}_CHK WHERE NOT ({OK})"
    )


def audit_sql(file_name, table, status, rows_read, rows_loaded, rows_rejected, message) -> str:
    return (
        "INSERT INTO FILE_AUDIT (FILE_NAME, TARGET_TABLE, STATUS, ROWS_READ, ROWS_LOADED, "
        "ROWS_REJECTED, MESSAGE) VALUES "
        f"({lit(file_name)}, {lit(table)}, {lit(status)}, {int(rows_read)}, "
        f"{int(rows_loaded)}, {int(rows_rejected)}, {lit(str(message)[:900])})"
    )


# ------------------------------------------------------------- Snowflake ----
def get_columns(cur, table: str) -> list:
    cur.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = CURRENT_SCHEMA() AND TABLE_NAME = {lit(table)} "
        "ORDER BY ORDINAL_POSITION"
    )
    return [row[0] for row in cur.fetchall()]


def drop_not_null(cur, table: str) -> None:
    """Staging and reject tables must accept empty values, so they can be checked."""
    cur.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = CURRENT_SCHEMA() AND TABLE_NAME = {lit(table)} "
        "AND IS_NULLABLE = 'NO'"
    )
    for (column,) in cur.fetchall():
        cur.execute(f"ALTER TABLE {table} ALTER COLUMN {q(column)} DROP NOT NULL")


def ensure_audit_columns(cur, table: str, columns: list) -> list:
    """Add _SOURCE_FILE and _LOADED_AT to the table if they are missing. Returns the columns."""
    have = {c.upper() for c in columns}
    added = False
    for name, data_type in AUDIT_COLUMNS:
        if name not in have:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {data_type}")
            added = True
    return get_columns(cur, table) if added else columns


def prepare_tables(cur, table: str) -> None:
    stg, rej = f"{table}_STG", f"{table}_REJECTS"
    # Re-create staging every time so it always has the same columns as the real table.
    cur.execute(f"CREATE OR REPLACE TRANSIENT TABLE {stg} LIKE {table}")
    cur.execute(f"CREATE TABLE IF NOT EXISTS {rej} LIKE {table}")
    drop_not_null(cur, stg)
    drop_not_null(cur, rej)
    for column, data_type in AUDIT_COLUMNS:
        cur.execute(f"ALTER TABLE {rej} ADD COLUMN IF NOT EXISTS {column} {data_type}")
    for column, data_type in (
        ("DQ_FILE_NAME", "VARCHAR"),
        ("DQ_REASON", "VARCHAR"),
        ("DQ_REJECTED_AT", "TIMESTAMP_NTZ"),
    ):
        cur.execute(f"ALTER TABLE {rej} ADD COLUMN IF NOT EXISTS {column} {data_type}")
    cur.execute(f"TRUNCATE TABLE {stg}")


def one_value(cur, sql: str):
    cur.execute(sql)
    return cur.fetchone()[0]


def completed_files(cur) -> set:
    statuses = ", ".join(lit(s) for s in COMPLETED)
    cur.execute(f"SELECT DISTINCT FILE_NAME FROM FILE_AUDIT WHERE STATUS IN ({statuses})")
    return {row[0] for row in cur.fetchall()}


def process_file(cur, file_name: str, table: str, rules: dict) -> dict:
    """Validate one file already PUT into the stage, and load it. Returns a result dict."""
    result = dict(file=file_name, table=table, status="FAILED",
                  rows_read=0, rows_loaded=0, rows_rejected=0, message="")
    try:
        columns = get_columns(cur, table)
        if not columns:
            # First load of this table: create it from the CSV header of this file.
            template = (SQL_DIR / "load" / "create_table_from_stage.sql").read_text()
            cur.execute(template.format(table=table, sample_file=file_name))
            columns = get_columns(cur, table)
            if not columns:
                raise RulesError(f"table {table} could not be created from {file_name}")
            logging.info("Created table %s from %s", table, file_name)
        columns = ensure_audit_columns(cur, table, columns)
        rules = resolve_rules(table, rules, columns)
        mode = rules["on_failure"]

        prepare_tables(cur, table)
        cur.execute(
            f"COPY INTO {table}_STG FROM {STAGE} FILES = ({lit(file_name)}) "
            f"FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}') "
            "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE"
        )
        if any(c.upper() == SOURCE_FILE for c in columns):
            cur.execute(f"UPDATE {table}_STG SET {q(SOURCE_FILE)} = {lit(file_name)}")
        cur.execute(check_sql(table, rules))

        rows_read = one_value(cur, f"SELECT COUNT(*) FROM {table}_CHK")
        rows_rejected = one_value(cur, f"SELECT COUNT(*) FROM {table}_CHK WHERE NOT ({OK})")
        result.update(rows_read=rows_read, rows_rejected=rows_rejected)

        if rows_rejected:
            cur.execute(
                f"SELECT DQ_REASON, COUNT(*) FROM {table}_CHK WHERE NOT ({OK}) "
                "GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
            )
            reasons = "; ".join(f"{r[0]} x{r[1]}" for r in cur.fetchall())
            logging.warning("%s: %s of %s rows failed checks (%s)",
                            file_name, rows_rejected, rows_read, reasons)
            result["message"] = reasons

        if mode == "reject_file" and rows_rejected:
            status, rows_loaded = "REJECTED", 0
            write_rows = False
        else:
            write_rows = True
            if mode == "report_only":
                rows_loaded = rows_read
                status = "LOADED_WITH_WARNINGS" if rows_rejected else "LOADED"
            else:
                rows_loaded = rows_read - rows_rejected
                status = "LOADED_WITH_REJECTS" if rows_rejected else "LOADED"

        cur.execute("BEGIN")
        try:
            cur.execute(f"DELETE FROM {table}_REJECTS WHERE DQ_FILE_NAME = {lit(file_name)}")
            if rows_rejected:
                cur.execute(rejects_sql(table, columns, file_name))
            if write_rows:
                if mode == "report_only":
                    cur.execute(insert_sql(table, columns, "TRUE"))
                elif rules["primary_key"] and rules["existing_key"] == "update":
                    cur.execute(merge_sql(table, columns, rules["primary_key"]))
                else:
                    cur.execute(insert_sql(table, columns, OK))
            cur.execute(audit_sql(file_name, table, status, rows_read, rows_loaded,
                                  rows_rejected, result["message"]))
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise

        result.update(status=status, rows_loaded=rows_loaded)
        logging.info("%s -> %s: %s (read %s, loaded %s, rejected %s)",
                     file_name, table, status, rows_read, rows_loaded, rows_rejected)
    except Exception as exc:  # record the failure; the file is retried on the next run
        logging.error("%s -> %s failed: %s", file_name, table, exc)
        result.update(status="FAILED", message=str(exc))
        cur.execute(audit_sql(file_name, table, "FAILED", result["rows_read"], 0,
                              result["rows_rejected"], str(exc)))
    return result


def seed_existing(cur, files_by_table: dict) -> int:
    """Mark files that were loaded before validation existed as done."""
    done = completed_files(cur)
    count = 0
    for file_name, table in files_by_table.items():
        if file_name in done:
            continue
        cur.execute(audit_sql(file_name, table, "SEEDED", 0, 0, 0,
                              "loaded before validation was enabled"))
        count += 1
    return count


def audit_existing(cur, table: str, rules: dict) -> list:
    """Run the rules over the rows already in the table (read-only). Returns summary lines."""
    columns = get_columns(cur, table)
    if not columns:
        return [f"{table}: table not found"]
    rules = resolve_rules(table, rules, columns)
    total = one_value(cur, f"SELECT COUNT(*) FROM {table}")
    lines = [f"{table}: {total} rows"]
    checked = False

    for c in rules["not_null"]:
        n = one_value(cur, f"SELECT COUNT(*) FROM {table} WHERE {q(c)} IS NULL")
        lines.append(f"  not_null {c}: {n} empty")
        checked = True

    pk = rules["primary_key"]
    if pk:
        keys = ", ".join(q(c) for c in pk)
        groups = one_value(
            cur,
            f"SELECT COUNT(*) FROM (SELECT 1 FROM {table} GROUP BY {keys} HAVING COUNT(*) > 1)",
        )
        rows = one_value(
            cur,
            f"SELECT COALESCE(SUM(n), 0) FROM (SELECT COUNT(*) AS n FROM {table} "
            f"GROUP BY {keys} HAVING COUNT(*) > 1)",
        )
        lines.append(f"  primary_key ({', '.join(pk)}): {groups} keys repeated, {rows} rows involved")
        checked = True

    for c, values in rules["accepted_values"].items():
        allowed = ", ".join(lit(as_text(v)) for v in values)
        where = f"{q(c)} IS NOT NULL AND TO_VARCHAR({q(c)}) NOT IN ({allowed})"
        n = one_value(cur, f"SELECT COUNT(*) FROM {table} WHERE {where}")
        lines.append(f"  accepted_values {c}: {n} rows outside the list")
        if n:
            cur.execute(
                f"SELECT TO_VARCHAR({q(c)}), COUNT(*) FROM {table} WHERE {where} "
                "GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
            )
            seen = ", ".join(f"{r[0]!r} x{r[1]}" for r in cur.fetchall())
            lines.append(f"    values found: {seen}")
        checked = True

    if not checked:
        lines.append("  (no rules set for this table)")
    return lines

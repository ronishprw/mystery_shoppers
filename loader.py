"""
loader.py - SFTP -> Snowflake stage -> validated bronze layer.

Run by the Azure Function (function_app.py calls run_load) or from the command line:

    python loader.py                 # load: new SFTP files -> stage -> validate -> bronze
    python loader.py seed-existing   # one-time: mark files loaded before validation as done
    python loader.py audit-existing  # read-only: run the rules over rows already in bronze

Steps of a normal load
  1. SFTP   : list the mystery_shopping_*.csv files on the provider's server
  2. STAGE  : download only files not yet in @sftp_stage and PUT them there
  3. BRONZE : for every file not yet processed (FILE_AUDIT), validator.process_file()
              loads it into <TABLE>_STG, checks it against validation_rules.json,
              writes good rows to the bronze table and bad rows to <TABLE>_REJECTS,
              and records the result in FILE_AUDIT.

Reruns are safe: staged files are not downloaded again, and a file that is already
LOADED / LOADED_WITH_REJECTS / LOADED_WITH_WARNINGS / SEEDED in FILE_AUDIT is skipped.
Files that FAILED or were REJECTED are retried on the next run.
"""

import logging
import sys
import tempfile
from pathlib import Path, PurePosixPath

from connection import SFTP_DIR, get_sftp, get_snowflake
from validator import (
    STAGE,
    audit_existing,
    completed_files,
    default_rules,
    load_rules,
    process_file,
    seed_existing,
)

BASE_DIR = Path(__file__).parent
SQL_DIR = BASE_DIR / "sql"
RULES_FILE = BASE_DIR / "validation_rules.json"
KINDS = ("categories", "jobs", "questions")


def read_sql(relative_path: str) -> str:
    return (SQL_DIR / relative_path).read_text()


def table_for(name: str):
    """MYSTERY_SHOPPING_<KIND> for a file name, or None if the name is not recognised."""
    for kind in KINDS:
        if f"mystery_shopping_{kind}__" in name:
            return f"MYSTERY_SHOPPING_{kind.upper()}"
    return None


def setup(conn) -> None:
    """Create the file format, stage and FILE_AUDIT table if they do not exist."""
    conn.execute_string(read_sql("setup/02_file_format_stage.sql"))
    conn.execute_string(read_sql("setup/03_file_audit.sql"))


# ------------------------------------------------------- 1 + 2: SFTP -> stage --
def staged_files(cur) -> set:
    cur.execute(f"LIST {STAGE}")
    return {PurePosixPath(row[0]).name for row in cur.fetchall()}


def land_files(sftp, cur) -> list:
    """Copy new CSVs from SFTP into the stage. Returns every CSV name found on SFTP."""
    names = sorted(n for n in sftp.listdir(SFTP_DIR) if n.lower().endswith(".csv"))
    already = staged_files(cur)
    new = [n for n in names if n not in already]
    logging.info("SFTP has %d CSV files; %d new to stage", len(names), len(new))
    with tempfile.TemporaryDirectory() as tmp:
        for name in new:
            local = Path(tmp) / name
            sftp.get(f"{SFTP_DIR}/{name}", str(local))
            cur.execute(f"PUT 'file://{local.as_posix()}' {STAGE} AUTO_COMPRESS=FALSE")
            logging.info("Staged %s", name)
            local.unlink()
    return names


# ------------------------------------------------------------ 3: bronze load --
def run_load() -> list:
    """SFTP -> stage -> validate -> bronze. Returns one result dict per file processed."""
    rules = load_rules(RULES_FILE)
    transport, sftp = get_sftp()
    conn = get_snowflake()
    results = []
    try:
        setup(conn)
        cur = conn.cursor()
        names = land_files(sftp, cur)

        done = completed_files(cur)
        todo = [n for n in names if n not in done and table_for(n)]
        for n in names:
            if not table_for(n):
                logging.warning("Ignoring %s (name does not match a known table)", n)
        logging.info("%d files to process, %d already done", len(todo), len(names) - len(todo))

        for name in todo:
            table = table_for(name)
            table_rules = rules.get(table)
            if table_rules is None:
                logging.warning("No rules for %s, using defaults (no checks)", table)
                table_rules = default_rules()
            results.append(process_file(cur, name, table, table_rules))

        failed = [r for r in results if r["status"] == "FAILED"]
        for r in results:
            logging.info("RESULT %(file)s -> %(table)s: %(status)s read=%(rows_read)s "
                         "loaded=%(rows_loaded)s rejected=%(rows_rejected)s", r)
        if failed:
            raise RuntimeError(f"{len(failed)} file(s) failed: " + ", ".join(r["file"] for r in failed))
    finally:
        sftp.close()
        transport.close()
        conn.close()
    return results


# -------------------------------------------------------------- other modes --
def run_seed_existing() -> None:
    """One-time: mark every CSV on SFTP as already loaded (use only if bronze already holds them)."""
    transport, sftp = get_sftp()
    conn = get_snowflake()
    try:
        setup(conn)
        cur = conn.cursor()
        names = land_files(sftp, cur)
        count = seed_existing(cur, {n: table_for(n) for n in names if table_for(n)})
        logging.info("Marked %d files as SEEDED", count)
    finally:
        sftp.close()
        transport.close()
        conn.close()


def run_audit_existing() -> None:
    """Read-only report of rule violations in the rows already in bronze."""
    rules = load_rules(RULES_FILE)
    conn = get_snowflake()
    try:
        cur = conn.cursor()
        for table, table_rules in rules.items():
            for line in audit_existing(cur, table, table_rules):
                logging.info(line)
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    mode = sys.argv[1] if len(sys.argv) > 1 else "load"
    if mode == "load":
        run_load()
    elif mode == "seed-existing":
        run_seed_existing()
    elif mode == "audit-existing":
        run_audit_existing()
    else:
        raise SystemExit("mode must be: load | seed-existing | audit-existing")
    logging.info("Done (%s)", mode)

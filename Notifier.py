"""
notifier.py - email a summary after every run (success or failure).

Uses only Python's standard library (smtplib). Settings come from environment
variables (Azure: Function App > Settings > Environment variables):

  NOTIFY_EMAILS    people to email, separated by commas or semicolons
  SMTP_SERVER      e.g. smtp.office365.com or smtp.gmail.com
  SMTP_PORT        587 (STARTTLS, default) or 465 (SSL)
  SMTP_USERNAME    the sending mailbox login
  SMTP_PASSWORD    its password / app password
  MAIL_FROM        sender address (defaults to SMTP_USERNAME)

If NOTIFY_EMAILS or SMTP_SERVER is missing, nothing is sent. Sending an email can
never make the pipeline fail: any mail error is only written to the log.
"""

import logging
import os
import re
import smtplib
import traceback
from datetime import datetime, timezone
from email.message import EmailMessage


def recipients() -> list:
    raw = os.environ.get("NOTIFY_EMAILS", "")
    return [a.strip() for a in re.split(r"[;,\s]+", raw) if a.strip()]


def build_message(results: list, silver_lines: list, error) -> tuple:
    """Return (subject, body)."""
    results = results or []
    rejected = sum(int(r.get("rows_rejected") or 0) for r in results)
    loaded = sum(int(r.get("rows_loaded") or 0) for r in results)
    failed_files = [r for r in results if r.get("status") == "FAILED"]

    if error or failed_files:
        status = "FAILED"
    elif rejected:
        status = "SUCCESS (some rows rejected)"
    else:
        status = "SUCCESS"

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"Mystery Shopper pipeline: {status}",
        f"Finished: {now}",
        "",
        f"Bronze: {len(results)} file(s) processed, {loaded} rows loaded, {rejected} rows rejected",
    ]
    if results:
        lines.append("")
        for r in results:
            lines.append(
                f"  {r['file']}\n"
                f"    {r['table']}: {r['status']}  read={r['rows_read']} "
                f"loaded={r['rows_loaded']} rejected={r['rows_rejected']}"
            )
    elif not error:
        lines.append("  No new files on SFTP this time.")

    if silver_lines:
        lines += ["", "Silver:"] + [f"  {s}" for s in silver_lines]

    if error:
        text = "".join(traceback.format_exception(type(error), error, error.__traceback__))
        lines += ["", f"Error: {type(error).__name__}: {error}", "", text[-3000:]]
    if rejected:
        lines += ["", "Rejected rows are in the *_REJECTS tables (reason in DQ_REASON); see FILE_AUDIT."]

    return f"[{status}] Mystery Shopper pipeline", "\n".join(lines)


def send_email(subject: str, body: str) -> None:
    to = recipients()
    server = os.environ.get("SMTP_SERVER")
    if not to or not server:
        logging.info("Email notification skipped (NOTIFY_EMAILS or SMTP_SERVER not set)")
        return
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USERNAME")
    password = os.environ.get("SMTP_PASSWORD")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = os.environ.get("MAIL_FROM") or user
    msg["To"] = ", ".join(to)
    msg.set_content(body)

    if port == 465:
        smtp = smtplib.SMTP_SSL(server, port, timeout=30)
    else:
        smtp = smtplib.SMTP(server, port, timeout=30)
    with smtp:
        if port != 465:
            smtp.starttls()
        if user and password:
            smtp.login(user, password)
        smtp.send_message(msg)
    logging.info("Notification email sent to %d recipient(s)", len(to))


def notify_run(results: list, silver_lines: list, error=None) -> None:
    """Build and send the summary. Never raises."""
    try:
        subject, body = build_message(results, silver_lines, error)
        send_email(subject, body)
    except Exception:
        logging.exception("Could not send notification email (pipeline result is not affected)")

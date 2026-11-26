import logging
import os

import azure.functions as func

app = func.FunctionApp()

# The schedule is read from the App Setting TIMER_SCHEDULE (it MUST exist, or the function will not load).
#   Normal value:  0 0 13 * * 1      = every Monday at 13:00 UTC = 5:00 AM PST (UTC-8)
#   Quick test:    0 */5 * * * *     = every 5 minutes (set it back afterwards!)
# Format: second minute hour day month day-of-week  (1 = Monday). Times are UTC.
@app.timer_trigger(
    schedule="%TIMER_SCHEDULE%",
    arg_name="timer",
    run_on_startup=False
)
def sftp_to_bronze(timer: func.TimerRequest) -> None:
    try:
        from notifier import notify_run
    except Exception:  # a missing or broken notifier must never stop the load
        logging.exception("notifier.py could not be imported - emails are disabled for this run")

        def notify_run(*args, **kwargs):
            pass

    results, silver_lines, error = [], [], None
    try:
        logging.info("Starting Bronze ingestion")
        from loader import run_load
        results = run_load()
        logging.info("Bronze ingestion completed")

        # Silver runs only when the App Setting RUN_SILVER is "true".
        # First run builds silver in full, later runs are incremental.
        if os.environ.get("RUN_SILVER", "").strip().lower() == "true":
            logging.info("Starting Silver build")
            from silver import run_silver
            silver_lines = run_silver()
            logging.info("Silver build completed")
    except Exception as exc:
        error = exc
        results = getattr(exc, "results", results)  # per-file results of a failed bronze run
        logging.exception("Pipeline failed")
        raise  # the run is still marked as failed in Azure
    finally:
        notify_run(results, silver_lines, error)  # email on success and on failure

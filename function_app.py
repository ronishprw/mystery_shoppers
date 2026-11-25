import logging
import os

import azure.functions as func

app = func.FunctionApp()

# Every Monday at 13:00 UTC = 5:00 AM PST (UTC-8).
# Format: second minute hour day month day-of-week  (1 = Monday). Times are UTC.
@app.timer_trigger(
    schedule="0 0 13 * * 1",
    arg_name="timer",
    run_on_startup=False
)
def sftp_to_bronze(timer: func.TimerRequest) -> None:
    logging.info("Starting Bronze ingestion")
    from loader import run_load
    run_load()
    logging.info("Bronze ingestion completed")

    # Silver runs only when the App Setting RUN_SILVER is "true".
    # First run builds silver in full, later runs are incremental.
    if os.environ.get("RUN_SILVER", "").strip().lower() == "true":
        logging.info("Starting Silver build")
        from silver import run_silver
        run_silver()
        logging.info("Silver build completed")

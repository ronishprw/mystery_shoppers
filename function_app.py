"""
function_app.py - timer-triggered Azure Function.

1. loader.run_load()     SFTP -> stage -> validation -> bronze tables
2. silver.run_silver()   bronze -> silver tables   (only when App Setting RUN_SILVER=true)
"""

import logging
import os

import azure.functions as func

from loader import run_load
from silver import run_silver

app = func.FunctionApp()


# Daily at 10:00 UTC. Change the schedule here if needed.
@app.timer_trigger(schedule="0 0 10 * * *", arg_name="timer", run_on_startup=False)
def sftp_to_snowflake(timer: func.TimerRequest) -> None:
    logging.info("sftp_to_snowflake started")
    try:
        run_load()
        if os.environ.get("RUN_SILVER", "").strip().lower() == "true":
            run_silver()
    except Exception:
        logging.exception("sftp_to_snowflake failed")
        raise  # mark the run as failed in Azure
    logging.info("sftp_to_snowflake finished")

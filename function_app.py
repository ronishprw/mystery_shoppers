import logging
import azure.functions as func

app = func.FunctionApp()

@app.timer_trigger(
    schedule="0 0 10 * * *",
    arg_name="timer",
    run_on_startup=False
)
def sftp_to_bronze(timer: func.TimerRequest) -> None:
    logging.info("Starting Bronze ingestion")
    from loader import run_load
    run_load()
    logging.info("Bronze ingestion completed")

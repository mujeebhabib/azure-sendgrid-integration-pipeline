import logging
import json
from pythonjsonlogger import jsonlogger

def setup_logging():
    """
    Configure structured JSON logging for Azure Functions.
    """

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Remove default handlers (Azure adds its own)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    log_handler = logging.StreamHandler()

    formatter = jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        json_ensure_ascii=False
    )

    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)

    logging.info("Structured JSON logging initialised.")

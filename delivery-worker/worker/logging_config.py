import logging
import os

from pythonjsonlogger import jsonlogger


def configure_logging() -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    log_file = os.environ.get("LOG_FILE")
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    formatter = jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    for handler in handlers:
        handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=handlers, force=True)

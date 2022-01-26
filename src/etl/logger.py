import os
import logging
from logging.handlers import RotatingFileHandler


def get_logger(log: str):  # type: ignore
    """
    Simple function to set up a logger
    """

    path = os.path.dirname(os.path.abspath(__file__))
    log_file = os.path.join(path, "data/logs/etl.log")

    logger = logging.getLogger(log)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(filename)s | %(funcName)s | %(message)s"
    )

    file_handler = RotatingFileHandler(log_file, maxBytes=2000000, backupCount=0)
    file_handler.setFormatter(formatter)

    stream = logging.StreamHandler()
    stream.setLevel(logging.INFO)
    stream.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream)

    return logger


if __name__ == "__main__":

    logger = get_logger("main")
    print(type(logger))

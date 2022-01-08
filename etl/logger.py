import logging
from logging.handlers import RotatingFileHandler
from config import loadconfig


CONF_FILE = "config.yaml"


def get_logger(log):

    conf = loadconfig(CONF_FILE)
    log_file = conf["log"]["filename"]

    logger = logging.getLogger(log)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)s | %(filename)s | %(funcName)s | %(message)s')

    file_handler = RotatingFileHandler(
        log_file, maxBytes=2000000, backupCount=0)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return(logger)

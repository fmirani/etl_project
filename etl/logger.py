import os
import logging
from logging.handlers import RotatingFileHandler
from etl.config import loadconfig


def get_logger(log):
    '''
    Simple function to set up a logger
    '''

    curr_path = os.path.dirname(os.path.abspath(__file__))

    conf = loadconfig()

    log_file = os.path.join(curr_path, conf["log"]["filename"])

    logger = logging.getLogger(log)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)s | %(filename)s | %(funcName)s | %(message)s')

    file_handler = RotatingFileHandler(
        log_file, maxBytes=2000000, backupCount=0)
    file_handler.setFormatter(formatter)

    stream = logging.StreamHandler()
    stream.setLevel(logging.INFO)
    stream.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream)

    return(logger)

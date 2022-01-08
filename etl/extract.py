import os
import pandas as pd
from transform import transform_youtube_data, transform_netflix_data
from load import update_db
from config import loadconfig
from logger import get_logger


CONF_FILE = "config.yaml"
logger = get_logger("extract")


def update_data(service: str, data_file: str) -> pd.DataFrame:
    '''
    Function to orchestrate the operation
    1. fetch data from latest files
    2. update the database
    3. return number of items added
    '''

    # Read new data
    if service == "youtube":  # from youtube
        if os.path.exists(data_file):  # Does the file exist?
            data = transform_youtube_data(data_file)  # Yes it does
        else:  # No it does not !
            logger.error("YouTube data file not found")
            return 0
    elif service == "netflix":  # from netflix
        if os.path.exists(data_file):  # Does the file exist
            data = transform_netflix_data(data_file)  # Yes it does
        else:  # No it does not !
            logger.error("Netflix data file not found")
            return 0
    else:  # only youtube or netflix possible
        logger.error("Incorrect service name")
        return 0

    # Update the database
    items_updated = update_db(data)

    return(items_updated)


if __name__ == "__main__":

    conf = loadconfig(CONF_FILE)

    yt_history = conf["youtube"]["history_file"]
    nf_history = conf["netflix"]["history_file"]

    logger.info(f"Start of program.")

    yt_data = update_data("youtube", yt_history)
    logger.info(f"{yt_data} items updated for YouTube in the table")

    nf_data = update_data("netflix", nf_history)
    logger.info(f"{nf_data} items updated for Netflix in the table")

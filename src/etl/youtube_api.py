import os
import json
import time
import string
from typing import Tuple, Any
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from etl.config import loadconfig
from etl.logger import get_logger


logger = get_logger("youtube")


def get_api_key() -> str:

    conf = loadconfig()
    api_path = os.path.expanduser(conf["global"]["data_path"])
    api_file = os.path.join(api_path, conf["youtube"]['api'])

    # Return empty strings if the api file does not exist
    if not os.path.exists(api_file):
        logger.error("Missing file: ", api_file)
        logger.error("Please create an api file")
        return("")

    with open(api_file, "r") as f:
        api_key = f.readline()

    if api_key == "YOUTUBE_API_KEY_GOES_HERE" or len(api_key) < 5:
        logger.error("Incorrect API. Please set correct API in the file")
        return("")

    return(api_key)


def create_cat_file(cfile: str) -> None:
    '''
    Function to create a video Categories file
    '''

    # No need to create a new file, one exists already
    if os.path.exists(cfile):
        logger.info("YouTube categories file exists already")
        return

    logger.info("Creating a new YouTube categories file")

    api_key = get_api_key()

    if len(api_key) == "":
        return

    logger.info(f"YouTube api_key: {api_key}")

    # Build Google API instance
    youtube = build("youtube", "v3", developerKey=api_key)

    # Prepare API Call
    request = youtube.videoCategories().list(part="snippet", regionCode="US")

    try:  # Try to get response
        response = request.execute()
    except HttpError as err:  # API call didn't work
        logger.error(err)
        logger.error("Could not create YouTube categories file. Exiting")
        raise HttpError

    cats = response["items"]
    dict = {}

    # Set up everything in a dict
    for cat in cats:
        dict[cat["id"]] = cat["snippet"]["title"]

    # Dump the dict in the category file
    with open(cfile, "w") as f:
        json.dump(dict, f)

    logger.info(f"YouTube categories file created")


def get_missing_data(link: str) -> Tuple[Any, Any]:
    '''
    Function to fill missing data from YouTube
    '''
    path = os.path.dirname(os.path.abspath(__file__))
    cat_file = os.path.join(path, "../../config/cats.json")

    # Load YouTube categroty file
    with open(cat_file) as jfile:
        cats = json.load(jfile)

    api_key = get_api_key()

    if len(api_key) == "":
        logger.error(f"Invalid or no API provided")
        return(1, 1)

    # Create a "build" instance
    youtube = build("youtube", "v3", developerKey=api_key)

    # Extract the video Id from the link
    id = link.split("v=")[1]

    logger.debug("Preparing YouTube API call")
    # Prepare to call videos.list() for "id" filter
    request = youtube.videos().list(
        part="snippet",
        id=id)

    try:  # Try to get response
        response = request.execute()
        logger.debug("YouTube API call successful")
    except HttpError as err:  # API call didn't work
        logger.error(f"YouTube API call failed: {err}")
        logger.error(f"Could not retrieve information for: {link}")
        # If the problem is from the remote end
        if err.resp.status in [403, 500, 503]:
            logger.warning(
                "Going to sleep for 5 secs and then return empty strings")
            time.sleep(5)
        return("", "")

    # Check to see if some data is returned in the API call
    if response["pageInfo"]["totalResults"] == 0:
        logger.debug("API response empty. Returning empty strings")
        return("", "")

    # Fill both columns with info retrieved from YouTube
    name = response["items"][0]["snippet"]["title"]
    cat = cats[response["items"][0]["snippet"]["categoryId"]]
    logger.debug(f"Name: {name}, Categories: {cat}")

    # Had to add this to remove any unprintable chars returned by API call
    name = ''.join([str(char) for char in name if char in string.printable])
    return(name, cat)

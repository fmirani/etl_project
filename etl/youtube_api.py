import os
import json
import time
import string
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from config import loadconfig
from logger import get_logger


logger = get_logger("youtube")


def create_cat_file(cfile: str) -> None:
    '''
    Function to create a video Categories file
    '''
    logger.info("Youtube categories file to be created now")
    conf = loadconfig()
    api_key = conf["youtube"]["api_key"]

    # Build Google API instance
    youtube = build("youtube", "v3", developerKey=api_key)

    # Prepare API Call
    request = youtube.videoCategories().list(part="snippet", regionCode="US")

    try:  # Try to get response
        response = request.execute()
    except HttpError as err:  # API call didn't work
        logger.error(f"API Call failed: {err}")
        logger.error("Could not create YouTube categories file. Exiting")
        exit()

    cats = response["items"]
    dict = {}

    # Set up everything in a dict
    for cat in cats:
        dict[cat["id"]] = cat["snippet"]["title"]

    # Dump the dict in the category file
    with open(cfile, "w") as f:
        json.dump(dict, f)


def fill_missing_data(link: str) -> tuple[str, str]:
    '''
    Function to fill missing data from YouTube
    '''

    conf = loadconfig()
    cat_file = conf["youtube"]["cat_file"]
    api_key = conf["youtube"]["api_key"]

    # If YouTube categroty file does not exist, create it
    if not os.path.exists(cat_file):
        logger.info("YouTube categories file not found.")
        create_cat_file(cat_file)

    # Load YouTube categroty file
    with open(cat_file) as jfile:
        cats = json.load(jfile)

    # Create a "build" instance
    youtube = build("youtube", "v3", developerKey=api_key)

    # Extract the video Id from the link
    id = link.split("v=")[1]

    # Prepare to call videos.list() for "id" filter
    request = youtube.videos().list(
        part="snippet",
        id=id)

    try:  # Try to get response
        response = request.execute()
    except HttpError as err:  # API call didn't work
        logger.error(f"API call failed: {err}")
        logger.error(f"Could not retrieve information for: {link}")
        # If the problem is from the remote end
        if err.resp.status in [403, 500, 503]:
            logger.info(
                "Going to sleep for 5 secs and then return empty strings")
            time.sleep(5)
        return("", "")

    # Check to see if some data is returned in the API call
    if response["pageInfo"]["totalResults"] == 0:
        logger.info("API response empty. Returning empty strings")
        return("", "")

    # Fill both columns with info retrieved from YouTube
    name = response["items"][0]["snippet"]["title"]
    cat = cats[response["items"][0]["snippet"]["categoryId"]]

    # Had to add this to remove any unprintable chars returned by API call
    name = ''.join([str(char) for char in name if char in string.printable])
    return(name, cat)

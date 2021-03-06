import os
import json
import time
import string
from etl.main import ETL
from typing import Tuple
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from etl.logger import get_logger

logger = get_logger("youtube")


def get_missing_data(link: str) -> Tuple[str, str]:
    """
    Function to fill missing data from YouTube
    """

    instance = ETL()
    api_key: str = instance.get_api()

    if len(api_key) != 39:
        logger.error("Invalid API key or API not set")
        return ("Invalid API", "Invalid API")

    # Load YouTube categroty file in 'cats'
    path = os.path.dirname(os.path.abspath(__file__))
    cat_file = os.path.join(path, "data/cats.json")
    with open(cat_file) as jfile:
        cats = json.load(jfile)

    # Extract the video Id from the link
    if "https://www.youtube.com/watch?v=" not in link:
        logger.error("Invalid YouTube link")
        return ("Invalid link", "Invalid link")
    id: str = link.split("v=")[1]

    logger.debug("Preparing YouTube API call")

    try:  # Try to get response
        youtube = build("youtube", "v3", developerKey=api_key)
        request = youtube.videos().list(part="snippet", id=id)
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
        return ("", "")

    # Check to see if some data is returned in the API call
    if response["pageInfo"]["totalResults"] == 0:
        logger.debug("API response empty. Returning empty strings")
        return ("", "")

    # Fill both columns with info retrieved from YouTube
    name: str = response["items"][0]["snippet"]["title"]
    cat: str = cats[response["items"][0]["snippet"]["categoryId"]]
    logger.debug(f"Name: {name}, Categories: {cat}")

    # Had to add this to remove any unprintable chars returned by API call
    name = "".join([str(char) for char in name if char in string.printable])
    return (name, cat)

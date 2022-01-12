import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup as bs
from config import loadconfig
from logger import get_logger


logger = get_logger("transform")


def transform_data(service: str, data_file: str) -> pd.DataFrame:
    '''
    Simple function to guide the request to the right function
    '''
    if service == "youtube":
        return(transform_youtube_data(data_file))
    else:
        return(transform_netflix_data(data_file))


def transform_youtube_data(filename: str) -> pd.DataFrame:
    '''
    Function to fetch youtube data from the history file
    1. Create a new dataframe to put data in
    2. parse the html file to find required data
    3. Format the data as needed
    4. Populate the dataframe
    '''
    logger.info("Transforming YouTube data now")
    conf = loadconfig()
    simulated = conf["youtube"]["simulated"]
    simulate_offset = conf["youtube"]["simulate_offset"]

    # Create a new dataframe
    data = pd.DataFrame(
        columns=["Timestamp", "Source", "Type", "Name", "Season", "Episode", "Category", "Link"])

    # Initialize some empty lists for the dataframe
    link = []
    timestamp = []

    # Open the watch history html file and parse through it for relevant data
    with open(filename, encoding='utf8') as f:
        soup = bs(f, "html.parser")
        tags = soup.find_all(
            "div", {"class": "content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1"})
        for i, tag in enumerate(tags):
            a_pointer = tag.find("a")
            dt = a_pointer.next_sibling.next_sibling
            date_time = datetime.strptime(
                str(dt)[:-4], "%b %d, %Y, %I:%M:%S %p")
            # If data fetching is simulated
            if simulated and date_time + timedelta(days=simulate_offset) > datetime.now():
                continue
            timestamp.append(date_time)
            link.append(a_pointer.text)

    # Populate the dataframe with the data
    data["Timestamp"] = timestamp
    data["Source"] = "YouTube"
    data["Type"] = "Video"
    data["Link"] = link

    # Return dataframe
    return(data)


def transform_netflix_data(filename: str) -> pd.DataFrame:
    '''
    Function to fetch netflix data from the history file
    1. Create a new dataframe to put data in
    2. parse the csv file to find required data
    3. Format the data as needed
    4. Populate the dataframe
    '''
    logger.info("Transforming Netflix data now")
    conf = loadconfig()
    simulated = conf["youtube"]["simulated"]
    simulate_offset = conf["youtube"]["simulate_offset"]

    # Create a new dataframe
    data = pd.DataFrame(
        columns=["Timestamp", "Source", "Type", "Name", "Season", "Episode", "Category", "Link"])

    # Read csv data into a separate dataframe
    nf_data = pd.read_csv(filename)

    # Import Timestamp column to our datadrame as datetime
    data["Timestamp"] = pd.to_datetime(nf_data["Date"], format="%m/%d/%y")

    # Set "Source" column to "Netflix"
    data["Source"] = "Netflix"

    # Import Name column to our dataframe
    data["Name"] = nf_data["Title"]

    # Keywords to identify if a title is a TV series
    keywds = ["Season", "Series", "Limited", "Part", "Volume", "Chapter"]

    # Set "Type" column to either "Movie" or "TV Series"
    data.loc[data["Name"].str.contains(
        "|".join(keywds)), "Type"] = "TV Series"
    data.loc[data["Type"].isnull(), "Type"] = "Movie"

    # Wherever Type is "TV Series" split the Title column
    # in three: Name, Season and Episode
    data.loc[data["Type"] == "TV Series", "Name"] = nf_data["Title"].str.rsplit(
        ":", n=2, expand=True)[0]
    data.loc[data["Type"] == "TV Series", "Season"] = nf_data["Title"].str.rsplit(
        ":", n=2, expand=True)[1]
    data.loc[data["Type"] == "TV Series", "Episode"] = nf_data["Title"].str.rsplit(
        ":", n=2, expand=True)[2]
    # Some cleaning needed in Episode column
    data["Episode"] = data["Episode"].str.strip()

    # If data fetching is simulated
    if simulated:
        data = data.loc[pd.to_datetime(
            data["Timestamp"]) < datetime.now() - timedelta(days=simulate_offset)]

    # return DataFrame
    return(data)

from config import loadconfig
import sqlite3 as sqlite
import pandas as pd
from youtube_api import fill_missing_data
from imdb_api import get_genre
from logger import get_logger


CONF_FILE = "config.yaml"
logger = get_logger("load")


def get_select_query(item) -> tuple[str, str]:
    '''
    Function to set up the SELECT query
    '''

    timestamp = str(item["Timestamp"])
    link = item["Link"]
    name = item["Name"]
    episode = item["Episode"]

    if item["Source"] == "YouTube":  # Source is YouTube
        query = "SELECT * FROM watched "\
            "WHERE timestamp = ? AND vlink = ?;"
        values = (timestamp, link)
    else:  # Source is Netflix
        if item["Type"] == "Movie":  # Type is Movie
            query = "SELECT * FROM watched "\
                "WHERE timestamp = ? AND vname = ?"
            values = (timestamp, name)
        else:  # Type is TV Series
            query = "SELECT * FROM watched "\
                "WHERE timestamp = ? AND vname = ? AND episode = ?"
            values = (timestamp, name, episode)

    return(query, values)


def get_insert_query(item) -> tuple[str, str]:
    '''
    Function to set up the INSERT query
    '''

    timestamp = str(item["Timestamp"])
    link = item["Link"]
    name = item["Name"]
    season = item["Season"]
    episode = item["Episode"]
    source = item["Source"]
    _type = item["Type"]
    cat = item["Category"]

    if source == "YouTube":
        name, cat = fill_missing_data(link)
    else:
        cat = get_genre(name)

    query = "INSERT INTO watched "\
        "(timestamp, source, type, vname, season, episode, category, vlink) "\
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?);"
    values = (timestamp, source, _type, name, season, episode, cat, link)

    return(query, values)


def update_db(data: pd.DataFrame) -> int:
    '''
    Function to update the database
    1. connect to the database
    2. run queries to see if update is needed
    3. insert new items in the database if applicable
    4. returns the number of items added
    '''

    conf = loadconfig(CONF_FILE)

    added = 0

    try:  # to connect to the database
        db_name = conf["database"]["name"]
        conn = sqlite.connect(db_name)
        logger.info(f"Database {conn} connected")

        # and create a cursor to perform operations
        cursor = conn.cursor()
    except Exception as err:
        logger.error(f"Unable to connect to the Database: {err}")
        logger.error("Please check the configuration in the yaml file.")
        return 0

    # i = 0
    for ind, item in data.iterrows():
        # if i >= 30:
        #     break
        #     i += 1
        select_query, values = get_select_query(item)
        try:  # to execute SELECT query
            cursor.execute(select_query, values)
            record = cursor.fetchall()
            logger.info("Select query successful")
        except Exception as err:
            logger.error(f"ERROR: {err}")
            logger.error("Unable to execute SELECT query.")
            return (0)

        # If SELECT query returns any result(s),
        # no need to INSERT
        if len(record) > 0:
            logger.info(f"But item {ind} exists already in the database")
            continue

        insert_query, values = get_insert_query(item)

        try:  # to execute INSERT query
            cursor.execute(insert_query, values)
            added += 1
            logger.info("Insert query successful")
            logger.info(f"Item no. {ind} added to database")
        except Exception as err:
            logger.error(f"ERROR: {err}")
            logger.error("Unable to execute INSERT query.")
            return (0)

        # Commit the INSERTs to the database in batches of 100
        if added % 100 == 0:
            conn.commit()

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("PostgreSQL connection is now closed")

    return(added)

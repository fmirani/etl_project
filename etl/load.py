import os
import sqlite3 as sqlite
import pandas as pd
from etl.youtube_api import get_missing_data
from etl.imdb_api import get_genre
from etl.logger import get_logger
from etl.config import loadconfig
from concurrent.futures import ThreadPoolExecutor


logger = get_logger("load")


def exec_select_query(item: pd.DataFrame, cursor: object) -> bool:
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

    try:  # to execute SELECT query
        cursor.execute(query, values)
        record = cursor.fetchall()
        logger.debug("Select query successful")
    except Exception as err:
        logger.error(f"ERROR: {err}")
        logger.error("Unable to execute SELECT query.")
        exit()

    # If SELECT query returns any result(s),
    # no need to INSERT
    if len(record) > 0:
        logger.debug(f"But this item already exists in the database")
        return(False)

    return(True)


def get_insert_query(item) -> tuple[str, str]:
    '''
    Function to set up the INSERT query
    '''

    # Not proud of this but it is what it is :)
    timestamp = str(item[0])
    source = item[1]
    _type = item[2]
    name = item[3]
    season = item[4]
    episode = item[5]
    cat = item[6]
    link = item[7]

    if source == "YouTube":
        name, cat = get_missing_data(link)
    else:
        cat = get_genre(name)

    query = "INSERT INTO watched "\
        "(timestamp, source, type, vname, season, episode, category, vlink) "\
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?);"
    values = (timestamp, source, _type, name, season, episode, cat, link)

    return(query, values)


def create_database(db_name: str) -> None:

    logger.info("Creating and setting up a new database")

    try:  # to connect to the database
        conn = sqlite.connect(db_name)
        logger.info(f"Database {conn} connected")
    except Exception as err:
        logger.error(f"Unable to connect to the Database: {err}")
        logger.error("Please check the configuration in the yaml file.")
        exit(1)

    # CREATE TABLE query
    query = 'CREATE TABLE watched '\
        '(id INTEGER PRIMARY KEY AUTOINCREMENT, '\
        '"timestamp" TIMESTAMP WITHOUT TIME ZONE NOT NULL, '\
        'source CHARACTER VARYING(255), '\
        '"type" CHARACTER VARYING(255), '\
        'vname CHARACTER VARYING(355), '\
        'season CHARACTER VARYING(255), '\
        'episode CHARACTER VARYING(255), '\
        'category CHARACTER VARYING(255), '\
        'vlink category CHARACTER VARYING(355) );'

    # Create a cursor to perform operations
    cursor = conn.cursor()

    try:  # to execute CREATE TABLE query
        cursor.execute(query)
        logger.debug("CREATE TABLE query successful")
    except Exception as err:
        logger.error(f"ERROR: {err}")
        logger.error("Unable to create table in the datebase.")
        exit(1)

    cursor.close()
    conn.close()


def load_data(data: pd.DataFrame) -> int:
    '''
    Function to update the database
    1. connect to the database
    2. run select queries to see if db update is needed
    3. run insert queries to add items in the database
    4. returns the number of items added to the db
    '''

    conf = loadconfig()
    data_path = os.path.expanduser(conf["global"]["data_path"])
    db_name = os.path.join(data_path, conf["database"]["name"])

    if not os.path.exists(db_name):
        logger.info(f"No database found at '{db_name}'")
        create_database(db_name)

    try:  # to connect to the database
        conn = sqlite.connect(db_name)
        logger.info(f"Database {conn} connected")
    except Exception as err:
        logger.error(f"Unable to connect to the Database: {err}")
        logger.error("Please check the configuration in the yaml file.")
        return 0

    # Create a cursor to perform operations
    cursor = conn.cursor()

    # Apply exec_select_query to all the row of the dataframe
    # exec_select_query returns:
    # True -> if this item needs to be added in the database
    # False -> if the item already exists in the database
    data["to_insert"] = data.apply(exec_select_query, args=(cursor,), axis=1)

    items = len(data[(data["to_insert"])])
    logger.info(f"{items} item(s) to add in the database")

    # Convert the relevant rows in the dataframe to list format
    data_list = data[data.to_insert].values.tolist()

    # Starting a context manager to handle parallel threads.
    # Passing each row of the dataframe to get_insert_query
    # in a separate thread.
    logger.info("Starting context manager for threading")
    with ThreadPoolExecutor(max_workers=5) as executor:
        p_queries = zip(executor.map(
            get_insert_query, data_list))

    logger.info("Out of context manager now")

    list_queries = list(p_queries)

    added: int = 0
    for tuple_query in list_queries:
        for query in tuple_query:
            # No query to execute, continue if true
            if len(query) < 1:
                logger.debug("No query to run for INSERT")
                continue

            try:  # to execute INSERT query
                cursor.execute(query[0], query[1])
                logger.debug("Insert query successful")
                added += 1
            except Exception as err:
                logger.error(f"ERROR: {err}")
                logger.error("Unable to execute INSERT query.")
                continue
        # Commit in batches of 100 to be on the safe side
        if added % 100 == 0:
            conn.commit()

    conn.commit()
    logger.info(f"{added} items added in the database")

    cursor.close()
    conn.close()
    logger.info("PostgreSQL connection is now closed")

    return(added)

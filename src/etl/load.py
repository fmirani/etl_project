import os
import sqlite3 as sqlite
import pandas as pd
from typing import List, Tuple
from etl.youtube_api import get_missing_data
from etl.youtube_api import create_cat_file
from etl.imdb_api import get_genre
from etl.logger import get_logger
from etl.config import loadconfig
from concurrent.futures import ThreadPoolExecutor


logger = get_logger("load")


def exec_select_query(item: pd.Series) -> bool:
    '''
    Function to set up the SELECT query
    '''

    # Connect to the database and create a cursor object
    conn = connect_db()
    cursor = conn.cursor()  # type: ignore

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
            values = (timestamp, name, episode)  # type: ignore

    cursor.execute(query, values)
    record = cursor.fetchall()
    logger.debug("Select query successful")

    insert = True

    # If SELECT query returns any result(s),
    # no need to INSERT
    if len(record) > 0:
        logger.debug(f"But this item already exists in the database")
        insert = False

    cursor.close()
    conn.close()    # type: ignore
    return(insert)


def exec_insert_query(item: List[str]) -> bool:
    '''
    Function to set up the INSERT query
    It receives an iterable object of 8 elements (0-7)
    and return a boolean value
    '''

    # Connect to the database and create a cursor object
    conn = connect_db()
    cursor = conn.cursor()  # type: ignore

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

    cursor.execute(query, values)
    logger.debug("Insert query successful")

    cursor.close()
    conn.close()    # type: ignore
    return(True)


def create_db(db_name: str) -> None:
    '''
    Function to create a new table called 'watched'
    '''

    logger.info("Creating and setting up a new database named 'watched'")

    conn = sqlite.connect(db_name)
    cursor = conn.cursor()
    logger.info(f"Database {conn} connected")

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

    # Execute CREATE TABLE query
    cursor.execute(query)
    logger.debug("CREATE TABLE query successful")

    cursor.close()
    conn.close()


def connect_db() -> object:
    '''
    This function connects to the database and returns
    and connection object back to the caller
    '''

    conf = loadconfig()
    data_path = os.path.expanduser(conf["global"]["data_path"])
    db_name = os.path.join(data_path, conf["database"]["name"])

    if not os.path.exists(db_name):
        logger.info(f"No database found at '{db_name}'")
        create_db(db_name)

    conn = sqlite.connect(db_name)
    logger.info(f"Database {conn} connected")

    return(conn)


def load_data(data: pd.DataFrame) -> int:
    '''
    Function to update the database
    1. connect to the database
    2. run select queries to see if db update is needed
    3. run insert queries to add items in the database
    4. returns the number of items added to the db
    '''

    # Apply exec_select_query to all the rows of the dataframe
    # exec_select_query returns:
    # True -> if this item needs to be added in the database
    # False -> if the item already exists in the database
    data["to_insert"] = data.apply(exec_select_query, axis=1)

    items = len(data[(data["to_insert"])])
    logger.info(f"{items} item(s) to add in the database")

    # Create YouTube categories file if it doesn't exist
    path = os.path.dirname(os.path.abspath(__file__))
    cat_file = os.path.join(path, "../../config/cats.json")
    create_cat_file(cat_file)

    # Convert the relevant rows in the dataframe to list format
    data_list = data[data.to_insert].values.tolist()

    # Starting a context manager to handle parallel threads.
    # Passing each row of the dataframe to get_insert_query
    # in a separate thread.
    logger.info("Starting context manager for multi-threading")
    with ThreadPoolExecutor(max_workers=5) as executor:
        inserts = executor.map(exec_insert_query, data_list)

    logger.info("Out of context manager now")

    added = sum(map(lambda x: x, list(inserts)))
    logger.info(f"{added} items added in the database")

    return(added)

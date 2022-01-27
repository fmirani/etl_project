import os
import sqlite3 as sqlite
import pandas as pd
from etl.main import ETL
from etl.youtube_api import get_missing_data
from etl.imdb_api import get_genre
from etl.logger import get_logger
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple


logger = get_logger("load")


def exec_select_query(item: pd.Series) -> bool:
    """
    Function to set up the SELECT query
    """

    instance = ETL()
    conn, cursor = instance.get_connection()

    timestamp = str(item["Timestamp"])
    link = item["Link"]
    name = item["Name"]
    episode = item["Episode"]

    if item["Source"] == "YouTube":  # Source is YouTube
        query = "SELECT * FROM watched " "WHERE timestamp = ? AND vlink = ?;"
        values = (timestamp, link)
    else:  # Source is Netflix
        if item["Type"] == "Movie":  # Type is Movie
            query = "SELECT * FROM watched " "WHERE timestamp = ? AND vname = ?"
            values = (timestamp, name)
        else:  # Type is TV Series
            query = (
                "SELECT * FROM watched "
                "WHERE timestamp = ? AND vname = ? AND episode = ?"
            )
            values = (timestamp, name, episode)  # type: ignore

    cursor.execute(query, values)
    record = cursor.fetchall()

    insert = True

    # If SELECT query returns any result(s),
    # no need to INSERT
    if len(record) > 0:
        logger.debug(f"But this item already exists in the database")
        insert = False

    return insert


def exec_insert_query(
    item: List[str],
) -> Tuple[str, Tuple[str, str, str, str, str, str, str, str]]:
    """
    Function to set up the INSERT query
    It receives an iterable object of 8 elements (0-7)
    and returns a boolean value
    """

    # Not proud of the following,
    # will one day come back to this to improve it
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

    if isinstance(name, int) or isinstance(cat, int):
        logger.error(f"INSERT query cancelled due to invalid API")
        return False

    query = (
        "INSERT INTO watched "
        "(timestamp, source, type, vname, season, episode, category, vlink) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?);"
    )
    values = (timestamp, source, _type, name, season, episode, cat, link)

    return (query, values)


def create_db() -> None:
    """
    Function to create a new table called 'watched'
    """

    logger.info("Creating and setting up a new database named 'watched'")

    instance = ETL()
    db_name = instance.get_db()

    conn = sqlite.connect(db_name)
    cursor = conn.cursor()

    instance.set_connection(conn, cursor)

    logger.info(f"Database connected")

    # CREATE TABLE query
    query = (
        "CREATE TABLE watched "
        "(id INTEGER PRIMARY KEY AUTOINCREMENT, "
        '"timestamp" TIMESTAMP WITHOUT TIME ZONE NOT NULL, '
        "source CHARACTER VARYING(255), "
        '"type" CHARACTER VARYING(255), '
        "vname CHARACTER VARYING(355), "
        "season CHARACTER VARYING(255), "
        "episode CHARACTER VARYING(255), "
        "category CHARACTER VARYING(255), "
        "vlink category CHARACTER VARYING(355) );"
    )

    # Execute CREATE TABLE query
    cursor.execute(query)
    logger.debug("CREATE TABLE query successful")

    logger.debug("Success")


def load_data(data: pd.DataFrame) -> int:
    """
    Function to update the database
    1. connect to the database
    2. run select queries to see if db update is needed
    3. run insert queries to add items in the database
    4. returns the number of items added to the db
    """

    # Check if there exists a database already
    # If not, create a new one
    instance = ETL()
    db_name = instance.get_db()

    if not os.path.exists(db_name):
        logger.info(f"No database found at '{db_name}'")
        logger.info(f"Setting up a new database at '{db_name}'")
        create_db()
        conn, cursor = instance.get_connection()
    else:
        logger.info(f"Database found at '{db_name}'. Connecting..")
        conn = sqlite.connect(db_name)
        cursor = conn.cursor()
        instance.set_connection(conn, cursor)

    # Check if there is any data in dataframe to load in the DB
    if data.shape[0] == 0:
        logger.warning("No data to add to the database")
        return 0

    # Apply exec_select_query to all the rows of the dataframe
    # exec_select_query returns:
    # True -> if this item needs to be added in the database
    # False -> if the item already exists in the database

    data["to_insert"] = data.apply(exec_select_query, axis=1)

    items = len(data[(data["to_insert"])])
    logger.info(f"{items} item(s) to add in the database")

    # Convert the relevant rows in the dataframe to list format
    data_list = data[data.to_insert].values.tolist()

    # Starting a context manager to handle parallel threads.
    # Passing each row of the dataframe to get_insert_query
    # in a separate thread.
    logger.info("Starting context manager for multi-threading")
    with ThreadPoolExecutor(max_workers=5) as executor:
        query_data = executor.map(exec_insert_query, data_list)

    logger.info("Out of context manager now")

    if len(list(query_data)) == 0:
        return(0)

    added: int = 0
    logger.info("Executing INSERT queries now")
    for query, values in query_data:
        print("Query: ", query)
        print("Values: ", list(values))
        cursor.execute(query, list(values))
        logger.debug(f"{values} added")
        added += 1
        if added % 100 == 0:
            conn.commit()

    logger.info(f"{added} items added in the database")

    conn.commit()
    cursor.close()
    conn.close()

    return added

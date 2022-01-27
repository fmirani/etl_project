import os
import sqlite3 as sqlite
from etl.main import ETL
from etl.transform import transform_data
from etl.load import load_data, create_db, exec_select_query, get_insert_query
from concurrent.futures import ThreadPoolExecutor


def test_create_db() -> None:

    instance = ETL()
    db_name = instance.get_db()

    # Testing nominal case
    if os.path.exists(db_name):
        os.remove(db_name)

    conn = sqlite.connect(db_name)
    cursor = conn.cursor()
    instance.set_connection(conn, cursor)

    create_db()

    query = (
        "SELECT count(name) "
        "FROM sqlite_master "
        "WHERE type='table' AND name='watched';"
    )
    cursor.execute(query)

    assert cursor.fetchone()[0] == 1

    cursor.close()
    conn.close()

    # Remove any databases created for testing purposes
    if os.path.exists(db_name):
        os.remove(db_name)


def test_queries() -> None:

    path = os.path.dirname(os.path.abspath(__file__))
    yt_history = os.path.join(path, "../src/etl/data/yt_sample.html")
    nf_history = os.path.join(path, "../src/etl/data/nf_sample.csv")

    instance = ETL()
    db_name = instance.get_db()

    if os.path.exists(db_name):
        os.remove(db_name)

    conn = sqlite.connect(db_name)
    cursor = conn.cursor()
    instance.set_connection(conn, cursor)

    create_db()

    # Testing nominal case
    for item in [["youtube", yt_history], ["netflix", nf_history]]:
        data = transform_data(item[0], item[1])
        if data.shape[0] > 5:
            data = data.sample(n=5)

        data["to_insert"] = data.apply(exec_select_query, axis=1)

        # Part 1: the database is empty
        assert data[data.to_insert == True].shape[0] == 5

        data_list = data[data.to_insert].values.tolist()
        with ThreadPoolExecutor(max_workers=5) as executor:
            query_data = executor.map(get_insert_query, data_list)

        for query, values in query_data:
            cursor.execute(query, list(values))
        conn.commit()

        # Part 2: the database is full
        data["to_insert"] = data.apply(exec_select_query, axis=1)

        assert data[data.to_insert == True].shape[0] == 0

    cursor.close()
    conn.close()

    if os.path.exists(db_name):
        os.remove(db_name)


def test_load_data() -> None:

    instance = ETL()
    db_name = instance.get_db()

    path = os.path.dirname(os.path.abspath(__file__))
    yt_history = os.path.join(path, "../src/etl/data/yt_sample.html")
    nf_history = os.path.join(path, "../src/etl/data/nf_sample.csv")

    # Testing the function when database doesn't exist already
    if os.path.exists(db_name):
        os.remove(db_name)
    added: int = 0
    for item in [["youtube", yt_history], ["netflix", nf_history]]:
        data = transform_data(item[0], item[1])
        if data.shape[0] > 5:
            data = data.head(5)
        added += load_data(data)
    assert added == 10

    # Testing the function when database exists already but empty
    if os.path.exists(db_name):
        os.remove(db_name)

    conn = sqlite.connect(db_name)
    cursor = conn.cursor()
    instance.set_connection(conn, cursor)
    create_db()
    cursor.close()
    conn.close()

    added = 0
    for item in [["youtube", yt_history], ["netflix", nf_history]]:
        data = transform_data(item[0], item[1])
        if data.shape[0] > 5:
            data = data.head(5)
        added += load_data(data)
    assert added == 10

    # Testing the function when database exists and full
    added = 0
    for item in [["youtube", yt_history], ["netflix", nf_history]]:
        data = transform_data(item[0], item[1])
        if data.shape[0] > 5:
            data = data.head(5)
        added += load_data(data)
    assert added == 0


if __name__ == "__main__":
    test_create_db()
    test_queries()
    test_load_data()

import os
from etl.main import ETL
from etl.transform import transform_data
from etl.load import load_data, create_db, exec_select_query, exec_insert_query
from concurrent.futures import ThreadPoolExecutor


def test_create_db() -> None:

    instance = ETL()
    db_name = instance.get_db()

    # Remove older database files
    if os.path.exists(db_name):
        os.remove(db_name)
        assert not os.path.exists(db_name)

    create_db()
    assert os.path.exists(db_name)

    # Remove any databases created for testing purposes
    if os.path.exists(db_name):
        os.remove(db_name)


def test_queries() -> None:

    path = os.path.dirname(os.path.abspath(__file__))
    yt_history = os.path.join(path, "../src/etl/data/yt_sample.html")
    nf_history = os.path.join(path, "../src/etl/data/nf_sample.csv")

    instance = ETL()
    db_name = instance.get_db()

    # Create database if it doesn't exist
    if not os.path.exists(db_name):
        create_db()

    for item in [["youtube", yt_history], ["netflix", nf_history]]:

        data = transform_data(item[0], item[1])

        if data.shape[0] > 5:
            data = data.sample(n=5)

        data["to_insert"] = data.apply(exec_select_query, axis=1)
        if data.shape[0] > 0:
            assert len(data[data.to_insert == ""]) == 0

        data_list = data[data.to_insert].values.tolist()

        with ThreadPoolExecutor(max_workers=5) as executor:
            query_data = executor.map(exec_insert_query, data_list)

        assert len(data_list) == len(list(query_data))

    if os.path.exists(db_name):
        os.remove(db_name)


def test_load_data() -> None:

    path = os.path.dirname(os.path.abspath(__file__))
    yt_history = os.path.join(path, "../src/etl/data/yt_sample.html")
    nf_history = os.path.join(path, "../src/etl/data/nf_sample.csv")

    added = 0
    for item in [["youtube", yt_history], ["netflix", nf_history]]:

        data = transform_data(item[0], item[1])
        if data.shape[0] > 5:
            data = data.sample(n=5)

        added += load_data(data)

    assert isinstance(added, int)


if __name__ == "__main__":
    test_create_db()
    test_queries()
    test_load_data()

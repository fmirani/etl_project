import os
from etl.main import ETL
from etl.transform import transform_data
from etl.load import load_data, create_db, exec_select_query, exec_insert_query
from concurrent.futures import ThreadPoolExecutor

def test_create_db():

    instance = ETL()
    db_name = instance.get_db()

    # Remove older database files
    if os.path.exists(db_name):
        os.remove(db_name)
        assert not os.path.exists(db_name)

    create_db(db_name)
    assert os.path.exists(db_name)

    # Remove any databases created for testing purposes
    if os.path.exists(db_name):
        os.remove(db_name)


def test_queries():

    path = os.path.dirname(os.path.abspath(__file__))
    yt_history = os.path.join(path, "../src/etl/data/yt_sample.html")
    nf_history = os.path.join(path, "../src/etl/data/nf_sample.csv")

    instance = ETL()
    db_name = instance.get_db()

    if not os.path.exists(db_name):
        create_db(db_name)

    for item in [["youtube", yt_history], ["netflix", nf_history]]:

        data = transform_data(item[0], item[1])

        if data.shape[0] > 5:
            data = data.sample(n=5)

        data["to_insert"] = data.apply(exec_select_query, axis=1)
        if data.shape[0] > 0:
            assert len(data[data.to_insert == '']) == 0

        data_list = data[data.to_insert].values.tolist()

        with ThreadPoolExecutor(max_workers=5) as executor:
            inserts = executor.map(exec_insert_query, data_list)
        assert len(data_list) == len(list(inserts))

    if os.path.exists(db_name):
        os.remove(db_name)

def test_load_data():

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
    pass

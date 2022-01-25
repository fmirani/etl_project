import os
from etl.extract import extract_data


def test_extract_data() -> None:

    path = os.path.dirname(os.path.abspath(__file__))
    yt_history = os.path.join(path, "../src/etl/data/yt_sample.html")
    nf_history = os.path.join(path, "../src/etl/data/nf_sample.csv")

    for item in [["youtube", yt_history],
                 ["netflix", nf_history]]:

        service, data_file = extract_data(item[0], item[1])
        assert len(service) > 0
        assert len(data_file) > 0

    for item in [["wrong_service", yt_history],
                 ["wrong_service", nf_history],
                 ["youtube", "wring_file.html"],
                 ["netflix", "wrong_file.csv"]]:

        service, data_file = extract_data(item[0], item[1])
        assert len(service) == 0
        assert len(data_file) == 0


if __name__ == "__main__":
    test_extract_data()

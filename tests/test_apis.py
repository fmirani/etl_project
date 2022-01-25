import os
from etl.youtube_api import get_missing_data
from etl.imdb_api import get_genre
from etl.transform import transform_data


def test_get_missing_data() -> None:

    path = os.path.dirname(os.path.abspath(__file__))

    yt_history = os.path.join(path, "../src/etl/data/yt_sample.html")
    data = transform_data("youtube", yt_history)
    if data.shape[0] > 5:
        data = data.sample(n=5)

    for link in data["Link"]:
        name, cat = get_missing_data(link)
        if isinstance(cat, int):
            assert cat == 0 and name == 0
        else:
            if len(cat) == 0 and len(name) == 0:
                assert name == ""
                assert cat == ""


def test_get_genre() -> None:

    path = os.path.dirname(os.path.abspath(__file__))
    nf_history = os.path.join(path, "../src/etl/data/nf_sample.csv")
    data = transform_data("netflix", nf_history)
    if data.shape[0] > 5:
        data = data.sample(n=5)

    for name in data["Name"]:
        genre = get_genre(name)
        assert isinstance(genre, str)


if __name__ == "__main__":
    test_get_missing_data()
    test_get_genre()

import os
from etl import transform_data
from etl import get_missing_data
from etl import get_genre
from etl.main import ETL


def test_get_missing_data() -> None:

    instance = ETL()

    path = os.path.dirname(os.path.abspath(__file__))

    yt_history = os.path.join(path, "../src/etl/data/yt_sample.html")
    data = transform_data("youtube", yt_history)
    if data.shape[0] > 5:
        data = data.sample(n=5)

    # Testing nominal conditions
    for link in data["Link"]:
        name, cat = get_missing_data(link)
        assert name != "Invalid"
        assert cat != "Invalid"
        assert len(name) > 0
        assert len(cat) > 0

    # Testing invalid API
    api_key: str = instance.get_api()
    instance.set_api("THIS_IS_WRONG_API_KEY")
    for link in data["Link"]:
        name, cat = get_missing_data(link)
        assert name == "Invalid API"
        assert cat == "Invalid API"
    instance.set_api(api_key)

    # Testing invalid input data (links)
    data["Link"] = "THIS_IS_WRONG_YOUTUBE_LINK_AND_SOME_MOR"
    for link in data["Link"]:
        name, cat = get_missing_data(link)
        assert name == "Invalid link"
        assert cat == "Invalid link"

    # Testing video not found
    data["Link"] = "https://www.youtube.com/watch?v=$$$$$$$$"
    for link in data["Link"]:
        name, cat = get_missing_data(link)
        assert name == ""
        assert cat == ""


def test_get_genre() -> None:

    path = os.path.dirname(os.path.abspath(__file__))
    nf_history = os.path.join(path, "../src/etl/data/nf_sample.csv")
    data = transform_data("netflix", nf_history)
    if data.shape[0] > 5:
        data = data.sample(n=5)

    # Testing nominal conditions
    for name in data["Name"]:
        genre = get_genre(name)
        assert len(genre) > 0

    # Testing invalid input data
    data["Name"] = "TH|$_I$_WR0N6_NAM€"
    for name in data["Name"]:
        genre = get_genre(name)
        assert genre == ""


if __name__ == "__main__":
    test_get_missing_data()
    test_get_genre()

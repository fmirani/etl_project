import os
import pandas as pd
from etl.transform import transform_data


def test_transform_data() -> None:

    path = os.path.dirname(os.path.abspath(__file__))

    yt_history = os.path.join(path, "../src/etl/data/yt_sample.html")
    nf_history = os.path.join(path, "../src/etl/data/nf_sample.csv")

    for item in [["youtube", yt_history], ["netflix", nf_history]]:
        data: pd.DataFrame = transform_data(item[0], item[1])
        assert isinstance(data, pd.DataFrame)
        assert data.shape[1] == 8


if __name__ == "__main__":
    test_transform_data()

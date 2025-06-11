from oxen.streaming_dataset import StreamingDataset
from oxen.providers.mock_provider import MockPathProvider


def test_stream_mock_data():
    # Test that we can iterate over a dataset of mock data
    paths = ["data_0.csv", "data_1.csv", "data_2.csv"]
    num_rows = 33
    mock = MockPathProvider(
        paths=paths,
        columns=["path", "x", "y"],
        num_rows=num_rows,
    )

    dataset = StreamingDataset(
        mock,
        features=["x", "y"],
        buffer_size=5,  # make them not fit evenly into the buffer
        sleep_interval=0.05,  # make the test run faster
    )

    # Make sure the length is correct for iteration
    assert len(dataset) == num_rows * len(paths)

    # Make sure the size is correct as an abstraction
    width, height = dataset.size
    assert width == 2
    assert height == num_rows * len(paths)

    for i, item in enumerate(dataset):
        assert "x" in item
        assert "y" in item
        assert "path" not in item  # we filtered it out with features

        assert item["x"] == f"x_{i}"
        assert item["y"] == f"y_{i}"


def test_stream_mock_data_no_features():
    # Test that we can iterate over a dataset of mock data
    paths = ["data_0.csv", "data_1.csv", "data_2.csv"]
    num_rows = 33
    mock = MockPathProvider(
        paths=paths,
        columns=["path", "x", "y"],
        num_rows=num_rows,
    )

    dataset = StreamingDataset(
        mock,
        buffer_size=5,  # make them not fit evenly into the buffer
        sleep_interval=0.05,  # make the test run faster
    )

    # Make sure the length is correct for iteration
    assert len(dataset) == num_rows * len(paths)

    # Make sure the size is correct as an abstraction
    width, height = dataset.size
    assert width == 3
    assert height == num_rows * len(paths)

    for i, item in enumerate(dataset):
        assert "x" in item
        assert "y" in item
        assert "path" in item

        assert item["x"] == f"x_{i}"
        assert item["y"] == f"y_{i}"
        assert item["path"] == f"path_{i}"

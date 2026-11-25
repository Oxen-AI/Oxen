from datetime import date, datetime, time, timedelta
from decimal import Decimal

import polars as pl
import pyarrow as pa
import pytest
from polars.testing import assert_frame_equal

from oxen import df_utils


def test_df_utils_save_load_round_trips_every_dtype(tmp_path):
    columns = {
        "int8": pl.Series([1, None], dtype=pl.Int8),
        "int16": pl.Series([1, None], dtype=pl.Int16),
        "int32": pl.Series([1, None], dtype=pl.Int32),
        "int64": pl.Series([1, None], dtype=pl.Int64),
        "int128": pl.Series([-(2**100), None], dtype=pl.Int128),
        "uint8": pl.Series([1, None], dtype=pl.UInt8),
        "uint16": pl.Series([1, None], dtype=pl.UInt16),
        "uint32": pl.Series([1, None], dtype=pl.UInt32),
        "uint64": pl.Series([1, None], dtype=pl.UInt64),
        "uint128": pl.Series([2**100, None], dtype=pl.UInt128),
        "float16": pl.Series([1.5, None], dtype=pl.Float16),
        "float32": pl.Series([1.5, None], dtype=pl.Float32),
        "float64": pl.Series([1.5, None], dtype=pl.Float64),
        "decimal": pl.Series([Decimal("1.25"), None], dtype=pl.Decimal(10, 2)),
        "string": pl.Series(["a", None], dtype=pl.String),
        "boolean": pl.Series([True, None], dtype=pl.Boolean),
        "categorical": pl.Series(["a", None], dtype=pl.Categorical),
        "enum": pl.Series(["b", None], dtype=pl.Enum(["a", "b"])),
        "date": pl.Series([date(2026, 11, 19), None], dtype=pl.Date),
        "time": pl.Series([time(12, 30), None], dtype=pl.Time),
        "duration": pl.Series([timedelta(seconds=5), None], dtype=pl.Duration("us")),
        "datetime_tz": pl.Series(
            [datetime(2026, 11, 19, 12, 30), None],
            dtype=pl.Datetime("us", "America/Denver"),
        ),
        "binary": pl.Series([b"\x00\x01", None], dtype=pl.Binary),
        "null": pl.Series([None, None], dtype=pl.Null),
        "list": pl.Series([[1, 2], None], dtype=pl.List(pl.Int64)),
        "array": pl.Series([[1, 2], None], dtype=pl.Array(pl.Int64, 2)),
        "struct": pl.Series(
            [{"x": 1, "y": "a"}, None],
            dtype=pl.Struct({"x": pl.Int64, "y": pl.String}),
        ),
    }
    df = pl.DataFrame(columns)

    path = tmp_path / "every_dtype.arrow"
    df_utils.save(df, path)
    loaded = df_utils.load(path)

    assert_frame_equal(loaded, df)

    # polars exports a frame as one Arrow batch, so a pyarrow stream supplies two.
    first = pa.RecordBatch.from_pydict({"a": [1, 2], "b": ["x", "y"]})
    second = pa.RecordBatch.from_pydict({"a": [3], "b": ["z"]})

    class TwoBatchFrame(pl.DataFrame):
        def __arrow_c_stream__(self, requested_schema=None):
            reader = pa.RecordBatchReader.from_batches(first.schema, [first, second])
            return reader.__arrow_c_stream__(requested_schema)

    df_utils.save(TwoBatchFrame(), path)
    assert_frame_equal(
        df_utils.load(path),
        pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}),
    )

    with pytest.raises(TypeError, match="expected a polars.DataFrame, got dict"):
        df_utils.save(columns, path)

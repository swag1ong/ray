import os
import random
import requests
import shutil
import time

from unittest.mock import patch
import math
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray

from ray.tests.conftest import *  # noqa
from ray.experimental.data.datasource import DummyOutputDatasource
from ray.experimental.data.block import BlockAccessor
import ray.experimental.data.tests.util as util


def test_basic_actors(shutdown_only):
    ray.init(num_cpus=2)
    ds = ray.experimental.data.range(5)
    assert sorted(ds.map(lambda x: x + 1,
                         compute="actors").take()) == [1, 2, 3, 4, 5]


def test_callable_classes(shutdown_only):
    ray.init(num_cpus=1)
    ds = ray.experimental.data.range(10)

    class StatefulFn:
        def __init__(self):
            self.num_reuses = 0

        def __call__(self, x):
            r = self.num_reuses
            self.num_reuses += 1
            return r

    # map
    task_reuse = ds.map(StatefulFn, compute="tasks").take()
    assert sorted(task_reuse) == list(range(10)), task_reuse
    actor_reuse = ds.map(StatefulFn, compute="actors").take()
    assert sorted(actor_reuse) == list(range(10)), actor_reuse

    class StatefulFn:
        def __init__(self):
            self.num_reuses = 0

        def __call__(self, x):
            r = self.num_reuses
            self.num_reuses += 1
            return [r]

    # flat map
    task_reuse = ds.flat_map(StatefulFn, compute="tasks").take()
    assert sorted(task_reuse) == list(range(10)), task_reuse
    actor_reuse = ds.flat_map(StatefulFn, compute="actors").take()
    assert sorted(actor_reuse) == list(range(10)), actor_reuse

    # map batches
    task_reuse = ds.map_batches(StatefulFn, compute="tasks").take()
    assert sorted(task_reuse) == list(range(10)), task_reuse
    actor_reuse = ds.map_batches(StatefulFn, compute="actors").take()
    assert sorted(actor_reuse) == list(range(10)), actor_reuse

    class StatefulFn:
        def __init__(self):
            self.num_reuses = 0

        def __call__(self, x):
            r = self.num_reuses
            self.num_reuses += 1
            return r > 0

    # filter
    task_reuse = ds.filter(StatefulFn, compute="tasks").take()
    assert len(task_reuse) == 9, task_reuse
    actor_reuse = ds.filter(StatefulFn, compute="actors").take()
    assert len(actor_reuse) == 9, actor_reuse


def test_basic(ray_start_regular_shared):
    ds = ray.experimental.data.range(5)
    assert sorted(ds.map(lambda x: x + 1).take()) == [1, 2, 3, 4, 5]
    assert ds.count() == 5
    assert sorted(ds.iter_rows()) == [0, 1, 2, 3, 4]


def test_batch_tensors(ray_start_regular_shared):
    import torch
    ds = ray.experimental.data.from_items(
        [torch.tensor([0, 0]) for _ in range(40)])
    res = "Dataset(num_rows=40, num_blocks=40, schema=<class 'torch.Tensor'>)"
    assert str(ds) == res, str(ds)
    with pytest.raises(pa.lib.ArrowInvalid):
        next(ds.iter_batches(batch_format="pyarrow"))
    df = next(ds.iter_batches(batch_format="pandas"))
    assert df.to_dict().keys() == {0, 1}


def test_write_datasource(ray_start_regular_shared):
    output = DummyOutputDatasource()
    ds = ray.experimental.data.range(10, parallelism=2)
    ds.write_datasource(output)
    assert output.num_ok == 1
    assert output.num_failed == 0
    assert ray.get(output.data_sink.get_rows_written.remote()) == 10

    ray.get(output.data_sink.set_enabled.remote(False))
    with pytest.raises(ValueError):
        ds.write_datasource(output)
    assert output.num_ok == 1
    assert output.num_failed == 1
    assert ray.get(output.data_sink.get_rows_written.remote()) == 10


def test_empty_dataset(ray_start_regular_shared):
    ds = ray.experimental.data.range(0)
    assert ds.count() == 0
    assert ds.size_bytes() is None
    assert ds.schema() is None

    ds = ray.experimental.data.range(1)
    ds = ds.filter(lambda x: x > 1)
    assert str(ds) == \
        "Dataset(num_rows=0, num_blocks=1, schema=Unknown schema)"


def test_schema(ray_start_regular_shared):
    ds = ray.experimental.data.range(10)
    ds2 = ray.experimental.data.range_arrow(10)
    ds3 = ds2.repartition(5)
    ds4 = ds3.map(lambda x: {"a": "hi", "b": 1.0}).limit(5).repartition(1)
    assert str(ds) == \
        "Dataset(num_rows=10, num_blocks=10, schema=<class 'int'>)"
    assert str(ds2) == \
        "Dataset(num_rows=10, num_blocks=10, schema={value: int64})"
    assert str(ds3) == \
        "Dataset(num_rows=10, num_blocks=5, schema={value: int64})"
    assert str(ds4) == \
        "Dataset(num_rows=5, num_blocks=1, schema={a: string, b: double})"


def test_lazy_loading_exponential_rampup(ray_start_regular_shared):
    ds = ray.experimental.data.range(100, parallelism=20)
    assert len(ds._blocks._blocks) == 1
    assert ds.take(10) == list(range(10))
    assert len(ds._blocks._blocks) == 2
    assert ds.take(20) == list(range(20))
    assert len(ds._blocks._blocks) == 4
    assert ds.take(30) == list(range(30))
    assert len(ds._blocks._blocks) == 8
    assert ds.take(50) == list(range(50))
    assert len(ds._blocks._blocks) == 16
    assert ds.take(100) == list(range(100))
    assert len(ds._blocks._blocks) == 20


def test_limit(ray_start_regular_shared):
    ds = ray.experimental.data.range(100, parallelism=20)
    for i in range(100):
        assert ds.limit(i).take(200) == list(range(i))


def test_convert_types(ray_start_regular_shared):
    plain_ds = ray.experimental.data.range(1)
    arrow_ds = plain_ds.map(lambda x: {"a": x})
    assert arrow_ds.take() == [{"a": 0}]
    assert "ArrowRow" in arrow_ds.map(lambda x: str(x)).take()[0]

    arrow_ds = ray.experimental.data.range_arrow(1)
    assert arrow_ds.map(lambda x: "plain_{}".format(x["value"])).take() \
        == ["plain_0"]
    assert arrow_ds.map(lambda x: {"a": (x["value"],)}).take() == \
        [{"a": (0,)}]


def test_from_items(ray_start_regular_shared):
    ds = ray.experimental.data.from_items(["hello", "world"])
    assert ds.take() == ["hello", "world"]


def test_repartition(ray_start_regular_shared):
    ds = ray.experimental.data.range(20, parallelism=10)
    assert ds.num_blocks() == 10
    assert ds.sum() == 190
    assert ds._block_sizes() == [2] * 10

    ds2 = ds.repartition(5)
    assert ds2.num_blocks() == 5
    assert ds2.sum() == 190
    # TODO: would be nice to re-distribute these more evenly
    ds2._block_sizes() == [10, 10, 0, 0, 0]

    ds3 = ds2.repartition(20)
    assert ds3.num_blocks() == 20
    assert ds3.sum() == 190
    ds2._block_sizes() == [2] * 10 + [0] * 10

    large = ray.experimental.data.range(10000, parallelism=10)
    large = large.repartition(20)
    assert large._block_sizes() == [500] * 20


def test_repartition_arrow(ray_start_regular_shared):
    ds = ray.experimental.data.range_arrow(20, parallelism=10)
    assert ds.num_blocks() == 10
    assert ds.count() == 20
    assert ds._block_sizes() == [2] * 10

    ds2 = ds.repartition(5)
    assert ds2.num_blocks() == 5
    assert ds2.count() == 20
    ds2._block_sizes() == [10, 10, 0, 0, 0]

    ds3 = ds2.repartition(20)
    assert ds3.num_blocks() == 20
    assert ds3.count() == 20
    ds2._block_sizes() == [2] * 10 + [0] * 10

    large = ray.experimental.data.range_arrow(10000, parallelism=10)
    large = large.repartition(20)
    assert large._block_sizes() == [500] * 20


def test_from_pandas(ray_start_regular_shared):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.experimental.data.from_pandas([ray.put(df1), ray.put(df2)])
    values = [(r["one"], r["two"]) for r in ds.take(6)]
    rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
    assert values == rows


def test_from_arrow(ray_start_regular_shared):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.experimental.data.from_arrow([
        ray.put(pa.Table.from_pandas(df1)),
        ray.put(pa.Table.from_pandas(df2))
    ])
    values = [(r["one"], r["two"]) for r in ds.take(6)]
    rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
    assert values == rows


def test_to_pandas(ray_start_regular_shared):
    n = 5
    df = pd.DataFrame({"value": list(range(n))})
    ds = ray.experimental.data.range_arrow(n)
    dfds = pd.concat(ray.get(ds.to_pandas()), ignore_index=True)
    assert df.equals(dfds)


def test_to_arrow(ray_start_regular_shared):
    n = 5

    # Zero-copy.
    df = pd.DataFrame({"value": list(range(n))})
    ds = ray.experimental.data.range_arrow(n)
    dfds = pd.concat(
        [t.to_pandas() for t in ray.get(ds.to_arrow())], ignore_index=True)
    assert df.equals(dfds)

    # Conversion.
    df = pd.DataFrame({0: list(range(n))})
    ds = ray.experimental.data.range(n)
    dfds = pd.concat(
        [t.to_pandas() for t in ray.get(ds.to_arrow())], ignore_index=True)
    assert df.equals(dfds)


def test_get_blocks(ray_start_regular_shared):
    blocks = ray.experimental.data.range(10).get_blocks()
    assert len(blocks) == 10
    out = []
    for b in ray.get(blocks):
        out.extend(list(BlockAccessor.for_block(b).iter_rows()))
    out = sorted(out)
    assert out == list(range(10)), out


def test_pandas_roundtrip(ray_start_regular_shared):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.experimental.data.from_pandas([ray.put(df1), ray.put(df2)])
    dfds = pd.concat(ray.get(ds.to_pandas()))
    assert pd.concat([df1, df2]).equals(dfds)


def test_parquet_read(ray_start_regular_shared, tmp_path):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    table = pa.Table.from_pandas(df1)
    pq.write_table(table, os.path.join(str(tmp_path), "test1.parquet"))
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    table = pa.Table.from_pandas(df2)
    pq.write_table(table, os.path.join(str(tmp_path), "test2.parquet"))

    ds = ray.experimental.data.read_parquet(str(tmp_path))

    # Test metadata-only parquet ops.
    assert len(ds._blocks._blocks) == 1
    assert ds.count() == 6
    assert ds.size_bytes() > 0
    assert ds.schema() is not None
    input_files = ds.input_files()
    assert len(input_files) == 2, input_files
    assert "test1.parquet" in str(input_files)
    assert "test2.parquet" in str(input_files)
    assert str(ds) == \
        "Dataset(num_rows=6, num_blocks=2, " \
        "schema={one: int64, two: string})", ds
    assert repr(ds) == \
        "Dataset(num_rows=6, num_blocks=2, " \
        "schema={one: int64, two: string})", ds
    assert len(ds._blocks._blocks) == 1

    # Forces a data read.
    values = [[s["one"], s["two"]] for s in ds.take()]
    assert len(ds._blocks._blocks) == 2
    assert sorted(values) == [[1, "a"], [2, "b"], [3, "c"], [4, "e"], [5, "f"],
                              [6, "g"]]

    # Test column selection.
    ds = ray.experimental.data.read_parquet(str(tmp_path), columns=["one"])
    values = [s["one"] for s in ds.take()]
    assert sorted(values) == [1, 2, 3, 4, 5, 6]


def test_parquet_write(ray_start_regular_shared, tmp_path):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    df = pd.concat([df1, df2])
    ds = ray.experimental.data.from_pandas([ray.put(df1), ray.put(df2)])
    path = os.path.join(tmp_path, "test_parquet_dir")
    os.mkdir(path)
    ds.write_parquet(path)
    path1 = os.path.join(path, "data0.parquet")
    path2 = os.path.join(path, "data1.parquet")
    dfds = pd.concat([pd.read_parquet(path1), pd.read_parquet(path2)])
    assert df.equals(dfds)


def test_convert_to_pyarrow(ray_start_regular_shared, tmp_path):
    ds = ray.experimental.data.range(100)
    assert ds.to_dask().sum().compute()[0] == 4950
    path = os.path.join(tmp_path, "test_parquet_dir")
    os.mkdir(path)
    ds.write_parquet(path)
    assert ray.experimental.data.read_parquet(path).count() == 100


def test_pyarrow(ray_start_regular_shared):
    ds = ray.experimental.data.range_arrow(5)
    assert ds.map(lambda x: {"b": x["value"] + 2}).take() == \
        [{"b": 2}, {"b": 3}, {"b": 4}, {"b": 5}, {"b": 6}]
    assert ds.map(lambda x: {"b": x["value"] + 2}) \
        .filter(lambda x: x["b"] % 2 == 0).take() == \
        [{"b": 2}, {"b": 4}, {"b": 6}]
    assert ds.filter(lambda x: x["value"] == 0) \
        .flat_map(lambda x: [{"b": x["value"] + 2}, {"b": x["value"] + 20}]) \
        .take() == [{"b": 2}, {"b": 20}]


def test_read_binary_files(ray_start_regular_shared):
    with util.gen_bin_files(10) as (_, paths):
        ds = ray.experimental.data.read_binary_files(paths, parallelism=10)
        for i, item in enumerate(ds.iter_rows()):
            expected = open(paths[i], "rb").read()
            assert expected == item
        # Test metadata ops.
        assert ds.count() == 10
        assert "bytes" in str(ds.schema()), ds
        assert "bytes" in str(ds), ds


def test_read_binary_files_with_fs(ray_start_regular_shared):
    with util.gen_bin_files(10) as (tempdir, paths):
        # All the paths are absolute, so we want the root file system.
        fs, _ = pa.fs.FileSystem.from_uri("/")
        ds = ray.experimental.data.read_binary_files(
            paths, filesystem=fs, parallelism=10)
        for i, item in enumerate(ds.iter_rows()):
            expected = open(paths[i], "rb").read()
            assert expected == item


def test_read_binary_files_with_paths(ray_start_regular_shared):
    with util.gen_bin_files(10) as (_, paths):
        ds = ray.experimental.data.read_binary_files(
            paths, include_paths=True, parallelism=10)
        for i, (path, item) in enumerate(ds.iter_rows()):
            assert path == paths[i]
            expected = open(paths[i], "rb").read()
            assert expected == item


# TODO(Clark): Hitting S3 in CI is currently broken due to some AWS
# credentials issue, unskip this test once that's fixed or once ported to moto.
@pytest.mark.skip(reason="Shouldn't hit S3 in CI")
def test_read_binary_files_s3(ray_start_regular_shared):
    ds = ray.experimental.data.read_binary_files(
        ["s3://anyscale-data/small-files/0.dat"])
    item = ds.take(1).pop()
    expected = requests.get(
        "https://anyscale-data.s3.us-west-2.amazonaws.com/small-files/0.dat"
    ).content
    assert item == expected


def test_iter_batches_basic(ray_start_regular_shared):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": [5, 6, 7]})
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": [8, 9, 10]})
    df4 = pd.DataFrame({"one": [10, 11, 12], "two": [11, 12, 13]})
    dfs = [df1, df2, df3, df4]
    ds = ray.experimental.data.from_pandas(
        [ray.put(df1), ray.put(df2),
         ray.put(df3), ray.put(df4)])

    # Default.
    for batch, df in zip(ds.iter_batches(), dfs):
        assert isinstance(batch, pd.DataFrame)
        assert batch.equals(df)

    # pyarrow.Table format.
    for batch, df in zip(ds.iter_batches(batch_format="pyarrow"), dfs):
        assert isinstance(batch, pa.Table)
        assert batch.equals(pa.Table.from_pandas(df))

    # blocks format.
    for batch, df in zip(ds.iter_batches(batch_format="_blocks"), dfs):
        assert batch.to_pandas().equals(df)

    # Batch size.
    batch_size = 2
    batches = list(ds.iter_batches(batch_size=batch_size))
    assert all(len(batch) == batch_size for batch in batches)
    assert (len(batches) == math.ceil(
        (len(df1) + len(df2) + len(df3) + len(df4)) / batch_size))
    assert pd.concat(
        batches, ignore_index=True).equals(pd.concat(dfs, ignore_index=True))

    # Batch size larger than block.
    batch_size = 4
    batches = list(ds.iter_batches(batch_size=batch_size))
    assert all(len(batch) == batch_size for batch in batches)
    assert (len(batches) == math.ceil(
        (len(df1) + len(df2) + len(df3) + len(df4)) / batch_size))
    assert pd.concat(
        batches, ignore_index=True).equals(pd.concat(dfs, ignore_index=True))

    # Batch size drop partial.
    batch_size = 5
    batches = list(ds.iter_batches(batch_size=batch_size, drop_last=True))
    assert all(len(batch) == batch_size for batch in batches)
    assert (len(batches) == (len(df1) + len(df2) + len(df3) + len(df4)) //
            batch_size)
    assert pd.concat(
        batches, ignore_index=True).equals(
            pd.concat(dfs, ignore_index=True)[:10])

    # Batch size don't drop partial.
    batch_size = 5
    batches = list(ds.iter_batches(batch_size=batch_size, drop_last=False))
    assert all(len(batch) == batch_size for batch in batches[:-1])
    assert (len(batches[-1]) == (len(df1) + len(df2) + len(df3) + len(df4)) %
            batch_size)
    assert (len(batches) == math.ceil(
        (len(df1) + len(df2) + len(df3) + len(df4)) / batch_size))
    assert pd.concat(
        batches, ignore_index=True).equals(pd.concat(dfs, ignore_index=True))

    # Prefetch.
    for batch, df in zip(ds.iter_batches(prefetch_blocks=1), dfs):
        assert isinstance(batch, pd.DataFrame)
        assert batch.equals(df)


def test_iter_batches_grid(ray_start_regular_shared):
    # Tests slicing, batch combining, and partial batch dropping logic over
    # a grid of dataset, batching, and dropping configurations.
    # Grid: num_blocks x num_rows_block_1 x ... x num_rows_block_N x
    #       batch_size x drop_last
    seed = int(time.time())
    print(f"Seeding RNG for test_iter_batches_grid with: {seed}")
    random.seed(seed)
    max_num_blocks = 20
    max_num_rows_per_block = 20
    num_blocks_samples = 3
    block_sizes_samples = 3
    batch_size_samples = 3

    for num_blocks in np.random.randint(
            1, max_num_blocks + 1, size=num_blocks_samples):
        block_sizes_list = [
            np.random.randint(1, max_num_rows_per_block + 1, size=num_blocks)
            for _ in range(block_sizes_samples)
        ]
        for block_sizes in block_sizes_list:
            # Create the dataset with the given block sizes.
            dfs = []
            running_size = 0
            for block_size in block_sizes:
                dfs.append(
                    pd.DataFrame({
                        "value": list(
                            range(running_size, running_size + block_size))
                    }))
                running_size += block_size
            num_rows = running_size
            ds = ray.experimental.data.from_pandas([ray.put(df) for df in dfs])
            for batch_size in np.random.randint(
                    1, num_rows + 1, size=batch_size_samples):
                for drop_last in (False, True):
                    batches = list(
                        ds.iter_batches(
                            batch_size=batch_size, drop_last=drop_last))
                    if num_rows % batch_size == 0 or not drop_last:
                        # Number of batches should be equal to
                        # num_rows / batch_size,  rounded up.
                        assert len(batches) == math.ceil(num_rows / batch_size)
                        # Concatenated batches should equal the DataFrame
                        # representation of the entire dataset.
                        assert pd.concat(
                            batches, ignore_index=True).equals(
                                pd.concat(
                                    ray.get(ds.to_pandas()),
                                    ignore_index=True))
                    else:
                        # Number of batches should be equal to
                        # num_rows / batch_size, rounded down.
                        assert len(batches) == num_rows // batch_size
                        # Concatenated batches should equal the DataFrame
                        # representation of the dataset with the partial batch
                        # remainder sliced off.
                        assert pd.concat(
                            batches, ignore_index=True).equals(
                                pd.concat(
                                    ray.get(ds.to_pandas()), ignore_index=True)
                                [:batch_size * (num_rows // batch_size)])
                    if num_rows % batch_size == 0 or drop_last:
                        assert all(
                            len(batch) == batch_size for batch in batches)
                    else:
                        assert all(
                            len(batch) == batch_size for batch in batches[:-1])
                        assert len(batches[-1]) == num_rows % batch_size


def test_lazy_loading_iter_batches_exponential_rampup(
        ray_start_regular_shared):
    ds = ray.experimental.data.range(32, parallelism=8)
    expected_num_blocks = [1, 2, 4, 4, 8, 8, 8, 8]
    for _, expected in zip(ds.iter_batches(), expected_num_blocks):
        assert len(ds._blocks._blocks) == expected


def test_map_batch(ray_start_regular_shared, tmp_path):
    # Test input validation
    ds = ray.experimental.data.range(5)
    with pytest.raises(ValueError):
        ds.map_batches(
            lambda x: x + 1, batch_format="pyarrow", batch_size=-1).take()

    # Test pandas
    df = pd.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    table = pa.Table.from_pandas(df)
    pq.write_table(table, os.path.join(tmp_path, "test1.parquet"))
    ds = ray.experimental.data.read_parquet(str(tmp_path))
    ds_list = ds.map_batches(lambda df: df + 1, batch_size=1).take()
    values = [s["one"] for s in ds_list]
    assert values == [2, 3, 4]
    values = [s["two"] for s in ds_list]
    assert values == [3, 4, 5]

    # Test Pyarrow
    ds = ray.experimental.data.read_parquet(str(tmp_path))
    ds_list = ds.map_batches(
        lambda pa: pa, batch_size=1, batch_format="pyarrow").take()
    values = [s["one"] for s in ds_list]
    assert values == [1, 2, 3]
    values = [s["two"] for s in ds_list]
    assert values == [2, 3, 4]

    # Test batch
    size = 300
    ds = ray.experimental.data.range(size)
    ds_list = ds.map_batches(lambda df: df + 1, batch_size=17).take(limit=size)
    for i in range(size):
        # The pandas column is "0", and it originally has rows from 0~299.
        # After the map batch, it should have 1~300.
        row = ds_list[i]
        assert row["0"] == i + 1
    assert ds.count() == 300

    # Test the lambda returns different types than the batch_format
    # pandas => list block
    ds = ray.experimental.data.read_parquet(str(tmp_path))
    ds_list = ds.map_batches(lambda df: [1], batch_size=1).take()
    assert ds_list == [1, 1, 1]
    assert ds.count() == 3

    # pyarrow => list block
    ds = ray.experimental.data.read_parquet(str(tmp_path))
    ds_list = ds.map_batches(
        lambda df: [1], batch_size=1, batch_format="pyarrow").take()
    assert ds_list == [1, 1, 1]
    assert ds.count() == 3

    # Test the wrong return value raises an exception.
    ds = ray.experimental.data.read_parquet(str(tmp_path))
    with pytest.raises(ValueError):
        ds_list = ds.map_batches(
            lambda df: 1, batch_size=2, batch_format="pyarrow").take()


def test_split(ray_start_regular_shared):
    ds = ray.experimental.data.range(20, parallelism=10)
    assert ds.num_blocks() == 10
    assert ds.sum() == 190
    assert ds._block_sizes() == [2] * 10

    datasets = ds.split(5)
    assert [2] * 5 == [len(dataset._blocks) for dataset in datasets]
    assert 190 == sum([dataset.sum() for dataset in datasets])

    datasets = ds.split(3)
    assert [4, 3, 3] == [len(dataset._blocks) for dataset in datasets]
    assert 190 == sum([dataset.sum() for dataset in datasets])

    datasets = ds.split(1)
    assert [10] == [len(dataset._blocks) for dataset in datasets]
    assert 190 == sum([dataset.sum() for dataset in datasets])

    datasets = ds.split(10)
    assert [1] * 10 == [len(dataset._blocks) for dataset in datasets]
    assert 190 == sum([dataset.sum() for dataset in datasets])

    datasets = ds.split(11)
    assert [1] * 10 + [0] == [len(dataset._blocks) for dataset in datasets]
    assert 190 == sum([dataset.sum() for dataset in datasets])


def test_split_hints(ray_start_regular_shared):
    @ray.remote
    class Actor(object):
        def __init__(self):
            pass

    def assert_split_assignment(block_node_ids, actor_node_ids,
                                expected_split_result):
        """Helper function to setup split hints test.

        Args:
            block_node_ids: a list of blocks with their locations. For
                example ["node1", "node2"] represents two blocks with
                "node1", "node2" as their location respectively.
            actor_node_ids: a list of actors with their locations. For
                example ["node1", "node2"] represents two actors with
                "node1", "node2" as their location respectively.
            expected_split_result: a list of allocation result, each entry
                in the list stores the block_index in the split dataset.
                For example, [[0, 1], [2]] represents the split result has
                two datasets, datasets[0] contains block 0 and 1; and
                datasets[1] contains block 2.
        """
        num_blocks = len(block_node_ids)
        ds = ray.experimental.data.range(num_blocks, parallelism=num_blocks)
        blocks = list(ds._blocks)
        assert len(block_node_ids) == len(blocks)
        actors = [Actor.remote() for i in range(len(actor_node_ids))]
        with patch("ray.experimental.get_object_locations") as location_mock:
            with patch("ray.state.actors") as state_mock:
                block_locations = {}
                for i, node_id in enumerate(block_node_ids):
                    if node_id:
                        block_locations[blocks[i]] = {"node_ids": [node_id]}
                location_mock.return_value = block_locations

                actor_state = {}
                for i, node_id in enumerate(actor_node_ids):
                    actor_state[actors[i]._actor_id.hex()] = {
                        "Address": {
                            "NodeID": node_id
                        }
                    }

                state_mock.return_value = actor_state

                datasets = ds.split(len(actors), actors)
                assert len(datasets) == len(actors)
                for i in range(len(actors)):
                    assert {blocks[j]
                            for j in expected_split_result[i]} == set(
                                datasets[i]._blocks)

    assert_split_assignment(["node2", "node1", "node1"], ["node1", "node2"],
                            [[1, 2], [0]])
    assert_split_assignment(["node1", "node1", "node1"], ["node1", "node2"],
                            [[2, 1], [0]])
    assert_split_assignment(["node2", "node2", None], ["node1", "node2"],
                            [[0, 2], [1]])
    assert_split_assignment(["node2", "node2", None], [None, None],
                            [[2, 1], [0]])
    assert_split_assignment(["n1", "n2", "n3", "n1", "n2"], ["n1", "n2"],
                            [[0, 2, 3], [1, 4]])

    assert_split_assignment(["n1", "n2"], ["n1", "n2", "n3"], [[0], [1], []])

    # perfect split:
    #
    # split 300 blocks
    #   with node_ids interleaving between "n0", "n1", "n2"
    #
    # to 3 actors
    #   with has node_id "n1", "n2", "n0"
    #
    # expect that block 1, 4, 7... are assigned to actor with node_id n1
    #             block 2, 5, 8... are assigned to actor with node_id n2
    #             block 0, 3, 6... are assigned to actor with node_id n0
    assert_split_assignment(
        ["n0", "n1", "n2"] * 100, ["n1", "n2", "n0"],
        [range(1, 300, 3),
         range(2, 300, 3),
         range(0, 300, 3)])

    # even split regardless of locality:
    #
    # split 301 blocks
    #   with block 0 to block 50 on "n0",
    #        block 51 to block 300 on "n1"
    #
    # to 3 actors
    #   with node_ids "n1", "n2", "n0"
    #
    # expect that block 200 to block 300 are assigned to actor with node_id n1
    #             block 100 to block 199 are assigned to actor with node_id n2
    #             block 0 to block 99 are assigned to actor with node_id n0
    assert_split_assignment(["n0"] * 50 + ["n1"] * 251, ["n1", "n2", "n0"], [
        range(200, 301),
        range(100, 200),
        list(range(0, 50)) + list(range(50, 100))
    ])


def test_from_dask(ray_start_regular_shared):
    import dask.dataframe as dd
    df = pd.DataFrame({"one": list(range(100)), "two": list(range(100))})
    ddf = dd.from_pandas(df, npartitions=10)
    ds = ray.experimental.data.from_dask(ddf)
    dfds = pd.concat(ray.get(ds.to_pandas()))
    assert df.equals(dfds)


def test_to_dask(ray_start_regular_shared):
    from ray.util.dask import ray_dask_get
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    df = pd.concat([df1, df2])
    ds = ray.experimental.data.from_pandas([ray.put(df1), ray.put(df2)])
    ddf = ds.to_dask()
    # Explicit Dask-on-Ray
    assert df.equals(ddf.compute(scheduler=ray_dask_get))
    # Implicit Dask-on-Ray.
    assert df.equals(ddf.compute())


def test_to_tf(ray_start_regular_shared):
    import tensorflow as tf
    df1 = pd.DataFrame({
        "one": [1, 2, 3],
        "two": [1.0, 2.0, 3.0],
        "label": [1.0, 2.0, 3.0]
    })
    df2 = pd.DataFrame({
        "one": [4, 5, 6],
        "two": [4.0, 5.0, 6.0],
        "label": [4.0, 5.0, 6.0]
    })
    df3 = pd.DataFrame({"one": [7, 8], "two": [7.0, 8.0], "label": [7.0, 8.0]})
    df = pd.concat([df1, df2, df3])
    ds = ray.experimental.data.from_pandas(
        [ray.put(df1), ray.put(df2), ray.put(df3)])
    tfd = ds.to_tf(
        "label",
        output_signature=(tf.TensorSpec(shape=(None, 2), dtype=tf.float32),
                          tf.TensorSpec(shape=(None), dtype=tf.float32)))
    iterations = []
    for batch in tfd.as_numpy_iterator():
        iterations.append(
            np.concatenate((batch[0], batch[1].reshape(-1, 1)), axis=1))
    combined_iterations = np.concatenate(iterations)
    assert np.array_equal(df.values, combined_iterations)


def test_to_tf_feature_columns(ray_start_regular_shared):
    import tensorflow as tf
    df1 = pd.DataFrame({
        "one": [1, 2, 3],
        "two": [1.0, 2.0, 3.0],
        "label": [1.0, 2.0, 3.0]
    })
    df2 = pd.DataFrame({
        "one": [4, 5, 6],
        "two": [4.0, 5.0, 6.0],
        "label": [4.0, 5.0, 6.0]
    })
    df3 = pd.DataFrame({"one": [7, 8], "two": [7.0, 8.0], "label": [7.0, 8.0]})
    df = pd.concat([df1, df2, df3]).drop("two", axis=1)
    ds = ray.experimental.data.from_pandas(
        [ray.put(df1), ray.put(df2), ray.put(df3)])
    tfd = ds.to_tf(
        "label",
        feature_columns=["one"],
        output_signature=(tf.TensorSpec(shape=(None, 1), dtype=tf.float32),
                          tf.TensorSpec(shape=(None), dtype=tf.float32)))
    iterations = []
    for batch in tfd.as_numpy_iterator():
        iterations.append(
            np.concatenate((batch[0], batch[1].reshape(-1, 1)), axis=1))
    combined_iterations = np.concatenate(iterations)
    assert np.array_equal(df.values, combined_iterations)


def test_to_torch(ray_start_regular_shared):
    import torch
    df1 = pd.DataFrame({
        "one": [1, 2, 3],
        "two": [1.0, 2.0, 3.0],
        "label": [1.0, 2.0, 3.0]
    })
    df2 = pd.DataFrame({
        "one": [4, 5, 6],
        "two": [4.0, 5.0, 6.0],
        "label": [4.0, 5.0, 6.0]
    })
    df3 = pd.DataFrame({"one": [7, 8], "two": [7.0, 8.0], "label": [7.0, 8.0]})
    df = pd.concat([df1, df2, df3])
    ds = ray.experimental.data.from_pandas(
        [ray.put(df1), ray.put(df2), ray.put(df3)])
    torchd = ds.to_torch(label_column="label", batch_size=3)

    num_epochs = 2
    for _ in range(num_epochs):
        iterations = []
        for batch in iter(torchd):
            iterations.append(torch.cat((*batch[0], batch[1]), axis=1).numpy())
        combined_iterations = np.concatenate(iterations)
        assert np.array_equal(np.sort(df.values), np.sort(combined_iterations))


def test_to_torch_feature_columns(ray_start_regular_shared):
    import torch
    df1 = pd.DataFrame({
        "one": [1, 2, 3],
        "two": [1.0, 2.0, 3.0],
        "label": [1.0, 2.0, 3.0]
    })
    df2 = pd.DataFrame({
        "one": [4, 5, 6],
        "two": [4.0, 5.0, 6.0],
        "label": [4.0, 5.0, 6.0]
    })
    df3 = pd.DataFrame({"one": [7, 8], "two": [7.0, 8.0], "label": [7.0, 8.0]})
    df = pd.concat([df1, df2, df3]).drop("two", axis=1)
    ds = ray.experimental.data.from_pandas(
        [ray.put(df1), ray.put(df2), ray.put(df3)])
    torchd = ds.to_torch("label", feature_columns=["one"], batch_size=3)
    iterations = []

    for batch in iter(torchd):
        iterations.append(torch.cat((*batch[0], batch[1]), axis=1).numpy())
    combined_iterations = np.concatenate(iterations)
    assert np.array_equal(df.values, combined_iterations)


def test_json_read(ray_start_regular_shared, tmp_path):
    # Single file.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(tmp_path, "test1.json")
    df1.to_json(path1, orient="records", lines=True)
    ds = ray.experimental.data.read_json(path1)
    assert df1.equals(ray.get(ds.to_pandas())[0])
    # Test metadata ops.
    assert ds.count() == 3
    assert ds.input_files() == [path1]
    assert ds.schema() is None

    # Two files, parallelism=2.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(tmp_path, "test2.json")
    df2.to_json(path2, orient="records", lines=True)
    ds = ray.experimental.data.read_json([path1, path2], parallelism=2)
    dsdf = pd.concat(ray.get(ds.to_pandas()))
    assert pd.concat([df1, df2]).equals(dsdf)
    # Test metadata ops.
    for block, meta in zip(ds._blocks, ds._blocks.get_metadata()):
        BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes

    # Three files, parallelism=2.
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    path3 = os.path.join(tmp_path, "test3.json")
    df3.to_json(path3, orient="records", lines=True)
    df = pd.concat([df1, df2, df3], ignore_index=True)
    ds = ray.experimental.data.read_json([path1, path2, path3], parallelism=2)
    dsdf = pd.concat(ray.get(ds.to_pandas()), ignore_index=True)
    assert df.equals(dsdf)

    # Directory, two files.
    path = os.path.join(tmp_path, "test_json_dir")
    os.mkdir(path)
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(path, "data0.json")
    df1.to_json(path1, orient="records", lines=True)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(path, "data1.json")
    df2.to_json(path2, orient="records", lines=True)
    ds = ray.experimental.data.read_json(path)
    df = pd.concat([df1, df2])
    dsdf = pd.concat(ray.get(ds.to_pandas()))
    assert df.equals(dsdf)
    shutil.rmtree(path)

    # Two directories, three files.
    path1 = os.path.join(tmp_path, "test_json_dir1")
    path2 = os.path.join(tmp_path, "test_json_dir2")
    os.mkdir(path1)
    os.mkdir(path2)
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    file_path1 = os.path.join(path1, "data0.json")
    df1.to_json(file_path1, orient="records", lines=True)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    file_path2 = os.path.join(path2, "data1.json")
    df2.to_json(file_path2, orient="records", lines=True)
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    file_path3 = os.path.join(path2, "data2.json")
    df3.to_json(file_path3, orient="records", lines=True)
    ds = ray.experimental.data.read_json([path1, path2])
    df = pd.concat([df1, df2, df3])
    dsdf = pd.concat(ray.get(ds.to_pandas()))
    assert df.equals(dsdf)
    shutil.rmtree(path1)
    shutil.rmtree(path2)

    # Directory and file, two files.
    dir_path = os.path.join(tmp_path, "test_json_dir")
    os.mkdir(dir_path)
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(dir_path, "data0.json")
    df1.to_json(path1, orient="records", lines=True)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(tmp_path, "data1.json")
    df2.to_json(path2, orient="records", lines=True)
    ds = ray.experimental.data.read_json([dir_path, path2])
    df = pd.concat([df1, df2])
    dsdf = pd.concat(ray.get(ds.to_pandas()))
    assert df.equals(dsdf)
    shutil.rmtree(dir_path)


def test_json_write(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_json_dir")

    # Single block.
    os.mkdir(path)
    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.experimental.data.from_pandas([ray.put(df)])
    ds.write_json(path)
    file_path = os.path.join(path, "data0.json")
    assert df.equals(pd.read_json(file_path))
    shutil.rmtree(path)

    # Two blocks.
    os.mkdir(path)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.experimental.data.from_pandas([ray.put(df), ray.put(df2)])
    ds.write_json(path)
    file_path2 = os.path.join(path, "data1.json")
    assert pd.concat([df, df2]).equals(
        pd.concat([pd.read_json(file_path),
                   pd.read_json(file_path2)]))
    shutil.rmtree(path)


def test_csv_read(ray_start_regular_shared, tmp_path):
    # Single file.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(tmp_path, "test1.csv")
    df1.to_csv(path1, index=False)
    ds = ray.experimental.data.read_csv(path1)
    dsdf = ray.get(ds.to_pandas())[0]
    assert df1.equals(dsdf)
    # Test metadata ops.
    assert ds.count() == 3
    assert ds.input_files() == [path1]
    assert ds.schema() is None

    # Two files, parallelism=2.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(tmp_path, "test2.csv")
    df2.to_csv(path2, index=False)
    ds = ray.experimental.data.read_csv([path1, path2], parallelism=2)
    dsdf = pd.concat(ray.get(ds.to_pandas()))
    df = pd.concat([df1, df2])
    assert df.equals(dsdf)
    # Test metadata ops.
    for block, meta in zip(ds._blocks, ds._blocks.get_metadata()):
        BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes

    # Three files, parallelism=2.
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    path3 = os.path.join(tmp_path, "test3.csv")
    df3.to_csv(path3, index=False)
    ds = ray.experimental.data.read_csv([path1, path2, path3], parallelism=2)
    df = pd.concat([df1, df2, df3], ignore_index=True)
    dsdf = pd.concat(ray.get(ds.to_pandas()), ignore_index=True)
    assert df.equals(dsdf)

    # Directory, two files.
    path = os.path.join(tmp_path, "test_csv_dir")
    os.mkdir(path)
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(path, "data0.csv")
    df1.to_csv(path1, index=False)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(path, "data1.csv")
    df2.to_csv(path2, index=False)
    ds = ray.experimental.data.read_csv(path)
    df = pd.concat([df1, df2])
    dsdf = pd.concat(ray.get(ds.to_pandas()))
    assert df.equals(dsdf)
    shutil.rmtree(path)

    # Two directories, three files.
    path1 = os.path.join(tmp_path, "test_csv_dir1")
    path2 = os.path.join(tmp_path, "test_csv_dir2")
    os.mkdir(path1)
    os.mkdir(path2)
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    file_path1 = os.path.join(path1, "data0.csv")
    df1.to_csv(file_path1, index=False)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    file_path2 = os.path.join(path2, "data1.csv")
    df2.to_csv(file_path2, index=False)
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    file_path3 = os.path.join(path2, "data2.csv")
    df3.to_csv(file_path3, index=False)
    ds = ray.experimental.data.read_csv([path1, path2])
    df = pd.concat([df1, df2, df3])
    dsdf = pd.concat(ray.get(ds.to_pandas()))
    assert df.equals(dsdf)
    shutil.rmtree(path1)
    shutil.rmtree(path2)

    # Directory and file, two files.
    dir_path = os.path.join(tmp_path, "test_csv_dir")
    os.mkdir(dir_path)
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(dir_path, "data0.csv")
    df1.to_csv(path1, index=False)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(tmp_path, "data1.csv")
    df2.to_csv(path2, index=False)
    ds = ray.experimental.data.read_csv([dir_path, path2])
    df = pd.concat([df1, df2])
    dsdf = pd.concat(ray.get(ds.to_pandas()))
    assert df.equals(dsdf)
    shutil.rmtree(dir_path)


def test_csv_write(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_csv_dir")

    # Single block.
    os.mkdir(path)
    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.experimental.data.from_pandas([ray.put(df)])
    ds.write_csv(path)
    file_path = os.path.join(path, "data0.csv")
    assert df.equals(pd.read_csv(file_path))
    shutil.rmtree(path)

    # Two blocks.
    os.mkdir(path)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.experimental.data.from_pandas([ray.put(df), ray.put(df2)])
    ds.write_csv(path)
    file_path2 = os.path.join(path, "data1.csv")
    assert pd.concat([df, df2]).equals(
        pd.concat([pd.read_csv(file_path),
                   pd.read_csv(file_path2)]))
    shutil.rmtree(path)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))

import sys, os
import numpy as np
sys.path.append('/home/user/Documents/workspace/projects/samActivities/experience3')
from utils import get_dask_array_from_hdf5
from benchmark import bench_rewrite


def test_bench_rewrite_increment():
    file_path = "tests/data/sample.hdf5"
    key = "data"
    arr = get_dask_array_from_hdf5(file_path, key)
    b = arr[:5,0,:5]
    incr = b + 1
    c = b.compute()
    d = incr.compute()
    assert np.array_equal(c + 1, d)


def test_bench_rewrite_rechunk():
    in_file_path="tests/data/small_sample.hdf5"
    out_file_path="tests/chunked_slabs_300_100.hdf5"
    chunks_shape = (300, 100, 1)
    bench_rewrite(in_file_path=in_file_path,
                  out_file_path=out_file_path,
                  function="rechunk",
                  chunks_shape=chunks_shape)
    hdf5_dataset = get_dask_array_from_hdf5(out_file_path, cast=False)
    assert hdf5_dataset.chunks == (300, 100, 1)
    os.remove(out_file_path)

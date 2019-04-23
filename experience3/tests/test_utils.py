import dask.array as da
import h5py
import numpy as np
import sys, os
sys.path.append('/home/user/Documents/workspace/projects/samActivities/experience3')
from utils import get_dask_array_from_hdf5, load_array_parts


def test_load_array_parts():
    file_path = "tests/data/sample.hdf5"
    key = "data"
    arr = get_dask_array_from_hdf5(file_path, key)

    rect_name = "right_cuboid_out.npy"
    slab_name = "slabs_out.npy"
    cubi_name = "cubic_blocks_out.npy"
    test_paths = [os.path.join("tests/data/", filename) for filename in (rect_name, slab_name, cubi_name)]
    true_paths = [os.path.join("tests/truth_data/", "test_" + filename) for filename in (rect_name, slab_name, cubi_name)]

    # random implementations
    out = load_array_parts(arr=arr, geometry="right_cuboid", shape=(100,100,100))
    np.save(test_paths[0], out)
    out = load_array_parts(arr=arr, geometry="slabs", nb_elements=2000)
    np.save(test_paths[1], out)
    out = load_array_parts(arr=arr, geometry="cubic_blocks", nb_elements=2000)
    np.save(test_paths[2], out)

    for i in range(len(test_paths)):
        a = np.load(test_paths[i])
        b = np.load(true_paths[i])
        assert np.array_equal(a, b)
        os.remove(test_paths[i])

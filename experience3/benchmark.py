from utils import get_dask_array_from_hdf5, load_array_parts
import dask.array as da
import time, os, h5py, subprocess


def get_mem_usage():
    '''
    Returns swap, buffer usage and cache usage, in this order.
    '''
    a = subprocess.check_output(['vmstat', '-a', '1', '1']).decode("utf-8")
    print(a)
    a = subprocess.check_output(['vmstat', '1', '1']).decode("utf-8")
    print(a)
    b = a.split('\n')[2].split()
    return b[2], b[4], b[5], b[7]


def bench_load_array_parts(nb_elements=500000000):
    file_path = "tests/data/sample.hdf5"
    key = "data"
    arr = get_dask_array_from_hdf5(file_path, key)

    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

    get_mem_usage()
    t1 = time.time()
    a = load_array_parts(arr, geometry="cubic_blocks", nb_elements=nb_elements)
    t1 = time.time()-t1
    get_mem_usage()

    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

    get_mem_usage()
    t2 = time.time()
    a = load_array_parts(arr, geometry="slabs", nb_elements=nb_elements)
    t2 = time.time()-t2
    get_mem_usage()

    with open("outputs/bench_load_array_parts.txt", "w+") as f:
        f.write("time to read cubic blocks: " + str(t1) + "\n")
        f.write("time to read slabs: " + str(t2))

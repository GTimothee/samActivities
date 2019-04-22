from utils import get_dask_array_from_hdf5, load_array_parts, save_arr
import dask.array as da
import time, os, h5py, subprocess

"""
have been created:

bench_rewrite(in_file_path="tests/data/sample.hdf5",
                  out_file_path="/run/media/user/HDD 1TB/chunked_blocks_100_100_100.hdf5",
                  function="rechunk",
                  chunks_shape=(100, 100, 100))

bench_rewrite(in_file_path="tests/data/sample.hdf5",
                  out_file_path="/run/media/user/HDD 1TB/chunked_slabs_1000_200.hdf5",
                  function="rechunk",
                  chunks_shape=(1000, 1000, 1))
"""

def get_mem_usage(cache_usage_only=True):
    '''
    Returns swap, buffer usage and cache usage, in this order.
    '''
    if not cache_usage_only:  # print active and inactive memory
        a = subprocess.check_output(['vmstat', '-a', '1', '1']).decode("utf-8")
        print(a)
    a = subprocess.check_output(['vmstat', '1', '1']).decode("utf-8")
    print(a)
    b = a.split('\n')[2].split()
    return b[2], b[4], b[5], b[7]

# TODO: be sure we don't need to trigger .compute() at the end of load_array_parts
def bench_load_array_parts_contiguous(nb_elements=500000000, file_path = "tests/data/sample.hdf5"):
    key = "data"
    arr = get_dask_array_from_hdf5(file_path)

    # os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

    get_mem_usage()
    t1 = time.time()
    a = load_array_parts(arr, geometry="cubic_blocks", nb_elements=nb_elements)
    t1 = time.time() - t1
    get_mem_usage()

    # os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

    get_mem_usage()
    t2 = time.time()
    a = load_array_parts(arr, geometry="slabs", nb_elements=nb_elements)
    t2 = time.time() - t2
    get_mem_usage()

    with open("outputs/bench_load_array_parts.txt", "w+") as f:
        f.write("time to read cubic blocks: " + str(t1) + "\n")
        f.write("time to read slabs: " + str(t2))

    return


def bench_load_array_parts_chunked(slabs_file_path,
                                   blocks_file_path,
                                   key_slabs_file = "data",
                                   key_blocks_file = "data"):

    """ Load one block / slab using the same shape as the chunks shapes of the chunked datasets.
    Do this for both block and slab gemoetries to compare the processing times.
    """

    def get_shapes(slabs_file_path, blocks_file_path):
        """ Get the chunks shape of chunked hdf5 datasets
        """
        shapes = [(slabs_file_path.split('_')[2:]),
                  (blocks_file_path.split('_')[2:])]
        for shape in shapes:
            shape = tuple([int(e) for e in shape])
        return tuple(shapes)

    slab_shape, block_shape = get_shapes(slabs_file_path, blocks_file_path)
    shapes = [slab_shape, block_shape, block_shape, slab_shape]
    files = [slabs_file_path, blocks_file_path, slabs_file_path, blocks_file_path]

    times = list()
    for shape, file_path in zip(shapes, files):
        arr = get_dask_array_from_hdf5(slabs_file_path)
        # os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

        get_mem_usage()
        t = time.time()
        a = load_array_parts(arr, geometry="rectangle_blocks", shape=shape, random=False)
        t = time.time() - t
        times.append(t)
        get_mem_usage()

    if not len(times) == 4:
        print("An error occured, aborting writing output file.")
        return

    with open("outputs/bench_load_array_parts.txt", "w+") as f:
        f.write("time to read one slab from slab file: " + str(times[0]) + "\n")
        f.write("time to read one block from block file: " + str(times[1]))
        f.write("time to read one block from slab file: " + str(times[2]) + "\n")
        f.write("time to read one slab from block file: " + str(times[3]))
    return

def bench_rewrite(in_file_path="tests/data/sample.hdf5",
                  out_file_path="/run/media/user/HDD 1TB/bench_increment.hdf5",
                  function=None,
                  chunks_shape=None):
    """ Copy the whole array into a new file.
    function: function to apply before/while rewriting the array into another file.
    """
    arr = get_dask_array_from_hdf5(in_file_path)
    info = "processing time : "
    out_filename = "bench_rewrite"

    if function:
        if function == "increment":
            out_filename = out_filename + "_increment"
            info = "time to increment and write back : "
            arr += 1
        if function == "rechunk":
            if not chunks_shape:
                print("Argument chunks_shape missing. Aborting.")
                return
            out_filename = out_filename + "_rechunk"
            info = "time to rewrite with rechunk : "

    # os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

    get_mem_usage()
    t1 = time.time()
    if function == "rechunk":
        save_arr(arr, "hdf5", out_file_path, chunks_shape=chunks_shape)
    else:
        save_arr(arr, "hdf5", out_file_path)
    t1 = time.time() - t1
    get_mem_usage()

    with open("outputs/" + out_filename + ".txt", "w+") as f:
        f.write(info + str(t1) + "seconds \n")
    return

from utils import *
import dask.array as da
import time, os, h5py, subprocess, shutil
import csv
import datetime

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

# remainder to create the cube
create_random_cube(storage_type="hdf5",
                   file_path="tests/data/bbsamplesize.hdf5",
                   shape=(1540,1210,1400),
                   chunks_shape=None,
                   dtype="float16")
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


def bench_load_array_parts_random(nb_elements=500000000, file_path = "tests/data/sample.hdf5"):
    """ To be used on a contiguous file
    """
    key = "data"
    arr = get_dask_array_from_hdf5(file_path)

    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

    get_mem_usage()
    t1 = time.time()
    a = load_array_parts(arr, geometry="cubic_blocks", nb_elements=nb_elements, as_numpy=True)
    t1 = time.time() - t1
    get_mem_usage()

    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

    get_mem_usage()
    t2 = time.time()
    a = load_array_parts(arr, geometry="slabs", nb_elements=nb_elements, as_numpy=True)
    t2 = time.time() - t2
    get_mem_usage()

    with open("outputs/bench_load_array_parts.txt", "w+") as f:
        f.write("time to read cubic blocks: " + str(t1) + "\n")
        f.write("time to read slabs: " + str(t2))

    return


def bench_load_array_parts(file_path = "tests/data/sample.hdf5",
                                      key="/data",
                                      cuboid_shape=(200, 200, 100),
                                      slab_shape=(2000, 2000, 1),
                                      rechunk=True):
    """ To be used on a contiguous file, using shapes and rechunk instead of number of elements
    """

    arr = get_dask_array_from_hdf5(file_path, key=key)

    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

    get_mem_usage()
    if rechunk:
        arr = arr.rechunk(slab_shape)
    t1 = time.time()
    a = load_array_parts(arr, geometry="right_cuboid", shape=slab_shape, random=False, as_numpy=True)
    t1 = time.time() - t1
    get_mem_usage()

    print("time to read slabs: " + str(t1) + "\n")

    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

    get_mem_usage()
    if rechunk:
        arr = arr.rechunk(cuboid_shape)
    t2 = time.time()
    a = load_array_parts(arr, geometry="right_cuboid", shape=cuboid_shape, random=False, as_numpy=True)
    t2 = time.time() - t2
    get_mem_usage()

    print("time to read cuboid blocks: " + str(t2))

    with open("outputs/bench_load_array_parts.txt", "w+") as f:
        f.write("time to read slabs: " + str(t1) + "\n")
        f.write("time to read cubic blocks: " + str(t2))
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
        os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

        get_mem_usage()
        t = time.time()
        a = load_array_parts(arr, geometry="right_cuboid", shape=shape, random=False)
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

# TODO: rechunk failed for slabs 1000x200x1
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

    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

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


def run_split_and_merge(geometry_shape, input_file_path, work_dir, prefix, rechunk, unmatch_dims, flush_cache=False):
    """ flush_cache only control cache flush BETWEEN both algrithms
    """
    os.mkdir(work_dir + "tmp")
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')
    print("begin the splitting process")
    split_total_time, split_IO_time = naive_split(input_file_path=input_file_path, geometry_shape=geometry_shape, rechunk=rechunk, work_dir=work_dir + "tmp/", unmatch_dims=unmatch_dims)
    if flush_cache:
        os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')
    print("begin the merging process")
    merge_total_time, merge_IO_time = naive_merge(work_dir=work_dir + "tmp/", prefix=prefix, ask=False, rechunk=rechunk)
    shutil.rmtree(work_dir + "tmp")
    return (split_total_time, split_IO_time, merge_total_time, merge_IO_time)


def bench_split_and_merge(hardware_type, work_dir, input_file_path, block_chunked_file_path):
    """
    selecting slab shape:
    -------------------------------
    - nb total delements: 326 095 000
    - nb delements dans un slice de hauteur 1: 1540 x 1210 x 1 = 1 863 400
    - 326 095 000 / (1540 x 1210) = 175 slices
    - 175 slices / 8 splits = 21,... (slab width)
    - resulting shape of a slab: 1540 x 1210 x 21 (result will not be exact but still comparable -> unmatch_dims=True)

    logic_chunk_shape: the first two dimensions are used and the last is set to 'auto'
    """

    csv_file_name = datetime.datetime.now().strftime("%d-%m-%Y_%Ih%M%p.csv")
    block_shape = (770, 605, 700)
    slab_shape = (1540, 1210, 21)
    prefix = "split_part_"
    rechunk = False

    with open("outputs/" + csv_file_name, "a+") as csvFile:
        writer = csv.writer(csvFile)
        writer.writerow(["hardware_type", "geometry", "shape", "flush_cache",
                         "hardware_chunk_shape", "logic_chunk_shape",
                         "split_total_time", "split_IO_time",
                         "merge_total_time", "merge_IO_time"])

        print("block with flushing cache")
        stats = run_split_and_merge(block_shape, input_file_path, work_dir, prefix, rechunk, unmatch_dims=False, flush_cache=True)
        writer.writerow([hardware_type, "block", block_shape, True, None, None] + list(stats))

        print("block without flushing cache")
        stats = run_split_and_merge(block_shape, input_file_path, work_dir, prefix, rechunk, unmatch_dims=False, flush_cache=False)
        writer.writerow([hardware_type, "block", block_shape, False, None, None] + list(stats))

        print("block with logic rechunk without flushing cache")
        stats = run_split_and_merge(block_shape, input_file_path, work_dir, prefix, rechunk=True, unmatch_dims=False, flush_cache=False)
        writer.writerow([hardware_type, "block", block_shape, False, None, block_shape] + list(stats))

        print("slab without flushing cache")
        stats = run_split_and_merge(slab_shape, input_file_path, work_dir, prefix, rechunk, unmatch_dims=True, flush_cache=False)
        writer.writerow([hardware_type, "slab", slab_shape, False, None, None] + list(stats))

        print("slab with logic rechunk without flushing cache")
        stats = run_split_and_merge(slab_shape, input_file_path, work_dir, prefix, rechunk=True, unmatch_dims=True, flush_cache=False)
        writer.writerow([hardware_type, "slab", slab_shape, False, None, slab_shape] + list(stats))

        print("create a rechunked file for block")
        bench_rewrite(in_file_path=input_file_path,
                      out_file_path=block_chunked_file_path,
                      function="rechunk",
                      chunks_shape=block_shape)

        print("run block split_and_merge on rechunked file for block")
        stats = run_split_and_merge(block_shape, block_chunked_file_path, work_dir, prefix, rechunk, unmatch_dims=False, flush_cache=False)
        writer.writerow([hardware_type, "block", block_shape, False, block_shape, None] + list(stats))
    return


def on_hdd_on_ssd(ssd=True, hdd=False):
    if ssd:
        hardware_type = "SSD"
        work_dir = "tests/data/"
        input_file_path = work_dir + "bbsamplesize.hdf5"
        block_chunked_file_path = work_dir + "rechunked.hdf5"
        bench_split_and_merge(hardware_type, work_dir, input_file_path, block_chunked_file_path)

    if hdd:
        hardware_type = "HDD"
        work_dir = "path/to/hdd"
        input_file_path = work_dir + "bbsamplesize.hdf5"
        bench_split_and_merge(hardware_type, work_dir, input_file_path, block_chunked_file_path)
    return

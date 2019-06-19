
import optimize_io
from optimize_io.main import *

from utils import *

import sys


cases = {
    0: 'slabs_dask_interpol',
    1: 'slabs_previous_exp',
    2: 'blocks_dask_interpol',
    3: 'blocks_previous_exp'
}


def test_convert_slices_list_to_numeric_slices():
    proxy_array_name = 'array-6f870a321e8529128cb9bb82b8573db5'
    original_array_name = "array-original-645364531"
    array_to_original = {proxy_array_name: original_array_name}
    original_array_chunks = {original_array_name: (10, 20, 30)}
    original_array_blocks_shape = {original_array_name: (5, 3, 2)}
    slices_dict = {'array-6f870a321e8529128cb9bb82b8573db5': [(0,0,1), (0,1,0), (0,2,1), (0,1,1)]}
    slices_dict = convert_slices_list_to_numeric_slices(slices_dict, array_to_original, original_array_blocks_shape)

    expected = [1, 2, 5, 3]
    if sorted(expected) != slices_dict[proxy_array_name]:
        print("error in", sys._getframe().f_code.co_name)
        print(slices_dict)
        return 
    print("success")


def test_main():
    data_path = '/home/user/Documents/workspace/projects/samActivities/experience3/tests/data/bbsamplesize.hdf5'
    key = "data"
    
    for i in range(4):
        arr = get_dask_array_from_hdf5(data_path, key)
        dask_array = logical_chunks_tests(arr, cases[0], number_of_arrays=2)
        graph = dask_array.dask.dicts
        graph = main(graph)

    print("success")
    return


def test_in_custom_dask():

    def do_a_run(message, number_of_arrays, viz=False, prefix=None, suffix=None):
        print(message)

        results = list()
        for case_index in range(3,4):
            # load array
            data_path = '/home/user/Documents/workspace/projects/samActivities/experience3/tests/data/bbsamplesize.hdf5'
            key = "data"
            arr = get_dask_array_from_hdf5(data_path, key)
            dask_array = logical_chunks_tests(arr, cases[case_index], number_of_arrays=number_of_arrays)
            
            # free cache
            os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

            # process
            t = time.time()
            if not viz:
                r = dask_array.compute()
                results.append(r)
            else:
                if not suffix:
                    print("please give a filename")
                    sys.exit()
                filename = ''.join([prefix, cases[case_index], '-', suffix])
                dask_array.visualize(filename=filename, optimize_graph=True)
            t = time.time() - t
            
            print("time to process:", t)
        return results

    number_of_arrays = 2
    viz = True
    non_opti = False

    # custom dask
    import sys, os, time
    sys.path.insert(0,'/home/user/Documents/workspace/projects/dask') 
    import dask
    from dask.array.io_optimization import optimize_func
    import numpy as np

    from multiprocessing.pool import ThreadPool
    dask.config.set(pool=ThreadPool(1))

    # without opti
    if non_opti:
        if not viz:
            results = do_a_run("without optimization", number_of_arrays, viz=viz)
        else:
            do_a_run("without optimization", number_of_arrays, viz=viz, prefix='./output_imgs/', suffix='non-opti.png')

    # with opti
    dask.config.set({'optimizations':[optimize_func]})
    one_gig = 100000000
    dask.config.set({'io-optimizer':{'memory_available':one_gig}})
    if not viz:
        results_opti = do_a_run("with optimization", number_of_arrays, viz=viz)

        if non_opti:
            for i in range(len(results)):
                if not np.array_equal(results[i], results_opti[i]):
                    print("error in", sys._getframe().f_code.co_name)
                    print("case", cases[i])
                else:
                    print("success")
    else:
        do_a_run("with optimization", number_of_arrays, viz=viz, prefix='./output_imgs/', suffix='opti.png')
        print("success")
    
    
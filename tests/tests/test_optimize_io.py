
import optimize_io
from optimize_io.main import *

from tests_utils import *


def test_convert_slices_list_to_numeric_slices():
    proxy_array_name = 'array-6f870a321e8529128cb9bb82b8573db5'
    original_array_name = "array-original-645364531"
    array_to_original = {proxy_array_name: original_array_name}
    original_array_chunks = {original_array_name: (10, 20, 30)}
    original_array_blocks_shape = {original_array_name: (5, 3, 2)}
    slices_dict = {'array-6f870a321e8529128cb9bb82b8573db5': [(0,0,1), (0,1,0), (0,2,1), (0,1,1)]}
    slices_dict = convert_slices_list_to_numeric_slices(slices_dict, array_to_original, original_array_chunks)
    print(slices_dict)


cases = {
    0:'slabs_dask_interpol',
    1:'slabs_previous_exp',
    2:'blocks_dask_interpol',
    3:'blocks_previous_exp'
}


def test_main():
    data_path = '/home/user/Documents/workspace/projects/samActivities/experience3/tests/data/bbsamplesize.hdf5'
    key = "data"
    
    for i in range(4):
        arr = get_dask_array_from_hdf5(data_path, key)
        dask_array = logical_chunks_tests(arr, cases[0])
        graph = dask_array.dask.dicts
        graph = main(graph)

    print("success")
    return


def test_in_custom_dask():

    def do_a_run(message):
        print(message)

        results = list()
        for case_index in range(4):
            # load array
            data_path = '/home/user/Documents/workspace/projects/samActivities/experience3/tests/data/bbsamplesize.hdf5'
            key = "data"
            arr = get_dask_array_from_hdf5(data_path, key)
            dask_array = logical_chunks_tests(arr, cases[case_index])
            
            # free cache
            os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

            # process
            t = time.time()
            r = dask_array.compute()
            results.append(r)
            t = time.time() - t
            
            print("time to process:", t)
        return results

    # custom dask
    import sys, os, time
    sys.path.insert(0,'/home/user/Documents/workspace/projects/dask') 
    import dask
    from dask.array.io_optimization import optimize_func
    import numpy as np

    # without opti
    results = do_a_run("without optimization")

    # with opti
    dask.config.set({'optimizations':[optimize_func]})
    one_gig = 1000000000
    dask.config.set({'io-optimizer':{'memory_available':one_gig}})
    results_opti = do_a_run("with optimization")
    
    for i in range(len(results)):
        if not np.array_equal(results[i], results_opti[i]):
            print("error")
            print(results[i])
            print(results_opti[i])
            return
            
    print("success")
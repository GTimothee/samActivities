
import optimize_io
from optimize_io.main import *

from utils import *
import copy
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
    slices_dict = {'array-6f870a321e8529128cb9bb82b8573db5': [(0,0,0), (0,0,1), (0,1,0), (0,1,1), (0,2,0), (0,2,1), (1,0,0)]}
    slices_dict = convert_slices_list_to_numeric_slices(slices_dict, array_to_original, original_array_blocks_shape)
    
    expected = [0,1,2,3,4,5,6]
    if sorted(expected) != slices_dict[proxy_array_name]:
        print("error in", sys._getframe().f_code.co_name)
        print(slices_dict)
        return 
    print("success")



def get_slices_dicts_for_verifier(graph):

    def apply_slices_on_slices(slices, arr_slices):
        s1 = [[s.start, s.stop, s.step] for s in slices]
        s2 = [[s.start, s.stop, s.step] for s in arr_slices]
        combined_slices = [[0,0,0], [0,0,0], [0,0,0]]
        for i in range(3):
            for j in range(3):
                if s1[i][j] == None and s2[i][j] == None:
                    combined_slices[i][j] = None 
                else:
                    if s1[i][j] == None:
                        s1[i][j] = 0
                    if s2[i][j] == None:
                        s2[i][j] = 0
                    combined_slices[i][j] = s1[i][j] + s2[i][j]
        return slice(combined_slices)

    def get_slices_dict_getitem(graph, getitem_key, proxy_array_part_to_orig_array_slices, buffer_part_to_orig_array_slices):
        slices_dict_getitem = dict()
        graph_getitem = graph[getitem_key]
        for k, v in graph_getitem.items():
            key = v[1]
            slices = v[2]

            if 'array' in key[0]:
                arr, arr_slices = proxy_array_part_to_orig_array_slices[key]
                arr_slices = apply_slices_on_slices(slices, arr_slices) # apply first on second
                slices_dict_getitem[k] = (arr, arr_slices)
            elif 'merged-part' in key[0]:
                arr, arr_slices = buffer_part_to_orig_array_slices[key]
                arr_slices = apply_slices_on_slices(slices, arr_slices) # apply first on second
                slices_dict_getitem[k] = (arr, arr_slices)
            else:
                pass
        return slices_dict_getitem

    def get_slices_dict_rechunk(graph, rechunk_key, proxy_array_part_to_orig_array_slices, buffer_part_to_orig_array_slices):
        

        def recursive_search(_list, proxy_array_part_to_orig_array_slices, buffer_part_to_orig_array_slices):
            new_list = list()
            if not isinstance(_list[0], tuple): # if it is not a list of targets
                for i in range(len(_list)):
                    sublist = copy.deepcopy(_list[i])
                    sublist = recursive_search(sublist, proxy_array_part_to_orig_array_slices, buffer_part_to_orig_array_slices)
                    new_list.append(sublist)
            else:
                for i in range(len(_list)):
                    target_key = _list[i] 
                    if 'array' in target_key[0]:
                        arr, arr_slices = proxy_array_part_to_orig_array_slices[key]
                        new_list.append((arr, arr_slices))
                    elif 'merged-part' in target_key[0]:
                        arr, arr_slices = buffer_part_to_orig_array_slices[key]
                        new_list.append((arr, arr_slices))
                    else:
                        # remove the items we dont want to compare
                        pass
            return new_list

        slices_dict_rechunk = dict()
        graph_rechunk = graph[rechunk_key]
        for k, v in graph_rechunk.items():
            if 'rechunk-split' in k[0]: # same as getitem
                key = v[1]
                slices = v[2]
                if 'array' in key[0]:
                    arr, arr_slices = proxy_array_part_to_orig_array_slices[key]
                    arr_slices = apply_slices_on_slices(slices, arr_slices) # apply first on second
                    slices_dict_rechunk[k] = (arr, arr_slices)
                elif 'merged-part' in key[0]:
                    arr, arr_slices = buffer_part_to_orig_array_slices[key]
                    arr_slices = apply_slices_on_slices(slices, arr_slices) # apply first on second
                    slices_dict_rechunk[k] = (arr, arr_slices)
                else:
                    pass
            elif 'rechunk-merge' in k[0]:
                f, concat_list = v
                concat_list = recursive_search(concat_list, proxy_array_part_to_orig_array_slices, buffer_part_to_orig_array_slices)
                slices_dict_rechunk[k] = concat_list
                
        return slices_dict_rechunk

    # begin program
    proxy_array_part_to_orig_array_slices = dict()
    buffer_part_to_orig_array_slices = dict()

    # get information on proxies
    for k, v in graph.items():
        if 'array' in k:
            for k_, v_ in v.items():
                if not isinstance(k_, str):
                    proxy_array_part_to_orig_array_slices[k_] = (v_[1], v_[2])

        if 'merged-part' in k:
            for k_, v_ in v.items():
                buffer_part_to_orig_array_slices[k_] = (v_[1], v_[2])

    # get slices dicts
    slices_dict_getitem_global = dict()
    for k, v in graph.items():
        if 'getitem' in k: 
            slices_dict_getitem = get_slices_dict_getitem(graph, k, proxy_array_part_to_orig_array_slices, buffer_part_to_orig_array_slices)
            for k2,v2 in slices_dict_getitem.items():
                if k2 in list(slices_dict_getitem_global.keys()):
                    slices_dict_getitem_global[k2] = slices_dict_getitem_global[k2] + v2
                else:
                    slices_dict_getitem_global[k2] = v2
        if 'rechunk-merge' in k:
            pass #slices_dict_rechunk = get_slices_dict_rechunk(graph, k, proxy_array_part_to_orig_array_slices, buffer_part_to_orig_array_slices)
            
    
    return slices_dict_getitem_global #, slices_dict_rechunk


def graph_verifier():
    # get data
    data_path = '/home/user/Documents/workspace/projects/samActivities/experience3/tests/data/bbsamplesize.hdf5'
    key = "data"
    arr = get_dask_array_from_hdf5(data_path, key)
    dask_array = logical_chunks_tests(arr, cases[0], number_of_arrays=2)

    # get first graph
    graph1 = dask_array.dask.dicts
    
    # evaluate the diff
    slices_dict_getitem1 = copy.deepcopy(get_slices_dicts_for_verifier(graph1))

    # modify it to get second graph
    graph2 = main(graph1)
    slices_dict_getitem2 = get_slices_dicts_for_verifier(graph2)

    getitem2_keys = list(slices_dict_getitem2.keys())
    for k, v in slices_dict_getitem1.items():
        if not k in getitem2_keys:
            print("not", k, "in getitem2_keys")
        else:
            if v != slices_dict_getitem2[k]:
                print("\n", v, "became \n", slices_dict_getitem2[k])

    """rechunk2_keys = list(slices_dict_rechunk2.keys())
    for k, v in slices_dict_rechunk1.items():
        if not k in rechunk2_keys:
            print("not", k, "in getitem2_keys")
        else:
            if v != slices_dict_rechunk2[k]:
                print(v, "became ", slices_dict_rechunk2[k])"""

    print("end")
    return

def test_main():
    data_path = '/home/user/Documents/workspace/projects/samActivities/experience3/tests/data/bbsamplesize.hdf5'
    key = "data"
    
    for i in range(1):
        arr = get_dask_array_from_hdf5(data_path, key)
        dask_array = logical_chunks_tests(arr, cases[i], number_of_arrays=2)
        graph = dask_array.dask.dicts
        graph_printer(graph)
        graph = main(graph)
        graph_printer(graph)

    print("success")
    return


def test_sum():
    number_of_arrays = 1
    # custom dask
    import sys, os, time
    sys.path.insert(0,'/home/user/Documents/workspace/projects/dask') 
    import dask
    from dask.array.io_optimization import optimize_func
    import numpy as np

    # dataset infos
    data_path = '/home/user/Documents/workspace/projects/samActivities/experience3/tests/data/bbsamplesize.hdf5'
    key = "data"

    # non opti
    arr = get_dask_array_from_hdf5(data_path, key)
    dask_array = logical_chunks_tests(arr, cases[0], number_of_arrays=1)
    result_non_opti = dask_array.sum()
    print(result_non_opti.compute())

    # opti
    dask.config.set({'optimizations': [optimize_func]})
    arr = get_dask_array_from_hdf5(data_path, key)
    dask_array = logical_chunks_tests(arr, cases[0], number_of_arrays=1)
    result_opti = dask_array.sum()
    print(result_opti.compute())


def test_in_custom_dask():

    def do_a_run(message, number_of_arrays, viz=False, prefix=None, suffix=None):
        print(message)

        results = list()
        for case_index in range(1):
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
    viz = False
    non_opti = True

    # custom dask
    import sys, os, time
    sys.path.insert(0,'/home/user/Documents/workspace/projects/dask') 
    import dask
    from dask.array.io_optimization import optimize_func
    import numpy as np

    # without opti
    if non_opti:
        if not viz:
            results = do_a_run("without optimization", number_of_arrays, viz=viz)
        else:
            do_a_run("without optimization", number_of_arrays, viz=viz, prefix='./output_imgs/', suffix='non-opti.png')

    # with opti
    dask.config.set({'optimizations': [optimize_func]})
    one_gig = 100000000
    dask.config.set({'io-optimizer': {'memory_available':one_gig}})
    if not viz:
        results_opti = do_a_run("with optimization", number_of_arrays, viz=viz)

        if non_opti:
            for i in range(len(results)):
                if not np.array_equal(results[i], results_opti[i]):
                    print("error in", sys._getframe().f_code.co_name)
                    # print(results[i], "\n\n", results_opti[i])
                else:
                    print("success")

    else:
        do_a_run("with optimization", number_of_arrays, viz=viz, prefix='./output_imgs/', suffix='opti.png')
        print("success")
    
    
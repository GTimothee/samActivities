import sys

sys.path.insert(0,'/home/user/Documents/workspace/projects/dask') # custom dask
sys.path.insert(1,'/home/user/Documents/workspace/projects/samActivities/tests') 
sys.path.insert(2,'/home/user/Documents/workspace/projects/samActivities/tests/optimize_io')
sys.path.insert(3,'/home/user/Documents/workspace/projects/samActivities/experience3')

import dask
import dask.array as da

import optimize_io
from optimize_io.clustered import *

from tests_utils import *

import experience3

import time, os
import numpy as np
import math
import h5py


def test_convert_proxy_to_buffer_slices():
    proxy_key = ("array-645364531", 1, 1, 0)
    merged_task_name = "buffer-645136513-5-13"
    slices = (slice(5, 10, None), slice(10, 20, None), slice(None, None, None))
    array_to_original = {"array-645364531": "array-original-645364531"}
    original_array_chunks = {"array-original-645364531": (10,20,30)}
    original_array_blocks_shape = {"array-original-645364531": (5, 3, 2)}
    result_slices = convert_proxy_to_buffer_slices(proxy_key, merged_task_name, slices, array_to_original, original_array_chunks, original_array_blocks_shape)
    
    """
    0,0,0 = 0
    0,0,1 = 1
    0,1,1 = sizeofrow + 1

    sizeofslice = 6
    sizeof_row = 2
    pos in img = 1*6 + 1*2 + 0*1 = 8
    pos in buffer = 8 - 5 = 3
    pos in buffer in 3d = (0, 1, 1)
    pos in buffer not in terms of block = (0:10, 20:40, 30:60)
    """

    expected_slices = (slice(5, 10, None), slice(30, 40, None), slice(30, 60, None))
    if result_slices != expected_slices:
        print("error in", sys._getframe().f_code.co_name)
        print("got", result_slices, ", expected", expected_slices)    
        return
    print('success')


def test_add_getitem_task_in_graph():
    graph = dict()
    buffer_node_name = 'buffer-465316453-10-23'
    array_proxy_key = ("array-645364531", 3, 1, 2)
    array_to_original = {"array-645364531": "array-original-645364531"}
    original_array_chunks = {"array-original-645364531": (10,20,30)}
    original_array_blocks_shape = {"array-original-645364531": (5, 3, 2)}

    """
    size_of_slice = 6
    size_of_row = 2 
    pos in img = 3*6 + 1*2 + 2*1 = 22
    pos in buffer = 22 - 10 = 12
    pos in buffer in 3d = (2, 0, 0)
    pos in buffer not in terms of block = (20:30, 0:20, 0:30)
    """

    getitem_task_name, graph = add_getitem_task_in_graph(graph, buffer_node_name, array_proxy_key, array_to_original, original_array_chunks, original_array_blocks_shape)
    expected_slices = (slice(20, 30, None), slice(0, 20, None), slice(0, 30, None))

    # try retrieving data from graph
    try:
        buffer_proxy_key = list(graph.keys())[0]
        buffer_proxy_dict = graph[buffer_proxy_key]
        buffer_proxy_subtask_key = list(buffer_proxy_dict.keys())[0]
        buffer_proxy_subtask_val = buffer_proxy_dict[buffer_proxy_subtask_key]
        
        buffer_proxy_name = buffer_proxy_subtask_key[0]
        pos_in_buffer = buffer_proxy_subtask_key[1:]
        getitem, buffer_key, slices_from_buffer = buffer_proxy_subtask_val
    except:
        print("error in", sys._getframe().f_code.co_name)
        print(graph)
        return

    # if expected data is here, test the values
    error = False
    if pos_in_buffer != (2, 0, 0):
        error = True
    if buffer_key[0] != 'buffer-465316453-10-23':
        error = True
    if buffer_proxy_name != 'buffer-465316453-10-23-proxy':
        error = True
    if slices_from_buffer != expected_slices:
        error = True
    if error:
        print("error in", sys._getframe().f_code.co_name)
        print(graph)
        return
    print('success')


def test_recursive_search_and_update():
    graph = dict()
    buffer_node_name = 'buffer-465316453-10-23'
    array_to_original = {"array-645364531": "array-original-645364531"}
    original_array_chunks = {"array-original-645364531": (10, 20, 30)}
    original_array_blocks_shape = {"array-original-645364531": (5, 3, 2)}

    _list = [[[('rechunk-split-7c9f5c6cedeb992c5f39c40adfae384b', 1), ("array-645364531", 2, 0, 0)],
              [('rechunk-split-7c9f5c6cedeb992c5f39c40adfae384b', 2), ("array-645364531", 1, 2, 0)],
              [('rechunk-split-7c9f5c6cedeb992c5f39c40adfae384b', 3), ("array-645364531", 2, 2, 2)]],
            [[('rechunk-split-7c9f5c6cedeb992c5f39c40adfae384b', 4), ("array-645364531", 2, 1, 1)],
             [('rechunk-split-7c9f5c6cedeb992c5f39c40adfae384b', 5), ("array-645364531", 3, 1, 1)],
             [('rechunk-split-7c9f5c6cedeb992c5f39c40adfae384b', 6), ("array-645364531", 3, 1, 2)]]]

    graph, _list = recursive_search_and_update(graph, _list, buffer_node_name, array_to_original, original_array_chunks, original_array_blocks_shape)
    
    slices_list = [("array-645364531", 2, 0, 0), ("array-645364531", 1, 2, 0), ("array-645364531", 2, 2, 2), ("array-645364531", 2, 1, 1), ("array-645364531", 3, 1, 1), ("array-645364531", 3, 1, 2)]
    buffer_pos_list = list()
    for i, s in enumerate(slices_list):
        pos_in_buffer, slices = convert_proxy_to_buffer_slices(s, 'buffer-465316453-10-23', (slice(None, None, None), slice(None, None, None), slice(None, None, None)), array_to_original, original_array_chunks, original_array_blocks_shape)
        buffer_pos_list.append(pos_in_buffer)

    expected_list = [[[('rechunk-split-7c9f5c6cedeb992c5f39c40adfae384b', 1), ("buffer-465316453-10-23-proxy", buffer_pos_list[0][0], buffer_pos_list[0][1], buffer_pos_list[0][2])],
                    [('rechunk-split-7c9f5c6cedeb992c5f39c40adfae384b', 2), ("buffer-465316453-10-23-proxy", buffer_pos_list[1][0], buffer_pos_list[1][1], buffer_pos_list[1][2])],
                    [('rechunk-split-7c9f5c6cedeb992c5f39c40adfae384b', 3), ("buffer-465316453-10-23-proxy", buffer_pos_list[2][0], buffer_pos_list[2][1], buffer_pos_list[2][2])]],
                    [[('rechunk-split-7c9f5c6cedeb992c5f39c40adfae384b', 4), ("buffer-465316453-10-23-proxy", buffer_pos_list[3][0], buffer_pos_list[3][1], buffer_pos_list[3][2])],
                    [('rechunk-split-7c9f5c6cedeb992c5f39c40adfae384b', 5), ("buffer-465316453-10-23-proxy", buffer_pos_list[4][0], buffer_pos_list[4][1], buffer_pos_list[4][2])],
                    [('rechunk-split-7c9f5c6cedeb992c5f39c40adfae384b', 6), ("buffer-465316453-10-23-proxy", buffer_pos_list[5][0], buffer_pos_list[5][1], buffer_pos_list[5][2])]]]
    
    if _list != expected_list:
        print("error in", sys._getframe().f_code.co_name)
        print(_list, "expected", _list)
        return
    
    print("success")
    return


def test_update_io_tasks_getitem():
    buffer_node_name = 'buffer-465316453-10-23'
    array_to_original = {"array-645364531": "array-original-645364531"}
    original_array_chunks = {"array-original-645364531": (10, 20, 30)}
    original_array_blocks_shape = {"array-original-645364531": (5, 3, 2)}

    getitem_grpah = get_getitem_dict_from_proxy_array_sample()
    getitem_graph = update_io_tasks_getitem(getitem_graph, original_array_chunks, buffer_node_name, original_array_blocks_shape)
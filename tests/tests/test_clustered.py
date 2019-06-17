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
        getitem, buffer_proxy_key, slices_from_buffer = buffer_proxy_subtask_val
    except:
        print("error in", sys._getframe().f_code.co_name)
        print(graph)
        return

    # if expected data is here, test the values
    error = False
    if pos_in_buffer != (2, 0, 0):
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

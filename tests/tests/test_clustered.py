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


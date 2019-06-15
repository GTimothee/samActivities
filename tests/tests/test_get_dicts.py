import sys

sys.path.insert(0,'/home/user/Documents/workspace/projects/dask') # custom dask
sys.path.insert(1,'/home/user/Documents/workspace/projects/samActivities/tests') 
sys.path.insert(2,'/home/user/Documents/workspace/projects/samActivities/tests/optimize_io')

import dask
import dask.array as da

import optimize_io
from optimize_io.get_dicts import *

from tests_utils import *

import time, os
import numpy as np
import math


def test_get_arrays_dictionaries():
    pass 


def test_get_original_array_from_proxy_array_name():
    pass 


def test_get_array_block_dims():
    shape = (500, 1200, 300)
    chunks = (100, 300, 20)
    block_dims = get_array_block_dims(shape, chunks)
    expected = (5, 4, 15)
    if block_dims != expected:
        print("error in", sys._getframe().f_code.co_name)
        print("expected", expected, ", got", block_dims)
        return 
    print("success")
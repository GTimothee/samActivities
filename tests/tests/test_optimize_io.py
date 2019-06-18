import sys

sys.path.insert(0,'/home/user/Documents/workspace/projects/dask') # custom dask
sys.path.insert(1,'/home/user/Documents/workspace/projects/samActivities/tests') 
sys.path.insert(2,'/home/user/Documents/workspace/projects/samActivities/tests/optimize_io')
sys.path.insert(3,'/home/user/Documents/workspace/projects/samActivities/experience3')

import dask
import dask.array as da

import optimize_io
from optimize_io.optimize_io import *

from tests_utils import *

import experience3

import time, os
import numpy as np
import math
import h5py

def test_convert_slices_list_to_numeric_slices():
    slices_dict = {
        'a': [(0,0,1), (0,1,0), (0,2,1), (0,1,1)],
        'b': [(1,1,0), (1,3,1), (0,2,2), (1,4,3)]
    }
    slices_dict = convert_slices_list_to_numeric_slices(slices_dict, array_to_original, original_array_chunks)


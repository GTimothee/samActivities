import sys

sys.path.insert(0,'/home/user/Documents/workspace/projects/dask') # custom dask
sys.path.insert(1,'/home/user/Documents/workspace/projects/samActivities/tests') 
sys.path.insert(2,'/home/user/Documents/workspace/projects/samActivities/tests/optimize_io')
sys.path.insert(3,'/home/user/Documents/workspace/projects/samActivities/experience3')

import dask
import dask.array as da

import optimize_io
from optimize_io.get_dicts import *

from tests_utils import *

import experience3

import time, os
import numpy as np
import math
import h5py


def test_convert_proxy_to_buffer_slices():
    convert_proxy_to_buffer_slices(proxy_key, merged_task_name, slices)
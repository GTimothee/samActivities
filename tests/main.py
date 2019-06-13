import sys
sys.path.insert(0,'/home/user/Documents/workspace/projects/dask') # custom dask
sys.path.insert(1,'/home/user/Documents/workspace/projects/samActivities/') # my utility lib
import dask
import dask.array as da
import experience3
from experience3.utils import create_random_cube, load_array_parts, get_dask_array_from_hdf5
from dask.array.io_optimization import optimize_io
import time, os
import numpy as np
import math
from tests_funcs import *

test_add_or_create_to_list_dict()
test_get_keys_from_graph()
test_get_rechunk_subkeys()
test_test_source_key()
test_get_slices_from_rechunk_subkeys()
test_get_slices_from_rechunk_keys()
test_get_slices_from_getitem_subkeys()
test_get_slices_from_getitem_keys()
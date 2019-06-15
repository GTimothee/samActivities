import sys

sys.path.insert(0,'/home/user/Documents/workspace/projects/dask') # custom dask
sys.path.insert(1,'/home/user/Documents/workspace/projects/samActivities/tests') 
sys.path.insert(2,'/home/user/Documents/workspace/projects/samActivities/tests/optimize_io')

import dask
import dask.array as da

from tests.test_optimize_io import *
from tests.test_get_dicts import *
from tests.test_get_slices import *
from tests.test_clustered import *
from tests_utils import *

import time, os
import numpy as np
import math


def test_get_slices():
    test_add_or_create_to_list_dict()
    test_get_keys_from_graph()
    test_get_rechunk_subkeys()
    test_test_source_key()
    test_get_slices_from_rechunk_subkeys()
    test_get_slices_from_rechunk_keys()
    test_get_slices_from_getitem_subkeys()
    test_get_slices_from_getitem_keys()
    test_get_slices_from_dask_graph()

def test_get_dicts():
    test_get_array_block_dims()

test_get_array_block_dims()
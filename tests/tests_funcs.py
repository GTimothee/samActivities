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

from tests import get_basic_data

def test_get_slices_from_rechunk_dict():
    graph = get_graph_for_tests(0)
    rechunk_keys, rechunk_graph, proxy_array_keys, others = get_rechunk_keys_lists_from_dask_array(graph, printer=False)

    slices_dict = dict()
    target = None
    for key, parent in rechunk_keys:
        rechunk_merge_graph = graph[parent]
        target_name, slices_list = get_slices_from_rechunk_dict(rechunk_merge_graph)
        if target_name is not None:
            target = target_name
            if len(total_slices) == 0:
                total_slices = slices_list
            else:
                total_slices = total_slices + slices_list
    total_slices = list(set(total_slices))
    print(total_slices)
    print(target_name)

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

cases = {
    0:'slabs_dask_interpol',
    1:'slabs_previous_exp',
    2:'blocks_dask_interpol',
    3:'blocks_previous_exp'
}

def logical_chunks_tests(arr, case):
    if case == 'slabs_dask_interpol':
        slab_width = 6
        new_chunks_shape = (slab_width, arr.shape[1], arr.shape[2])
        nb_chunks = [math.floor(arr.shape[0] / slab_width), 1, 1]
        arr = arr[:slab_width * nb_chunks[0],:,:]
        arr = arr.rechunk((tuple([slab_width] * nb_chunks[0]),(1210),(1400)))
    elif case == 'slabs_previous_exp':
        slab_width = 192
        new_chunks_shape = (slab_width, arr.shape[1], arr.shape[2])
        nb_chunks = [math.floor(arr.shape[0] / slab_width), 1, 1]
        arr = arr[:slab_width * nb_chunks[0],:,:]
        arr = arr.rechunk((tuple([slab_width] * nb_chunks[0]),(1210),(1400)))
    elif case == 'blocks_dask_interpol':
        new_chunks_shape = tuple([c[0] for c in arr.chunks])
        nb_chunks = [len(c) for c in arr.chunks]
    elif case == 'blocks_previous_exp':
        new_chunks_shape = (770,605,700)
        nb_chunks = [int(arr.shape[i] / new_chunks_shape[i]) for i in range(3)]
        arr = arr.rechunk((tuple([new_chunks_shape[0]] * nb_chunks[0]),
                          tuple([new_chunks_shape[1]] * nb_chunks[1]),
                          tuple([new_chunks_shape[2]] * nb_chunks[2])))
    else:
        raise ValueError("error")
    print("new chunks", arr.shape, arr.chunks)
    print("new_chunks_shape", new_chunks_shape)

    all_arrays = list()
    for i in range(nb_chunks[0]):
        for j in range(nb_chunks[1]):
            for k in range(nb_chunks[2]):
                all_arrays.append(load_array_parts(arr=arr,
                                                   geometry="right_cuboid",
                                                   shape=new_chunks_shape,
                                                   upper_corner=(i * new_chunks_shape[0],
                                                                 j * new_chunks_shape[1],
                                                                 k * new_chunks_shape[2]),
                                                   random=False))

    # to del:
    all_arrays = all_arrays[:2]
    a5 = all_arrays.pop(0)
    for a in all_arrays:
        a5 = a5 + a
    return a5

def get_graph_for_tests(i):
    data_path = '/home/user/Documents/workspace/projects/samActivities/experience3/tests/data/bbsamplesize.hdf5'
    one_gig = 1000000000
    key = "data"
    arr = get_dask_array_from_hdf5(data_path, key)
    a5 = logical_chunks_tests(arr, cases[i])
    graph = a5.dask.dicts
    return graph

def neatly_print_dict(d):
    for k, v in d.items():
        print(k, v, "\n")


global example_rechunk_dict = {('rechunk-merge-bcfb966a39aa5079f6457f1530dd85df',
  0,
  0,
  0): (<function dask.array.core.concatenate3(arrays)>, [[[('rechunk-split-bcfb966a39aa5079f6457f1530dd85df',
      0)],
    [('rechunk-split-bcfb966a39aa5079f6457f1530dd85df', 1)],
    [('rechunk-split-bcfb966a39aa5079f6457f1530dd85df', 2)]]]),
 ('rechunk-merge-bcfb966a39aa5079f6457f1530dd85df',
  0,
  0,
  1): (<function dask.array.core.concatenate3(arrays)>, [[[('rechunk-split-bcfb966a39aa5079f6457f1530dd85df',
      3)],
    [('rechunk-split-bcfb966a39aa5079f6457f1530dd85df', 4)],
    [('rechunk-split-bcfb966a39aa5079f6457f1530dd85df', 5)]]]),
 ('rechunk-split-bcfb966a39aa5079f6457f1530dd85df',
  0): (<function _operator.getitem(a, b, /)>, ('rechunk-merge-1ea6569403cc56ee4e2ec69a35dc1d1d',
   0,
   0,
   0), (slice(0, 60, None), slice(0, 403, None), slice(0, 700, None))),
 ('rechunk-split-bcfb966a39aa5079f6457f1530dd85df',
  1): (<function _operator.getitem(a, b, /)>, ('rechunk-merge-1ea6569403cc56ee4e2ec69a35dc1d1d',
   0,
   1,
   0), (slice(0, 60, None), slice(0, 403, None), slice(0, 700, None))),
 ('rechunk-split-bcfb966a39aa5079f6457f1530dd85df',
  2): (<function _operator.getitem(a, b, /)>, ('rechunk-merge-1ea6569403cc56ee4e2ec69a35dc1d1d',
   0,
   2,
   0), (slice(0, 60, None), slice(0, 404, None), slice(0, 700, None))),
 ('rechunk-split-bcfb966a39aa5079f6457f1530dd85df',
  3): (<function _operator.getitem(a, b, /)>, ('rechunk-merge-1ea6569403cc56ee4e2ec69a35dc1d1d',
   0,
   0,
   1), (slice(0, 60, None), slice(0, 403, None), slice(0, 700, None))),
 ('rechunk-split-bcfb966a39aa5079f6457f1530dd85df',
  4): (<function _operator.getitem(a, b, /)>, ('rechunk-merge-1ea6569403cc56ee4e2ec69a35dc1d1d',
   0,
   1,
   1), (slice(0, 60, None), slice(0, 403, None), slice(0, 700, None))),
 ('rechunk-split-bcfb966a39aa5079f6457f1530dd85df',
  5): (<function _operator.getitem(a, b, /)>, ('rechunk-merge-1ea6569403cc56ee4e2ec69a35dc1d1d',
   0,
   2,
   1), (slice(0, 60, None), slice(0, 404, None), slice(0, 700, None)))
}
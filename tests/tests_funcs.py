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

from tests_utils import *
from en_dev import *


def test_add_or_create_to_list_dict():
    d = {'a': [1], 'c': [5, 6]}
    d = add_or_create_to_list_dict(d, 'b', [2, 3])
    d = add_or_create_to_list_dict(d, 'b', [5])
    expected = {'a': [1], 'b': [2, 3, 4], 'c': [5, 6]}
    
    # test 
    if expected != d:
        print("error")
        print(d, "VS", expected)
    print("it works")


def test_get_keys_from_graph():
    d = {'a-165152': 1, 
         'b-865134': 2,
         'a-864531': 3,
         'c-864535': 4}
    keys_dict = get_keys_from_graph(d, printer=False)
    expected = {'a': ['b-865134'],
                'b': ['a-165152', 'a-864531'],
                'c': ['c-864535']}
    
    # test 
    for k, v in expected:
        if not k in list(keys_dict.keys()):
            print("error")
            print(k, "VS", list(keys_dict.keys()))
            return 
        if set(keys_dict[k]) != set(v):
            print("error")
            print(keys_dict[k], "VS", v)
            return 
    print("it works")


def test_get_rechunk_subkeys():
    d = example_rechunk_dict
    split_keys, merge_keys = get_rechunk_subkeys(d)

    expected_split_keys = [('rechunk-merge-bcfb966a39aa5079f6457f1530dd85df', 0, 0, 0),
                           ('rechunk-merge-bcfb966a39aa5079f6457f1530dd85df', 0, 0, 1)]

    expected_merge_keys = [('rechunk-split-bcfb966a39aa5079f6457f1530dd85df', 1)
                           ('rechunk-split-bcfb966a39aa5079f6457f1530dd85df', 2)
                           ('rechunk-split-bcfb966a39aa5079f6457f1530dd85df', 3)]

    if not split_keys == expected_split_keys:
        print("error")
        print(split_keys, "VS", expected_merge_keys)

    if not merge_keys == expected_split_keys:
        print("error")
        print(merge_keys, "VS", expected_merge_keys)
    print("it works")


def test_test_source_key():
    slices_dict = dict()
    sample_source_key = ['array-645318645', 
                         slice(None, None, None), 
                         slice(None, None, None), 
                         slice(None, None, None)]
    expected = {'array-645318645': tuple(sample_source_key)}
    slices_dict = test_source_key(slices_dict, tuple(sample_source_key))
    sample_source_key[0] = "tmp"
    slices_dict = test_source_key(slices_dict, tuple(sample_source_key))
    sample_source_key[0] = 864513
    slices_dict = test_source_key(slices_dict, tuple(sample_source_key))
    
    if slices_dict != expected:
        print("error")
        print(slices_dict, "VS", expected)
    print("it works")

    
def test_get_slices_from_rechunk_subkeys():
    pass 


def test_get_slices_from_rechunk_keys():
    pass 


def test_get_slices_from_getitem_subkeys():
    pass


def test_get_slices_from_getitem_keys():
    pass 


def test_get_slices_from_dask_graph():
    graph = get_graph_for_tests(0)
    slices_dict = get_slices_from_dask_graph(graph)
import sys
sys.path.insert(0, './tests')

from test_optimize_io import *
from test_get_dicts import *
from test_get_slices import *
from test_clustered import *
from tests_utils import *


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
    test_get_original_array_from_proxy_array_name()
    test_get_arrays_dictionaries()


def test_clustered():
    test_convert_proxy_to_buffer_slices()
    test_add_getitem_task_in_graph()
    test_recursive_search_and_update()

test_update_io_tasks_getitem()
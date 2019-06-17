from .optimize_io import *
from dask.base import tokenize

import operator
from operator import getitem

__all__ = ("apply_clustered_strategy", "create_buffers", "create_buffer_node", 
           "update_io_tasks", "update_io_tasks_rechunk", "update_io_tasks_getitem", 
           "add_getitem_task_in_graph", "recursive_search_and_update", "convert_proxy_to_buffer_slices")


def apply_clustered_strategy(graph, slices_dict, array_to_original, original_array_chunks, original_array_shapes, original_array_blocks_shape):
    for proxy_array_name, slices_list in slices_dict.items(): 
        buffers = create_buffers(slices_list, proxy_array_name)

        for load_index in range(len(buffers)):
            load = buffers[load_index]
            if len(load) > 1:
                graph, buffer_node_name = create_buffer_node(graph, proxy_array_name, load, original_array_blocks_shape, original_array_chunk)
                update_io_tasks(graph, deps_dict, proxy_array_name, original_array_chunk, original_array_blocks_shape, buffer_node_name)
    return graph


def create_buffers(slices_list, proxy_array_name, nb_bytes_per_val=8):
    """ current strategy : entire blocks
    # TODO support more strategies
    """

    def get_buffer_mem_size(config):
        try:
            optimization = config.get("io-optimizer")
            try:
                return config.get("io-optimizer.memory_available")
            except:
                print("missing configuration information memory_available")
                print("using default configuration: 1 gigabytes")
                return 1000000000
        except:
            raise ValueError("io-optimizer not enabled")


    def get_load_strategy(buffer_mem_size, nb_bytes_per_val):
        """ get clustered writes best load strategy given the memory available for io optimization
        """
        block_mem_size = block_shape[0] * block_shape[1] * block_shape[2] * nb_bytes_per_val
        block_row_size = block_mem_size * img_nb_blocks_per_dim[2]
        block_slice_size = block_row_size * img_nb_blocks_per_dim[1]

        if buffer_mem_size >= block_slice_size:
            nb_slices = math.floor(buffer_mem_size / block_slice_size)
            return "slices", nb_slices * img_nb_blocks_per_dim[2] * img_nb_blocks_per_dim[1]
        elif buffer_mem_size >= block_row_size:
            nb_rows = math.floor(buffer_mem_size / block_row_size)
            return "rows", nb_rows * img_nb_blocks_per_dim[2]
        else:
            return "blocks", math.floor(buffer_mem_size / block_mem_size)


    def new_list(list_of_lists):
        list_of_lists.append(list())
        return list_of_lists, None


    def bad_configuration_incoming(prev_i, strategy, original_array_blocks_shape):
            """ to avoid bad configurations in clustered writes
            """
            if not prev_i:
                return False 
            elif strategy == "blocks" and prev_i % original_array_blocks_shape[2] == 0:
                return True 
            elif strategy == "rows" and prev_i % (original_array_blocks_shape[1] * original_array_blocks_shape[1]) == 0:
                return True 
            else:
                return False


    def test_if_create_new_load(list_of_lists, prev_i, strategy, original_array_blocks_shape):
        if len(list_of_lists[-1]) == nb_blocks_per_load:
            return new_list(list_of_lists)
        elif prev_i and next_i != prev_i + 1:
            return new_list(list_of_lists)
        elif bad_configuration_incoming(prev_i, strategy, original_array_blocks_shape):
            return new_list(list_of_lists)
        else:
            return list_of_lists, prev_i
            

    original_array_name = array_to_original[proxy_array_name]
    original_array_blocks_shape = original_array_blocks_shape[original_array_name]
    buffer_mem_size = get_buffer_mem_size(config)
    strategy, nb_blocks_per_load = get_load_strategy(buffer_mem_size)

    slices_list, prev_i = new_list(list())
    while len(slices_list) > 0:
        next_i = slices_list.pop(0)
        slices_list, prev_i = test_if_create_new_load(list_of_lists, prev_i, strategy, original_array_blocks_shape)
        list_of_lists[len(list_of_lists) - 1].append(next_i)
        prev_i = next_i       

    return list_of_lists


def create_buffer_node(dask_graph, proxy_array_name, load, original_array_blocks_shape, original_array_chunk):
    def get_coords_in_image(block_coord, original_array_chunk):
        return tuple([block_coord[i] * original_array_chunk[i] for i in range(3)])

    def get_buffer_slices_from_original_array(load, original_array_blocks_shape, original_array_chunk):
        _min = [None, None, None]
        _max = [None, None, None]
        for block_index_num in range(load[0], load[-1] + 1):
            block_index_3d = numeric_to_3d_pos(block_index_num, original_array_blocks_shape, order='C') 
            for j in range(3):
                if _max[j] == None:
                    _max[j] = block_index_3d[j]
                if _min[j] == None:
                    _min[j] = block_index_3d[j]
                if block_index_3d[j] > _max[j]:
                    _max[j] = block_index_3d[j]
                if block_index_3d[j] < _min[j]:
                    _min[j] = block_index_3d[j]

        start = get_coords_in_image(tuple(_min), original_array_chunk)
        end = tuple([x + 1 for x in tuple(_max)])
        end = get_coords_in_image(end, original_array_chunk)
        return (slice(start[0], end[0], None),
                slice(start[1], end[1], None),
                slice(start[2], end[2], None))
    
    # get new key
    merged_array_proxy_name = 'merged-part-' + str(load[0]) + '-' + str(load[-1])
    key = (merged_array_proxy_name, 0, 0, 0)
    
    # get new value
    array_proxy_dict = dask_graph[proxy_array_name]
    original_array_name = array_to_original[proxy_array_name]
    buffer_block_slices = get_buffer_slices_from_original_array(load, original_array_blocks_shape, original_array_chunk)
    get_func = array_proxy_dict[list(array_proxy_dict.keys())[0]][0]
    value = (get_func, original_array_name, (buffer_block_slices[0], 
                                             buffer_block_slices[1], 
                                             buffer_block_slices[2]))

    # add new key/val pair to the dask graph
    dask_graph[merged_array_proxy_name] = {key: value}
    return dask_graph, merged_array_proxy_name


def update_io_tasks(graph, deps_dict, proxy_array_name, original_array_chunk, original_array_blocks_shape, buffer_node_name):
    keys_dict = get_keys_from_graph(graph)
    rechunk_keys = keys_dict['rechunk-merge']
    getitem_keys = keys_dict['getitem']
    
    dependent_tasks = deps_dict[proxy_array_name]

    for key in rechunk_keys:
        update_io_tasks_rechunk(graph, graph[key], original_array_chunk, original_array_blocks_shape, dependent_tasks, buffer_node_name)

    for key in getitem_keys:
        graph[key] = update_io_tasks_getitem(graph[key], proxy_array_name, dependent_tasks, dependent_tasks, buffer_node_name)   


def update_io_tasks_rechunk(graph, rechunk_graph, original_array_chunk, original_array_blocks_shape, dependent_tasks, buffer_node_name):
    def replace_rechunk_merge(val, buffer_node_name, array_to_original, original_array_chunks, original_array_blocks_shape):
        f, concat_list = val
        graph, concat_list = recursive_search_and_update(graph, concat_list, buffer_node_name, array_to_original, original_array_chunks, original_array_blocks_shape)
        return graph, (f, concat_list)

    def replace_rechunk_split(val, original_array_blocks_shape):
        get_func, target_key, slices = val
        _, s1, s2, s3 = target_key
        array_part_num = _3d_to_numeric_pos((s1, s2, s3), original_array_blocks_shape, order='C') 

        if not array_part_num in load:
            return val
        
        slice_of_interest = convert_proxy_to_buffer_slices(proxy_array_part, original_array_chunk, merged_task_name, original_array_blocks_shape, slices)
        return (get_func, (merged_task_name, 0, 0, 0), slice_of_interest)

    for k in list(rechunk_graph.keys()):
        if k in dependent_tasks:
            key_name = k[0]
            val = rechunk_graph[k]
            if 'rechunk-merge' in key_name:
                graph, new_val = replace_rechunk_merge(val, buffer_node_name, array_to_original, original_array_chunks, original_array_blocks_shape)
            elif 'rechunk-split' in key_name:
                new_val = replace_rechunk_split(val)
            rechunk_graph[k] = new_val


def update_io_tasks_getitem(getitem_graph, original_array_chunks, buffer_node_name, original_array_blocks_shape):
    for k in list(getitem_graph.keys()):
        if k in dependent_tasks:
            val = getitem_graph[k]
            get_func, proxy_key, slices = val
            slice_of_interest = convert_proxy_to_buffer_slices(proxy_key, buffer_node_name, slices, array_to_original, original_array_chunks, original_array_blocks_shape)
            new_val = (get_func, (buffer_node_name, 0, 0, 0), slice_of_interest)
            getitem_graph[k] = new_val   
    return getitem_graph


def recursive_search_and_update(graph, _list, buffer_node_name, array_to_original, original_array_chunks, original_array_blocks_shape):
    if not isinstance(_list[0], tuple):
        for i in range(len(_list)):
            sublist = _list[i] 
            graph, sublist = recursive_search_and_update(graph, sublist, buffer_node_name, array_to_original, original_array_chunks, original_array_blocks_shape)
            _list[i] = sublist
    else:
        for i in range(len(_list)):
            target_key = _list[i]
            if 'array-' in target_key[0]:
                getitem_task_key, graph = add_getitem_task_in_graph(graph, buffer_node_name, target_key, array_to_original, original_array_chunks, original_array_blocks_shape)
                _list[i] = getitem_task_key
    return graph, _list


def add_getitem_task_in_graph(graph, buffer_node_name, proxy_key, array_to_original, original_array_chunks, original_array_blocks_shape):
    """ replace a rechunk-merged call to an array proxy part by a rechunk-merged call to a buffer proxy part 
    to do that, create a buffer proxy, add it the buffer proxy part 
    """

    # new key
    target_name = proxy_key[0]
    slices = (slice(None, None, None), slice(None, None, None), slice(None, None, None))
    buffer_proxy_name = buffer_node_name + '-proxy'
    
    # get slices from buffer_proxy
    pos_in_buffer, slices_from_buffer = convert_proxy_to_buffer_slices(proxy_key, buffer_node_name, slices, array_to_original, original_array_chunks, original_array_blocks_shape)
    buffer_proxy_subtask_key = tuple([buffer_proxy_name] + list(pos_in_buffer))
    buffer_proxy_subtask_val = (getitem, (buffer_node_name, 0, 0, 0), slices_from_buffer)

    # create buffer_proxy if does not exist
    if not buffer_proxy_name in list(graph.keys()):
        graph[buffer_proxy_name] = dict()
    
    # add to buffer proxy
    d = graph[buffer_proxy_name]
    if not buffer_proxy_subtask_key in list(d.keys()):  # it is not possible to have two keys with different values
        d[buffer_proxy_subtask_key] = buffer_proxy_subtask_val
    return buffer_proxy_subtask_key, graph


def convert_proxy_to_buffer_slices(proxy_key, buffer_proxy_name, slices, array_to_original, original_array_chunks, original_array_blocks_shape):
    """ Get the slices of the targetted block in the buffer, from the index of this block in the proxy array. 
    + apply the slices 
    """
    proxy_array_name = proxy_key[0]
    pos_in_proxy_array = proxy_key[1:]
    original_array_name = array_to_original[proxy_array_name]
    img_chunks_sizes = original_array_chunks[original_array_name]
    img_nb_blocks_per_dim = original_array_blocks_shape[original_array_name]

    split = buffer_proxy_name.split('-')
    if len(split) != 4:
        raise ValueError("expected a buffer task name")
    num_start_of_buffer = split[2]

    # convert 3d pos in image to 3d pos in buffer (merged block)
    num_pos_in_proxy = _3d_to_numeric_pos(pos_in_proxy_array, img_nb_blocks_per_dim, order='C') 
    num_pos_in_buffer = num_pos_in_proxy - int(num_start_of_buffer)
    pos_in_buffer = numeric_to_3d_pos(num_pos_in_buffer, img_nb_blocks_per_dim, order='C')

    _slice = pos_in_buffer

    start = [None] * 3
    stop = [None] * 3
    for i, sl in enumerate(slices):
        if sl.start != None:
            start[i] = (_slice[i] * img_chunks_sizes[i]) + sl.start
        else:
            start[i] = _slice[i] * img_chunks_sizes[i]

        if sl.stop != None:
            stop[i] = (_slice[i] * img_chunks_sizes[i]) + sl.stop
        else:
            stop[i] = (_slice[i] + 1) * img_chunks_sizes[i] 
            
    return pos_in_buffer, (slice(start[0], stop[0], None),
                            slice(start[1], stop[1], None),
                            slice(start[2], stop[2], None))

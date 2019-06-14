
import sys

"""
    slices_dict :                       proxy-array -> list of slices
    array_to_original :                 proxy-array -> original-array
    original_array_chunks :             original-array-name -> block shapes (anciennement dims_dict) [shapes of each logical block]
    original_array_shapes :             original-array-name -> original-array shape
    original_array_blocks_shape :       original-array-name -> nb blocks in each dim
    deps_dict :                         proxy-array -> proxy-array-dependent tasks list
"""


def add_or_create_to_list_dict(d, k, v):
    if k not in list(d.keys()):
        d[k] = [v]
    else:
        d[k].append(v)
    return d


def get_keys_from_graph(graph, printer=False):
    key_dict = dict()
    for k, v in graph.items():
        if isinstance(k, tuple):
            k2 = k[0]
        elif isinstance(k, str):
            k2 = k 
        else:
            raise ValueError("type of key unsupported", k, type(k))
        split = k2.split('-')
        key_name = "-".join(split[:-1])
        key_dict = add_or_create_to_list_dict(key_dict, key_name, k)
    return key_dict


def get_rechunk_subkeys(rechunk_graph):
    keys_dict = get_keys_from_graph(rechunk_graph, printer=False)
    return keys_dict['rechunk-split'], keys_dict['rechunk-merge']


def test_source_key(slices_dict, deps_dict, source_key, dependent_key):
    """ test if source is an array proxy: if yes, add source key data to slices_dict
    dependent_key: key of the task dependent from array proxy 
    """

    if len(source_key) != 4:
        raise ValueError("not enough elements to unpack in", source_key)
    if not isinstance(source_key, tuple):
        raise ValueError("expected a tuple:", source_key)
    source, s1, s2, s3 = source_key
    
    if not isinstance(source, str):
        raise ValueError("expected a string:", source)
    if 'array' in source:
        slices_dict = add_or_create_to_list_dict(slices_dict, source, (s1, s2, s3))
        deps_dict = add_or_create_to_list_dict(deps_dict, source, dependent_key)
    return slices_dict, deps_dict


def get_slices_from_rechunk_subkeys(rechunk_merge_graph, split_keys, merge_keys):

    def get_slices_from_splits(split_keys, slices_dict, deps_dict):
        for split_key in split_keys:
            split_value = rechunk_merge_graph[split_key]
            _, source_key, slices = split_value
            slices_dict, deps_dict = test_source_key(slices_dict, deps_dict, source_key, split_key)
        return slices_dict, deps_dict

    # TODO: make better
    def get_slices_from_merges(merge_keys, slices_dict, deps_dict):
        for merge_key in merge_keys:
            merge_value = rechunk_merge_graph[merge_key]
            _, concat_list = merge_value
            while not isinstance(concat_list[0][0], tuple):
                concat_list = concat_list[0]
            for block in concat_list:
                for source_key in block:
                    if len(source_key) == 4:
                        slices_dict, deps_dict = test_source_key(slices_dict, deps_dict, source_key, merge_key)
        return slices_dict, deps_dict

    slices_dict = dict()
    deps_dict = dict()
    slices_dict, deps_dict = get_slices_from_splits(split_keys, slices_dict, deps_dict)
    slices_dict, deps_dict = get_slices_from_merges(merge_keys, slices_dict, deps_dict)
    return slices_dict, deps_dict


def get_slices_from_rechunk_keys(graph, rechunk_keys):
    global_slices_dict = dict()
    global_deps_dict = dict()
    for rechunk_key in rechunk_keys:
        rechunk_graph = graph[rechunk_key]
        split_keys, merge_keys = get_rechunk_subkeys(rechunk_graph)
        local_slices_dict, local_deps_dict = get_slices_from_rechunk_subkeys(rechunk_graph, split_keys, merge_keys)
        global_slices_dict.update(local_slices_dict)
        global_deps_dict.update(local_deps_dict)
    return global_slices_dict, global_deps_dict


def get_slices_from_getitem_subkeys(getitem_graph):
    slices_dict = dict()
    deps_dict = dict()
    for k, v in getitem_graph.items():
        f, source_key, s = v 
        slices_dict, deps_dict = test_source_key(slices_dict, deps_dict, source_key, k)
    return slices_dict, deps_dict


def get_slices_from_getitem_keys(graph, getitem_keys):
    global_slices_dict = dict()
    global_deps_dict = dict()
    for getitem_key in getitem_keys:
        getitem_graph = graph[getitem_key]
        local_slices_dict, local_deps_dict = get_slices_from_getitem_subkeys(getitem_graph)
        global_slices_dict.update(local_slices_dict)
        global_deps_dict.update(local_deps_dict)
    return global_slices_dict, global_deps_dict


# TODO generalize it to a graph/tree search
def get_slices_from_dask_graph(graph):
    keys_dict = get_keys_from_graph(graph)
    
    rechunk_keys = keys_dict['rechunk-merge']
    getitem_keys = keys_dict['getitem']

    slices_dict, deps_dict = get_slices_from_rechunk_keys(graph, rechunk_keys)
    slices_dict_2, deps_dict_2 = get_slices_from_getitem_keys(graph, getitem_keys)
    slices_dict.update(slices_dict_2)
    deps_dict.update(deps_dict_2)
    return slices_dict, deps_dict


######### not tested yet:


def get_original_array_from_proxy_array_name(graph, proxy_array_name):
    proxy_dict = graph[proxy_array_name]
    for chunk_key in list(proxy_dict.keys()):
        if isinstance(chunk_key, str):
            if 'array-original' in chunk_key:
                array_original_name = chunk_key
                break 
    if not array_original_name:
        raise ValueError("Original array not found. Are you sure that you gave a proxy array?")
    return original_array_name, proxy_dict[original_array_name]


def get_array_block_dims(shape, chunks):
    """ from shape of image and size of chukns=blocks, return the dimensions of the array in terms of blocks
    i.e. number of blocks in each dimension
    """
    if not len(shape) == len(chunks):
        raise ValueError("chunks and shape should have the same dimension", shape, chunks)
    return [int(s/c) for s, c in zip(shape, chunks)]


def get_arrays_dictionaries(graph, slices_dict):
    """
        Fill in three utility dictionnaries that will be useful for the next steps
    """

    array_to_original = dict()
    original_array_chunks = dict()
    original_array_shapes = dict()
    original_array_blocks_shape = dict()

    for proxy_array_name in list(slices_dict.keys()):
        original_array_name, original_array_obj = get_original_array_from_proxy_array_name(graph, proxy_array_name)
        array_to_original[proxy_array_name] = original_array_name
        original_array_shapes[original_array_name] = original_array_obj.shape
        original_array_chunks[original_array_name] = original_array_obj.chunks
        original_array_blocks_shape[original_array_name] = get_array_block_dims(original_array_obj.shape, original_array_obj.chunks)
        
    return array_to_original, original_array_chunks, original_array_shapes, original_array_blocks_shape


def convert_slices_list_to_numeric_slices(slices_dict, original_array_chunks):
    for proxy_array, slices_list in slices_dict.items():
        slices_list = sorted(list(set(slices_list)))
        array_dims = original_array_chunks[proxy_array]
        slices_list = [_3d_to_numeric_pos(s, array_dims, order='C') for s in slices_list]
    return slices_dict


def main():
    graph = None # TODO
    slices_dict = get_slices_from_dask_graph(graph)
    array_to_original, original_array_chunks, original_array_shapes, original_array_blocks_shape = get_arrays_dictionaries(graph, slices_dict)
    slices_dict = convert_slices_list_to_numeric_slices(slices_dict, original_array_chunks)
    apply_clustered_strategy(graph, slices_dict, array_to_original, original_array_chunks, original_array_shapes, original_array_blocks_shape)


def apply_clustered_strategy(graph, slices_dict, array_to_original, original_array_chunks, original_array_shapes, original_array_blocks_shape):
    for proxy_array_name, slices_list in slices_dict.items(): 
        buffers = create_buffers(slices_list, proxy_array_name)

        for load_index in range(len(buffers)):
            load = buffers[load_index]
            if len(load) > 1:
                graph, root_node_name = create_root_node(graph, proxy_array_name, load, original_array_blocks_shape, original_array_chunk)
                update_io_tasks(proxy_array_name, deps_dict, root_node_name, original_array_blocks_shape, original_array_chunk, load)
    return graph


# TODO support more strategies
def create_buffers(slices_list, proxy_array_name, nb_bytes_per_val=8):
    """ current strategy : entire blocks
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



def create_root_node(dask_graph, proxy_array_name, load, original_array_blocks_shape, original_array_chunk):

    def get_coords_in_image(block_coord, original_array_chunk):
        return tuple([block_coord[i] * original_array_chunk[i] for i in range(3)])

    def get_buffer_slices_from_original_array(load, original_array_blocks_shape, original_array_chunk):
        _min = [None, None, None]
        _max = [None, None, None]
        for block_index_num in range(load[0], load[-1] + 1):
            block_index_3d = numeric_to_3d_pos(block_index_num, original_array_blocks_shape, order='C') 
            for j in range(3):
                if _max[j] = None:
                    _max[j] = block_index_3d[j]
                if _min[j] = None:
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


def update_io_tasks():
    if config.get("io-optimizer.mode") == 'rechunked':
        rechunk_task_keys = proxy_arrays_to_task_names_dict[proxy_array_name]
        rechunk_dicts = [(rechunk_task_key, dask_graph[rechunk_task_key]) for rechunk_task_key in rechunk_task_keys]                       

        for rechunk_task_key, rechunk_dict in rechunk_dicts:
            replace_proxy_array_by_merged_proxy(rechunk_dict, proxy_array_name)


######### not refactored:  

def numeric_to_3d_pos(numeric_pos, shape, order):
    if order == 'F':  
        # nb_blocks_per_row = shape[0]
        # nb_blocks_per_slice = shape[0] * shape[1]
        raise ValueError("to_verify")
    elif order == 'C':
        nb_blocks_per_row = shape[2]
        nb_blocks_per_slice = shape[1] * shape[2]
    else:
        raise ValueError("unsupported")
    
    i = math.floor(numeric_pos / nb_blocks_per_slice)
    numeric_pos -= i * nb_blocks_per_slice
    j = math.floor(numeric_pos / nb_blocks_per_row)
    numeric_pos -= j * nb_blocks_per_row
    k = numeric_pos
    return (i, j, k)

def _3d_to_numeric_pos(_3d_pos, shape, order):
    """
    in C order for example, should be ((_3d_pos[0]-1) * nb_blocks_per_slice) 
    but as we start at 0 we can keep (_3d_pos[0] * nb_blocks_per_slice)
    """
    if order == 'F':  
        # nb_blocks_per_row = shape[0]s
        # nb_blocks_per_slice = shape[0] * shape[1]
        raise ValueError("to_verify")
    elif order == 'C':
        nb_blocks_per_row = shape[2]
        nb_blocks_per_slice = shape[1] * shape[2]
    else:
        raise ValueError("unsupported")
    return (_3d_pos[0] * nb_blocks_per_slice) + (_3d_pos[1] * nb_blocks_per_row) + _3d_pos[2] 


    
def update_io_tasks(proxy_array_name, 
                    proxy_arrays_to_task_names_dict, 
                    merged_task_name,
                    img_nb_blocks_per_dim,
                    img_chunks_sizes,
                    load):
    """
    -find the tasks using the array_name (using dependencies?)
    for each task
        -replace the array_name subpart 
        -replace the slices part
    """
    
    def replace_proxy_array_by_merged_proxy(rechunk_dict, proxy_array_name):
        """def replace_rechunk_merge(val):
            _, concat_list = val
            while not isinstance(concat_list[0][0], tuple):
                concat_list = concat_list[0]
            for _list in concat_list:
                for i in range(len(_list)):
                    tupl = _list[i]
                    if proxy_array_name == tupl[0]:
                        #target_name, s1, s2, s3 = tupl
                        pass
            return"""
        
        def replace_rechunk_split(val):
            get_func, target_key, slices = val
            _, s1, s2, s3 = target_key
            proxy_array_part = (s1, s2, s3)
            array_part_num = _3d_to_numeric_pos(proxy_array_part, img_nb_blocks_per_dim, order='C') 
            if array_part_num in load:
                slice_of_interest = get_slice_from_proxy_part_key(proxy_array_part, img_chunks_sizes, merged_task_name, img_nb_blocks_per_dim, slices)
                new_val = (get_func, (merged_task_name, 0, 0, 0), slice_of_interest)
                return new_val
            else:
                return val

        for k in list(rechunk_dict.keys()):
            key_name = k[0]
            val = rechunk_dict[k]
            if 'rechunk-merge' in key_name:
                #new_val = replace_rechunk_merge(val)
                #rechunk_dict[k] = new_val
                pass
            elif 'rechunk-split' in key_name:
                new_val = replace_rechunk_split(val)
                rechunk_dict[k] = new_val
            else:
                pass 
    
    def get_tasks_targeting_proxy(getitem_dict, proxy_array_name):
        return [(k,v) for k, v in getitem_dict.items() if v[1][0] == proxy_array_name]

    def get_slice_from_proxy_part_key(proxy_array_part_targeted, img_chunks_sizes, merged_task_name, img_nb_blocks_per_dim, slices):
        """
        proxy_array_part_targeted: tuple (x, y, z) from (proxy_array_name, x, y, z)
        """
        _, _, start_of_block, _ = merged_task_name.split('-')

        # convert 3d pos in image to 3d pos in merged block
        num_pos = _3d_to_numeric_pos(proxy_array_part_targeted, img_nb_blocks_per_dim, order='C') # TODO à remove car on le fait deja avant
        num_pos_in_merged = num_pos - int(start_of_block)
        proxy_array_part_in_merged = numeric_to_3d_pos(num_pos_in_merged, img_nb_blocks_per_dim, order='C')

        _slice = proxy_array_part_in_merged

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
                
        return (slice(start[0], stop[0], None),
                slice(start[1], stop[1], None),
                slice(start[2], stop[2], None))


    def get_slice_from_merged_task_name(merged_task_name, target_slice, img_nb_blocks_per_dim):
        _, _, start_of_block, _ = merged_task_name.split('-')
        
        sot = [s.start for s in target_slice]
        sot = _3d_to_numeric_pos(sot, img_nb_blocks_per_dim, order='C')
        sot = sot - start_of_block

        eot = [s.stop + 1 for s in target_slice]
        eot = _3d_to_numeric_pos(eot, img_nb_blocks_per_dim, order='C')
        eot = eot - start_of_block

        return (slice(sot[0], eot[0], None), 
                slice(sot[1], eot[1], None), 
                slice(sot[2], eot[2], None))

    if config.get("io-optimizer.mode") == 'rechunked':
        rechunk_task_keys = proxy_arrays_to_task_names_dict[proxy_array_name]
        rechunk_dicts = [(rechunk_task_key, dask_graph[rechunk_task_key]) for rechunk_task_key in rechunk_task_keys]                       

        for rechunk_task_key, rechunk_dict in rechunk_dicts:
            replace_proxy_array_by_merged_proxy(rechunk_dict, proxy_array_name)
    else:
        getitem_task_names = proxy_arrays_to_task_names_dict[proxy_array_name]
        # dict with key = proxy_array_name, val = tasks using this proxy_array
        getitem_dicts = [(getitem_task_name, dask_graph[getitem_task_name]) for getitem_task_name in getitem_task_names] 
        for task_name, getitem_dict in getitem_dicts:
            print("processing", task_name, "for load", load)
            subtasks = get_tasks_targeting_proxy(getitem_dict, proxy_array_name)
            for kv_pair in subtasks: 
                getitem_key, value = kv_pair
                get_func, _, slices = value # TODO replace _ by proxy_key and replace value[1] below by proxy_key
                slices_key = tuple(value[1][1:])
                num_pos = _3d_to_numeric_pos(slices_key, img_nb_blocks_per_dim, order='C')
                
                if num_pos in load:
                    #print("\tbeing treated", value[1])
                    if np.all([tuple([sl.start, sl.stop]) == (None, None) for sl in slices]):
                        proxy_array_part_targeted = value[1][1:]
                        slice_of_interest = get_slice_from_proxy_part_key(proxy_array_part_targeted, 
                                                                        img_chunks_sizes, 
                                                                        merged_task_name, 
                                                                        img_nb_blocks_per_dim,
                                                                        None) # TODO a modifier
                    else:
                        raise ValueError("TODO!")
                        #slice_of_interest = get_slice_from_merged_task_name(merged_task_name, slices, img_nb_blocks_per_dim)
                    new_value = (get_func, (merged_task_name, 0, 0, 0), slice_of_interest)
                    getitem_dict[getitem_key] = new_value # replace
    return


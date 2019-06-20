__all__ = ("get_slices_from_dask_graph", "get_slices_from_rechunk_keys", "get_slices_from_getitem_keys", "get_slices_from_rechunk_subkeys", 
"get_slices_from_getitem_subkeys", "get_slices_from_getitem_subkeys", "test_source_key", "get_rechunk_subkeys", "get_keys_from_graph",
"add_or_create_to_list_dict")

# TODO generalize it to a graph/tree search
def get_slices_from_dask_graph(graph):
    keys_dict = get_keys_from_graph(graph)
    slices_dict = dict()
    deps_dict = dict()

    if 'rechunk-merge' in list(keys_dict.keys()):
        rechunk_keys = keys_dict['rechunk-merge']
        s1, d1 = get_slices_from_rechunk_keys(graph, rechunk_keys)
        slices_dict.update(s1)
        deps_dict.update(d1)
        
    if 'getitem' in list(keys_dict.keys()):
        getitem_keys = keys_dict['getitem']

        s2, d2 = get_slices_from_getitem_keys(graph, getitem_keys)
        
        slices_dict.update(s2)
        deps_dict.update(d2)

    return slices_dict, deps_dict


# get slices from keys
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


def get_slices_from_getitem_keys(graph, getitem_keys):
    global_slices_dict = dict()
    global_deps_dict = dict()
    for getitem_key in getitem_keys:
        getitem_graph = graph[getitem_key]
        local_slices_dict, local_deps_dict = get_slices_from_getitem_subkeys(getitem_graph)
        
        for k, v in local_slices_dict.items():
            if not k in list(global_slices_dict.keys()):
                global_slices_dict.update(local_slices_dict)
            else:
                global_slices_dict[k] = global_slices_dict[k] + local_slices_dict[k]

        for k, v in local_deps_dict.items():
            if not k in list(global_deps_dict.keys()):
                global_deps_dict.update(local_deps_dict)
            else:
                global_deps_dict[k] = global_deps_dict[k] + local_deps_dict[k]


    return global_slices_dict, global_deps_dict


# get slices from subkeys
def get_slices_from_rechunk_subkeys(rechunk_merge_graph, split_keys, merge_keys):

    def get_slices_from_splits(split_keys, slices_dict, deps_dict):
        
        for split_key in split_keys:
            split_value = rechunk_merge_graph[split_key]
            _, source_key, slices = split_value
            slices_dict, deps_dict = test_source_key(slices_dict, deps_dict, source_key, split_key)
        return slices_dict, deps_dict

    
    def recursive_search(_list, merge_key, slices_dict, deps_dict):
        if not isinstance(_list[0], tuple): # if it is not a list of targets
            for i in range(len(_list)):
                sublist = _list[i] 
                slices_dict, deps_dict = recursive_search(sublist, merge_key, slices_dict, deps_dict)
        else:
            for i in range(len(_list)):
                target_key = _list[i] 
                if 'array' in target_key[0]:
                    slices_dict, deps_dict = test_source_key(slices_dict, deps_dict, target_key, merge_key)
    
        return slices_dict, deps_dict


    def get_slices_from_merges2(rechunk_merge_graph, merge_keys, slices_dict, deps_dict):
        for merge_key in merge_keys:
            val = rechunk_merge_graph[merge_key]
            f, concat_list = val
            slices_dict, deps_dict = recursive_search(concat_list, merge_key, slices_dict, deps_dict)
        return slices_dict, deps_dict


    # TODO: make better
    """def get_slices_from_merges(merge_keys, slices_dict, deps_dict):
        for merge_key in merge_keys:
            merge_value = rechunk_merge_graph[merge_key]
            _, concat_list = merge_value
            while not isinstance(concat_list[0][0], tuple):
                concat_list = concat_list[0]
            for block in concat_list:
                for source_key in block:
                    if len(source_key) == 4:
                        slices_dict, deps_dict = test_source_key(slices_dict, deps_dict, source_key, merge_key)
        return slices_dict, deps_dict"""

    slices_dict = dict()
    deps_dict = dict()
    slices_dict, deps_dict = get_slices_from_splits(split_keys, slices_dict, deps_dict)

    slices_dict, deps_dict = get_slices_from_merges2(rechunk_merge_graph, merge_keys, slices_dict, deps_dict)
    return slices_dict, deps_dict


def get_slices_from_getitem_subkeys(getitem_graph):
    slices_dict = dict()
    deps_dict = dict()

    for k, v in getitem_graph.items():
        
        f, source_key, s = v 
        if isinstance(k[0], str) and "getitem" in k[0]:
            slices_dict, deps_dict = test_source_key(slices_dict, deps_dict, source_key, k)
    return slices_dict, deps_dict


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


#get subkeys
def get_rechunk_subkeys(rechunk_graph):
    keys_dict = get_keys_from_graph(rechunk_graph, printer=False)
    return keys_dict['rechunk-split'], keys_dict['rechunk-merge']


# get keys
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


# utility
def add_or_create_to_list_dict(d, k, v):
   
    if k not in list(d.keys()):
        d[k] = [v]
    else:
        d[k].append(v)
    return d
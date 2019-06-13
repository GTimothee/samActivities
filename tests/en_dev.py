
import sys


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


def test_source_key(slices_dict, source_key):
    """ test if source is an array proxy: if yes, add source key data to slices_dict
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
    return slices_dict


def get_slices_from_rechunk_subkeys(rechunk_merge_graph, split_keys, merge_keys):

    def get_slices_from_splits(split_keys, slices_dict):
        for split_key in split_keys:
            
            split_value = rechunk_merge_graph[split_key]
            _, source_key, slices = split_value
            slices_dict = test_source_key(slices_dict, source_key)
        return slices_dict

    # TODO: make better
    def get_slices_from_merges(merge_keys, slices_dict):
        for merge_key in merge_keys:
            merge_value = rechunk_merge_graph[merge_key]
            _, concat_list = merge_value
            while not isinstance(concat_list[0][0], tuple):
                concat_list = concat_list[0]
            for block in concat_list:
                for source_key in block:
                    if len(source_key) == 4:
                        slices_dict = test_source_key(slices_dict, source_key)
        return slices_dict

    slices_dict = dict()
    slices_dict = get_slices_from_splits(split_keys, slices_dict)
    slices_dict = get_slices_from_merges(merge_keys, slices_dict)
    return slices_dict


def get_slices_from_rechunk_keys(graph, rechunk_keys):
    global_slices_dict = dict()
    for rechunk_key in rechunk_keys:
        rechunk_graph = graph[rechunk_key]
        split_keys, merge_keys = get_rechunk_subkeys(rechunk_graph)
        local_slices_dict = get_slices_from_rechunk_subkeys(rechunk_graph, split_keys, merge_keys)
        global_slices_dict.update(local_slices_dict)
    return global_slices_dict


def get_slices_from_getitem_subkeys(getitem_graph):
    slices_dict = dict()
    for k, v in getitem_graph.items():
        f, source_key, s = v 
        slices_dict = test_source_key(slices_dict, source_key)
    return slices_dict


def get_slices_from_getitem_keys(graph, getitem_keys):
    global_slices_dict = dict()
    for getitem_key in getitem_keys:
        getitem_graph = graph[getitem_key]
        local_slices_dict = get_slices_from_getitem_subkeys(getitem_graph)
        global_slices_dict.update(local_slices_dict)
    return global_slices_dict


# TODO generalize it to a graph/tree search
def get_slices_from_dask_graph(graph):
    keys_dict = get_keys_from_graph(graph)
    
    rechunk_keys = keys_dict['rechunk-merge']
    getitem_keys = keys_dict['getitem']

    slices_dict = get_slices_from_rechunk_keys(graph, rechunk_keys)
    slices_dict.update(get_slices_from_getitem_keys(graph, getitem_keys))
    return slices_dict
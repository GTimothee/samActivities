
import sys

def get_rechunk_keys_lists_from_dask_array(graph, printer=False):
    """
    (k, rechunk_merge_key) : k is the key inside the merge graph, rechunk_merge_key is the key of the merge graph in the whole graph
    """
    def get_rechunkkeys_and_proxyarraykeys_from_dask_graph():
        proxy_array_keys = list()
        rechunk_merge_keys = list()
        for key in keys:
            if "array" in key:
                proxy_array_keys.append(key)
            elif "rechunk-merge" in key:
                rechunk_merge_keys.append(key)
            else:
                others.append(key)
        return proxy_array_keys, rechunk_merge_keys

    keys = list(graph.keys())
    others = list()


    proxy_array_keys, rechunk_merge_keys = get_rechunkkeys_and_proxyarraykeys_from_dask_graph()
    if len(rechunk_merge_keys) == 0:
        raise ValueError("no rechunk in this graph")

    rechunk_keys = list()
    for key in rechunk_merge_keys:
        rechunk_graph = graph[key]
        for k in list(rechunk_graph.keys()):
            if "rechunk-split" in k[0]:
                rechunk_keys.append((k, key))

    if printer:
        print(rechunk_keys)
        print(proxy_array_keys)
        print(others)
    return rechunk_keys, rechunk_graph, proxy_array_keys, others


def get_slices_from_rechunk_dict(rechunk_merge_graph):
    """
    -assumes that the rechunk merge dict only take data from a same proxy array
    """
    def get_slices_from_splits(rechunk_splits):
        target_name = None
        slices_list = list()
        for split_key in rechunk_splits:
            split_value = rechunk_merge_graph[split_key]
            _, array_vals, slices = split_value
            source, s1, s2, s3 = array_vals
            if 'array' in source:
                if not target_name:
                    target_name = source
                slices_list.append((s1, s2, s3))
        return target_name, slices_list

    def get_keys_by_type():

        rechunk_merges = list()
        rechunk_splits = list()
        for key in list(rechunk_merge_graph.keys()):
            if 'rechunk-merge' in key[0]:
                rechunk_merges.append(key)
            elif 'rechunk-split' in key[0]:
                rechunk_splits.append(key)
        return rechunk_merges, rechunk_splits

    rechunk_merges, rechunk_splits = get_keys_by_type()
    target_name, slices_list = get_slices_from_splits(rechunk_splits)

    """
    print("target_name", target_name)"""
    """for merge_key in rechunk_merges:
        merge_value = rechunk_merge_graph[merge_key]
        _, concat_list = merge_value
        l = concat_list
        while not isinstance(l[0][0], tuple):
            l = l[0]
        for block in l:
            for tupl in block:
                print(tupl)
                if 'array' in tupl[0]:
                    target_name, s1, s2, s3 = tupl
                    slices_list.append((s1, s2, s3))"""

    return target_name, slices_list


def get_getitem_keys_lists_from_dask_array(graph, printer=False):
    keys = list(graph.keys())
    getitem_keys = list()
    proxy_array_keys = list()
    actions = list()

    for k in keys:
        if "array" in k:
            proxy_array_keys.append(k)
        elif "getitem" in k:
            getitem_keys.append(k)
        else:
            actions.append(k)

    if printer:
        print(getitem_keys)
        print(proxy_array_keys)
        print(actions)
    return getitem_keys, proxy_array_keys, actions

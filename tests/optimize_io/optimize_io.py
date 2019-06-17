
import sys
import math

__all__ = ("main", "convert_slices_list_to_numeric_slices", "numeric_to_3d_pos", "_3d_to_numeric_pos", "get_slice_from_merged_task_name")


def main():
    """
        slices_dict :                       proxy-array -> list of slices
        array_to_original :                 proxy-array -> original-array
        original_array_chunks :             original-array-name -> block shapes (anciennement dims_dict) [shapes of each logical block]
        original_array_shapes :             original-array-name -> original-array shape
        original_array_blocks_shape :       original-array-name -> nb blocks in each dim
        deps_dict :                         proxy-array -> proxy-array-dependent tasks list
    """
    graph = None # TODO
    slices_dict = get_slices_from_dask_graph(graph)
    array_to_original, original_array_chunks, original_array_shapes, original_array_blocks_shape = get_arrays_dictionaries(graph, slices_dict)
    slices_dict = convert_slices_list_to_numeric_slices(slices_dict, original_array_chunks)
    graph = apply_clustered_strategy(graph, slices_dict, array_to_original, original_array_chunks, original_array_shapes, original_array_blocks_shape)


def convert_slices_list_to_numeric_slices(slices_dict, original_array_chunks):
    for proxy_array, slices_list in slices_dict.items():
        slices_list = sorted(list(set(slices_list)))
        array_dims = original_array_chunks[proxy_array]
        slices_list = [_3d_to_numeric_pos(s, array_dims, order='C') for s in slices_list]
    return slices_dict


def numeric_to_3d_pos(numeric_pos, shape, order):
    if order == 'F':  
        nb_blocks_per_row = shape[0]
        nb_blocks_per_slice = shape[0] * shape[1]
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
        nb_blocks_per_row = shape[0]
        nb_blocks_per_slice = shape[0] * shape[1]
    elif order == 'C':
        nb_blocks_per_row = shape[2]
        nb_blocks_per_slice = shape[1] * shape[2]
    else:
        raise ValueError("unsupported")
    return (_3d_pos[0] * nb_blocks_per_slice) + (_3d_pos[1] * nb_blocks_per_row) + _3d_pos[2] 


def get_slice_from_merged_task_name(merged_task_name, target_slice, img_nb_blocks_per_dim):
    """ not used for the moment
    """
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
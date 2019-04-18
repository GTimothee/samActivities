"""
processing 3d arrays only for the moment

2 paradimgs:
    -dask paradigm
    -neuroimaging paradigm

dask paradigm two cases:
    -1 big file
    -several chunks (more in the spirit of dask array usage using the specific formats of geo scientists)
"""


import dask.array as da
import h5py
import os
import time
import math
import random


def create_random_cube(storage_type, file_path, shape, axis=0):
    """ Generate random cubic array from normal distribution and store it on disk.
        storage_type: string
        shape: tuple or integer
        file_path: has to contain the filename if hdf5 type, should not contain a filename if npy type.
        axis: for numpy: splitting dimension because numpy store matrices
    """
    arr = da.random.normal(size=shape)
    if storage_type == "hdf5":
        da.to_hdf5(file_path, '/data', arr)
    elif storage_type == "numpy":
        da.to_npy_stack(os.path.join(file_path, "npy/"), arr, axis=axis)
    return


def get_dask_array_from_hdf5(file_path, key):
    """
    file path: path to hdf5 file (string)
    key: key of the dictionary to retrieve data
    """
    f = h5py.File(file_path, 'r')
    if len(list(f.keys())) > 0:
        return f[key]
    else:
        return


def get_random_cubic_block(nb_elements, arr, seed):
    random.seed(seed)
    dim_size = math.ceil(nb_elements**(1./3.))
    corner_index = [random.randint(0, arr.shape[i]-dim_size) for i in range(3)]
    return arr[corner_index[0]:corner_index[0] + dim_size,
               corner_index[1]:corner_index[1] + dim_size,
               corner_index[2]:corner_index[2] + dim_size]


def get_random_slab(nb_elements, arr, axis, seed):
    """
    axis: 0 (y-axis) = horizontal slab, 1 (x-axis) or 2 (z-axis) = vertical slab
    """
    random.seed(seed)
    complete_dims = [i for i in range(3) if i != axis] # get the other dimensions
    slab_area = arr.shape[complete_dims[0]] * arr.shape[complete_dims[1]]
    slab_width = math.ceil(nb_elements / slab_area)
    pos = random.randint(0, arr.shape[axis] - slab_width)

    if axis == 0:
        return arr[pos: pos + slab_width, :, :]
    elif axis == 1:
        return arr[:, pos: pos + slab_width, :]
    else:
        return arr[:, :, pos: pos + slab_width]


def get_random_rectangle_blocks(arr, shape, seed):
    """
    shape has to be a tuple containing the width of the rectangle in the three dimensions
    """
    random.seed(seed)
    pos = [random.randint(0, arr.shape[i] - shape[i]) for i in range(3)]
    return arr[pos[0]:pos[0] + shape[0],
               pos[1]:pos[1] + shape[0],
               pos[2]:pos[2] + shape[2]]


# dask paradigm
def load_array_parts(arr, geometry="slabs", nb_elements=0, shape=None, axis=0, random=True, seed=0, upper_corner=(0,0,0)):
    """ Load part of array.
    Load 1 or more parts of a too-big-for-memory array from file into memory.
    -given 1 or more parts (not necessarily close to each other)
    -take into account geometry
    -take into account storage type (unique_file or multiple_files) thanks to dask

    geometry: name of geometry to use
    nb_elements: nb elements wanted, not used for rectangle_blocks geometry
    shape: shape to use for rectangle_blocks
    axis: support axis for the slab
    random: if random cut or at a precise position. If set to False, upper_corner should be set.
    upper_corner: upper corner of the block/slab to be extracted (position from which to extract in the array).

    Returns a numpy array
    """
    if random:
        if geometry == "slabs":
            return get_random_slab(nb_elements, arr, axis, seed)
        elif geometry == "cubic_blocks":
            return get_random_cubic_block(nb_elements, arr, seed)
        elif geometry == "rectangle_blocks":
            return get_random_rectangle_blocks(arr, shape, seed)
    return


# dask paradigm
def overwrite(geometry):
    """ Rewrite the whole array with modifications.
    Load 1 or more part of a too-big-for-memory array from file, modify it and overwrite it.
    -given 1 or more parts (not necessarily close to each other)
    -take into account geometry
    -take into account storage type (unique_file or multiple_files) thanks to dask
    """
    pass


# dask paradigm
def resplit_arrray(in_geometry, out_geometry):
    """ Rewrite the array file chunks with an other geometry.
    Given a list of parts of array splitted in file chunks, resplit it with another geometry and write the result to files.
    Dask paradigm in the sense that there is still one image, but splitted in the sense of the file chunks of this image.
    """
    pass


# neuroimaging paradigm
def split_array_file(geometry):
    """
    Given a big file, split it into several files, following a given geometry.
    <=> naive split algorithm
    """
    pass


# neuroimaging paradigm
def resplit_array(in_geometry, out_geometry):
    """ Rewrite the array splits with an other geometry
    Given a too-big-for-memory array, resplit it.
    Neuroimaging paradigm in the sense that there is one file per image.
    Note to self: evaluate the different possible formats
    """
    pass


# neuroimaging paradigm
def merge(geometry):
    """ Write multiple files into a big array file.
    """
    pass

"""
traiter sur toute limage
un cas ou on est dans le meileur des cas et un cas ou on est dans le pire des cas
comment se passe la lecture en utilisant dask
impact du scheduler a la lecture

donc les chunks ont un impact direct sur les seeks du fait de lextraction
mais on veut pas modifier ça car on suppose que lapplication a ses raisons que ce soit
sous ce format de chunk donc du coup on veut voir limpact des seeks sur les perfs de daskgiven a geometry de chunk et une geometry de fichier (formar de stockage de données)

naive split avec une geometry coherente devrait etre plus rapide quavec une geometry random

processing 3d arrays only for the moment

2 paradimgs:
    -dask paradigm
    -neuroimaging paradigm

dask paradigm two cases:
    -1 big file
    -several chunks (more in the spirit of dask array usage using the specific formats of geo scientists)


note: créer deux fichiers en chunked pour voir les différences → (juste a titre informatif pour voir les diférences de vitesse,
 sachant que dans notre cas on utilisera les fichiers nii mais ca renseigne qd meme sur la portée de notre travail et pk pas prendre en compte)
"""


import dask.array as da
import h5py
import os
import time
import math
import random


def create_random_cube(storage_type, file_path, shape, axis=0, chunks_shape=None, dtype=None):
    """ Generate random cubic array from normal distribution and store it on disk.
        storage_type: string
        shape: tuple or integer
        file_path: has to contain the filename if hdf5 type, should not contain a filename if npy type.
        axis: for numpy: splitting dimension because numpy store matrices
        chunks_shape: for hdf5 only (as far as i am concerned for the moment)
    """
    arr = da.random.normal(size=shape)
    if dtype:
        arr = arr.astype(dtype)
    save_arr(arr, storage_type, file_path, key='/data', axis=axis, chunks_shape=chunks_shape)
    return


def save_arr(arr, storage_type, file_path, key='/data', axis=0, chunks_shape=None):
    """ Save array to hdf5 dataset or numpy file stack.
    """
    if storage_type == "hdf5":
        if chunks_shape:
            da.to_hdf5(file_path, key, arr, chunks=chunks_shape)
        else:
            da.to_hdf5(file_path, key, arr)
    elif storage_type == "numpy":
        da.to_npy_stack(os.path.join(file_path, "npy/"), arr, axis=axis)
    return


def get_dask_array_from_hdf5(file_path="tests/data/sample.hdf5", cast=True, key='/data'):
    """
    file path: path to hdf5 file (string)
    key: key of the dictionary to retrieve data
    cast: if you want to cast the dataset into a dask array.
        If you want to do it yourself (ex for adjusting the chunks),
        set this parameter to False.
    """
    f = h5py.File(file_path, 'r')
    if len(list(f.keys())) > 0:
        if not cast:
            return f[key]
        dataset = f[key]
        return da.from_array(dataset, chunks=dataset.chunks)
    else:
        print('Key not found. Aborting.')
        return


def get_random_cubic_block(nb_elements, arr, seed, random=True, corner_index=(0, 0, 0)):
    """ Get a cubic block which contains nb_elements from array arr at a random position.
    """
    dim_size = math.ceil(nb_elements**(1./3.))

    if random:
        random.seed(seed)
        corner_index = [random.randint(0, arr.shape[i]-dim_size) for i in range(3)]

    return arr[corner_index[0]:corner_index[0] + dim_size,
               corner_index[1]:corner_index[1] + dim_size,
               corner_index[2]:corner_index[2] + dim_size]


def get_random_slab(nb_elements, arr, axis, seed, random=True, pos=(0, 0)):
    """ Get a random slab which contains nb_elements from array arr at a random position.
    axis: 0 (y-axis) = horizontal slab, 1 (x-axis) or 2 (z-axis) = vertical slab
    """

    complete_dims = [i for i in range(3) if i != axis] # get the other dimensions
    slab_area = arr.shape[complete_dims[0]] * arr.shape[complete_dims[1]]
    slab_width = math.ceil(nb_elements / slab_area)

    if random:
        random.seed(seed)
        pos = random.randint(0, arr.shape[axis] - slab_width)

    if axis == 0:
        return arr[pos: pos + slab_width, :, :]
    elif axis == 1:
        return arr[:, pos: pos + slab_width, :]
    else:
        return arr[:, :, pos: pos + slab_width]


def get_random_right_cuboid(arr, shape, seed, random=True, pos=(0, 0, 0)):
    """ Get a random rectangle of shape 'shape' from array arr at a random position.
    shape has to be a tuple containing the width of the rectangle in the three dimensions
    """
    if random:
        random.seed(seed)
        pos = [random.randint(0, arr.shape[i] - shape[i]) for i in range(3)]

    return arr[pos[0]:pos[0] + shape[0],
               pos[1]:pos[1] + shape[1],
               pos[2]:pos[2] + shape[2]]


# dask paradigm
def load_array_parts(arr, geometry="slabs", nb_elements=0, shape=None, axis=0, random=True, seed=0, upper_corner=(0,0,0), as_numpy=False):
    """ Load part of array.
    Load 1 (or more parts -> one for the moment) of a too-big-for-memory array from file into memory.
    -given 1 or more parts (not necessarily close to each other)
    -take into account geometry
    -take into account storage type (unique_file or multiple_files) thanks to dask

    geometry: name of geometry to use
    nb_elements: nb elements wanted, not used for right_cuboid geometry
    shape: shape to use for right_cuboid
    axis: support axis for the slab
    random: if random cut or at a precise position. If set to False, upper_corner should be set.
    upper_corner: upper corner of the block/slab to be extracted (position from which to extract in the array).

    Returns a numpy array
    """
    if geometry not in ["slabs", "cubic_blocks", "right_cuboid"]:
        print("bad geometry type. Aborting.")
        return

    if geometry == "slabs":
        if random == False and len(upper_corner) != 2:
            print("Bad shape for upper corner: must be of dimension 2. Aborting.")
            return
        arr = get_random_slab(nb_elements, arr, axis, seed, random, pos=upper_corner)
    elif geometry == "cubic_blocks":
        arr = get_random_cubic_block(nb_elements, arr, seed, random, corner_index=upper_corner)
    elif geometry == "right_cuboid":
        arr = get_random_right_cuboid(arr, shape, seed, random, pos=upper_corner)

    if as_numpy:
        return arr.compute()
    else:
        return arr


# neuroimaging paradigm
def naive_split(input_file_path="/run/media/user/HDD 1TB/bbsamplesize.hdf5",
                geometry_shape=(770, 605, 700),
                rechunk=False,
                work_dir="/run/media/user/HDD 1TB/",
                unmatch_dims=False):
    """
    Given a big file, split it into several files, following a given geometry.
    <=> naive split algorithm

    unmatch_dims: set to true if you don't care of the mismatch of the geometry shape regarding the array shape
    """
    total_time = time.time()
    arr = get_dask_array_from_hdf5(file_path=input_file_path, cast=True, key='/data')
    if rechunk:
        arr = arr.rechunk((geometry_shape[0], geometry_shape[1], "auto"))

    arr_shape = arr.shape
    if not unmatch_dims:
        for a, g in zip(arr_shape, geometry_shape):
            if a % g != 0:
                print(str(a) + " % " + str(g) + " = " + str(a % g))
                print("Bad geometry shape, the array cannot be divided by this shape. Aborting.")
                return
        a = [int(arr_shape[0]/geometry_shape[0]),
             int(arr_shape[1]/geometry_shape[1]),
             int(arr_shape[2]/geometry_shape[2])]
    else:
        a = [math.floor(arr_shape[0]/geometry_shape[0]),
             math.floor(arr_shape[1]/geometry_shape[1]),
             math.floor(arr_shape[2]/geometry_shape[2])]

    IO_time = 0
    for i in range(a[0]):
        for j in range(a[1]):
            for k in range(a[2]):
                file_name = "split_part_{0}_{1}_{2}.hdf5".format(i, j, k)

                print("extracting " + file_name + "...")
                pos = (i * geometry_shape[0],
                       j * geometry_shape[1],
                       k * geometry_shape[2])
                split = load_array_parts(arr,
                                         geometry="right_cuboid",
                                         shape=geometry_shape,
                                         random=False,
                                         upper_corner=pos)

                print("saving " + file_name + "...")
                file_path = work_dir + file_name
                write_time = time.time()
                save_arr(split, "hdf5", file_path, key='/data', chunks_shape=None)
                write_time = time.time() - write_time
                IO_time += write_time

    total_time = time.time() - total_time
    return total_time, IO_time


# neuroimaging paradigm
def naive_merge(work_dir="/run/media/user/HDD 1TB/", prefix="split_part_", ask=False, rechunk=False):
    """ Write multiple files into a big array file.
    """
    def get_tuple_id(file_name, prefix):
        """ this function returns the position of the block file in the total array
        """
        strings = file_name.replace(prefix, "").split('_')
        integers = map(lambda s: int(s), strings)
        return tuple(integers)

    def get_max_dim(keys, dim):
        """ key = (x, y, z) position of the block file in the total array
        this function returns the number of blocks in a given dimension
        """
        return max([key[dim] for key in keys])

    total_time = time.time()

    file_names = {get_tuple_id(f.split('.')[0], prefix) : f for f in os.listdir(work_dir) if os.path.isfile(os.path.join(work_dir, f)) and prefix in f}
    keys = file_names.keys()
    i_max, j_max, k_max = (get_max_dim(keys, 0), get_max_dim(keys, 1), get_max_dim(keys, 2))

    data = list()
    for i in range(i_max + 1):
        stack_i = list()
        for j in range(j_max + 1):
            stack_j = list()
            for k in range(k_max + 1):
                file_name = file_names[(i, j, k)]
                arr_k = get_dask_array_from_hdf5(file_path=os.path.join(work_dir, file_name), cast=True, key='/data')
                if rechunk:
                    arr_k = arr_k.rechunk((arr_k.shape[0], arr_k.shape[1], "auto"))
                stack_j.append(arr_k)
            stack_i.append(stack_j)
        data.append(stack_i)

    arr = da.block(data)
    print("Output shape: " + str(arr.shape))

    if ask:
        while True:
            try:
                save = input("Do you want to proceed to the saving ? (y/n)")
                if save in ["y", "n"]:
                    break
            except ValueError:
                print("Invalid answer.")
                continue
    else:
        save = "y"

    if save == "y":
        print("start saving...")
        IO_time = time.time()
        save_arr(arr, "hdf5", work_dir + "merged.hdf5",
                 key='/data', chunks_shape=None)
        IO_time = time.time() - IO_time

    total_time = time.time() - total_time
    return total_time, IO_time


# neuroimaging paradigm
def resplit_array(in_geometry, out_geometry):
    """ Rewrite the array splits with an other geometry
    Given a too-big-for-memory array, resplit it.
    Neuroimaging paradigm in the sense that there is one file per image.
    Note to self: evaluate the different possible formats
    """
    pass

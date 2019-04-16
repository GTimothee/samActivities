"""
2 paradimgs:
    -dask paradigm
    -neuroimaging paradigm

dask paradigm two cases:
    -1 big file
    -several chunks (more in the spirit of dask array usage using the specific formats of geo scientists)
"""


from dask.array as da


# dask paradigm
def load_array_parts(geometry, storage_type):
    """ Load part of array.
    Load 1 or more parts of a too-big-for-memory array from file into memory.
    -given 1 or more parts (not necessarily close to each other)
    -take into account geometry
    -take into account storage type (unique_file or multiple_files)
    """
    # mydaskarray.compute() # returns a numy array
    pass


# dask paradigm
def overwrite(geometry, storage_type):
    """ Rewrite the whole array with modifications.
    Load 1 or more part of a too-big-for-memory array from file, modify it and overwrite it.
    -given 1 or more parts (not necessarily close to each other)
    -take into account geometry
    -take into account storage type (unique_file or multiple_files)
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

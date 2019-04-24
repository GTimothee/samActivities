from benchmark import *
from utils import create_random_cube

def main():
    # all ssd
    create_random_cube(storage_type="hdf5",
                       file_path="tests/data/bbsamplesize.hdf5",
                       shape=(1540,1210,1400),
                       chunks_shape=None,
                       dtype="float16")
    on_hdd_on_ssd()

if __name__ == "__main__":
    main()

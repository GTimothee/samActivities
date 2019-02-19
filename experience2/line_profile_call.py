import os, line_profiler, sys
import random
nb_repeats = 8

tmpfs_path="/dev/shm/book.txt"
ssd_path="./book.txt"
hdd_path="/mnt/hdd/book.txt"

def setup(filename):
    """
    Setup for rand_python_time
    """
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')
    file = open(filename, "r")
    filesize = os.stat(filename).st_size
    random.seed(0)
    return file, filesize

def rand_run(path):
    for i in range(nb_repeats):
        file,filesize = setup(path)

        for j in range(number):
            seek_dist = random.randint(0, filesize);
            file.seek(seek_dist)

        file.close()
    return times

def rand_(number):
    print("tmpfs")
    rand_run(number, tmpfs_path)
    print("ssd")
    rand_run(number, ssd_path)
    print("hdd")
    rand_run(number, hdd_path)

if __name__ == "__main__":
    rand_((int)sys.argv[1])

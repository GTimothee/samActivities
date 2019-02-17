import timeit, os, csv, time, line_profiler
import random
nb_repeats = 8

def setup(filename):
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')
    file = open(filename, "r")
    filesize = os.stat(filename).st_size
    random.seed(0)
    return file, filesize

@profile
def seeks_random_profile():

    print("tmpfs")

    for i in range(nb_repeats):
        file,filesize = setup('/dev/shm/history_of_maths.txt')

        for j in range(10000000):
            seek_dist = random.randint(0, filesize);
            file.seek(seek_dist)
        file.close()

    print("hdd")

    for i in range(nb_repeats):
        file,filesize = setup('/run/media/user/HDD 1TB/workspace/bigdataneurolab/history_of_maths.txt')

        for j in range(10000000):
            seek_dist = random.randint(0, filesize);
            file.seek(seek_dist)
        file.close()

def seeks_(seek_dist):
    setup = '''import os; \
    filename = '/dev/shm/history_of_maths.txt'; \
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches'); \
    file = open(filename, "r")'''
    stmt = "file.seek( " + str(seek_dist) + " )"

    times=timeit.repeat(setup=setup,
                    stmt=stmt,
                    number=10000000,
                    repeat=nb_repeats)

    print("tmpfs")
    for i, t in enumerate(times):
        out_writer.writerow(["ram", "tmpfs", "seeks_", str(i), str(seek_dist), str(t)])

    setup = '''import os; \
    filename = '/run/media/user/HDD 1TB/workspace/bigdataneurolab/history_of_maths.txt'; \
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches'); \
    file = open(filename, "r")'''

    times=timeit.repeat(setup=setup,
                    stmt=stmt,
                    number=10000000,
                    repeat=nb_repeats)

    print("hdd")
    for i, t in enumerate(times):
        out_writer.writerow(["hdd", "ext4", "seeks_", str(i), str(seek_dist), str(t)])

def random_():
    setup = '''import os, random; \
    filename = '/dev/shm/history_of_maths.txt'; \
    filesize = os.stat(filename).st_size; \
    random.seed(0)'''
    stmt = '''seek_dist = random.randint(0, filesize)'''

    times=timeit.repeat(setup=setup,
                    stmt=stmt,
                    number=10000000,
                    repeat=nb_repeats)

    print("random")
    for i, t in enumerate(times):
        out_writer.writerow(["","", "random", str(i), "", str(t)])

def seeks_random():
    setup = '''import os, random; \
    filename = '/dev/shm/history_of_maths.txt'; \
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches'); \
    file = open(filename, "r"); \
    filesize = os.stat(filename).st_size; \
    random.seed(0)'''
    stmt = '''seek_dist = random.randint(0, filesize); \
    file.seek(seek_dist)'''

    times=timeit.repeat(setup=setup,
                    stmt=stmt,
                    number=10000000,
                    repeat=nb_repeats)

    print("tmpfs")
    for i, t in enumerate(times):
        out_writer.writerow(["ram", "tmpfs", "seeks_random", str(i), "", str(t)])

    setup2 = '''import os, random; \
    filename = '/run/media/user/HDD 1TB/workspace/bigdataneurolab/history_of_maths.txt'; \
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches'); \
    file = open(filename, "r"); \
    filesize = os.stat(filename).st_size; \
    random.seed(0)'''

    times=timeit.repeat(setup=setup2,
                    stmt=stmt,
                    number=10000000,
                    repeat=nb_repeats)

    print("hdd")
    for i, t in enumerate(times):
        out_writer.writerow(["hdd", "ext4", "seeks_random", str(i), "", str(t)])

def seek_and_(seek_dist, isread):
    function = 'write("Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit")'
    mode="w"
    func_name = "write"
    if isread:
        function = 'read(10000)'
        mode="r"
        func_name = "read"

    setup = "import os; \
    filename = '/dev/shm/history_of_maths.txt'; \
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches'); \
    file = open(filename, '" + mode + "')"

    stmt = "file.seek( " + str(seek_dist) + " ); \
    file." + function + ";\
    file.seek(0);"

    times=timeit.repeat(setup=setup,
                    stmt=stmt,
                    number=10000000,
                    repeat=nb_repeats)

    print("tmpfs")
    for i, t in enumerate(times):
        out_writer.writerow(["ram", "tmpfs", "seek_and_" + func_name, str(i), str(seek_dist), str(t)])

    setup = "import os; \
    filename = '/run/media/user/HDD 1TB/workspace/bigdataneurolab/history_of_maths.txt'; \
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches'); \
    file = open(filename, '" + mode + "')"

    times=timeit.repeat(setup=setup,
                    stmt=stmt,
                    number=10000000,
                    repeat=nb_repeats)

    print("hdd")
    for i, t in enumerate(times):
        out_writer.writerow(["hdd", "ext4", "seek_and_" + func_name, str(i), str(seek_dist), str(t)])


if __name__ == "__main__":
    seeks_random_profile()

    """
    csv_file = open("output.csv", "w+")
    out_writer = csv.writer(csv_file)
    out_writer.writerow(["hardware_type", "file_system", "function", "iteration", "seek_distance", "time"])#write header


    print("RANDOM")
    random_()

    print("RANDOM SEEKS")
    seeks_random()

    print("READ")
    for seek_dist in [5000, 50000, 100000, 500000]:
        print('seek dist :', seek_dist)
        seek_and_(seek_dist, True)

    print("WRITE")
    for seek_dist in [5000, 50000, 100000, 500000]:
        print('seek dist :', seek_dist)
        seek_and_(seek_dist, False)

    print("SEEKS")
    for seek_dist in [0, 100, 1000, 5000, 10000, 50000, 100000, 500000]:
        print('seek dist :', seek_dist)
        seeks_(seek_dist)
    """

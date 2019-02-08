import timeit, random, os

nb_repeats = 8

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
    print('tmpfs')
    print(times)

    setup = '''import os; \
    filename = '/run/media/user/HDD 1TB/workspace/bigdataneurolab/history_of_maths.txt'; \
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches'); \
    file = open(filename, "r")'''

    times=timeit.repeat(setup=setup,
                    stmt=stmt,
                    number=10000000,
                    repeat=nb_repeats)
    print('hdd')
    print(times)

def random():
    setup = '''import os, random; \
    filename = '/dev/shm/history_of_maths.txt'; \
    filesize = os.stat(filename).st_size; \
    random.seed(0)'''
    stmt = '''seek_dist = random.randint(0, filesize)'''

    times=timeit.repeat(setup=setup,
                    stmt=stmt,
                    number=10000000,
                    repeat=nb_repeats)
    print('time for random')
    print(times)

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
    print('tmpfs')
    print(times)

    setup = '''import os, random; \
    filename = '/run/media/user/HDD 1TB/workspace/bigdataneurolab/history_of_maths.txt'; \
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches'); \
    file = open(filename, "r"); \
    filesize = os.stat(filename).st_size; \
    random.seed(0)'''

    times=timeit.repeat(setup=setup,
                    stmt=stmt,
                    number=10000000,
                    repeat=nb_repeats)
    print('hdd')
    print(times)

def seek_and_(seek_dist, isread):
    function = 'write("Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit")'
    mode="w"
    if isread:
        function = 'read(10000)'
        mode="r"

    setup = "import os; \
    filename = '/dev/shm/history_of_maths.txt'; \
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches'); \
    file = open(filename, '" + mode + "')"

    stmt = "file.seek( " + str(seek_dist) + " ); \
    file." + function + ";\
    file.seek(0)"

    times=timeit.repeat(setup=setup,
                    stmt=stmt,
                    number=10000000,
                    repeat=nb_repeats)
    print('tmpfs')
    print(times)

    setup = "import os; \
    filename = '/run/media/user/HDD 1TB/workspace/bigdataneurolab/history_of_maths.txt'; \
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches'); \
    file = open(filename, '" + mode + "')"

    times=timeit.repeat(setup=setup,
                    stmt=stmt,
                    number=10000000,
                    repeat=nb_repeats)
    print('hdd')
    print(times)


if __name__ == "__main__":
    #print("RANDOM")
    #random()

    #print("RANDOM SEEKS")
    #seeks_random()

    """print("READ")
    for seek_dist in [5000, 50000, 100000, 500000]:
        print('seek dist :', seek_dist)
        seek_and_(seek_dist, True)"""

    print("WRITE")
    for seek_dist in [5000, 50000, 100000, 500000]:
        print('seek dist :', seek_dist)
        seek_and_(seek_dist, False)


    """
    print("SEEKS")
    for seek_dist in [0, 100, 1000, 5000, 10000, 50000, 100000, 500000]:
        print('seek dist :', seek_dist)
        seeks_(seek_dist)"""

import timeit, os, csv, time, random
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

def rand_python_time_run(number, path):
    """
    Using python's time function and making individual calls
    """
    times=list()
    for i in range(nb_repeats):
        file,filesize = setup(path)
        total_time = 0.0
        for j in range(number):
            seek_dist = random.randint(0, filesize);
            t=time.time()
            file.seek(seek_dist)
            t=time.time()-t
            total_time+=t
        total_time/=number
        times.append(total_time)
        file.close()
    return times

def rand_python_time(number):
    """
    Using python's time function and making individual calls
    """
    print("tmpfs")
    times = rand_python_time_run(number, tmpfs_path)
    for i, t in enumerate(times):
        out_writer.writerow(["ram", "tmpfs", "rand_python_time", str(i), str(number), "", str(t)])
    print("ssd")
    times = rand_python_time_run(number, ssd_path)
    for i, t in enumerate(times):
        out_writer.writerow(["ssd", "ext4", "rand_python_time", str(i), str(number), "", str(t)])
    print("hdd")
    times = rand_python_time_run(number, hdd_path)
    for i, t in enumerate(times):
        out_writer.writerow(["hdd", "ext4", "rand_python_time", str(i), str(number), "", str(t)])

def timeit_seeks_run(seek_dist, number, path):
    """
    Run a given number of seek syscall for a given seek distance
    """
    setup = '''import os; \
    filename = "''' + path + '''"; \
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches'); \
    file = open(filename, "r")'''
    stmt = "file.seek( " + str(seek_dist) + " )"

    return timeit.repeat(setup=setup,
                    stmt=stmt,
                    number=number,
                    repeat=nb_repeats)

def timeit_seeks_(seek_dist, number):
    print("tmpfs")
    times = timeit_seeks_run(seek_dist, number, tmpfs_path)
    for i, t in enumerate(times):
        out_writer.writerow(["ram", "tmpfs", "timeit_seeks_", str(i), str(number), str(seek_dist), str(t)])

    print("ssd")
    times = timeit_seeks_run(seek_dist, number, ssd_path)
    for i, t in enumerate(times):
        out_writer.writerow(["ssd", "ext4", "timeit_seeks_", str(i), str(number), str(seek_dist), str(t)])

    print("hdd")
    times = timeit_seeks_run(seek_dist, number, hdd_path)
    for i, t in enumerate(times):
        out_writer.writerow(["hdd", "ext4", "timeit_seeks_", str(i), str(number), str(seek_dist), str(t)])

def timeit_random_(number):
    setup = '''import os, random; \
    filename = "''' + tmpfs_path + '''"; \
    filesize = os.stat(filename).st_size; \
    random.seed(0)'''
    stmt = '''seek_dist = random.randint(0, filesize)'''

    times=timeit.repeat(setup=setup,
                    stmt=stmt,
                    number=number,
                    repeat=nb_repeats)

    print("random")
    for i, t in enumerate(times):
        out_writer.writerow(["","", "timeit_random_", str(i), str(number), "", str(t)])

def timeit_seeks_random_run(number,path):
    setup = '''import os, random; \
    filename = "''' + path + '''"; \
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches'); \
    file = open(filename, "r"); \
    filesize = os.stat(filename).st_size; \
    random.seed(0)'''
    stmt = '''seek_dist = random.randint(0, filesize); \
    file.seek(seek_dist)'''

    return timeit.repeat(setup=setup,
                    stmt=stmt,
                    number=number,
                    repeat=nb_repeats)

def timeit_seeks_random(number):
    print("tmpfs")
    times = timeit_seeks_random_run(number, tmpfs_path)
    for i, t in enumerate(times):
        out_writer.writerow(["ram", "tmpfs", "timeit_seeks_random", str(i), str(number), "", str(t)])

    print("ssd")
    times = timeit_seeks_random_run(number, ssd_path)
    for i, t in enumerate(times):
        out_writer.writerow(["ssd", "ext4", "timeit_seeks_random", str(i), str(number), "", str(t)])

    print("hdd")
    times = timeit_seeks_random_run(number, hdd_path)
    for i, t in enumerate(times):
        out_writer.writerow(["hdd", "ext4", "timeit_seeks_random", str(i), str(number), "", str(t)])

def timeit_seek_and_run(seek_dist, number, isread, path):
    function = 'write("Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet,Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velitNeque porro quisquam est qui dolorem ipsum quia dolor sit amet.")'

    mode="w"
    func_name = "write"
    if isread:
        function = 'read(10000)'
        mode="r"
        func_name = "read"

    setup = "import os; \
    filename ='" + path + "'; \
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches'); \
    file = open(filename, '" + mode + "')"

    stmt = "file.seek( " + str(seek_dist) + " ); \
    file." + function + ";\
    file.seek(0);"

    return timeit.repeat(setup=setup,
                    stmt=stmt,
                    number=number,
                    repeat=nb_repeats)

def timeit_seek_and_(seek_dist, number, isread):
    func_name="write"
    if isread:
        func_name="read"

    print("tmpfs")
    times = timeit_seek_and_run(seek_dist, number, isread, tmpfs_path)
    for i, t in enumerate(times):
        out_writer.writerow(["ram", "tmpfs", "timeit_seek_and_"+func_name, str(i), str(number), str(seek_dist), str(t)])

    print("ssd")
    times = timeit_seek_and_run(seek_dist, number, isread, ssd_path)
    for i, t in enumerate(times):
        out_writer.writerow(["ssd", "ext4", "timeit_seek_and_"+func_name, str(i), str(number), str(seek_dist), str(t)])

    print("hdd")
    times = timeit_seek_and_run(seek_dist, number, isread, hdd_path)
    for i, t in enumerate(times):
        out_writer.writerow(["hdd", "ext4", "timeit_seek_and_"+func_name, str(i), str(number), str(seek_dist), str(t)])

def bench_read_speed_run(number, nb_bytes, path):
    function = 'read(' + str(nb_bytes) +')'
    mode="rb"

    filesize = os.stat(path).st_size; \

    setup = "import os, random; \
    filename ='" + path + "'; \
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches'); \
    file = open(filename, '" + mode + "')"

    stmt = '''seek_dist = random.randint(0, ''' + str(filesize-nb_bytes) + '''); \
    file.seek(seek_dist); \
    file.''' + function + ''';\
    file.seek(0);'''

    return timeit.repeat(setup=setup,
                    stmt=stmt,
                    number=number,
                    repeat=5)

def bench_read_speed():
    for nb_bytes in [10000]:
        print(nb_bytes)
        for number in [500000, 1000000, 5000000, 10000000]:
            print(number)
            print("ssd")
            times = bench_read_speed_run(number, nb_bytes, '/home/tim/data/bigbrain_40microns.nii.gz')
            for i, t in enumerate(times):
                out_writer.writerow(["ssd", "ext4", "bench_read_speed"+str(nb_bytes), str(i), str(number), "", str(t)])

            print("hdd")
            times = bench_read_speed_run(number, nb_bytes, '/mnt/hdd/bigbrain.nii.gz')
            for i, t in enumerate(times):
                out_writer.writerow(["hdd", "ext4", "bench_read_speed"+str(nb_bytes), str(i), str(number), "", str(t)])

def run_bench(number):
    """
    Run all functions the one after the other
    """

    print("PYTHON RAND TIME")
    rand_python_time(number)

    print("RANDOM")
    timeit_random_(number)

    print("RANDOM SEEKS")
    timeit_seeks_random(number)

    print("READ")
    for seek_dist in [5000, 50000, 500000]:
        print('seek dist :', seek_dist)
        timeit_seek_and_(seek_dist, number, True)

    print("WRITE")
    for seek_dist in [5000, 50000, 500000]:
        print('seek dist :', seek_dist)
        timeit_seek_and_(seek_dist, number, False)

    print("SEEKS")
    for seek_dist in [0, 5000, 50000, 500000]:
        print('seek dist :', seek_dist)
        timeit_seeks_(seek_dist, number)

if __name__ == "__main__":
    csv_file = open("profile_call_py_benchreadspeed_out.csv", "w+")
    out_writer = csv.writer(csv_file)
    out_writer.writerow(["hardware_type", "file_system", "function", "iteration", "nb_runs", "seek_distance", "time"])

    """print("PYTHON RAND TIME")
    rand_python_time(1)

    print("PYTHON RAND TIME")
    rand_python_time(10)

    print("PYTHON RAND TIME")
    rand_python_time(100)

    run_bench(1000000)
    run_bench(10000000)"""

    bench_read_speed()

import sys

#faire un grand nombre de seek de python sur ssd, hdd, tmpfs
def seek():
    with open(sys.argv[1]) as file:
        for i in range(10000000):
            file.seek(0)

#faire un grand nombre de seek de python sur ssd, hdd, tmpfs avec des offset différents tirés au hasard

#faire des reads/write sur les trois supports

if __name__ == "__main__":
    seek()

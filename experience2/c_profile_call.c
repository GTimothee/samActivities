#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

enum Spec {TMPFS = 0, HDD = 1, SSD = 2};
char * file_names[3] = {"/dev/shm/book.txt","/mnt/hdd/book.txt","./book.txt"};

typedef struct Profiles{
    double pread_t;
    double seek_and_read_t;
}profiles;

double profile_seek_and_read(int fd){
    clock_t t;
    char buffer[120];
    int offset = 0;

    t = clock();
    off_t of = lseek(fd, offset, SEEK_SET);
    ssize_t nr = read(fd, buffer, 120);
    t = clock() - t;
    return ((double)t)/CLOCKS_PER_SEC;
}

double profile_pread(int fd){
    clock_t t;
    char buffer[120];
    int offset = 0;

    t = clock();
    ssize_t nr = pread(fd, buffer, 120, offset);
    t = clock() - t;
    return ((double)t)/CLOCKS_PER_SEC;

}

profiles * profile_it(int nb_its,int spec){
    profiles * prof = malloc(sizeof(profiles));
    char * file_name = file_names[spec];
    int fd = open(file_name,O_RDONLY);

    prof->pread_t = 0.0;
    prof->seek_and_read_t=0.0;

    if(fd>=0){
        for(int j =0; j<nb_its; j++){
            prof->pread_t+=profile_pread(fd);
            prof->seek_and_read_t+=profile_seek_and_read(fd);
        }

        close(fd);
        return prof;
    }
    printf("nothing happend, fd=%d", fd);
    return NULL;
}

void write_output(profiles** profs, int nb_its, int spec){
    FILE * file = fopen("c_profile_call_pread.csv","w+");
    fprintf(file, "pread_time, seek_and_read_time, file_path\n");
    for(int k=0; k<nb_its;k++){
        fprintf(file, "%f, %f, %s\n", profs[k]->pread_t, profs[k]->seek_and_read_t, file_names[spec]);
        free(profs[k]);
    }
    profs=NULL;
    fclose(file);
}

int time_profiler(int nb_its, int spec){
    profiles * prof=NULL;
    profiles ** profs = malloc(nb_its*sizeof(profiles*));
    for(int i=0;i<nb_its;i++){
        prof = profile_it(10000000, spec);
        if(!prof)
            return -1;
        profs[i]=prof;
        printf("pread time: %f\n", prof->pread_t);
        printf("seek + read time: %f\n", prof->seek_and_read_t);
    }

    write_output(profs,nb_its, spec);
    return 0;
}

int main(int argc, char** argv){
    int nb_its=5;
    return time_profiler(nb_its, TMPFS);
}

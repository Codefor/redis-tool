#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#define BUF 1024*16

int main(int argc, char *argv[]) {
    if( argc < 3 ){  
        printf("usage: %s %s\n", argv[0], "infile outfile");  
        return -1;
    }  

    int fd = open(argv[2], O_CREAT|O_WRONLY, 0644);
    if (fd == -1) return -1;

    char* REDIS_HEAD = "REDIS0002";
    unsigned char REDIS_SELEBTDB = 254;
    unsigned char REDIS_DB = 0;
    unsigned char REDIS_STRING = 0;
    unsigned char REDIS_EOF = 255;

    write(fd, REDIS_HEAD, strlen(REDIS_HEAD)); // redis_header db num
    write(fd, &REDIS_SELEBTDB, 1);
    write(fd, &REDIS_DB, 1);

    int r_fd = open(argv[1], O_RDONLY); // 读src文件，写数据文件
    char buf[BUF] = {0};
    char wbuf[BUF] = {0};
    while (1) {
        //memset(buf, BUF, 0);
        //memset(wbuf, BUF, 0);
        int r = read(r_fd, buf, BUF-1);
        if (r <= 0) break;

        char * pw = wbuf;
        char * pk = buf; // key pointer
        char * pv = buf; // val pointer

        int nidx = 0; // next line idx
        int wlen = 0;
        int i;
        for (i = 0; i < r; ++i) {
            if (buf[i] == '\t') {
                int len = buf + i - pk;
                memcpy(pw++, &REDIS_STRING, 1); // string type
                memcpy(pw++, &len, 1); // key len
                memcpy(pw, pk, len); // key
                pw += len;

                pv = buf + i + 1;
            } else if (buf[i] == '\n') {
                nidx = i;
                int len = buf + i - pv;
                memcpy(pw++, &len, 1); // val len
                memcpy(pw, pv, len); // val
                pw += len;

                wlen = pw - wbuf;
                pk = buf + i + 1;
            }
        }

        write(fd, wbuf, wlen);
        //memset(wbuf, BUF, 0);
        lseek(r_fd, nidx - r + 1, SEEK_CUR);
    }
    write(fd, &REDIS_EOF, 1); // EOF

    close(r_fd);
    close(fd);
    return 0;
}

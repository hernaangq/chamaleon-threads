#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <inttypes.h>
#include <time.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <omp.h>
#include "blake3.h"

#define NONCE_SIZE 6
#define HASH_SIZE  10
#define RECORD_SIZE (HASH_SIZE + NONCE_SIZE)

typedef struct {
    uint8_t hash[HASH_SIZE];
    uint8_t nonce[NONCE_SIZE];
} Record;

typedef struct {
    char *approach;
    int threads;
    int iothreads;
    int k;
    int memory_mb;
    char *file_temp;
    char *file_final;
    int debug;
    int print_num;
    int search_num;
    int difficulty;
    int verify;
} Config;

Config cfg = {0};
uint64_t total_records, chunk_size, rounds;

double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec * 1e-6;
}

void print_hex(const uint8_t *data, size_t len) {
    for (size_t i = 0; i < len; i++) printf("%02x", data[i]);
}

uint64_t nonce_to_u64(const uint8_t *nonce) {
    uint64_t v = 0;
    memcpy(&v, nonce, NONCE_SIZE);
    return v;
}

int is_zero_nonce(const uint8_t *nonce) {
    for (int i = 0; i < NONCE_SIZE; i++) if (nonce[i]) return 0;
    return 1;
}

void generate_hash(uint64_t nonce_val, Record *rec) {
    memcpy(rec->nonce, &nonce_val, NONCE_SIZE);
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, rec->nonce, NONCE_SIZE);
    blake3_hasher_finalize(&hasher, rec->hash, HASH_SIZE);
}

int record_cmp(const void *a, const void *b) {
    return memcmp(((Record*)a)->hash, ((Record*)b)->hash, HASH_SIZE);
}

/* ---------- I/O Worker ---------- */
typedef struct {
    int fd;
    off_t off;
    char *buf;
    size_t size;
} WriteJob;

void *write_worker(void *arg) {
    WriteJob *job = (WriteJob*)arg;
    (void)pwrite(job->fd, job->buf, job->size, job->off);  // Suppress warning
    free(job->buf);
    free(job);
    return NULL;
}

/* ---------- Task Worker (pthreads) ---------- */
typedef struct {
    uint64_t start;
    uint64_t count;
    Record *buf;
} HashTask;

void *hash_worker(void *arg) {
    HashTask *task = (HashTask*)arg;
    for (uint64_t i = 0; i < task->count; i++) {
        generate_hash(task->start + i, &task->buf[i]);
    }
    free(task);
    return NULL;
}

/* ---------- Generate + Sort + Write Chunk ---------- */
void generate_chunk(uint64_t start, uint64_t count, const char *tmpfile) {
    Record *buf = malloc(count * RECORD_SIZE);
    if (!buf) { perror("malloc"); exit(1); }

    if (!strcmp(cfg.approach, "for")) {
        #pragma omp parallel for num_threads(cfg.threads)
        for (uint64_t i = 0; i < count; i++) {
            generate_hash(start + i, &buf[i]);
        }
    } else {
        uint64_t per_thread = count / cfg.threads;
        pthread_t *tids = malloc(cfg.threads * sizeof(pthread_t));
        for (int t = 0; t < cfg.threads; t++) {
            uint64_t s = start + t * per_thread;
            uint64_t e = (t == cfg.threads - 1) ? count : (t + 1) * per_thread;
            HashTask *task = malloc(sizeof(HashTask));
            task->start = s; task->count = e - s; task->buf = buf + s;
            pthread_create(&tids[t], NULL, hash_worker, task);
        }
        for (int t = 0; t < cfg.threads; t++) pthread_join(tids[t], NULL);
        free(tids);
    }

    qsort(buf, count, RECORD_SIZE, record_cmp);

    int fd = open(tmpfile, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd < 0) { perror("open"); exit(1); }

    uint64_t per_io = count / cfg.iothreads;
    pthread_t *io_tids = malloc(cfg.iothreads * sizeof(pthread_t));
    for (int t = 0; t < cfg.iothreads; t++) {
        uint64_t s = t * per_io;
        uint64_t e = (t == cfg.iothreads - 1) ? count : (t + 1) * per_io;
        size_t size = (e - s) * RECORD_SIZE;
        off_t off = s * RECORD_SIZE;
        char *data = malloc(size);
        memcpy(data, (char*)buf + s * RECORD_SIZE, size);
        WriteJob *job = malloc(sizeof(WriteJob));
        job->fd = fd; job->off = off; job->buf = data; job->size = size;
        pthread_create(&io_tids[t], NULL, write_worker, job);
    }
    for (int t = 0; t < cfg.iothreads; t++) pthread_join(io_tids[t], NULL);
    free(io_tids);
    close(fd);
    free(buf);
}

/* ---------- Merge Chunks ---------- */
void merge_chunks() {
    int fd_out = open(cfg.file_final, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd_out < 0) { perror("open"); exit(1); }

    for (uint64_t r = 0; r < rounds; r++) {
        char tmpfile[256];
        snprintf(tmpfile, sizeof(tmpfile), "%s.%lu", cfg.file_temp, r);
        int fd_in = open(tmpfile, O_RDONLY);
        if (fd_in < 0) continue;

        off_t off = lseek(fd_out, 0, SEEK_END);
        char buf[1<<20];
        ssize_t n;
        while ((n = read(fd_in, buf, sizeof(buf))) > 0) {
            (void)pwrite(fd_out, buf, n, off);  // Suppress warning
            off += n;
        }
        close(fd_in);
        remove(tmpfile);
    }
    close(fd_out);
    remove(cfg.file_temp);
}

/* ---------- Generate Mode ---------- */
void mode_generate() {
    total_records = 1ULL << cfg.k;
    uint64_t mem_bytes = (uint64_t)cfg.memory_mb * 1024 * 1024;
    chunk_size = mem_bytes / RECORD_SIZE;
    if (chunk_size == 0) chunk_size = 1;
    rounds = (total_records + chunk_size - 1) / chunk_size;

    if (cfg.debug) {
        printf("Selected Approach           : %s\n", cfg.approach);
        printf("Number of Threads           : %d\n", cfg.threads);
        printf("Exponent K                  : %d\n", cfg.k);
        printf("File Size (GB)              : %.2f\n", (double)total_records * RECORD_SIZE / (1ULL<<30));
        printf("Memory Size (MB)            : %d\n", cfg.memory_mb);
        printf("Rounds                      : %lu\n", rounds);
        printf("Temporary File              : %s\n", cfg.file_temp);
        printf("Final Output File           : %s\n", cfg.file_final);
    }

    double t0 = get_time();
    for (uint64_t r = 0; r < rounds; r++) {
        uint64_t start = r * chunk_size;
        uint64_t count = (start + chunk_size > total_records) ? (total_records - start) : chunk_size;
        char tmpfile[256];
        snprintf(tmpfile, sizeof(tmpfile), "%s.%lu", cfg.file_temp, r);
        generate_chunk(start, count, tmpfile);
    }

    merge_chunks();

    double total_time = get_time() - t0;
    double mh = total_records / total_time / 1e6;
    double mb = total_records * RECORD_SIZE / total_time / 1048576.0;
    printf("Total Throughput: %.2f MH/s  %.2f MB/s\n", mh, mb);
    printf("Total Time: %.6f seconds\n", total_time);
}

/* ---------- Print Mode ---------- */
void mode_print(const char *filename, int n) {
    FILE *f = fopen(filename, "rb");
    if (!f) { perror("fopen"); exit(1); }
    Record r;
    for (int i = 0; i < n && fread(&r, RECORD_SIZE, 1, f); i++) {
        printf("[%d] stored: ", i * RECORD_SIZE);
        if (is_zero_nonce(r.nonce)) printf("BLANK nonce: BLANK\n");
        else {
            print_hex(r.hash, HASH_SIZE);
            printf(" nonce: %" PRIu64 "\n", nonce_to_u64(r.nonce));
        }
    }
    fclose(f);
}

/* ---------- Search Mode ---------- */
void mode_search(const char *filename) {
    struct stat st;
    if (stat(filename, &st)) { perror("stat"); exit(1); }
    FILE *f = fopen(filename, "rb");
    if (!f) { perror("fopen"); exit(1); }

    printf("searches=%d difficulty=%d\n", cfg.search_num, cfg.difficulty);
    printf("Parsed k                     : %d\n", cfg.k);
    printf("Nonce Size                   : %d\n", NONCE_SIZE);
    printf("Record Size                  : %d\n", RECORD_SIZE);
    printf("Hash Size                    : %d\n", HASH_SIZE);
    printf("On-disk Record Size          : %d\n", RECORD_SIZE);
    printf("Number of Buckets            : %llu\n", 1ULL << 24);
    printf("Number of Records in Bucket  : %llu\n", 1ULL << (cfg.k - 24));
    printf("Number of Hashes             : %llu\n", total_records);
    printf("File Size to be read (bytes) : %llu\n", total_records * RECORD_SIZE);
    printf("File Size to be read (GB)    : %.6f\n", (double)(total_records * RECORD_SIZE) / (1ULL << 30));
    printf("Actual file size on disk     : %lld bytes\n", (long long)st.st_size);

    srand(time(NULL));
    double total_time = 0, total_seeks = 0, total_comps = 0, total_matches = 0;
    int found = 0, notfound = 0;

    for (int q = 0; q < cfg.search_num; q++) {
        uint8_t prefix[10] = {0};
        for (int i = 0; i < cfg.difficulty; i++) prefix[i] = rand() & 0xFF;

        double t0 = get_time();
        uint64_t idx = 0;
        for (int i = 0; i < cfg.difficulty; i++) idx = (idx << 8) | prefix[i];
        idx = (idx * total_records) >> (cfg.difficulty * 8);
        off_t byte_off = idx * RECORD_SIZE;
        fseek(f, byte_off, SEEK_SET);

        int comps = 0, matches = 0;
        Record r;
        while (fread(&r, RECORD_SIZE, 1, f) == 1) {
            comps++;
            if (memcmp(r.hash, prefix, cfg.difficulty) != 0) break;
            if (cfg.debug) {
                printf("MATCH "); print_hex(r.hash, HASH_SIZE);
                printf(" %lu time=%.3f ms comps=%d\n", nonce_to_u64(r.nonce), (get_time() - t0) * 1000, comps);
            }
            matches++;
        }
        double t = (get_time() - t0) * 1000;

        total_time += t / 1000.0;
        total_seeks += 1;
        total_comps += comps;
        total_matches += matches;
        if (matches) found++; else notfound++;
    }
    fclose(f);

    double avg_ms = total_time * 1000 / cfg.search_num;
    double sps = cfg.search_num / total_time;
    printf("Search Summary: requested=%d performed=%d found_queries=%d total_matches=%d notfound=%d\n",
           cfg.search_num, cfg.search_num, found, (int)total_matches, notfound);
    printf("total_time=%.6f s avg_ms=%.3f ms searches/sec=%.3f total_seeks=%d\n",
           total_time, avg_ms, sps, (int)total_seeks);
    printf("avg_seeks_per_search=%.3f total_comps=%d avg_comps_per_search=%.3f\n",
           total_seeks * 1.0 / cfg.search_num, (int)total_comps, total_comps * 1.0 / cfg.search_num);
    printf("avg_matches_per_found=%.3f\n", found ? total_matches / found : 0);
}

/* ---------- Verify Mode ---------- */
void mode_verify(const char *filename) {
    struct stat st;
    if (stat(filename, &st)) { perror("stat"); exit(1); }
    FILE *f = fopen(filename, "rb");
    if (!f) { perror("fopen"); exit(1); }

    printf("verifying sorted order by bucket/index of final stored file...\n");
    printf("Size of '%s' is %lld bytes.\n", filename, (long long)st.st_size);

    Record prev = {0}, curr;
    int valid = 1;
    uint64_t i = 0;
    while (fread(&curr, RECORD_SIZE, 1, f)) {
        if (i > 0 && memcmp(prev.hash, curr.hash, HASH_SIZE) > 0) {
            printf("ERROR at record %lu: ", i);
            print_hex(prev.hash, HASH_SIZE); printf(" > "); print_hex(curr.hash, HASH_SIZE); printf("\n");
            valid = 0;
            break;
        }
        prev = curr;
        i++;
    }
    fclose(f);
    if (valid) printf("Sorted order verified: %lu records\n", i);
}

/* ---------- Main ---------- */
int main(int argc, char *argv[]) {
    cfg.approach = "for";
    cfg.threads = omp_get_max_threads();
    cfg.iothreads = 1;
    cfg.k = 20;
    cfg.memory_mb = 256;
    cfg.debug = 0;
    cfg.print_num = 0;
    cfg.search_num = 0;
    cfg.difficulty = 3;
    cfg.verify = 0;

    int opt;
    static struct option long_opts[] = {
        {"approach", 1, 0, 'a'}, {"threads", 1, 0, 't'}, {"iothreads", 1, 0, 'i'},
        {"exponent", 1, 0, 'k'}, {"memory", 1, 0, 'm'}, {"file", 1, 0, 'f'},
        {"file_final", 1, 0, 'g'}, {"debug", 1, 0, 'd'}, {"print", 1, 0, 'p'},
        {"search", 1, 0, 's'}, {"difficulty", 1, 0, 'q'}, {"verify", 1, 0, 'v'},
        {"help", 0, 0, 'h'}, {0,0,0,0}
    };

    while ((opt = getopt_long(argc, argv, "a:t:i:k:m:f:g:d:p:s:q:v:h", long_opts, NULL)) != -1) {
        switch (opt) {
            case 'a': cfg.approach = optarg; break;
            case 't': cfg.threads = atoi(optarg); break;
            case 'i': cfg.iothreads = atoi(optarg); break;
            case 'k': cfg.k = atoi(optarg); break;
            case 'm': cfg.memory_mb = atoi(optarg); break;
            case 'f': cfg.file_final = optarg; break;
            case 'g': cfg.file_temp = optarg; break;
            case 'd': cfg.debug = !strcmp(optarg, "true"); break;
            case 'p': cfg.print_num = atoi(optarg); break;
            case 's': cfg.search_num = atoi(optarg); break;
            case 'q': cfg.difficulty = atoi(optarg); break;
            case 'v': cfg.verify = !strcmp(optarg, "true"); break;
            case 'h':
                printf("Usage: ./vaultx -t 24 -i 1 -m 256 -k 26 -g memo.t -f memo.x\n");
                return 0;
        }
    }

    if (cfg.print_num > 0) {
        mode_print(cfg.file_final, cfg.print_num);
    } else if (cfg.search_num > 0) {
        mode_search(cfg.file_final);
    } else if (cfg.verify) {
        mode_verify(cfg.file_final);
    } else {
        if (!cfg.file_temp || !cfg.file_final) {
            fprintf(stderr, "Need -g and -f\n");
            return 1;
        }
        mode_generate();
    }

    return 0;
}
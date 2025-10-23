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
    ssize_t written = pwrite(job->fd, job->buf, job->size, job->off);
    if (written < 0) {
        perror("pwrite");
    } else if ((size_t)written != job->size) {
        fprintf(stderr, "Partial pwrite: expected %zu wrote %zd\n", job->size, written);
    }
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
ssize_t safe_pwrite_all(int fd, const void *buf, size_t size, off_t off) {
    const char *p = buf;
    size_t remaining = size;
    while (remaining > 0) {
        ssize_t w = pwrite(fd, p, remaining, off);
        if (w < 0) return -1;
        p += w;
        off += w;
        remaining -= w;
    }
    return (ssize_t)size;
}

void generate_chunk(uint64_t start, uint64_t count, const char *tmpfile) {
    /* allocate records using sizeof(Record) */
    Record *buf = malloc(count * sizeof(Record));
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
            uint64_t e = (t == cfg.threads - 1) ? (start + count) : (start + (t + 1) * per_thread);
            HashTask *task = malloc(sizeof(HashTask));
            task->start = s; task->count = e - s; task->buf = buf + (s - start);
            pthread_create(&tids[t], NULL, hash_worker, task);
        }
        for (int t = 0; t < cfg.threads; t++) pthread_join(tids[t], NULL);
        free(tids);
    }

    /* sort using sizeof(Record) */
    qsort(buf, count, sizeof(Record), record_cmp);

    /* write chunk to the temporary file (not directly to final) */
    int fd = open(tmpfile, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd < 0) { perror("open tmpfile"); close(fd); free(buf); exit(1); }
    ssize_t wrote = safe_pwrite_all(fd, buf, count * sizeof(Record), 0);
    if (wrote < 0) {
        perror("pwrite tmpfile");
        close(fd);
        free(buf);
        exit(1);
    }
    close(fd);
    free(buf);
}

/* ---------- Merge Chunks ---------- */
void merge_chunks() {
    int fd_out = open(cfg.file_final, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd_out < 0) { perror("open final"); exit(1); }

    /* open all tmp files */
    FILE **fps = calloc(rounds, sizeof(FILE*));
    int *indices = calloc(rounds, sizeof(int));
    int nfiles = 0;
    for (uint64_t r = 0; r < rounds; r++) {
        char tmpfile[256];
        snprintf(tmpfile, sizeof(tmpfile), "%s.%lu", cfg.file_temp, r);
        FILE *f = fopen(tmpfile, "rb");
        if (!f) continue;
        fps[nfiles++] = f;
        indices[nfiles-1] = (int)r; /* remember file index to remove later */
    }

    if (nfiles == 0) { close(fd_out); free(fps); free(indices); return; }

    /* heap node */
    typedef struct { Record rec; int src; } Node;

    /* simple binary min-heap by hash */
    Node *heap = malloc(nfiles * sizeof(Node));
    int heap_sz = 0;

    auto heap_swap = [](Node *a, Node *b) {
        Node t = *a; *a = *b; *b = t;
    };

    /* compare by hash */
    auto node_less = [](const Node *x, const Node *y) {
        int cmp = memcmp(x->rec.hash, y->rec.hash, HASH_SIZE);
        if (cmp != 0) return cmp < 0;
        /* tie-breaker by nonce to get deterministic order */
        return memcmp(x->rec.nonce, y->rec.nonce, NONCE_SIZE) < 0;
    };

    /* push initial record from each file */
    for (int i = 0; i < nfiles; i++) {
        if (fread(&heap[heap_sz].rec, sizeof(Record), 1, fps[i]) != 1) {
            fclose(fps[i]);
            fps[i] = NULL;
            continue;
        }
        heap[heap_sz].src = i;
        /* sift up */
        int ci = heap_sz++;
        while (ci > 0) {
            int pi = (ci - 1) >> 1;
            if (!node_less(&heap[ci], &heap[pi])) break;
            Node tmp = heap[ci]; heap[ci] = heap[pi]; heap[pi] = tmp;
            ci = pi;
        }
    }

    /* merge */
    while (heap_sz > 0) {
        /* pop min (root) */
        Node top = heap[0];
        /* write top.rec to output */
        const char *p = (const char*)&top.rec;
        size_t remaining = sizeof(Record);
        while (remaining > 0) {
            ssize_t w = write(fd_out, p, remaining);
            if (w < 0) { perror("write final"); goto cleanup; }
            p += w;
            remaining -= w;
        }

        int src = top.src;
        /* replace root with next from same source (or remove if EOF) */
        if (fps[src] && fread(&heap[0].rec, sizeof(Record), 1, fps[src]) == 1) {
            heap[0].src = src;
        } else {
            /* remove root by replacing with last element */
            heap[0] = heap[--heap_sz];
        }
        /* sift down root */
        int i = 0;
        while (1) {
            int l = (i << 1) + 1;
            int r = l + 1;
            int smallest = i;
            if (l < heap_sz && node_less(&heap[l], &heap[smallest])) smallest = l;
            if (r < heap_sz && node_less(&heap[r], &heap[smallest])) smallest = r;
            if (smallest == i) break;
            Node tmp = heap[i]; heap[i] = heap[smallest]; heap[smallest] = tmp;
            i = smallest;
        }
    }

cleanup:
    /* close and remove tmp files */
    for (int i = 0; i < nfiles; i++) {
        if (fps[i]) {
            fclose(fps[i]);
            char tmpfile[256];
            snprintf(tmpfile, sizeof(tmpfile), "%s.%lu", cfg.file_temp, (unsigned long)indices[i]);
            remove(tmpfile);
        } else {
            /* file was NULL because it couldn't be read initially; still try remove */
            char tmpfile[256];
            snprintf(tmpfile, sizeof(tmpfile), "%s.%lu", cfg.file_temp, (unsigned long)indices[i]);
            remove(tmpfile);
        }
    }

    close(fd_out);
    free(heap);
    free(fps);
    free(indices);
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
    printf("Rounds                      : %" PRIu64 "\n", rounds);
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
    printf("Number of Buckets            : %" PRIu64 "\n", (uint64_t)(1ULL << 24));
    printf("Number of Records in Bucket  : %" PRIu64 "\n", (uint64_t)(1ULL << (cfg.k - 24)) );
    printf("Number of Hashes             : %" PRIu64 "\n", total_records);
    printf("File Size to be read (bytes) : %" PRIu64 "\n", (uint64_t)(total_records * RECORD_SIZE));
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
                printf(" %" PRIu64 " time=%.3f ms comps=%d\n", nonce_to_u64(r.nonce), (get_time() - t0) * 1000, comps);
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
                printf("Usage: ./vaultx [OPTIONS]\n\n"
                       "Options:\n"
                       " -a, --approach [task|for] Parallelization mode (default: for)\n"
                       " -t, --threads NUM Hashing threads (default: all cores)\n"
                       " -i, --iothreads NUM Number of I/O threads to use (default: 1)\n"
                       " -c, --compression NUM Compression: number of hash bytes to discard from the\n"
                       "     end (0..HASH_SIZE)\n"
                       " -k, --exponent NUM Exponent k for 2^K iterations (default: 26)\n"
                       " -m, --memory NUM Memory size in MB (default: 1)\n"
                       " -f, --file NAME Final output file (moved/renamed to at end)\n"
                       " -g, --file_final NAME Temporary file (intermediate output)\n"
                       " -d, --debug [true|false] Enable per-search debug prints (default: false)\n"
                       " -b, --batch-size NUM Batch size (default: 1024)\n"
                       " -p, --print NUM Print NUM records and exit\n"
                       " -s, --search Enable search of specified number of records\n"
                       " -q, --difficulty Set difficulty for search in bytes\n"
                       " -h, --help Display this help message\n\n"
                       "Example:\n"
                       " ./vaultx -t 24 -i 1 -m 1024 -k 26 -g memo.t -f memo.x -d true\n");
                return 0;
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
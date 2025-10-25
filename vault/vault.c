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
/* keep RECORD_SIZE macro present but we will use runtime record_size variable */
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

/* Compression/runtime sizing globals */
int compression = 0;                /* number of bytes to drop from end of hash */
int effective_hash_len = HASH_SIZE; /* HASH_SIZE - compression */
int record_size = HASH_SIZE + NONCE_SIZE; /* effective on-disk record size */

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
    /* generate full HASH_SIZE bytes into rec->hash */
    blake3_hasher_finalize(&hasher, rec->hash, HASH_SIZE);
}

/* returns -1,0,1 like memcmp on effective_hash_len bytes */
static inline int compare_hash_bytes(const uint8_t *a, const uint8_t *b) {
    if (effective_hash_len == 0) return 0; /* nothing to compare */
    int cmp = memcmp(a, b, effective_hash_len);
    if (cmp < 0) return -1;
    if (cmp > 0) return 1;
    return 0;
}

/* record compare used by qsort */
int record_cmp(const void *a, const void *b) {
    const Record *ra = (const Record*)a;
    const Record *rb = (const Record*)b;
    return compare_hash_bytes(ra->hash, rb->hash);
}

/* forward decl for parallel sorter (defined later) */
static void parallel_qsort_records(Record *a, size_t n);

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

/* pack a single Record into dst (dst must have room for record_size bytes) */
static inline void pack_record(const Record *r, void *dst) {
    memcpy(dst, r->hash, effective_hash_len);
    memcpy((char*)dst + effective_hash_len, r->nonce, NONCE_SIZE);
}

/* unpack from src (record_size bytes) into a Record struct */
static inline void unpack_record(const void *src, Record *r) {
    /* copy effective hash bytes to r->hash[0..effective_hash_len-1] */
    memcpy(r->hash, src, effective_hash_len);
    /* zero the remainder of r->hash (optional but safer) */
    if (effective_hash_len < HASH_SIZE) memset(r->hash + effective_hash_len, 0, HASH_SIZE - effective_hash_len);
    memcpy(r->nonce, (char*)src + effective_hash_len, NONCE_SIZE);
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
    double t0 = get_time();

    /* allocate records using sizeof(Record) (in-memory full struct) */
    Record *buf = malloc(count * sizeof(Record));
    if (!buf) { perror("malloc"); exit(1); }

    /* Hashing */
    double th0 = get_time();
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
    double th1 = get_time();

    /* Sort */
    double ts0 = get_time();
    #pragma omp parallel
    {
        #pragma omp single nowait
        parallel_qsort_records(buf, count);
    }
    double ts1 = get_time();

    /* write chunk to the temporary file (not directly to final) */
    double tw0 = get_time();
    int fd = open(tmpfile, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd < 0) { perror("open tmpfile"); free(buf); exit(1); }

    /* pack buffer into compact on-disk format */
    size_t out_size = (size_t)count * (size_t)record_size;
    char *out = malloc(out_size);
    if (!out) { perror("malloc out"); close(fd); free(buf); exit(1); }
    char *p = out;
    for (uint64_t i = 0; i < count; i++) {
        pack_record(&buf[i], p);
        p += record_size;
    }

    ssize_t wrote = safe_pwrite_all(fd, out, out_size, 0);
    if (wrote < 0) {
        perror("pwrite tmpfile");
        close(fd); free(buf); free(out); exit(1);
    }
    close(fd);
    free(out);
    double tw1 = get_time();

    free(buf);
    double t1 = get_time();

    if (cfg.debug) {
        fprintf(stderr, "chunk start=%" PRIu64 " count=%" PRIu64 " times: hash=%.3f s sort=%.3f s write=%.3f s total=%.3f s\n",
                start, count, th1 - th0, ts1 - ts0, tw1 - tw0, t1 - t0);
    }
}

/* ---------- heap node used by merge_chunks (moved to file scope) */
typedef struct { Record rec; int src; } Node;

/* helper: swap two heap nodes (file-scope) */
static void heap_swap(Node *a, Node *b) {
    Node t = *a; *a = *b; *b = t;
}

/* compare by hash then nonce (file-scope) */
static int node_less(const Node *x, const Node *y) {
    int cmp = compare_hash_bytes(x->rec.hash, y->rec.hash);
    if (cmp != 0) return cmp < 0;
    /* tie-break by nonce lexicographically (6 bytes) */
    return memcmp(x->rec.nonce, y->rec.nonce, NONCE_SIZE) < 0;
}

void merge_chunks() {
    double m0 = get_time();

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

    /* per-file read buffer */
    const size_t IN_RECS = 4096;   /* tune: number of records to read per fread */
    typedef struct {
        FILE *f;
        Record *buf;    /* in-memory Records (full struct) */
        size_t len; /* valid records in buf */
        size_t pos; /* next index to consume */
        int idx;    /* file index (for removal naming) */
    } FileBuf;

    FileBuf *fb = calloc(nfiles, sizeof(FileBuf));
    for (int i = 0; i < nfiles; i++) {
        fb[i].f = fps[i];
        fb[i].buf = malloc(IN_RECS * sizeof(Record));
        /* read packed records and unpack into fb[i].buf */
        char *tmpbuf = malloc(IN_RECS * record_size);
        if (!tmpbuf) { perror("malloc tmpbuf"); exit(1); }
        size_t nread = fread(tmpbuf, record_size, IN_RECS, fb[i].f);
        fb[i].len = nread;
        fb[i].pos = 0;
        fb[i].idx = indices[i];
        for (size_t j = 0; j < nread; j++) {
            unpack_record(tmpbuf + j * record_size, &fb[i].buf[j]);
        }
        free(tmpbuf);
        if (fb[i].len == 0) { /* empty file */
            free(fb[i].buf);
            fb[i].buf = NULL;
            fclose(fb[i].f);
            fb[i].f = NULL;
        }
    }

    /* heap node and helpers already exist at file scope: Node, heap_swap, node_less */

    Node *heap = malloc((size_t)nfiles * sizeof(Node));

    int heap_sz = 0;

    /* push initial record from each file (from buffer) */
    for (int i = 0; i < nfiles; i++) {
        if (!fb[i].f || !fb[i].buf) continue;
        heap[heap_sz].rec = fb[i].buf[fb[i].pos++];
        heap[heap_sz].src = i;
        heap_sz++;
    }
    /* heapify by sifting up each inserted element (or build-heap) */
    for (int ci = 1; ci < heap_sz; ci++) {
        int cur = ci;
        while (cur > 0) {
            int pi = (cur - 1) >> 1;
            if (!node_less(&heap[cur], &heap[pi])) break;
            heap_swap(&heap[cur], &heap[pi]);
            cur = pi;
        }
    }

    /* output buffer */
    const size_t OUT_RECS = 65536; /* large write chunk (tune) */
    Record *outbuf = malloc(OUT_RECS * sizeof(Record));
    size_t out_cnt = 0;

    /* merge loop */
    while (heap_sz > 0) {
        Node top = heap[0];
        /* append to outbuf */
        outbuf[out_cnt++] = top.rec;
        if (out_cnt == OUT_RECS) {
            /* flush: pack outbuf into compact buffer and write */
            size_t pack_size = out_cnt * record_size;
            char *pack = malloc(pack_size);
            if (!pack) { perror("malloc pack"); goto cleanup; }
            char *pp = pack;
            for (size_t ii = 0; ii < out_cnt; ii++) {
                pack_record(&outbuf[ii], pp);
                pp += record_size;
            }
            /* write all */
            const char *p = pack;
            size_t remaining = pack_size;
            while (remaining > 0) {
                ssize_t w = write(fd_out, p, remaining);
                if (w < 0) { perror("write final"); free(pack); goto cleanup; }
                p += w;
                remaining -= w;
            }
            free(pack);
            out_cnt = 0;
        }

        int src = top.src;
        /* refill from that file buffer if available */
        if (fb[src].f && fb[src].pos < fb[src].len) {
            heap[0].rec = fb[src].buf[fb[src].pos++];
            heap[0].src = src;
        } else if (fb[src].f) {
            /* buffer exhausted; try to refill */
            /* read packed records and unpack into fb[src].buf */
            char *tmpbuf = malloc(IN_RECS * record_size);
            if (!tmpbuf) { perror("malloc tmpbuf2"); goto cleanup; }
            size_t nread = fread(tmpbuf, record_size, IN_RECS, fb[src].f);
            fb[src].len = nread;
            fb[src].pos = 0;
            if (nread > 0) {
                for (size_t j = 0; j < nread; j++) {
                    unpack_record(tmpbuf + j * record_size, &fb[src].buf[j]);
                }
                heap[0].rec = fb[src].buf[fb[src].pos++];
                heap[0].src = src;
            } else {
                /* EOF on this file: close and remove heap root by replacing with last */
                fclose(fb[src].f);
                fb[src].f = NULL;
                heap[0] = heap[--heap_sz];
            }
            free(tmpbuf);
        } else {
            /* file already closed, should not happen */
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
            heap_swap(&heap[i], &heap[smallest]);
            i = smallest;
        }
    }

    /* flush remaining output */
    if (out_cnt > 0) {
        size_t pack_size = out_cnt * record_size;
        char *pack = malloc(pack_size);
        if (!pack) { perror("malloc pack_final"); goto cleanup; }
        char *pp = pack;
        for (size_t ii = 0; ii < out_cnt; ii++) {
            pack_record(&outbuf[ii], pp);
            pp += record_size;
        }
        const char *p = pack;
        size_t remaining = pack_size;
        while (remaining > 0) {
            ssize_t w = write(fd_out, p, remaining);
            if (w < 0) { perror("write final"); free(pack); goto cleanup; }
            p += w;
            remaining -= w;
        }
        free(pack);
        out_cnt = 0;
    }

cleanup:
    /* close and remove tmp files and free buffers */
    for (int i = 0; i < nfiles; i++) {
        if (fb[i].buf) free(fb[i].buf);
        if (fb[i].f) fclose(fb[i].f);
        char tmpfile[256];
        snprintf(tmpfile, sizeof(tmpfile), "%s.%lu", cfg.file_temp, (unsigned long)fb[i].idx);
        remove(tmpfile);
    }
    free(fb);
    free(heap);
    free(outbuf);
    free(fps);
    free(indices);
    close(fd_out);

    double m1 = get_time();
    if (cfg.debug) fprintf(stderr, "Merge Time                  : %.3f s\n", m1 - m0);
}

/* ---------- Generate Mode ---------- */
void mode_generate() {
    total_records = 1ULL << cfg.k;
    uint64_t mem_bytes = (uint64_t)cfg.memory_mb * 1024 * 1024;
    chunk_size = mem_bytes / record_size;
    if (chunk_size == 0) chunk_size = 1;
    rounds = (total_records + chunk_size - 1) / chunk_size;

    if (cfg.debug) {
        printf("Selected Approach           : %s\n", cfg.approach);
        printf("Number of Threads           : %d\n", cfg.threads);
        printf("Exponent K                  : %d\n", cfg.k);
        printf("File Size (GB)              : %.2f\n", (double)total_records * record_size / (1ULL<<30));
        printf("Memory Size (MB)            : %d\n", cfg.memory_mb);
        printf("Rounds                      : %" PRIu64 "\n", rounds);
        printf("Temporary File              : %s\n", cfg.file_temp);
        printf("Final Output File           : %s\n", cfg.file_final);
        printf("Compression bytes dropped   : %d\n", compression);
        printf("Effective hash bytes        : %d\n", effective_hash_len);
        printf("On-disk record size         : %d\n", record_size);
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
    double mb = (double)total_records * record_size / total_time / 1048576.0;
    printf("Total Throughput: %.2f MH/s  %.2f MB/s\n", mh, mb);
    printf("Total Time: %.6f seconds\n", total_time);
}

/* ---------- Print Mode ---------- */
void mode_print(const char *filename, int n) {
    FILE *f = fopen(filename, "rb");
    if (!f) { perror("fopen"); exit(1); }
    Record r;
    char *tmp = malloc(record_size);
    if (!tmp) { perror("malloc tmp"); fclose(f); exit(1); }
    for (int i = 0; i < n && fread(tmp, record_size, 1, f); i++) {
        unpack_record(tmp, &r);
        off_t offset = (off_t)i * record_size;
        printf("[%lld] stored: ", (long long)offset);
        if (is_zero_nonce(r.nonce)) printf("BLANK nonce: BLANK\n");
        else {
            print_hex(r.hash, effective_hash_len);
            printf(" nonce: %" PRIu64 "\n", nonce_to_u64(r.nonce));
        }
    }
    free(tmp);
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
    printf("Record Size                  : %d\n", record_size);
    printf("Hash Size (stored)           : %d\n", effective_hash_len);
    printf("On-disk Record Size          : %d\n", record_size);
    printf("Number of Buckets            : %" PRIu64 "\n", (uint64_t)(1ULL << 24));
    printf("Number of Records in Bucket  : %" PRIu64 "\n", (uint64_t)(1ULL << (cfg.k - 24)) );
    printf("Number of Hashes             : %" PRIu64 "\n", total_records);
    printf("File Size to be read (bytes) : %" PRIu64 "\n", (uint64_t)(total_records * (uint64_t)record_size));
    printf("File Size to be read (GB)    : %.6f\n", (double)(total_records * (uint64_t)record_size) / (1ULL << 30));
    printf("Actual file size on disk     : %lld bytes\n", (long long)st.st_size);

    srand(time(NULL));
    double total_time = 0, total_seeks = 0, total_comps = 0, total_matches = 0;
    int found = 0, notfound = 0;

    char *tmp = malloc(record_size);
    if (!tmp) { perror("malloc tmp"); fclose(f); exit(1); }

    for (int q = 0; q < cfg.search_num; q++) {
        uint8_t prefix[HASH_SIZE];
        memset(prefix, 0, sizeof(prefix));
        for (int i = 0; i < cfg.difficulty; i++) prefix[i] = rand() & 0xFF;

        double t0 = get_time();
        uint64_t idx = 0;
        for (int i = 0; i < cfg.difficulty; i++) idx = (idx << 8) | prefix[i];
        idx = (idx * total_records) >> (cfg.difficulty * 8);
        off_t byte_off = (off_t)idx * (off_t)record_size;
        fseek(f, byte_off, SEEK_SET);

        int comps = 0, matches = 0;
        Record r;
        while (fread(tmp, record_size, 1, f) == 1) {
            unpack_record(tmp, &r);
            comps++;
            if (memcmp(r.hash, prefix, cfg.difficulty) != 0) break;
            if (cfg.debug) {
                printf("MATCH "); print_hex(r.hash, effective_hash_len);
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
    free(tmp);
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
    char *tmp = malloc(record_size);
    if (!tmp) { perror("malloc tmp"); fclose(f); exit(1); }
    while (fread(tmp, record_size, 1, f)) {
        unpack_record(tmp, &curr);
        if (i > 0 && memcmp(prev.hash, curr.hash, effective_hash_len) > 0) {
            printf("ERROR at record %lu: ", i);
            print_hex(prev.hash, effective_hash_len); printf(" > "); print_hex(curr.hash, effective_hash_len); printf("\n");
            valid = 0;
            break;
        }
        prev = curr;
        i++;
    }
    free(tmp);
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
    compression = 0;

    int opt;
    static struct option long_opts[] = {
        {"approach", 1, 0, 'a'}, {"threads", 1, 0, 't'}, {"iothreads", 1, 0, 'i'},
        {"exponent", 1, 0, 'k'}, {"memory", 1, 0, 'm'}, {"file", 1, 0, 'f'},
        {"file_final", 1, 0, 'g'}, {"debug", 1, 0, 'd'}, {"print", 1, 0, 'p'},
        {"search", 1, 0, 's'}, {"difficulty", 1, 0, 'q'}, {"verify", 1, 0, 'v'},
        {"compression", 1, 0, 'c'}, {"help", 0, 0, 'h'}, {0,0,0,0}
    };

    while ((opt = getopt_long(argc, argv, "a:t:i:k:m:f:g:d:p:s:q:v:c:h", long_opts, NULL)) != -1) {
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
            case 'c': compression = atoi(optarg); break;
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
                       " ./vaultx -t 24 -i 1 -m 1024 -k 26 -g memo.t -f memo.x -c 2 -d true\n");
                return 0;
                return 0;
        }
    }

    /* sanitize compression */
    if (compression < 0) compression = 0;
    if (compression > HASH_SIZE) compression = HASH_SIZE;
    effective_hash_len = HASH_SIZE - compression;
    record_size = effective_hash_len + NONCE_SIZE;

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

static void record_swap(Record *a, Record *b) {
    Record t = *a; *a = *b; *b = t;
}

static void parallel_qsort_records(Record *a, size_t n) {
    const size_t SERIAL_THRESHOLD = 1024;     /* use serial qsort for small arrays */
    const size_t TASK_THRESHOLD   = 1 << 14;  /* only spawn tasks for large partitions */

    if (n <= SERIAL_THRESHOLD) {
        qsort(a, n, sizeof(Record), record_cmp);
        return;
    }

    /* choose pivot (middle) and partition */
    Record pivot = a[n / 2];
    size_t i = 0, j = n - 1;
    while (i <= j) {
        while (record_cmp(&a[i], &pivot) < 0) i++;
        while (record_cmp(&a[j], &pivot) > 0) {
            if (j == 0) break; /* defensive */
            j--;
        }
        if (i <= j) {
            if (i != j) record_swap(&a[i], &a[j]);
            i++; if (j == 0) break; j--;
        }
    }

    /* recurse in parallel via OpenMP tasks */
    if (j + 1 > 0) {
        if (n > TASK_THRESHOLD) {
            #pragma omp task firstprivate(a, j)
            parallel_qsort_records(a, j + 1);
        } else {
            parallel_qsort_records(a, j + 1);
        }
    }
    if (n > i) {
        if (n > TASK_THRESHOLD) {
            #pragma omp task firstprivate(a, i, n)
            parallel_qsort_records(a + i, n - i);
        } else {
            parallel_qsort_records(a + i, n - i);
        }
    }
    #pragma omp taskwait
}

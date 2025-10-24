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
#include <sys/mman.h>
#include "blake3.h"

#define NONCE_SIZE 6
#define HASH_SIZE  10
#define RECORD_SIZE (HASH_SIZE + NONCE_SIZE)
#define BUCKET_BITS 24
#define NUM_BUCKETS (1ULL << BUCKET_BITS)

typedef struct {
    uint8_t hash[HASH_SIZE];
    uint8_t nonce[NONCE_SIZE];
} Record;

typedef struct {
    char *approach;
    int threads;
    int iothreads;
    int compression;
    int k;
    int memory_mb;
    char *file_temp;
    char *file_final;
    int debug;
    int batch_size;
    int print_num;
    int search_num;
    int difficulty;
    int verify;
} Config;

Config cfg = {0};
uint64_t total_records, chunk_size, rounds, bucket_size;

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

static inline uint64_t be_u64(const uint8_t *p) {
    uint64_t v;
    memcpy(&v, p, 8);
#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
    v = __builtin_bswap64(v);
#endif
    return v;
}

static inline uint16_t be_u16(const uint8_t *p) {
    uint16_t v;
    memcpy(&v, p, 2);
#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
    v = __builtin_bswap16(v);
#endif
    return v;
}

static inline int compare_hash_bytes(const uint8_t *a, const uint8_t *b) {
    int cmp_len = HASH_SIZE - cfg.compression;
    uint64_t va = be_u64(a);
    uint64_t vb = be_u64(b);
    if (va < vb) return -1;
    if (va > vb) return 1;
    if (cmp_len > 8) {
        uint16_t sa = be_u16(a + 8);
        uint16_t sb = be_u16(b + 8);
        if (sa < sb) return -1;
        if (sa > sb) return 1;
    }
    return 0;
}

int record_cmp(const void *a, const void *b) {
    const Record *ra = (const Record*)a;
    const Record *rb = (const Record*)b;
    return compare_hash_bytes(ra->hash, rb->hash);
}

typedef struct {
    int fd;
    off_t off;
    char *buf;
    size_t size;
} WriteJob;

void *write_worker(void *arg) {
    WriteJob *job = (WriteJob*)arg;
    (void)pwrite(job->fd, job->buf, job->size, job->off);
    free(job->buf);
    free(job);
    return NULL;
}

typedef struct {
    uint64_t start;
    uint64_t end;
    Record *buf;
    uint64_t chunk_start;
} Task;

void *hash_thread(void *arg) {
    Task *t = (Task*)arg;
    for (uint64_t j = t->start; j < t->end; j++) {
        generate_hash(j, &t->buf[j - t->chunk_start]);
    }
    free(t);
    return NULL;
}

void generate_chunk(uint64_t start, uint64_t count, const char *tmpfile) {
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
            uint64_t e = (t == cfg.threads - 1) ? start + count : s + per_thread;
            Task *task = malloc(sizeof(Task));
            task->start = s;
            task->end = e;
            task->buf = buf;
            task->chunk_start = start;
            pthread_create(&tids[t], NULL, hash_thread, task);
        }
        for (int t = 0; t < cfg.threads; t++) pthread_join(tids[t], NULL);
        free(tids);
    }

    qsort(buf, count, sizeof(Record), record_cmp);

    int fd = open(tmpfile, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd < 0) { perror("open"); exit(1); }

    int packed_size = HASH_SIZE - cfg.compression + NONCE_SIZE;
    char *packed = malloc(count * packed_size);
    for (uint64_t i = 0; i < count; i++) {
        memcpy(packed + i * packed_size, buf[i].hash, HASH_SIZE - cfg.compression);
        memcpy(packed + i * packed_size + HASH_SIZE - cfg.compression, buf[i].nonce, NONCE_SIZE);
    }
    free(buf);

    uint64_t per_io = count / cfg.iothreads;
    pthread_t *io_tids = malloc(cfg.iothreads * sizeof(pthread_t));
    for (int t = 0; t < cfg.iothreads; t++) {
        uint64_t s = t * per_io;
        uint64_t e = (t == cfg.iothreads - 1) ? count : (t + 1) * per_io;
        size_t size = (e - s) * packed_size;
        off_t off = s * packed_size;
        char *data = malloc(size);
        memcpy(data, packed + s * packed_size, size);
        WriteJob *job = malloc(sizeof(WriteJob));
        job->fd = fd;
        job->off = off;
        job->buf = data;
        job->size = size;
        pthread_create(&io_tids[t], NULL, write_worker, job);
    }
    for (int t = 0; t < cfg.iothreads; t++) pthread_join(io_tids[t], NULL);
    free(io_tids);
    close(fd);
    free(packed);
}

void merge_chunks() {
    double global_t0 = get_time();
    double shuffle_t0 = get_time();
    int num_files = (int)rounds;
    int packed_size = HASH_SIZE - cfg.compression + NONCE_SIZE;

    int fd_out = open(cfg.file_final, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd_out < 0) { perror("open final"); exit(1); }

    struct {
        int fd;
        Record *map;
        size_t num_records;
    } files[num_files];
    int active_files = 0;
    for (int i = 0; i < num_files; i++) {
        char tmpfile[256];
        snprintf(tmpfile, sizeof(tmpfile), "%s.%" PRIu64, cfg.file_temp, (uint64_t)i);
        int fd = open(tmpfile, O_RDONLY);
        if (fd < 0) continue;
        off_t fsize = lseek(fd, 0, SEEK_END);
        void *map = mmap(NULL, fsize, PROT_READ, MAP_SHARED, fd, 0);
        if (map == MAP_FAILED) { perror("mmap"); close(fd); continue; }
        madvise(map, fsize, MADV_SEQUENTIAL);
        files[active_files].fd = fd;
        files[active_files].map = (Record*)map;
        files[active_files].num_records = fsize / packed_size;
        active_files++;
    }

    if (active_files == 0) { close(fd_out); return; }

    Node *heap = malloc(active_files * sizeof(Node));
    int heap_size = 0;

    uint64_t *pointers = calloc(active_files, sizeof(uint64_t));
    for (int i = 0; i < active_files; i++) {
        if (pointers[i] < files[i].num_records) {
            Record *packed_rec = &files[i].map[pointers[i]];
            memcpy(heap[heap_size].rec.hash, packed_rec->hash, HASH_SIZE - cfg.compression);
            memset(heap[heap_size].rec.hash + HASH_SIZE - cfg.compression, 0, cfg.compression);
            memcpy(heap[heap_size].rec.nonce, packed_rec->nonce, NONCE_SIZE);
            heap[heap_size].src = i;
            heap_size++;
        }
    }

    for (int i = heap_size / 2 - 1; i >= 0; i--) {
        int cur = i;
        while (1) {
            int l = 2 * cur + 1;
            int r = l + 1;
            int smallest = cur;
            if (l < heap_size && node_less(&heap[l], &heap[smallest])) smallest = l;
            if (r < heap_size && node_less(&heap[r], &heap[smallest])) smallest = r;
            if (smallest == cur) break;
            heap_swap(&heap[cur], &heap[smallest]);
            cur = smallest;
        }
    }

    uint64_t written_bytes = 0;
    uint64_t file_size = total_records * packed_size;
    char *out_buf = malloc(1 << 24); // 16MB
    char *out_ptr = out_buf;
    size_t out_capacity = 1 << 24;
    while (heap_size > 0) {
        Node min = heap[0];
        memcpy(out_ptr, min.rec.hash, HASH_SIZE - cfg.compression);
        memcpy(out_ptr + HASH_SIZE - cfg.compression, min.rec.nonce, NONCE_SIZE);
        out_ptr += packed_size;
        written_bytes += packed_size;
        if (out_ptr - out_buf >= (ptrdiff_t)out_capacity - packed_size) {
            off_t off = lseek(fd_out, 0, SEEK_END);
            safe_pwrite_all(fd_out, out_buf, out_ptr - out_buf, off);
            out_ptr = out_buf;
        }

        if (written_bytes % (file_size / 4) == 0) {
            double now = get_time();
            double pct = (double)written_bytes / file_size * 100;
            double mb_s = written_bytes / (now - shuffle_t0) / 1048576.0;
            printf("[%.2f] Shuffle %.2f%%: %.2f MB/s\n", now - global_t0, pct, mb_s);
        }

        int src = min.src;
        pointers[src]++;
        if (pointers[src] < files[src].num_records) {
            Record *packed_rec = &files[src].map[pointers[src]];
            memcpy(heap[0].rec.hash, packed_rec->hash, HASH_SIZE - cfg.compression);
            memset(heap[0].rec.hash + HASH_SIZE - cfg.compression, 0, cfg.compression);
            memcpy(heap[0].rec.nonce, packed_rec->nonce, NONCE_SIZE);
            heap[0].src = src;
        } else {
            heap[0] = heap[--heap_size];
        }

        int cur = 0;
        while (1) {
            int l = 2 * cur + 1;
            int r = l + 1;
            int smallest = cur;
            if (l < heap_size && node_less(&heap[l], &heap[smallest])) smallest = l;
            if (r < heap_size && node_less(&heap[r], &heap[smallest])) smallest = r;
            if (smallest == cur) break;
            heap_swap(&heap[cur], &heap[smallest]);
            cur = smallest;
        }
    }

    if (out_ptr > out_buf) {
        off_t off = lseek(fd_out, 0, SEEK_END);
        safe_pwrite_all(fd_out, out_buf, out_ptr - out_buf, off);
    }

    for (int i = 0; i < active_files; i++) {
        munmap(files[i].map, files[i].num_records * packed_size);
        close(files[i].fd);
        char tmpfile[256];
        snprintf(tmpfile, sizeof(tmpfile), "%s.%" PRIu64, cfg.file_temp, (uint64_t)i);
        remove(tmpfile);
    }

    free(heap);
    free(pointers);
    free(out_buf);
    close(fd_out);
}

void mode_generate() {
    total_records = 1ULL << cfg.k;
    uint64_t mem_bytes = (uint64_t)cfg.memory_mb * (1ULL << 20);
    chunk_size = mem_bytes / RECORD_SIZE;
    if (chunk_size == 0) chunk_size = 1;
    rounds = (total_records + chunk_size - 1) / chunk_size;

    bucket_size = total_records >> BUCKET_BITS;

    if (cfg.debug) {
        printf("Selected Approach           : %s\n", cfg.approach);
        printf("Number of Threads           : %d\n", cfg.threads);
        printf("Exponent K                  : %d\n", cfg.k);
        printf("File Size (GB)              : %.2f\n", (double)total_records * (HASH_SIZE - cfg.compression + NONCE_SIZE) / (1ULL << 30));
        printf("File Size (bytes)           : %" PRIu64 "\n", total_records * (HASH_SIZE - cfg.compression + NONCE_SIZE));
        printf("Memory Size (MB)            : %d\n", cfg.memory_mb);
        printf("Memory Size (bytes)         : %" PRIu64 "\n", mem_bytes);
        printf("Number of Hashes (RAM)      : %" PRIu64 "\n", chunk_size);
        printf("Number of Hashes (Disk)     : %" PRIu64 "\n", total_records);
        printf("Size of HASH                : %d\n", HASH_SIZE - cfg.compression);
        printf("Size of NONCE               : %d\n", NONCE_SIZE);
        printf("Size of MemoRecord          : %d\n", HASH_SIZE - cfg.compression + NONCE_SIZE);
        printf("Rounds                      : %" PRIu64 "\n", rounds);
        printf("Number of Buckets           : %" PRIu64 "\n", NUM_BUCKETS);
        printf("Number of Records in Bucket : %" PRIu64 "\n", bucket_size);
        printf("BATCH_SIZE                  : %d\n", cfg.batch_size);
        printf("Temporary File              : %s\n", cfg.file_temp);
        printf("Final Output File           : %s\n", cfg.file_final);
    }

    double t0 = get_time();
    for (uint64_t r = 0; r < rounds; r++) {
        uint64_t start = r * chunk_size;
        uint64_t count = (start + chunk_size > total_records) ? (total_records - start) : chunk_size;
        char tmpfile[256];
        snprintf(tmpfile, sizeof(tmpfile), "%s.%" PRIu64, cfg.file_temp, r);
        generate_chunk(start, count, tmpfile);

        if (cfg.debug) {
            double now = get_time();
            double pct = (r + 1.0) / rounds * 100;
            double mh = (double)count / (now - t0) / 1e6;
            double mb = (double)count * (HASH_SIZE - cfg.compression + NONCE_SIZE) / (now - t0) / 1048576.0;
            printf("[%.2f] HashGen %.2f%%: %.2f MH/s : I/O %.2f MB/s\n", now - t0, pct, mh, mb);
        }
    }

    merge_chunks();

    double total_time = get_time() - t0;
    double total_mh = (double)total_records / total_time / 1e6;
    double total_mb = (double)total_records * (HASH_SIZE - cfg.compression + NONCE_SIZE) / total_time / 1048576.0;
    printf("Total Throughput: %.2f MH/s  %.2f MB/s\n", total_mh, total_mb);
    printf("Total Time: %.6f seconds\n", total_time);
}

void mode_verify(const char *filename) {
    printf("verifying sorted order by bucketIndex of final stored file...\n");

    struct stat st;
    if (stat(filename, &st) == 0) {
        printf("Size of '%s' is %" PRIi64 " bytes.\n", filename, (int64_t)st.st_size);
    }

    FILE *f = fopen(filename, "rb");
    if (!f) { perror("fopen"); exit(1); }

    double t0 = get_time();
    uint64_t sorted = 0, not_sorted = 0, zero_nonces = 0, total_rec = 0;
    int packed_size = HASH_SIZE - cfg.compression + NONCE_SIZE;
    char *prev_packed = malloc(packed_size);
    char *curr_packed = malloc(packed_size);
    int first = 1;
    while (fread(curr_packed, packed_size, 1, f) == 1) {
        total_rec++;
        uint8_t *nonce = (uint8_t*)(curr_packed + HASH_SIZE - cfg.compression);
        if (is_zero_nonce(nonce)) {
            zero_nonces++;
        } else {
            if (!first) {
                uint8_t prev_hash[HASH_SIZE];
                uint8_t curr_hash[HASH_SIZE];
                memcpy(prev_hash, prev_packed, HASH_SIZE - cfg.compression);
                memset(prev_hash + HASH_SIZE - cfg.compression, 0, cfg.compression);
                memcpy(curr_hash, curr_packed, HASH_SIZE - cfg.compression);
                memset(curr_hash + HASH_SIZE - cfg.compression, 0, cfg.compression);
                if (compare_hash_bytes(prev_hash, curr_hash) > 0) not_sorted++;
            }
            sorted++;
            memcpy(prev_packed, curr_packed, packed_size);
            first = 0;
        }

        if (total_rec % (total_records / 4) == 0 && total_rec > 0) {
            double now = get_time();
            double pct = (double)total_rec / total_records * 100;
            double mb = (double)total_rec * packed_size / (now - t0) / 1048576.0;
            printf("[%.2f] Verify %.2f%%: %.2f MB/s\n", now - t0, pct, mb);
        }
    }
    fclose(f);
    free(prev_packed);
    free(curr_packed);

    printf("sorted=%" PRIu64 " not_sorted=%" PRIu64 " zero_nonces=%" PRIu64 " total_records=%" PRIu64 "\n", sorted, not_sorted, zero_nonces, total_rec);
}

void mode_print(const char *filename, int n) {
    FILE *f = fopen(filename, "rb");
    if (!f) { perror("fopen"); exit(1); }
    int packed_size = HASH_SIZE - cfg.compression + NONCE_SIZE;
    char *packed = malloc(packed_size);
    for (int i = 0; i < n && fread(packed, packed_size, 1, f); i++) {
        printf("[%d] stored: ", i * packed_size);
        uint8_t *nonce = (uint8_t*)(packed + HASH_SIZE - cfg.compression);
        if (is_zero_nonce(nonce)) {
            printf("BLANK nonce: BLANK\n");
        } else {
            print_hex((uint8_t*)packed, HASH_SIZE - cfg.compression);
            printf(" nonce: %" PRIu64 "\n", nonce_to_u64(nonce));
        }
    }
    fclose(f);
    free(packed);
}

void mode_search(const char *filename) {
    printf("searches=%d difficulty=%d\n", cfg.search_num, cfg.difficulty);
    printf("Parsed k                     : %d\n", cfg.k);
    printf("Nonce Size                   : %d\n", NONCE_SIZE);
    printf("Record Size                  : %d\n", RECORD_SIZE - cfg.compression);
    printf("Hash Size                    : %d\n", HASH_SIZE - cfg.compression);
    printf("On-disk Record Size          : %d\n", RECORD_SIZE - cfg.compression);
    printf("Number of Buckets            : %" PRIu64 "\n", (1ULL << 24));
    printf("Number of Records in Bucket  : %" PRIu64 "\n", (1ULL << (cfg.k - 24)));
    printf("Number of Hashes      : %" PRIu64 "\n", total_records);
    printf("File Size to be read (bytes) : %" PRIu64 "\n", total_records * (RECORD_SIZE - cfg.compression));
    printf("File Size to be read (GB)    : %.6f\n", (double)total_records * (RECORD_SIZE - cfg.compression) / (1ULL << 30));

    struct stat st;
    if (stat(filename, &st) == 0) {
        printf("Actual file size on disk     : %" PRIi64 " bytes\n", (int64_t)st.st_size);
    }

    FILE *f = fopen(filename, "rb");
    if (!f) { perror("fopen"); exit(1); }

    srand(time(NULL));
    double total_time = 0;
    int total_seeks = 0;
    int total_comps = 0;
    int total_matches = 0;
    int found_queries = 0;
    int notfound = 0;

    int packed_size = RECORD_SIZE - cfg.compression;
    char *packed = malloc(packed_size);
    for (int i = 0; i < cfg.search_num; i++) {
        uint8_t prefix[HASH_SIZE] = {0};
        for (int j = 0; j < cfg.difficulty; j++) prefix[j] = rand() % 256;

        double t0 = get_time();

        uint64_t prefix_val = 0;
        for (int j = 0; j < cfg.difficulty; j++) {
            prefix_val = (prefix_val << 8) | prefix[j];
        }
        uint64_t start_index = prefix_val * total_records >> (cfg.difficulty * 8);
        off_t byte_off = start_index * packed_size;
        fseek(f, byte_off, SEEK_SET);

        int comps = 0;
        int matches = 0;
        while (fread(packed, packed_size, 1, f) == 1) {
            uint8_t hash[HASH_SIZE];
            memcpy(hash, packed, HASH_SIZE - cfg.compression);
            memset(hash + HASH_SIZE - cfg.compression, 0, cfg.compression);
            uint8_t *nonce = (uint8_t*)(packed + HASH_SIZE - cfg.compression);

            comps++;
            if (memcmp(hash, prefix, cfg.difficulty) != 0) break;
            matches++;
            if (cfg.debug) {
                printf("[%d] MATCH ", i);
                print_hex(hash, HASH_SIZE);
                printf(" %" PRIu64 " time=%.3f ms comps=%d\n", nonce_to_u64(nonce), (get_time() - t0) * 1000, comps);
            }
        }

        double time_ms = (get_time() - t0) * 1000;
        if (cfg.debug && matches == 0) {
            printf("[%d] ", i);
            print_hex(prefix, cfg.difficulty);
            printf(" NOTFOUND time=%.3f ms comps=%d\n", time_ms, comps);
        }

        total_time += time_ms / 1000.0;
        total_seeks += 1;
        total_comps += comps;
        total_matches += matches;
        if (matches > 0) found_queries++;
        else notfound++;
    }
    fclose(f);
    free(packed);

    double avg_ms = total_time * 1000 / cfg.search_num;
    double searches_per_sec = cfg.search_num / total_time;
    double avg_seeks = (double)total_seeks / cfg.search_num;
    double avg_comps = (double)total_comps / cfg.search_num;
    double avg_matches = found_queries > 0 ? (double)total_matches / found_queries : 0.0;

    printf("Search Summary: requested=%d performed=%d found_queries=%d total_matches=%d notfound=%d\n",
           cfg.search_num, cfg.search_num, found_queries, total_matches, notfound);
    printf("total_time=%.6f s avg_ms=%.3f ms searches/sec=%.3f total_seeks=%d\n",
           total_time, avg_ms, searches_per_sec, total_seeks);
    printf("avg_seeks_per_search=%.3f total_comps=%d avg_comps_per_search=%.3f\n",
           avg_seeks, total_comps, avg_comps);
    printf("avg_matches_per_found=%.3f\n", avg_matches);
}

static void record_swap(Record *a, Record *b) {
    Record t = *a;
    *a = *b;
    *b = t;
}

static void parallel_qsort_records(Record *a, size_t n) {
    const size_t threshold = 1 << 16; // 65536

    if (n < threshold) {
        qsort(a, n, sizeof(Record), record_cmp);
        return;
    }

    Record *p1 = &a[0], *p2 = &a[n/2], *p3 = &a[n-1];
    if (record_cmp(p1, p2) > 0) record_swap(p1, p2);
    if (record_cmp(p1, p3) > 0) record_swap(p1, p3);
    if (record_cmp(p2, p3) > 0) record_swap(p2, p3);
    Record pivot = *p2;

    size_t i = 0, j = n - 1;
    while (i <= j) {
        while (record_cmp(&a[i], &pivot) < 0) i++;
        while (record_cmp(&a[j], &pivot) > 0) j--;
        if (i <= j) {
            record_swap(&a[i], &a[j]);
            i++;
            j--;
        }
    }

    #pragma omp task
    parallel_qsort_records(a, i);
    #pragma omp task
    parallel_qsort_records(a + i, n - i);
    #pragma omp taskwait
}

int main(int argc, char *argv[]) {
    cfg.approach = "for";
    cfg.threads = omp_get_max_threads();
    cfg.iothreads = 1;
    cfg.compression = 0;
    cfg.k = 26;
    cfg.memory_mb = 1;
    cfg.debug = 0;
    cfg.batch_size = 1024;
    cfg.print_num = 0;
    cfg.search_num = 0;
    cfg.difficulty = 0;
    cfg.verify = 0;

    int opt;
    static struct option long_opts[] = {
        {"approach", required_argument, 0, 'a'},
        {"threads", required_argument, 0, 't'},
        {"iothreads", required_argument, 0, 'i'},
        {"compression", required_argument, 0, 'c'},
        {"exponent", required_argument, 0, 'k'},
        {"memory", required_argument, 0, 'm'},
        {"file", required_argument, 0, 'f'},
        {"file_final", required_argument, 0, 'g'},
        {"debug", required_argument, 0, 'd'},
        {"batch-size", required_argument, 0, 'b'},
        {"print", required_argument, 0, 'p'},
        {"search", required_argument, 0, 's'},
        {"difficulty", required_argument, 0, 'q'},
        {"verify", required_argument, 0, 'v'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}
    };

    while ((opt = getopt_long(argc, argv, "a:t:i:c:k:m:f:g:d:b:p:s:q:v:h", long_opts, NULL)) != -1) {
        switch (opt) {
            case 'a': cfg.approach = optarg; break;
            case 't': cfg.threads = atoi(optarg); break;
            case 'i': cfg.iothreads = atoi(optarg); break;
            case 'c': cfg.compression = atoi(optarg); break;
            case 'k': cfg.k = atoi(optarg); break;
            case 'm': cfg.memory_mb = atoi(optarg); break;
            case 'f': cfg.file_final = optarg; break;
            case 'g': cfg.file_temp = optarg; break;
            case 'd': cfg.debug = !strcmp(optarg, "true"); break;
            case 'b': cfg.batch_size = atoi(optarg); break;
            case 'p': cfg.print_num = atoi(optarg); break;
            case 's': cfg.search_num = atoi(optarg); break;
            case 'q': cfg.difficulty = atoi(optarg); break;
            case 'v': cfg.verify = !strcmp(optarg, "true"); break;
            case 'h':
                printf("Usage: ./vaultx [OPTIONS]\n");
                // print usage as in homework
                return 0;
        }
    }

    total_records = 1ULL << cfg.k;

    if (cfg.print_num > 0) {
        mode_print(cfg.file_final, cfg.print_num);
    } else if (cfg.search_num > 0) {
        mode_search(cfg.file_final);
    } else if (cfg.verify) {
        mode_verify(cfg.file_final);
    } else {
        mode_generate();
    }

    return 0;
}

static int node_less(const Node *x, const Node *y) {
    int cmp = compare_hash_bytes(x->rec.hash, y->rec.hash);
    if (cmp != 0) return cmp < 0;
    return memcmp(x->rec.nonce, y->rec.nonce, NONCE_SIZE) < 0;
}

static void heap_swap(Node *a, Node *b) {
    Node t = *a;
    *a = *b;
    *b = t;
}
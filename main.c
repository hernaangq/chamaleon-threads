// main.c
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>     // For getopt()
#include <inttypes.h>   // For PRIu64 macro to print uint64_t portably

// Define constants for the record structure as per the assignment
#define NONCE_SIZE 6
#define HASH_SIZE 10

// Structure to hold a 16-byte record [cite: 23-25]
typedef struct {
    uint8_t hash[HASH_SIZE]; // 10-byte hash value
    uint8_t nonce[NONCE_SIZE]; // 6-byte nonce value
} Record;

/**
 * @brief A placeholder function to simulate hash generation.
 * @param nonce_val The input nonce value.
 * @param record A pointer to the Record struct to fill.
 *
 * TODO: Replace this with the actual BLAKE3 hashing algorithm.
 */
void generate_hash(uint64_t nonce_val, Record* record) {
    // 1. Populate the nonce field using memcpy
    memcpy(record->nonce, &nonce_val, NONCE_SIZE);

    // 2. *** HASHING PLACEHOLDER ***
    // This creates a dummy hash by repeating the nonce bytes.
    // YOU MUST REPLACE THIS with real BLAKE3 hashing.
    for (int i = 0; i < HASH_SIZE; ++i) {
        record->hash[i] = record->nonce[i % NONCE_SIZE];
    }
}

// --- Main Program ---
int main(int argc, char *argv[]) {
    // Default values
    int exponent = 20; // Using a smaller default (2^20) for quick testing
    char* output_filename = "output.dat";
    int opt;

    // Parse command line arguments for exponent (-k) and filename (-f)
    while ((opt = getopt(argc, argv, "k:f:h")) != -1) {
        switch (opt) {
            case 'k':
                exponent = atoi(optarg);
                break;
            case 'f':
                output_filename = optarg;
                break;
            case 'h':
                printf("Usage: %s [-k exponent] [-f filename]\n", argv[0]);
                return EXIT_SUCCESS;
            default:
                fprintf(stderr, "Usage: %s [-k exponent] [-f filename]\n", argv[0]);
                return EXIT_FAILURE;
        }
    }

    // Calculate the total number of records to generate: 2^exponent
    uint64_t total_records = 1ULL << exponent;

    printf("Starting benchmark...\n");
    printf("Exponent (k): %d\n", exponent);
    printf("Total records: %" PRIu64 "\n", total_records);
    printf("Output file: %s\n", output_filename);

    // Open the output file for writing in binary mode ("wb")
    FILE *outfile = fopen(output_filename, "wb");
    if (!outfile) {
        perror("Error opening output file");
        return EXIT_FAILURE;
    }

    // Main loop to generate and write records
    for (uint64_t i = 0; i < total_records; ++i) {
        Record current_record;
        
        // Generate a hash and nonce pair
        generate_hash(i, &current_record);
        
        // Write the 16-byte record to the file
        fwrite(&current_record, sizeof(Record), 1, outfile);
    }

    fclose(outfile);
    printf("Benchmark finished. Wrote %" PRIu64 " records to %s.\n", total_records, output_filename);

    return EXIT_SUCCESS;
}

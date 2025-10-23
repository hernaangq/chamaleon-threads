CC = gcc
CFLAGS = -std=c11 -O3 -march=native -Wall -Wextra
LIBS = -lblake3 -lpthread -fopenmp
TARGET = vaultx

all: $(TARGET)

$(TARGET): vault.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS)

clean:
	rm -f $(TARGET) *.dat *.x memo.*

.PHONY: all clean
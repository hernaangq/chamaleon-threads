#!/bin/bash

mkdir -p results

filename="./results/results.csv"
maxIOThreads=2 #power of 2 equivalent to 4 IO threads
cpuThreads="1 2 4 8 12 24 48" #CPU threads options list
memSizes="256 512 1024" #memory size options list
memSizesLarge="1024 2048 4096 8192 16384 32768 65536"
base=2
tmpfile="memo.t"
finalfile="memo.x"


echo "Initializing automated tests..."

#create or replace file
touch $filename

#Empty preexisting file
> $filename
echo "approach,threads,iothreads,k,mem_mb,rounds,tmpfile,finalfile,compression,eff_hash,record_size,merge_time,mh_per_s,mb_per_s,total_time" > "$filename"


# ================ HASH GENERATION TEST - SMALL ======================

k=26

echo "Testing for K-value: $k"

for threadNo in $cpuThreads; do

    for memSize in $memSizes; do

        for (( i=0 ; i<=$maxIOThreads; i++ )); do
            iothreadNo=$(($base**$i))
            echo "  CPU Threads: $threadNo, Memory Size: $memSize, I/O threads: $iothreadNo"
            # Run vaultx and collect its metrics
            ./vaultx -t $threadNo -i $iothreadNo -m $memSize -k $k -g $tmpfile -f $finalfile -c 0 -d true 2>&1| \
            #./vaultx -t 24 -i 1 -m 1024 -k 26 -g memo.t -f memo.x -c 0 -d true 2>&1| \
            awk -v threads="$threadNo" -v iothreads="$iothreadNo" -v mem="$memSize" -v kval="$k" '
            /Selected Approach/          {approach=$4}
            /Number of Threads/           {threads_used=$5}
            /Rounds/                      {rounds=$3}
            /Temporary File/              {tmpfile=$4}
            /Final Output File/           {finalfile=$5}
            /Compression bytes dropped/   {compression=$5}
            /Effective hash bytes/        {eff_hash=$5}
            /On-disk record size/         {record_size=$5}
            /Merge Time :/                 {merge_time=$4}
            /Total Throughput:/          {mh_per_s=$3; mb_per_s=$5}
            /Total Time:/                {total_time=$3}
            END {
                printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
                    approach, threads_used, iothreads, kval, mem, rounds,
                    tmpfile, finalfile, compression, eff_hash, record_size,
                    merge_time, mh_per_s, mb_per_s, total_time
            }' >> "$filename"
        done

    done

done

# ================ HASH GENERATION TEST - LARGE ======================

echo "Results exported to  $filename"

k=32

echo "Testing for K-value: $k"

threadNo=24

for memSize in $memSizesLarge; do
    iothreadNo=1
    echo "  CPU Threads: $threadNo, Memory Size: $memSize, I/O threads: $iothreadNo"
    # Run vaultx and collect its metrics
    ./vaultx -t $threadNo -i $iothreadNo -m $memSize -k $k -g $tmpfile -f $finalfile -c 0 -d true 2>&1| \
    #./vaultx -t 24 -i 1 -m 1024 -k 26 -g memo.t -f memo.x -c 0 -d true 2>&1| \
    awk -v threads="$threadNo" -v iothreads="$iothreadNo" -v mem="$memSize" -v kval="$k" '
    /Selected Approach/          {approach=$4}
    /Number of Threads/           {threads_used=$5}
    /Rounds/                      {rounds=$3}
    /Temporary File/              {tmpfile=$4}
    /Final Output File/           {finalfile=$5}
    /Compression bytes dropped/   {compression=$5}
    /Effective hash bytes/        {eff_hash=$5}
    /On-disk record size/         {record_size=$5}
    /Merge Time :/                 {merge_time=$4}
    /Total Throughput:/          {mh_per_s=$3; mb_per_s=$5}
    /Total Time:/                {total_time=$3}
    END {
        printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
            approach, threads_used, iothreads, kval, mem, rounds,
            tmpfile, finalfile, compression, eff_hash, record_size,
            merge_time, mh_per_s, mb_per_s, total_time
    }' >> "$filename"
done




query_hash_gen_sm = """
SELECT df.threads,
        df.mem_mb,
        df.total_time iot_1,
        df2.total_time iot_2,
        df3.total_time iot_4
FROM hash_gen_sm_df df 
    INNER JOIN (SELECT * FROM hash_gen_sm_df WHERE iothreads = 2) df2 ON 
        df.threads = df2.threads AND
        df.mem_mb = df2.mem_mb
    INNER JOIN (SELECT * FROM hash_gen_sm_df WHERE iothreads = 4) df3 ON
        df.threads = df3.threads AND
        df.mem_mb = df3.mem_mb
WHERE df.iothreads = 1 AND
        df.k = 26
ORDER BY df.threads, df.mem_mb
"""

query_hash_gen_lg = """
SELECT df.threads,
        df.mem_mb,
        df.total_time iot_1
FROM hash_gen_lg_df df 
WHERE df.k = 32
ORDER BY df.threads, df.mem_mb
"""

query_hash_gen_sm_best_config = """
WITH data as (
    SELECT df.threads,
            df.mem_mb,
            df.total_time iot_1,
            df2.total_time iot_2,
            df3.total_time iot_4,
            CASE 
                WHEN df.total_time <= df2.total_time AND df.total_time <= df3.total_time THEN df.total_time 
                WHEN df2.total_time <= df.total_time AND df2.total_time <= df3.total_time THEN df2.total_time
                WHEN df3.total_time <= df.total_time AND df3.total_time <= df2.total_time THEN df3.total_time
            END as best_time,
            CASE 
                WHEN df.total_time <= df2.total_time AND df.total_time <= df3.total_time THEN 'IO_1' 
                WHEN df2.total_time <= df.total_time AND df2.total_time <= df3.total_time THEN 'IO_2'
                WHEN df3.total_time <= df.total_time AND df3.total_time <= df2.total_time THEN 'IO_3'
            END as best_io
    FROM hash_gen_sm_df df 
        INNER JOIN (SELECT * FROM hash_gen_sm_df WHERE iothreads = 2) df2 ON 
            df.threads = df2.threads AND
            df.mem_mb = df2.mem_mb
        INNER JOIN (SELECT * FROM hash_gen_sm_df WHERE iothreads = 4) df3 ON
            df.threads = df3.threads AND
            df.mem_mb = df3.mem_mb
    WHERE df.iothreads = 1 AND
            df.k = 26
    ORDER BY df.threads, df.mem_mb
), min as (
    SELECT MIN(best_time) as abs_min from data
)
SELECT data.*, 
        CASE WHEN min.abs_min = data.best_time then 'Y' else 'N' END AS abs_best 
FROM data, min
"""

query_hash_search = """
SELECT df.approach k,
        df.threads difficulty,
        df.iothreads no_searches,
        df.k avg_seeks,
        df.mem_mb avg_data_read,
        df.rounds total_time,
        df.tmpfile avg_ms,
        df.finalfile throughput,
        df.compression found,
        df.eff_hash not_found
FROM hash_srch_df df 
ORDER BY 1, 2
"""

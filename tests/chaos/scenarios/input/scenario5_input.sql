BEGIN;
    INSERT INTO T1
    SELECT i,
        RANDOM() * 1000000,
            md5(random()::text || clock_timestamp()::text)::uuid,
            md5(random()::text || clock_timestamp()::text)::uuid
    FROM generate_series(1,2000000) i;
COMMIT;

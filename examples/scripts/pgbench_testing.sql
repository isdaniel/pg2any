INSERT INTO T1 (val, col1, col2)
SELECT
    RANDOM() * 1000000,
    md5(random()::text || clock_timestamp()::text)::uuid,
    md5(random()::text || clock_timestamp()::text)::uuid
FROM generate_series(1, 3000) AS i;

-- Scenario 3: DELETE operations
-- This scenario tests delete replication with chaos

INSERT INTO T1
SELECT i,
    RANDOM() * 1000000,
        md5(random()::text || clock_timestamp()::text)::uuid,
        md5(random()::text || clock_timestamp()::text)::uuid
FROM generate_series(1,300000) i;


-- Delete records with specific condition
DELETE FROM public.t1 WHERE id <= 250000;
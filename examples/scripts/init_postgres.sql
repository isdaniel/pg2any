-- PostgreSQL 13+: Use publish_via_partition_root to send parent table name
CREATE PUBLICATION cdc_pub FOR ALL TABLES WITH (publish_via_partition_root = true);

CREATE TABLE public.t1 (
    ID SERIAL PRIMARY KEY,
    val INT NOT NULL,
    col1 UUID NOT NULL,
    col2 UUID NOT NULL
)
PARTITION BY HASH (ID);

DO $$
DECLARE 
    i INT;
    part_name TEXT;
BEGIN
    FOR i IN 0..49 LOOP
        part_name := format('t1_p%s', i);
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF public.t1
             FOR VALUES WITH (MODULUS 50, REMAINDER %s);',
            part_name, i
        );
    END LOOP;
END $$;

-- INSERT INTO T1
-- SELECT i,
--        RANDOM() * 1000000,
-- 	   md5(random()::text || clock_timestamp()::text)::uuid,
-- 	   md5(random()::text || clock_timestamp()::text)::uuid
-- FROM generate_series(1,1000) i;

-- SELECT
--    format($f$
--       BEGIN;
--       INSERT INTO public.T1 (val, col1, col2)
--       VALUES (
--          RANDOM() * 1000000,
--          md5(random()::text || clock_timestamp()::text)::uuid,
--          md5(random()::text || clock_timestamp()::text)::uuid
--       );
--       COMMIT;
--    $f$)
-- FROM generate_series(1, 10000);
-- \gexec

--select pg_drop_replication_slot('cdc_slot');
-- Scenario 4: Mixed operations (INSERT, UPDATE, DELETE)
-- This scenario tests complex transaction patterns under chaos

-- Insert batch 1
DO $$
DECLARE 
    i INT;
BEGIN
    FOR i IN 1..30 LOOP
        INSERT INTO public.t1 (val, col1, col2)
        VALUES (
            floor(random() * 1000000)::int,
            md5(random()::text || clock_timestamp()::text)::uuid,
            md5(random()::text || clock_timestamp()::text)::uuid
        );
    END LOOP;
END $$;

SELECT pg_sleep(1);

-- Update some records
UPDATE public.t1 
SET val = 999999
WHERE val > 800000;

SELECT pg_sleep(1);

-- Insert batch 2
DO $$
DECLARE 
    i INT;
BEGIN
    FOR i IN 1..20 LOOP
        INSERT INTO public.t1 (val, col1, col2)
        VALUES (
            floor(random() * 1000000)::int,
            md5(random()::text || clock_timestamp()::text)::uuid,
            md5(random()::text || clock_timestamp()::text)::uuid
        );
    END LOOP;
END $$;

SELECT pg_sleep(1);

-- Delete some records
DELETE FROM public.t1 WHERE val < 100000;

SELECT pg_sleep(1);

-- Final insert
DO $$
DECLARE 
    i INT;
BEGIN
    FOR i IN 1..10 LOOP
        INSERT INTO public.t1 (val, col1, col2)
        VALUES (
            floor(random() * 1000000)::int,
            md5(random()::text || clock_timestamp()::text)::uuid,
            md5(random()::text || clock_timestamp()::text)::uuid
        );
    END LOOP;
END $$;

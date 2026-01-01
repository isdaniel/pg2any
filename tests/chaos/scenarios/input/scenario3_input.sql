-- Scenario 3: DELETE operations
-- This scenario tests delete replication with chaos

-- Insert records
DO $$
DECLARE 
    i INT;
BEGIN
    FOR i IN 1..80 LOOP
        INSERT INTO public.t1 (val, col1, col2)
        VALUES (
            floor(random() * 1000000)::int,
            md5(random()::text || clock_timestamp()::text)::uuid,
            md5(random()::text || clock_timestamp()::text)::uuid
        );
    END LOOP;
END $$;

-- Wait for replication
SELECT pg_sleep(2);

-- Get the count before delete
DO $$
DECLARE
    count_before INT;
BEGIN
    SELECT COUNT(*) INTO count_before FROM public.t1;
    RAISE NOTICE 'Records before delete: %', count_before;
END $$;

-- Delete records with specific condition
DELETE FROM public.t1 WHERE val < 250000;

-- Get the count after delete
DO $$
DECLARE
    count_after INT;
BEGIN
    SELECT COUNT(*) INTO count_after FROM public.t1;
    RAISE NOTICE 'Records after delete: %', count_after;
END $$;

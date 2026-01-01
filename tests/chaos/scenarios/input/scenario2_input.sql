-- Scenario 2: UPDATE operations
-- This scenario tests update replication with chaos

-- First, insert some baseline records
DO $$
DECLARE 
    i INT;
BEGIN
    FOR i IN 1..50 LOOP
        INSERT INTO public.t1 (val, col1, col2)
        VALUES (
            floor(random() * 1000000)::int,
            md5(random()::text || clock_timestamp()::text)::uuid,
            md5(random()::text || clock_timestamp()::text)::uuid
        );
    END LOOP;
END $$;

-- Wait a moment for replication
SELECT pg_sleep(2);

-- Update records with a specific pattern
UPDATE public.t1 
SET val = val + 10000
WHERE val < 500000;

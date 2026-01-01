-- Scenario 1: Basic INSERT operations
-- This scenario tests basic insert replication under chaos conditions

-- Insert 100 records with random data
DO $$
DECLARE 
    i INT;
BEGIN
    FOR i IN 1..100 LOOP
        INSERT INTO public.t1 (val, col1, col2)
        VALUES (
            floor(random() * 1000000)::int,
            md5(random()::text || clock_timestamp()::text)::uuid,
            md5(random()::text || clock_timestamp()::text)::uuid
        );
    END LOOP;
END $$;

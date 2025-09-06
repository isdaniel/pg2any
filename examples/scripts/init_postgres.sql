CREATE TABLE T1 (
    ID INT NOT NULL PRIMARY KEY,
	val INT NOT NULL,
	col1 UUID NOT NULL,
	col2 UUID NOT NULL
);

INSERT INTO T1
SELECT i,
       RANDOM() * 1000000,
	   md5(random()::text || clock_timestamp()::text)::uuid,
	   md5(random()::text || clock_timestamp()::text)::uuid
FROM generate_series(1,1000) i;

SELECT
   format($f$
      BEGIN;
      INSERT INTO public.T1 (val, col1, col2)
      VALUES (
         RANDOM() * 1000000,
         md5(random()::text || clock_timestamp()::text)::uuid,
         md5(random()::text || clock_timestamp()::text)::uuid
      );
      COMMIT;
   $f$)
FROM generate_series(1, 10000);
\gexec

CREATE PUBLICATION cdc_pub FOR ALL TABLES;

select pg_drop_replication_slot('cdc_slot');
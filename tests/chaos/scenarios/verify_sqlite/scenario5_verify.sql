-- Scenario 5: Verification - Check if high-volume data was replicated
-- Expected row count must match scenario5_input.sql generate_series(1, N)
WITH expected(cnt) AS (VALUES (3000000))
SELECT
    CASE
        WHEN t.actual_count = expected.cnt THEN 'PASS'
        ELSE 'FAIL'
    END AS test_result,
    t.actual_count AS actual_count,
    expected.cnt AS expected_count,
    CASE
        WHEN t.actual_count = expected.cnt THEN 'All ' || CAST(expected.cnt AS TEXT) || ' rows replicated successfully'
        WHEN t.actual_count > 0 THEN 'Partial replication: ' || CAST(t.actual_count AS TEXT) || ' of ' || CAST(expected.cnt AS TEXT) || ' rows'
        ELSE 'No data replicated'
    END AS status_message
FROM (SELECT COUNT(*) AS actual_count FROM t1) t, expected;

-- Scenario 5: Verification - Check if high-volume data was replicated
-- Expected row count must match scenario5_input.sql generate_series(1, N)
WITH expected(cnt) AS (VALUES (3000000))
SELECT
    CASE
        WHEN COUNT(*) = expected.cnt THEN 'PASS'
        ELSE 'FAIL'
    END AS test_result,
    COUNT(*) AS actual_count,
    expected.cnt AS expected_count,
    CASE
        WHEN COUNT(*) = expected.cnt THEN 'All ' || CAST(expected.cnt AS TEXT) || ' rows replicated successfully'
        WHEN COUNT(*) > 0 THEN 'Partial replication: ' || CAST(COUNT(*) AS TEXT) || ' of ' || CAST(expected.cnt AS TEXT) || ' rows'
        ELSE 'No data replicated'
    END AS status_message
FROM t1, expected
GROUP BY expected.cnt;

-- Scenario 4: Verification - Check if mixed operations were replicated
SELECT
    CASE
        WHEN COUNT(*) >= 30 AND EXISTS (SELECT 1 FROM t1 WHERE val = 999999)
        THEN 'PASS'
        ELSE 'FAIL'
    END AS test_result,
    COUNT(*) AS total_count,
    (SELECT COUNT(*) FROM t1 WHERE val = 999999) AS marker_count
FROM t1;

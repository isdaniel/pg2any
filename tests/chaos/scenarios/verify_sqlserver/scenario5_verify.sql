-- Scenario 5: Verification - Check if high-volume data (3M rows) was replicated
SELECT
    CASE
        WHEN COUNT(*) = 3000000 THEN 'PASS'
        ELSE 'FAIL'
    END AS test_result,
    COUNT(*) AS actual_count,
    3000000 AS expected_count,
    CASE
        WHEN COUNT(*) = 3000000 THEN 'All 3M rows replicated successfully'
        WHEN COUNT(*) > 0 THEN CONCAT('Partial replication: ', CAST(COUNT(*) AS VARCHAR(20)), ' of 3M rows')
        ELSE 'No data replicated'
    END AS status_message
FROM dbo.t1;
GO

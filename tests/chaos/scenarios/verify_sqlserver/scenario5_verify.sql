-- Scenario 5: Verification - Check if high-volume data was replicated
-- Expected row count must match scenario5_input.sql generate_series(1, N)
DECLARE @expected_count INT = 3000000;
SELECT
    CASE
        WHEN COUNT(*) = @expected_count THEN 'PASS'
        ELSE 'FAIL'
    END AS test_result,
    COUNT(*) AS actual_count,
    @expected_count AS expected_count,
    CASE
        WHEN COUNT(*) = @expected_count THEN CONCAT('All ', @expected_count, ' rows replicated successfully')
        WHEN COUNT(*) > 0 THEN CONCAT('Partial replication: ', CAST(COUNT(*) AS VARCHAR(20)), ' of ', @expected_count, ' rows')
        ELSE 'No data replicated'
    END AS status_message
FROM dbo.t1;
GO

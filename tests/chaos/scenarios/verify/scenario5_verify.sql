-- Scenario 5: Verification - Check if high-volume data (5M rows) was replicated
SELECT 
    CASE 
        WHEN COUNT(*) = 2000000 THEN 'PASS'
        ELSE 'FAIL'
    END AS test_result,
    COUNT(*) AS actual_count,
    2000000 AS expected_count,
    CASE 
        WHEN COUNT(*) = 2000000 THEN 'All 2M rows replicated successfully'
        WHEN COUNT(*) > 0 THEN CONCAT('Partial replication: ', COUNT(*), ' of 2M rows')
        ELSE 'No data replicated'
    END AS status_message
FROM cdc_db.t1;

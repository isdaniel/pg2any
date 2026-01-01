-- Scenario 4: Verification - Check if mixed operations were replicated
-- Verify total count is reasonable and contains our marker value (999999)
SELECT 
    CASE 
        WHEN COUNT(*) >= 30 AND EXISTS (SELECT 1 FROM cdc_db.t1 WHERE val = 999999) 
        THEN 'PASS'
        ELSE 'FAIL'
    END AS test_result,
    COUNT(*) AS total_count,
    (SELECT COUNT(*) FROM cdc_db.t1 WHERE val = 999999) AS marker_count
FROM cdc_db.t1;

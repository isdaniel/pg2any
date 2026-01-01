-- Scenario 1: Verification - Check if 100 records were replicated
SELECT 
    CASE 
        WHEN COUNT(*) >= 100 THEN 'PASS'
        ELSE 'FAIL'
    END AS test_result,
    COUNT(*) AS actual_count,
    100 AS expected_count
FROM cdc_db.t1;

-- Scenario 3: Verification - Check if deletes were replicated
-- This scenario verifies that the delete operations from scenario 3 were correctly replicated
SELECT 
    CASE 
        WHEN COUNT(*) = 50000 THEN 'PASS'
        ELSE 'FAIL'
    END AS test_result,
    COUNT(*) AS actual_count,
    50000 AS  expected_count
FROM cdc_db.t1;

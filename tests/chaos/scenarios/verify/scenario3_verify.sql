-- Scenario 3: Verification - Check if deletes were replicated
-- Verify that some records were deleted (count should be less than 80)
SELECT 
    CASE 
        WHEN COUNT(*) = 50000 THEN 'PASS'
        ELSE 'FAIL'
    END AS test_result,
    COUNT(*) AS remaining_count
FROM cdc_db.t1;

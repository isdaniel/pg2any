-- Scenario 2: Verification - Check if updates were replicated
-- Count records where val >= 10000 (updated records)
SELECT 
    CASE 
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS test_result,
    COUNT(*) AS updated_count
FROM cdc_db.t1
WHERE val >= 10000;

-- GETTING SALARY PER HOUR FOR BRANCH 1
SELECT 
    year, 
    month, 
    branch_id, 
    ROUND(salary_per_hour_amount, 2) AS salary_per_hour_amount 
FROM 
    gold_layer.sum_branch_monthly 
WHERE 
    branch_id=1 
ORDER BY 2;

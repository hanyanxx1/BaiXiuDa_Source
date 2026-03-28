SELECT 
    COUNT(1) AS total_rows, 
    COUNT(DISTINCT telephone) AS unique_phones,
    (COUNT(1) - COUNT(DISTINCT telephone)) AS duplicate_rows,
    CONCAT(ROUND((COUNT(1) - COUNT(DISTINCT telephone)) / COUNT(1) * 100, 2), '%') AS duplicate_ratio
FROM customer_info;
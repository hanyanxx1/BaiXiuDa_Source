SELECT 
    COUNT(1) AS '总数据量', 
    COUNT(DISTINCT telephone) AS '唯一号码数',
    (COUNT(1) - COUNT(DISTINCT telephone)) AS '重复号码数',
    CONCAT(ROUND(COUNT(DISTINCT telephone) / COUNT(1) * 100, 2), '%') AS '唯一值占比',
    CONCAT(ROUND((COUNT(1) - COUNT(DISTINCT telephone)) / COUNT(1) * 100, 2), '%') AS '重复值占比'
FROM customer_info;
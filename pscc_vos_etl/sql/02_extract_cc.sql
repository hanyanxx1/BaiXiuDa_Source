-- ==========================================
-- [修改开始：SELECT 中提取 admin_id，WHERE 条件中改为动态占位符]
-- ==========================================
SELECT 
  c.admin_id AS 'cc_admin_id',
  c.telephone AS 'cc_telephone',
  c.start_stamp AS 'cc_start_time',
  c.duration AS 'cc_duration',
  c.consume AS 'cc_consume',
  c.status AS 'cc_status',
  g.name AS 'cc_gateway_name'
FROM 
  customer_cdr_{table_suffix} c  
LEFT JOIN 
  gateway g ON c.gateway_id = g.id
WHERE 
  c.start_stamp >= '{start_date} 00:00:00' 
  AND c.start_stamp <= '{start_date} 23:59:59' 
  AND c.answered = 1 
  AND c.admin_id = {current_admin_id};
-- ==========================================
-- [修改结束]
-- ==========================================
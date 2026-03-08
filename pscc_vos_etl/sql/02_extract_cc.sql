SELECT 
  c.admin_id AS 'cc_admin_id',
  c.telephone AS 'cc_telephone',
  c.start_stamp AS 'cc_start_time',
  c.billsec AS 'cc_billsec',       -- 核心时长字段
  c.consume AS 'cc_consume',       -- 👉 被你误删的金额字段，现在找回来了！
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
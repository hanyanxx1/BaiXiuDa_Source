-- 使用 DELIMITER // 更改语句结束符
DELIMITER //

-- 如果事件已存在，可以先删除它 (可选)
DROP EVENT IF EXISTS daily_export_calldata;

CREATE EVENT daily_export_calldata
ON SCHEDULE 
    EVERY 1 DAY            -- 每天重复执行
    STARTS '2025-12-25 22:00:00' -- 第一次执行时间示例 (请根据您的实际需求修改)
DO
BEGIN
    -- 声明两个变量
    DECLARE current_table_name VARCHAR(50); -- 用于存储 e_cdr_YYYYMMDD
    DECLARE export_path VARCHAR(255);       -- 用于存储完整的导出路径

    -- 1. 构造当前日期的表名字符串，例如 'e_cdr_20251215'
    SET current_table_name = CONCAT('e_cdr_', DATE_FORMAT(CURDATE(), '%Y%m%d'));

    -- 2. 构造完整的导出路径，例如 '/var/lib/mysql-files/e_cdr_20251215/'
    SET export_path = CONCAT('/var/lib/mysql-files/', current_table_name, '/all/');

    -- 3. 执行您的函数，传入动态生成的参数
    CALL ExportCallData(current_table_name, export_path, 'holdtime <= 0');
		
END //

-- 恢复语句结束符为分号
DELIMITER ;

SHOW EVENTS;
DELIMITER //

DROP PROCEDURE IF EXISTS ExportCallDataByDateRange //

CREATE PROCEDURE ExportCallDataByDateRange(
    IN start_date VARCHAR(8), 
    IN end_date VARCHAR(8),
    IN base_export_path VARCHAR(255),
    IN base_condition VARCHAR(500)
)
BEGIN
    DECLARE current_date_val DATE;
    DECLARE end_date_val DATE;
    DECLARE current_date_str VARCHAR(8);
    DECLARE target_table VARCHAR(50);
    DECLARE target_path VARCHAR(255);
    DECLARE table_exists INT;
    DECLARE normalized_path VARCHAR(255);
    
    -- 【关键修正】强制路径规范化，不再拼接日期子目录
    IF RIGHT(base_export_path, 1) = '/' THEN
        SET normalized_path = base_export_path;
    ELSE
        SET normalized_path = CONCAT(base_export_path, '/');
    END IF;

    SET current_date_val = STR_TO_DATE(start_date, '%Y%m%d');
    SET end_date_val = STR_TO_DATE(end_date, '%Y%m%d');

    WHILE current_date_val <= end_date_val DO
        SET current_date_str = DATE_FORMAT(current_date_val, '%Y%m%d');
        SET target_table = CONCAT('e_cdr_', current_date_str);
        
        -- 【方案 B 核心】target_path 直接指向根目录，不再带日期子文件夹
        -- 这样能绕过 MySQL 的 secure_file_priv 限制
        SET target_path = normalized_path;
        
        SELECT COUNT(*) INTO table_exists 
        FROM information_schema.tables 
        WHERE table_schema = DATABASE() AND table_name = target_table;
        
        IF table_exists > 0 THEN
            SELECT CONCAT('>>> 导出中 (根目录模式): ', target_table) AS '执行状态';
            -- 调用单表导出，传入根目录路径
            CALL ExportCallData(target_table, target_path, base_condition);
        ELSE
            SELECT CONCAT('--- 跳过不存在的表: ', target_table) AS '执行状态';
        END IF;

        SET current_date_val = DATE_ADD(current_date_val, INTERVAL 1 DAY);
    END WHILE;
    
    SELECT '全部文件已平铺导出至根目录！' AS '最终状态';
END //

DELIMITER ;
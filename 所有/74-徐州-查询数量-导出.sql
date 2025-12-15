-- 需求如下：
-- 1. 按照传入的whereCondition 过滤数据
-- 2. 分批导出e_cdr_XXXXXXXX 表中所有数据，导出到csv文件中，默认每个文件最大数量104万条
-- 3. 导出文件命名为：表名_partX.csv, X为文件序号，从1开始
-- 4. 在最后一行输出总数量

use vos3000;

-- 导出到CSV文件:
-- 创建导出存储过程
-- 1. 首先删除已存在的存储过程
DROP PROCEDURE IF EXISTS ExportCallData;
DROP PROCEDURE IF EXISTS ExportBatchData;

-- 2. 创建新的存储过程，接受表名和导出路径作为参数
DELIMITER //
CREATE PROCEDURE ExportCallData(IN table_name VARCHAR(50), IN export_path VARCHAR(255), IN base_condition VARCHAR(500))
BEGIN
    DECLARE batch_size INT DEFAULT 1040000; -- 每个文件最大104万条
    DECLARE where_condition VARCHAR(1000);
    DECLARE total_records INT;
    
    -- 设置默认值
    IF base_condition IS NULL OR base_condition = '' THEN
        SET where_condition = '1=1';
    ELSE
        SET where_condition = base_condition;
    END IF;
    
    -- 获取总记录数
    SET @count_sql = CONCAT('SELECT COUNT(*) INTO @total_records FROM ', table_name, ' WHERE ', where_condition);
    PREPARE stmt_count FROM @count_sql;
    EXECUTE stmt_count;
    DEALLOCATE PREPARE stmt_count;
    SET total_records = @total_records;
    
    -- 调用导出函数
    CALL ExportBatchData(
        table_name, 
        export_path, 
        where_condition, 
        table_name  -- 简化文件前缀，只使用表名
    );
    
    -- 输出导出信息和总数量
    SELECT CONCAT('导出完成，总记录数: ', total_records) AS '导出结果';
END //
DELIMITER ;

-- 创建批量导出数据的子过程
DELIMITER //
CREATE PROCEDURE ExportBatchData(
    IN table_name VARCHAR(50), 
    IN export_path VARCHAR(255), 
    IN where_condition VARCHAR(1000),
    IN file_prefix VARCHAR(255)
)
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE batch_size INT DEFAULT 1040000; -- 每个文件最大104万条
    DECLARE total_records INT;
    DECLARE num_batches INT;
    
    -- 动态计算总记录数和批次数
    SET @count_sql = CONCAT('SELECT COUNT(*) INTO @total_records FROM ', table_name, ' WHERE ', where_condition);
    PREPARE stmt_count FROM @count_sql;
    EXECUTE stmt_count;
    DEALLOCATE PREPARE stmt_count;
    
    SET total_records = @total_records;
    SET num_batches = CEILING(total_records / batch_size);
    
    -- 循环导出每个批次
    SET i = 0;
    
    batch_loop: WHILE i < num_batches DO
        -- 每一批都包含表头
        SET @sql = CONCAT(
            'SELECT ''主叫号码'',''被叫号码'',''起始时间'',''终止时间'',''主叫经由网关'' ',
            'UNION ALL ',
            'SELECT callere164, calleee164, ',
            'FROM_UNIXTIME(starttime/1000), FROM_UNIXTIME(stoptime/1000), callergatewayid ',
            'FROM ', table_name, ' WHERE ', where_condition, ' ',
            'LIMIT ', batch_size, ' OFFSET ', i * batch_size,
            ' INTO OUTFILE ''', export_path, '/', file_prefix, '_part', i+1, '.csv'' ',
            'FIELDS TERMINATED BY '','' ',
            'ENCLOSED BY ''"'' ',
            'ESCAPED BY ''\\\\'' ',
            'LINES TERMINATED BY ''\\n'''
        );
        
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        
        SET i = i + 1;
        DO SLEEP(1); -- 添加短暂延迟，避免文件系统冲突
    END WHILE batch_loop;
END //
DELIMITER ;

-- 74-徐州：
-- CALL ExportCallData('e_cdr_20250902', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250904', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250905', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250906', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250907', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250908', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250909', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250910', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250911', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250912', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250913', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250914', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250915', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250916', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250917', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250918', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250919', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250920', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250921', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250922', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250923', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250924', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250925', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250926', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250927', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250928', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250929', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20250930', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251001', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251002', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251003', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251004', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251005', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251006', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251007', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251008', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251009', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251010', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251011', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251012', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251013', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251014', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251015', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251016', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251017', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251018', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251019', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251020', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251021', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251022', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251023', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251024', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251025', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251026', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251027', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251028', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251029', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251030', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251031', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251101', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251102', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251103', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251104', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251105', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251106', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251107', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251108', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251109', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251110', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251111', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251112', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251113', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251114', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251115', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251116', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251117', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251118', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251119', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251120', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251121', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251122', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251123', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251124', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251125', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251126', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251127', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251128', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251129', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251130', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251201', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251202', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251203', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251204', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251205', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251206', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251207', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251208', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251209', '/var/lib/mysql-files/', 'holdtime <= 0');
-- CALL ExportCallData('e_cdr_20251210', '/var/lib/mysql-files/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251211', '/var/lib/mysql-files/e_cdr_20251211/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251212', '/var/lib/mysql-files/e_cdr_20251212/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251213', '/var/lib/mysql-files/e_cdr_20251213/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251214', '/var/lib/mysql-files/e_cdr_20251214/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251215', '/var/lib/mysql-files/e_cdr_20251215/', 'holdtime <= 0');

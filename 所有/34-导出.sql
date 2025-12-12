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

-- 34：
-- 继续生成后续日期的导出调用
CALL ExportCallData('e_cdr_20250830', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250831', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250901', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250902', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250903', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250904', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250905', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250906', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250907', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250908', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250909', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250910', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250911', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250912', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250913', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250914', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250915', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250916', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250917', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250918', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250919', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250920', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250921', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250922', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250923', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250924', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250925', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250926', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250927', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250928', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250929', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20250930', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251001', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251002', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251003', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251004', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251005', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251006', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251007', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251008', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251009', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251010', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251011', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251012', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251013', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251014', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251015', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251016', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251017', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251018', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251019', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251020', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251021', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251022', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251023', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251024', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251025', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251026', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251027', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251028', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251029', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251030', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251031', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251101', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251102', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251103', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251104', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251105', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251106', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251107', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251108', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251109', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251110', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251111', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251112', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251113', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251114', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251115', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251116', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251117', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251118', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251119', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251120', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251121', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251122', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251123', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251124', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251125', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251126', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251127', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251128', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251129', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251130', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251201', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251202', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251203', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251204', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251205', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251206', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251207', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251208', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251209', '/export_dir', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20251210', '/export_dir', 'holdtime <= 0');

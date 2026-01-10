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

CALL ExportCallData('e_cdr_20250101', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250102', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250103', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250104', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250105', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250106', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250107', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250108', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250109', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250110', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250111', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250112', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250113', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250114', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250115', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250116', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250117', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250118', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250119', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250120', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250121', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250122', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250123', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250124', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250125', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250126', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250127', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250128', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250129', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250130', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250131', '/export_dir', 'holdtime >= 120');

CALL ExportCallData('e_cdr_20250201', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250202', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250203', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250204', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250205', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250206', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250207', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250208', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250209', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250210', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250211', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250212', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250213', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250214', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250215', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250216', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250217', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250218', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250219', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250220', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250221', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250222', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250223', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250224', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250225', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250226', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250227', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250228', '/export_dir', 'holdtime >= 120');

CALL ExportCallData('e_cdr_20250301', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250302', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250303', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250304', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250305', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250306', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250307', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250308', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250309', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250310', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250311', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250312', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250313', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250314', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250315', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250316', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250317', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250318', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250319', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250320', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250321', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250322', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250323', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250324', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250325', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250326', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250327', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250328', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250329', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250330', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250331', '/export_dir', 'holdtime >= 120');

CALL ExportCallData('e_cdr_20250401', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250402', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250403', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250404', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250405', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250406', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250407', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250408', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250409', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250410', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250411', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250412', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250413', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250414', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250415', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250416', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250417', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250418', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250419', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250420', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250421', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250422', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250423', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250424', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250425', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250426', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250427', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250428', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250429', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250430', '/export_dir', 'holdtime >= 120');

CALL ExportCallData('e_cdr_20250501', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250502', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250503', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250504', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250505', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250506', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250507', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250508', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250509', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250510', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250511', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250512', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250513', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250514', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250515', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250516', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250517', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250518', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250519', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250520', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250521', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250522', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250523', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250524', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250525', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250526', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250527', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250528', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250529', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250530', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250531', '/export_dir', 'holdtime >= 120');

CALL ExportCallData('e_cdr_20250601', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250602', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250603', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250604', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250605', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250606', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250607', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250608', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250609', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250610', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250611', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250612', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250613', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250614', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250615', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250616', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250617', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250618', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250619', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250620', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250621', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250622', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250623', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250624', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250625', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250626', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250627', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250628', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250629', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250630', '/export_dir', 'holdtime >= 120');

CALL ExportCallData('e_cdr_20250701', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250702', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250703', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250704', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250705', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250706', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250707', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250708', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250709', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250710', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250711', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250712', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250713', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250714', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250715', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250716', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250717', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250718', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250719', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250720', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250721', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250722', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250723', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250724', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250725', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250726', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250727', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250728', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250729', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250730', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250731', '/export_dir', 'holdtime >= 120');

CALL ExportCallData('e_cdr_20250801', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250802', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250803', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250804', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250805', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250806', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250807', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250808', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250809', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250810', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250811', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250812', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250813', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250814', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250815', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250816', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250817', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250818', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250819', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250820', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250821', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250822', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250823', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250824', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250825', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250826', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250827', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250828', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250829', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250830', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250831', '/export_dir', 'holdtime >= 120');

CALL ExportCallData('e_cdr_20250901', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250902', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250903', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250904', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250905', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250906', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250907', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250908', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250909', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250910', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250911', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250912', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250913', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250914', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250915', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250916', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250917', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250918', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250919', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250920', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250921', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250922', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250923', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250924', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250925', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250926', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250927', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250928', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250929', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20250930', '/export_dir', 'holdtime >= 120');

CALL ExportCallData('e_cdr_20251001', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251002', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251003', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251004', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251005', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251006', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251007', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251008', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251009', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251010', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251011', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251012', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251013', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251014', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251015', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251016', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251017', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251018', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251019', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251020', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251021', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251022', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251023', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251024', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251025', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251026', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251027', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251028', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251029', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251030', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251031', '/export_dir', 'holdtime >= 120');

CALL ExportCallData('e_cdr_20251101', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251102', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251103', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251104', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251105', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251106', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251107', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251108', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251109', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251110', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251111', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251112', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251113', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251114', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251115', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251116', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251117', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251118', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251119', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251120', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251121', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251122', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251123', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251124', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251125', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251126', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251127', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251128', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251129', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251130', '/export_dir', 'holdtime >= 120');

CALL ExportCallData('e_cdr_20251201', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251202', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251203', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251204', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251205', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251206', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251207', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251208', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251209', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251210', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251211', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251212', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251213', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251214', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251215', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251216', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251217', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251218', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251219', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251220', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251221', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251222', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251223', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251224', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251225', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251226', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251227', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251228', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251229', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251230', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20251231', '/export_dir', 'holdtime >= 120');

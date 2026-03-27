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

-- CALL ExportCallData('e_cdr_20240101', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240102', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240103', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240104', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240105', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240106', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240107', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240108', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240109', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240110', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240111', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240112', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240113', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240114', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240115', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240116', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240117', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240118', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240119', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240120', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240121', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240122', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240123', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240124', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240125', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240126', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240127', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240128', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240129', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240130', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240131', '/export_dir', 'holdtime >= 120');
-- 
-- CALL ExportCallData('e_cdr_20240201', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240202', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240203', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240204', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240205', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240206', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240207', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240208', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240209', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240210', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240211', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240212', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240213', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240214', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240215', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240216', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240217', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240218', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240219', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240220', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240221', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240222', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240223', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240224', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240225', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240226', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240227', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240228', '/export_dir', 'holdtime >= 120');
-- 
-- CALL ExportCallData('e_cdr_20240301', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240302', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240303', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240304', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240305', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240306', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240307', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240308', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240309', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240310', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240311', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240312', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240313', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240314', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240315', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240316', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240317', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240318', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240319', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240320', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240321', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240322', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240323', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240324', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240325', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240326', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240327', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240328', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240329', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240330', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240331', '/export_dir', 'holdtime >= 120');
-- 
-- CALL ExportCallData('e_cdr_20240401', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240402', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240403', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240404', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240405', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240406', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240407', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240408', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240409', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240410', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240411', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240412', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240413', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240414', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240415', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240416', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240417', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240418', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240419', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240420', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240421', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240422', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240423', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240424', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240425', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240426', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240427', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240428', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240429', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240430', '/export_dir', 'holdtime >= 120');
-- 
-- CALL ExportCallData('e_cdr_20240501', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240502', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240503', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240504', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240505', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240506', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240507', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240508', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240509', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240510', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240511', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240512', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240513', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240514', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240515', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240516', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240517', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240518', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240519', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240520', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240521', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240522', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240523', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240524', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240525', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240526', '/export_dir', 'holdtime >= 120');
-- CALL ExportCallData('e_cdr_20240527', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240528', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240529', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240530', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240531', '/export_dir', 'holdtime >= 120');

CALL ExportCallData('e_cdr_20240601', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240602', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240603', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240604', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240605', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240606', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240607', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240608', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240609', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240610', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240611', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240612', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240613', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240614', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240615', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240616', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240617', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240618', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240619', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240620', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240621', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240622', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240623', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240624', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240625', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240626', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240627', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240628', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240629', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240630', '/export_dir', 'holdtime >= 120');

CALL ExportCallData('e_cdr_20240701', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240702', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240703', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240704', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240705', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240706', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240707', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240708', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240709', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240710', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240711', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240712', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240713', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240714', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240715', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240716', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240717', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240718', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240719', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240720', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240721', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240722', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240723', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240724', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240725', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240726', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240727', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240728', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240729', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240730', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240731', '/export_dir', 'holdtime >= 120');

CALL ExportCallData('e_cdr_20240801', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240802', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240803', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240804', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240805', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240806', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240807', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240808', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240809', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240810', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240811', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240812', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240813', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240814', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240815', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240816', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240817', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240818', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240819', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240820', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240821', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240822', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240823', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240824', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240825', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240826', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240827', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240828', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240829', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240830', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240831', '/export_dir', 'holdtime >= 120');

CALL ExportCallData('e_cdr_20240901', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240902', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240903', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240904', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240905', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240906', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240907', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240908', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240909', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240910', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240911', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240912', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240913', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240914', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240915', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240916', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240917', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240918', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240919', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240920', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240921', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240922', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240923', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240924', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240925', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240926', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240927', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240928', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240929', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20240930', '/export_dir', 'holdtime >= 120');

CALL ExportCallData('e_cdr_20241001', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241002', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241003', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241004', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241005', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241006', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241007', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241008', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241009', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241010', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241011', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241012', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241013', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241014', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241015', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241016', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241017', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241018', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241019', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241020', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241021', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241022', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241023', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241024', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241025', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241026', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241027', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241028', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241029', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241030', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241031', '/export_dir', 'holdtime >= 120');

CALL ExportCallData('e_cdr_20241101', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241102', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241103', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241104', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241105', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241106', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241107', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241108', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241109', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241110', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241111', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241112', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241113', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241114', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241115', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241116', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241117', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241118', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241119', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241120', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241121', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241122', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241123', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241124', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241125', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241126', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241127', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241128', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241129', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241130', '/export_dir', 'holdtime >= 120');

CALL ExportCallData('e_cdr_20241201', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241202', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241203', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241204', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241205', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241206', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241207', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241208', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241209', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241210', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241211', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241212', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241213', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241214', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241215', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241216', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241217', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241218', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241219', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241220', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241221', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241222', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241223', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241224', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241225', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241226', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241227', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241228', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241229', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241230', '/export_dir', 'holdtime >= 120');
CALL ExportCallData('e_cdr_20241231', '/export_dir', 'holdtime >= 120');

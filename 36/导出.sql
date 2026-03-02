-- 36：
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

CALL ExportCallData('e_cdr_20260227', '/var/lib/mysql-files/e_cdr_20260227/all/', 'holdtime <= 0');
-- 20260228
CALL ExportCallData('e_cdr_20260228', '/var/lib/mysql-files/e_cdr_20260228/all/', 'holdtime <= 0');

-- 20260301
CALL ExportCallData('e_cdr_20260301', '/var/lib/mysql-files/e_cdr_20260301/all/', 'holdtime <= 0');

-- 20260302
CALL ExportCallData('e_cdr_20260302', '/var/lib/mysql-files/e_cdr_20260302/all/', 'holdtime <= 0');

-- -- 20260303
-- CALL ExportCallData('e_cdr_20260303', '/var/lib/mysql-files/e_cdr_20260303/all/', 'holdtime <= 0');
-- 
-- -- 20260304
-- CALL ExportCallData('e_cdr_20260304', '/var/lib/mysql-files/e_cdr_20260304/all/', 'holdtime <= 0');
-- 
-- -- 20260305
-- CALL ExportCallData('e_cdr_20260305', '/var/lib/mysql-files/e_cdr_20260305/all/', 'holdtime <= 0');
-- 
-- -- 20260306
-- CALL ExportCallData('e_cdr_20260306', '/var/lib/mysql-files/e_cdr_20260306/all/', 'holdtime <= 0');
-- 
-- -- 20260307
-- CALL ExportCallData('e_cdr_20260307', '/var/lib/mysql-files/e_cdr_20260307/all/', 'holdtime <= 0');
-- 
-- -- 20260308
-- CALL ExportCallData('e_cdr_20260308', '/var/lib/mysql-files/e_cdr_20260308/all/', 'holdtime <= 0');
-- 
-- -- 20260309
-- CALL ExportCallData('e_cdr_20260309', '/var/lib/mysql-files/e_cdr_20260309/all/', 'holdtime <= 0');
-- 
-- -- 20260310
-- CALL ExportCallData('e_cdr_20260310', '/var/lib/mysql-files/e_cdr_20260310/all/', 'holdtime <= 0');
-- 
-- -- 20260311
-- CALL ExportCallData('e_cdr_20260311', '/var/lib/mysql-files/e_cdr_20260311/all/', 'holdtime <= 0');
-- 
-- -- 20260312
-- CALL ExportCallData('e_cdr_20260312', '/var/lib/mysql-files/e_cdr_20260312/all/', 'holdtime <= 0');
-- 
-- -- 20260313
-- CALL ExportCallData('e_cdr_20260313', '/var/lib/mysql-files/e_cdr_20260313/all/', 'holdtime <= 0');
-- 
-- -- 20260314
-- CALL ExportCallData('e_cdr_20260314', '/var/lib/mysql-files/e_cdr_20260314/all/', 'holdtime <= 0');
-- 
-- -- 20260315
-- CALL ExportCallData('e_cdr_20260315', '/var/lib/mysql-files/e_cdr_20260315/all/', 'holdtime <= 0');
-- 
-- -- 20260316
-- CALL ExportCallData('e_cdr_20260316', '/var/lib/mysql-files/e_cdr_20260316/all/', 'holdtime <= 0');
-- 
-- -- 20260317
-- CALL ExportCallData('e_cdr_20260317', '/var/lib/mysql-files/e_cdr_20260317/all/', 'holdtime <= 0');
-- 
-- -- 20260318
-- CALL ExportCallData('e_cdr_20260318', '/var/lib/mysql-files/e_cdr_20260318/all/', 'holdtime <= 0');
-- 
-- -- 20260319
-- CALL ExportCallData('e_cdr_20260319', '/var/lib/mysql-files/e_cdr_20260319/all/', 'holdtime <= 0');
-- 
-- -- 20260320
-- CALL ExportCallData('e_cdr_20260320', '/var/lib/mysql-files/e_cdr_20260320/all/', 'holdtime <= 0');
-- 
-- -- 20260321
-- CALL ExportCallData('e_cdr_20260321', '/var/lib/mysql-files/e_cdr_20260321/all/', 'holdtime <= 0');
-- 
-- -- 20260322
-- CALL ExportCallData('e_cdr_20260322', '/var/lib/mysql-files/e_cdr_20260322/all/', 'holdtime <= 0');
-- 
-- -- 20260323
-- CALL ExportCallData('e_cdr_20260323', '/var/lib/mysql-files/e_cdr_20260323/all/', 'holdtime <= 0');
-- 
-- -- 20260324
-- CALL ExportCallData('e_cdr_20260324', '/var/lib/mysql-files/e_cdr_20260324/all/', 'holdtime <= 0');
-- 
-- -- 20260325
-- CALL ExportCallData('e_cdr_20260325', '/var/lib/mysql-files/e_cdr_20260325/all/', 'holdtime <= 0');
-- 
-- -- 20260326
-- CALL ExportCallData('e_cdr_20260326', '/var/lib/mysql-files/e_cdr_20260326/all/', 'holdtime <= 0');
-- 
-- -- 20260327
-- CALL ExportCallData('e_cdr_20260327', '/var/lib/mysql-files/e_cdr_20260327/all/', 'holdtime <= 0');
-- 
-- -- 20260328
-- CALL ExportCallData('e_cdr_20260328', '/var/lib/mysql-files/e_cdr_20260328/all/', 'holdtime <= 0');
-- 
-- -- 20260329
-- CALL ExportCallData('e_cdr_20260329', '/var/lib/mysql-files/e_cdr_20260329/all/', 'holdtime <= 0');
-- 
-- -- 20260330
-- CALL ExportCallData('e_cdr_20260330', '/var/lib/mysql-files/e_cdr_20260330/all/', 'holdtime <= 0');
-- 
-- -- 20260331
-- CALL ExportCallData('e_cdr_20260331', '/var/lib/mysql-files/e_cdr_20260331/all/', 'holdtime <= 0');

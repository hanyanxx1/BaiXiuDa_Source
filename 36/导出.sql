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

-- 36：
CALL ExportCallData('e_cdr_20260113', '/var/lib/mysql-files/e_cdr_20260113/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260114', '/var/lib/mysql-files/e_cdr_20260114/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260115', '/var/lib/mysql-files/e_cdr_20260115/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260116', '/var/lib/mysql-files/e_cdr_20260116/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260117', '/var/lib/mysql-files/e_cdr_20260117/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260118', '/var/lib/mysql-files/e_cdr_20260118/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260119', '/var/lib/mysql-files/e_cdr_20260119/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260120', '/var/lib/mysql-files/e_cdr_20260120/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260121', '/var/lib/mysql-files/e_cdr_20260121/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260122', '/var/lib/mysql-files/e_cdr_20260122/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260123', '/var/lib/mysql-files/e_cdr_20260123/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260124', '/var/lib/mysql-files/e_cdr_20260124/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260125', '/var/lib/mysql-files/e_cdr_20260125/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260126', '/var/lib/mysql-files/e_cdr_20260126/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260127', '/var/lib/mysql-files/e_cdr_20260127/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260128', '/var/lib/mysql-files/e_cdr_20260128/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260129', '/var/lib/mysql-files/e_cdr_20260129/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260130', '/var/lib/mysql-files/e_cdr_20260130/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260131', '/var/lib/mysql-files/e_cdr_20260131/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260201', '/var/lib/mysql-files/e_cdr_20260201/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260202', '/var/lib/mysql-files/e_cdr_20260202/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260203', '/var/lib/mysql-files/e_cdr_20260203/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260204', '/var/lib/mysql-files/e_cdr_20260204/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260205', '/var/lib/mysql-files/e_cdr_20260205/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260206', '/var/lib/mysql-files/e_cdr_20260206/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260207', '/var/lib/mysql-files/e_cdr_20260207/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260208', '/var/lib/mysql-files/e_cdr_20260208/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260209', '/var/lib/mysql-files/e_cdr_20260209/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260210', '/var/lib/mysql-files/e_cdr_20260210/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260211', '/var/lib/mysql-files/e_cdr_20260211/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260212', '/var/lib/mysql-files/e_cdr_20260212/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260213', '/var/lib/mysql-files/e_cdr_20260213/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260214', '/var/lib/mysql-files/e_cdr_20260214/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260215', '/var/lib/mysql-files/e_cdr_20260215/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260216', '/var/lib/mysql-files/e_cdr_20260216/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260217', '/var/lib/mysql-files/e_cdr_20260217/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260218', '/var/lib/mysql-files/e_cdr_20260218/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260219', '/var/lib/mysql-files/e_cdr_20260219/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260220', '/var/lib/mysql-files/e_cdr_20260220/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260221', '/var/lib/mysql-files/e_cdr_20260221/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260222', '/var/lib/mysql-files/e_cdr_20260222/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260223', '/var/lib/mysql-files/e_cdr_20260223/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260224', '/var/lib/mysql-files/e_cdr_20260224/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260225', '/var/lib/mysql-files/e_cdr_20260225/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260226', '/var/lib/mysql-files/e_cdr_20260226/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260227', '/var/lib/mysql-files/e_cdr_20260227/all/', 'holdtime <= 0');
CALL ExportCallData('e_cdr_20260228', '/var/lib/mysql-files/e_cdr_20260228/all/', 'holdtime <= 0');

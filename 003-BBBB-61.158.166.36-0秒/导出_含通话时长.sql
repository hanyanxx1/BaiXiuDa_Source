-- 36：
-- 需求如下：
-- 1. 按照传入的whereCondition 过滤数据
-- 2. 分批导出e_cdr_XXXXXXXX 表中所有数据，导出到csv文件中，默认每个文件最大数量104万条
-- 3. 【全新修改】：导出文件命名为：表名_含通话时长_partX.csv, X为文件序号，从1开始
-- 4. 在最后一行输出总数量
-- 5. 【独立新增】：导出的字段和表头中，包含“通话时长” (holdtime)

use vos3000;

-- 导出到CSV文件:
-- 创建全新独立的导出存储过程，不影响原有脚本
-- 1. 首先删除已存在的同名新存储过程
DROP PROCEDURE IF EXISTS ExportCallData_WithHoldtime;
DROP PROCEDURE IF EXISTS ExportBatchData_WithHoldtime;

-- 2. 创建独立的主入口存储过程
DELIMITER //
CREATE PROCEDURE ExportCallData_WithHoldtime(IN table_name VARCHAR(50), IN export_path VARCHAR(255), IN base_condition VARCHAR(500))
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
    
    -- 调用全新独立的分批导出函数
    CALL ExportBatchData_WithHoldtime(
        table_name, 
        export_path, 
        where_condition, 
        table_name  -- 传入表名作为前缀
    );
    
    -- 输出导出信息和总数量
    SELECT CONCAT('导出完成(含通话时长)，总记录数: ', total_records) AS '导出结果';
END //
DELIMITER ;

-- 3. 创建全新独立的批量导出数据子过程 (表头包含通话时长，数据列包含 holdtime)
DELIMITER //
CREATE PROCEDURE ExportBatchData_WithHoldtime(
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
        -- 精准在表头增加 '通话时长'，数据列最后增加 holdtime 字段
        -- 【命名修改点】：INTO OUTFILE 处拼接了 '_含通话时长_part' 标识符
        SET @sql = CONCAT(
            'SELECT ''主叫号码'',''被叫号码'',''起始时间'',''终止时间'',''主叫经由网关'',''通话时长'' ',
            'UNION ALL ',
            'SELECT callere164, calleee164, ',
            'FROM_UNIXTIME(starttime/1000), FROM_UNIXTIME(stoptime/1000), callergatewayid, holdtime ',
            'FROM ', table_name, ' WHERE ', where_condition, ' ',
            'LIMIT ', batch_size, ' OFFSET ', i * batch_size,
            ' INTO OUTFILE ''', export_path, '/', file_prefix, '_含通话时长_part', i+1, '.csv'' ',
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

-- =========================================================
-- 【测试调用示例】
-- =========================================================
-- 执行下行后，将在指定目录下看到：e_cdr_20260331_含通话时长_part1.csv
-- CALL ExportCallData_WithHoldtime('e_cdr_20260331', '/var/lib/mysql-files/e_cdr_20260331/all/', 'holdtime <= 0');
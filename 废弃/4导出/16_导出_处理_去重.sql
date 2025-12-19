-- 需求如下：
-- 1. 按照传入的whereCondition 过滤数据
-- 2. 对calleee164 被叫号码进行如下处理
-- 2.1 如果calleee164 被叫号码以PASS开头，则从9开始，截取到末尾
-- 2.2 对其他的calleee164 都从5开始，截取到末尾
-- 2.3 将处理后的calleee164 被叫号码，的数据类型转换为长整型
-- 3. 对2处理后的数据结果，进行 calleee164 被叫号码进行去重，要求最后整个tableName中的calleee164 被叫号码不存在重复的
-- 4. 过滤掉calleee164 被叫号码中位数小于10位 和 位数大于11位的数据
-- 5. 对4处理后的结果数据进行乱序处理
-- 6. 分批导出e_cdr_XXXXXXXX 表中所有数据，导出到csv文件中，默认每个文件最大数量104万条
-- 7. 单表数量最多可达3000万条，请优化执行效率，尽可能减少执行时间
-- 8. 导出文件命名为：表名_distinct_partX.csv, X为文件序号，从1开始
-- 9. 全部执行完毕后，打印总数量
use vos3000;

-- 导出到CSV文件:
-- 创建导出存储过程
-- 1. 首先删除已存在的存储过程
DROP PROCEDURE IF EXISTS ExportDistinctCallData;
DROP PROCEDURE IF EXISTS ExportBatchData;

-- 2. 创建新的存储过程，接受表名和导出路径作为参数
DELIMITER //
CREATE PROCEDURE ExportDistinctCallData(IN table_name VARCHAR(50), IN export_path VARCHAR(255), IN whereCondition VARCHAR(500))
BEGIN
    DECLARE total_records INT;
    
    -- 1. 设置默认的 whereCondition
    IF whereCondition IS NULL OR whereCondition = '' THEN
        SET whereCondition = '1=1';
    END IF;
    
    -- 创建临时表用于存储处理后的数据
    DROP TEMPORARY TABLE IF EXISTS temp_processed_calls;
    
    -- 2. 处理 calleee164 被叫号码
    -- 2.1, 2.2, 2.3 处理号码格式并转换为长整型
    SET @process_sql = CONCAT('CREATE TEMPORARY TABLE temp_processed_calls AS 
        SELECT 
            id,
            callere164,
            CAST(
                CASE 
                    WHEN calleee164 LIKE ''PASS%'' THEN SUBSTRING(calleee164, 9)
                    ELSE SUBSTRING(calleee164, 5)
                END AS UNSIGNED
            ) AS calleee164
        FROM ', table_name, ' 
        WHERE ', whereCondition);
    
    PREPARE stmt_process FROM @process_sql;
    EXECUTE stmt_process;
    DEALLOCATE PREPARE stmt_process;
    
    -- 3. 对处理后的数据进行去重
    DROP TEMPORARY TABLE IF EXISTS temp_distinct_calls;
    CREATE TEMPORARY TABLE temp_distinct_calls AS
    SELECT MIN(id) as id, MIN(callere164) as callere164, calleee164
    FROM temp_processed_calls
    GROUP BY calleee164;
    
    -- 4. 过滤号码位数
    DROP TEMPORARY TABLE IF EXISTS temp_filtered_calls;
    CREATE TEMPORARY TABLE temp_filtered_calls AS
    SELECT *
    FROM temp_distinct_calls
    WHERE LENGTH(CAST(calleee164 AS CHAR)) >= 10
    AND LENGTH(CAST(calleee164 AS CHAR)) <= 11;
    
    -- 5. 对结果数据进行乱序处理
    DROP TEMPORARY TABLE IF EXISTS temp_shuffled_calls;
    CREATE TEMPORARY TABLE temp_shuffled_calls AS
    SELECT * FROM temp_filtered_calls ORDER BY RAND();
    
    -- 计算总记录数
    SELECT COUNT(*) INTO total_records FROM temp_shuffled_calls;
    
    -- 6. 调用批量导出函数进行分批导出
    CALL ExportBatchData(
        'temp_shuffled_calls', 
        export_path, 
        '', 
        CONCAT(table_name, '_distinct')
    );
    
    -- 清理临时表
    DROP TEMPORARY TABLE IF EXISTS temp_processed_calls;
    DROP TEMPORARY TABLE IF EXISTS temp_distinct_calls;
    DROP TEMPORARY TABLE IF EXISTS temp_filtered_calls;
    DROP TEMPORARY TABLE IF EXISTS temp_shuffled_calls;
    
    -- 9. 打印总数量
    SELECT CONCAT('成功导出 ', total_records, ' 条记录到 ', export_path, ' 目录') AS '导出结果';
    SELECT total_records AS '总数量';
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
    SET @count_sql = CONCAT('SELECT COUNT(*) INTO @total_records FROM ', table_name);
    
    -- 添加WHERE条件（如果有）
    IF where_condition != '' THEN
        SET @count_sql = CONCAT(@count_sql, ' WHERE ', where_condition);
    END IF;
    
    PREPARE stmt_count FROM @count_sql;
    EXECUTE stmt_count;
    DEALLOCATE PREPARE stmt_count;
    
    SET total_records = @total_records;
    SET num_batches = CEILING(total_records / batch_size);
    
    -- 循环导出每一批数据，每批都包含表头
    batch_loop: WHILE i < num_batches DO
        SET @sql = CONCAT(
            'SELECT ''主叫号码'',''被叫号码'' ',
            'UNION ALL ',
            'SELECT callere164, calleee164 ',
            'FROM ', table_name, ' ',
            IF(where_condition != '', CONCAT('WHERE ', where_condition, ' '), ''),
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
    END WHILE batch_loop;
    
    -- 输出总数量
    SELECT CONCAT('总导出记录数: ', total_records) AS '导出统计';
END //
DELIMITER ;

-- -- 34:
-- CALL ExportDistinctCallData('e_cdr_20250305', '/export_dir', 
--     'calleee164 NOT LIKE "%QIANHAO%" 
--     AND calleee164 NOT LIKE "%WuRaoHaoMa%" 
--     AND calleee164 NOT LIKE "%DONGTAIDIFANG%" 
--     AND calleee164 NOT LIKE "%/%" 
--     AND calleee164 NOT LIKE "%?%" 
--     AND calleee164 NOT LIKE "%,%" 
--     AND calleee164 NOT LIKE "%#%" 
--     AND calleee164 NOT LIKE "%\\\\%" 
--     AND calleee164 NOT LIKE "%*%" 
--     AND calleee164 NOT LIKE "%-%" 
--     AND holdtime <= 0 
--     AND callere164 != "27330" 
--     AND callere164 != "551"'
-- );

-- 35
-- CALL ExportDistinctCallData('e_cdr_20250427', '/export_dir', 
--     'calleee164 NOT LIKE "%QIANHAO%" 
--     AND calleee164 NOT LIKE "%WuRaoHaoMa%" 
--     AND calleee164 NOT LIKE "%DONGTAIDIFANG%" 
--     AND calleee164 NOT LIKE "%/%" 
--     AND calleee164 NOT LIKE "%?%" 
--     AND calleee164 NOT LIKE "%,%" 
--     AND calleee164 NOT LIKE "%#%" 
--     AND calleee164 NOT LIKE "%\\\\%" 
--     AND calleee164 NOT LIKE "%*%" 
--     AND calleee164 NOT LIKE "%-" 
--     AND holdtime <= 0 
--     AND callergatewayid = "陕西玖恒瑞账户对接"'
-- );
-- 
-- CALL ExportDistinctCallData('e_cdr_20250428', '/export_dir', 
--     'calleee164 NOT LIKE "%QIANHAO%" 
--     AND calleee164 NOT LIKE "%WuRaoHaoMa%" 
--     AND calleee164 NOT LIKE "%DONGTAIDIFANG%" 
--     AND calleee164 NOT LIKE "%/%" 
--     AND calleee164 NOT LIKE "%?%" 
--     AND calleee164 NOT LIKE "%,%" 
--     AND calleee164 NOT LIKE "%#%" 
--     AND calleee164 NOT LIKE "%\\\\%" 
--     AND calleee164 NOT LIKE "%*%" 
--     AND calleee164 NOT LIKE "%-" 
--     AND holdtime <= 0 
--     AND callergatewayid = "陕西玖恒瑞账户对接"'
-- );
-- 
-- CALL ExportDistinctCallData('e_cdr_20250429', '/export_dir', 
--     'calleee164 NOT LIKE "%QIANHAO%" 
--     AND calleee164 NOT LIKE "%WuRaoHaoMa%" 
--     AND calleee164 NOT LIKE "%DONGTAIDIFANG%" 
--     AND calleee164 NOT LIKE "%/%" 
--     AND calleee164 NOT LIKE "%?%" 
--     AND calleee164 NOT LIKE "%,%" 
--     AND calleee164 NOT LIKE "%#%" 
--     AND calleee164 NOT LIKE "%\\\\%" 
--     AND calleee164 NOT LIKE "%*%" 
--     AND calleee164 NOT LIKE "%-" 
--     AND holdtime <= 0 
--     AND callergatewayid = "陕西玖恒瑞账户对接"'
-- );
-- 
-- CALL ExportDistinctCallData('e_cdr_20250430', '/export_dir', 
--     'calleee164 NOT LIKE "%QIANHAO%" 
--     AND calleee164 NOT LIKE "%WuRaoHaoMa%" 
--     AND calleee164 NOT LIKE "%DONGTAIDIFANG%" 
--     AND calleee164 NOT LIKE "%/%" 
--     AND calleee164 NOT LIKE "%?%" 
--     AND calleee164 NOT LIKE "%,%" 
--     AND calleee164 NOT LIKE "%#%" 
--     AND calleee164 NOT LIKE "%\\\\%" 
--     AND calleee164 NOT LIKE "%*%" 
--     AND calleee164 NOT LIKE "%-" 
--     AND holdtime <= 0 
--     AND callergatewayid = "陕西玖恒瑞账户对接"'
-- );

-- CALL ExportDistinctCallData('e_cdr_20250501', '/export_dir', 
--     'calleee164 NOT LIKE "%QIANHAO%" 
--     AND calleee164 NOT LIKE "%WuRaoHaoMa%" 
--     AND calleee164 NOT LIKE "%DONGTAIDIFANG%" 
--     AND calleee164 NOT LIKE "%/%" 
--     AND calleee164 NOT LIKE "%?%" 
--     AND calleee164 NOT LIKE "%,%" 
--     AND calleee164 NOT LIKE "%#%" 
--     AND calleee164 NOT LIKE "%\\\\%" 
--     AND calleee164 NOT LIKE "%*%" 
--     AND calleee164 NOT LIKE "%-" 
--     AND holdtime <= 0 
--     AND callergatewayid = "陕西玖恒瑞账户对接"'
-- );

CALL ExportDistinctCallData('e_cdr_20250502', '/export_dir', 
    'calleee164 NOT LIKE "%QIANHAO%" 
    AND calleee164 NOT LIKE "%WuRaoHaoMa%" 
    AND calleee164 NOT LIKE "%DONGTAIDIFANG%" 
    AND calleee164 NOT LIKE "%/%" 
    AND calleee164 NOT LIKE "%?%" 
    AND calleee164 NOT LIKE "%,%" 
    AND calleee164 NOT LIKE "%#%" 
    AND calleee164 NOT LIKE "%\\\\%" 
    AND calleee164 NOT LIKE "%*%" 
    AND calleee164 NOT LIKE "%-" 
    AND holdtime <= 0 
    AND callergatewayid = "陕西玖恒瑞账户对接"'
);

CALL ExportDistinctCallData('e_cdr_20250503', '/export_dir', 
    'calleee164 NOT LIKE "%QIANHAO%" 
    AND calleee164 NOT LIKE "%WuRaoHaoMa%" 
    AND calleee164 NOT LIKE "%DONGTAIDIFANG%" 
    AND calleee164 NOT LIKE "%/%" 
    AND calleee164 NOT LIKE "%?%" 
    AND calleee164 NOT LIKE "%,%" 
    AND calleee164 NOT LIKE "%#%" 
    AND calleee164 NOT LIKE "%\\\\%" 
    AND calleee164 NOT LIKE "%*%" 
    AND calleee164 NOT LIKE "%-" 
    AND holdtime <= 0 
    AND callergatewayid = "陕西玖恒瑞账户对接"'
);

CALL ExportDistinctCallData('e_cdr_20250504', '/export_dir', 
    'calleee164 NOT LIKE "%QIANHAO%" 
    AND calleee164 NOT LIKE "%WuRaoHaoMa%" 
    AND calleee164 NOT LIKE "%DONGTAIDIFANG%" 
    AND calleee164 NOT LIKE "%/%" 
    AND calleee164 NOT LIKE "%?%" 
    AND calleee164 NOT LIKE "%,%" 
    AND calleee164 NOT LIKE "%#%" 
    AND calleee164 NOT LIKE "%\\\\%" 
    AND calleee164 NOT LIKE "%*%" 
    AND calleee164 NOT LIKE "%-" 
    AND holdtime <= 0 
    AND callergatewayid = "陕西玖恒瑞账户对接"'
);

CALL ExportDistinctCallData('e_cdr_20250505', '/export_dir', 
    'calleee164 NOT LIKE "%QIANHAO%" 
    AND calleee164 NOT LIKE "%WuRaoHaoMa%" 
    AND calleee164 NOT LIKE "%DONGTAIDIFANG%" 
    AND calleee164 NOT LIKE "%/%" 
    AND calleee164 NOT LIKE "%?%" 
    AND calleee164 NOT LIKE "%,%" 
    AND calleee164 NOT LIKE "%#%" 
    AND calleee164 NOT LIKE "%\\\\%" 
    AND calleee164 NOT LIKE "%*%" 
    AND calleee164 NOT LIKE "%-" 
    AND holdtime <= 0 
    AND callergatewayid = "陕西玖恒瑞账户对接"'
);

CALL ExportDistinctCallData('e_cdr_20250506', '/export_dir', 
    'calleee164 NOT LIKE "%QIANHAO%" 
    AND calleee164 NOT LIKE "%WuRaoHaoMa%" 
    AND calleee164 NOT LIKE "%DONGTAIDIFANG%" 
    AND calleee164 NOT LIKE "%/%" 
    AND calleee164 NOT LIKE "%?%" 
    AND calleee164 NOT LIKE "%,%" 
    AND calleee164 NOT LIKE "%#%" 
    AND calleee164 NOT LIKE "%\\\\%" 
    AND calleee164 NOT LIKE "%*%" 
    AND calleee164 NOT LIKE "%-" 
    AND holdtime <= 0 
    AND callergatewayid = "陕西玖恒瑞账户对接"'
);

CALL ExportDistinctCallData('e_cdr_20250507', '/export_dir', 
    'calleee164 NOT LIKE "%QIANHAO%" 
    AND calleee164 NOT LIKE "%WuRaoHaoMa%" 
    AND calleee164 NOT LIKE "%DONGTAIDIFANG%" 
    AND calleee164 NOT LIKE "%/%" 
    AND calleee164 NOT LIKE "%?%" 
    AND calleee164 NOT LIKE "%,%" 
    AND calleee164 NOT LIKE "%#%" 
    AND calleee164 NOT LIKE "%\\\\%" 
    AND calleee164 NOT LIKE "%*%" 
    AND calleee164 NOT LIKE "%-" 
    AND holdtime <= 0 
    AND callergatewayid = "陕西玖恒瑞账户对接"'
);

CALL ExportDistinctCallData('e_cdr_20250508', '/export_dir', 
    'calleee164 NOT LIKE "%QIANHAO%" 
    AND calleee164 NOT LIKE "%WuRaoHaoMa%" 
    AND calleee164 NOT LIKE "%DONGTAIDIFANG%" 
    AND calleee164 NOT LIKE "%/%" 
    AND calleee164 NOT LIKE "%?%" 
    AND calleee164 NOT LIKE "%,%" 
    AND calleee164 NOT LIKE "%#%" 
    AND calleee164 NOT LIKE "%\\\\%" 
    AND calleee164 NOT LIKE "%*%" 
    AND calleee164 NOT LIKE "%-" 
    AND holdtime <= 0 
    AND callergatewayid = "陕西玖恒瑞账户对接"'
);
-- 需求如下：
-- 1. 按照传入的whereCondition 过滤数据
-- 2. 对calleee164 被叫号码进行如下处理
-- 2.1 如果calleee164 被叫号码的长度等于16为, 则从第6位开始截取到末尾
-- 2.2 如果calleee164 被叫号码的长度等于15位，则从第5位开始截取到末尾
-- 2.3 将处理后的calleee164 被叫号码，的数据类型转换为长整型
-- 3. 对2处理后的数据结果，进行 calleee164 被叫号码进行去重，要求最后整个tableName中的calleee164 被叫号码不存在重复的
-- 4. 过滤掉calleee164 被叫号码中位数小于10位 和 位数大于11位的数据
-- 5. 对去重处理后的数据，按照callere164 主叫号码分组，第一列输出callere164 主叫号码分组，第二列输出对应的数量
-- 5.1 判断callere164 主叫号码分组数量是否大于50000条，如果大于50000条，则将该分组数据，分批导出到csv文件中，默认每个文件最大数量5万条，文件命名为：序号-callere164-0-月份.日期.csv
-- 5.2 判断callere164 主叫号码分组数量是否小于50000条，如果小于50000条，则与其他小于50000条的分组数据合并后，分批导出到csv文件中，默认每个文件最大数量5万条，文件命名为：序号-YYBB-0-月份.日期.csv
-- 5.3 导出的csv文件，列头为客户姓名、客户号码、地址、购买套数、签收电话、备注，其中客户号码列的内容为calleee164 被叫号码，其他列内容为空
-- 6. 对每个分组数据进行乱序处理
-- 7. 单表数量最多可达3000万条，请优化执行效率，尽可能减少执行时间
-- 8. 在最后一行输出总数量
use vos3000;

-- 导出到CSV文件:
-- 创建导出存储过程
-- 1. 首先删除已存在的存储过程
DROP PROCEDURE IF EXISTS ExportDistinctGroupedCallData;
DROP PROCEDURE IF EXISTS ExportBatchData;
DROP PROCEDURE IF EXISTS ProcessLargeGroups;

-- 创建处理大分组的子过程
DELIMITER //
CREATE PROCEDURE ProcessLargeGroups(IN export_path VARCHAR(255), IN table_name VARCHAR(50))
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE curr_callere164 VARCHAR(50);
    DECLARE curr_count INT;
    DECLARE large_group_cursor CURSOR FOR 
        SELECT callere164, record_count FROM temp_large_groups;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    OPEN large_group_cursor;
    
    large_group_loop: LOOP
        FETCH large_group_cursor INTO curr_callere164, curr_count;
        IF done THEN
            LEAVE large_group_loop;
        END IF;
        
        DROP TEMPORARY TABLE IF EXISTS temp_large_group_data;
        SET @create_large_group = CONCAT('
            CREATE TEMPORARY TABLE temp_large_group_data AS
            SELECT callere164, calleee164
            FROM temp_distinct_calls
            WHERE callere164 = ''', curr_callere164, '''
            ORDER BY RAND()
        ');
        
        PREPARE stmt_large_group FROM @create_large_group;
        EXECUTE stmt_large_group;
        DEALLOCATE PREPARE stmt_large_group;
        
        CALL ExportBatchData(
            'temp_large_group_data',
            export_path,
            '',
            curr_callere164
        );
        
        DROP TEMPORARY TABLE IF EXISTS temp_large_group_data;
    END LOOP;
    
    CLOSE large_group_cursor;
END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE ExportDistinctGroupedCallData(IN table_name VARCHAR(50), IN export_path VARCHAR(255), IN whereCondition VARCHAR(500))
BEGIN
    DECLARE total_records INT;
    DECLARE total_large_groups INT DEFAULT 0;
    DECLARE total_small_groups INT DEFAULT 0;
    DECLARE total_exported INT DEFAULT 0;
    
    IF whereCondition IS NULL OR whereCondition = '' THEN
        SET whereCondition = '1=1';
    END IF;
    
    DROP TEMPORARY TABLE IF EXISTS temp_distinct_calls;
    SET @create_temp_table = CONCAT('
        CREATE TEMPORARY TABLE temp_distinct_calls AS 
        SELECT 
            callere164,
            CAST(
                CASE 
                    WHEN LENGTH(calleee164) = 16 THEN SUBSTRING(calleee164, 6)
                    WHEN LENGTH(calleee164) = 15 THEN SUBSTRING(calleee164, 5)
                    ELSE calleee164
                END AS UNSIGNED
            ) AS calleee164
        FROM ', table_name, ' 
        WHERE ', whereCondition, '
        GROUP BY 
            CAST(
                CASE 
                    WHEN LENGTH(calleee164) = 16 THEN SUBSTRING(calleee164, 6)
                    WHEN LENGTH(calleee164) = 15 THEN SUBSTRING(calleee164, 5)
                    ELSE calleee164
                END AS UNSIGNED
            )
        HAVING calleee164 IS NOT NULL 
            AND LENGTH(CAST(calleee164 AS CHAR)) >= 10
            AND LENGTH(CAST(calleee164 AS CHAR)) <= 11'
    );
    
    PREPARE stmt_create_temp FROM @create_temp_table;
    EXECUTE stmt_create_temp;
    DEALLOCATE PREPARE stmt_create_temp;
    
    DROP TEMPORARY TABLE IF EXISTS temp_caller_groups;
    CREATE TEMPORARY TABLE temp_caller_groups AS
    SELECT 
        callere164,
        COUNT(*) AS record_count
    FROM temp_distinct_calls
    GROUP BY callere164;
    
    DROP TEMPORARY TABLE IF EXISTS temp_large_groups;
    CREATE TEMPORARY TABLE temp_large_groups AS
    SELECT callere164, record_count
    FROM temp_caller_groups
    WHERE record_count >= 50000;
    
    DROP TEMPORARY TABLE IF EXISTS temp_small_groups;
    CREATE TEMPORARY TABLE temp_small_groups AS
    SELECT callere164, record_count
    FROM temp_caller_groups
    WHERE record_count < 50000;
    
    SELECT COUNT(*) INTO total_large_groups FROM temp_large_groups;
    SELECT COUNT(*) INTO total_small_groups FROM temp_small_groups;
    
    IF total_large_groups > 0 THEN
        CALL ProcessLargeGroups(export_path, table_name);
        SELECT SUM(record_count) INTO @large_groups_total FROM temp_large_groups;
        SET total_exported = total_exported + @large_groups_total;
    END IF;
    
    IF total_small_groups > 0 THEN
        DROP TEMPORARY TABLE IF EXISTS temp_small_groups_data;
        CREATE TEMPORARY TABLE temp_small_groups_data AS
        SELECT dc.callere164, dc.calleee164
        FROM temp_distinct_calls dc
        JOIN temp_small_groups sg ON dc.callere164 = sg.callere164
        ORDER BY RAND();
        
        SELECT COUNT(*) INTO @small_groups_total FROM temp_small_groups_data;
        SET total_exported = total_exported + @small_groups_total;
        
        CALL ExportBatchData(
            'temp_small_groups_data',
            export_path,
            '',
            'CCCC'
        );
        
        DROP TEMPORARY TABLE IF EXISTS temp_small_groups_data;
    END IF;
    
    SELECT COUNT(*) INTO total_records FROM temp_distinct_calls;
    
    DROP TEMPORARY TABLE IF EXISTS temp_distinct_calls;
    DROP TEMPORARY TABLE IF EXISTS temp_caller_groups;
    DROP TEMPORARY TABLE IF EXISTS temp_large_groups;
    DROP TEMPORARY TABLE IF EXISTS temp_small_groups;
    
    SELECT total_records AS '总记录数';
END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE ExportBatchData(
    IN table_name VARCHAR(50), 
    IN export_path VARCHAR(255), 
    IN where_condition VARCHAR(1000),
    IN file_prefix VARCHAR(255)
)
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE batch_size INT DEFAULT 50000;
    DECLARE total_records INT;
    DECLARE num_batches INT;
    DECLARE curr_date VARCHAR(10);
    
    SET curr_date = DATE_FORMAT(NOW(), '%m.%d');
    
    SET @count_sql = CONCAT('SELECT COUNT(*) INTO @total_records FROM ', table_name);
    
    IF where_condition != '' THEN
        SET @count_sql = CONCAT(@count_sql, ' WHERE ', where_condition);
    END IF;
    
    PREPARE stmt_count FROM @count_sql;
    EXECUTE stmt_count;
    DEALLOCATE PREPARE stmt_count;
    
    SET total_records = @total_records;
    SET num_batches = CEILING(total_records / batch_size);
    
    batch_loop: WHILE i < num_batches DO
        SET @sql = CONCAT(
            'SELECT ''客户姓名'',''客户号码'',''地址'',''购买套数'',''签收电话'',''备注'' ',
            'UNION ALL ',
            'SELECT '''', calleee164, '''', '''', '''', '''' ',
            'FROM ', table_name, ' ',
            IF(where_condition != '', CONCAT('WHERE ', where_condition, ' '), ''),
            'LIMIT ', batch_size, ' OFFSET ', i * batch_size,
            ' INTO OUTFILE ''', export_path, '/', 
            i+1, '-', 
            file_prefix,
            '-0-', curr_date, '.csv'' ',
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
END //
DELIMITER;

-- 249
CALL ExportDistinctGroupedCallData('e_cdr_20250705', '/export_dir', 
    'calleee164 NOT LIKE "%QIANHAO%" 
    AND calleee164 NOT LIKE "%WuRaoHaoMa%" 
    AND calleee164 NOT LIKE "%DONGTAIDIFANG%" 
    AND calleee164 NOT LIKE "%/%" 
    AND calleee164 NOT LIKE "%?%" 
    AND calleee164 NOT LIKE "%,%" 
    AND calleee164 NOT LIKE "%#%" 
    AND calleee164 NOT LIKE "%\\\\%" 
    AND calleee164 NOT LIKE "%*%" 
    AND calleee164 NOT LIKE "%-%" 
    AND holdtime <= 0'
);
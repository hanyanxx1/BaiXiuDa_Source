-- 需求如下：
-- 1. 按照传入的whereCondition 过滤数据
-- 2. 对calleee164 被叫号码进行如下处理：从最后一位开始，向前截取11位
-- 3. 进行 calleee164 被叫号码去重 (保留该号码最大通话时长)
-- 4. 按照holdtime通话时长降序排序
-- 5. 导出一个新的csv文件，格式：第一列序号(自增)、第二列号码、第三列时长
-- 6. 在最后一行输出总数量

USE vos3000;

-- 1. 首先删除可能已存在的同名存储过程
DROP PROCEDURE IF EXISTS ExportRankedDurationData;

-- 2. 创建独立的主入口存储过程
DELIMITER //
CREATE PROCEDURE ExportRankedDurationData(
    IN table_name VARCHAR(50), 
    IN export_path VARCHAR(255), 
    IN where_condition VARCHAR(1000),
    IN file_name VARCHAR(255)
)
BEGIN
    DECLARE total_records INT;
    
    -- 设置默认过滤条件
    IF where_condition IS NULL OR where_condition = '' THEN
        SET where_condition = '1=1';
    END IF;

    -- 清理可能残留的临时表
    DROP TEMPORARY TABLE IF EXISTS temp_grouped_calls;
    DROP TEMPORARY TABLE IF EXISTS temp_sorted_calls;

    -- 步骤 2 & 3: 截取号码、正则清洗并去重 (保留该号码的最大时长)
    SET @create_group_sql = CONCAT('
        CREATE TEMPORARY TABLE temp_grouped_calls AS 
        SELECT 
            RIGHT(TRIM(calleee164), 11) AS phone_number,
            MAX(holdtime) AS duration
        FROM ', table_name, ' 
        WHERE ', where_condition, '
        GROUP BY RIGHT(TRIM(calleee164), 11)
        HAVING phone_number REGEXP ''^[0-9]{11}$''
    ');
    
    PREPARE stmt_group FROM @create_group_sql;
    EXECUTE stmt_group;
    DEALLOCATE PREPARE stmt_group;

    -- 步骤 4 & 5: 按时长降序排序，并生成自增序号
    SET @row_num = 0;
    CREATE TEMPORARY TABLE temp_sorted_calls AS 
    SELECT 
        (@row_num := @row_num + 1) AS seq_no,
        phone_number,
        duration
    FROM temp_grouped_calls
    ORDER BY duration DESC;

    -- 记录总数量以备最后输出
    SELECT COUNT(*) INTO total_records FROM temp_sorted_calls;

    -- 导出至 CSV，使用 UNION ALL 拼装表头
    -- 子查询中包含 LIMIT 100000000 是为了强制 MySQL 保留内部的排序顺序
    SET @export_sql = CONCAT(
        'SELECT ''序号'',''号码'',''时长'' ',
        'UNION ALL ',
        'SELECT * FROM ( ',
        '   SELECT CAST(seq_no AS CHAR) AS c1, phone_number AS c2, CAST(duration AS CHAR) AS c3 ',
        '   FROM temp_sorted_calls ',
        '   ORDER BY seq_no ASC LIMIT 100000000 ',
        ') AS tmp ',
        'INTO OUTFILE ''', export_path, '/', file_name, '.csv'' ',
        'FIELDS TERMINATED BY '','' ',
        'ENCLOSED BY '''' ',
        'ESCAPED BY ''\\\\'' ',
        'LINES TERMINATED BY ''\\n'''
    );
    
    PREPARE stmt_export FROM @export_sql;
    EXECUTE stmt_export;
    DEALLOCATE PREPARE stmt_export;

    -- 清除内存中的临时表
    DROP TEMPORARY TABLE IF EXISTS temp_grouped_calls;
    DROP TEMPORARY TABLE IF EXISTS temp_sorted_calls;

    -- 步骤 6: 最后一行输出总数量
    SELECT CONCAT('导出完成，总记录数: ', total_records) AS '执行结果';

END //
DELIMITER ;

-- =========================================================
-- 【测试调用示例】
-- =========================================================
-- 假设你想导出 20260401 的数据，并命名为 e_cdr_20260401_时长排序降序
-- CALL ExportRankedDurationData(
--    'e_cdr_20260401', 
--    '/var/lib/mysql-files/001-AAAA-112.25.240.74-0秒/', 
--    'holdtime > 0', 
--    'e_cdr_20260401_时长排序降序'
-- );
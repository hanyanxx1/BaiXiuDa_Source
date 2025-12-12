-- 需求如下：
-- 1. 按照传入的whereCondition 过滤数据
-- 2. 对calleee164 被叫号码进行如下处理
-- 2.1 如果calleee164 被叫号码以PASS开头，则从9开始，截取到末尾
-- 2.2 对其他的calleee164 都从5开始，截取到末尾
-- 2.3 将处理后的calleee164 被叫号码，的数据类型转换为长整型
-- 3. 对2处理后的数据结果，进行 calleee164 被叫号码进行去重，要求最后整个tableName中的calleee164 被叫号码不存在重复的
-- 4. 过滤掉calleee164 被叫号码中位数小于10位 和 位数大于11位的数据
use vos3000;

-- 删除已存在的存储过程
DROP PROCEDURE IF EXISTS GetDistinctCallDataCount;

-- 创建获取去重后数据总数的存储过程
DELIMITER //
CREATE PROCEDURE GetDistinctCallDataCount(
    IN tableName VARCHAR(100),
    IN whereCondition VARCHAR(500)
)
BEGIN
    DECLARE countQuery TEXT;
    
    -- 设置默认值
    IF whereCondition IS NULL OR whereCondition = '' THEN
        SET whereCondition = '1=1';
    END IF;
    
    -- 移除索引检查和创建部分，因为查询只执行一次
    
    -- 构建计数SQL查询
    SET countQuery = CONCAT('
        SELECT COUNT(*) AS ''去重后总数量''
        FROM (
            -- 4. 过滤号码位数
            SELECT processed_calleee164
            FROM (
                -- 3. 对处理后的数据进行去重
                SELECT DISTINCT processed_calleee164
                FROM (
                    -- 2. 对calleee164进行处理
                    SELECT 
                        CAST(   -- 2.3 转换为长整型
                            CASE -- 2.1 和 2.2 处理calleee164
                                WHEN calleee164 LIKE ''PASS%'' THEN SUBSTRING(calleee164, 9)
                                ELSE SUBSTRING(calleee164, 5)
                            END AS UNSIGNED
                        ) AS processed_calleee164
                    FROM ', tableName, '
                    -- 1. 首先按照传入的whereCondition过滤数据
                    WHERE ', whereCondition, '
                ) AS processed_data
                WHERE processed_calleee164 IS NOT NULL 
                    AND processed_calleee164 != ''''
            ) AS distinct_data
            WHERE LENGTH(CAST(processed_calleee164 AS CHAR)) BETWEEN 10 AND 11
        ) AS filtered_length_data');
    
    -- 执行查询
    SET @sql = countQuery;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //
DELIMITER ;

-- 34:
CALL GetDistinctCallDataCount(
    'e_cdr_20250424', 
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

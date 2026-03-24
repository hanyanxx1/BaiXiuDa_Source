-- 需求如下：
-- 1. 按照传入的whereCondition 过滤数据
-- 2. 对calleee164 被叫号码进行如下处理
-- 2.1 从最后一位开始，向前截取11位
-- 3. 对2处理后的数据结果，进行 calleee164 被叫号码进行去重，要求最后整个tableName中的calleee164 被叫号码不存在重复的

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
        SET whereCondition = 'holdtime <= 0 AND LENGTH(TRIM(calleee164)) > 0';
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
                        RIGHT(TRIM(calleee164), 11) AS processed_calleee164
                    FROM ', tableName, '
                    -- 1. 首先按照传入的whereCondition过滤数据
                    WHERE ', whereCondition, '
                ) AS processed_data
                WHERE processed_calleee164 IS NOT NULL 
                    AND processed_calleee164 != ''''
            ) AS distinct_data
            WHERE processed_calleee164 REGEXP ''^[0-9]{11}$''
        ) AS filtered_length_data');
    
    -- 执行查询
    SET @sql = countQuery;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //
DELIMITER;

-- 74:
CALL GetDistinctCallDataCount('e_cdr_20251224', 'holdtime <= 0');


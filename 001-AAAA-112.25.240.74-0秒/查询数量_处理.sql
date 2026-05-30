-- 需求如下：
-- 1. 按照传入的whereCondition 过滤数据
-- 2. 对calleee164 被叫号码进行如下处理
-- 2.1 从最后一位开始，向前截取11位
-- 3. 对2处理后的数据结果，进行 calleee164 被叫号码进行去重，要求最后整个tableName中的calleee164 被叫号码不存在重复的
USE vos3000;

DROP PROCEDURE IF EXISTS GetDistinctCallDataCount;

DELIMITER //
CREATE PROCEDURE GetDistinctCallDataCount(
    IN tableName VARCHAR(100),
    IN whereCondition VARCHAR(500)
)
BEGIN
    DECLARE countQuery TEXT;
    
    SET countQuery = CONCAT('
        SELECT COUNT(*) AS ''去重后总数量''
        FROM (
            SELECT DISTINCT RIGHT(TRIM(calleee164), 11) AS processed_calleee164
            FROM ', tableName,
            IF(whereCondition IS NULL OR whereCondition = '',
               '',
               CONCAT(' WHERE ', whereCondition)),
            '
        ) AS t
        WHERE processed_calleee164 REGEXP ''^[0-9]{11}$''
    ');
    
    SET @sql = countQuery;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //
DELIMITER ;
-- 74:
CALL GetDistinctCallDataCount('e_cdr_20251224', 'holdtime <= 0');


-- 需求如下：
-- 1. calleee164 被叫号码进行去重，要求最后整个tableName中的calleee164 被叫号码不存在重复的，考虑效率问题

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
    IF whereCondition IS NULL THEN
        SET whereCondition = '';
    END IF;
    
    -- 检查索引是否存在
    SET @checkIndex = CONCAT('SELECT COUNT(1) INTO @indexExists FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ''', tableName, ''' AND index_name = ''idx_calleee164''');
    PREPARE checkStmt FROM @checkIndex;
    EXECUTE checkStmt;
    DEALLOCATE PREPARE checkStmt;
    
    -- 如果索引不存在，则创建索引以提高效率
    IF @indexExists = 0 THEN
        SET @indexSql = CONCAT('CREATE INDEX idx_calleee164 ON ', tableName, '(calleee164)');
        PREPARE indexStmt FROM @indexSql;
        EXECUTE indexStmt;
        DEALLOCATE PREPARE indexStmt;
    END IF;
    
    -- 构建计数SQL查询，使用COUNT(DISTINCT)去重计数
    SET countQuery = CONCAT('SELECT COUNT(DISTINCT calleee164) AS ''去重后总数量'' FROM ', tableName);
    
    -- 添加WHERE条件（如果有）
    IF whereCondition != '' THEN
        SET countQuery = CONCAT(countQuery, ' WHERE ', whereCondition);
    END IF;
    
    -- 执行查询
    SET @sql = countQuery;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //
DELIMITER ;

-- 使用示例:
-- 查询表中去重后的数据总数:
-- CALL GetDistinctCallDataCount('e_cdr_20250227', '');

-- 查询符合条件的去重后数据总数:
-- CALL GetDistinctCallDataCount('e_cdr_20250227', 'holdtime >= 0');

CALL GetDistinctCallDataCount('e_cdr_20250316', 'holdtime <= 0');
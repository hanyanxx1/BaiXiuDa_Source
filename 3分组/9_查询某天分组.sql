-- 需求如下：
-- 1. 分组，按照callere164 主叫号码分组，第一列输出callere164 主叫号码分组，第二列输出对应的数量
-- 2. 在最后一行输出总数量

use vos3000;

-- 删除已存在的存储过程
DROP PROCEDURE IF EXISTS GetCallDataGrouped;

-- 创建存储过程，接受表名和条件参数
DELIMITER //
CREATE PROCEDURE GetCallDataGrouped(
    IN tableName VARCHAR(100),
    IN whereCondition VARCHAR(500)
)
BEGIN
    DECLARE sqlQuery TEXT;
    DECLARE totalQuery TEXT;
    
    -- 设置默认值
    IF whereCondition IS NULL THEN
        SET whereCondition = '';
    END IF;
    
    -- 检查索引是否存在（主叫号码索引）
    SET @checkIndex = CONCAT('SELECT COUNT(1) INTO @indexExists FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ''', tableName, ''' AND index_name = ''idx_callere164''');
    PREPARE checkStmt FROM @checkIndex;
    EXECUTE checkStmt;
    DEALLOCATE PREPARE checkStmt;
    
    -- 如果索引不存在，则创建索引以提高效率
    IF @indexExists = 0 THEN
        SET @indexSql = CONCAT('CREATE INDEX idx_callere164 ON ', tableName, '(callere164)');
        PREPARE indexStmt FROM @indexSql;
        EXECUTE indexStmt;
        DEALLOCATE PREPARE indexStmt;
    END IF;
    
    -- 构建SQL查询，按主叫号码分组并计数
    SET sqlQuery = CONCAT('SELECT 
        callere164 as ''主叫号码'',
        COUNT(*) as ''数量''
    FROM ', tableName);
    
    -- 添加WHERE条件（如果有）
    IF whereCondition != '' THEN
        SET sqlQuery = CONCAT(sqlQuery, ' WHERE ', whereCondition);
    END IF;
    
    -- 添加GROUP BY实现分组
    SET sqlQuery = CONCAT(sqlQuery, ' GROUP BY callere164 ORDER BY COUNT(*) DESC');
    
    -- 执行查询
    SET @sql = sqlQuery;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    -- 构建总数量查询
    SET totalQuery = CONCAT('SELECT ''总计'' as ''主叫号码'', COUNT(*) as ''数量'' FROM ', tableName);
    
    -- 添加WHERE条件（如果有）
    IF whereCondition != '' THEN
        SET totalQuery = CONCAT(totalQuery, ' WHERE ', whereCondition);
    END IF;
    
    -- 执行总数量查询
    SET @totalSql = totalQuery;
    PREPARE totalStmt FROM @totalSql;
    EXECUTE totalStmt;
    DEALLOCATE PREPARE totalStmt;
END //
DELIMITER ;

-- 使用示例:
-- 基本用法:
-- CALL GetCallDataGrouped('e_cdr_20250227', '');

-- 带条件查询:
-- CALL GetCallDataGrouped('e_cdr_20250227', 'holdtime > 0');

CALL GetCallDataGrouped('e_cdr_20250316', 'holdtime <= 0');

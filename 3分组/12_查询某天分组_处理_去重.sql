-- 需求如下：
-- 1. 过滤掉 被叫号码 calleee164 中包含 "QIANHAO" 、 "WuRaoHaoMa" 、"DONGTAIDIFANG" 的数据项
-- 2. calleee164 被叫号码进行去重，要求最后整个tableName中的calleee164 被叫号码不存在重复的，考虑效率问题
-- 3. 分组，按照callere164 主叫号码分组，第一列输出callere164 主叫号码分组，第二列输出对应的数量
-- 4. 在最后一行输出总数量
use vos3000;

-- 删除已存在的存储过程
DROP PROCEDURE IF EXISTS GetDistinctCallDataGrouped;

-- 创建存储过程，接受表名和条件参数
DELIMITER //
CREATE PROCEDURE GetDistinctCallDataGrouped(
    IN tableName VARCHAR(100),
    IN whereCondition VARCHAR(500)
)
BEGIN
    DECLARE sqlQuery TEXT;
    DECLARE totalQuery TEXT;
    DECLARE tempTableName VARCHAR(100);
    DECLARE finalCondition TEXT;
    
    -- 设置默认值
    IF whereCondition IS NULL THEN
        SET whereCondition = '';
    END IF;
    
    -- 添加过滤条件：排除包含"QIANHAO"、"WuRaoHaoMa"和"DONGTAIDIFANG"的被叫号码
    SET finalCondition = 'calleee164 NOT LIKE "%QIANHAO%" AND calleee164 NOT LIKE "%WuRaoHaoMa%" AND calleee164 NOT LIKE "%DONGTAIDIFANG%"';
    
    -- 如果有其他条件，则合并条件
    IF whereCondition != '' THEN
        SET finalCondition = CONCAT(finalCondition, ' AND (', whereCondition, ')');
    END IF;
    
    -- 创建临时表名
    SET tempTableName = CONCAT('temp_distinct_', tableName);
    
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
    
    -- 检查被叫号码索引是否存在
    SET @checkIndex2 = CONCAT('SELECT COUNT(1) INTO @indexExists2 FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ''', tableName, ''' AND index_name = ''idx_calleee164''');
    PREPARE checkStmt2 FROM @checkIndex2;
    EXECUTE checkStmt2;
    DEALLOCATE PREPARE checkStmt2;
    
    -- 如果被叫号码索引不存在，则创建索引
    IF @indexExists2 = 0 THEN
        SET @indexSql2 = CONCAT('CREATE INDEX idx_calleee164 ON ', tableName, '(calleee164)');
        PREPARE indexStmt2 FROM @indexSql2;
        EXECUTE indexStmt2;
        DEALLOCATE PREPARE indexStmt2;
    END IF;
    
    -- 先创建临时表，存储去重后的被叫号码数据
    SET @dropTempTable = CONCAT('DROP TEMPORARY TABLE IF EXISTS ', tempTableName);
    PREPARE dropStmt FROM @dropTempTable;
    EXECUTE dropStmt;
    DEALLOCATE PREPARE dropStmt;
    
    -- 创建临时表，只保留去重后的被叫号码记录
    SET @createTempTable = CONCAT('CREATE TEMPORARY TABLE ', tempTableName, ' AS 
        SELECT callere164, calleee164 FROM ', tableName, ' WHERE ', finalCondition);
    
    -- 添加GROUP BY实现去重
    SET @createTempTable = CONCAT(@createTempTable, ' GROUP BY calleee164');
    
    PREPARE createTempStmt FROM @createTempTable;
    EXECUTE createTempStmt;
    DEALLOCATE PREPARE createTempStmt;
    
    -- 构建SQL查询，按主叫号码分组并计数
    SET sqlQuery = CONCAT('SELECT 
        callere164 as ''主叫号码'',
        COUNT(*) as ''被叫号码数量''
    FROM ', tempTableName);
    
    -- 添加GROUP BY实现分组
    SET sqlQuery = CONCAT(sqlQuery, ' GROUP BY callere164 ORDER BY COUNT(*) DESC');
    
    -- 执行查询
    SET @sql = sqlQuery;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    -- 构建总数量查询
    SET totalQuery = CONCAT('SELECT ''总计'' as ''主叫号码'', COUNT(*) as ''被叫号码数量'' FROM ', tempTableName);
    
    -- 执行总数量查询
    SET @totalSql = totalQuery;
    PREPARE totalStmt FROM @totalSql;
    EXECUTE totalStmt;
    DEALLOCATE PREPARE totalStmt;
    
    -- 清理临时表
    SET @dropTempTable = CONCAT('DROP TEMPORARY TABLE IF EXISTS ', tempTableName);
    PREPARE dropStmt FROM @dropTempTable;
    EXECUTE dropStmt;
    DEALLOCATE PREPARE dropStmt;
END //
DELIMITER ;

-- 使用示例:
-- 基本用法:
-- CALL GetDistinctCallDataGrouped('e_cdr_20250227', '');

-- 带条件查询:
-- CALL GetDistinctCallDataGrouped('e_cdr_20250227', 'holdtime > 0');

CALL GetDistinctCallDataGrouped('e_cdr_20250317', 'holdtime <= 0');

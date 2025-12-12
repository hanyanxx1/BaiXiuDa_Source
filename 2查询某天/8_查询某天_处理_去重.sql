-- 需求如下：
-- 1. 过滤掉 被叫号码 calleee164 中包含 "QIANHAO" 、 "WuRaoHaoMa" 、"DONGTAIDIFANG" 的数据项
-- 2. calleee164 被叫号码进行去重，要求最后整个tableName中的calleee164 被叫号码不存在重复的，考虑效率问题

use vos3000;

-- 删除已存在的存储过程
DROP PROCEDURE IF EXISTS GetDistinctCallData;

-- 创建存储过程，接受表名和条件参数
DELIMITER //
CREATE PROCEDURE GetDistinctCallData(
    IN tableName VARCHAR(100),
    IN whereCondition VARCHAR(500)
)
BEGIN
    DECLARE sqlQuery TEXT;
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
    
    -- 构建SQL查询，使用GROUP BY去重
    SET sqlQuery = CONCAT('SELECT 
        callere164 as ''主叫号码'',
        calleee164 as ''被叫号码'',
        FROM_UNIXTIME(starttime/1000) as ''起始时间'',
        FROM_UNIXTIME(stoptime/1000) as ''终止时间'', 
        callergatewayid as ''主叫经由网关''
    FROM ', tableName, ' WHERE ', finalCondition);
    
    -- 添加GROUP BY实现去重
    SET sqlQuery = CONCAT(sqlQuery, ' GROUP BY calleee164');
    
    -- 执行查询
    SET @sql = sqlQuery;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //
DELIMITER ;

-- 使用示例:
-- 基本用法:
-- CALL GetDistinctCallData('e_cdr_20250227', '');

-- 带条件查询:
-- CALL GetDistinctCallData('e_cdr_20250227', 'holdtime > 0');

CALL GetDistinctCallData('e_cdr_20250316', 'holdtime <= 0');

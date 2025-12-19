-- 需求如下：
-- 1. 过滤掉 被叫号码 calleee164 中包含 "QIANHAO" 、 "WuRaoHaoMa" 、"DONGTAIDIFANG" 的数据项
-- 2. 查询符合条件的数据总数

use vos3000;

-- 删除已存在的存储过程
DROP PROCEDURE IF EXISTS GetCallDataCount;

-- 创建获取数据总数的存储过程
DELIMITER //
CREATE PROCEDURE GetCallDataCount(
    IN tableName VARCHAR(100),
    IN whereCondition VARCHAR(500)
)
BEGIN
    DECLARE countQuery TEXT;
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
    
    -- 构建计数SQL查询
    SET countQuery = CONCAT('SELECT COUNT(*) AS ''总记录数'' FROM ', tableName, ' WHERE ', finalCondition);
    
    -- 执行查询
    SET @sql = countQuery;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //
DELIMITER ;

-- 使用示例:
-- 查询表中数据总数(已过滤掉包含"QIANHAO"、"WuRaoHaoMa"和"DONGTAIDIFANG"的记录):
-- CALL GetCallDataCount('e_cdr_20250228', '');

-- 查询符合条件的数据总数:
-- CALL GetCallDataCount('e_cdr_20250227', 'holdtime > 0');

CALL GetCallDataCount('e_cdr_20250316', 'holdtime <= 0');
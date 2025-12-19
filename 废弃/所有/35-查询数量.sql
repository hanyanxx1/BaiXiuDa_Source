-- 需求如下：
-- 查询符合条件的数据总数

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
    
    -- 设置默认值
    IF whereCondition IS NULL THEN
        SET whereCondition = '';
    END IF;
    
    -- 构建计数SQL查询
    IF whereCondition != '' THEN
        SET countQuery = CONCAT('SELECT COUNT(*) AS ''总记录数'' FROM ', tableName, ' WHERE ', whereCondition);
    ELSE
        SET countQuery = CONCAT('SELECT COUNT(*) AS ''总记录数'' FROM ', tableName);
    END IF;
    
    -- 执行查询
    SET @sql = countQuery;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //
DELIMITER ;

-- 35
CALL GetCallDataCount('e_cdr_20250612', 'holdtime <= 0 AND calleee164 LIKE "%HMD%"');

-- CALL GetCallDataCount('e_cdr_20250508', "holdtime <= 0 AND callergatewayid = '陕西玖恒瑞账户对接'");
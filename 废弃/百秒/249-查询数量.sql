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

-- 249
CALL GetCallDataCount('e_cdr_20250611', 'holdtime >= 100');
CALL GetCallDataCount('e_cdr_20250612', 'holdtime >= 100');
CALL GetCallDataCount('e_cdr_20250622', 'holdtime >= 100');
CALL GetCallDataCount('e_cdr_20250623', 'holdtime >= 100');
CALL GetCallDataCount('e_cdr_20250702', 'holdtime >= 100');
CALL GetCallDataCount('e_cdr_20250703', 'holdtime >= 100');
CALL GetCallDataCount('e_cdr_20250704', 'holdtime >= 100');
CALL GetCallDataCount('e_cdr_20250705', 'holdtime >= 100');
CALL GetCallDataCount('e_cdr_20250714', 'holdtime >= 100');
CALL GetCallDataCount('e_cdr_20250715', 'holdtime >= 100');
CALL GetCallDataCount('e_cdr_20250716', 'holdtime >= 100');

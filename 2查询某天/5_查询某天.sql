-- 需求如下：
-- 直接查询所有数据

use vos3000;

-- 删除已存在的存储过程
DROP PROCEDURE IF EXISTS GetAllCallData;

-- 创建存储过程，接受表名和条件参数
DELIMITER //
CREATE PROCEDURE GetAllCallData(
    IN tableName VARCHAR(100),
    IN whereCondition VARCHAR(500)
)
BEGIN
    DECLARE sqlQuery TEXT;
    
    -- 设置默认值
    IF whereCondition IS NULL THEN
        SET whereCondition = '';
    END IF;
    
    -- 构建SQL查询，查询所有数据
    SET sqlQuery = CONCAT('SELECT 
        callere164 as ''主叫号码'',
        calleee164 as ''被叫号码'',
        FROM_UNIXTIME(starttime/1000) as ''起始时间'',
        FROM_UNIXTIME(stoptime/1000) as ''终止时间'', 
        callergatewayid as ''主叫经由网关''
    FROM ', tableName);
    
    -- 添加WHERE条件（如果有）
    IF whereCondition != '' THEN
        SET sqlQuery = CONCAT(sqlQuery, ' WHERE ', whereCondition);
    END IF;
    
    -- 执行查询
    SET @sql = sqlQuery;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //
DELIMITER ;

-- 使用示例:
-- 基本用法:
-- CALL GetAllCallData('e_cdr_20250227', '');

-- 带条件查询:
-- CALL GetAllCallData('e_cdr_20250227', 'holdtime > 0');

CALL GetAllCallData('e_cdr_20250426', "holdtime <= 0 AND callergatewayid = '陕西玖恒瑞账户对接'");

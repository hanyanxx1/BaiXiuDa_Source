-- 需求如下：
-- 1. 增加是否处理参数，如果处理了，则对"calleee164 被叫号码"进行处理，被叫号码的数据内容可能有如下几种情况
--    a. 如果数据类似 PASS660815122196124，需要将内容中符合手机号的内容提取出来

use vos3000;

-- 删除已存在的存储过程
DROP PROCEDURE IF EXISTS GetCallDataPaged;

-- 创建存储过程，接受表名和分页参数
DELIMITER //
CREATE PROCEDURE GetCallDataPaged(
    IN tableName VARCHAR(100),
    IN pageSize INT,
    IN pageNum INT,
    IN whereCondition VARCHAR(500)
)
BEGIN
    DECLARE offsetValue INT;
    DECLARE sqlQuery TEXT;
    
    -- 设置默认值
    IF pageSize IS NULL THEN
        SET pageSize = 100000;
    END IF;
    
    IF pageNum IS NULL THEN
        SET pageNum = 1;
    END IF;
    
    IF whereCondition IS NULL THEN
        SET whereCondition = '';
    END IF;
    
    -- 计算偏移量
    SET offsetValue = (pageNum - 1) * pageSize;
    
    -- 构建SQL查询
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
    
    -- 添加分页
    SET sqlQuery = CONCAT(sqlQuery, ' LIMIT ', pageSize, ' OFFSET ', offsetValue);
    
    -- 执行查询
    SET @sql = sqlQuery;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //
DELIMITER ;

-- 使用示例:
-- 基本用法（默认每页10万条，第1页）:
-- CALL GetCallDataPaged('e_cdr_20250227', 100000, 1, '');

-- 指定页大小和页码:
-- CALL GetCallDataPaged('e_cdr_20250227', 50000, 2, '');

-- 带条件查询:
-- CALL GetCallDataPaged('e_cdr_20250227', 100000, 1, 'holdtime > 0');

CALL GetCallDataPaged('e_cdr_20250228', 100000, 1, 'holdtime >= 0');

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

-- 74-徐州
-- CALL GetCallDataCount('e_cdr_20250906', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20250908', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20250925', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20250926', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20250927', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20250928', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20250929', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20250930', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251001', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251002', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251003', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251004', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251005', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251006', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251007', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251008', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251009', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251010', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251011', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251012', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251013', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251014', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251015', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251016', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251017', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251018', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251019', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251020', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251021', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251022', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251023', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251024', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251025', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251026', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251027', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251028', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251029', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251030', 'holdtime <= 0');
-- CALL GetCallDataCount('e_cdr_20251031', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251101', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251102', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251103', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251104', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251105', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251106', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251107', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251108', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251109', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251110', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251111', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251112', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251113', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251114', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251115', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251116', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251117', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251118', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251119', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251120', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251121', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251122', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251123', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251124', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251125', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251126', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251127', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251128', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251129', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251130', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251201', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251202', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251203', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251204', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251205', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251206', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251207', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251208', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251209', 'holdtime <= 0');
CALL GetCallDataCount('e_cdr_20251210', 'holdtime <= 0');


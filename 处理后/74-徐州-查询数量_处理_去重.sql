-- 需求如下：
-- 1. 按照传入的whereCondition 过滤数据
-- 2. 对calleee164 被叫号码进行如下处理
-- 2.1 从最后一位开始，向前截取11位
-- 3. 对2处理后的数据结果，进行 calleee164 被叫号码进行去重，要求最后整个tableName中的calleee164 被叫号码不存在重复的

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
    IF whereCondition IS NULL OR whereCondition = '' THEN
        SET whereCondition = 'holdtime <= 0 AND LENGTH(TRIM(calleee164)) > 0';
    END IF;
    
    -- 移除索引检查和创建部分，因为查询只执行一次
    
    -- 构建计数SQL查询
    SET countQuery = CONCAT('
        SELECT COUNT(*) AS ''去重后总数量''
        FROM (
            -- 4. 过滤号码位数
            SELECT processed_calleee164
            FROM (
                -- 3. 对处理后的数据进行去重
                SELECT DISTINCT processed_calleee164
                FROM (
                    -- 2. 对calleee164进行处理
                    SELECT 
                        RIGHT(TRIM(calleee164), 11) AS processed_calleee164
                    FROM ', tableName, '
                    -- 1. 首先按照传入的whereCondition过滤数据
                    WHERE ', whereCondition, '
                ) AS processed_data
                WHERE processed_calleee164 IS NOT NULL 
                    AND processed_calleee164 != ''''
            ) AS distinct_data
            WHERE processed_calleee164 REGEXP ''^[0-9]{11}$''
        ) AS filtered_length_data');
    
    -- 执行查询
    SET @sql = countQuery;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //
DELIMITER;

-- 74:
-- CALL GetDistinctCallDataCount('e_cdr_20250908', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250909', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250910', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250911', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250912', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250913', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250914', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250915', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250916', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250917', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250918', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250919', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250920', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250921', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250922', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250923', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250924', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250925', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250926', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250927', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250928', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250929', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20250930', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251001', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251002', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251003', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251004', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251005', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251006', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251007', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251008', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251009', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251010', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251011', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251012', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251013', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251014', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251015', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251016', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251017', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251018', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251019', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251020', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251021', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251022', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251023', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251024', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251025', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251026', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251027', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251028', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251029', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251030', 'holdtime <= 0');
-- CALL GetDistinctCallDataCount('e_cdr_20251031', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251101', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251102', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251103', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251104', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251105', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251106', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251107', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251108', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251109', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251110', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251111', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251112', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251113', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251114', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251115', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251116', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251117', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251118', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251119', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251120', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251121', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251122', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251123', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251124', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251125', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251126', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251127', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251128', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251129', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251130', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251201', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251202', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251203', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251204', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251205', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251206', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251207', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251208', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251209', 'holdtime <= 0');
CALL GetDistinctCallDataCount('e_cdr_20251210', 'holdtime <= 0');


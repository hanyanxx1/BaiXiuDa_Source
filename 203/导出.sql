-- 203
-- 需求如下：
-- 1. 按照传入的whereCondition 过滤数据
-- 2. 分批导出e_cdr_XXXXXXXX 表中所有数据，导出到csv文件中，默认每个文件最大数量104万条
-- 3. 导出文件命名为：表名_partX.csv, X为文件序号，从1开始
-- 4. 在最后一行输出总数量

use vos3000;

-- 导出到CSV文件:
-- 创建导出存储过程
-- 1. 首先删除已存在的存储过程
DROP PROCEDURE IF EXISTS ExportCallData;
DROP PROCEDURE IF EXISTS ExportBatchData;

-- 2. 创建新的存储过程，接受表名和导出路径作为参数
DELIMITER //
CREATE PROCEDURE ExportCallData(IN table_name VARCHAR(50), IN export_path VARCHAR(255), IN base_condition VARCHAR(500))
BEGIN
    DECLARE batch_size INT DEFAULT 1040000; -- 每个文件最大104万条
    DECLARE where_condition VARCHAR(1000);
    DECLARE total_records INT;
    
    -- 设置默认值
    IF base_condition IS NULL OR base_condition = '' THEN
        SET where_condition = '1=1';
    ELSE
        SET where_condition = base_condition;
    END IF;
    
    -- 获取总记录数
    SET @count_sql = CONCAT('SELECT COUNT(*) INTO @total_records FROM ', table_name, ' WHERE ', where_condition);
    PREPARE stmt_count FROM @count_sql;
    EXECUTE stmt_count;
    DEALLOCATE PREPARE stmt_count;
    SET total_records = @total_records;
    
    -- 调用导出函数
    CALL ExportBatchData(
        table_name, 
        export_path, 
        where_condition, 
        table_name  -- 简化文件前缀，只使用表名
    );
    
    -- 输出导出信息和总数量
    SELECT CONCAT('导出完成，总记录数: ', total_records) AS '导出结果';
END //
DELIMITER ;

-- 创建批量导出数据的子过程
DELIMITER //
CREATE PROCEDURE ExportBatchData(
    IN table_name VARCHAR(50), 
    IN export_path VARCHAR(255), 
    IN where_condition VARCHAR(1000),
    IN file_prefix VARCHAR(255)
)
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE batch_size INT DEFAULT 1040000; -- 每个文件最大104万条
    DECLARE total_records INT;
    DECLARE num_batches INT;
    
    -- 动态计算总记录数和批次数
    SET @count_sql = CONCAT('SELECT COUNT(*) INTO @total_records FROM ', table_name, ' WHERE ', where_condition);
    PREPARE stmt_count FROM @count_sql;
    EXECUTE stmt_count;
    DEALLOCATE PREPARE stmt_count;
    
    SET total_records = @total_records;
    SET num_batches = CEILING(total_records / batch_size);
    
    -- 循环导出每个批次
    SET i = 0;
    
    batch_loop: WHILE i < num_batches DO
        -- 每一批都包含表头
        SET @sql = CONCAT(
            'SELECT ''主叫号码'',''被叫号码'',''起始时间'',''终止时间'',''主叫经由网关'' ',
            'UNION ALL ',
            'SELECT callere164, calleee164, ',
            'FROM_UNIXTIME(starttime/1000), FROM_UNIXTIME(stoptime/1000), callergatewayid ',
            'FROM ', table_name, ' WHERE ', where_condition, ' ',
            'LIMIT ', batch_size, ' OFFSET ', i * batch_size,
            ' INTO OUTFILE ''', export_path, '/', file_prefix, '_part', i+1, '.csv'' ',
            'FIELDS TERMINATED BY '','' ',
            'ENCLOSED BY ''"'' ',
            'ESCAPED BY ''\\\\'' ',
            'LINES TERMINATED BY ''\\n'''
        );
        
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        
        SET i = i + 1;
        DO SLEEP(1); -- 添加短暂延迟，避免文件系统冲突
    END WHILE batch_loop;
END //
DELIMITER ;


CALL ExportDistinctGroupedCallData ( 'e_cdr_20260227', '/var/lib/mysql-files/e_cdr_20260227/', 'holdtime <= 0' );
-- 2026年2月28日
CALL ExportDistinctGroupedCallData ( 'e_cdr_20260228', '/var/lib/mysql-files/e_cdr_20260228/', 'holdtime <= 0' );

-- 2026年3月1日
CALL ExportDistinctGroupedCallData ( 'e_cdr_20260301', '/var/lib/mysql-files/e_cdr_20260301/', 'holdtime <= 0' );

-- 2026年3月2日
CALL ExportDistinctGroupedCallData ( 'e_cdr_20260302', '/var/lib/mysql-files/e_cdr_20260302/', 'holdtime <= 0' );

-- -- 2026年3月3日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260303', '/var/lib/mysql-files/e_cdr_20260303/', 'holdtime <= 0' );
-- 
-- -- 2026年3月4日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260304', '/var/lib/mysql-files/e_cdr_20260304/', 'holdtime <= 0' );
-- 
-- -- 2026年3月5日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260305', '/var/lib/mysql-files/e_cdr_20260305/', 'holdtime <= 0' );
-- 
-- -- 2026年3月6日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260306', '/var/lib/mysql-files/e_cdr_20260306/', 'holdtime <= 0' );
-- 
-- -- 2026年3月7日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260307', '/var/lib/mysql-files/e_cdr_20260307/', 'holdtime <= 0' );
-- 
-- -- 2026年3月8日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260308', '/var/lib/mysql-files/e_cdr_20260308/', 'holdtime <= 0' );
-- 
-- -- 2026年3月9日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260309', '/var/lib/mysql-files/e_cdr_20260309/', 'holdtime <= 0' );
-- 
-- -- 2026年3月10日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260310', '/var/lib/mysql-files/e_cdr_20260310/', 'holdtime <= 0' );
-- 
-- -- 2026年3月11日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260311', '/var/lib/mysql-files/e_cdr_20260311/', 'holdtime <= 0' );
-- 
-- -- 2026年3月12日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260312', '/var/lib/mysql-files/e_cdr_20260312/', 'holdtime <= 0' );
-- 
-- -- 2026年3月13日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260313', '/var/lib/mysql-files/e_cdr_20260313/', 'holdtime <= 0' );
-- 
-- -- 2026年3月14日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260314', '/var/lib/mysql-files/e_cdr_20260314/', 'holdtime <= 0' );
-- 
-- -- 2026年3月15日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260315', '/var/lib/mysql-files/e_cdr_20260315/', 'holdtime <= 0' );
-- 
-- -- 2026年3月16日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260316', '/var/lib/mysql-files/e_cdr_20260316/', 'holdtime <= 0' );
-- 
-- -- 2026年3月17日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260317', '/var/lib/mysql-files/e_cdr_20260317/', 'holdtime <= 0' );
-- 
-- -- 2026年3月18日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260318', '/var/lib/mysql-files/e_cdr_20260318/', 'holdtime <= 0' );
-- 
-- -- 2026年3月19日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260319', '/var/lib/mysql-files/e_cdr_20260319/', 'holdtime <= 0' );
-- 
-- -- 2026年3月20日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260320', '/var/lib/mysql-files/e_cdr_20260320/', 'holdtime <= 0' );
-- 
-- -- 2026年3月21日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260321', '/var/lib/mysql-files/e_cdr_20260321/', 'holdtime <= 0' );
-- 
-- -- 2026年3月22日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260322', '/var/lib/mysql-files/e_cdr_20260322/', 'holdtime <= 0' );
-- 
-- -- 2026年3月23日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260323', '/var/lib/mysql-files/e_cdr_20260323/', 'holdtime <= 0' );
-- 
-- -- 2026年3月24日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260324', '/var/lib/mysql-files/e_cdr_20260324/', 'holdtime <= 0' );
-- 
-- -- 2026年3月25日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260325', '/var/lib/mysql-files/e_cdr_20260325/', 'holdtime <= 0' );
-- 
-- -- 2026年3月26日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260326', '/var/lib/mysql-files/e_cdr_20260326/', 'holdtime <= 0' );
-- 
-- -- 2026年3月27日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260327', '/var/lib/mysql-files/e_cdr_20260327/', 'holdtime <= 0' );
-- 
-- -- 2026年3月28日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260328', '/var/lib/mysql-files/e_cdr_20260328/', 'holdtime <= 0' );
-- 
-- -- 2026年3月29日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260329', '/var/lib/mysql-files/e_cdr_20260329/', 'holdtime <= 0' );
-- 
-- -- 2026年3月30日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260330', '/var/lib/mysql-files/e_cdr_20260330/', 'holdtime <= 0' );
-- 
-- -- 2026年3月31日
-- CALL ExportDistinctGroupedCallData ( 'e_cdr_20260331', '/var/lib/mysql-files/e_cdr_20260331/', 'holdtime <= 0' );

-- 主叫号码					callere164				varchar(64)
-- 被叫号码					calleee164				varchar(64)
-- 起始时间					starttime   			bigint			毫秒时间戳
-- 终止时间					stoptime   				bigint			毫秒时间戳
-- 通话时长					holdtime				int				秒
-- 计费时长					feetime					int				秒
-- 通话费用					fee						double
-- 话费成本					agentfee				double
-- 终止原因					endreason				int				枚举：-8（被叫挂断）....
-- 挂断方					enddirection			int				枚举：1（被叫）...
-- 主叫经由网关				callergatewayid			varchar(64)		
-- 被叫经由网关				calleegatewayid			varchar(64)	
-- 主叫ip					callerip				varchar(64)	
-- 被叫ip					calleeip				varchar(64)	
-- 账户名称					customername			varchar（255）
-- 账户号码					customeraccount			varchar（255）
-- 代理商账号				agentaccount			varchar（255）
-- 通话类型					callertype				int				枚举：1（国内长途）...
-- 地区前缀					callerareacode			varchar(64)
-- 呼入主叫					calleraccesse164		varchar(64)
-- 呼入被叫					calleeaccesse164		varchar(64)
-- 呼出主叫					callertogatewaye164		varchar(64)
-- 呼出被叫					calleetogatewaye164		varchar(64)
-- 主叫设备名称				callerproductid			varchar(64)
-- 被叫设备名称				calleeproductid			varchar(64)
-- 套餐时长					suitefeetime			int
-- 套餐费用					suitefee				int
-- 计费方式					billingtype				int
-- 计费模式					billingmode				int
-- 续接时长					callerpdd				int				结果除以1000
-- 接通延迟					calleepdd				int				结果除以1000
-- 主叫Call-Id				calleroriginalcallid	varchar(64)
-- 被叫Call-Id				calleecallid			varchar(64)
-- 是否录音					recordstarttime			bigint			如果大于0 显示"是"  如果为0  显示"否"
-- 序列号					flowno					bigint

-- 我的mysql表中，存在数据2000万以上，字段多达40列的一张表，如何把这张表导出成.csv文件
-- 使用 SELECT INTO OUTFILE（最高效，需FILE权限）

-- SELECT * FROM your_table
-- INTO OUTFILE '/tmp/output.csv'
-- FIELDS TERMINATED BY ',' 
-- ENCLOSED BY '"' 
-- ESCAPED BY '\\'
-- LINES TERMINATED BY '\n';

-- SELECT 
--     callere164 as '主叫号码',
--     calleraccesse164,
--     calleee164 as '被叫号码',
--     calleeaccesse164,
--     starttime as '起始时间',
--     stoptime as '终止时间', 
--     holdtime as '通话时长',
--     feetime as '计费时长',
--     fee as '通话费用',
--     agentfee as '话费成本',
--     endreason as '终止原因',
--     enddirection as '挂断方',
--     callergatewayid as '主叫经由网关'
--     calleegatewayid as '被叫经由网关',
--     callerip as '主叫ip',
--     calleeip as '被叫ip',
--     customername as '账户名称',
--     customeraccount as '账户号码',
--     agentaccount as '代理商账号',
--     callertype as '通话类型',
--     callerareacode as '地区前缀',
--     calleraccesse164 as '呼入主叫',
--     calleeaccesse164 as '呼入被叫',
--     callertogatewaye164 as '呼出主叫',
--     calleetogatewaye164 as '呼出被叫'
--     callerproductid as '主叫设备名称',
--     calleeproductid as '被叫设备名称',
--     suitefeetime as '套餐时长',
--     suitefee as '套餐费用',
--     billingtype as '计费方式',
--     billingmode as '计费模式',
--     callerpdd as '续接时长',
--     calleepdd as '接通延迟',
--     calleroriginalcallid as '主叫Call-Id',
--     calleecallid as '被叫Call-Id',
--     recordstarttime as '是否录音',
--     flowno as '序列号'
-- FROM e_cdr_20250227

-- SELECT 
--     callere164 as '主叫号码',
--     calleee164 as '被叫号码',
--     starttime as '起始时间',
--     stoptime as '终止时间', 
--     callergatewayid as '主叫经由网关'
-- FROM e_cdr_20250227

-- ... existing code ...

-- ... existing code ...

-- 1. 首先删除已存在的存储过程
DROP PROCEDURE IF EXISTS export_data;

-- 2. 创建新的存储过程，接受表名作为参数
DELIMITER //
CREATE PROCEDURE export_data(IN table_name VARCHAR(50))
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE current_time_str VARCHAR(6);
    DECLARE batch_size INT DEFAULT 1000000;
    DECLARE total_records INT;
    DECLARE num_batches INT;
    
    -- 动态计算总记录数和批次数
    SET @count_sql = CONCAT('SELECT COUNT(*) INTO @total_records FROM ', table_name, ' WHERE holdtime <= 0');
    PREPARE stmt_count FROM @count_sql;
    EXECUTE stmt_count;
    DEALLOCATE PREPARE stmt_count;
    
    SET total_records = @total_records;
    SET num_batches = CEILING(total_records / batch_size);
    
    batch_loop: WHILE i < num_batches DO
        SET current_time_str = DATE_FORMAT(NOW(), '%i%H%s');
        
        SET @sql = CONCAT(
            'SELECT callere164 as ''主叫号码'', calleee164 as ''被叫号码'', ',
            'starttime as ''起始时间'', stoptime as ''终止时间'', ',
            'callergatewayid as ''主叫经由网关'' ',
            'FROM ', table_name, ' ',
            'WHERE holdtime <= 0 ',
            'LIMIT ', batch_size, ' OFFSET ', i * batch_size,
            ' INTO OUTFILE ''/tmp/export_dir/', table_name, '_', current_time_str, '.csv'' ',
            'FIELDS TERMINATED BY '','' ',
            'ENCLOSED BY ''"'' ',
            'ESCAPED BY ''\\\\'' ',
            'LINES TERMINATED BY ''\\n'''
        );
        
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        
        SET i = i + 1;
        DO SLEEP(1);
    END WHILE batch_loop;
END //
DELIMITER ;

-- 4. 执行存储过程（使用时传入实际的表名）
-- 例如：CALL export_data('e_cdr_20250317');
CALL export_data('e_cdr_20250228');
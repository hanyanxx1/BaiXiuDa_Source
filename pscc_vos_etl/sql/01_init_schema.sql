-- 1. 创建专门用于存放清洗后报表数据的独立数据库
CREATE DATABASE IF NOT EXISTS `pscc_report` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

-- 2. 切换到新创建的数据库
USE `pscc_report`;

-- ==========================================
-- [修改开始：新增了 DROP 语句，并在表结构中正式加入了 cc_admin_id 及对应索引]
-- ==========================================
-- 如果表已存在则直接删掉重建，保持环境纯洁
DROP TABLE IF EXISTS `etl_merged_cdr`;

-- 3. 创建核心的跨系统对账合并宽表
CREATE TABLE `etl_merged_cdr` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  
  `cc_admin_id` int(11) NOT NULL COMMENT '租户/管理员ID',
  `target_number` varchar(64) NOT NULL COMMENT '客户号码/被叫号码',
  -- ==========================================
  -- 1. CC库数据源 (PSCC) 侧字段
  -- ==========================================
  `cc_start_time` datetime DEFAULT NULL COMMENT 'CC-开始时间',
  `cc_duration` int DEFAULT '0' COMMENT 'CC-总时长(秒)',
  `cc_consume` decimal(7,4) DEFAULT '0.0000' COMMENT 'CC-金额(元)',
  `cc_status` tinyint(1) DEFAULT NULL COMMENT 'CC-挂断状态',
  `cc_gateway_name` varchar(255) DEFAULT NULL COMMENT 'CC-网关名称',
  
  -- ==========================================
  -- 2. VOS3000 数据源 侧字段
  -- ==========================================
  `vos_caller_number` varchar(64) DEFAULT NULL COMMENT 'VOS-主叫号码',
  `vos_start_time` datetime DEFAULT NULL COMMENT 'VOS-起始时间',
  `vos_hold_time` int DEFAULT '0' COMMENT 'VOS-通话时长(秒)',
  `vos_callee_gateway` varchar(64) DEFAULT NULL COMMENT 'VOS-被叫经由网关',
  
  -- 运维审计字段
  `etl_create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '数据写入时间',
  
  PRIMARY KEY (`id`),

  KEY `idx_admin` (`cc_admin_id`),
  KEY `idx_time` (`cc_start_time`),          
  KEY `idx_number` (`target_number`),        
  KEY `idx_vos_gateway` (`vos_callee_gateway`) 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='CC与VOS跨库对账合并话单表';
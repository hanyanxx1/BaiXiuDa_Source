CREATE DATABASE IF NOT EXISTS `pscc_report` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE `pscc_report`;

DROP TABLE IF EXISTS `etl_merged_cdr`;
CREATE TABLE `etl_merged_cdr` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `cc_admin_id` int(11) NOT NULL COMMENT '租户/管理员ID',
  `target_number` varchar(64) NOT NULL COMMENT '客户号码/被叫号码',
  `cc_start_time` datetime DEFAULT NULL COMMENT 'CC-开始时间',
  `cc_billsec` int(11) DEFAULT '0' COMMENT 'CC-总时长(秒)',
  `cc_consume` decimal(7,4) DEFAULT '0.0000' COMMENT 'CC-金额(元)',
  `cc_status` tinyint(1) DEFAULT NULL COMMENT 'CC-挂断状态',
  `cc_gateway_name` varchar(255) DEFAULT NULL COMMENT 'CC-网关名称',
  `vos_caller_number` varchar(64) DEFAULT NULL COMMENT 'VOS-主叫号码',
  `vos_start_time` datetime DEFAULT NULL COMMENT 'VOS-起始时间',
  `vos_hold_time` int(11) DEFAULT '0' COMMENT 'VOS-通话时长(秒)',
  `vos_callee_gateway` varchar(64) DEFAULT NULL COMMENT 'VOS-被叫经由网关',
  `vos_agentfee` decimal(10,4) DEFAULT '0.0000' COMMENT 'VOS-代理费(元)',
  `vos_agentaccount` varchar(255) DEFAULT NULL COMMENT 'VOS-代理商账号',
  `etl_create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '数据写入时间',
  
  PRIMARY KEY (`id`),
  -- 👇 完美保留您原版的所有核心索引，确保老业务性能毫不受损
  KEY `idx_admin` (`cc_admin_id`),
  KEY `idx_time` (`cc_start_time`),
  KEY `idx_number` (`target_number`),
  KEY `idx_vos_gateway` (`vos_callee_gateway`),
  -- 👇 额外新增这一行，专门为您新改版的代理商大盘查询提供爆发式的加速支持
  KEY `idx_vos_agentaccount` (`vos_agentaccount`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='跨系统话单清洗融合对账宽表';

CREATE TABLE `customer_cdr_template`  (
                                          `uuid` varchar(36) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '',
                                          `direction` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '',
                                          `telephone` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '',
                                          `group` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT 'default',
                                          `extno` varchar(8) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '',
                                          `start_stamp` datetime(0) NOT NULL,
                                          `answer_stamp` varchar(19) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '',
                                          `bridge_stamp` varchar(19) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '',
                                          `end_stamp` datetime(0) NULL DEFAULT NULL,
                                          `duration` int(0) NULL DEFAULT 0,
                                          `billsec` int(0) NULL DEFAULT 0,
                                          `hangup_cause` varchar(36) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '',
                                          `hangup_disposition` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '',
                                          `info_uuid` varchar(36) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '',
                                          `consume` decimal(7, 4) NULL DEFAULT NULL,
                                          `record_file` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '',
                                          `project_id` int(0) NULL DEFAULT 0,
                                          `status` tinyint(1) NULL DEFAULT 0,
                                          `inspection` tinyint(0) NULL DEFAULT 0 COMMENT '0 未质检\r\n1 合格\r\n2 不合格',
                                          `digits` varchar(8) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '' COMMENT '按键',
                                          `recovered` tinyint(0) NULL DEFAULT 0,
                                          `task_id` int(0) NULL DEFAULT 0,
                                          `admin_id` int(0) NULL DEFAULT 0,
                                          `gateway_id` int(1) NULL DEFAULT 0,
                                          `satisfaction` varchar(8) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '' COMMENT '服务质量',
                                          `expand` json NULL,
                                          `ai_minute` int(0) NULL DEFAULT 0,
                                          `ai_consume` decimal(7, 4) DEFAULT 0,
                                          `bot_id` int(0) NULL DEFAULT 0,
                                          `recording` tinyint(1) NULL DEFAULT 0,
                                          `answered` tinyint(1) NULL DEFAULT 0,
                                          `detect_early_media` tinyint(1) NULL DEFAULT 0,
                                          PRIMARY KEY (`uuid`) USING BTREE,
                                          KEY `idx_tel` (`telephone`) USING BTREE,
                                          KEY `idx_ext` (`extno`) USING BTREE,
                                          KEY `idx_bot` (`bot_id`,`start_stamp` DESC) USING BTREE,
                                          KEY `idx_record` (`recording`,`admin_id`,`start_stamp` DESC) USING BTREE,
                                          KEY `idx_record_project` (`project_id`,`recording`,`start_stamp`) USING BTREE,
                                          KEY `idx_record_status` (`status`,`project_id`,`recording`,`start_stamp`) USING BTREE,
                                          KEY `idx_cdr` (`answered`, `admin_id`, `start_stamp` DESC) USING BTREE,
                                          KEY `idx_cdr_project` (`project_id`,`answered`,`start_stamp`) USING BTREE,
                                          KEY `idx_cdr_status` (`status`,`project_id`,`answered`,`start_stamp`) USING BTREE,
                                          KEY `idx_task` (`task_id`,`status`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

CREATE TRIGGER `t_ok_template` AFTER UPDATE ON `customer_cdr_template` FOR EACH ROW begin
    if(OLD.status=4) then
    delete from ok_note where uuid=OLD.uuid;
end if;
if(NEW.status=4) then
		insert into ok_note(uuid,start_stamp,admin_id,project_id,task_id,gateway_id,`ext`) values(NEW.uuid,NEW.start_stamp,NEW.admin_id,NEW.project_id,NEW.task_id,NEW.gateway_id,NEW.extno);
end if;
	if(NEW.status>0) then
update customer_info set answered=NEW.status where uuid=OLD.info_uuid;
end if;
	if(NEW.end_stamp IS NOT NULL) then
update customer_info set billsec=NEW.billsec where uuid=OLD.info_uuid;
end if;
end
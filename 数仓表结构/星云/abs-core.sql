/*
Navicat MySQL Data Transfer

Source Server         : 星云3.0-测试
Source Server Version : 50718
Source Host           : 10.83.16.15:3306
Source Database       : abs-core

Target Server Type    : MYSQL
Target Server Version : 50718
File Encoding         : 65001

Date: 2020-09-17 14:20:05
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for QRTZ_BLOB_TRIGGERS
-- ----------------------------
-- DROP TABLE IF EXISTS `QRTZ_BLOB_TRIGGERS`;
CREATE TABLE `QRTZ_BLOB_TRIGGERS` (
  `SCHED_NAME`                                varchar(120) NOT NULL,
  `TRIGGER_NAME`                              varchar(200) NOT NULL,
  `TRIGGER_GROUP`                             varchar(200) NOT NULL,
  `BLOB_DATA`                                 blob,
  PRIMARY KEY (`SCHED_NAME`,`TRIGGER_NAME`,`TRIGGER_GROUP`),
  KEY `SCHED_NAME` (`SCHED_NAME`,`TRIGGER_NAME`,`TRIGGER_GROUP`),
  CONSTRAINT `QRTZ_BLOB_TRIGGERS_ibfk_1` FOREIGN KEY (`SCHED_NAME`, `TRIGGER_NAME`, `TRIGGER_GROUP`) REFERENCES `QRTZ_TRIGGERS` (`SCHED_NAME`, `TRIGGER_NAME`, `TRIGGER_GROUP`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='jobDetail信息';

-- ----------------------------
-- Table structure for QRTZ_CALENDARS
-- ----------------------------
-- DROP TABLE IF EXISTS `QRTZ_CALENDARS`;
CREATE TABLE `QRTZ_CALENDARS` (
  `SCHED_NAME`                                varchar(120) NOT NULL,
  `CALENDAR_NAME`                             varchar(200) NOT NULL,
  `CALENDAR`                                  blob NOT NULL,
  PRIMARY KEY (`SCHED_NAME`,`CALENDAR_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='jobDetail信息';

-- ----------------------------
-- Table structure for QRTZ_CRON_TRIGGERS
-- ----------------------------
-- DROP TABLE IF EXISTS `QRTZ_CRON_TRIGGERS`;
CREATE TABLE `QRTZ_CRON_TRIGGERS` (
  `SCHED_NAME`                                varchar(120) NOT NULL,
  `TRIGGER_NAME`                              varchar(200) NOT NULL,
  `TRIGGER_GROUP`                             varchar(200) NOT NULL,
  `CRON_EXPRESSION`                           varchar(120) NOT NULL,
  `TIME_ZONE_ID`                              varchar(80) DEFAULT NULL,
  PRIMARY KEY (`SCHED_NAME`,`TRIGGER_NAME`,`TRIGGER_GROUP`),
  CONSTRAINT `QRTZ_CRON_TRIGGERS_ibfk_1` FOREIGN KEY (`SCHED_NAME`, `TRIGGER_NAME`, `TRIGGER_GROUP`) REFERENCES `QRTZ_TRIGGERS` (`SCHED_NAME`, `TRIGGER_NAME`, `TRIGGER_GROUP`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='cron类型触发器信息';

-- ----------------------------
-- Table structure for QRTZ_FIRED_TRIGGERS
-- ----------------------------
-- DROP TABLE IF EXISTS `QRTZ_FIRED_TRIGGERS`;
CREATE TABLE `QRTZ_FIRED_TRIGGERS` (
  `SCHED_NAME`                                varchar(120) NOT NULL,
  `ENTRY_ID`                                  varchar(95) NOT NULL,
  `TRIGGER_NAME`                              varchar(200) NOT NULL,
  `TRIGGER_GROUP`                             varchar(200) NOT NULL,
  `INSTANCE_NAME`                             varchar(200) NOT NULL,
  `FIRED_TIME`                                bigint(13) NOT NULL,
  `SCHED_TIME`                                bigint(13) NOT NULL,
  `PRIORITY`                                  int(11) NOT NULL,
  `STATE`                                     varchar(16) NOT NULL,
  `JOB_NAME`                                  varchar(200) DEFAULT NULL,
  `JOB_GROUP`                                 varchar(200) DEFAULT NULL,
  `IS_NONCONCURRENT`                          varchar(1) DEFAULT NULL,
  `REQUESTS_RECOVERY`                         varchar(1) DEFAULT NULL,
  PRIMARY KEY (`SCHED_NAME`,`ENTRY_ID`),
  KEY `IDX_QRTZ_FT_TRIG_INST_NAME` (`SCHED_NAME`,`INSTANCE_NAME`),
  KEY `IDX_QRTZ_FT_INST_JOB_REQ_RCVRY` (`SCHED_NAME`,`INSTANCE_NAME`,`REQUESTS_RECOVERY`),
  KEY `IDX_QRTZ_FT_J_G` (`SCHED_NAME`,`JOB_NAME`,`JOB_GROUP`),
  KEY `IDX_QRTZ_FT_JG` (`SCHED_NAME`,`JOB_GROUP`),
  KEY `IDX_QRTZ_FT_T_G` (`SCHED_NAME`,`TRIGGER_NAME`,`TRIGGER_GROUP`),
  KEY `IDX_QRTZ_FT_TG` (`SCHED_NAME`,`TRIGGER_GROUP`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='jobDetail信息';

-- ----------------------------
-- Table structure for QRTZ_JOB_DETAILS
-- ----------------------------
-- DROP TABLE IF EXISTS `QRTZ_JOB_DETAILS`;
CREATE TABLE `QRTZ_JOB_DETAILS` (
  `SCHED_NAME`                                varchar(120) NOT NULL,
  `JOB_NAME`                                  varchar(200) NOT NULL,
  `JOB_GROUP`                                 varchar(200) NOT NULL,
  `DESCRIPTION`                               varchar(250) DEFAULT NULL,
  `JOB_CLASS_NAME`                            varchar(250) NOT NULL,
  `IS_DURABLE`                                varchar(1) NOT NULL,
  `IS_NONCONCURRENT`                          varchar(1) NOT NULL,
  `IS_UPDATE_DATA`                            varchar(1) NOT NULL,
  `REQUESTS_RECOVERY`                         varchar(1) NOT NULL,
  `JOB_DATA`                                  blob,
  PRIMARY KEY (`SCHED_NAME`,`JOB_NAME`,`JOB_GROUP`),
  KEY `IDX_QRTZ_J_REQ_RECOVERY` (`SCHED_NAME`,`REQUESTS_RECOVERY`),
  KEY `IDX_QRTZ_J_GRP` (`SCHED_NAME`,`JOB_GROUP`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='任务详细信息';

-- ----------------------------
-- Table structure for QRTZ_LOCKS
-- ----------------------------
-- DROP TABLE IF EXISTS `QRTZ_LOCKS`;
CREATE TABLE `QRTZ_LOCKS` (
  `SCHED_NAME`                                varchar(120) NOT NULL,
  `LOCK_NAME`                                 varchar(40) NOT NULL,
  PRIMARY KEY (`SCHED_NAME`,`LOCK_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='jobDetail信息';

-- ----------------------------
-- Table structure for QRTZ_PAUSED_TRIGGER_GRPS
-- ----------------------------
-- DROP TABLE IF EXISTS `QRTZ_PAUSED_TRIGGER_GRPS`;
CREATE TABLE `QRTZ_PAUSED_TRIGGER_GRPS` (
  `SCHED_NAME`                                varchar(120) NOT NULL,
  `TRIGGER_GROUP`                             varchar(200) NOT NULL,
  PRIMARY KEY (`SCHED_NAME`,`TRIGGER_GROUP`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='jobDetail信息';

-- ----------------------------
-- Table structure for QRTZ_SCHEDULER_STATE
-- ----------------------------
-- DROP TABLE IF EXISTS `QRTZ_SCHEDULER_STATE`;
CREATE TABLE `QRTZ_SCHEDULER_STATE` (
  `SCHED_NAME`                                varchar(120) NOT NULL,
  `INSTANCE_NAME`                             varchar(200) NOT NULL,
  `LAST_CHECKIN_TIME`                         bigint(13) NOT NULL,
  `CHECKIN_INTERVAL`                          bigint(13) NOT NULL,
  PRIMARY KEY (`SCHED_NAME`,`INSTANCE_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='jobDetail信息';

-- ----------------------------
-- Table structure for QRTZ_SIMPLE_TRIGGERS
-- ----------------------------
-- DROP TABLE IF EXISTS `QRTZ_SIMPLE_TRIGGERS`;
CREATE TABLE `QRTZ_SIMPLE_TRIGGERS` (
  `SCHED_NAME`                                varchar(120) NOT NULL,
  `TRIGGER_NAME`                              varchar(200) NOT NULL,
  `TRIGGER_GROUP`                             varchar(200) NOT NULL,
  `REPEAT_COUNT`                              bigint(7) NOT NULL,
  `REPEAT_INTERVAL`                           bigint(12) NOT NULL,
  `TIMES_TRIGGERED`                           bigint(10) NOT NULL,
  PRIMARY KEY (`SCHED_NAME`,`TRIGGER_NAME`,`TRIGGER_GROUP`),
  CONSTRAINT `QRTZ_SIMPLE_TRIGGERS_ibfk_1` FOREIGN KEY (`SCHED_NAME`, `TRIGGER_NAME`, `TRIGGER_GROUP`) REFERENCES `QRTZ_TRIGGERS` (`SCHED_NAME`, `TRIGGER_NAME`, `TRIGGER_GROUP`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='简单触发器信息';

-- ----------------------------
-- Table structure for QRTZ_SIMPROP_TRIGGERS
-- ----------------------------
-- DROP TABLE IF EXISTS `QRTZ_SIMPROP_TRIGGERS`;
CREATE TABLE `QRTZ_SIMPROP_TRIGGERS` (
  `SCHED_NAME`                                varchar(120) NOT NULL,
  `TRIGGER_NAME`                              varchar(200) NOT NULL,
  `TRIGGER_GROUP`                             varchar(200) NOT NULL,
  `STR_PROP_1`                                varchar(512) DEFAULT NULL,
  `STR_PROP_2`                                varchar(512) DEFAULT NULL,
  `STR_PROP_3`                                varchar(512) DEFAULT NULL,
  `INT_PROP_1`                                int(11) DEFAULT NULL,
  `INT_PROP_2`                                int(11) DEFAULT NULL,
  `LONG_PROP_1`                               bigint(20) DEFAULT NULL,
  `LONG_PROP_2`                               bigint(20) DEFAULT NULL,
  `DEC_PROP_1`                                decimal(13,4) DEFAULT NULL,
  `DEC_PROP_2`                                decimal(13,4) DEFAULT NULL,
  `BOOL_PROP_1`                               varchar(1) DEFAULT NULL,
  `BOOL_PROP_2`                               varchar(1) DEFAULT NULL,
  PRIMARY KEY (`SCHED_NAME`,`TRIGGER_NAME`,`TRIGGER_GROUP`),
  CONSTRAINT `QRTZ_SIMPROP_TRIGGERS_ibfk_1` FOREIGN KEY (`SCHED_NAME`, `TRIGGER_NAME`, `TRIGGER_GROUP`) REFERENCES `QRTZ_TRIGGERS` (`SCHED_NAME`, `TRIGGER_NAME`, `TRIGGER_GROUP`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='jobDetail信息';

-- ----------------------------
-- Table structure for QRTZ_TRIGGERS
-- ----------------------------
-- DROP TABLE IF EXISTS `QRTZ_TRIGGERS`;
CREATE TABLE `QRTZ_TRIGGERS` (
  `SCHED_NAME`                                varchar(120) NOT NULL,
  `TRIGGER_NAME`                              varchar(200) NOT NULL,
  `TRIGGER_GROUP`                             varchar(200) NOT NULL,
  `JOB_NAME`                                  varchar(200) NOT NULL,
  `JOB_GROUP`                                 varchar(200) NOT NULL,
  `DESCRIPTION`                               varchar(250) DEFAULT NULL,
  `NEXT_FIRE_TIME`                            bigint(13) DEFAULT NULL,
  `PREV_FIRE_TIME`                            bigint(13) DEFAULT NULL,
  `PRIORITY`                                  int(11) DEFAULT NULL,
  `TRIGGER_STATE`                             varchar(16) NOT NULL,
  `TRIGGER_TYPE`                              varchar(8) NOT NULL,
  `START_TIME`                                bigint(13) NOT NULL,
  `END_TIME`                                  bigint(13) DEFAULT NULL,
  `CALENDAR_NAME`                             varchar(200) DEFAULT NULL,
  `MISFIRE_INSTR`                             smallint(2) DEFAULT NULL,
  `JOB_DATA`                                  blob,
  PRIMARY KEY (`SCHED_NAME`,`TRIGGER_NAME`,`TRIGGER_GROUP`),
  KEY `IDX_QRTZ_T_J` (`SCHED_NAME`,`JOB_NAME`,`JOB_GROUP`),
  KEY `IDX_QRTZ_T_JG` (`SCHED_NAME`,`JOB_GROUP`),
  KEY `IDX_QRTZ_T_C` (`SCHED_NAME`,`CALENDAR_NAME`),
  KEY `IDX_QRTZ_T_G` (`SCHED_NAME`,`TRIGGER_GROUP`),
  KEY `IDX_QRTZ_T_STATE` (`SCHED_NAME`,`TRIGGER_STATE`),
  KEY `IDX_QRTZ_T_N_STATE` (`SCHED_NAME`,`TRIGGER_NAME`,`TRIGGER_GROUP`,`TRIGGER_STATE`),
  KEY `IDX_QRTZ_T_N_G_STATE` (`SCHED_NAME`,`TRIGGER_GROUP`,`TRIGGER_STATE`),
  KEY `IDX_QRTZ_T_NEXT_FIRE_TIME` (`SCHED_NAME`,`NEXT_FIRE_TIME`),
  KEY `IDX_QRTZ_T_NFT_ST` (`SCHED_NAME`,`TRIGGER_STATE`,`NEXT_FIRE_TIME`),
  KEY `IDX_QRTZ_T_NFT_MISFIRE` (`SCHED_NAME`,`MISFIRE_INSTR`,`NEXT_FIRE_TIME`),
  KEY `IDX_QRTZ_T_NFT_ST_MISFIRE` (`SCHED_NAME`,`MISFIRE_INSTR`,`NEXT_FIRE_TIME`,`TRIGGER_STATE`),
  KEY `IDX_QRTZ_T_NFT_ST_MISFIRE_GRP` (`SCHED_NAME`,`MISFIRE_INSTR`,`NEXT_FIRE_TIME`,`TRIGGER_GROUP`,`TRIGGER_STATE`),
  CONSTRAINT `QRTZ_TRIGGERS_ibfk_1` FOREIGN KEY (`SCHED_NAME`, `JOB_NAME`, `JOB_GROUP`) REFERENCES `QRTZ_JOB_DETAILS` (`SCHED_NAME`, `JOB_NAME`, `JOB_GROUP`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='触发器基本信息';

-- ----------------------------
-- Table structure for t_account_cash_flow
-- ----------------------------
-- DROP TABLE IF EXISTS `t_account_cash_flow`;
CREATE TABLE `t_account_cash_flow` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `product_id`                                int(11) DEFAULT NULL COMMENT '产品Id',
  `period`                                    int(11) DEFAULT NULL COMMENT '期数',
  `begin_interest_date`                       date DEFAULT NULL COMMENT '起息日',
  `pay_interest_date`                         date DEFAULT NULL COMMENT '付息日',
  `redemption_date`                           date DEFAULT NULL COMMENT '收益兑付日',
  `begin_principal_account_balance`           decimal(20,2) DEFAULT NULL COMMENT '本金账户期初余额',
  `end_principal_account_balance`             decimal(20,2) DEFAULT NULL COMMENT '本金账户期末余额',
  `begin_interest_account_balance`            decimal(20,2) DEFAULT NULL COMMENT '利息账户期初余额',
  `end_interest_account_balance`              decimal(20,2) DEFAULT NULL COMMENT '利息账户期末余额',
  `begin_account_balance`                     decimal(20,2) DEFAULT NULL COMMENT '本息账户期初余额',
  `end_account_balance`                       decimal(20,2) DEFAULT NULL COMMENT '本息账户期末余额',
  `available_amount`                          decimal(32,2) DEFAULT NULL COMMENT '当期可动用金额',
  `total_available_amount`                    decimal(32,2) DEFAULT NULL COMMENT '累计可动用金额',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_account_cash_flow_project_id_product_id` (`project_id`,`product_id`)
) ENGINE=InnoDB AUTO_INCREMENT=709 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='账户现金流';

-- ----------------------------
-- Table structure for t_account_cash_flow_copy
-- ----------------------------
-- DROP TABLE IF EXISTS `t_account_cash_flow_copy`;
CREATE TABLE `t_account_cash_flow_copy` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `product_id`                                int(11) DEFAULT NULL COMMENT '产品Id',
  `period`                                    int(11) DEFAULT NULL COMMENT '期数',
  `begin_interest_date`                       date DEFAULT NULL COMMENT '起息日',
  `pay_interest_date`                         date DEFAULT NULL COMMENT '付息日',
  `redemption_date`                           date DEFAULT NULL COMMENT '收益兑付日',
  `begin_principal_account_balance`           decimal(20,2) DEFAULT NULL COMMENT '本金账户期初余额',
  `end_principal_account_balance`             decimal(20,2) DEFAULT NULL COMMENT '本金账户期末余额',
  `begin_interest_account_balance`            decimal(20,2) DEFAULT NULL COMMENT '利息账户期初余额',
  `end_interest_account_balance`              decimal(20,2) DEFAULT NULL COMMENT '利息账户期末余额',
  `begin_account_balance`                     decimal(20,2) DEFAULT NULL COMMENT '本息账户期初余额',
  `end_account_balance`                       decimal(20,2) DEFAULT NULL COMMENT '本息账户期末余额',
  `available_amount`                          decimal(32,2) DEFAULT NULL COMMENT '当期可动用金额',
  `total_available_amount`                    decimal(32,2) DEFAULT NULL COMMENT '累计可动用金额',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_account_cash_flow_project_id_product_id` (`project_id`,`product_id`)
) ENGINE=InnoDB AUTO_INCREMENT=709 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='账户现金流';

-- ----------------------------
-- Table structure for t_actualrepayinfo
-- ----------------------------
-- DROP TABLE IF EXISTS `t_actualrepayinfo`;
CREATE TABLE `t_actualrepayinfo` (
  `id`                                        int(32) NOT NULL AUTO_INCREMENT COMMENT '实际还款信息主键',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `agency_id`                                 varchar(32) DEFAULT NULL COMMENT '机构编号',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `term`                                      int(4) DEFAULT NULL COMMENT '期次',
  `shoud_repay_date`                          date DEFAULT NULL COMMENT '应还款日',
  `shoud_repay_principal`                     decimal(20,6) DEFAULT NULL COMMENT '应还本金（元）',
  `shoud_repay_interest`                      decimal(20,6) DEFAULT NULL COMMENT '应还利息（元）',
  `shoud_repay_fee`                           decimal(20,6) DEFAULT NULL COMMENT '应还费用（元）',
  `repay_type`                                varchar(32) DEFAULT NULL COMMENT '还款类型 1，提前还款 2，正常还款 3，部分还款 4，逾期还款',
  `actual_work_interest_rate`                 decimal(20,6) DEFAULT NULL COMMENT '实际还款执行利率',
  `actual_repay_principal`                    decimal(20,6) DEFAULT NULL COMMENT '实还本金（元）',
  `actual_repay_interest`                     decimal(20,6) DEFAULT NULL COMMENT '实还利息（元）',
  `actual_repay_fee`                          decimal(20,6) DEFAULT NULL COMMENT '实还费用（元）',
  `actual_repay_time`                         date DEFAULT NULL COMMENT '实际还清日期 : ( 若没有还清传还款日期，若还清就传实际还清的日期  )',
  `current_period_loan_balance`               decimal(20,6) DEFAULT NULL COMMENT '当期贷款余额  （各期还款日（T+1）更新该字段，即截至当期还款日资产的剩余（未偿还）贷款本金余额）',
  `current_account_status`                    varchar(32) DEFAULT NULL COMMENT '当期账户状态： （各期还款日（T+1）更新该字段，\r\n     正常：在还款日前该期应还已还清\r\n     提前还清（早偿）：贷款在该期还款日前提前全部还清\r\n     逾期：贷款在该期还款日时，实还金额小于应还金额。\r\n     ）',
  `penalbond`                                 decimal(20,6) DEFAULT NULL COMMENT '违约金',
  `penalty_interest`                          decimal(20,6) DEFAULT NULL COMMENT '罚息',
  `compensation`                              decimal(20,6) DEFAULT NULL COMMENT '赔偿金（提前还款/逾期所产生的赔偿金）',
  `advanced_commission_charge`                decimal(20,6) DEFAULT NULL COMMENT '提前还款手续费',
  `other_fee`                                 decimal(20,6) DEFAULT NULL COMMENT '其他相关费用 （违约金、罚款、赔偿金和提前还款手续费以外的费用）',
  `is_borrowers_oneself_repayment`            varchar(16) DEFAULT NULL COMMENT '是否借款人本人还款 预定义字段 Y N',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入Id',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `actualrepayinfo_index` (`project_id`,`agency_id`,`serial_number`,`term`,`shoud_repay_date`,`actual_repay_time`,`current_period_loan_balance`) USING BTREE,
  KEY `actualrepayinfo_index1` (`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=19889709 DEFAULT CHARSET=utf8 COMMENT='实际还款信息表-文件七';

-- ----------------------------
-- Table structure for t_actualrepayinfo_temporary
-- ----------------------------
-- DROP TABLE IF EXISTS `t_actualrepayinfo_temporary`;
CREATE TABLE `t_actualrepayinfo_temporary` (
  `id`                                        int(32) NOT NULL AUTO_INCREMENT COMMENT '实际还款信息主键',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `agency_id`                                 varchar(32) DEFAULT NULL COMMENT '机构编号',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `term`                                      int(4) DEFAULT NULL COMMENT '期次',
  `shoud_repay_date`                          date DEFAULT NULL COMMENT '应还款日',
  `shoud_repay_principal`                     decimal(20,6) DEFAULT NULL COMMENT '应还本金（元）',
  `shoud_repay_interest`                      decimal(20,6) DEFAULT NULL COMMENT '应还利息（元）',
  `shoud_repay_fee`                           decimal(20,6) DEFAULT NULL COMMENT '应还费用（元）',
  `repay_type`                                varchar(32) DEFAULT NULL COMMENT '还款类型 1，提前还款 2，正常还款 3，部分还款 4，逾期还款',
  `actual_work_interest_rate`                 decimal(20,6) DEFAULT NULL COMMENT '实际还款执行利率',
  `actual_repay_principal`                    decimal(20,6) DEFAULT NULL COMMENT '实还本金（元）',
  `actual_repay_interest`                     decimal(20,6) DEFAULT NULL COMMENT '实还利息（元）',
  `actual_repay_fee`                          decimal(20,6) DEFAULT NULL COMMENT '实还费用（元）',
  `actual_repay_time`                         date DEFAULT NULL COMMENT '实际还清日期 : ( 若没有还清传还款日期，若还清就传实际还清的日期  )',
  `current_period_loan_balance`               decimal(20,6) DEFAULT NULL COMMENT '当期贷款余额  （各期还款日（T+1）更新该字段，即截至当期还款日资产的剩余（未偿还）贷款本金余额）',
  `current_account_status`                    varchar(32) DEFAULT NULL COMMENT '当期账户状态： （各期还款日（T+1）更新该字段，\r\n     正常：在还款日前该期应还已还清\r\n     提前还清（早偿）：贷款在该期还款日前提前全部还清\r\n     逾期：贷款在该期还款日时，实还金额小于应还金额。\r\n     ）',
  `penalbond`                                 decimal(20,6) DEFAULT NULL COMMENT '违约金',
  `penalty_interest`                          decimal(20,6) DEFAULT NULL COMMENT '罚息',
  `compensation`                              decimal(20,6) DEFAULT NULL COMMENT '赔偿金（提前还款/逾期所产生的赔偿金）',
  `advanced_commission_charge`                decimal(20,6) DEFAULT NULL COMMENT '提前还款手续费',
  `other_fee`                                 decimal(20,6) DEFAULT NULL COMMENT '其他相关费用 （违约金、罚款、赔偿金和提前还款手续费以外的费用）',
  `is_borrowers_oneself_repayment`            varchar(16) DEFAULT NULL COMMENT '是否借款人本人还款 预定义字段 Y N',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入Id',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `actualrepayinfo_index` (`project_id`,`agency_id`,`serial_number`,`term`,`shoud_repay_date`,`actual_repay_time`,`current_period_loan_balance`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=9714542 DEFAULT CHARSET=utf8 COMMENT='实际还款信息表-文件七';

-- ----------------------------
-- Table structure for t_agency
-- ----------------------------
-- DROP TABLE IF EXISTS `t_agency`;
CREATE TABLE `t_agency` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `agency_code`                               varchar(32) NOT NULL COMMENT '机构编号',
  `agency_name`                               varchar(128) NOT NULL COMMENT '机构名',
  `principal`                                 varchar(128) DEFAULT NULL COMMENT '负责人',
  `tenant_id`                                 int(11) DEFAULT NULL COMMENT '租户id',
  `app_token`                                 varchar(128) DEFAULT NULL COMMENT '令牌',
  `ent_no`                                    varchar(32) NOT NULL COMMENT '商户号',
  `status`                                    tinyint(2) DEFAULT '2' COMMENT '机构状态 1表示停用 2表示启用',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `idx_agency_code` (`agency_code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=2049 DEFAULT CHARSET=utf8 COMMENT='机构表';

-- ----------------------------
-- Table structure for t_agency_type
-- ----------------------------
-- DROP TABLE IF EXISTS `t_agency_type`;
CREATE TABLE `t_agency_type` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `agency_type`                               varchar(32) NOT NULL COMMENT '机构类型',
  `agency_type_name`                          varchar(128) DEFAULT NULL COMMENT '机构类型名',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `idx_agency_type` (`agency_type`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=utf8 COMMENT='机构类型表';

-- ----------------------------
-- Table structure for t_asset_bag
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_bag`;
CREATE TABLE `t_asset_bag` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `asset_bag_id`                              varchar(50) DEFAULT NULL COMMENT '资产包编号',
  `asset_bag_name`                            varchar(300) DEFAULT NULL COMMENT '资产包名称',
  `asset_count`                               int(11) DEFAULT NULL COMMENT '资产包资产数量',
  `package_principal_balance`                 decimal(20,2) DEFAULT NULL COMMENT '封包总本金余额',
  `bag_date`                                  date DEFAULT NULL COMMENT '封包日期',
  `bus_product_id`                            varchar(32) DEFAULT NULL COMMENT '业务产品编号',
  `bag_conditions`                            varchar(2048) DEFAULT NULL COMMENT '封包条件',
  `weight_avg_remain_term`                    decimal(10,2) DEFAULT NULL COMMENT '加权平均剩余期限(月)',
  `bag_rate`                                  decimal(20,2) DEFAULT NULL COMMENT '封包平均利率',
  `bag_weight_rate`                           decimal(20,2) DEFAULT NULL COMMENT '加权平均利率',
  `asset_bag_calc_rule`                       tinyint(2) DEFAULT NULL COMMENT '封包规模计算规则 1:按实际剩余本金,2:按还款计划',
  `asset_bag_type`                            tinyint(2) DEFAULT NULL COMMENT '资产包类型 1:初始包,2:循环包',
  `status`                                    tinyint(2) DEFAULT NULL COMMENT '包状态, 1:未封包,2:已封包,3:已解包,4:封包中,5:封包失败',
  `package_filter_id`                         varchar(50) DEFAULT NULL COMMENT '虚拟过滤包id',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_bag_project_id_asset_bag_id` (`project_id`,`asset_bag_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1017 DEFAULT CHARSET=utf8 COMMENT='资产包';

-- ----------------------------
-- Table structure for t_asset_cash_flow
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_cash_flow`;
CREATE TABLE `t_asset_cash_flow` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `product_id`                                int(11) DEFAULT NULL COMMENT '产品Id',
  `period`                                    int(11) DEFAULT NULL COMMENT '期数',
  `period_begin_date`                         date DEFAULT NULL COMMENT '本期起始日',
  `period_end_date`                           date DEFAULT NULL COMMENT '本期终止日',
  `income_report_date`                        date DEFAULT NULL COMMENT '收益报告日',
  `income_transfer_date`                      date DEFAULT NULL COMMENT '收益转付日',
  `asset_principal_amount`                    decimal(20,2) DEFAULT NULL COMMENT '资产端本金流入金额',
  `asset_interest_amount`                     decimal(20,2) DEFAULT NULL COMMENT '资产端利息流入金额',
  `total_principal_amount`                    decimal(20,2) DEFAULT NULL COMMENT '累积本金金额',
  `period_begin_principal_balance`            decimal(20,2) DEFAULT NULL COMMENT '期初未尝本金余额',
  `period_end_principal_balance`              decimal(20,2) DEFAULT NULL COMMENT '期末未尝本金余额',
  `period_begin_total_should_repay_principal` decimal(20,2) DEFAULT NULL COMMENT '期初累积应收本金',
  `period_end_total_should_repay_principal`   decimal(20,2) DEFAULT NULL COMMENT '期末累积应收本金',
  `period_begin_total_should_repay_interest`  decimal(20,2) DEFAULT NULL COMMENT '期初累积应收利息',
  `period_end_total_should_repay_interest`    decimal(20,2) DEFAULT NULL COMMENT '期末累积应收利息',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_asset_cash_flow_project_id_product_id` (`project_id`,`product_id`)
) ENGINE=InnoDB AUTO_INCREMENT=709 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='资产端现金流';

-- ----------------------------
-- Table structure for t_asset_distribution_config
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_distribution_config`;
CREATE TABLE `t_asset_distribution_config` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `asset_bag_id`                              varchar(50) DEFAULT NULL COMMENT '资产包编号',
  `interval`                                  varchar(100) DEFAULT NULL COMMENT '分布区间',
  `unit`                                      tinyint(2) DEFAULT NULL COMMENT '单位(1:万元,2:百分比,3:个月,4:天,5:岁)',
  `distribution_type`                         tinyint(3) DEFAULT NULL COMMENT '分布种类(1:未偿本金余额分布,2:资产质量分布,3:资产利率分布,4:资产合同期数分布,5:资产剩余期数分布,6:已还期数分布,7:还款方式分布,8:借款人年龄分布,9:借款人行业分布,10:借款人年收入分布,11:借款人信用等级分布,12:借款人反欺诈等级分布,13:借款人资产等级分布,14:借款人地区分布,15:抵押率分布)',
  `use_type`                                  tinyint(2) DEFAULT NULL COMMENT '用途类型(1:资产概览,2:封包时)',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_asset_dist_config_project_id` (`project_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='资产分布配置表';

-- ----------------------------
-- Table structure for t_asset_image
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_image`;
CREATE TABLE `t_asset_image` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) NOT NULL COMMENT '项目编号',
  `project_type`                              varchar(32) NOT NULL COMMENT '项目类型-泛华、易鑫、先锋太盟、云南信托、小米',
  `serial_number`                             varchar(32) NOT NULL COMMENT '借据号',
  `guaranty_code`                             varchar(32) NOT NULL COMMENT '抵押物编号',
  `upload_date`                               varchar(32) DEFAULT NULL COMMENT '日期',
  `file_name`                                 varchar(32) NOT NULL COMMENT '影像材料文件名 如：租赁合同_NN，NN为序号',
  `file_type`                                 varchar(32) NOT NULL COMMENT '影像材料文件格式：PDF、jpg、jpeg',
  `material_category`                         tinyint(10) NOT NULL COMMENT '影像材料类别:0-合同协议相关、1-借款方相关、2-抵押物相关、3-放款后',
  `image_material`                            tinyint(10) NOT NULL COMMENT '影像材料:0-借款合同1-抵押合同2-转让协议3-放款凭证4-身份证正面5-身份证反面6-手持协议7-车辆登记证书8-抵押受理证明9-保险单10-客户申请表11-代扣授权书12-担保合同13-手持三方转让协议14-驾驶证15-车辆行驶证16-车辆照片17-评估报告18-放款核实音频',
  `path`                                      varchar(255) NOT NULL COMMENT '文件路径',
  `sftp_path`                                 varchar(255) NOT NULL COMMENT '星连文件路径',
  `image_available`                           tinyint(2) DEFAULT '1' COMMENT '是否可用：0-不可用，1-可用',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_image_type` (`project_id`,`serial_number`,`guaranty_code`,`file_name`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=48688 DEFAULT CHARSET=utf8 COMMENT='资产影像文件路径表';

-- ----------------------------
-- Table structure for t_asset_image_20190522
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_image_20190522`;
CREATE TABLE `t_asset_image_20190522` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) NOT NULL COMMENT '项目编号',
  `project_type`                              varchar(32) NOT NULL COMMENT '项目类型-泛华、易鑫、先锋太盟、云南信托、小米',
  `serial_number`                             varchar(32) NOT NULL COMMENT '借据号',
  `guaranty_code`                             varchar(32) NOT NULL COMMENT '抵押物编号',
  `upload_date`                               varchar(32) DEFAULT NULL COMMENT '日期',
  `file_name`                                 varchar(32) NOT NULL COMMENT '影像材料文件名 如：租赁合同_NN，NN为序号',
  `file_type`                                 varchar(32) NOT NULL COMMENT '影像材料文件格式：PDF、jpg、jpeg',
  `material_category`                         tinyint(10) NOT NULL COMMENT '影像材料类别:0-合同协议相关、1-借款方相关、2-抵押物相关',
  `image_material`                            tinyint(10) NOT NULL COMMENT '影像材料:0-借款合同1-抵押合同2-转让协议3-放款凭证4-身份证正面5-身份证反面6-手持协议7-车辆登记证书8-抵押受理证明9-保险单10-客户申请表11-代扣授权书12-担保合同13-手持三方转让协议14-驾驶证15-车辆行驶证16-车辆照片17-评估报告',
  `path`                                      varchar(255) NOT NULL COMMENT '文件路径',
  `sftp_path`                                 varchar(255) NOT NULL COMMENT '星连文件路径',
  `image_available`                           tinyint(2) DEFAULT '1' COMMENT '是否可用：0-不可用，1-可用',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_image_type` (`project_id`,`serial_number`,`guaranty_code`,`file_name`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=32849 DEFAULT CHARSET=utf8 COMMENT='资产影像文件路径表';

-- ----------------------------
-- Table structure for t_asset_image_deploy
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_image_deploy`;
CREATE TABLE `t_asset_image_deploy` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) NOT NULL COMMENT '项目编号',
  `asset_type`                                varchar(32) NOT NULL COMMENT '资产类型：个人消费贷款、车辆抵押贷款',
  `material_category`                         tinyint(10) NOT NULL COMMENT '影像材料类别:0-合同协议相关、1-借款方相关、2-抵押物相关、3-放款后',
  `image_material`                            tinyint(10) NOT NULL COMMENT '影像材料:0-借款合同1-抵押合同2-转让协议3-放款凭证4-身份证正面5-身份证反面6-手持协议7-车辆登记证书8-抵押受理证明9-保险单10-客户申请表11-代扣授权书12-担保合同13-手持三方转让协议14-驾驶证15-车辆行驶证16-车辆照片17-评估报告18-放款核实音频',
  `image_show`                                tinyint(2) DEFAULT '1' COMMENT '是否展示影像材料：0-不展示，1展示',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_image_type` (`project_id`,`image_material`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1479 DEFAULT CHARSET=utf8 COMMENT='资产影像资料要求配置表';

-- ----------------------------
-- Table structure for t_asset_image_deploy_20190531
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_image_deploy_20190531`;
CREATE TABLE `t_asset_image_deploy_20190531` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) NOT NULL COMMENT '项目编号',
  `asset_type`                                varchar(32) NOT NULL COMMENT '资产类型：个人消费贷款、车辆抵押贷款',
  `material_category`                         tinyint(10) NOT NULL COMMENT '影像材料类别:0-合同协议相关、1-借款方相关、2-抵押物相关、3-放款后',
  `image_material`                            tinyint(10) NOT NULL COMMENT '影像材料:0-借款合同1-抵押合同2-转让协议3-放款凭证4-身份证正面5-身份证反面6-手持协议7-车辆登记证书8-抵押受理证明9-保险单10-客户申请表11-代扣授权书12-担保合同13-手持三方转让协议14-驾驶证15-车辆行驶证16-车辆照片17-评估报告18-放款核实音频',
  `image_show`                                tinyint(2) DEFAULT '1' COMMENT '是否展示影像材料：0-不展示，1展示',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_image_type` (`project_id`,`image_material`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1361 DEFAULT CHARSET=utf8 COMMENT='资产影像资料要求配置表';

-- ----------------------------
-- Table structure for t_asset_image_list
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_image_list`;
CREATE TABLE `t_asset_image_list` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `material_category`                         tinyint(10) NOT NULL COMMENT '影像材料类别:0-合同协议相关、1-借款方相关、2-抵押物相关、3-放款后',
  `image_material`                            tinyint(10) NOT NULL COMMENT '影像材料:0-借款合同1-抵押合同2-转让协议3-放款凭证4-身份证正面5-身份证反面6-手持协议7-车辆登记证书8-抵押受理证明9-保险单10-客户申请表11-代扣授权书12-担保合同13-手持三方转让协议14-驾驶证15-车辆行驶证16-车辆照片17-评估报告18-放款核实音频',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_image_type` (`material_category`,`image_material`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=46 DEFAULT CHARSET=utf8 COMMENT='资产影像资料表';

-- ----------------------------
-- Table structure for t_asset_image_list_20190531
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_image_list_20190531`;
CREATE TABLE `t_asset_image_list_20190531` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `material_category`                         tinyint(10) NOT NULL COMMENT '影像材料类别:0-合同协议相关、1-借款方相关、2-抵押物相关、3-放款后',
  `image_material`                            tinyint(10) NOT NULL COMMENT '影像材料:0-借款合同1-抵押合同2-转让协议3-放款凭证4-身份证正面5-身份证反面6-手持协议7-车辆登记证书8-抵押受理证明9-保险单10-客户申请表11-代扣授权书12-担保合同13-手持三方转让协议14-驾驶证15-车辆行驶证16-车辆照片17-评估报告18-放款核实音频',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_image_type` (`material_category`,`image_material`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=46 DEFAULT CHARSET=utf8 COMMENT='资产影像资料表';

-- ----------------------------
-- Table structure for t_asset_package
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_package`;
CREATE TABLE `t_asset_package` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `asset_bag_id`                              varchar(50) DEFAULT NULL COMMENT '资产包编号',
  `asset_type`                                varchar(64) DEFAULT NULL COMMENT '资产类型',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `contract_code`                             varchar(64) DEFAULT NULL COMMENT '贷款合同编号',
  `contract_amount`                           decimal(32,2) DEFAULT NULL COMMENT '贷款合同金额',
  `interest_rate_type`                        varchar(32) DEFAULT NULL COMMENT '利率类型',
  `contract_interest_rate`                    float(20,4) DEFAULT NULL COMMENT '合同利率',
  `repayment_type`                            varchar(32) DEFAULT NULL COMMENT '还款方式',
  `guarantee_type`                            varchar(32) DEFAULT NULL COMMENT '担保方式',
  `borrower_type`                             varchar(32) DEFAULT NULL COMMENT '借款人类型',
  `borrower_name`                             varchar(100) DEFAULT NULL COMMENT '借款人姓名',
  `borrower_industry`                         varchar(32) DEFAULT NULL COMMENT '借款人行业',
  `birthday`                                  date DEFAULT NULL COMMENT '出生日期',
  `age`                                       int(11) DEFAULT NULL COMMENT '年龄',
  `province`                                  varchar(32) DEFAULT NULL COMMENT '所在省份',
  `city`                                      varchar(32) DEFAULT NULL COMMENT '所在城市',
  `annual_income`                             decimal(32,2) DEFAULT NULL COMMENT '年收入',
  `account_age`                               float(11,2) DEFAULT NULL COMMENT '帐龄',
  `loan_issue_date`                           date DEFAULT NULL COMMENT '贷款发放日',
  `loan_expiry_date`                          date DEFAULT NULL COMMENT '贷款到期日',
  `frist_repayment_date`                      date DEFAULT NULL COMMENT '首次还款日',
  `bag_date`                                  date DEFAULT NULL COMMENT '封包日期',
  `package_principal_balance`                 decimal(32,2) DEFAULT '0.00' COMMENT '封包本金余额',
  `periods`                                   int(11) DEFAULT NULL COMMENT '总期数',
  `package_remain_principal`                  decimal(20,2) DEFAULT NULL COMMENT '封包时当前剩余本金(元)',
  `package_remain_periods`                    tinyint(4) DEFAULT NULL COMMENT '封包时当前剩余期数',
  `wind_control_status`                       varchar(32) DEFAULT NULL COMMENT '风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
  `cheat_level`                               varchar(10) DEFAULT NULL COMMENT '反欺诈等级:1~5,-1:null,其它:异常值',
  `score_range`                               varchar(10) DEFAULT NULL COMMENT '评分等级:1~20,-1:null,其它:异常值',
  `score_level`                               varchar(10) DEFAULT NULL COMMENT '评分区间:1~5,-1:null,其它:异常值',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建记录时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `ix_asset_package_project_id_bag_id` (`project_id`,`asset_bag_id`,`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=161922 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='封包时基础资产信息表';

-- ----------------------------
-- Table structure for t_asset_package_cash_flow
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_package_cash_flow`;
CREATE TABLE `t_asset_package_cash_flow` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `asset_bag_id`                              varchar(50) DEFAULT NULL COMMENT '资产包编号',
  `collection_date`                           date NOT NULL COMMENT '归集日',
  `begin_principal_balance`                   decimal(32,2) DEFAULT NULL COMMENT '期初本金余额(元)',
  `amount`                                    decimal(32,2) DEFAULT NULL COMMENT '金额(元)',
  `principal`                                 decimal(32,2) DEFAULT NULL COMMENT '本金(元)',
  `interest`                                  decimal(32,2) DEFAULT NULL COMMENT '利息(元)',
  `cost`                                      decimal(32,2) DEFAULT NULL COMMENT '费用(元)',
  `end_principal_balance`                     decimal(32,2) DEFAULT NULL COMMENT '期末本金余额(元)',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_cash_project_id_asset_bag_id` (`project_id`,`asset_bag_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=95466 DEFAULT CHARSET=utf8 COMMENT='封包资产现金流';

-- ----------------------------
-- Table structure for t_asset_package_guaranty
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_package_guaranty`;
CREATE TABLE `t_asset_package_guaranty` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `asset_bag_id`                              varchar(50) DEFAULT NULL COMMENT '资产包编号',
  `contract_code`                             varchar(64) DEFAULT NULL COMMENT '贷款合同编号',
  `guaranty_code`                             varchar(100) DEFAULT NULL COMMENT '抵押物编号',
  `pawn_value`                                decimal(20,6) DEFAULT NULL COMMENT '评估价格(元)',
  `car_sales_price`                           decimal(20,6) DEFAULT NULL COMMENT '车辆销售价格(元)',
  `car_new_price`                             decimal(20,6) DEFAULT NULL COMMENT '新车指导价(元)',
  `total_investment`                          decimal(20,6) DEFAULT NULL COMMENT '投资总额(元)',
  `purchase_tax_amouts`                       decimal(20,6) DEFAULT NULL COMMENT '购置税金额(元)',
  `car_brand`                                 varchar(100) DEFAULT NULL COMMENT '车辆品牌',
  `car_type`                                  varchar(32) DEFAULT NULL COMMENT '车类型 预定义字段：新车 二手车',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建记录时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `ix_asset_package_guaranty` (`project_id`,`asset_bag_id`,`serial_number`,`guaranty_code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=25715 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='封包时基础资产抵押物信息表';

-- ----------------------------
-- Table structure for t_asset_package_profile
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_package_profile`;
CREATE TABLE `t_asset_package_profile` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `asset_bag_id`                              varchar(50) DEFAULT NULL COMMENT '资产包编号',
  `asset_count`                               int(11) DEFAULT NULL COMMENT '资产数量(条)',
  `principal_balance`                         decimal(32,2) DEFAULT NULL COMMENT '本金余额(元)',
  `principal_interest_balance`                decimal(32,2) DEFAULT NULL COMMENT '本息余额(元)',
  `borrower_count`                            int(11) DEFAULT NULL COMMENT '借款人数量(户)',
  `borrower_avg_principal_balance`            decimal(32,2) DEFAULT NULL COMMENT '借款人平均本金余额(元)',
  `single_max_principal_balance`              decimal(32,2) DEFAULT NULL COMMENT '单笔最大本金余额(元)',
  `single_min_principal_balance`              decimal(32,2) DEFAULT NULL COMMENT '单笔最小本金余额(元)',
  `avg_principal_balance`                     decimal(32,2) DEFAULT NULL COMMENT '平均本金余额(元)',
  `single_max_ontract_amount`                 decimal(32,2) DEFAULT NULL COMMENT '单笔最大合同金额(元)',
  `single_min_ontract_amount`                 decimal(32,2) DEFAULT NULL COMMENT '单笔最小合同金额(元)',
  `avg_contract_amount`                       decimal(32,2) DEFAULT NULL COMMENT '平均合同金额(元)',
  `single_max_contract_periods`               int(5) DEFAULT NULL COMMENT '单笔最大合同期数(月)',
  `single_min_contract_periods`               int(5) DEFAULT NULL COMMENT '单笔最小合同期数(月)',
  `avg_contract_periods`                      decimal(10,2) DEFAULT NULL COMMENT '平均合同期数(月)',
  `weight_avg_contract_periods`               decimal(10,2) DEFAULT NULL COMMENT '加权平均合同期数(月)',
  `single_max_remain_periods`                 int(5) DEFAULT NULL COMMENT '单笔最大剩余期数(月)',
  `single_min_remain_periods`                 int(5) DEFAULT NULL COMMENT '单笔最小剩余期数(月)',
  `avg_remain_periods`                        decimal(10,2) DEFAULT NULL COMMENT '平均剩余期数(月)',
  `weight_avg_remain_periods`                 decimal(10,2) DEFAULT NULL COMMENT '加权平均剩余期数(月)',
  `single_max_replay_periods`                 int(5) DEFAULT NULL COMMENT '单笔最大已还期数(月)',
  `single_min_replay_periods`                 int(5) DEFAULT NULL COMMENT '单笔最小已还期数(月)',
  `avg_replay_periods`                        decimal(10,2) DEFAULT NULL COMMENT '平均已还期数(月)',
  `weight_avg_replay_periods`                 decimal(10,2) DEFAULT NULL COMMENT '加权平均已还期数(月)',
  `single_max_interest_rate`                  decimal(10,2) DEFAULT NULL COMMENT '单笔最大年利率(%)',
  `single_min_interest_rate`                  decimal(10,2) DEFAULT NULL COMMENT '单笔最小年利率(%)',
  `avg_interest_rate`                         decimal(10,2) DEFAULT NULL COMMENT '平均年利率(%)',
  `weight_avg_interest_rate`                  decimal(10,2) DEFAULT NULL COMMENT '加权平均年利率(%)',
  `max_borrower_age`                          decimal(5,2) DEFAULT NULL COMMENT '借款人最大年龄(岁)',
  `min_borrower_age`                          decimal(5,2) DEFAULT NULL COMMENT '借款人最小年龄(岁)',
  `avg_borrower_age`                          decimal(5,2) DEFAULT NULL COMMENT '借款人平均年龄(岁)',
  `weight_avg_borrower_age`                   decimal(5,2) DEFAULT NULL COMMENT '借款人加权平均年龄(岁)',
  `max_borrower_annual_income`                decimal(32,2) DEFAULT NULL COMMENT '借款人最大年收入(元)',
  `min_borrower_annual_income`                decimal(32,2) DEFAULT NULL COMMENT '借款人最小年收入(元)',
  `avg_borrower_annual_income`                decimal(32,2) DEFAULT NULL COMMENT '借款人平均年收入(元)',
  `weight_avg_borrower_annual_income`         decimal(32,2) DEFAULT NULL COMMENT '借款人加权平均年收入(元)',
  `max_income_debt_ratio`                     decimal(10,2) DEFAULT NULL COMMENT '最大收入债务比(%)',
  `min_income_debt_ratio`                     decimal(10,2) DEFAULT NULL COMMENT '最小收入债务比(%)',
  `avg_income_debt_ratio`                     decimal(10,2) DEFAULT NULL COMMENT '平均收入债务比(%)',
  `weight_avg_income_debt_ratio`              decimal(10,2) DEFAULT NULL COMMENT '加权平均收入债务比(%)',
  `mortgage_asset_balance`                    decimal(32,2) DEFAULT NULL COMMENT '抵押的资产余额(元)',
  `mortgage_asset_count`                      int(11) DEFAULT NULL COMMENT '抵押的资产笔数(笔)',
  `mortgage_init_valuation`                   decimal(32,2) DEFAULT NULL COMMENT '抵押初始评估价值(元)',
  `weight_avg_mortgage_rate`                  decimal(10,2) DEFAULT NULL COMMENT '加权平均抵押率(%)',
  `mortgage_asset_balance_ratio`              decimal(10,2) DEFAULT NULL COMMENT '抵押资产余额占比(%)',
  `mortgage_asset_count_ratio`                decimal(10,2) DEFAULT NULL COMMENT '抵押资产笔数占比(%)',
  `min_aging`                                 decimal(10,2) DEFAULT NULL COMMENT '最小账龄',
  `max_aging`                                 decimal(10,2) DEFAULT NULL COMMENT '最大账龄',
  `avg_aging`                                 decimal(10,2) DEFAULT NULL COMMENT '平均账龄',
  `weighted_aging`                            decimal(10,2) DEFAULT NULL COMMENT '加权平均账龄',
  `min_surplus_term`                          decimal(10,2) DEFAULT NULL COMMENT '最小合同剩余期限',
  `max_surplus_term`                          decimal(10,2) DEFAULT NULL COMMENT '最大合同剩余期限',
  `avg_surplus_term`                          decimal(10,2) DEFAULT NULL COMMENT '平均合同剩余期限',
  `weighted_surplus_term`                     decimal(10,2) DEFAULT NULL COMMENT '加权平均剩余期限',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_profile_project_id_asset_bag_id` (`project_id`,`asset_bag_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1075 DEFAULT CHARSET=utf8 COMMENT='封包资产总览';

-- ----------------------------
-- Table structure for t_asset_transfer_info
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_transfer_info`;
CREATE TABLE `t_asset_transfer_info` (
  `id`                                        int(11) unsigned zerofill NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `product_code`                              varchar(50) DEFAULT NULL COMMENT '产品代码',
  `asset_bag_id`                              varchar(50) DEFAULT NULL COMMENT '资产包编码',
  `initial_start_date`                        date DEFAULT NULL COMMENT '初始起算日',
  `trust_establish_date`                      date DEFAULT NULL COMMENT '信托设立日',
  `transfer_date`                             date DEFAULT NULL COMMENT '转让日',
  `package_amount`                            decimal(20,6) DEFAULT NULL COMMENT '封包规模',
  `actual_amount`                             decimal(20,6) DEFAULT NULL COMMENT '实际结算规模',
  `termination_confirmation_type`             varchar(2) DEFAULT NULL COMMENT '终止确认类型',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=42 DEFAULT CHARSET=utf8 COMMENT='资产转让信息表';

-- ----------------------------
-- Table structure for t_asset_wind_control_history
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_wind_control_history`;
CREATE TABLE `t_asset_wind_control_history` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `asset_bag_id`                              varchar(50) DEFAULT NULL COMMENT '资产包编号',
  `asset_type`                                varchar(64) DEFAULT NULL COMMENT '资产类型',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `contract_code`                             varchar(64) DEFAULT NULL COMMENT '贷款合同编号',
  `statistics_date`                           date DEFAULT NULL COMMENT '统计日期',
  `status`                                    int(4) DEFAULT '0' COMMENT '资产状态(-1征信异常 0准入 1在池 2封包 3发行 4历史',
  `wind_control_status`                       varchar(32) DEFAULT NULL COMMENT '当前风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
  `wind_control_status_pool`                  varchar(32) DEFAULT NULL COMMENT '入池时风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
  `cheat_level`                               varchar(10) DEFAULT NULL COMMENT '反欺诈等级:1~5,-1:null,其它:异常值',
  `score_range`                               varchar(10) DEFAULT NULL COMMENT '评分等级:1~20,-1:null,其它:异常值',
  `score_level`                               varchar(10) DEFAULT NULL COMMENT '评分区间:1~5,-1:null,其它:异常值',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建记录时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `basicasset_index` (`project_id`),
  KEY `basicasset_index1` (`serial_number`),
  KEY `basicasset_index2` (`contract_code`),
  KEY `basicasset_index3` (`statistics_date`)
) ENGINE=InnoDB AUTO_INCREMENT=2813968 DEFAULT CHARSET=utf8 COMMENT='基础资产风控评分历史表';

-- ----------------------------
-- Table structure for t_assetaccountcheck
-- ----------------------------
-- DROP TABLE IF EXISTS `t_assetaccountcheck`;
CREATE TABLE `t_assetaccountcheck` (
  `id`                                        int(32) NOT NULL AUTO_INCREMENT COMMENT '资产对账信息主键',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `agency_id`                                 varchar(32) DEFAULT NULL COMMENT '机构编号',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `is_change_repay_schedule`                  varchar(16) DEFAULT NULL COMMENT '是否变更还款计划（YES - 是，NO - 否 ）',
  `change_time`                               date DEFAULT NULL COMMENT '变更时间',
  `total_loan_amount`                         decimal(20,6) DEFAULT NULL COMMENT '贷款总金额',
  `year_loan_rate`                            varchar(32) DEFAULT NULL COMMENT '贷款年利率',
  `total_term`                                int(4) DEFAULT NULL COMMENT '总期数',
  `already_repay_term`                        int(4) DEFAULT NULL COMMENT '已还期数',
  `remain_term`                               int(4) DEFAULT NULL COMMENT '剩余期数',
  `remain_principal`                          decimal(20,6) DEFAULT NULL COMMENT '剩余本金',
  `remain_interest`                           decimal(20,6) DEFAULT NULL COMMENT '剩余利息',
  `remain_other_fee`                          decimal(20,6) DEFAULT NULL COMMENT '剩余其他费用',
  `settle_payment_advance`                    decimal(20,6) DEFAULT NULL COMMENT '提前结清实还费用',
  `early_settlement_interest`                 decimal(20,6) DEFAULT NULL COMMENT '提前结清实还利息',
  `principal_settled_advance`                 decimal(20,6) DEFAULT NULL COMMENT '提前结清实还本金',
  `next_term_shouldrepay_time`                date DEFAULT NULL COMMENT '下一期应还款日',
  `asset_status`                              varchar(32) DEFAULT NULL COMMENT '资产状态 枚举 正常，逾期，已结清 ',
  `closeoff_reason`                           varchar(255) DEFAULT NULL COMMENT '结清原因 枚举 正常结清，提前结清，处置结束，资产回购，逾期结清，合同取消',
  `current_overdue_principal`                 decimal(20,6) DEFAULT NULL COMMENT '当前逾期本金',
  `current_overdue_interest`                  decimal(20,6) DEFAULT NULL COMMENT '当前逾期利息',
  `current_overdue_fee`                       decimal(20,6) DEFAULT NULL COMMENT '当前逾期费用',
  `current_overdue_daynum`                    int(4) DEFAULT NULL COMMENT '当前逾期天数',
  `total_overdue_daynum`                      int(4) DEFAULT NULL COMMENT '累计逾期天数',
  `history_most_overdue_daynum`               int(4) DEFAULT NULL COMMENT '历史最高逾期天数',
  `history_total_overdue_daynum`              int(4) DEFAULT NULL COMMENT '历史累计逾期天数',
  `current_overdue_termnum`                   int(4) DEFAULT NULL COMMENT '当前逾期期数',
  `total_overdue_termnum`                     int(4) DEFAULT NULL COMMENT '累计逾期期数',
  `history_longest_overdue_term`              int(4) DEFAULT NULL COMMENT '历史单次最长逾期期数',
  `history_top_overdue_principal`             decimal(20,6) DEFAULT NULL COMMENT '历史最大逾期本金',
  `data_extract_time`                         date DEFAULT NULL COMMENT '数据提取日',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入Id',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport,3:systemgenerated',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `assetaccountcheck_index` (`project_id`,`agency_id`,`serial_number`,`data_extract_time`) USING BTREE,
  KEY `assetaccountcheck_index2` (`project_id`,`serial_number`,`data_extract_time`) USING BTREE,
  KEY `assetaccountcheck_index3` (`serial_number`,`data_extract_time`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=191291599 DEFAULT CHARSET=utf8 COMMENT='资产对账信息表-文件十';

-- ----------------------------
-- Table structure for t_assetaccountcheck_20190428
-- ----------------------------
-- DROP TABLE IF EXISTS `t_assetaccountcheck_20190428`;
CREATE TABLE `t_assetaccountcheck_20190428` (
  `id`                                        int(32) NOT NULL AUTO_INCREMENT COMMENT '资产对账信息主键',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `agency_id`                                 varchar(32) DEFAULT NULL COMMENT '机构编号',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `is_change_repay_schedule`                  varchar(16) DEFAULT NULL COMMENT '是否变更还款计划（YES - 是，NO - 否 ）',
  `change_time`                               date DEFAULT NULL COMMENT '变更时间',
  `total_loan_amount`                         decimal(20,6) DEFAULT NULL COMMENT '贷款总金额',
  `year_loan_rate`                            varchar(32) DEFAULT NULL COMMENT '贷款年利率',
  `total_term`                                int(4) DEFAULT NULL COMMENT '总期数',
  `already_repay_term`                        int(4) DEFAULT NULL COMMENT '已还期数',
  `remain_term`                               int(4) DEFAULT NULL COMMENT '剩余期数',
  `remain_principal`                          decimal(20,6) DEFAULT NULL COMMENT '剩余本金',
  `remain_interest`                           decimal(20,6) DEFAULT NULL COMMENT '剩余利息',
  `remain_other_fee`                          decimal(20,6) DEFAULT NULL COMMENT '剩余其他费用',
  `settle_payment_advance`                    decimal(20,6) DEFAULT NULL COMMENT '提前结清实还费用',
  `early_settlement_interest`                 decimal(20,6) DEFAULT NULL COMMENT '提前结清实还利息',
  `principal_settled_advance`                 decimal(20,6) DEFAULT NULL COMMENT '提前结清实还本金',
  `next_term_shouldrepay_time`                date DEFAULT NULL COMMENT '下一期应还款日',
  `asset_status`                              varchar(32) DEFAULT NULL COMMENT '资产状态 枚举 正常，逾期，已结清 ',
  `closeoff_reason`                           varchar(255) DEFAULT NULL COMMENT '结清原因 枚举 正常结清，提前结清，处置结束，资产回购，逾期结清，合同取消',
  `current_overdue_principal`                 decimal(20,6) DEFAULT NULL COMMENT '当前逾期本金',
  `current_overdue_interest`                  decimal(20,6) DEFAULT NULL COMMENT '当前逾期利息',
  `current_overdue_fee`                       decimal(20,6) DEFAULT NULL COMMENT '当前逾期费用',
  `current_overdue_daynum`                    int(4) DEFAULT NULL COMMENT '当前逾期天数',
  `total_overdue_daynum`                      int(4) DEFAULT NULL COMMENT '累计逾期天数',
  `history_most_overdue_daynum`               int(4) DEFAULT NULL COMMENT '历史最高逾期天数',
  `history_total_overdue_daynum`              int(4) DEFAULT NULL COMMENT '历史累计逾期天数',
  `current_overdue_termnum`                   int(4) DEFAULT NULL COMMENT '当前逾期期数',
  `total_overdue_termnum`                     int(4) DEFAULT NULL COMMENT '累计逾期期数',
  `history_longest_overdue_term`              int(4) DEFAULT NULL COMMENT '历史单次最长逾期期数',
  `history_top_overdue_principal`             decimal(20,6) DEFAULT NULL COMMENT '历史最大逾期本金',
  `data_extract_time`                         date DEFAULT NULL COMMENT '数据提取日',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入Id',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport,3:systemgenerated',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `assetaccountcheck_index` (`project_id`,`agency_id`,`serial_number`,`data_extract_time`) USING BTREE,
  KEY `assetaccountcheck_index2` (`project_id`,`serial_number`,`data_extract_time`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=96307930 DEFAULT CHARSET=utf8 COMMENT='资产对账信息表-文件十';

-- ----------------------------
-- Table structure for t_assetaccountcheck_temporary
-- ----------------------------
-- DROP TABLE IF EXISTS `t_assetaccountcheck_temporary`;
CREATE TABLE `t_assetaccountcheck_temporary` (
  `id`                                        int(32) NOT NULL AUTO_INCREMENT COMMENT '资产对账信息主键',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `agency_id`                                 varchar(32) DEFAULT NULL COMMENT '机构编号',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `is_change_repay_schedule`                  varchar(16) DEFAULT NULL COMMENT '是否变更还款计划（YES - 是，NO - 否 ）',
  `change_time`                               date DEFAULT NULL COMMENT '变更时间',
  `total_loan_amount`                         decimal(20,6) DEFAULT NULL COMMENT '贷款总金额',
  `year_loan_rate`                            varchar(32) DEFAULT NULL COMMENT '贷款年利率',
  `total_term`                                int(4) DEFAULT NULL COMMENT '总期数',
  `already_repay_term`                        int(4) DEFAULT NULL COMMENT '已还期数',
  `remain_term`                               int(4) DEFAULT NULL COMMENT '剩余期数',
  `remain_principal`                          decimal(20,6) DEFAULT NULL COMMENT '剩余本金',
  `remain_interest`                           decimal(20,6) DEFAULT NULL COMMENT '剩余利息',
  `remain_other_fee`                          decimal(20,6) DEFAULT NULL COMMENT '剩余其他费用',
  `settle_payment_advance`                    decimal(20,6) DEFAULT NULL COMMENT '提前结清实还费用',
  `early_settlement_interest`                 decimal(20,6) DEFAULT NULL COMMENT '提前结清实还利息',
  `principal_settled_advance`                 decimal(20,6) DEFAULT NULL COMMENT '提前结清实还本金',
  `next_term_shouldrepay_time`                date DEFAULT NULL COMMENT '下一期应还款日',
  `asset_status`                              varchar(32) DEFAULT NULL COMMENT '资产状态 枚举 正常，逾期，已结清 ',
  `closeoff_reason`                           varchar(255) DEFAULT NULL COMMENT '结清原因 枚举 正常结清，提前结清，处置结束，资产回购，逾期结清，合同取消',
  `current_overdue_principal`                 decimal(20,6) DEFAULT NULL COMMENT '当前逾期本金',
  `current_overdue_interest`                  decimal(20,6) DEFAULT NULL COMMENT '当前逾期利息',
  `current_overdue_fee`                       decimal(20,6) DEFAULT NULL COMMENT '当前逾期费用',
  `current_overdue_daynum`                    int(4) DEFAULT NULL COMMENT '当前逾期天数',
  `total_overdue_daynum`                      int(4) DEFAULT NULL COMMENT '累计逾期天数',
  `history_most_overdue_daynum`               int(4) DEFAULT NULL COMMENT '历史最高逾期天数',
  `history_total_overdue_daynum`              int(4) DEFAULT NULL COMMENT '历史累计逾期天数',
  `current_overdue_termnum`                   int(4) DEFAULT NULL COMMENT '当前逾期期数',
  `total_overdue_termnum`                     int(4) DEFAULT NULL COMMENT '累计逾期期数',
  `history_longest_overdue_term`              int(4) DEFAULT NULL COMMENT '历史单次最长逾期期数',
  `history_top_overdue_principal`             decimal(20,6) DEFAULT NULL COMMENT '历史最大逾期本金',
  `data_extract_time`                         date DEFAULT NULL COMMENT '数据提取日',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入Id',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport,3:systemgenerated',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `assetaccountcheck_index` (`project_id`,`agency_id`,`serial_number`,`data_extract_time`) USING BTREE,
  KEY `assetaccountcheck_index2` (`project_id`,`serial_number`,`data_extract_time`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1703582 DEFAULT CHARSET=utf8 COMMENT='资产对账信息表-文件十';

-- ----------------------------
-- Table structure for t_assetaddtradeinfo
-- ----------------------------
-- DROP TABLE IF EXISTS `t_assetaddtradeinfo`;
CREATE TABLE `t_assetaddtradeinfo` (
  `id`                                        int(32) NOT NULL AUTO_INCREMENT COMMENT '资产补充交易信息主键',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `agency_id`                                 varchar(32) DEFAULT NULL COMMENT '机构编号',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `trade_type`                                varchar(32) DEFAULT NULL COMMENT '交易类型',
  `trade_reason`                              varchar(255) DEFAULT NULL COMMENT '交易原因',
  `trade_time`                                date DEFAULT NULL COMMENT '交易日期',
  `trade_total_amount`                        decimal(20,6) DEFAULT NULL COMMENT '交易总金额',
  `principal`                                 decimal(20,6) DEFAULT NULL COMMENT '本金',
  `interest`                                  decimal(20,6) DEFAULT NULL COMMENT '利息',
  `penalty_interest`                          decimal(20,6) DEFAULT NULL COMMENT '罚息',
  `other_fee`                                 decimal(20,6) DEFAULT NULL COMMENT '其他费用',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入Id',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `assetaddtradeinfo_index` (`project_id`,`agency_id`,`serial_number`) USING BTREE,
  KEY `assetaddtradeinfo_index1` (`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=38 DEFAULT CHARSET=utf8 COMMENT='资产补充交易信息表-文件九';

-- ----------------------------
-- Table structure for t_assetdealprocessinfo
-- ----------------------------
-- DROP TABLE IF EXISTS `t_assetdealprocessinfo`;
CREATE TABLE `t_assetdealprocessinfo` (
  `id`                                        int(32) NOT NULL AUTO_INCREMENT COMMENT '资产处置过程信息主键',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `agency_id`                                 varchar(32) DEFAULT NULL COMMENT '机构编号',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `deal_status`                               varchar(32) DEFAULT NULL COMMENT '处置状态（处置中 已处置 ）',
  `deal_type`                                 varchar(32) DEFAULT NULL COMMENT '处置类型（SUSONG-诉讼 FEISONGSU-非诉讼 ）',
  `lawsuit_node`                              varchar(32) DEFAULT NULL COMMENT '诉讼节点 处置开始 诉讼准备 法院受理 执行拍卖 处置结束',
  `lawsuit_node_time`                         date DEFAULT NULL COMMENT '诉讼节点时间',
  `deal_result`                               varchar(32) DEFAULT NULL COMMENT '处置结果',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入Id',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `assetdealprocessinfo_index` (`project_id`,`agency_id`,`serial_number`) USING BTREE,
  KEY `assetdealprocessinfo_index1` (`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=48 DEFAULT CHARSET=utf8 COMMENT='资产处置过程信息表-文件八';

-- ----------------------------
-- Table structure for t_assettradeflow
-- ----------------------------
-- DROP TABLE IF EXISTS `t_assettradeflow`;
CREATE TABLE `t_assettradeflow` (
  `id`                                        int(32) NOT NULL AUTO_INCREMENT COMMENT '资产交易支付流水信息',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `agency_id`                                 varchar(32) DEFAULT NULL COMMENT '机构编号',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `trade_channel`                             varchar(32) DEFAULT NULL COMMENT '交易渠道',
  `trade_type`                                varchar(32) DEFAULT NULL COMMENT '交易类型',
  `order_number`                              varchar(100) DEFAULT NULL COMMENT '订单编号',
  `order_amount`                              decimal(20,6) DEFAULT NULL COMMENT '订单金额',
  `trade_currency`                            varchar(32) DEFAULT NULL COMMENT '币种',
  `name`                                      varchar(255) DEFAULT NULL COMMENT '姓名',
  `bank_account`                              varchar(100) DEFAULT NULL COMMENT '银行账号',
  `trade_time`                                date DEFAULT NULL COMMENT '交易日期',
  `trade_status`                              varchar(32) DEFAULT NULL COMMENT '交易状态',
  `trade_digest`                              varchar(128) DEFAULT NULL COMMENT '交易摘要',
  `confirm_repay_time`                        date DEFAULT NULL COMMENT '确认还款日期',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入Id',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `assettradeflow_index` (`project_id`,`agency_id`,`serial_number`,`order_number`) USING BTREE,
  KEY `assettradeflow_index1` (`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=12573496 DEFAULT CHARSET=utf8 COMMENT='资产交易支付流水信息表-文件六';

-- ----------------------------
-- Table structure for t_associatesinfo
-- ----------------------------
-- DROP TABLE IF EXISTS `t_associatesinfo`;
CREATE TABLE `t_associatesinfo` (
  `id`                                        int(32) NOT NULL AUTO_INCREMENT COMMENT '关联人信息主键',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `agency_id`                                 varchar(32) DEFAULT NULL COMMENT '机构编号',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `contract_role`                             varchar(32) DEFAULT NULL COMMENT '合同角色-枚举 预定义字段: 共同借款人, 担保人, 无',
  `borrower_name`                             varchar(100) DEFAULT NULL COMMENT '客户姓名',
  `certificate_type`                          varchar(32) DEFAULT NULL COMMENT '证件类型-预定义字段：身份证 护照 户口本 外国人护照',
  `document_num`                              varchar(100) DEFAULT NULL COMMENT '身份证号',
  `phone_num`                                 varchar(32) DEFAULT NULL COMMENT '手机号',
  `age`                                       int(4) DEFAULT NULL COMMENT '年龄',
  `sex`                                       varchar(16) DEFAULT NULL COMMENT '性别预定义字段：男，女',
  `relationship_with_borrowers`               varchar(32) DEFAULT NULL COMMENT '与借款人关系 预定义字段：配偶 父母 子女 亲戚 朋友 同事',
  `career`                                    varchar(32) DEFAULT NULL COMMENT '职业',
  `working_state`                             varchar(32) DEFAULT NULL COMMENT '工作状态 预定义字段：在职 失业',
  `annual_income`                             decimal(20,6) DEFAULT NULL COMMENT '年收入(元)',
  `mailing_address`                           varchar(128) DEFAULT NULL COMMENT '通讯地址',
  `unit_address`                              varchar(129) DEFAULT NULL COMMENT '单位详细地址',
  `unit_contact_mode`                         varchar(130) DEFAULT NULL COMMENT '单位联系方式',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入Id',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `associatesinfo_index` (`project_id`,`agency_id`,`serial_number`,`document_num`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=203499 DEFAULT CHARSET=utf8 COMMENT='关联人信息表-文件三';

-- ----------------------------
-- Table structure for t_basic_asset
-- ----------------------------
-- DROP TABLE IF EXISTS `t_basic_asset`;
CREATE TABLE `t_basic_asset` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入记录编号',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `asset_bag_id`                              varchar(50) DEFAULT NULL COMMENT '资产包编号',
  `asset_type`                                varchar(64) DEFAULT NULL COMMENT '资产类型',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `contract_code`                             varchar(64) DEFAULT NULL COMMENT '贷款合同编号',
  `contract_amount`                           decimal(32,2) DEFAULT NULL COMMENT '贷款合同金额',
  `interest_rate_type`                        varchar(32) DEFAULT NULL COMMENT '利率类型',
  `contract_interest_rate`                    float(20,4) DEFAULT NULL COMMENT '合同利率',
  `base_interest_rate`                        float(20,4) DEFAULT NULL COMMENT '基准利率',
  `fixed_interest_rate`                       float(20,4) DEFAULT NULL COMMENT '固定利率',
  `fixed_interest_diff`                       varchar(45) DEFAULT NULL COMMENT '固定利差',
  `interest_rate_ajustment`                   varchar(64) DEFAULT NULL COMMENT '利率调整方式',
  `loan_issue_date`                           date DEFAULT NULL COMMENT '贷款发放日',
  `loan_expiry_date`                          date DEFAULT NULL COMMENT '贷款到期日',
  `frist_repayment_date`                      date DEFAULT NULL COMMENT '首次还款日',
  `repayment_type`                            varchar(32) DEFAULT NULL COMMENT '还款方式',
  `repayment_frequency`                       varchar(32) DEFAULT NULL COMMENT '还款频率',
  `loan_repay_date`                           int(11) DEFAULT NULL COMMENT '贷款还款日',
  `tail_amount`                               decimal(32,2) DEFAULT NULL COMMENT '尾款金额',
  `tail_amount_rate`                          float DEFAULT NULL COMMENT '尾款金额占比',
  `consume_use`                               varchar(32) DEFAULT NULL COMMENT '消费用途',
  `guarantee_type`                            varchar(32) DEFAULT NULL COMMENT '担保方式',
  `extract_date`                              date DEFAULT NULL COMMENT '提取日期',
  `extract_date_principal_amount`             decimal(32,2) DEFAULT NULL COMMENT '提取日本金金额',
  `loan_cur_interest_rate`                    float DEFAULT NULL COMMENT '贷款现行利率',
  `borrower_type`                             varchar(32) DEFAULT NULL COMMENT '借款人类型',
  `borrower_name`                             varchar(300) DEFAULT NULL COMMENT '借款人姓名',
  `document_type`                             varchar(32) DEFAULT NULL COMMENT '证件类型',
  `document_num`                              varchar(100) DEFAULT NULL COMMENT '证件号码',
  `borrower_rating`                           varchar(32) DEFAULT NULL COMMENT '借款人评级',
  `borrower_industry`                         varchar(32) DEFAULT NULL COMMENT '借款人行业',
  `phone_num`                                 varchar(100) DEFAULT NULL COMMENT '手机号码',
  `sex`                                       varchar(32) DEFAULT NULL COMMENT '性别',
  `birthday`                                  date DEFAULT NULL COMMENT '出生日期',
  `age`                                       int(11) DEFAULT NULL COMMENT '年龄',
  `province`                                  varchar(32) DEFAULT NULL COMMENT '所在省份',
  `city`                                      varchar(32) DEFAULT NULL COMMENT '所在城市',
  `marital_status`                            varchar(32) DEFAULT NULL COMMENT '婚姻状况',
  `country`                                   varchar(32) DEFAULT NULL COMMENT '国籍',
  `annual_income`                             decimal(32,2) DEFAULT NULL COMMENT '年收入',
  `income_debt_rate`                          float(32,4) DEFAULT NULL COMMENT '收入债务比',
  `education_level`                           varchar(32) DEFAULT NULL COMMENT '教育程度',
  `period_exp`                                int(11) DEFAULT NULL COMMENT '提取日剩余还款期数',
  `account_age`                               float(11,2) DEFAULT NULL COMMENT '帐龄',
  `residual_maturity_con`                     float(11,2) DEFAULT NULL COMMENT '合同期限',
  `residual_maturity_ext`                     float(11,2) DEFAULT NULL COMMENT '提取日剩余期限',
  `package_principal_balance`                 decimal(32,2) DEFAULT '0.00' COMMENT '封包本金余额',
  `statistics_date`                           date DEFAULT NULL COMMENT '统计日期',
  `extract_interest_date`                     date DEFAULT NULL COMMENT '提取日计息日',
  `curr_over_days`                            int(11) DEFAULT '0' COMMENT '当前逾期天数',
  `remain_counts`                             int(11) DEFAULT NULL COMMENT '当前剩余期数',
  `remain_amounts`                            decimal(20,2) DEFAULT NULL COMMENT '当前剩余本金(元)',
  `remain_interest`                           decimal(20,2) DEFAULT NULL COMMENT '当前剩余利息(元)',
  `remain_other_amounts`                      decimal(20,2) DEFAULT NULL COMMENT '当前剩余费用(元)',
  `periods`                                   int(11) DEFAULT NULL COMMENT '总期数',
  `period_amounts`                            decimal(20,2) DEFAULT NULL COMMENT '每期固定费用',
  `bus_product_id`                            varchar(100) DEFAULT NULL COMMENT '业务产品编号',
  `bus_product_name`                          varchar(100) DEFAULT NULL COMMENT '业务产品名称',
  `shoufu_amount`                             decimal(32,2) DEFAULT NULL COMMENT '首付款金额',
  `selling_price`                             decimal(32,2) DEFAULT NULL COMMENT '销售价格',
  `contract_daily_interest_rate`              float DEFAULT NULL COMMENT '合同日利率',
  `repay_plan_cal_rule`                       varchar(32) DEFAULT NULL COMMENT '还款计划计算规则',
  `contract_daily_interest_rate_count`        int(11) DEFAULT NULL COMMENT '日利率计算基础',
  `total_investment_amount`                   decimal(32,2) DEFAULT NULL COMMENT '投资总额(元)',
  `contract_month_interest_rate`              float(20,4) DEFAULT NULL COMMENT '合同月利率',
  `status_change_log`                         text COMMENT '资产状态变更日志',
  `package_filter_id`                         varchar(50) DEFAULT NULL COMMENT '虚拟过滤包id',
  `virtual_asset_bag_id`                      varchar(32) DEFAULT NULL COMMENT '虚拟资产包id',
  `package_remain_principal`                  decimal(20,2) DEFAULT NULL COMMENT '封包时当前剩余本金(元)',
  `package_remain_periods`                    tinyint(4) DEFAULT NULL COMMENT '封包时当前剩余期数',
  `status`                                    int(4) DEFAULT '0' COMMENT '资产状态(-1征信异常 0准入 1在池 2封包 3发行 4历史',
  `wind_control_status`                       varchar(32) DEFAULT NULL COMMENT '当前风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
  `wind_control_status_pool`                  varchar(32) DEFAULT NULL COMMENT '入池时风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
  `cheat_level`                               varchar(10) DEFAULT NULL COMMENT '反欺诈等级:1~5,-1:null,其它:异常值',
  `score_range`                               varchar(10) DEFAULT NULL COMMENT '评分等级:1~20,-1:null,其它:异常值',
  `score_level`                               varchar(10) DEFAULT NULL COMMENT '评分区间:1~5,-1:null,其它:异常值',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建记录时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport',
  `address`                                   varchar(255) DEFAULT NULL COMMENT '居住地址',
  `mortgage_rates`                            float(20,4) DEFAULT NULL COMMENT '抵押率(%)',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `basicasset_index` (`project_id`,`serial_number`,`contract_code`) USING BTREE,
  KEY `basicasset_index2` (`extract_date`) USING BTREE,
  KEY `basicasset_index3` (`asset_bag_id`) USING BTREE,
  KEY `basicasset_index4` (`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1167340 DEFAULT CHARSET=utf8 COMMENT='基础资产表';

-- ----------------------------
-- Table structure for t_basic_asset_20190401
-- ----------------------------
-- DROP TABLE IF EXISTS `t_basic_asset_20190401`;
CREATE TABLE `t_basic_asset_20190401` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入记录编号',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `asset_bag_id`                              varchar(50) DEFAULT NULL COMMENT '资产包编号',
  `asset_type`                                varchar(64) DEFAULT NULL COMMENT '资产类型',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `contract_code`                             varchar(64) DEFAULT NULL COMMENT '贷款合同编号',
  `contract_amount`                           decimal(32,2) DEFAULT NULL COMMENT '贷款合同金额',
  `interest_rate_type`                        varchar(32) DEFAULT NULL COMMENT '利率类型',
  `contract_interest_rate`                    float(20,4) DEFAULT NULL COMMENT '合同利率',
  `base_interest_rate`                        float(20,4) DEFAULT NULL COMMENT '基准利率',
  `fixed_interest_rate`                       float(20,4) DEFAULT NULL COMMENT '固定利率',
  `fixed_interest_diff`                       varchar(45) DEFAULT NULL COMMENT '固定利差',
  `interest_rate_ajustment`                   varchar(64) DEFAULT NULL COMMENT '利率调整方式',
  `loan_issue_date`                           date DEFAULT NULL COMMENT '贷款发放日',
  `loan_expiry_date`                          date DEFAULT NULL COMMENT '贷款到期日',
  `frist_repayment_date`                      date DEFAULT NULL COMMENT '首次还款日',
  `repayment_type`                            varchar(32) DEFAULT NULL COMMENT '还款方式',
  `repayment_frequency`                       varchar(32) DEFAULT NULL COMMENT '还款频率',
  `loan_repay_date`                           int(11) DEFAULT NULL COMMENT '贷款还款日',
  `tail_amount`                               decimal(32,2) DEFAULT NULL COMMENT '尾款金额',
  `tail_amount_rate`                          float DEFAULT NULL COMMENT '尾款金额占比',
  `consume_use`                               varchar(32) DEFAULT NULL COMMENT '消费用途',
  `guarantee_type`                            varchar(32) DEFAULT NULL COMMENT '担保方式',
  `extract_date`                              date DEFAULT NULL COMMENT '提取日期',
  `extract_date_principal_amount`             decimal(32,2) DEFAULT NULL COMMENT '提取日本金金额',
  `loan_cur_interest_rate`                    float DEFAULT NULL COMMENT '贷款现行利率',
  `borrower_type`                             varchar(32) DEFAULT NULL COMMENT '借款人类型',
  `borrower_name`                             varchar(100) DEFAULT NULL COMMENT '借款人姓名',
  `document_type`                             varchar(32) DEFAULT NULL COMMENT '证件类型',
  `document_num`                              varchar(100) DEFAULT NULL COMMENT '证件号码',
  `borrower_rating`                           varchar(32) DEFAULT NULL COMMENT '借款人评级',
  `borrower_industry`                         varchar(32) DEFAULT NULL COMMENT '借款人行业',
  `phone_num`                                 varchar(100) DEFAULT NULL COMMENT '手机号码',
  `sex`                                       varchar(32) DEFAULT NULL COMMENT '性别',
  `birthday`                                  date DEFAULT NULL COMMENT '出生日期',
  `age`                                       int(11) DEFAULT NULL COMMENT '年龄',
  `province`                                  varchar(32) DEFAULT NULL COMMENT '所在省份',
  `city`                                      varchar(32) DEFAULT NULL COMMENT '所在城市',
  `marital_status`                            varchar(32) DEFAULT NULL COMMENT '婚姻状况',
  `country`                                   varchar(32) DEFAULT NULL COMMENT '国籍',
  `annual_income`                             decimal(32,2) DEFAULT NULL COMMENT '年收入',
  `income_debt_rate`                          float(32,4) DEFAULT NULL COMMENT '收入债务比',
  `education_level`                           varchar(32) DEFAULT NULL COMMENT '教育程度',
  `period_exp`                                int(11) DEFAULT NULL COMMENT '提取日剩余还款期数',
  `account_age`                               float(11,2) DEFAULT NULL COMMENT '帐龄',
  `residual_maturity_con`                     float(11,2) DEFAULT NULL COMMENT '合同期限',
  `residual_maturity_ext`                     float(11,2) DEFAULT NULL COMMENT '提取日剩余期限',
  `package_principal_balance`                 decimal(32,2) DEFAULT '0.00' COMMENT '封包本金余额',
  `statistics_date`                           date DEFAULT NULL COMMENT '统计日期',
  `extract_interest_date`                     date DEFAULT NULL COMMENT '提取日计息日',
  `curr_over_days`                            int(11) DEFAULT '0' COMMENT '当前逾期天数',
  `remain_counts`                             int(11) DEFAULT NULL COMMENT '当前剩余期数',
  `remain_amounts`                            decimal(20,2) DEFAULT NULL COMMENT '当前剩余本金(元)',
  `remain_interest`                           decimal(20,2) DEFAULT NULL COMMENT '当前剩余利息(元)',
  `remain_other_amounts`                      decimal(20,2) DEFAULT NULL COMMENT '当前剩余费用(元)',
  `periods`                                   int(11) DEFAULT NULL COMMENT '总期数',
  `period_amounts`                            decimal(20,2) DEFAULT NULL COMMENT '每期固定费用',
  `bus_product_id`                            varchar(100) DEFAULT NULL COMMENT '业务产品编号',
  `bus_product_name`                          varchar(100) DEFAULT NULL COMMENT '业务产品名称',
  `shoufu_amount`                             decimal(32,2) DEFAULT NULL COMMENT '首付款金额',
  `selling_price`                             decimal(32,2) DEFAULT NULL COMMENT '销售价格',
  `contract_daily_interest_rate`              float DEFAULT NULL COMMENT '合同日利率',
  `repay_plan_cal_rule`                       varchar(32) DEFAULT NULL COMMENT '还款计划计算规则',
  `contract_daily_interest_rate_count`        int(11) DEFAULT NULL COMMENT '日利率计算基础',
  `total_investment_amount`                   decimal(32,2) DEFAULT NULL COMMENT '投资总额(元)',
  `contract_month_interest_rate`              float(20,4) DEFAULT NULL COMMENT '合同月利率',
  `status_change_log`                         text COMMENT '资产状态变更日志',
  `package_filter_id`                         varchar(50) DEFAULT NULL COMMENT '虚拟过滤包id',
  `virtual_asset_bag_id`                      varchar(32) DEFAULT NULL COMMENT '虚拟资产包id',
  `package_remain_principal`                  decimal(20,2) DEFAULT NULL COMMENT '封包时当前剩余本金(元)',
  `package_remain_periods`                    tinyint(4) DEFAULT NULL COMMENT '封包时当前剩余期数',
  `status`                                    int(4) DEFAULT '0' COMMENT '资产状态(-1征信异常 0准入 1在池 2封包 3发行 4历史',
  `wind_control_status`                       varchar(32) DEFAULT NULL COMMENT '风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
  `cheat_level`                               tinyint(4) DEFAULT NULL COMMENT '反欺诈等级 1~5',
  `score_range`                               tinyint(4) DEFAULT NULL COMMENT '评分等级 1~20',
  `score_level`                               tinyint(4) DEFAULT NULL COMMENT '评分区间 1~5',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建记录时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport',
  `address`                                   varchar(255) DEFAULT NULL COMMENT '居住地址',
  `mortgage_rates`                            float(20,4) DEFAULT NULL COMMENT '抵押率(%)',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `basicasset_index` (`project_id`,`serial_number`,`contract_code`) USING BTREE,
  KEY `basicasset_index2` (`extract_date`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=928611 DEFAULT CHARSET=utf8 COMMENT='基础资产表';

-- ----------------------------
-- Table structure for t_basic_asset_20190617
-- ----------------------------
-- DROP TABLE IF EXISTS `t_basic_asset_20190617`;
CREATE TABLE `t_basic_asset_20190617` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入记录编号',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `asset_bag_id`                              varchar(50) DEFAULT NULL COMMENT '资产包编号',
  `asset_type`                                varchar(64) DEFAULT NULL COMMENT '资产类型',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `contract_code`                             varchar(64) DEFAULT NULL COMMENT '贷款合同编号',
  `contract_amount`                           decimal(32,2) DEFAULT NULL COMMENT '贷款合同金额',
  `interest_rate_type`                        varchar(32) DEFAULT NULL COMMENT '利率类型',
  `contract_interest_rate`                    float(20,4) DEFAULT NULL COMMENT '合同利率',
  `base_interest_rate`                        float(20,4) DEFAULT NULL COMMENT '基准利率',
  `fixed_interest_rate`                       float(20,4) DEFAULT NULL COMMENT '固定利率',
  `fixed_interest_diff`                       varchar(45) DEFAULT NULL COMMENT '固定利差',
  `interest_rate_ajustment`                   varchar(64) DEFAULT NULL COMMENT '利率调整方式',
  `loan_issue_date`                           date DEFAULT NULL COMMENT '贷款发放日',
  `loan_expiry_date`                          date DEFAULT NULL COMMENT '贷款到期日',
  `frist_repayment_date`                      date DEFAULT NULL COMMENT '首次还款日',
  `repayment_type`                            varchar(32) DEFAULT NULL COMMENT '还款方式',
  `repayment_frequency`                       varchar(32) DEFAULT NULL COMMENT '还款频率',
  `loan_repay_date`                           int(11) DEFAULT NULL COMMENT '贷款还款日',
  `tail_amount`                               decimal(32,2) DEFAULT NULL COMMENT '尾款金额',
  `tail_amount_rate`                          float DEFAULT NULL COMMENT '尾款金额占比',
  `consume_use`                               varchar(32) DEFAULT NULL COMMENT '消费用途',
  `guarantee_type`                            varchar(32) DEFAULT NULL COMMENT '担保方式',
  `extract_date`                              date DEFAULT NULL COMMENT '提取日期',
  `extract_date_principal_amount`             decimal(32,2) DEFAULT NULL COMMENT '提取日本金金额',
  `loan_cur_interest_rate`                    float DEFAULT NULL COMMENT '贷款现行利率',
  `borrower_type`                             varchar(32) DEFAULT NULL COMMENT '借款人类型',
  `borrower_name`                             varchar(300) DEFAULT NULL COMMENT '借款人姓名',
  `document_type`                             varchar(32) DEFAULT NULL COMMENT '证件类型',
  `document_num`                              varchar(100) DEFAULT NULL COMMENT '证件号码',
  `borrower_rating`                           varchar(32) DEFAULT NULL COMMENT '借款人评级',
  `borrower_industry`                         varchar(32) DEFAULT NULL COMMENT '借款人行业',
  `phone_num`                                 varchar(100) DEFAULT NULL COMMENT '手机号码',
  `sex`                                       varchar(32) DEFAULT NULL COMMENT '性别',
  `birthday`                                  date DEFAULT NULL COMMENT '出生日期',
  `age`                                       int(11) DEFAULT NULL COMMENT '年龄',
  `province`                                  varchar(32) DEFAULT NULL COMMENT '所在省份',
  `city`                                      varchar(32) DEFAULT NULL COMMENT '所在城市',
  `marital_status`                            varchar(32) DEFAULT NULL COMMENT '婚姻状况',
  `country`                                   varchar(32) DEFAULT NULL COMMENT '国籍',
  `annual_income`                             decimal(32,2) DEFAULT NULL COMMENT '年收入',
  `income_debt_rate`                          float(32,4) DEFAULT NULL COMMENT '收入债务比',
  `education_level`                           varchar(32) DEFAULT NULL COMMENT '教育程度',
  `period_exp`                                int(11) DEFAULT NULL COMMENT '提取日剩余还款期数',
  `account_age`                               float(11,2) DEFAULT NULL COMMENT '帐龄',
  `residual_maturity_con`                     float(11,2) DEFAULT NULL COMMENT '合同期限',
  `residual_maturity_ext`                     float(11,2) DEFAULT NULL COMMENT '提取日剩余期限',
  `package_principal_balance`                 decimal(32,2) DEFAULT '0.00' COMMENT '封包本金余额',
  `statistics_date`                           date DEFAULT NULL COMMENT '统计日期',
  `extract_interest_date`                     date DEFAULT NULL COMMENT '提取日计息日',
  `curr_over_days`                            int(11) DEFAULT '0' COMMENT '当前逾期天数',
  `remain_counts`                             int(11) DEFAULT NULL COMMENT '当前剩余期数',
  `remain_amounts`                            decimal(20,2) DEFAULT NULL COMMENT '当前剩余本金(元)',
  `remain_interest`                           decimal(20,2) DEFAULT NULL COMMENT '当前剩余利息(元)',
  `remain_other_amounts`                      decimal(20,2) DEFAULT NULL COMMENT '当前剩余费用(元)',
  `periods`                                   int(11) DEFAULT NULL COMMENT '总期数',
  `period_amounts`                            decimal(20,2) DEFAULT NULL COMMENT '每期固定费用',
  `bus_product_id`                            varchar(100) DEFAULT NULL COMMENT '业务产品编号',
  `bus_product_name`                          varchar(100) DEFAULT NULL COMMENT '业务产品名称',
  `shoufu_amount`                             decimal(32,2) DEFAULT NULL COMMENT '首付款金额',
  `selling_price`                             decimal(32,2) DEFAULT NULL COMMENT '销售价格',
  `contract_daily_interest_rate`              float DEFAULT NULL COMMENT '合同日利率',
  `repay_plan_cal_rule`                       varchar(32) DEFAULT NULL COMMENT '还款计划计算规则',
  `contract_daily_interest_rate_count`        int(11) DEFAULT NULL COMMENT '日利率计算基础',
  `total_investment_amount`                   decimal(32,2) DEFAULT NULL COMMENT '投资总额(元)',
  `contract_month_interest_rate`              float(20,4) DEFAULT NULL COMMENT '合同月利率',
  `status_change_log`                         text COMMENT '资产状态变更日志',
  `package_filter_id`                         varchar(50) DEFAULT NULL COMMENT '虚拟过滤包id',
  `virtual_asset_bag_id`                      varchar(32) DEFAULT NULL COMMENT '虚拟资产包id',
  `package_remain_principal`                  decimal(20,2) DEFAULT NULL COMMENT '封包时当前剩余本金(元)',
  `package_remain_periods`                    tinyint(4) DEFAULT NULL COMMENT '封包时当前剩余期数',
  `status`                                    int(4) DEFAULT '0' COMMENT '资产状态(-1征信异常 0准入 1在池 2封包 3发行 4历史',
  `wind_control_status`                       varchar(32) DEFAULT NULL COMMENT '当前风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
  `wind_control_status_pool`                  varchar(32) DEFAULT NULL COMMENT '入池时风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
  `cheat_level`                               varchar(10) DEFAULT NULL COMMENT '反欺诈等级:1~5,-1:null,其它:异常值',
  `score_range`                               varchar(10) DEFAULT NULL COMMENT '评分等级:1~20,-1:null,其它:异常值',
  `score_level`                               varchar(10) DEFAULT NULL COMMENT '评分区间:1~5,-1:null,其它:异常值',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建记录时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport',
  `address`                                   varchar(255) DEFAULT NULL COMMENT '居住地址',
  `mortgage_rates`                            float(20,4) DEFAULT NULL COMMENT '抵押率(%)',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `basicasset_index` (`project_id`,`serial_number`,`contract_code`) USING BTREE,
  KEY `basicasset_index2` (`extract_date`) USING BTREE,
  KEY `basicasset_index3` (`asset_bag_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1160750 DEFAULT CHARSET=utf8 COMMENT='基础资产表';

-- ----------------------------
-- Table structure for t_basic_asset_temporary
-- ----------------------------
-- DROP TABLE IF EXISTS `t_basic_asset_temporary`;
CREATE TABLE `t_basic_asset_temporary` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入记录编号',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `asset_type`                                varchar(64) DEFAULT NULL COMMENT '资产类型',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `contract_code`                             varchar(64) DEFAULT NULL COMMENT '贷款合同编号',
  `consume_use`                               varchar(32) DEFAULT NULL COMMENT '消费用途',
  `guarantee_type`                            varchar(32) DEFAULT NULL COMMENT '担保方式',
  `borrower_type`                             varchar(32) DEFAULT NULL COMMENT '借款人类型',
  `borrower_name`                             varchar(300) DEFAULT NULL COMMENT '借款人姓名',
  `phone_num`                                 varchar(100) DEFAULT NULL COMMENT '手机号码',
  `sex`                                       varchar(32) DEFAULT NULL COMMENT '性别',
  `birthday`                                  date DEFAULT NULL COMMENT '出生日期',
  `age`                                       int(11) DEFAULT NULL COMMENT '年龄',
  `education_level`                           varchar(32) DEFAULT NULL COMMENT '教育程度',
  `province`                                  varchar(32) DEFAULT NULL COMMENT '所在省份',
  `city`                                      varchar(32) DEFAULT NULL COMMENT '所在城市',
  `address`                                   varchar(255) DEFAULT NULL COMMENT '居住地址',
  `annual_income`                             decimal(32,2) DEFAULT NULL COMMENT '年收入',
  `borrower_industry`                         varchar(32) DEFAULT NULL COMMENT '借款人行业',
  `marital_status`                            varchar(32) DEFAULT NULL COMMENT '婚姻状况',
  `car_type`                                  varchar(32) DEFAULT NULL COMMENT '车类型 预定义字段：新车 二手车',
  `frame_num`                                 varchar(100) DEFAULT NULL COMMENT '车架号',
  `license_num`                               varchar(100) DEFAULT NULL COMMENT '车牌号码',
  `car_brand`                                 varchar(100) DEFAULT NULL COMMENT '车辆品牌',
  `car_model`                                 varchar(100) DEFAULT NULL COMMENT '车型',
  `car_colour`                                varchar(100) DEFAULT NULL COMMENT '车辆颜色',
  `register_date`                             date DEFAULT NULL COMMENT '注册日期',
  `pawn_value`                                decimal(20,6) DEFAULT NULL COMMENT '评估价格(元)',
  `gps_code`                                  varchar(100) DEFAULT NULL COMMENT 'GPS编号',
  `mortgage_rates`                            float(20,4) DEFAULT NULL COMMENT '抵押率(%)',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建记录时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `basicasset_temporary_index` (`import_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8 COMMENT='基础资产临时表';

-- ----------------------------
-- Table structure for t_borrowerinfo
-- ----------------------------
-- DROP TABLE IF EXISTS `t_borrowerinfo`;
CREATE TABLE `t_borrowerinfo` (
  `id`                                        int(32) NOT NULL AUTO_INCREMENT COMMENT '主借款人信息主键',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `agency_id`                                 varchar(32) DEFAULT NULL COMMENT '机构编号',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `borrower_name`                             varchar(100) DEFAULT NULL COMMENT '客户姓名',
  `certificate_type`                          varchar(32) DEFAULT NULL COMMENT '证件类型 预定义字段：身份证 护照 户口本 外国人护照',
  `document_num`                              varchar(100) DEFAULT NULL COMMENT '身份证号',
  `phone_num`                                 varchar(32) DEFAULT NULL COMMENT '手机号',
  `age`                                       int(11) DEFAULT NULL COMMENT '年龄',
  `sex`                                       varchar(16) DEFAULT NULL COMMENT '性别-枚举 男，女',
  `marital_status`                            varchar(32) DEFAULT NULL COMMENT '婚姻状况-枚举 预定义字段：已婚，未婚，离异，丧偶',
  `children_number`                           int(4) DEFAULT NULL COMMENT '子女数量',
  `work_years`                                varchar(5) DEFAULT NULL COMMENT '工作年限',
  `customer_type`                             varchar(32) DEFAULT NULL COMMENT '客户类型-枚举 "01-农户 02-工薪 03-个体工商户 04-学生 99-其他"',
  `education_level`                           varchar(32) DEFAULT NULL COMMENT '学历-枚举 预定义字段： 小学， 初中， 高中/职高/技校， 大专， 本科, 硕士, 博士， 文盲和半文盲',
  `degree`                                    varchar(32) DEFAULT NULL COMMENT '学位',
  `is_capacity_civil_conduct`                 varchar(16) DEFAULT NULL COMMENT '是否具有完全民事行为能力"预定义字段：是 否"',
  `living_state`                              varchar(32) DEFAULT NULL COMMENT '居住状态',
  `province`                                  varchar(32) DEFAULT NULL COMMENT '客户所在省 传输省份城市代码则提供对应关系',
  `city`                                      varchar(32) DEFAULT NULL COMMENT '客户所在市 传输省份城市代码则提供对应关系',
  `address`                                   varchar(128) DEFAULT NULL COMMENT '客户居住地址',
  `house_province`                            varchar(32) DEFAULT NULL COMMENT '客户户籍所在省',
  `house_city`                                varchar(32) DEFAULT NULL COMMENT '客户户籍所在市',
  `house_address`                             varchar(128) DEFAULT NULL COMMENT '客户户籍地址',
  `communications_zip_code`                   varchar(128) DEFAULT NULL COMMENT '通讯邮编',
  `mailing_address`                           varchar(128) DEFAULT NULL COMMENT '客户通讯地址',
  `career`                                    varchar(32) DEFAULT NULL COMMENT '客户职业',
  `working_state`                             varchar(32) DEFAULT NULL COMMENT '工作状态 "预定义字段：在职，失业"',
  `position`                                  varchar(32) DEFAULT NULL COMMENT '职务',
  `title`                                     varchar(32) DEFAULT NULL COMMENT '职称',
  `borrower_industry`                         varchar(255) DEFAULT NULL COMMENT '借款人行业-枚举\r\n借款人行业\r\nNIL--空\r\nA--农、林、牧、渔业\r\nB--采矿业\r\nC--制造业\r\nD--电力、热力、燃气及水生产和供应业\r\nE--建筑业\r\nF--批发和零售业\r\nG--交通运输、仓储和邮政业\r\nH--住宿和餐饮业\r\nI--信息传输、软件和信息技术服务业\r\nJ--金融业\r\nK--房地产业\r\nL--租赁和商务服务业\r\nM--科学研究和技术服务业\r\nN--水利、环境和公共设施管理业\r\nO--居民服务、修理和其他服务业\r\nP--教育\r\nQ--卫生和社会工作\r\nR--文化、体育和娱乐业\r\nS--公共管理、社会保障和社会组织\r\nT--国际组织\r\nZ--其他',
  `is_car`                                    varchar(16) DEFAULT NULL COMMENT '是否有车"预定义字段:是 否"',
  `is_mortgage_financing`                     varchar(16) DEFAULT NULL COMMENT '是否有按揭车贷"预定义字段:是 否"',
  `is_house`                                  varchar(16) DEFAULT NULL COMMENT '是否有房"预定义字段:是 否"',
  `is_mortgage_loans`                         varchar(16) DEFAULT NULL COMMENT '是否有按揭房贷"预定义字段:是 否"',
  `is_credit_card`                            varchar(16) DEFAULT NULL COMMENT '是否有信用卡"预定义字段:是 否"',
  `credit_limit`                              decimal(20,6) DEFAULT NULL COMMENT '信用卡额度',
  `annual_income`                             decimal(20,6) DEFAULT NULL COMMENT '年收入(元)',
  `internal_credit_rating`                    varchar(32) DEFAULT NULL COMMENT '内部信用等级',
  `blacklist_level`                           varchar(32) DEFAULT NULL COMMENT '黑名单等级',
  `unit_name`                                 varchar(100) DEFAULT NULL COMMENT '单位名称',
  `fixed_telephone`                           varchar(18) DEFAULT NULL COMMENT '固定电话',
  `zip_code`                                  varchar(18) DEFAULT NULL COMMENT '邮编',
  `unit_address`                              varchar(100) DEFAULT NULL COMMENT '单位详细地址',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入Id',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `borrowerinfo_index` (`project_id`,`agency_id`,`serial_number`) USING BTREE,
  KEY `index2` (`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=99998553 DEFAULT CHARSET=utf8 COMMENT='主借款人信息表-文件二';

-- ----------------------------
-- Table structure for t_duration_risk_control_result
-- ----------------------------
-- DROP TABLE IF EXISTS `t_duration_risk_control_result`;
CREATE TABLE `t_duration_risk_control_result` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `project_id`                                varchar(50) NOT NULL,
  `monitor_date`                              date NOT NULL COMMENT '监控日期',
  `serial_number`                             varchar(50) NOT NULL COMMENT '借据号',
  `asset_level`                               int(4) NOT NULL COMMENT '资产等级',
  `credit_level`                              int(4) NOT NULL COMMENT '信用等级',
  `antifraud_level`                           int(4) NOT NULL COMMENT '反欺诈等级',
  `is_black_list`                             tinyint(2) NOT NULL COMMENT '是否命中黑名单',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=9555 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT;

-- ----------------------------
-- Table structure for t_enterpriseinfo
-- ----------------------------
-- DROP TABLE IF EXISTS `t_enterpriseinfo`;
CREATE TABLE `t_enterpriseinfo` (
  `id`                                        int(32) NOT NULL AUTO_INCREMENT COMMENT '企业名称信息主键',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `agency_id`                                 varchar(32) DEFAULT NULL COMMENT '机构编号',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `contract_role`                             varchar(32) DEFAULT NULL COMMENT '合同角色 预定义字段: 主借款企业 共同借款企业 担保企业 无',
  `enterprise_name`                           varchar(100) DEFAULT NULL COMMENT '企业姓名',
  `registration_number`                       varchar(32) DEFAULT NULL COMMENT '工商注册号',
  `organization_code`                         varchar(32) DEFAULT NULL COMMENT '组织机构代码',
  `taxpayer_identification_number`            varchar(18) DEFAULT NULL COMMENT '纳税人识别号',
  `uniform_credit_code`                       varchar(18) DEFAULT NULL COMMENT '统一信用代码',
  `registered_address`                        varchar(130) DEFAULT NULL COMMENT '注册地址',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入Id',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `enterpriseinfo_index` (`project_id`,`agency_id`,`serial_number`,`registration_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8 COMMENT='企业名称信息表-文件十二';

-- ----------------------------
-- Table structure for t_excel_import_extract_date
-- ----------------------------
-- DROP TABLE IF EXISTS `t_excel_import_extract_date`;
CREATE TABLE `t_excel_import_extract_date` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `project_id`                                varchar(50) NOT NULL COMMENT '项目编号',
  `import_date`                               date NOT NULL COMMENT '导入时间',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_index1` (`project_id`,`import_date`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=21445 DEFAULT CHARSET=utf8 COMMENT='excel导入的数据提取日';

-- ----------------------------
-- Table structure for t_file_status_log
-- ----------------------------
-- DROP TABLE IF EXISTS `t_file_status_log`;
CREATE TABLE `t_file_status_log` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `asset_bag_id`                              varchar(50) DEFAULT NULL COMMENT '资产包编号',
  `file_type`                                 tinyint(2) DEFAULT NULL COMMENT '文件类型 1 资产文件、2 产品设计、3 存续期',
  `file_path`                                 varchar(100) DEFAULT NULL COMMENT '文件保存位置',
  `file_name`                                 varchar(80) DEFAULT NULL COMMENT '文件名称',
  `file_name_en`                              varchar(80) DEFAULT NULL COMMENT '文件英文名',
  `cur_step`                                  tinyint(2) DEFAULT NULL COMMENT '当前步骤 1-上传中 2-上传失败 3-上传成功 4-处理中 5-处理失败 6-处理成功',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_file_status_log_projectid` (`project_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=25 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='文件状态记录日志';

-- ----------------------------
-- Table structure for t_fund_account_performance
-- ----------------------------
-- DROP TABLE IF EXISTS `t_fund_account_performance`;
CREATE TABLE `t_fund_account_performance` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `project_id`                                varchar(50) DEFAULT NULL COMMENT '项目编号',
  `occur_amount`                              decimal(20,2) DEFAULT NULL COMMENT '发生金额',
  `occur_direction`                           varchar(10) DEFAULT NULL COMMENT '发生方向（in 流入 , out 流出）',
  `occur_type`                                varchar(50) DEFAULT NULL COMMENT '发生类型',
  `occur_date`                                date DEFAULT NULL COMMENT '发生日期',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=875 DEFAULT CHARSET=utf8 COMMENT='资金端账户表现';

-- ----------------------------
-- Table structure for t_generate_number
-- ----------------------------
-- DROP TABLE IF EXISTS `t_generate_number`;
CREATE TABLE `t_generate_number` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `project_id`                                varchar(50) NOT NULL COMMENT '项目编号',
  `number_type`                               int(2) NOT NULL COMMENT '编号类型(1资产包编号)',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=459 DEFAULT CHARSET=utf8 COMMENT='生成编号表';

-- ----------------------------
-- Table structure for t_guarantycarinfo
-- ----------------------------
-- DROP TABLE IF EXISTS `t_guarantycarinfo`;
CREATE TABLE `t_guarantycarinfo` (
  `id`                                        int(32) NOT NULL AUTO_INCREMENT COMMENT '抵押物(车)信息主键',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `agency_id`                                 varchar(32) DEFAULT NULL COMMENT '机构编号',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `guaranty_code`                             varchar(100) DEFAULT NULL COMMENT '抵押物编号',
  `guaranty_handling_status`                  varchar(32) DEFAULT NULL COMMENT '抵押办理状态 预定义字段：办理中 办理完成 尚未办理',
  `guaranty_alignment`                        varchar(32) DEFAULT NULL COMMENT '抵押顺位 1-第一顺位 2-第二顺位 9-其他',
  `car_property`                              varchar(32) DEFAULT NULL COMMENT '车辆性质 预定义字段：非融资车分期非 融资车抵贷 融资租赁车分期 融资租赁车抵贷',
  `financing_type`                            varchar(32) DEFAULT NULL COMMENT '融资方式 预定义字段：正租 反租',
  `guarantee_type`                            varchar(32) DEFAULT NULL COMMENT '担保方式 预定义字段：质押担保，信用担保，保证担保，抵押担保',
  `pawn_value`                                decimal(20,6) DEFAULT NULL COMMENT '评估价格(元)',
  `car_sales_price`                           decimal(20,6) DEFAULT NULL COMMENT '车辆销售价格(元)',
  `car_new_price`                             decimal(20,6) DEFAULT NULL COMMENT '新车指导价(元)',
  `total_investment`                          decimal(20,6) DEFAULT NULL COMMENT '投资总额(元)',
  `purchase_tax_amouts`                       decimal(20,6) DEFAULT NULL COMMENT '购置税金额(元)',
  `insurance_type`                            varchar(100) DEFAULT NULL COMMENT '保险种类  交强险 第三者责任险 盗抢险 车损险 不计免赔 其他',
  `car_insurance_premium`                     decimal(20,6) DEFAULT NULL COMMENT '汽车保险总费用',
  `total_poundage`                            decimal(20,6) DEFAULT NULL COMMENT '手续总费用(元)',
  `cumulative_car_transfer_number`            int(11) DEFAULT NULL COMMENT '累计车辆过户次数',
  `one_year_car_transfer_number`              int(11) DEFAULT NULL COMMENT '一年内车辆过户次数',
  `liability_insurance_cost1`                 decimal(20,6) DEFAULT NULL COMMENT '责信保费用1',
  `liability_insurance_cost2`                 decimal(20,6) DEFAULT NULL COMMENT '责信保费用2',
  `car_type`                                  varchar(32) DEFAULT NULL COMMENT '车类型 预定义字段：新车 二手车',
  `frame_num`                                 varchar(100) DEFAULT NULL COMMENT '车架号',
  `engine_num`                                varchar(100) DEFAULT NULL COMMENT '发动机号',
  `gps_code`                                  varchar(100) DEFAULT NULL COMMENT 'GPS编号',
  `gps_cost`                                  decimal(20,6) DEFAULT NULL COMMENT 'GPS费用',
  `license_num`                               varchar(100) DEFAULT NULL COMMENT '车牌号码',
  `car_brand`                                 varchar(100) DEFAULT NULL COMMENT '车辆品牌',
  `car_system`                                varchar(100) DEFAULT NULL COMMENT '车系',
  `car_model`                                 varchar(100) DEFAULT NULL COMMENT '车型',
  `car_age`                                   decimal(20,6) DEFAULT NULL COMMENT '车龄',
  `car_energy_type`                           varchar(32) DEFAULT NULL COMMENT '车辆能源类型 预定义字段： 混合动力 纯电 非新能源车',
  `production_date`                           varchar(32) DEFAULT NULL COMMENT '生产日期',
  `mileage`                                   decimal(20,6) DEFAULT NULL COMMENT '里程数',
  `register_date`                             date DEFAULT NULL COMMENT '注册日期',
  `buy_car_address`                           varchar(255) DEFAULT NULL COMMENT '车辆购买地',
  `car_colour`                                varchar(100) DEFAULT NULL COMMENT '车辆颜色',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入Id',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `guarantycarinfo_index` (`project_id`,`agency_id`,`serial_number`,`guaranty_code`) USING BTREE,
  KEY `guarantycarinfo_index1` (`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=100386270 DEFAULT CHARSET=utf8 COMMENT='抵押物(车)信息表-文件四';

-- ----------------------------
-- Table structure for t_guarantyhouseinfo
-- ----------------------------
-- DROP TABLE IF EXISTS `t_guarantyhouseinfo`;
CREATE TABLE `t_guarantyhouseinfo` (
  `id`                                        int(32) NOT NULL AUTO_INCREMENT COMMENT '房抵押物信息主键',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `agency_id`                                 varchar(32) DEFAULT NULL COMMENT '机构编号',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `guaranty_number`                           varchar(100) DEFAULT NULL COMMENT '抵押物编号',
  `guaranty_name`                             varchar(100) DEFAULT NULL COMMENT '抵押物名称',
  `guaranty_describe`                         varchar(100) DEFAULT NULL COMMENT '抵押物描述',
  `guaranty_handle_status`                    varchar(32) DEFAULT NULL COMMENT '抵押办理状态 预定义字段 办理中 办理完成 尚未办理',
  `guaranty_alignment`                        varchar(32) DEFAULT NULL COMMENT '抵押顺位 第一顺位 第二顺位 其他',
  `guaranty_front_hand_balance`               decimal(20,6) DEFAULT NULL COMMENT '前手抵押余额',
  `guaranty_type`                             varchar(32) DEFAULT NULL COMMENT '抵押类型',
  `ownership_name`                            varchar(32) DEFAULT NULL COMMENT '所有权人姓名',
  `ownership_document_type`                   varchar(32) DEFAULT NULL COMMENT '所有权人证件类型 预定义字段：身份证 护照 户口本 外国人护照',
  `ownership_document_number`                 varchar(100) DEFAULT NULL COMMENT '所有权人证件号码',
  `ownership_job`                             varchar(16) DEFAULT NULL COMMENT '所有人职业 0 1 3 4 5 6 X Y Z',
  `is_guaranty_ownership_only_domicile`       varchar(16) DEFAULT NULL COMMENT '押品是否为所有权人/借款人名下唯一住所 1 2 3 9',
  `house_area`                                decimal(20,6) DEFAULT NULL COMMENT '房屋建筑面积 平米',
  `house_age`                                 decimal(20,6) DEFAULT NULL COMMENT '楼龄',
  `house_location_province`                   varchar(50) DEFAULT NULL COMMENT '房屋所在省',
  `house_location_city`                       varchar(50) DEFAULT NULL COMMENT '房屋所在城市',
  `house_location_district_county`            varchar(50) DEFAULT NULL COMMENT '房屋所在区县',
  `house_address`                             varchar(256) DEFAULT NULL COMMENT '房屋地址',
  `property_years`                            decimal(20,6) DEFAULT NULL COMMENT '产权年限',
  `purchase_contract_number`                  varchar(100) DEFAULT NULL COMMENT '购房合同编号',
  `warrant_type`                              varchar(32) DEFAULT NULL COMMENT '权证类型 房产证 房屋他项权证',
  `property_certificate_number`               varchar(100) DEFAULT NULL COMMENT '房产证编号',
  `house_warrant_number`                      varchar(100) DEFAULT NULL COMMENT '房屋他项权证编号',
  `house_type`                                varchar(32) DEFAULT NULL COMMENT '房屋类别 01 02 03 04 05 06 07 08 09 00',
  `is_property_right_co_owner`                varchar(32) DEFAULT NULL COMMENT '是否有产权共有人',
  `property_co_owner_informed_situation`      varchar(255) DEFAULT NULL COMMENT '产权共有人知情情况 1 2 3 9',
  `guaranty_registration`                     varchar(32) DEFAULT NULL COMMENT '抵押登记办理 已完成 已递交申请并取得回执 尚未办理',
  `enforcement_notarization`                  varchar(32) DEFAULT NULL COMMENT '强制执行公证 已完成 已递交申请并取得回执 尚未办理',
  `is_arbitration_prove`                      varchar(255) DEFAULT NULL COMMENT '网络仲裁办仲裁证明',
  `assessment_price_evaluation_company`       decimal(20,6) DEFAULT NULL COMMENT '评估价格-评估公司(元)',
  `assessment_price_letting_agent`            decimal(20,6) DEFAULT NULL COMMENT '评估价格-房屋中介(元)',
  `assessment_price_original_rights_day`      decimal(20,6) DEFAULT NULL COMMENT '评估价格-原始权益日内部评估(元)',
  `house_selling_price`                       decimal(20,6) DEFAULT NULL COMMENT '房屋销售价格(元)',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入Id',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `guarantyhouseinfo_index` (`project_id`,`agency_id`,`serial_number`,`guaranty_number`) USING BTREE,
  KEY `guarantyhouseinfo_index1` (`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=69 DEFAULT CHARSET=utf8 COMMENT='房抵押物信息表-文件十三';

-- ----------------------------
-- Table structure for t_import_check_detail
-- ----------------------------
-- DROP TABLE IF EXISTS `t_import_check_detail`;
CREATE TABLE `t_import_check_detail` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入结果汇总Id',
  `field_name`                                varchar(50) DEFAULT NULL COMMENT '字段名',
  `field_cn_name`                             varchar(255) DEFAULT NULL COMMENT '字段中文名',
  `field_value`                               varchar(100) DEFAULT NULL COMMENT '字段值',
  `prompt_type`                               tinyint(2) DEFAULT NULL COMMENT '提示类型 1:提醒,2:错误',
  `prompt_desc`                               varchar(255) DEFAULT NULL COMMENT '提醒描述',
  `error_location`                            int(11) DEFAULT NULL COMMENT '错误位置',
  `prompt_desc_type`                          varchar(100) DEFAULT NULL COMMENT '提醒描述类型',
  `sheet`                                     int(4) DEFAULT NULL COMMENT '校验页码',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_import_check_detail_projectid_summaryid` (`project_id`,`import_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=515156 DEFAULT CHARSET=utf8 COMMENT='数据导入校验详情表';

-- ----------------------------
-- Table structure for t_import_check_summary
-- ----------------------------
-- DROP TABLE IF EXISTS `t_import_check_summary`;
CREATE TABLE `t_import_check_summary` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入Id',
  `field_name`                                varchar(50) DEFAULT NULL COMMENT '字段名',
  `field_cn_name`                             varchar(255) DEFAULT NULL COMMENT '字段中文名',
  `prompt_type`                               tinyint(2) DEFAULT NULL COMMENT '提示类型 1:提醒,2:错误',
  `prompt_desc`                               varchar(255) DEFAULT NULL COMMENT '提醒描述',
  `error_location`                            varchar(255) DEFAULT NULL COMMENT '错误位置',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_import_check_summary_projectid_importid` (`project_id`,`import_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据导入校验汇总表';

-- ----------------------------
-- Table structure for t_import_record
-- ----------------------------
-- DROP TABLE IF EXISTS `t_import_record`;
CREATE TABLE `t_import_record` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `file_original_name`                        varchar(255) DEFAULT NULL COMMENT '原始文件名称',
  `file_name`                                 varchar(255) DEFAULT NULL COMMENT '文件名称 时间戳加编号',
  `file_path`                                 varchar(255) DEFAULT NULL COMMENT '文件路径',
  `upload_date`                               timestamp NULL DEFAULT NULL COMMENT '上传日期',
  `import_num`                                int(11) DEFAULT NULL COMMENT '导入条数',
  `pass_num`                                  int(11) DEFAULT NULL COMMENT '校验通过条数',
  `remaining_number`                          int(11) DEFAULT NULL COMMENT '库中剩余条数',
  `import_status`                             tinyint(2) DEFAULT NULL COMMENT '导入状态 1:上传成功,2:导入中,3:已完成,4:失败-模板失败,5:失败-其它,6:失败-文件异常,7:风控审核中,8:失败-数据为空,10:待确认',
  `error_desc`                                varchar(500) DEFAULT NULL COMMENT '错误描述',
  `operate_user`                              varchar(255) NOT NULL COMMENT '操作人',
  `whether_pass_wind`                         tinyint(2) DEFAULT NULL COMMENT '是否大数据风控评估 1:是,2:否',
  `last_interest_cal_rule`                    tinyint(2) DEFAULT NULL COMMENT '等额本息最后一期利息计算方法 1:剩余本金*期间利率,2:每期支付金额-应还本金',
  `last_repay_date_cal_rule`                  tinyint(2) DEFAULT NULL COMMENT '最后一期还款日期计算规则 1:按还款日,2:按合同到期日',
  `import_type`                               tinyint(4) DEFAULT NULL COMMENT '导入类型 1:资产导入,2:资产状态更新',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_import_record_projectid` (`project_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1710 DEFAULT CHARSET=utf8 COMMENT='数据导入记录表';

-- ----------------------------
-- Table structure for t_import_wind_control_detail
-- ----------------------------
-- DROP TABLE IF EXISTS `t_import_wind_control_detail`;
CREATE TABLE `t_import_wind_control_detail` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入Id',
  `status`                                    tinyint(2) DEFAULT NULL COMMENT '状态 1:评估中,2:已完成',
  `success_num`                               int(11) DEFAULT NULL COMMENT '成功条数',
  `fail_num`                                  int(11) DEFAULT NULL COMMENT '失败条数',
  `pass_num`                                  int(11) DEFAULT NULL COMMENT 'yes条数',
  `un_pass_num`                               int(11) DEFAULT NULL COMMENT 'no条数',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_import_wind_control_projectid_importid` (`project_id`,`import_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=245 DEFAULT CHARSET=utf8 COMMENT='数据导入风控评估详情表';

-- ----------------------------
-- Table structure for t_loancontractinfo
-- ----------------------------
-- DROP TABLE IF EXISTS `t_loancontractinfo`;
CREATE TABLE `t_loancontractinfo` (
  `id`                                        int(32)   NOT NULL AUTO_INCREMENT COMMENT '借款合同信息主键',
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `agency_id`                                 varchar(32)        DEFAULT NULL   COMMENT '机构编号',
  `serial_number`                             varchar(100)       DEFAULT NULL   COMMENT '借据号',
  `asset_type`                                varchar(32)        DEFAULT NULL   COMMENT '资产类型-枚举预定义字段：个人消费贷款 汽车抵押贷款 住房抵押贷款',
  `contract_code`                             varchar(100)       DEFAULT NULL   COMMENT '贷款合同编号',
  `product_id`                                varchar(100)       DEFAULT NULL   COMMENT '产品编号',
  `product_scheme_name`                       varchar(100)       DEFAULT NULL   COMMENT '产品方案名称',
  `contract_amount`                           decimal(20,6)      DEFAULT NULL   COMMENT '贷款总金额(元)',
  `interest_rate_type`                        varchar(32)        DEFAULT NULL   COMMENT '利率类型-枚举 固定利率 浮动利率',
  `contract_interest_rate`                    decimal(20,6)      DEFAULT NULL   COMMENT '贷款年利率(%)',
  `contract_daily_rate`                       decimal(20,6)      DEFAULT NULL   COMMENT '贷款日利率(%)',
  `contract_an_rate`                          decimal(20,6)      DEFAULT NULL   COMMENT '贷款月利率(%)',
  `daily_calculation_basis`                   varchar(32)        DEFAULT NULL   COMMENT '日利率计算基础-枚举预定义字段： 360， 365',
  `repayment_type`                            varchar(32)        DEFAULT NULL   COMMENT '还款方式-枚举预定义字段： 等额本息， 等额本金， 等本等息， 先息后本， 一次性还本付息 气球贷 自定义还款计划',
  `repayment_frequency`                       varchar(32)        DEFAULT NULL   COMMENT '还款频率-枚举预定义字段：月，季，半年，年，到期一次付清 自定义还款频率',
  `periods`                                   int(4)             DEFAULT NULL   COMMENT '总期数',
  `profit_tol_rate`                           decimal(20,6)      DEFAULT NULL   COMMENT '总收益率(%)',
  `rest_principal`                            decimal(20,6)      DEFAULT NULL   COMMENT '剩余本金(元)',
  `rest_other_cost`                           decimal(20,6)      DEFAULT NULL   COMMENT '剩余费用其他(元)',
  `rest_interest`                             decimal(20,6)      DEFAULT NULL   COMMENT '剩余利息(元)',
  `shoufu_amount`                             decimal(20,6)      DEFAULT NULL   COMMENT '首付款金额(元)',
  `shoufu_proportion`                         decimal(20,6)      DEFAULT NULL   COMMENT '首付比例(%)',
  `tail_amount`                               decimal(20,6)      DEFAULT NULL   COMMENT '尾付款金额(元)',
  `tail_amount_rate`                          decimal(20,6)      DEFAULT NULL   COMMENT '尾付比例(%)',
  `poundage`                                  decimal(20,6)      DEFAULT NULL   COMMENT '手续费',
  `poundage_rate`                             decimal(20,6)      DEFAULT NULL   COMMENT '手续费利率',
  `poundage_deduction_type`                   varchar(32)        DEFAULT NULL   COMMENT '手续费扣款方式-枚举 预定义字段： 一次性 分期扣款',
  `settlement_poundage_rate`                  decimal(20,6)      DEFAULT NULL   COMMENT '结算手续费率',
  `subsidie_poundage`                         decimal(20,6)      DEFAULT NULL   COMMENT '补贴手续费',
  `discount_amount`                           decimal(20,6)      DEFAULT NULL   COMMENT '贴息金额',
  `margin_rate`                               decimal(20,6)      DEFAULT NULL   COMMENT '保证金比例',
  `margin`                                    decimal(20,6)      DEFAULT NULL   COMMENT '保证金',
  `margin_offset_type`                        varchar(32)        DEFAULT NULL   COMMENT '保证金冲抵方式-枚举预定义字段：不冲抵 冲抵',
  `account_management_expense`                decimal(20,6)      DEFAULT NULL   COMMENT '帐户管理费',
  `mortgage_rate`                             decimal(20,6)      DEFAULT NULL   COMMENT '抵押率(%)',
  `loan_issue_date`                           date               DEFAULT NULL   COMMENT '合同开始时间',
  `loan_expiry_date`                          date               DEFAULT NULL   COMMENT '合同结束时间',
  `actual_loan_date`                          date               DEFAULT NULL   COMMENT '实际放款时间',
  `frist_repayment_date`                      date               DEFAULT NULL   COMMENT '首次还款时间',
  `loan_repay_date`                           varchar(32)        DEFAULT NULL   COMMENT '贷款还款日',
  `last_estimated_deduction_date`             date               DEFAULT NULL   COMMENT '最后一次预计扣款时间',
  `month_repay_amount`                        decimal(20,6)      DEFAULT NULL   COMMENT '月还款额',
  `loan_use`                                  varchar(32)        DEFAULT NULL   COMMENT '贷款用途-车抵押,旅游，教育，医美，经营类 其他 农业，个人综合消费',
  `loan_application`                          varchar(100)       DEFAULT NULL   COMMENT '申请用途',
  `guarantee_type`                            varchar(32)        DEFAULT NULL   COMMENT '担保方式-枚举\r\n预定义字段：\r\n质押担保，\r\n信用担保，\r\n保证担保，\r\n抵押担保',
  `contract_term`                             decimal(20,6)      DEFAULT NULL   COMMENT '合同期限(月)',
  `total_overdue_daynum`                      int(11)            DEFAULT NULL   COMMENT '累计逾期期数',
  `history_most_overdue_daynum`               int(11)            DEFAULT NULL   COMMENT '历史最高逾期天数',
  `history_total_overdue_daynum`              int(11)            DEFAULT NULL   COMMENT '历史累计逾期天数',
  `current_overdue_daynum`                    int(11)            DEFAULT NULL   COMMENT '当前逾期天数',
  `contract_status`                           varchar(32)        DEFAULT NULL   COMMENT '合同状态-枚举\r\n预定义字段：\r\n生效\r\n不生效',
  `apply_status_code`                         varchar(32)        DEFAULT NULL   COMMENT '申请状态代码-枚举\r\n"预定义字段：\r\n已放款\r\n放款失败"',
  `apply_channel`                             varchar(100)       DEFAULT NULL   COMMENT '申请渠道',
  `apply_place`                               varchar(100)       DEFAULT NULL   COMMENT '申请地点',
  `borrowerp_status`                          varchar(100)       DEFAULT NULL   COMMENT '借款人状态-\r\n"1-首次申请\r\n2-内部续贷\r\n3-其他机构转单\r\n9-其他"',
  `dealer_name`                               varchar(32)        DEFAULT NULL   COMMENT '经销商名称',
  `dealer_sale_address`                       varchar(128)       DEFAULT NULL   COMMENT '经销商卖场地址',
  `store_provinces`                           varchar(32)        DEFAULT NULL   COMMENT '店面省份',
  `store_cities`                              varchar(32)        DEFAULT NULL   COMMENT '店面城市',
  `data_extraction_day`                       date               DEFAULT NULL   COMMENT '数据提取日',
  `remaining_life_extraction`                 decimal(20,6)      DEFAULT NULL   COMMENT '提取日剩余期限(月)',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `import_id`                                 int(11)            DEFAULT NULL   COMMENT '导入Id',
  `data_source`                               tinyint(2)         DEFAULT NULL   COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `loancontractinfo_index` (`project_id`,`agency_id`,`serial_number`) USING BTREE,
  KEY `index2` (`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=99983721 DEFAULT CHARSET=utf8 COMMENT='借款合同信息表-文件一';

-- ----------------------------
-- Table structure for t_monitor_asset_statistics
-- ----------------------------
-- DROP TABLE IF EXISTS `t_monitor_asset_statistics`;
CREATE TABLE `t_monitor_asset_statistics` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `project_id`                                varchar(50) NOT NULL COMMENT '项目编号',
  `product_id`                                varchar(50) NOT NULL COMMENT '产品编号',
  `monitor_date`                              date NOT NULL COMMENT '监控日期',
  `monitor_asset_count`                       int(10) NOT NULL COMMENT '监控资产笔数',
  `monitor_asset_amount`                      decimal(20,6) NOT NULL COMMENT '监控资产金额',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=619 DEFAULT CHARSET=utf8 COMMENT='监控资产信息统计表';

-- ----------------------------
-- Table structure for t_package_assetbag_log
-- ----------------------------
-- DROP TABLE IF EXISTS `t_package_assetbag_log`;
CREATE TABLE `t_package_assetbag_log` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) NOT NULL COMMENT '项目编号',
  `assetbag_id`                               varchar(32) NOT NULL COMMENT '资产包编号',
  `package_time`                              date NOT NULL COMMENT '封包时间',
  `step`                                      tinyint(11) NOT NULL COMMENT '当前步骤：1.已生成任务  2.已生成文件  3.正在POST  4.POST结束（成功/失败）',
  `status`                                    tinyint(11) DEFAULT NULL COMMENT '是否POST成功（1是0否）',
  `msg`                                       varchar(500) DEFAULT NULL COMMENT '若POST失败，失败原因',
  `asset_file_name`                           varchar(100) DEFAULT NULL COMMENT '资产包文件名称',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=474 DEFAULT CHARSET=utf8 COMMENT='封包日志';

-- ----------------------------
-- Table structure for t_product
-- ----------------------------
-- DROP TABLE IF EXISTS `t_product`;
CREATE TABLE `t_product` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `asset_bag_id`                              varchar(50) DEFAULT NULL COMMENT '资产包编号',
  `product_name`                              varchar(64) DEFAULT NULL COMMENT '产品名',
  `product_type`                              varchar(32) DEFAULT NULL COMMENT '产品类型',
  `issue_amount`                              decimal(20,2) DEFAULT NULL COMMENT '发行金额',
  `establish_date`                            date DEFAULT NULL COMMENT '成立日期',
  `issue_date`                                date DEFAULT NULL COMMENT '发行日期',
  `first_profit_begin_date`                   date DEFAULT NULL COMMENT '首期收益起始日',
  `first_profit_end_date`                     date DEFAULT NULL COMMENT '首期收益终止日',
  `first_begin_interest_date`                 date DEFAULT NULL COMMENT '首期计息起始日',
  `first_end_interest_date`                   date DEFAULT NULL COMMENT '首期计息截止日',
  `first_pay_date`                            date DEFAULT NULL COMMENT '首次兑付日',
  `first_income_report_time`                  date DEFAULT NULL COMMENT '首期收益报告日',
  `product_expiry_date`                       date DEFAULT NULL COMMENT '产品到期日',
  `first_transfer_date`                       date DEFAULT NULL COMMENT '首期收益转付日',
  `settlement_frequency`                      varchar(20) DEFAULT NULL COMMENT '收益结算频率',
  `other_product_id`                          int(11) DEFAULT NULL COMMENT '其它产品编号',
  `status`                                    tinyint(2) DEFAULT NULL COMMENT '1-产品设计 2-产品发行',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_product_asset_bag_id_project_id` (`project_id`,`asset_bag_id`)
) ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='产品信息';

-- ----------------------------
-- Table structure for t_product_credit_measures
-- ----------------------------
-- DROP TABLE IF EXISTS `t_product_credit_measures`;
CREATE TABLE `t_product_credit_measures` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `product_id`                                int(11) DEFAULT NULL COMMENT '产品Id',
  `field_name`                                varchar(100) DEFAULT NULL COMMENT '现金储备账户设置字段名',
  `field_value`                               varchar(100) DEFAULT NULL COMMENT '现金储备账户设置值',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_credit_measures_project_id_product_id` (`project_id`,`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='增信措施';

-- ----------------------------
-- Table structure for t_product_income_allocation
-- ----------------------------
-- DROP TABLE IF EXISTS `t_product_income_allocation`;
CREATE TABLE `t_product_income_allocation` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `product_id`                                int(11) DEFAULT NULL COMMENT '产品Id',
  `period`                                    int(11) DEFAULT NULL COMMENT '期数',
  `calc_begin_date`                           date DEFAULT NULL COMMENT '计算开始日',
  `calc_end_date`                             date DEFAULT NULL COMMENT '计算截止日',
  `transfer_date`                             date DEFAULT NULL COMMENT '转付日',
  `distribution_date`                         date DEFAULT NULL COMMENT '分配日',
  `redemption_date`                           date DEFAULT NULL COMMENT '兑付日',
  `allocation_scene`                          varchar(32) DEFAULT NULL COMMENT '分配场景',
  `actual_redemption_total_amount`            decimal(32,2) DEFAULT NULL COMMENT '实际兑付付息总额',
  `actual_fee_amount`                         decimal(32,2) DEFAULT NULL COMMENT '实际费用总额',
  `actual_interest_amount`                    decimal(32,2) DEFAULT NULL COMMENT '实际付税总额',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_income_allocation_project_id_product_id` (`project_id`,`product_id`)
) ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='产品收益分配';

-- ----------------------------
-- Table structure for t_product_income_allocation_plan
-- ----------------------------
-- DROP TABLE IF EXISTS `t_product_income_allocation_plan`;
CREATE TABLE `t_product_income_allocation_plan` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `product_id`                                int(11) DEFAULT NULL COMMENT '产品Id',
  `allocation_id`                             int(11) DEFAULT NULL COMMENT '分配Id',
  `period`                                    int(11) DEFAULT NULL COMMENT '期数',
  `payment_order`                             int(11) DEFAULT NULL COMMENT '支付顺序',
  `payment_item`                              varchar(128) DEFAULT NULL COMMENT '支付对象',
  `payment_item_type`                         varchar(32) DEFAULT NULL COMMENT '支付项类型',
  `theory_redemption_amount`                  decimal(32,2) DEFAULT NULL COMMENT '理论兑付金额',
  `actual_redemption_amount`                  decimal(32,2) DEFAULT NULL COMMENT '实际兑付金额',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_income_allocation_plan_project_id_product_id` (`project_id`,`product_id`)
) ENGINE=InnoDB AUTO_INCREMENT=100 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='产品收益分配方案';

-- ----------------------------
-- Table structure for t_product_securities_redemption
-- ----------------------------
-- DROP TABLE IF EXISTS `t_product_securities_redemption`;
CREATE TABLE `t_product_securities_redemption` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `product_id`                                int(11) DEFAULT NULL COMMENT '产品Id',
  `allocation_id`                             int(11) DEFAULT NULL COMMENT '分配Id',
  `period`                                    int(11) DEFAULT NULL COMMENT '期数',
  `split_info`                                varchar(32) DEFAULT NULL COMMENT '分档信息',
  `securities_name`                           varchar(32) DEFAULT NULL COMMENT '证券名称',
  `issue_amount`                              decimal(32,2) DEFAULT NULL COMMENT '发行金额',
  `begin_balance`                             decimal(32,2) DEFAULT NULL COMMENT '期初余额',
  `redemption_principal`                      decimal(32,2) DEFAULT NULL COMMENT '兑付本金',
  `end_balance`                               decimal(32,2) DEFAULT NULL COMMENT '期末余额',
  `redemption_rate`                           decimal(5,2) DEFAULT NULL COMMENT '兑付比例',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_securities_redemption_project_id_product_id` (`project_id`,`product_id`)
) ENGINE=InnoDB AUTO_INCREMENT=37 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='证券兑付情况';

-- ----------------------------
-- Table structure for t_product_tax_fee_cash_flow
-- ----------------------------
-- DROP TABLE IF EXISTS `t_product_tax_fee_cash_flow`;
CREATE TABLE `t_product_tax_fee_cash_flow` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `product_id`                                int(11) DEFAULT NULL COMMENT '产品Id',
  `tax_id`                                    int(11) DEFAULT NULL COMMENT '税费编号',
  `period`                                    int(11) DEFAULT NULL COMMENT '期数',
  `begin_billing_date`                        date DEFAULT NULL COMMENT '计费起始日',
  `end_billing_date`                          date DEFAULT NULL COMMENT '计费结束日',
  `pay_date`                                  date DEFAULT NULL COMMENT '缴费日期',
  `billing_days`                              int(11) DEFAULT NULL COMMENT '计费天数',
  `should_billing_amount`                     decimal(32,2) DEFAULT NULL COMMENT '应计费用金额',
  `should_pay_amount`                         decimal(32,2) DEFAULT NULL COMMENT '应缴费用金额',
  `total_should_pay_amount`                   decimal(32,2) DEFAULT NULL COMMENT '累计应缴金额',
  `billing_base_total_amount`                 decimal(32,2) DEFAULT NULL COMMENT '计费基数总额',
  `theory_actual_pay_amount`                  decimal(32,2) DEFAULT NULL COMMENT '理论实缴金额',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_tax_fee_cash_flow_project_id_product_id` (`project_id`,`product_id`)
) ENGINE=InnoDB AUTO_INCREMENT=728 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='产品税费现金流';

-- ----------------------------
-- Table structure for t_product_tax_fee_info
-- ----------------------------
-- DROP TABLE IF EXISTS `t_product_tax_fee_info`;
CREATE TABLE `t_product_tax_fee_info` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `product_id`                                int(11) DEFAULT NULL COMMENT '产品Id',
  `agency_id`                                 int(11) DEFAULT NULL COMMENT '机构id',
  `agency_type`                               varchar(32) DEFAULT NULL COMMENT '机构类型',
  `tax_name`                                  varchar(250) DEFAULT NULL COMMENT '费用名称',
  `billing_type`                              varchar(32) DEFAULT NULL COMMENT '计费类型',
  `billing_begin`                             int(5) DEFAULT NULL COMMENT '计费开始期',
  `billing_end`                               int(32) DEFAULT NULL COMMENT '计费结束期',
  `billing_frequency`                         varchar(32) DEFAULT NULL COMMENT '计费频率',
  `billing_method`                            varchar(32) DEFAULT NULL COMMENT '计费方式',
  `pay_frequency`                             varchar(32) DEFAULT NULL COMMENT '缴费频率',
  `rate`                                      decimal(10,2) DEFAULT NULL COMMENT '费率',
  `total_cost`                                decimal(32,2) DEFAULT NULL COMMENT '费用总额',
  `calc_split`                                varchar(32) DEFAULT NULL COMMENT '计算分档',
  `total_billing_amount`                      decimal(64,0) DEFAULT NULL COMMENT '总计费金额',
  `calc_base`                                 varchar(32) DEFAULT NULL COMMENT '计算基数',
  `cost_type`                                 varchar(32) DEFAULT NULL COMMENT '费用类型',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_tax fee_project_id_product_id` (`project_id`,`product_id`)
) ENGINE=InnoDB AUTO_INCREMENT=16 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='产品税费信息';

-- ----------------------------
-- Table structure for t_project
-- ----------------------------
-- DROP TABLE IF EXISTS `t_project`;
CREATE TABLE `t_project` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT '' COMMENT '项目编号',
  `project_stage`                             tinyint(2) DEFAULT NULL COMMENT '项目阶段 1:项目立项,2:资产管理,3:产品设计,4:存续期,5:已结束',
  `project_progress`                          tinyint(2) DEFAULT NULL COMMENT '项目进展情况',
  `client_name`                               varchar(255) DEFAULT NULL COMMENT '客户名称',
  `project_name`                              varchar(255) NOT NULL COMMENT '项目名称',
  `project_full_name`                         varchar(255) NOT NULL COMMENT '项目名全称',
  `asset_type`                                tinyint(2) NOT NULL COMMENT '资产类别 1:汽车贷,2:房贷,3:消费贷',
  `project_type`                              tinyint(2) DEFAULT NULL COMMENT '业务模式 1:存量,2:增量',
  `channel`                                   tinyint(2) DEFAULT NULL COMMENT '渠道',
  `mode`                                      tinyint(2) DEFAULT NULL COMMENT '模型归属：1-新分享，2-联合',
  `fee_type`                                  tinyint(2) DEFAULT NULL COMMENT '费率类型',
  `team`                                      tinyint(11) DEFAULT NULL COMMENT '团队',
  `trading_structure`                         varchar(255) DEFAULT NULL COMMENT '交易结构',
  `exp_coop_scale`                            varchar(255) DEFAULT NULL COMMENT '预计合作规模',
  `origination`                               tinyint(11) DEFAULT NULL COMMENT 'whos origination (WeShare or Tencent)',
  `exp_borrow_time`                           datetime DEFAULT NULL COMMENT '预计出借时间',
  `guarantee`                                 varchar(255) DEFAULT NULL COMMENT '担保',
  `asset_yield_rate`                          decimal(10,5) DEFAULT NULL COMMENT '资产端收益率',
  `current_stage`                             varchar(255) DEFAULT NULL COMMENT '目前进展',
  `next_stage`                                varchar(255) DEFAULT NULL COMMENT '下一步工作安排',
  `set_up_time`                               datetime DEFAULT NULL COMMENT '目标产品成立时间',
  `collaboration`                             varchar(255) DEFAULT NULL COMMENT '外部协作机构',
  `project_time`                              date NOT NULL COMMENT '立项时间',
  `project_begin_date`                        date DEFAULT NULL COMMENT '项目开始时间',
  `project_end_date`                          date DEFAULT NULL COMMENT '项目结束时间',
  `asset_pool_type`                           tinyint(2) NOT NULL COMMENT '资产池类型 1:静态池,2:动态池',
  `image_show_type`                           tinyint(2) DEFAULT NULL COMMENT '影像入口类型',
  `data_source`                               tinyint(2) NOT NULL COMMENT '数据来源 1:接口导入,2:excel导入',
  `public_offer`                              varchar(50) DEFAULT NULL COMMENT '公募名称',
  `create_user`                               varchar(255) NOT NULL COMMENT '创建人',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `idx_project_name` (`project_name`) USING BTREE,
  UNIQUE KEY `idx_project_full_name` (`project_full_name`) USING BTREE,
  UNIQUE KEY `idx_project_id` (`project_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=194 DEFAULT CHARSET=utf8 COMMENT='项目表';

-- ----------------------------
-- Table structure for t_project_20190702
-- ----------------------------
-- DROP TABLE IF EXISTS `t_project_20190702`;
CREATE TABLE `t_project_20190702` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT '' COMMENT '项目编号',
  `project_stage`                             tinyint(2) DEFAULT NULL COMMENT '项目阶段 1:项目立项,2:资产管理,3:产品设计,4:存续期,5:已结束',
  `project_progress`                          tinyint(2) DEFAULT NULL COMMENT '项目进展情况',
  `client_name`                               varchar(255) DEFAULT NULL COMMENT '客户名称',
  `project_name`                              varchar(255) NOT NULL COMMENT '项目名称',
  `asset_type`                                tinyint(2) NOT NULL COMMENT '资产类别 1:汽车贷,2:房贷,3:消费贷',
  `project_type`                              tinyint(2) DEFAULT NULL COMMENT '业务模式 1:存量,2:增量',
  `channel`                                   tinyint(2) DEFAULT NULL COMMENT '渠道',
  `mode`                                      tinyint(2) DEFAULT NULL COMMENT '模型归属：1-新分享，2-联合',
  `fee_type`                                  tinyint(2) DEFAULT NULL COMMENT '费率类型',
  `team`                                      tinyint(11) DEFAULT NULL COMMENT '团队',
  `trading_structure`                         varchar(255) DEFAULT NULL COMMENT '交易结构',
  `exp_coop_scale`                            varchar(255) DEFAULT NULL COMMENT '预计合作规模',
  `origination`                               tinyint(11) DEFAULT NULL COMMENT 'whos origination (WeShare or Tencent)',
  `exp_borrow_time`                           datetime DEFAULT NULL COMMENT '预计出借时间',
  `guarantee`                                 varchar(255) DEFAULT NULL COMMENT '担保',
  `asset_yield_rate`                          decimal(10,5) DEFAULT NULL COMMENT '资产端收益率',
  `current_stage`                             varchar(255) DEFAULT NULL COMMENT '目前进展',
  `next_stage`                                varchar(255) DEFAULT NULL COMMENT '下一步工作安排',
  `set_up_time`                               datetime DEFAULT NULL COMMENT '目标产品成立时间',
  `collaboration`                             varchar(255) DEFAULT NULL COMMENT '外部协作机构',
  `project_time`                              date NOT NULL COMMENT '立项时间',
  `asset_pool_type`                           tinyint(2) NOT NULL COMMENT '资产池类型 1:静态池,2:动态池',
  `data_source`                               tinyint(2) NOT NULL COMMENT '数据来源 1:接口导入,2:excel导入',
  `public_offer`                              varchar(50) DEFAULT NULL COMMENT '公募名称',
  `create_user`                               varchar(255) NOT NULL COMMENT '创建人',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `idx_project_name` (`project_name`) USING BTREE,
  UNIQUE KEY `idx_project_id` (`project_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=156 DEFAULT CHARSET=utf8 COMMENT='项目表';

-- ----------------------------
-- Table structure for t_project_agency
-- ----------------------------
-- DROP TABLE IF EXISTS `t_project_agency`;
CREATE TABLE `t_project_agency` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) NOT NULL COMMENT '项目编号',
  `agency_id`                                 int(11) NOT NULL COMMENT '机构编号',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=345 DEFAULT CHARSET=utf8 COMMENT='项目机构表';

-- ----------------------------
-- Table structure for t_project_early_payment_definition
-- ----------------------------
-- DROP TABLE IF EXISTS `t_project_early_payment_definition`;
CREATE TABLE `t_project_early_payment_definition` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `field_type`                                tinyint(2) DEFAULT NULL COMMENT '字段类型 1:新增早偿率-按金额,2:新增早偿率-按笔数,3:新增早偿率-按客户数,4:累计早偿率-按金额,5:累计早偿率-按笔数,6:累计早偿率-按客户数',
  `field_cn_name`                             varchar(255) DEFAULT NULL COMMENT '字段中文名',
  `field_def_type`                            tinyint(2) DEFAULT NULL COMMENT '字段定义 1:区间早偿本金/区期初剩余本金,2:区间早偿笔数/区期初剩余笔数,3:区间早偿客户数/区期初剩余客户数,4:累计早偿本金/封包规模,5:累计早偿笔数/封包笔数,6:累计早偿客户数/封包客户数',
  `whether_warn`                              tinyint(2) DEFAULT NULL COMMENT '是否预警 1:是,2:否',
  `warn_value`                                decimal(10,2) DEFAULT NULL COMMENT '预警值(%)',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_early_definition_projectid_field_type` (`project_id`,`field_type`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1307 DEFAULT CHARSET=utf8 COMMENT='项目早偿定义配置表';

-- ----------------------------
-- Table structure for t_project_early_payment_statistics
-- ----------------------------
-- DROP TABLE IF EXISTS `t_project_early_payment_statistics`;
CREATE TABLE `t_project_early_payment_statistics` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `asset_bag_id`                              varchar(32) DEFAULT NULL COMMENT '资产包编号',
  `statistics_date`                           date DEFAULT NULL COMMENT '统计日期',
  `statistics_type`                           tinyint(2) DEFAULT NULL COMMENT '统计类型 1:新增,2:累计',
  `early_principal`                           decimal(20,2) DEFAULT NULL COMMENT '早偿本金',
  `early_costomer_num`                        int(11) DEFAULT NULL COMMENT '早偿客户数',
  `early_asset_num`                           int(11) DEFAULT NULL COMMENT '早偿资产数',
  `costomer_num`                              int(11) DEFAULT NULL COMMENT '客户数',
  `asset_num`                                 int(11) DEFAULT NULL COMMENT '资产数',
  `early_rate_by_amount`                      decimal(10,2) DEFAULT NULL COMMENT '早偿率-按金额(%)',
  `early_rate_by_asset_num`                   decimal(10,2) DEFAULT NULL COMMENT '早偿率-按笔数(%)',
  `early_rate_by_customer_num`                decimal(10,2) DEFAULT NULL COMMENT '早偿率-按客户数(%)',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_early_payment_projectid_date` (`project_id`,`statistics_date`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=15445290 DEFAULT CHARSET=utf8 COMMENT='早偿情况统计表';

-- ----------------------------
-- Table structure for t_project_early_payment_statistics_20190325
-- ----------------------------
-- DROP TABLE IF EXISTS `t_project_early_payment_statistics_20190325`;
CREATE TABLE `t_project_early_payment_statistics_20190325` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `asset_bag_id`                              varchar(32) DEFAULT NULL COMMENT '资产包编号',
  `statistics_date`                           date DEFAULT NULL COMMENT '统计日期',
  `statistics_type`                           tinyint(2) DEFAULT NULL COMMENT '统计类型 1:当前,2:累计',
  `early_principal`                           decimal(20,2) DEFAULT NULL COMMENT '早偿本金',
  `early_costomer_num`                        int(11) DEFAULT NULL COMMENT '早偿客户数',
  `early_asset_num`                           int(11) DEFAULT NULL COMMENT '早偿资产数',
  `costomer_num`                              int(11) DEFAULT NULL COMMENT '客户数',
  `asset_num`                                 int(11) DEFAULT NULL COMMENT '资产数',
  `early_rate_by_amount`                      decimal(10,2) DEFAULT NULL COMMENT '逾期率-按金额(%)',
  `early_rate_by_asset_num`                   decimal(10,2) DEFAULT NULL COMMENT '逾期率-按笔数(%)',
  `early_rate_by_customer_num`                decimal(10,2) DEFAULT NULL COMMENT '逾期率-按客户数(%)',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_early_payment_projectid_date` (`project_id`,`statistics_date`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=8017 DEFAULT CHARSET=utf8 COMMENT='早偿情况统计表';

-- ----------------------------
-- Table structure for t_project_overdue_definition
-- ----------------------------
-- DROP TABLE IF EXISTS `t_project_overdue_definition`;
CREATE TABLE `t_project_overdue_definition` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `overdue_day`                               int(11) DEFAULT NULL COMMENT '逾期天数',
  `bad_day`                                   int(11) DEFAULT NULL COMMENT '不良天数',
  `dpd_value`                                 varchar(50) DEFAULT NULL COMMENT 'DPD定义值-如:1,2,3,4',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_overdue_definition_type` (`project_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=304 DEFAULT CHARSET=utf8 COMMENT='项目逾期定义配置表';

-- ----------------------------
-- Table structure for t_project_overdue_definition_detail
-- ----------------------------
-- DROP TABLE IF EXISTS `t_project_overdue_definition_detail`;
CREATE TABLE `t_project_overdue_definition_detail` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `overdue_def_id`                            int(11) DEFAULT NULL COMMENT '逾期定义Id',
  `field_type`                                tinyint(2) DEFAULT NULL COMMENT '字段类型 1:当前逾期率-按金额,2:当前逾期率-按笔数,3:当前逾期率-按客户数,4:累计逾期率-按金额,5:累计逾期率-按笔数,6:累计逾期率-按客户数,7:新增逾期率-按金额,8:新增逾期率-按笔数,9:新增逾期率-按客户数',
  `field_cn_name`                             varchar(255) DEFAULT NULL COMMENT '字段中文名',
  `field_def_type`                            tinyint(2) DEFAULT NULL COMMENT '字段定义 1:当期逾期资产剩余本金/封包规模,2:当前逾期资产笔数/封包笔数,3:当前逾期资产客户数/封包客户数,4:累计逾期资产剩余本金/封包规模,5:累计逾期资产笔数/封包笔数,6:累计逾期资产客户数/封包客户数,7:新增逾期率-按金额/封包规模,8:新增逾期率-按笔数/封包笔数,9:新增逾期率-按客户数/封包客户数',
  `whether_warn`                              tinyint(2) DEFAULT NULL COMMENT '是否预警 1:是,2:否',
  `overdue_warn_value`                        decimal(10,2) DEFAULT NULL COMMENT '逾期预警值(%)',
  `bad_warn_valuey`                           decimal(10,2) DEFAULT NULL COMMENT '不良预警值(%)',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_overdue_field_type` (`project_id`,`field_type`) USING BTREE,
  KEY `idx_overdue_definition_detail_projectid_overduedefid` (`project_id`,`overdue_def_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=2728 DEFAULT CHARSET=utf8 COMMENT='项目逾期定义明细配置表';

-- ----------------------------
-- Table structure for t_project_overdue_statistics
-- ----------------------------
-- DROP TABLE IF EXISTS `t_project_overdue_statistics`;
CREATE TABLE `t_project_overdue_statistics` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `statistics_id`                             varchar(32) NOT NULL COMMENT '逾期统计详情流水号',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `asset_bag_id`                              varchar(32) DEFAULT NULL COMMENT '资产包编号',
  `statistics_date`                           date DEFAULT NULL COMMENT '统计日期',
  `overdue_type`                              tinyint(2) DEFAULT NULL COMMENT '统计类型 1:当前,2:累计,3:新增',
  `overdue_interval`                          varchar(32) NOT NULL COMMENT '逾期区间',
  `overdue_remain_principal`                  decimal(20,2) DEFAULT NULL COMMENT '逾期剩余本金',
  `overdue_principal`                         decimal(20,2) DEFAULT NULL COMMENT '当前逾期本金',
  `overdue_costomer_num`                      int(11) DEFAULT NULL COMMENT '逾期客户数',
  `overdue_asset_num`                         int(11) DEFAULT NULL COMMENT '逾期资产数',
  `costomer_num`                              int(11) DEFAULT NULL COMMENT '客户数',
  `asset_num`                                 int(11) DEFAULT NULL COMMENT '资产数',
  `overdue_rate_by_amount`                    decimal(10,2) DEFAULT NULL COMMENT '逾期率-按金额(%)',
  `overdue_rate_by_asset_num`                 decimal(10,2) DEFAULT NULL COMMENT '逾期率-按笔数(%)',
  `overdue_rate_by_customer_num`              decimal(10,2) DEFAULT NULL COMMENT '逾期率-按客户数(%)',
  `package_principal_balance`                 decimal(20,2) DEFAULT NULL COMMENT '封包总本金余额',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `statistics_id` (`statistics_id`),
  UNIQUE KEY `idx_overdue_statistics` (`project_id`,`asset_bag_id`,`statistics_date`,`overdue_type`,`overdue_interval`) USING BTREE,
  KEY `idx_current_overdue_projectid_date` (`project_id`,`asset_bag_id`,`statistics_date`,`overdue_type`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=108052581 DEFAULT CHARSET=utf8 COMMENT='当前逾期情况统计表';

-- ----------------------------
-- Table structure for t_project_overdue_statistics_20190411
-- ----------------------------
-- DROP TABLE IF EXISTS `t_project_overdue_statistics_20190411`;
CREATE TABLE `t_project_overdue_statistics_20190411` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `asset_bag_id`                              varchar(32) DEFAULT NULL COMMENT '资产包编号',
  `statistics_date`                           date DEFAULT NULL COMMENT '统计日期',
  `overdue_type`                              tinyint(2) DEFAULT NULL COMMENT '统计类型 1:当前,2:累计',
  `overdue_interval`                          varchar(32) NOT NULL COMMENT '逾期区间',
  `overdue_remain_principal`                  decimal(20,2) DEFAULT NULL COMMENT '逾期剩余本金',
  `overdue_principal`                         decimal(20,2) DEFAULT NULL COMMENT '当前逾期本金',
  `overdue_costomer_num`                      int(11) DEFAULT NULL COMMENT '逾期客户数',
  `overdue_asset_num`                         int(11) DEFAULT NULL COMMENT '逾期资产数',
  `costomer_num`                              int(11) DEFAULT NULL COMMENT '客户数',
  `asset_num`                                 int(11) DEFAULT NULL COMMENT '资产数',
  `overdue_rate_by_amount`                    decimal(10,2) DEFAULT NULL COMMENT '逾期率-按金额(%)',
  `overdue_rate_by_asset_num`                 decimal(10,2) DEFAULT NULL COMMENT '逾期率-按笔数(%)',
  `overdue_rate_by_customer_num`              decimal(10,2) DEFAULT NULL COMMENT '逾期率-按客户数(%)',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_current_overdue_projectid_date` (`project_id`,`statistics_date`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=14756787 DEFAULT CHARSET=utf8 COMMENT='当前逾期情况统计表';

-- ----------------------------
-- Table structure for t_project_report_setting
-- ----------------------------
-- DROP TABLE IF EXISTS `t_project_report_setting`;
CREATE TABLE `t_project_report_setting` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `project_id`                                varchar(255) NOT NULL COMMENT '项目编号',
  `report_type`                               int(2) NOT NULL COMMENT '报告类型（1.资产服务报告2.待定）',
  `status`                                    int(2) NOT NULL COMMENT '设置状态（0.无法开启1.可开启未设置2.设置模板完成3.全部设置完成）',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=121 DEFAULT CHARSET=utf8 COMMENT='项目报告设置表';

-- ----------------------------
-- Table structure for t_project_statistics_detail
-- ----------------------------
-- DROP TABLE IF EXISTS `t_project_statistics_detail`;
CREATE TABLE `t_project_statistics_detail` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `statistics_id`                             varchar(32) DEFAULT NULL COMMENT '逾期统计详情流水号',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `asset_bag_id`                              varchar(32) DEFAULT NULL COMMENT '资产包编号',
  `statistics_date`                           date DEFAULT NULL COMMENT '统计日期',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `document_num`                              varchar(100) DEFAULT NULL COMMENT '证件号码',
  `record_type`                               tinyint(2) DEFAULT NULL COMMENT '记录类型 1:早偿,2:逾期',
  `overdue_type`                              tinyint(2) DEFAULT NULL COMMENT '统计类型 1:当前,2:累计,3:新增',
  `asset_status`                              varchar(32) DEFAULT NULL COMMENT '资产状态',
  `closeoff_reason`                           varchar(32) DEFAULT NULL COMMENT '结清原因',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_project_statistics_detail` (`project_id`,`asset_bag_id`,`statistics_date`,`serial_number`,`document_num`) USING BTREE,
  KEY `idx_project_statistics_detail2` (`statistics_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=783052019 DEFAULT CHARSET=utf8 COMMENT='逾期详情(客户-资产)统计表';

-- ----------------------------
-- Table structure for t_projectaccountcheck
-- ----------------------------
-- DROP TABLE IF EXISTS `t_projectaccountcheck`;
CREATE TABLE `t_projectaccountcheck` (
  `id`                                        int(32) NOT NULL AUTO_INCREMENT COMMENT '项目对账信息主键',
  `project_id`                                varchar(32)   DEFAULT NULL      COMMENT '项目编号',
  `agency_id`                                 varchar(32)   DEFAULT NULL      COMMENT '机构编号',
  `loan_sum`                                  int(8)        DEFAULT NULL      COMMENT '贷款总数量',
  `loan_remaining_principal`                  decimal(20,6) DEFAULT NULL      COMMENT '贷款剩余本金',
  `loan_totalamount`                          decimal(20,6) DEFAULT NULL      COMMENT '贷款合同总金额',
  `overdue_assets_sum1`                       int(4)        DEFAULT NULL      COMMENT '1~7逾期资产数',
  `overdue_assets_outstanding_principal1`     decimal(20,6) DEFAULT NULL      COMMENT '1~7逾期资产剩余未还本金',
  `overdue_assets_sum8`                       int(4)        DEFAULT NULL      COMMENT '8~30逾期资产数',
  `overdue_assets_outstanding_principal8`     decimal(20,6) DEFAULT NULL      COMMENT '8~30逾期资产剩余未还本金',
  `overdue_assets_sum31`                      int(4)        DEFAULT NULL      COMMENT '31~60逾期资产数',
  `overdue_assets_outstanding_principal31`    decimal(20,6) DEFAULT NULL      COMMENT '31~60逾期资产剩余未还本金',
  `overdue_assets_sum61`                      int(4)        DEFAULT NULL      COMMENT '61~90逾期资产数',
  `overdue_assets_outstanding_principal61`    decimal(20,6) DEFAULT NULL      COMMENT '61~90逾期资产剩余未还本金',
  `overdue_assets_sum91`                      int(4)        DEFAULT NULL      COMMENT '91~180逾期资产数',
  `overdue_assets_outstanding_principal91`    decimal(20,6) DEFAULT NULL      COMMENT '91~180逾期资产剩余未还本金',
  `overdue_assets_sum180`                     int(4)        DEFAULT NULL      COMMENT '180+逾期资产数',
  `overdue_assets_outstanding_principal180`   decimal(20,6) DEFAULT NULL      COMMENT '180+逾期资产剩余未还本金',
  `add_loan_sum`                              int(4)        DEFAULT NULL      COMMENT '当日新增贷款笔数',
  `add_loan_totalamount`                      decimal(20,6) DEFAULT NULL      COMMENT '当日新增贷款总金额',
  `actual_payment_sum`                        int(4)        DEFAULT NULL      COMMENT '当日实还资产笔数',
  `actual_payment_totalamount`                decimal(20,6) DEFAULT NULL      COMMENT '当日实还总金额',
  `back_assets_sum`                           int(4)        DEFAULT NULL      COMMENT '当日回购资产笔数',
  `back_assets_totalamount`                   decimal(20,6) DEFAULT NULL      COMMENT '当日回购总金额',
  `disposal_assets_sum`                       int(4)        DEFAULT NULL      COMMENT '当日处置资产笔数',
  `disposal_assets_totalamount`               decimal(20,6) DEFAULT NULL      COMMENT '当日处置回收总金额',
  `differentia_complement_assets_sum`         int(4)        DEFAULT NULL      COMMENT '当日差额补足资产笔数',
  `differentia_complement_assets_totalamount` decimal(20,6) DEFAULT NULL      COMMENT '当日差额补足总金额',
  `compensatory_assets_sum`                   int(4)        DEFAULT NULL      COMMENT '当日代偿资产笔数',
  `compensatory_assets_totalamount`           decimal(20,6) DEFAULT NULL      COMMENT '当日代偿总金额',
  `data_extraction_day`                       date          DEFAULT NULL      COMMENT '数据提取日',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `import_id`                                 int(11)       DEFAULT NULL      COMMENT '导入Id',
  `data_source`                               tinyint(2)    DEFAULT NULL      COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `projectaccountcheck_index` (`project_id`,`agency_id`,`data_extraction_day`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=91236 DEFAULT CHARSET=utf8 COMMENT='项目对账信息表-文件十一';

-- ----------------------------
-- Table structure for t_related_assets
-- ----------------------------
-- DROP TABLE IF EXISTS `t_related_assets`;
CREATE TABLE `t_related_assets` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `import_id`                                 int(11) NOT NULL,
  `serial_number`                             varchar(50) NOT NULL COMMENT '借据号',
  `project_id`                                varchar(50) NOT NULL COMMENT '项目编号',
  `related_serial_number`                     varchar(100) NOT NULL COMMENT '关联借据号',
  `related_project_id`                        varchar(50) NOT NULL COMMENT '关联项目编号',
  `related_status`                            int(10) NOT NULL DEFAULT '1' COMMENT '关联状态（1正常2解除关联）',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `relatedassets_index` (`import_id`,`project_id`,`related_serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=2487 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_repayment_order
-- ----------------------------
-- DROP TABLE IF EXISTS `t_repayment_order`;
CREATE TABLE `t_repayment_order` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `product_id`                                int(11) DEFAULT NULL COMMENT '产品Id',
  `repayment_order_id`                        int(11) DEFAULT NULL COMMENT '偿付顺序编号',
  `repayment_order`                           int(3) DEFAULT NULL COMMENT '偿付顺序',
  `object_name`                               varchar(250) DEFAULT NULL COMMENT '对象名称',
  `object_type`                               varchar(32) DEFAULT NULL COMMENT '对象类型',
  `repayment_object_id`                       int(11) DEFAULT NULL COMMENT '偿付对象编号',
  `scene_name`                                varchar(32) DEFAULT NULL COMMENT '场景名称',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_repayment_order_project_id_product_id` (`project_id`,`product_id`)
) ENGINE=InnoDB AUTO_INCREMENT=332 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='偿付顺序信息';

-- ----------------------------
-- Table structure for t_repaymentplan
-- ----------------------------
-- DROP TABLE IF EXISTS `t_repaymentplan`;
CREATE TABLE `t_repaymentplan` (
  `id`                                        int(32) NOT NULL AUTO_INCREMENT COMMENT '还款计划主键',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `agency_id`                                 varchar(32) DEFAULT NULL COMMENT '机构编号',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `period`                                    int(8) DEFAULT NULL COMMENT '期次',
  `should_repay_date`                         date DEFAULT NULL COMMENT '应还款日',
  `should_repay_principal`                    decimal(20,6) DEFAULT NULL COMMENT '应还本金',
  `should_repay_interest`                     decimal(20,6) DEFAULT NULL COMMENT '应还利息',
  `should_repay_cost`                         decimal(20,6) DEFAULT NULL COMMENT '应还费用',
  `begin_principal_balance`                   decimal(20,6) DEFAULT NULL COMMENT '期初剩余本金',
  `end_principal_balance`                     decimal(20,6) DEFAULT NULL COMMENT '期末剩余本金',
  `effective_date`                            date DEFAULT NULL COMMENT '生效日期',
  `repay_status`                              tinyint(4) DEFAULT NULL COMMENT '还款状态 1:入池前已还,2:入池前未还',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入Id',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport,3:systemgenerated',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `repaymentplan_index` (`project_id`,`serial_number`,`agency_id`,`period`) USING BTREE,
  KEY `repaymentplan_index2` (`serial_number`),
  KEY `repaymentplan_index3` (`project_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=118904357 DEFAULT CHARSET=utf8 COMMENT='还款计划表-文件五';

-- ----------------------------
-- Table structure for t_repaymentplan_history
-- ----------------------------
-- DROP TABLE IF EXISTS `t_repaymentplan_history`;
CREATE TABLE `t_repaymentplan_history` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '还款计划主键',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `agency_id`                                 varchar(32) DEFAULT NULL COMMENT '机构编号',
  `serial_number`                             varchar(100) DEFAULT NULL COMMENT '借据号',
  `period`                                    tinyint(8) DEFAULT NULL COMMENT '期次',
  `should_repay_date`                         date DEFAULT NULL COMMENT '应还款日',
  `should_repay_principal`                    decimal(20,6) DEFAULT NULL COMMENT '应还本金',
  `should_repay_interest`                     decimal(20,6) DEFAULT NULL COMMENT '应还利息',
  `should_repay_cost`                         decimal(20,6) DEFAULT NULL COMMENT '应还费用',
  `begin_principal_balance`                   decimal(20,6) DEFAULT NULL COMMENT '期初剩余本金',
  `end_principal_balance`                     decimal(20,6) DEFAULT NULL COMMENT '期末剩余本金',
  `effective_date`                            date DEFAULT NULL COMMENT '生效日期',
  `repay_status`                              tinyint(4) DEFAULT NULL COMMENT '还款状态 1:入池前已还,2:入池前未还',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `import_id`                                 int(11) DEFAULT NULL COMMENT '导入Id',
  `data_source`                               tinyint(2) DEFAULT NULL COMMENT '数据来源 1:startLink,2:excelImport,3:systemgenerated',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `repaymentplan_history_index` (`project_id`,`serial_number`,`agency_id`,`period`,`effective_date`) USING BTREE,
  KEY `IX_REPAY_HIS_SERIAL_EFFECTIVE` (`serial_number`,`effective_date`)
) ENGINE=InnoDB AUTO_INCREMENT=26950022 DEFAULT CHARSET=utf8 COMMENT='还款计划历史';

-- ----------------------------
-- Table structure for t_report_date_list
-- ----------------------------
-- DROP TABLE IF EXISTS `t_report_date_list`;
CREATE TABLE `t_report_date_list` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `date_setting_id`                           int(11) NOT NULL COMMENT '日期设置表主键',
  `start_date`                                date NOT NULL COMMENT '报告区间开始日',
  `end_date`                                  date NOT NULL COMMENT '报告区间结束日',
  `report_date`                               date NOT NULL COMMENT '报告日',
  `term`                                      int(4) NOT NULL COMMENT '期次',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6917 DEFAULT CHARSET=utf8 COMMENT='报告日期列表信息表';

-- ----------------------------
-- Table structure for t_report_date_setting
-- ----------------------------
-- DROP TABLE IF EXISTS `t_report_date_setting`;
CREATE TABLE `t_report_date_setting` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `project_id`                                varchar(32) NOT NULL COMMENT '项目编号',
  `product_id`                                varchar(50) DEFAULT NULL COMMENT '产品编号',
  `report_type`                               int(2) NOT NULL COMMENT '报告类型',
  `dimension_type`                            int(2) NOT NULL COMMENT '维度类型',
  `setting_status`                            tinyint(2) NOT NULL COMMENT '设置状态（0.未设置1.已设置）',
  `is_effective`                              tinyint(2) NOT NULL COMMENT '是否生效（0.否1.是）',
  `package_bag_date`                          date NOT NULL COMMENT '封包日',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=102 DEFAULT CHARSET=utf8 COMMENT='报告日期设置表';

-- ----------------------------
-- Table structure for t_report_head
-- ----------------------------
-- DROP TABLE IF EXISTS `t_report_head`;
CREATE TABLE `t_report_head` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `project_id`                                varchar(32) NOT NULL COMMENT '项目编号',
  `module_id`                                 int(11) NOT NULL COMMENT '模块id',
  `sub_module_type`                           varchar(32) DEFAULT NULL COMMENT '子模块类型',
  `code`                                      varchar(50) NOT NULL COMMENT '字段code',
  `ori_name`                                  varchar(100) NOT NULL COMMENT '字段原表述',
  `new_name`                                  varchar(100) DEFAULT NULL COMMENT '字段新表述',
  `content_describe`                          varchar(255) DEFAULT NULL COMMENT '字段描述',
  `is_displayed`                              tinyint(2) NOT NULL COMMENT '是否展示（0.否1.是）',
  `mark`                                      tinyint(2) NOT NULL COMMENT '字段属性(1.横向表头2.竖向表头3.表格内容)',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx1` (`project_id`,`module_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=5671 DEFAULT CHARSET=utf8 COMMENT='报告模块内容配置表';

-- ----------------------------
-- Table structure for t_report_info
-- ----------------------------
-- DROP TABLE IF EXISTS `t_report_info`;
CREATE TABLE `t_report_info` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `date_list_id`                              int(11) NOT NULL COMMENT '日期列表主键id',
  `file_cn_name`                              varchar(100) DEFAULT NULL COMMENT '文件中文名',
  `file_en_name`                              varchar(100) DEFAULT NULL COMMENT '文件英文名',
  `file_path`                                 varchar(100) DEFAULT NULL COMMENT '文件存放路径',
  `status`                                    int(2) NOT NULL COMMENT '文件状态（0.待生成1.可生成2.生成中3.已生成4.生成失败）',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_date_list_id` (`date_list_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=5151 DEFAULT CHARSET=utf8 COMMENT='报告信息表';

-- ----------------------------
-- Table structure for t_report_module
-- ----------------------------
-- DROP TABLE IF EXISTS `t_report_module`;
CREATE TABLE `t_report_module` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `project_id`                                varchar(50) NOT NULL,
  `report_type`                               tinyint(2) NOT NULL COMMENT '报告类型（1.资产服务报告2.待定）',
  `module_ori_name`                           varchar(255) NOT NULL COMMENT '模块原名称',
  `module_new_name`                           varchar(255) DEFAULT NULL COMMENT '模块新名称',
  `module_describe`                           varchar(500) DEFAULT NULL COMMENT '模块描述',
  `is_displayed`                              tinyint(2) NOT NULL COMMENT '模块是否展示（0.否1.是）',
  `module_order`                              tinyint(2) NOT NULL COMMENT '模块位置',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=294 DEFAULT CHARSET=utf8 COMMENT='报告模块配置表';

-- ----------------------------
-- Table structure for t_risk_control_execution_date_list
-- ----------------------------
-- DROP TABLE IF EXISTS `t_risk_control_execution_date_list`;
CREATE TABLE `t_risk_control_execution_date_list` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `project_id`                                varchar(50) NOT NULL COMMENT '项目编号',
  `task_type`                                 tinyint(2) NOT NULL COMMENT '任务类型：8 调用存续期风控 ',
  `monitor_date`                              date NOT NULL COMMENT '监控日期',
  `execution_time`                            timestamp NULL DEFAULT NULL COMMENT '任务执行时间',
  `task_status`                               tinyint(2) NOT NULL COMMENT '任务执行状态: 1未执行 2执行中 3已执行 4执行失败',
  `task_executions_sum`                       int(11) DEFAULT NULL COMMENT '任务执行条数',
  `task_yes_sum`                              int(11) DEFAULT NULL COMMENT '任务调用成功条数',
  `task_no_sum`                               int(11) DEFAULT NULL COMMENT '任务调用失败条数',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `t_risk_control_execution_date_list_index` (`project_id`,`task_type`,`monitor_date`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1221 DEFAULT CHARSET=utf8 COMMENT='风控执行时间列表';

-- ----------------------------
-- Table structure for t_risk_control_model_config
-- ----------------------------
-- DROP TABLE IF EXISTS `t_risk_control_model_config`;
CREATE TABLE `t_risk_control_model_config` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `project_id`                                varchar(50) NOT NULL COMMENT '项目编号',
  `model_type`                                tinyint(2) NOT NULL COMMENT '风控模型类型1.入池风控模型2.存续风控模型',
  `risk_control_model`                        tinyint(2) DEFAULT NULL COMMENT '风控接入模式',
  `model_belong`                              tinyint(2) DEFAULT NULL COMMENT '模型归属',
  `model_id`                                  varchar(32) DEFAULT NULL COMMENT '模型id',
  `model_name`                                varchar(32) DEFAULT NULL COMMENT '模型名称',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=532 DEFAULT CHARSET=utf8 COMMENT='项目风控模型配置表';

-- ----------------------------
-- Table structure for t_risk_control_model_config_detail
-- ----------------------------
-- DROP TABLE IF EXISTS `t_risk_control_model_config_detail`;
CREATE TABLE `t_risk_control_model_config_detail` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `project_id`                                varchar(50) NOT NULL COMMENT '项目编号',
  `asset_level`                               varchar(10) DEFAULT NULL COMMENT '资产等级',
  `credit_level`                              varchar(10) DEFAULT NULL COMMENT '信用等级',
  `antifraud_level`                           varchar(10) DEFAULT NULL COMMENT '反欺诈等级',
  `is_open_duration_plan`                     tinyint(2) NOT NULL COMMENT '是否开启存续期风控计划0否1是',
  `first_risk_control_date`                   date DEFAULT NULL COMMENT '首次存续风控日期',
  `is_open_month_rule`                        tinyint(2) DEFAULT NULL COMMENT '是否开启月末法则0否1是',
  `cycle`                                     int(4) DEFAULT NULL COMMENT '周期',
  `risk_control_end_date`                     date DEFAULT NULL COMMENT '存续风控截止日期',
  `is_include_end_date`                       tinyint(2) DEFAULT NULL COMMENT '是否包含截止日',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=265 DEFAULT CHARSET=utf8 COMMENT='风控模型配置详情表';

-- ----------------------------
-- Table structure for t_risk_control_warning_info
-- ----------------------------
-- DROP TABLE IF EXISTS `t_risk_control_warning_info`;
CREATE TABLE `t_risk_control_warning_info` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `project_id`                                varchar(50) NOT NULL COMMENT '项目编号',
  `product_id`                                varchar(50) NOT NULL COMMENT '产品编号',
  `warning_type`                              tinyint(2) NOT NULL COMMENT '预警类型：1关注 2预警',
  `monitor_date`                              date NOT NULL COMMENT '监控日期',
  `serial_number`                             varchar(50) NOT NULL COMMENT '借据号',
  `contract_code`                             varchar(50) NOT NULL COMMENT '合同编号',
  `total_term`                                int(4) DEFAULT NULL COMMENT '总期数',
  `contract_amount`                           decimal(20,6) DEFAULT NULL COMMENT '合同金额',
  `remain_principal`                          decimal(20,6) DEFAULT NULL COMMENT '当前剩余本金',
  `remain_term`                               int(4) DEFAULT NULL COMMENT '剩余期数',
  `current_overdue_amount`                    decimal(20,6) DEFAULT NULL COMMENT '当前逾期金额',
  `current_overdue_daynum`                    int(4) DEFAULT NULL COMMENT '当前逾期天数',
  `history_most_overdue_daynum`               int(4) DEFAULT NULL COMMENT '最长单期逾期天数',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=15995 DEFAULT CHARSET=utf8 COMMENT='风控关注预警信息表';

-- ----------------------------
-- Table structure for t_risk_control_warning_setting
-- ----------------------------
-- DROP TABLE IF EXISTS `t_risk_control_warning_setting`;
CREATE TABLE `t_risk_control_warning_setting` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `project_id`                                varchar(50) NOT NULL COMMENT '项目编号',
  `warning_type`                              tinyint(2) NOT NULL COMMENT '预警类型：1关注 2预警',
  `is_include_asset_dimension`                tinyint(2) NOT NULL COMMENT '是否包含资产维度',
  `overdue_days`                              int(4) DEFAULT NULL COMMENT '逾期天数',
  `is_include_risk_control_dimension`         tinyint(2) NOT NULL COMMENT '是否包含风控维度',
  `frequency`                                 int(4) DEFAULT NULL COMMENT '最近次数',
  `is_include_asset_level`                    tinyint(2) DEFAULT NULL COMMENT '是否包含资产等级',
  `asset_level`                               varchar(10) DEFAULT NULL COMMENT '资产等级',
  `is_include_credit_level`                   tinyint(2) DEFAULT NULL COMMENT '是否包含信用等级',
  `credit_level`                              varchar(10) DEFAULT NULL COMMENT '信用等级',
  `is_include_antifraud_level`                tinyint(2) DEFAULT NULL COMMENT '是否包含反欺诈等级',
  `antifraud_level`                           varchar(10) DEFAULT NULL COMMENT '反欺诈等级',
  `is_include_black_list`                     tinyint(2) DEFAULT NULL COMMENT '是否包含命中黑名单',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=133 DEFAULT CHARSET=utf8 COMMENT='风控关注预警设置表';

-- ----------------------------
-- Table structure for t_split_cash_flow
-- ----------------------------
-- DROP TABLE IF EXISTS `t_split_cash_flow`;
CREATE TABLE `t_split_cash_flow` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `product_id`                                int(11) DEFAULT NULL COMMENT '产品Id',
  `period`                                    int(11) DEFAULT '0' COMMENT '期数',
  `block_structure_id`                        varchar(32) DEFAULT NULL COMMENT '分档结构编码',
  `split_info`                                varchar(32) DEFAULT NULL COMMENT '分档信息',
  `interest_payment_start_date`               date DEFAULT NULL COMMENT '计息开始日',
  `interest_payment_end_date`                 date DEFAULT NULL COMMENT '计息结束日',
  `interest_payment_date`                     date DEFAULT NULL COMMENT '付息日',
  `should_repay_principal`                    decimal(32,2) DEFAULT NULL COMMENT '应付本金',
  `should_repay_interest`                     decimal(32,2) DEFAULT NULL COMMENT '应付利息',
  `begin_balance`                             decimal(32,2) DEFAULT NULL COMMENT '期初余额',
  `end_balance`                               decimal(32,2) DEFAULT NULL COMMENT '期末余额',
  `weight_avg_term`                           decimal(32,2) DEFAULT NULL COMMENT '加权平均期限',
  `interest_days`                             int(11) DEFAULT NULL COMMENT '计息天数',
  `theory_actual_pay_principal`               decimal(32,2) DEFAULT NULL COMMENT '理论实付本金',
  `theory_actual_pay_interest`                decimal(32,2) DEFAULT NULL COMMENT '理论实付利息',
  `theory_begin_principal_balance`            decimal(32,2) DEFAULT NULL COMMENT '理论期初本金余额',
  `theory_end_principal_balance`              decimal(32,2) DEFAULT NULL COMMENT '理论期末本金余额',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_split_cash_flow_project_id_product_id` (`project_id`,`product_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1278 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='分档现金流';

-- ----------------------------
-- Table structure for t_split_securities_info
-- ----------------------------
-- DROP TABLE IF EXISTS `t_split_securities_info`;
CREATE TABLE `t_split_securities_info` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `product_id`                                int(11) DEFAULT NULL COMMENT '产品Id',
  `block_structure_id`                        varchar(32) DEFAULT NULL COMMENT '分档结构编码',
  `credit_level`                              varchar(32) DEFAULT NULL COMMENT '信用等级',
  `split_info`                                varchar(32) DEFAULT NULL COMMENT '分档信息',
  `securities_code`                           varchar(64) DEFAULT NULL COMMENT '证券代码',
  `securities_name`                           varchar(64) DEFAULT NULL COMMENT '证券名称',
  `securities_rate`                           decimal(20,2) DEFAULT NULL COMMENT '该档证券占比',
  `issue_amount`                              decimal(20,2) DEFAULT NULL COMMENT '发行金额',
  `issue_rate`                                decimal(20,2) DEFAULT NULL COMMENT '发行利率',
  `interest_frequency`                        varchar(32) DEFAULT NULL COMMENT '付息频率',
  `pay_type`                                  varchar(32) DEFAULT NULL COMMENT '偿还方式',
  `begin_split_interval`                      int(11) DEFAULT NULL COMMENT '分档区间 开始期',
  `end_split_interval`                        int(11) DEFAULT NULL COMMENT '分档区间 结束期',
  `interest_method`                           varchar(30) DEFAULT NULL COMMENT '计息方式',
  `issue`                                     tinyint(2) DEFAULT '0' COMMENT '是否发布',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_split_securities_project_id_product_id` (`project_id`,`product_id`)
) ENGINE=InnoDB AUTO_INCREMENT=38 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='分档证券信息';

-- ----------------------------
-- Table structure for t_startLink_image
-- ----------------------------
-- DROP TABLE IF EXISTS `t_startLink_image`;
CREATE TABLE `t_startLink_image` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_type`                              varchar(32) NOT NULL COMMENT '项目类型-泛华、易鑫、先锋太盟、云南信托、小米',
  `project_path`                              varchar(100) NOT NULL COMMENT '星连文件所在位置',
  `path_available`                            tinyint(2) DEFAULT '1' COMMENT '是否可用：0-不可用，1-可用',
  `full_download`                             tinyint(2) DEFAULT '0' COMMENT '是否全量下载：0-否，1-是',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_image_type` (`project_type`,`project_path`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8 COMMENT='星连压缩影像文件路径表';

-- ----------------------------
-- Table structure for t_task
-- ----------------------------
-- DROP TABLE IF EXISTS `t_task`;
CREATE TABLE `t_task` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `task_code`                                 varchar(50) DEFAULT NULL COMMENT '任务代码',
  `task_name`                                 varchar(50) DEFAULT NULL COMMENT '任务名称',
  `operate_type`                              tinyint(2) DEFAULT NULL COMMENT '操作类型, 1:手动,2:自动',
  `execute_url`                               varchar(100) DEFAULT NULL COMMENT '执行请求地址',
  `task_desc`                                 varchar(200) DEFAULT NULL COMMENT '任务描述',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_t_task_task_code` (`task_code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8 COMMENT='任务信息';

-- ----------------------------
-- Table structure for t_task_log
-- ----------------------------
-- DROP TABLE IF EXISTS `t_task_log`;
CREATE TABLE `t_task_log` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `task_code`                                 varchar(32) DEFAULT NULL COMMENT '任务代码',
  `task_name`                                 varchar(50) DEFAULT NULL COMMENT '任务名称',
  `deal_date`                                 date DEFAULT NULL COMMENT '处理日期',
  `task_param`                                varchar(300) DEFAULT NULL COMMENT '任务参数 json格式',
  `operate_type`                              tinyint(2) DEFAULT NULL COMMENT '操作类型, 1:手动,2:自动',
  `task_status`                               tinyint(2) DEFAULT NULL COMMENT '任务状态, 1:未执行,2:执行中,3:成功,4:失败',
  `result_desc`                               varchar(200) DEFAULT NULL COMMENT '执行结果描述',
  `begin_time`                                datetime DEFAULT NULL COMMENT '执行开始时间',
  `end_time`                                  datetime DEFAULT NULL COMMENT '执行结束时间',
  `rt`                                        int(11) DEFAULT NULL COMMENT '耗时(ms)',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_t_task_log_deal_date` (`deal_date`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=13136 DEFAULT CHARSET=utf8 COMMENT='任务执行日志';

-- ----------------------------
-- Table structure for t_user_agency
-- ----------------------------
-- DROP TABLE IF EXISTS `t_user_agency`;
CREATE TABLE `t_user_agency` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) NOT NULL COMMENT '项目编号',
  `agency_id`                                 int(11) NOT NULL COMMENT '机构编号',
  `user_id`                                   varchar(64) DEFAULT NULL COMMENT '用户Id',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `agency_type`                               varchar(32) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL COMMENT '机构类型',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `project_agency_user_type` (`project_id`,`agency_id`,`user_id`,`agency_type`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=488 DEFAULT CHARSET=utf8 COMMENT='项目机构表';

-- ----------------------------
-- Table structure for t_user_agency_20190301
-- ----------------------------
-- DROP TABLE IF EXISTS `t_user_agency_20190301`;
CREATE TABLE `t_user_agency_20190301` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `project_id`                                varchar(32) NOT NULL COMMENT '项目编号',
  `agency_id`                                 int(11) NOT NULL COMMENT '机构编号',
  `user_id`                                   varchar(64) DEFAULT NULL COMMENT '用户Id',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `agency_type`                               varchar(32) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL COMMENT '机构类型',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `project_agency_user_type` (`project_id`,`agency_id`,`user_id`,`agency_type`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=354 DEFAULT CHARSET=utf8 COMMENT='项目机构表';

-- ----------------------------
-- Table structure for t_user_opera_log
-- ----------------------------
-- DROP TABLE IF EXISTS `t_user_opera_log`;
CREATE TABLE `t_user_opera_log` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `user_id`                                   varchar(32) DEFAULT NULL COMMENT '用户ID',
  `project_id`                                varchar(32) DEFAULT NULL COMMENT '项目编号',
  `opera_type`                                tinyint(2) DEFAULT NULL COMMENT '操作类型 1:项目切换,2:现金流设置',
  `opera_name`                                varchar(100) DEFAULT NULL COMMENT '操作内容',
  `opera_pre_value`                           varchar(500) DEFAULT NULL COMMENT '操作前内容',
  `opera_after_name`                          varchar(500) DEFAULT NULL COMMENT '操作后内容',
  `opera_status`                              tinyint(2) DEFAULT NULL COMMENT '是否成功：1-成功，2-失败',
  `opera_desc`                                varchar(500) DEFAULT NULL COMMENT '操作描述',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2170 DEFAULT CHARSET=utf8 COMMENT='用户操作日志';

-- ----------------------------
-- Table structure for t_user_white_list
-- ----------------------------
-- DROP TABLE IF EXISTS `t_user_white_list`;
CREATE TABLE `t_user_white_list` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `agency_code`                               varchar(32) NOT NULL COMMENT '机构编号',
  `user_id`                                   varchar(64) DEFAULT NULL COMMENT '用户Id',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `status`                                    tinyint(1) NOT NULL DEFAULT '1' COMMENT '状态 1启用2禁用',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=95 DEFAULT CHARSET=utf8 COMMENT='用户白名单';

-- ----------------------------
-- Table structure for t_wind_control_req_log
-- ----------------------------
-- DROP TABLE IF EXISTS `t_wind_control_req_log`;
CREATE TABLE `t_wind_control_req_log` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `task_id`                                   varchar(32) NOT NULL COMMENT '任务流水号',
  `ent_no`                                    varchar(32) NOT NULL COMMENT '商户号',
  `project_id`                                varchar(32) NOT NULL COMMENT '项目编号',
  `apply_no`                                  varchar(32) NOT NULL COMMENT '申请编号(借据号)',
  `timestamp`                                 varchar(20) NOT NULL COMMENT '时间戳',
  `req_url`                                   varchar(100) NOT NULL COMMENT '请求url',
  `req_content`                               text NOT NULL COMMENT '请求报文',
  `req_step`                                  tinyint(2) NOT NULL COMMENT '调用链 1:星连调星云,2:星云调风控,3:内部调用',
  `req_status`                                tinyint(2) NOT NULL COMMENT '请求状态 1:成功,2:失败',
  `req_interface`                             tinyint(2) NOT NULL COMMENT '请求接口 1:风控审核4号接口,2:风控结果查询新分享本地接口,3:风控结果查询5号接口,4:风控查询6号接口',
  `wind_moment`                               tinyint(10) DEFAULT NULL COMMENT '风控时刻（0：入池时，1：资产跟踪)',
  `error_msg`                                 varchar(100) DEFAULT NULL COMMENT '错误信息',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=147276 DEFAULT CHARSET=utf8 COMMENT='风控请求日志';

-- ----------------------------
-- Table structure for t_wind_control_resp_log
-- ----------------------------
-- DROP TABLE IF EXISTS `t_wind_control_resp_log`;
CREATE TABLE `t_wind_control_resp_log` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `req_id`                                    int(11) NOT NULL COMMENT '请求Id',
  `project_id`                                varchar(32) NOT NULL COMMENT '项目编号',
  `apply_no`                                  varchar(32) NOT NULL COMMENT '申请编号(借据号)',
  `creid_no`                                  varchar(100) DEFAULT NULL COMMENT '身份证号',
  `name`                                      varchar(300) DEFAULT NULL COMMENT '姓名',
  `mobile_phone`                              varchar(100) DEFAULT NULL COMMENT '手机号',
  `credit_line`                               int(255) DEFAULT NULL COMMENT '申请额度  以分为单位，取值范围（0,1000000000）',
  `ret_code`                                  varchar(10) NOT NULL COMMENT '应答码',
  `ret_msg`                                   varchar(100) NOT NULL COMMENT '应答信息',
  `status`                                    varchar(10) DEFAULT NULL COMMENT '查询结果状态 0:未查得,1:查得',
  `rating_result`                             varchar(10) DEFAULT NULL COMMENT '风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Error:风控异常,Fail:调用风控失败)',
  `cheat_level`                               varchar(10) DEFAULT NULL COMMENT '反欺诈等级:1~5,-1:null,其它:异常值',
  `score_range`                               varchar(10) DEFAULT NULL COMMENT '评分等级:1~20,-1:null,其它:异常值',
  `score_level`                               varchar(10) DEFAULT NULL COMMENT '评分区间:1~5,-1:null,其它:异常值',
  `ret_content`                               text COMMENT '风控结果报文',
  `wind_moment`                               tinyint(10) DEFAULT NULL COMMENT '风控时刻（0：入池时，1：资产跟踪)',
  `wind_interface_type`                       tinyint(10) DEFAULT NULL COMMENT '查询的风控接口类型(1:风控审核4号接口,2:风控结果查询新分享本地接口,3:风控结果查询5号接口,4:风控查询6号接口)',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `IX_WIND_RESP_PROJECTID` (`project_id`,`apply_no`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=956408 DEFAULT CHARSET=utf8 COMMENT='风控结果日志';

-- ----------------------------
-- Table structure for t_wind_control_tem
-- ----------------------------
-- DROP TABLE IF EXISTS `t_wind_control_tem`;
CREATE TABLE `t_wind_control_tem` (
  `id`                                        int(11) NOT NULL AUTO_INCREMENT,
  `apply_no`                                  varchar(32) NOT NULL COMMENT '申请编号(借据号)',
  `ret_msg`                                   varchar(32) DEFAULT NULL COMMENT '返回信息',
  `ret_code`                                  varchar(32) DEFAULT NULL COMMENT '返回码',
  `rsp_data`                                  varchar(500) DEFAULT NULL COMMENT '数据',
  `create_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`                               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_apply_no` (`apply_no`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=82 DEFAULT CHARSET=utf8 COMMENT='风控结果临时表';

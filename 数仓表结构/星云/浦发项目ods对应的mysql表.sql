select a.* from
(select *,'${d_date}' as D_DATE,'${partition}' as P_TYPE from ${table_name} )a
left join
(select *
from ecas_loan
where product_code in(${product_list}))b
on a.due_bill_no=b.due_bill_no
where b.due_bill_no is not null and \$CONDITIONS

hadoop fs -get /user/admin/sqoop/data_syn/${product} .
while read line;do var="$var,'$line'";done < ${product}

--t_loan_contract_info;   58585
--t_principal_borrower_info;   58188
--t_contact_person_info;   5432
--t_guaranty_info;   45224
--t_repayment_schedule;  1650799
--t_asset_pay_flow;   514256
--t_repayment_info;   827915
--t_repayment_info_fix;  61246
--t_asset_disposal;    11
--t_asset_supplement;  728
--t_asset_check;   34734290
--t_project;  32
--t_guaranty_car; 46450
--t_ods_credit;  93851
--t_credit_loan; 14143

CREATE TABLE `t_loan_contract_info` (
  `id` int(32) NOT NULL AUTO_INCREMENT COMMENT '贷款合同信息主键',
  `import_id` varchar(64) NOT NULL COMMENT '数据批次号-由接入系统生产',
  `project_id` varchar(32) NOT NULL COMMENT '项目id',
  `agency_id` varchar(16) NOT NULL COMMENT '机构编号',
  `asset_id` varchar(64) NOT NULL COMMENT '资产借据号',
  `contract_code` varchar(64) NOT NULL COMMENT '贷款合同编号',
  `loan_total_amount` decimal(16,2) NOT NULL COMMENT '贷款总金额',
  `periods` int(4) NOT NULL COMMENT '总期数',
  `repay_type` varchar(32) DEFAULT NULL COMMENT '还款方式,0-等额本息,1-等额本金,2-等本等息3-先息后本4-一次性还本付息,5-气球贷,6-自定义还款计划',
  `interest_rate_type` varchar(32) DEFAULT NULL COMMENT '0-固定利率,1-浮动利率',
  `loan_interest_rate` decimal(8,6) DEFAULT NULL COMMENT '贷款年利率，单位:%',
  `contract_data_status` int(1) DEFAULT '1' COMMENT '合同数据与还款计划中的总额是否一致，0-一致，1-不一致',
  `contract_status` varchar(8) NOT NULL COMMENT '0-生效,1-不生效',
  `first_repay_date` varchar(16) DEFAULT NULL COMMENT '首次还款日期，yyyy-MM-dd',
  `extra_info` json DEFAULT NULL COMMENT '扩展信息',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `verify_status` varchar(32) DEFAULT '' COMMENT '数据校验状态',
  `verify_mark` varchar(1) DEFAULT '' COMMENT '校验标志',
  `first_loan_end_date` date DEFAULT NULL COMMENT '合同结束时间',
  `per_loan_end_date` date DEFAULT NULL COMMENT '上一次合同结束时间',
  `cur_loan_end_date` date DEFAULT NULL COMMENT '当前合同结束时间',
  `loan_begin_date` date DEFAULT NULL COMMENT '合同开始时间',
  `abs_push_flag` varchar(16) NOT NULL DEFAULT 'NOPUSH' COMMENT 'abs推送标志',
  `repay_frequency` varchar(32) DEFAULT NULL COMMENT '还款频率',
  `nominal_rate` decimal(16,4) DEFAULT '0.0000' COMMENT '名义费率',
  `daily_penalty_rate` decimal(16,4) DEFAULT '0.0000' COMMENT '日罚息率',
  `loan_use` varchar(32) DEFAULT NULL COMMENT '贷款用途',
  `guarantee_type` varchar(32) DEFAULT NULL COMMENT '担保方式',
  `loan_type` varchar(32) DEFAULT NULL COMMENT '借款方类型',
  `loan_total_interest` decimal(16,2) DEFAULT NULL COMMENT '贷款总利息',
  `loan_total_fee` decimal(16,2) DEFAULT NULL COMMENT '贷款总费用',
  `loan_penalty_rate` decimal(8,6) DEFAULT NULL COMMENT '贷款罚息利率',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `idx_pro_asset` (`project_id`,`asset_id`) USING BTREE,
  KEY `idx_import_id` (`import_id`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=62080 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='贷款合同信息表';


CREATE TABLE `t_principal_borrower_info` (
  `id` int(32) NOT NULL AUTO_INCREMENT COMMENT '主借款人信息主键',
  `project_id` varchar(32) NOT NULL COMMENT '项目id',
  `agency_id` varchar(16) NOT NULL COMMENT '机构编号',
  `asset_id` varchar(64) NOT NULL COMMENT '资产借据号',
  `customer_name` varchar(256) NOT NULL COMMENT '客户姓名',
  `document_num` varchar(256) NOT NULL COMMENT '身份证号码',
  `phone_num` varchar(256) NOT NULL COMMENT '手机号码',
  `age` int(4) NOT NULL DEFAULT '0' COMMENT '年龄',
  `sex` varchar(16) NOT NULL COMMENT '性别，0-男，2-女',
  `marital_status` varchar(16) NOT NULL DEFAULT '0' COMMENT '婚姻状态,0-已婚，1-未婚，2-离异，3-丧偶',
  `degree` varchar(16) NOT NULL COMMENT '学位，0-小学，1-初中，2-高中/职高/技校，3-大专，4-本科,5-硕士,6-博士，7-文盲和半文盲',
  `province` varchar(128) NOT NULL COMMENT '客户所在省',
  `city` varchar(128) NOT NULL COMMENT '客户所在市',
  `address` varchar(256) NOT NULL COMMENT '客户所在地区',
  `extra_info` json DEFAULT NULL COMMENT '扩展信息',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `ecif_no` varchar(32) DEFAULT NULL COMMENT 'ecifNo',
  `card_no` varchar(256) DEFAULT NULL COMMENT '身份证号码(脱敏处理)',
  `phone_no` varchar(256) DEFAULT NULL COMMENT '手机号码(脱敏处理)',
  `imei` varchar(32) DEFAULT NULL COMMENT 'imei号',
  `education` varchar(16) DEFAULT NULL COMMENT '学位',
  `annual_income` decimal(16,2) DEFAULT NULL COMMENT '年收入(元)',
  `processed` int(1) DEFAULT NULL COMMENT '是否已处理',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE,
  KEY `idx_pro_asset` (`asset_id`,`project_id`) USING BTREE,
  KEY `idx_asset_idnum` (`asset_id`,`document_num`(255)) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=59012 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='主借款人信息表';



CREATE TABLE `t_contact_person_info` (
  `id` int(32) NOT NULL AUTO_INCREMENT COMMENT '主借款人信息主键',
  `project_id` varchar(32) NOT NULL COMMENT '项目id',
  `agency_id` varchar(16) NOT NULL COMMENT '机构编号',
  `asset_id` varchar(64) NOT NULL COMMENT '资产借据号',
  `customer_name` varchar(256) NOT NULL COMMENT '客户姓名',
  `document_num` varchar(256) NOT NULL COMMENT '身份证号码',
  `phone_num` varchar(256) NOT NULL COMMENT '手机号码',
  `age` int(4) NOT NULL DEFAULT '0' COMMENT '年龄',
  `sex` varchar(1) NOT NULL COMMENT '性别，0-男，2-女',
  `mainborrower_relationship` varchar(16) NOT NULL COMMENT '与主借款人的关系，0-配偶,1-父母,2-子女,3-亲戚,4-朋友,5-同事',
  `occupation` varchar(32) NOT NULL DEFAULT '' COMMENT '职业',
  `work_status` varchar(32) NOT NULL DEFAULT '0' COMMENT '工作状态，0-在职，1-失业',
  `annual_income` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '年收入',
  `communication_address` varchar(128) NOT NULL DEFAULT '' COMMENT '通讯地址',
  `unit_address` varchar(128) NOT NULL DEFAULT '' COMMENT '单位地址',
  `unit_phone_number` varchar(16) NOT NULL DEFAULT '' COMMENT '单位联系方式',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `ecif_no` varchar(32) DEFAULT NULL COMMENT 'ecifNo',
  `card_no` varchar(256) DEFAULT NULL COMMENT '身份证号码(脱敏处理)',
  `phone_no` varchar(256) DEFAULT NULL COMMENT '手机号码(脱敏处理)',
  `imei` varchar(32) DEFAULT NULL COMMENT 'imei号',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE,
  KEY `idx_pro_asset` (`asset_id`,`project_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=5433 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='主借款人联系人信息表'




CREATE TABLE `t_guaranty_info` (
  `id` int(32) NOT NULL AUTO_INCREMENT COMMENT '抵押物信息主键',
  `project_id` varchar(32) NOT NULL COMMENT '项目id',
  `agency_id` varchar(16) NOT NULL COMMENT '机构编号',
  `asset_id` varchar(64) NOT NULL COMMENT '资产借据号',
  `guaranty_type` varchar(255) DEFAULT NULL COMMENT '抵押物类型',
  `guaranty_umber` varchar(128) DEFAULT NULL COMMENT '抵押物编号',
  `mortgage_handle_status` varchar(128) NOT NULL COMMENT '抵押办理状态，0-办理中，1-办理完成，2-尚未办理',
  `mortgage_alignment` varchar(128) NOT NULL COMMENT '抵押顺位0-第一顺位,2-第二顺位,3-其他',
  `extra_info` json DEFAULT NULL COMMENT '扩展信息',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `processed` int(1) DEFAULT NULL COMMENT '是否已处理',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE,
  KEY `idx_pro_asset` (`asset_id`,`project_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=45225 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='抵押物信息表'




CREATE TABLE `t_repayment_schedule` (
  `id` int(32) NOT NULL AUTO_INCREMENT COMMENT '实际还款信息表主键',
  `import_id` varchar(64) NOT NULL COMMENT '数据批次号-由接入系统生产',
  `project_id` varchar(32) NOT NULL COMMENT '项目id',
  `agency_id` varchar(16) NOT NULL COMMENT '机构编号',
  `asset_id` varchar(64) NOT NULL COMMENT '资产借据号',
  `period` int(4) NOT NULL COMMENT '期次',
  `repay_date` date NOT NULL COMMENT '应还款日期',
  `repay_principal` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '应还本金(元)',
  `repay_interest` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '应还利息(元)',
  `repay_fee` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '应还费用(元)',
  `begin_loan_principal` decimal(16,2) DEFAULT NULL COMMENT '期初剩余本金',
  `end_loan_principal` decimal(16,2) DEFAULT NULL COMMENT '期末剩余本金',
  `execute_date` date NOT NULL COMMENT '生效日期',
  `timestamp` timestamp(3) NOT NULL DEFAULT '0000-00-00 00:00:00.000' ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '时间戳信息，区分还款计划的批次问题',
  `extra_info` json DEFAULT NULL COMMENT '扩展信息',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `begin_loan_interest` decimal(16,2) DEFAULT NULL COMMENT '期初剩余利息',
  `end_loan_interest` decimal(16,2) DEFAULT NULL COMMENT '期末剩余利息',
  `begin_loan_fee` decimal(16,2) DEFAULT NULL COMMENT '期初剩余费用',
  `end_loan_fee` decimal(16,2) DEFAULT NULL COMMENT '期末剩余费用',
  `remainder_periods` decimal(16,2) DEFAULT NULL COMMENT '剩余期数',
  `next_repay_date` date DEFAULT NULL COMMENT '下次应还日期',
  `repay_penalty` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '应还罚息',
  PRIMARY KEY (`id`,`asset_id`,`period`,`project_id`) USING BTREE,
  UNIQUE KEY `idx_pro_asset` (`project_id`,`asset_id`,`period`) USING BTREE,
  KEY `idx_asset_date` (`project_id`,`asset_id`,`repay_date`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=4968197 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='还款计划信息表'


CREATE TABLE `t_asset_pay_flow` (
  `id` int(32) NOT NULL AUTO_INCREMENT COMMENT '抵押物信息主键',
  `project_id` varchar(32) NOT NULL COMMENT '项目id',
  `agency_id` varchar(16) NOT NULL COMMENT '机构编号',
  `asset_id` varchar(64) NOT NULL COMMENT '资产借据号',
  `trade_channel` varchar(128) NOT NULL COMMENT '交易渠道',
  `trade_type` varchar(64) NOT NULL COMMENT '0-放款,1-代扣,2-主动还款,3-代偿,4-回购,5-差额补足,6-处置回收',
  `order_id` varchar(128) NOT NULL COMMENT '订单id',
  `order_amount` decimal(16,2) NOT NULL COMMENT '订单金额',
  `trade_currency` varchar(4) NOT NULL DEFAULT 'RMB' COMMENT '交易币种，默认人民币',
  `name` varchar(256) NOT NULL COMMENT '姓名-银行户名',
  `bank_account` varchar(128) NOT NULL COMMENT '银行账号',
  `trade_time` varchar(16) NOT NULL DEFAULT '0000-00-00' COMMENT '交易时间',
  `trade_status` varchar(16) NOT NULL DEFAULT '0' COMMENT '交易状态，0-成功，1-失败',
  `extra_info` json DEFAULT NULL COMMENT '扩展字段',
  `trad_desc` varchar(512) DEFAULT '' COMMENT '交易摘要',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`,`order_id`,`asset_id`,`project_id`) USING BTREE,
  KEY `idx_order_id` (`order_id`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=540287 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='资产支付流水信息表'


CREATE TABLE `t_repayment_info` (
  `id` int(32) NOT NULL AUTO_INCREMENT COMMENT '实际还款信息表主键',
  `import_id` varchar(64) NOT NULL COMMENT '数据批次号-由接入系统生产',
  `project_id` varchar(32) NOT NULL COMMENT '项目id',
  `agency_id` varchar(16) NOT NULL COMMENT '机构编号',
  `asset_id` varchar(64) NOT NULL COMMENT '资产借据号',
  `repay_date` date NOT NULL COMMENT '应还款日期',
  `repay_principal` decimal(16,2) DEFAULT NULL,
  `repay_interest` decimal(16,2) DEFAULT NULL,
  `repay_fee` decimal(16,2) DEFAULT NULL,
  `rel_pay_date` date NOT NULL COMMENT '实际还清日期',
  `rel_principal` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '实还本金',
  `rel_interest` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '实还利息',
  `rel_fee` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '实还费用',
  `period` int(4) NOT NULL COMMENT '期次',
  `timestamp` timestamp(3) NULL DEFAULT NULL,
  `extra_info` json DEFAULT NULL COMMENT '扩展信息',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `free_amount` decimal(16,2) DEFAULT '0.00' COMMENT '免息金额',
  `remainder_principal` decimal(16,2) DEFAULT NULL COMMENT '剩余本金',
  `remainder_interest` decimal(16,2) DEFAULT NULL COMMENT '剩余利息',
  `remainder_fee` decimal(16,2) DEFAULT NULL COMMENT '剩余费用',
  `remainder_periods` int(4) DEFAULT NULL COMMENT '剩余期数',
  `repay_type` varchar(32) DEFAULT NULL COMMENT '还款类型',
  `current_loan_balance` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '当期贷款余额',
  `account_status` varchar(64) DEFAULT '' COMMENT '当期账户状态',
  `current_status` varchar(64) DEFAULT NULL COMMENT '当期状态',
  `overdue_day` int(4) DEFAULT '0' COMMENT '逾期天数',
  `finish_periods` int(64) DEFAULT '0' COMMENT '已还期数',
  `plan_begin_loan_principal` decimal(16,2) DEFAULT '0.00' COMMENT '期初剩余本金',
  `plan_end_loan_principal` decimal(16,2) DEFAULT '0.00' COMMENT '期末剩余本金',
  `plan_begin_loan_interest` decimal(16,2) DEFAULT '0.00' COMMENT '期初剩余利息',
  `plan_end_loan_interest` decimal(16,2) DEFAULT '0.00' COMMENT '期末剩余利息',
  `plan_begin_loan_fee` decimal(16,2) DEFAULT '0.00' COMMENT '期初剩余费用',
  `plan_end_loan_fee` decimal(16,2) DEFAULT '0.00' COMMENT '期末剩余费用',
  `plan_remainder_periods` int(14) DEFAULT '0' COMMENT '剩余期数',
  `plan_next_repay_date` date DEFAULT NULL COMMENT '下次应还日期',
  `real_interest_rate` decimal(16,2) DEFAULT '0.00' COMMENT '实际执行利率',
  `repay_penalty` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '应还罚息',
  `rel_penalty` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '实还罚息',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `idx_pro_asset_repay_date` (`project_id`,`asset_id`,`repay_date`,`period`,`rel_pay_date`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE,
  KEY `idx_pro_asset` (`project_id`,`asset_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1074367 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='实际还款信息表'


CREATE TABLE `t_repayment_info_fix` (
  `id` int(32) NOT NULL AUTO_INCREMENT COMMENT '实际还款信息表主键',
  `import_id` varchar(64) NOT NULL COMMENT '数据批次号-由接入系统生产',
  `project_id` varchar(32) NOT NULL COMMENT '项目id',
  `agency_id` varchar(16) NOT NULL COMMENT '机构编号',
  `asset_id` varchar(64) NOT NULL COMMENT '资产借据号',
  `repay_date` date NOT NULL COMMENT '应还款日期',
  `repay_principal` decimal(16,2) DEFAULT NULL,
  `repay_interest` decimal(16,2) DEFAULT NULL,
  `repay_fee` decimal(16,2) DEFAULT NULL,
  `rel_pay_date` date NOT NULL COMMENT '实际还清日期',
  `rel_principal` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '实还本金',
  `rel_interest` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '实还利息',
  `rel_fee` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '实还费用',
  `period` int(4) NOT NULL COMMENT '期次',
  `timestamp` timestamp(3) NULL DEFAULT NULL,
  `extra_info` json DEFAULT NULL COMMENT '扩展信息',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `free_amount` decimal(16,2) DEFAULT '0.00' COMMENT '免息金额',
  `remainder_principal` decimal(16,2) DEFAULT NULL COMMENT '剩余本金',
  `remainder_interest` decimal(16,2) DEFAULT NULL COMMENT '剩余利息',
  `remainder_fee` decimal(16,2) DEFAULT NULL COMMENT '剩余费用',
  `remainder_periods` int(4) DEFAULT NULL COMMENT '剩余期数',
  `repay_type` varchar(32) DEFAULT NULL COMMENT '还款类型',
  `current_loan_balance` decimal(16,2) DEFAULT NULL COMMENT '当期贷款余额',
  `account_status` varchar(64) DEFAULT '' COMMENT '当期账户状态',
  `current_status` varchar(64) DEFAULT NULL COMMENT '当期状态',
  `overdue_day` int(4) DEFAULT '0' COMMENT '逾期天数',
  `finish_periods` int(64) DEFAULT '0' COMMENT '已还期数',
  `plan_begin_loan_principal` decimal(16,2) DEFAULT '0.00' COMMENT '期初剩余本金',
  `plan_end_loan_principal` decimal(16,2) DEFAULT '0.00' COMMENT '期末剩余本金',
  `plan_begin_loan_interest` decimal(16,2) DEFAULT '0.00' COMMENT '期初剩余利息',
  `plan_end_loan_interest` decimal(16,2) DEFAULT '0.00' COMMENT '期末剩余利息',
  `plan_begin_loan_fee` decimal(16,2) DEFAULT '0.00' COMMENT '期初剩余费用',
  `plan_end_loan_fee` decimal(16,2) DEFAULT '0.00' COMMENT '期末剩余费用',
  `plan_remainder_periods` int(14) DEFAULT '0' COMMENT '剩余期数',
  `plan_next_repay_date` date DEFAULT NULL COMMENT '下次应还日期',
  `real_interest_rate` decimal(16,2) DEFAULT '0.00' COMMENT '实际执行利率',
  `repay_penalty` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '应还罚息',
  `rel_penalty` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '实还罚息',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `idx_pro_asset_repay_date` (`project_id`,`asset_id`,`repay_date`,`period`,`rel_pay_date`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE,
  KEY `idx_pro_asset` (`project_id`,`asset_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=583500 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='实际还款信息表'




CREATE TABLE `t_asset_disposal` (
  `id` int(32) NOT NULL AUTO_INCREMENT COMMENT '抵押物信息主键',
  `project_id` varchar(32) NOT NULL COMMENT '项目id',
  `agency_id` varchar(16) NOT NULL COMMENT '机构编号',
  `asset_id` varchar(64) NOT NULL COMMENT '资产借据号',
  `disposi_status` varchar(64) NOT NULL DEFAULT '1' COMMENT '处置状态,0-已处置，1-未处置',
  `disposi_type` varchar(64) DEFAULT NULL COMMENT '处置类型，0-诉讼，1-非诉讼',
  `litigate_node` varchar(64) DEFAULT NULL COMMENT '诉讼节点，0-处置开始，1-诉讼准备，2-法院受理，3-执行拍卖，4-处置结束',
  `litigate_node_time` varchar(16) DEFAULT '0000-00-00' COMMENT '诉讼节点时间',
  `disposi_esult` varchar(64) DEFAULT NULL COMMENT '处置结果,0-经处置无拖欠,1-经处置已结清,2-经处置已核销',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='资产处置过程信息表'



CREATE TABLE `t_asset_supplement` (
  `id` int(32) NOT NULL AUTO_INCREMENT COMMENT '抵押物信息主键',
  `project_id` varchar(32) NOT NULL COMMENT '项目id',
  `agency_id` varchar(16) NOT NULL COMMENT '机构编号',
  `asset_id` varchar(64) NOT NULL COMMENT '资产借据号',
  `tradeType` varchar(16) NOT NULL COMMENT '交易类型,0-回购,1-处置回收,2-代偿,3-差额补足',
  `tradeReason` varchar(128) DEFAULT NULL COMMENT '交易原因',
  `tradeDate` varchar(16) NOT NULL DEFAULT '0000-00-00' COMMENT '交易日期',
  `trade_tol_amounts` decimal(16,2) NOT NULL COMMENT '交易总金额',
  `principal` decimal(16,2) NOT NULL COMMENT '本金',
  `interest` decimal(16,2) NOT NULL COMMENT '利息',
  `punish_interest` decimal(16,2) NOT NULL COMMENT '罚息',
  `oth_fee` decimal(16,2) NOT NULL COMMENT '其他费用',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=732 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='资产补充交易信息表'


CREATE TABLE `t_asset_check` (
  `id` int(32) NOT NULL AUTO_INCREMENT COMMENT '资产对账信息主键',
  `project_id` varchar(32) NOT NULL COMMENT '项目id',
  `agency_id` varchar(16) NOT NULL COMMENT '机构编号',
  `asset_id` varchar(64) NOT NULL COMMENT '资产借据号',
  `account_date` date DEFAULT NULL COMMENT '记账时间',
  `repayedPeriod` int(4) DEFAULT NULL COMMENT '已还期数',
  `remain_period` int(4) DEFAULT NULL COMMENT '剩余期数',
  `remain_principal` decimal(16,2) DEFAULT '0.00' COMMENT '剩余本金(元)',
  `remain_interest` decimal(16,2) DEFAULT '0.00' COMMENT '剩余利息(元)',
  `remain_othAmounts` decimal(16,2) DEFAULT '0.00' COMMENT '剩余其他费用(元)',
  `next_pay_date` date DEFAULT NULL COMMENT '下一个还款日期',
  `assets_status` varchar(16) DEFAULT NULL COMMENT '资产状态，0-正常，1-逾期，2-已结清',
  `settle_reason` varchar(16) DEFAULT NULL COMMENT '结清原因,0-正常结清,1-提前结清,2-处置结束,3-资产回购',
  `current_overdue_principal` decimal(16,2) DEFAULT '0.00' COMMENT '当前逾期本金',
  `current_overdue_interest` decimal(16,2) DEFAULT '0.00' COMMENT '当前逾期利息',
  `current_overdue_fee` decimal(16,2) DEFAULT '0.00' COMMENT '当前逾期费用',
  `current_overdue_days` int(4) DEFAULT '0' COMMENT '当前逾期天数(天)',
  `accum_overdue_days` int(4) DEFAULT '0' COMMENT '累计逾期天数',
  `his_accum_overdue_days` int(4) DEFAULT '0' COMMENT '历史累计逾期天数',
  `his_overdue_mdays` int(4) DEFAULT '0' COMMENT '历史单次最长逾期天数',
  `current_overdue_period` int(4) DEFAULT '0' COMMENT '当前逾期期数',
  `accum_overdue_period` int(4) DEFAULT '0' COMMENT '累计逾期期数',
  `his_overdue_mperiods` int(4) DEFAULT '0' COMMENT '历史单次最长逾期期数',
  `his_overdue_mprincipal` decimal(16,2) DEFAULT '0.00' COMMENT '历史最大逾期本金',
  `extra_info` json DEFAULT NULL COMMENT '扩展信息',
  `remark` varchar(64) DEFAULT '' COMMENT '备注信息',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `loan_total_amount` decimal(16,2) DEFAULT '0.00' COMMENT '贷款总金额',
  `loan_interest_rate` decimal(16,2) DEFAULT '0.00' COMMENT '贷款年利率(%)',
  `periods` int(4) DEFAULT '0' COMMENT '总期数',
  `recovered_total_amount` decimal(16,2) DEFAULT '0.00' COMMENT '实还当天回收款总金额',
  `rel_principal` decimal(16,2) DEFAULT '0.00' COMMENT '实还本金',
  `rel_interest` decimal(16,2) DEFAULT '0.00' COMMENT '实还利息',
  `rel_fee` decimal(16,2) DEFAULT '0.00' COMMENT '实还费用',
  `current_continuity_overdays` int(4) DEFAULT '0' COMMENT '当前连续逾期天数',
  `max_single_overduedays` int(4) DEFAULT '0' COMMENT '最长单期逾期天数',
  `max_continuity_overdays` int(4) DEFAULT '0' COMMENT '最长连续逾期天数',
  `accum_overdue_principal` decimal(16,2) DEFAULT '0.00' COMMENT '历史累计逾期本金',
  `remain_penalty` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '剩余罚息',
  `loan_settlement_date` date DEFAULT NULL COMMENT '结清日期',
  `loss_principal` decimal(16,2) DEFAULT NULL COMMENT '损失本金',
  `total_rel_amount` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '累计实还金额',
  `total_rel_principal` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '累计实还本金',
  `total_rel_interest` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '累计实还利息',
  `total_rel_fee` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '累计实还费用',
  `total_rel_penalty` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '累计实还罚息',
  `rel_penalty` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '当天实还罚息',
  `curr_period` int(4) DEFAULT NULL COMMENT '当前期次',
  `first_term_overdue` varchar(16) NOT NULL DEFAULT '' COMMENT '首期是否逾期',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE,
  KEY `idx_pro_asset` (`asset_id`,`project_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=63293914 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='资产对账信息表'


CREATE TABLE `t_project` (
  `id` int(32) NOT NULL AUTO_INCREMENT COMMENT '主键id,自增',
  `project_id` varchar(32) NOT NULL COMMENT '项目编号',
  `project_name` varchar(64) DEFAULT NULL COMMENT '项目名称',
  `project_type` varchar(32) DEFAULT NULL COMMENT '项目类型',
  `chain_id` varchar(64) DEFAULT NULL COMMENT '链id',
  `node_id` varchar(256) DEFAULT NULL COMMENT '节点Id',
  `append_url` varchar(255) DEFAULT NULL COMMENT '新增数据地址',
  `query_url` varchar(255) DEFAULT NULL COMMENT '查询地址',
  `tx_url` varchar(255) DEFAULT NULL COMMENT '根据高度获取数据',
  `private_key` varchar(64) DEFAULT NULL COMMENT '密钥',
  `mch_id` varchar(64) DEFAULT NULL COMMENT '机构id',
  `version` varchar(4) DEFAULT NULL COMMENT '版本',
  `current_read_height` int(16) DEFAULT '0' COMMENT '当前读取区块高度',
  `max_height` int(16) DEFAULT '0' COMMENT '最大区块高度',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建记录时间',
  `modify_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `block_sign` varchar(32) DEFAULT NULL COMMENT '区块链标志',
  `agency_id` varchar(11) DEFAULT NULL COMMENT '关联机构id',
  `action_data` json DEFAULT NULL COMMENT '行为控制',
  `calculate_data` json DEFAULT NULL COMMENT '计算控制',
  `packet_date` date DEFAULT NULL COMMENT '项目封包日',
  `time_difference` varchar(4) DEFAULT '0' COMMENT '文件10计算时间差',
  `video_handle_startdate` date DEFAULT NULL COMMENT '影像文件处理开始日期',
  `video_handle_enddate` date DEFAULT NULL COMMENT '影像文件处理结束日期',
  `video_file_path` varchar(256) DEFAULT NULL COMMENT '影像文件路径',
  `grace_days` int(4) DEFAULT '0' COMMENT '豁免天数',
  `check_difference` int(4) DEFAULT '0',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `idx_project_id` (`project_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=36 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='项目表'


CREATE TABLE `t_guaranty_car` (
  `id` int(10) NOT NULL AUTO_INCREMENT COMMENT '主键id,自增',
  `bill_no` varchar(32) DEFAULT NULL COMMENT '借据编号',
  `frame_num` varchar(32) DEFAULT NULL COMMENT '车架号',
  `car_brand` varchar(32) DEFAULT NULL COMMENT '车辆品牌',
  `car_model` varchar(50) DEFAULT NULL COMMENT '车辆型号',
  `car_colour` varchar(10) DEFAULT NULL COMMENT '车辆颜色',
  `license_num` varchar(32) DEFAULT NULL COMMENT '车辆号码',
  `register_date` varchar(10) DEFAULT NULL COMMENT '注册日期',
  `pawn_value` decimal(15,4) DEFAULT '0.0000' COMMENT '评估价值',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `update_time` timestamp NULL DEFAULT NULL,
  `car_series` varchar(32) DEFAULT NULL COMMENT '车系',
  `drive_years` varchar(5) DEFAULT NULL COMMENT '车龄',
  `import_id` varchar(32) DEFAULT NULL COMMENT 'IMPORTID',
  `gps_code` varchar(32) DEFAULT NULL COMMENT 'GPS编号',
  `gps_fee` decimal(16,2) DEFAULT '0.00' COMMENT 'GPS费用',
  `timestamp` varchar(32) DEFAULT NULL COMMENT 'TIMESTAMP',
  `car_type` varchar(10) DEFAULT NULL COMMENT '车类型',
  `milage` varchar(10) DEFAULT NULL COMMENT '里程数',
  `insurance_type` varchar(32) DEFAULT NULL COMMENT '保险种类',
  `engine_num` varchar(32) DEFAULT NULL COMMENT '发动机号',
  `mortgage_order` varchar(10) DEFAULT NULL COMMENT '抵押顺位',
  `guarantee_method` varchar(10) DEFAULT NULL COMMENT '担保方式',
  `org_code` varchar(10) DEFAULT NULL COMMENT '机构编号',
  `product_date` varchar(10) DEFAULT NULL COMMENT '生产日期',
  `financing` varchar(10) DEFAULT NULL COMMENT '融资方式',
  `car_nature` varchar(10) DEFAULT NULL COMMENT '车辆性质',
  `project_num` varchar(20) DEFAULT NULL COMMENT '项目编号',
  `import_filetype` varchar(10) DEFAULT NULL COMMENT 'IMPORTFILETYPE',
  `mortage_num` varchar(32) DEFAULT NULL COMMENT '抵押物编号',
  `purchase_place` varchar(10) DEFAULT NULL COMMENT '车辆购买地',
  `fee_one` decimal(16,2) DEFAULT '0.00' COMMENT '责信保费用1',
  `fee_two` decimal(16,2) DEFAULT '0.00' COMMENT '责信保费用2',
  `total_investment` decimal(16,2) DEFAULT '0.00' COMMENT '投资总额(元)',
  `mortage_status` varchar(10) DEFAULT NULL COMMENT '抵押办理状态',
  `energy_type` varchar(10) DEFAULT NULL COMMENT '车辆能源类型',
  `formalities_fee` decimal(16,2) DEFAULT '0.00' COMMENT '手续总费用(元)',
  `guide_price` decimal(16,2) DEFAULT '0.00' COMMENT '新车指导价(元)',
  `purchase_tax` decimal(16,2) DEFAULT '0.00' COMMENT '购置税金额(元)',
  `insurance_fee` decimal(16,2) DEFAULT '0.00' COMMENT '汽车保险总费用',
  `sales_price` decimal(16,2) DEFAULT '0.00' COMMENT '车辆销售价格(元)',
  `total_trans_times` int(4) DEFAULT '0' COMMENT '累计车辆过户次数',
  `year_trans_times` int(4) DEFAULT '0' COMMENT '一年内车辆过户次数',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=58408 DEFAULT CHARSET=utf8


CREATE TABLE `t_ods_credit` (
  `credit_id` varchar(64) DEFAULT NULL,
  `assessment_date` date NOT NULL COMMENT '评估日期',
  `credit_result` varchar(32) NOT NULL COMMENT '授信结果',
  `credit_amount` decimal(16,2) DEFAULT '0.00' COMMENT '授信金额',
  `credit_validity` date DEFAULT NULL COMMENT '授信有效期',
  `refuse_reason` varchar(32) DEFAULT NULL COMMENT '授信拒绝原因',
  `current_remain_credit_amount` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '当前剩余额度',
  `current_credit_amount_utilization_rate` decimal(16,4) DEFAULT '0.0000' COMMENT '当前额度使用率',
  `accumulate_credit_amount_utilization_rate` decimal(16,4) DEFAULT '0.0000' COMMENT '累计额度使用率',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `project_id` varchar(32) DEFAULT NULL COMMENT '项目编号',
  `agency_id` varchar(32) DEFAULT NULL COMMENT '机构编号',
  `ecif_no` varchar(32) DEFAULT NULL COMMENT 'ecifNo',
  `card_no` varchar(256) DEFAULT NULL COMMENT '身份证号码(脱敏处理)',
  `phone_no` varchar(256) DEFAULT NULL COMMENT '手机号码(脱敏处理)',
  `imei` varchar(32) DEFAULT NULL COMMENT 'imei号',
  UNIQUE KEY `bill_no` (`credit_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8




CREATE TABLE `t_credit_loan` (
  `id` int(8) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `credit_id` varchar(64) NOT NULL COMMENT '授信编号',
  `loan_id` varchar(64) NOT NULL COMMENT '借据编号',
  `project_id` varchar(64) DEFAULT NULL COMMENT '项目编号',
  `agency_id` varchar(64) DEFAULT NULL COMMENT '机构编号',
  `assessment_date` date DEFAULT NULL COMMENT '评估日期',
  `asset_level` varchar(32) DEFAULT NULL COMMENT '资产等级',
  `credit_level` varchar(32) DEFAULT NULL COMMENT '信用等级',
  `anti_fraud_level` varchar(32) DEFAULT NULL COMMENT '反欺诈等级',
  `risk_control_result` varchar(32) DEFAULT NULL COMMENT '风控结果',
  `refuse_reason` varchar(32) DEFAULT NULL COMMENT '风控结果',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `product_terms` int(4) DEFAULT '0' COMMENT '产品总期数',
  `ecif_no` varchar(32) DEFAULT NULL COMMENT 'ecifNo',
  `card_no` varchar(256) DEFAULT NULL COMMENT '身份证号码(脱敏处理)',
  `phone_no` varchar(256) DEFAULT NULL COMMENT '手机号码(脱敏处理)',
  `imei` varchar(32) DEFAULT NULL COMMENT 'imei号',
  `apply_amt` decimal(16,2) DEFAULT NULL COMMENT '申请用信金额',
  `approval_amt` decimal(16,2) DEFAULT NULL COMMENT '用信审批通过金额',
  `first_use_credit` int(1) DEFAULT NULL COMMENT 'credit_id是否存在',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=14149 DEFAULT CHARSET=utf8 COMMENT='授信借据映射表'








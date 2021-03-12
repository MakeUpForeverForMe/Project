10.80.16.5:3306    sqoop_user  xy@Eh93AmnCkMbiU  库 asset_status
`t_loan_contract_info`      '文件01' '贷款合同信息表'
`t_principal_borrower_info` '文件02' '主借款人信息表'

`t_contact_person_info`     '文件03' '主借款人联系人信息表'
`t_guaranty_info`           '文件04' '抵押物信息表' -- 文件13也在这里，用类型区分  -- 有Excel导入
`t_enterprise_info`         '文件12' '企业名称信息表-文件十二'

`t_repayment_schedule`      '文件05' '还款计划信息表'  -- 有Excel导入
`t_asset_pay_flow`          '文件06' '资产支付流水信息表'
`t_repayment_info`          '文件07' '实际还款信息表'  -- 有Excel导入
`t_asset_check`             '文件10' '资产对账信息表'  -- 有Excel导入

`t_asset_disposal`          '文件08' '资产处置过程信息表'         -- 不需要
`t_asset_supplement`        '文件09' '资产补充交易信息表'         -- 不需要
`t_project_check`           '文件11' '项目对账信息表-文件十一'    -- 不需要
                            '文件13' '房抵押物信息表-文件十三'    -- 数据在文件4中，用类型区分

10.80.16.21:3306   sqoop_user  xy@Eh93AmnCkMbiU  库 abs-core
`t_repaymentplan_history`      '还款计划历史'
`t_asset_wind_control_history` '基础资产风控评分历史表'
`t_wind_control_resp_log`      '风控结果日志'


CREATE TABLE `t_loan_contract_info` (
  `id`                          int(32)       COMMENT '贷款合同信息主键',
  `import_id`                   varchar(64)   COMMENT '数据批次号-由接入系统生产',
  `project_id`                  varchar(32)   COMMENT '项目id',
  `agency_id`                   varchar(16)   COMMENT '机构编号',
  `asset_id`                    varchar(64)   COMMENT '资产借据号',
  `contract_code`               varchar(64)   COMMENT '贷款合同编号',
  `loan_total_amount`           decimal(16,2) COMMENT '贷款总金额',
  `periods`                     int(4)        COMMENT '总期数',
  `repay_type`                  varchar(32)   COMMENT '还款方式,0-等额本息,1-等额本金,2-等本等息3-先息后本4-一次性还本付息,5-气球贷,6-自定义还款计划',
  `interest_rate_type`          varchar(32)   COMMENT '0-固定利率,1-浮动利率',
  `loan_interest_rate`          decimal(8,6)  COMMENT '贷款年利率，单位:%',
  `contract_data_status`        int(1)        COMMENT '合同数据与还款计划中的总额是否一致，0-一致，1-不一致',
  `contract_status`             varchar(8)    COMMENT '0-生效,1-不生效',
  `first_repay_date`            varchar(16)   COMMENT '首次还款日期，yyyy-MM-dd',
  `extra_info`                  json          COMMENT '扩展信息',
  `create_time`                 timestamp     COMMENT '创建时间',
  `update_time`                 timestamp     COMMENT '更新时间',
  `verify_status`               varchar(32)   COMMENT '数据校验状态',
  `verify_mark`                 varchar(1)    COMMENT '校验标志',
  `first_loan_end_date`         date          COMMENT '合同结束时间',
  `per_loan_end_date`           date          COMMENT '上一次合同结束时间',
  `cur_loan_end_date`           date          COMMENT '当前合同结束时间',
  `loan_begin_date`             date          COMMENT '合同开始时间',
  `abs_push_flag`               varchar(16)   COMMENT 'abs推送标志',
  `repay_frequency`             varchar(32)   COMMENT '还款频率',
  `nominal_rate`                decimal(16,4) COMMENT '名义费率',
  `daily_penalty_rate`          decimal(16,4) COMMENT '日罚息率',
  `loan_use`                    varchar(32)   COMMENT '贷款用途',
  `guarantee_type`              varchar(32)   COMMENT '担保方式',
  `loan_type`                   varchar(32)   COMMENT '借款方类型',
  `loan_total_interest`         decimal(16,2) COMMENT '贷款总利息',
  `loan_total_fee`              decimal(16,2) COMMENT '贷款总费用',
  `loan_penalty_rate`           decimal(8,6)  COMMENT '贷款罚息利率',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `idx_pro_asset` (`project_id`,`asset_id`) USING BTREE,
  KEY `idx_import_id` (`import_id`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=62080 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='贷款合同信息表';


CREATE TABLE `t_principal_borrower_info` (
  `id`                          int(32)       COMMENT '主借款人信息主键',
  `project_id`                  varchar(32)   COMMENT '项目id',
  `agency_id`                   varchar(16)   COMMENT '机构编号',
  `asset_id`                    varchar(64)   COMMENT '资产借据号',
  `customer_name`               varchar(256)  COMMENT '客户姓名',
  `document_num`                varchar(256)  COMMENT '身份证号码',
  `phone_num`                   varchar(256)  COMMENT '手机号码',
  `age`                         int(4)        COMMENT '年龄',
  `sex`                         varchar(16)   COMMENT '性别，0-男，2-女',
  `marital_status`              varchar(16)   COMMENT '婚姻状态,0-已婚，1-未婚，2-离异，3-丧偶',
  `degree`                      varchar(16)   COMMENT '学位，0-小学，1-初中，2-高中/职高/技校，3-大专，4-本科,5-硕士,6-博士，7-文盲和半文盲',
  `province`                    varchar(128)  COMMENT '客户所在省',
  `city`                        varchar(128)  COMMENT '客户所在市',
  `address`                     varchar(256)  COMMENT '客户所在地区',
  `extra_info`                  json          COMMENT '扩展信息',
  `create_time`                 timestamp     COMMENT '创建时间',
  `update_time`                 timestamp     COMMENT '更新时间',
  `ecif_no`                     varchar(32)   COMMENT 'ecifNo',
  `card_no`                     varchar(256)  COMMENT '身份证号码(脱敏处理)',
  `phone_no`                    varchar(256)  COMMENT '手机号码(脱敏处理)',
  `imei`                        varchar(32)   COMMENT 'imei号',
  `education`                   varchar(16)   COMMENT '学位',
  `annual_income`               decimal(16,2) COMMENT '年收入(元)',
  `processed`                   int(1)        COMMENT '是否已处理',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE,
  KEY `idx_pro_asset` (`asset_id`,`project_id`) USING BTREE,
  KEY `idx_asset_idnum` (`asset_id`,`document_num`(255)) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=59012 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='主借款人信息表';


CREATE TABLE `t_contact_person_info` (
  `id`                          int(32)       COMMENT '主借款人信息主键',
  `project_id`                  varchar(32)   COMMENT '项目id',
  `agency_id`                   varchar(16)   COMMENT '机构编号',
  `asset_id`                    varchar(64)   COMMENT '资产借据号',
  `customer_name`               varchar(256)  COMMENT '客户姓名',
  `document_num`                varchar(256)  COMMENT '身份证号码',
  `phone_num`                   varchar(256)  COMMENT '手机号码',
  `age`                         int(4)        COMMENT '年龄',
  `sex`                         varchar(1)    COMMENT '性别，0-男，2-女',
  `mainborrower_relationship`   varchar(16)   COMMENT '与主借款人的关系，0-配偶,1-父母,2-子女,3-亲戚,4-朋友,5-同事',
  `occupation`                  varchar(32)   COMMENT '职业',
  `work_status`                 varchar(32)   COMMENT '工作状态，0-在职，1-失业',
  `annual_income`               decimal(16,2) COMMENT '年收入',
  `communication_address`       varchar(128)  COMMENT '通讯地址',
  `unit_address`                varchar(128)  COMMENT '单位地址',
  `unit_phone_number`           varchar(16)   COMMENT '单位联系方式',
  `create_time`                 timestamp     COMMENT '创建时间',
  `update_time`                 timestamp     COMMENT '更新时间',
  `ecif_no`                     varchar(32)   COMMENT 'ecifNo',
  `card_no`                     varchar(256)  COMMENT '身份证号码(脱敏处理)',
  `phone_no`                    varchar(256)  COMMENT '手机号码(脱敏处理)',
  `imei`                        varchar(32)   COMMENT 'imei号',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE,
  KEY `idx_pro_asset` (`asset_id`,`project_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=5433 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='主借款人联系人信息表'


CREATE TABLE `t_guaranty_info` (
  `id`                          int(32)       COMMENT '抵押物信息主键',
  `project_id`                  varchar(32)   COMMENT '项目id',
  `agency_id`                   varchar(16)   COMMENT '机构编号',
  `asset_id`                    varchar(64)   COMMENT '资产借据号',
  `guaranty_type`               varchar(255)  COMMENT '抵押物类型',
  `guaranty_umber`              varchar(128)  COMMENT '抵押物编号',
  `mortgage_handle_status`      varchar(128)  COMMENT '抵押办理状态，0-办理中，1-办理完成，2-尚未办理',
  `mortgage_alignment`          varchar(128)  COMMENT '抵押顺位0-第一顺位,2-第二顺位,3-其他',
  `extra_info`                  json          COMMENT '扩展信息',
  `create_time`                 timestamp     COMMENT '创建时间',
  `update_time`                 timestamp     COMMENT '更新时间',
  `processed`                   int(1)        COMMENT '是否已处理',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE,
  KEY `idx_pro_asset` (`asset_id`,`project_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=45225 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='抵押物信息表'


CREATE TABLE `t_repayment_schedule` (
  `id`                          int(32)       COMMENT '实际还款信息表主键',
  `project_id`                  varchar(32)   COMMENT '项目id',
  `agency_id`                   varchar(16)   COMMENT '机构编号',
  `asset_id`                    varchar(64)   COMMENT '资产借据号',
  `period`                      int(4)        COMMENT '期次',
  `repay_date`                  date          COMMENT '应还款日期',
  `repay_principal`             decimal(16,2) COMMENT '应还本金(元)',
  `repay_interest`              decimal(16,2) COMMENT '应还利息(元)',
  `repay_fee`                   decimal(16,2) COMMENT '应还费用(元)',
  `begin_loan_principal`        decimal(16,2) COMMENT '期初剩余本金',
  `end_loan_principal`          decimal(16,2) COMMENT '期末剩余本金',
  `execute_date`                date          COMMENT '生效日期',
  `timestamp`                   timestamp(3)  COMMENT '时间戳信息，区分还款计划的批次问题',
  `extra_info`                  json          COMMENT '扩展信息',
  `create_time`                 timestamp     COMMENT '创建时间',
  `update_time`                 timestamp     COMMENT '更新时间',
  `begin_loan_interest`         decimal(16,2) COMMENT '期初剩余利息',
  `end_loan_interest`           decimal(16,2) COMMENT '期末剩余利息',
  `begin_loan_fee`              decimal(16,2) COMMENT '期初剩余费用',
  `end_loan_fee`                decimal(16,2) COMMENT '期末剩余费用',
  `remainder_periods`           decimal(16,2) COMMENT '剩余期数',
  `next_repay_date`             date          COMMENT '下次应还日期',
  `repay_penalty`               decimal(16,2) COMMENT '应还罚息',
  `import_id`                   varchar(64)   COMMENT '数据批次号-由接入系统生产',
  PRIMARY KEY (`id`,`asset_id`,`period`,`project_id`) USING BTREE,
  UNIQUE KEY `idx_pro_asset` (`project_id`,`asset_id`,`period`) USING BTREE,
  KEY `idx_asset_date` (`project_id`,`asset_id`,`repay_date`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=4968197 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='还款计划信息表'


CREATE TABLE `t_asset_pay_flow` (
  `id`                          int(32)       COMMENT '抵押物信息主键',
  `project_id`                  varchar(32)   COMMENT '项目id',
  `agency_id`                   varchar(16)   COMMENT '机构编号',
  `asset_id`                    varchar(64)   COMMENT '资产借据号',
  `trade_channel`               varchar(128)  COMMENT '交易渠道',
  `trade_type`                  varchar(64)   COMMENT '0-放款,1-代扣,2-主动还款,3-代偿,4-回购,5-差额补足,6-处置回收',
  `order_id`                    varchar(128)  COMMENT '订单id',
  `order_amount`                decimal(16,2) COMMENT '订单金额',
  `trade_currency`              varchar(4)    COMMENT '交易币种，默认人民币',
  `name`                        varchar(256)  COMMENT '姓名-银行户名',
  `bank_account`                varchar(128)  COMMENT '银行账号',
  `trade_time`                  varchar(16)   COMMENT '交易时间',
  `trade_status`                varchar(16)   COMMENT '交易状态，0-成功，1-失败',
  `extra_info`                  json          COMMENT '扩展字段',
  `trad_desc`                   varchar(512)  COMMENT '交易摘要',
  `create_time`                 timestamp     COMMENT '创建时间',
  `update_time`                 timestamp     COMMENT '更新时间',
  PRIMARY KEY (`id`,`order_id`,`asset_id`,`project_id`) USING BTREE,
  KEY `idx_order_id` (`order_id`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=540287 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='资产支付流水信息表'


CREATE TABLE `t_repayment_info` (
  `id`                          int(32)       COMMENT '实际还款信息表主键',
  `import_id`                   varchar(64)   COMMENT '数据批次号-由接入系统生产',
  `project_id`                  varchar(32)   COMMENT '项目id',
  `agency_id`                   varchar(16)   COMMENT '机构编号',
  `asset_id`                    varchar(64)   COMMENT '资产借据号',
  `repay_date`                  date          COMMENT '应还款日期',
  `repay_principal`             decimal(16,2) COMMENT '应还款本金',
  `repay_interest`              decimal(16,2) COMMENT '应还款利息',
  `repay_fee`                   decimal(16,2) COMMENT '应还款费用',
  `rel_pay_date`                date          COMMENT '实际还清日期',
  `rel_principal`               decimal(16,2) COMMENT '实还本金',
  `rel_interest`                decimal(16,2) COMMENT '实还利息',
  `rel_fee`                     decimal(16,2) COMMENT '实还费用',
  `period`                      int(4)        COMMENT '期次',
  `timestamp`                   timestamp(3),
  `extra_info`                  json          COMMENT '扩展信息',
  `create_time`                 timestamp     COMMENT '创建时间',
  `update_time`                 timestamp     COMMENT '更新时间',
  `free_amount`                 decimal(16,2) COMMENT '免息金额',
  `remainder_principal`         decimal(16,2) COMMENT '剩余本金',
  `remainder_interest`          decimal(16,2) COMMENT '剩余利息',
  `remainder_fee`               decimal(16,2) COMMENT '剩余费用',
  `remainder_periods`           int(4)        COMMENT '剩余期数',
  `repay_type`                  varchar(32)   COMMENT '还款类型',
  `current_loan_balance`        decimal(16,2) COMMENT '当期贷款余额',
  `account_status`              varchar(64)   COMMENT '当期账户状态',
  `current_status`              varchar(64)   COMMENT '当期状态',
  `overdue_day`                 int(4)        COMMENT '逾期天数',
  `finish_periods`              int(64)       COMMENT '已还期数',
  `plan_begin_loan_principal`   decimal(16,2) COMMENT '期初剩余本金',
  `plan_end_loan_principal`     decimal(16,2) COMMENT '期末剩余本金',
  `plan_begin_loan_interest`    decimal(16,2) COMMENT '期初剩余利息',
  `plan_end_loan_interest`      decimal(16,2) COMMENT '期末剩余利息',
  `plan_begin_loan_fee`         decimal(16,2) COMMENT '期初剩余费用',
  `plan_end_loan_fee`           decimal(16,2) COMMENT '期末剩余费用',
  `plan_remainder_periods`      int(14)       COMMENT '剩余期数',
  `plan_next_repay_date`        date          COMMENT '下次应还日期',
  `real_interest_rate`          decimal(16,2) COMMENT '实际执行利率',
  `repay_penalty`               decimal(16,2) COMMENT '应还罚息',
  `rel_penalty`                 decimal(16,2) COMMENT '实还罚息',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `idx_pro_asset_repay_date` (`project_id`,`asset_id`,`repay_date`,`period`,`rel_pay_date`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE,
  KEY `idx_pro_asset` (`project_id`,`asset_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1074367 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='实际还款信息表'


CREATE TABLE `t_asset_disposal` (
  `id`                          int(32)       COMMENT '抵押物信息主键',
  `project_id`                  varchar(32)   COMMENT '项目id',
  `agency_id`                   varchar(16)   COMMENT '机构编号',
  `asset_id`                    varchar(64)   COMMENT '资产借据号',
  `disposi_status`              varchar(64)   COMMENT '处置状态,0-已处置，1-未处置',
  `disposi_type`                varchar(64)   COMMENT '处置类型，0-诉讼，1-非诉讼',
  `litigate_node`               varchar(64)   COMMENT '诉讼节点，0-处置开始，1-诉讼准备，2-法院受理，3-执行拍卖，4-处置结束',
  `litigate_node_time`          varchar(16)   COMMENT '诉讼节点时间',
  `disposi_esult`               varchar(64)   COMMENT '处置结果,0-经处置无拖欠,1-经处置已结清,2-经处置已核销',
  `create_time`                 timestamp     COMMENT '创建时间',
  `update_time`                 timestamp     COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='资产处置过程信息表'


CREATE TABLE `t_asset_supplement` (
  `id`                          int(32)       COMMENT '抵押物信息主键',
  `project_id`                  varchar(32)   COMMENT '项目id',
  `agency_id`                   varchar(16)   COMMENT '机构编号',
  `asset_id`                    varchar(64)   COMMENT '资产借据号',
  `tradeType`                   varchar(16)   COMMENT '交易类型,0-回购,1-处置回收,2-代偿,3-差额补足',
  `tradeReason`                 varchar(128)  COMMENT '交易原因',
  `tradeDate`                   varchar(16)   COMMENT '交易日期',
  `trade_tol_amounts`           decimal(16,2) COMMENT '交易总金额',
  `principal`                   decimal(16,2) COMMENT '本金',
  `interest`                    decimal(16,2) COMMENT '利息',
  `punish_interest`             decimal(16,2) COMMENT '罚息',
  `oth_fee`                     decimal(16,2) COMMENT '其他费用',
  `create_time`                 timestamp     COMMENT '创建时间',
  `update_time`                 timestamp     COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=732 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='资产补充交易信息表'


CREATE TABLE `t_asset_check` (
  `id`                          int(32)       COMMENT '资产对账信息主键',
  `project_id`                  varchar(32)   COMMENT '项目id',
  `agency_id`                   varchar(16)   COMMENT '机构编号',
  `asset_id`                    varchar(64)   COMMENT '资产借据号',
  `account_date`                date          COMMENT '记账时间',
  `repayedPeriod`               int(4)        COMMENT '已还期数',
  `remain_period`               int(4)        COMMENT '剩余期数',
  `remain_principal`            decimal(16,2) COMMENT '剩余本金(元)',
  `remain_interest`             decimal(16,2) COMMENT '剩余利息(元)',
  `remain_othAmounts`           decimal(16,2) COMMENT '剩余其他费用(元)',
  `next_pay_date`               date          COMMENT '下一个还款日期',
  `assets_status`               varchar(16)   COMMENT '资产状态，0-正常，1-逾期，2-已结清',
  `settle_reason`               varchar(16)   COMMENT '结清原因,0-正常结清,1-提前结清,2-处置结束,3-资产回购',
  `current_overdue_principal`   decimal(16,2) COMMENT '当前逾期本金',
  `current_overdue_interest`    decimal(16,2) COMMENT '当前逾期利息',
  `current_overdue_fee`         decimal(16,2) COMMENT '当前逾期费用',
  `current_overdue_days`        int(4)        COMMENT '当前逾期天数(天)',
  `accum_overdue_days`          int(4)        COMMENT '累计逾期天数',
  `his_accum_overdue_days`      int(4)        COMMENT '历史累计逾期天数',
  `his_overdue_mdays`           int(4)        COMMENT '历史单次最长逾期天数',
  `current_overdue_period`      int(4)        COMMENT '当前逾期期数',
  `accum_overdue_period`        int(4)        COMMENT '累计逾期期数',
  `his_overdue_mperiods`        int(4)        COMMENT '历史单次最长逾期期数',
  `his_overdue_mprincipal`      decimal(16,2) COMMENT '历史最大逾期本金',
  `extra_info`                  json          COMMENT '扩展信息',
  `remark`                      varchar(64)   COMMENT '备注信息',
  `create_time`                 timestamp     COMMENT '创建时间',
  `update_time`                 timestamp     COMMENT '更新时间',
  `loan_total_amount`           decimal(16,2) COMMENT '贷款总金额',
  `loan_interest_rate`          decimal(16,2) COMMENT '贷款年利率(%)',
  `periods`                     int(4)        COMMENT '总期数',
  `recovered_total_amount`      decimal(16,2) COMMENT '实还当天回收款总金额',
  `rel_principal`               decimal(16,2) COMMENT '实还本金',
  `rel_interest`                decimal(16,2) COMMENT '实还利息',
  `rel_fee`                     decimal(16,2) COMMENT '实还费用',
  `current_continuity_overdays` int(4)        COMMENT '当前连续逾期天数',
  `max_single_overduedays`      int(4)        COMMENT '最长单期逾期天数',
  `max_continuity_overdays`     int(4)        COMMENT '最长连续逾期天数',
  `accum_overdue_principal`     decimal(16,2) COMMENT '历史累计逾期本金',
  `remain_penalty`              decimal(16,2) COMMENT '剩余罚息',
  `loan_settlement_date`        date          COMMENT '结清日期',
  `loss_principal`              decimal(16,2) COMMENT '损失本金',
  `total_rel_amount`            decimal(16,2) COMMENT '累计实还金额',
  `total_rel_principal`         decimal(16,2) COMMENT '累计实还本金',
  `total_rel_interest`          decimal(16,2) COMMENT '累计实还利息',
  `total_rel_fee`               decimal(16,2) COMMENT '累计实还费用',
  `total_rel_penalty`           decimal(16,2) COMMENT '累计实还罚息',
  `rel_penalty`                 decimal(16,2) COMMENT '当天实还罚息',
  `curr_period`                 int(4)        COMMENT '当前期次',
  `first_term_overdue`          varchar(16)   COMMENT '首期是否逾期',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE,
  KEY `idx_pro_asset` (`asset_id`,`project_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=63293914 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='资产对账信息表'


CREATE TABLE `t_enterprise_info` (
  `id`                          int(32)      NOT     NULL AUTO_INCREMENT COMMENT '抵押物信息主键',
  `project_id`                  varchar(32)  NOT     NULL COMMENT '项目id',
  `agency_id`                   varchar(16)  NOT     NULL COMMENT '机构编号',
  `asset_id`                    varchar(64)  NOT     NULL COMMENT '资产借据号',
  `contract_role`               varchar(32)  DEFAULT '3'  COMMENT '合同角色 0-主借款企业，1-共同借款企业,2-担保企业,3-无',
  `enterprise_name`             varchar(128) DEFAULT ''   COMMENT '企业，名称',
  `business_number`             varchar(16)  DEFAULT NULL COMMENT '工商注册号',
  `organizate_code`             varchar(16)  DEFAULT NULL COMMENT '组织机构代码',
  `taxpayer_number`             varchar(32)  DEFAULT NULL COMMENT '纳税人识别号',
  `unified_credit_code`         varchar(32)  DEFAULT NULL COMMENT '统一信用代码',
  `registered_address`          varchar(256) DEFAULT NULL COMMENT '注册地址',
  `create_time`                 timestamp    NOT     NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                 timestamp    NOT     NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `loan_type`                   varchar(8)   DEFAULT NULL COMMENT '借款方类型',
  `industry`                    varchar(255) DEFAULT NULL COMMENT '企业行业',
  `legal_person_name`           varchar(32)  DEFAULT NULL COMMENT '法人姓名',
  `id_type`                     varchar(16)  DEFAULT NULL COMMENT '法人证件类型',
  `id_no`                       varchar(64)  DEFAULT NULL COMMENT '法人证件号',
  `legal_person_phone`          varchar(255) DEFAULT NULL COMMENT '法人手机号码',
  `phone`                       varchar(255) DEFAULT NULL COMMENT '企业联系电话',
  `operate_years`               int(4)       DEFAULT NULL COMMENT '企业运营年限',
  `is_linked`                   varchar(255) DEFAULT NULL COMMENT '是否挂靠企业',
  `province`                    varchar(255) DEFAULT NULL COMMENT '企业省份',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_asset_id` (`asset_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=689 DEFAULT CHARSET=utf8 COMMENT='企业信息表'

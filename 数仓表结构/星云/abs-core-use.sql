CREATE TABLE `t_loancontractinfo`             '借款合同信息表-文件一'
CREATE TABLE `t_borrowerinfo`                 '主借款人信息表-文件二'
CREATE TABLE `t_associatesinfo`               '关联人信息表-文件三'
CREATE TABLE `t_guarantycarinfo`              '抵押物(车)信息表-文件四'
CREATE TABLE `t_repaymentplan`                '还款计划表-文件五'
CREATE TABLE `t_repaymentplan_history`        '还款计划历史'
CREATE TABLE `t_assettradeflow`               '资产交易支付流水信息表-文件六'
CREATE TABLE `t_actualrepayinfo`              '实际还款信息表-文件七'
CREATE TABLE `t_assetdealprocessinfo`         '资产处置过程信息表-文件八'
CREATE TABLE `t_assetaddtradeinfo`            '资产补充交易信息表-文件九'
CREATE TABLE `t_assetaccountcheck`            '资产对账信息表-文件十'
CREATE TABLE `t_projectaccountcheck`          '项目对账信息表-文件十一'
CREATE TABLE `t_enterpriseinfo`               '企业名称信息表-文件十二'
CREATE TABLE `t_guarantyhouseinfo`            '房抵押物信息表-文件十三'

CREATE TABLE `t_basic_asset`                  '基础资产表'  --  提供视图

CREATE TABLE `t_asset_package_cash_flow`      '封包资产现金流' -- 封包时的表结构
CREATE TABLE `t_asset_package`                '封包时基础资产信息表'
CREATE TABLE `t_asset_package_profile`        '封包资产总览'
CREATE TABLE `t_asset_package_guaranty`       '封包时基础资产抵押物信息表'

CREATE TABLE `t_asset_bag`                    '资产包'          --  作为资产包的参考

CREATE TABLE `t_account_cash_flow`            '账户现金流'
CREATE TABLE `t_asset_cash_flow`              '资产端现金流'

CREATE TABLE `t_asset_wind_control_history`   '基础资产风控评分历史表'
CREATE TABLE `t_duration_risk_control_result` '存续风控结果记录表'  -- 无数据，暂时不添加
CREATE TABLE `t_wind_control_req_log`         '风控请求日志'
CREATE TABLE `t_wind_control_resp_log`        '风控结果日志'




-- ----------------------------
-- Table structure for t_loancontractinfo
-- ----------------------------
-- DROP TABLE IF EXISTS `t_loancontractinfo`;
CREATE TABLE `t_loancontractinfo` (
  `id`                                        int(32)            NOT NULL       COMMENT '借款合同信息主键',
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
  `create_time`                               timestamp                         COMMENT '创建时间',
  `update_time`                               timestamp                         COMMENT '更新时间',
  `import_id`                                 int(11)            DEFAULT NULL   COMMENT '导入Id',
  `data_source`                               tinyint(2)         DEFAULT NULL   COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `loancontractinfo_index` (`project_id`,`agency_id`,`serial_number`) USING BTREE,
  KEY `index2` (`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=99983721 DEFAULT CHARSET=utf8 COMMENT='借款合同信息表-文件一';

-- ----------------------------
-- Table structure for t_borrowerinfo
-- ----------------------------
-- DROP TABLE IF EXISTS `t_borrowerinfo`;
CREATE TABLE `t_borrowerinfo` (
  `id`                                        int(32)            NOT NULL       COMMENT '主借款人信息主键',
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `agency_id`                                 varchar(32)        DEFAULT NULL   COMMENT '机构编号',
  `serial_number`                             varchar(100)       DEFAULT NULL   COMMENT '借据号',
  `borrower_name`                             varchar(100)       DEFAULT NULL   COMMENT '客户姓名',
  `certificate_type`                          varchar(32)        DEFAULT NULL   COMMENT '证件类型 预定义字段：身份证 护照 户口本 外国人护照',
  `document_num`                              varchar(100)       DEFAULT NULL   COMMENT '身份证号',
  `phone_num`                                 varchar(32)        DEFAULT NULL   COMMENT '手机号',
  `age`                                       int(11)            DEFAULT NULL   COMMENT '年龄',
  `sex`                                       varchar(16)        DEFAULT NULL   COMMENT '性别-枚举 男，女',
  `marital_status`                            varchar(32)        DEFAULT NULL   COMMENT '婚姻状况-枚举 预定义字段：已婚，未婚，离异，丧偶',
  `children_number`                           int(4)             DEFAULT NULL   COMMENT '子女数量',
  `work_years`                                varchar(5)         DEFAULT NULL   COMMENT '工作年限',
  `customer_type`                             varchar(32)        DEFAULT NULL   COMMENT '客户类型-枚举 "01-农户 02-工薪 03-个体工商户 04-学生 99-其他"',
  `education_level`                           varchar(32)        DEFAULT NULL   COMMENT '学历-枚举 预定义字段： 小学， 初中， 高中/职高/技校， 大专， 本科, 硕士, 博士， 文盲和半文盲',
  `degree`                                    varchar(32)        DEFAULT NULL   COMMENT '学位',
  `is_capacity_civil_conduct`                 varchar(16)        DEFAULT NULL   COMMENT '是否具有完全民事行为能力"预定义字段：是 否"',
  `living_state`                              varchar(32)        DEFAULT NULL   COMMENT '居住状态',
  `province`                                  varchar(32)        DEFAULT NULL   COMMENT '客户所在省 传输省份城市代码则提供对应关系',
  `city`                                      varchar(32)        DEFAULT NULL   COMMENT '客户所在市 传输省份城市代码则提供对应关系',
  `address`                                   varchar(128)       DEFAULT NULL   COMMENT '客户居住地址',
  `house_province`                            varchar(32)        DEFAULT NULL   COMMENT '客户户籍所在省',
  `house_city`                                varchar(32)        DEFAULT NULL   COMMENT '客户户籍所在市',
  `house_address`                             varchar(128)       DEFAULT NULL   COMMENT '客户户籍地址',
  `communications_zip_code`                   varchar(128)       DEFAULT NULL   COMMENT '通讯邮编',
  `mailing_address`                           varchar(128)       DEFAULT NULL   COMMENT '客户通讯地址',
  `career`                                    varchar(32)        DEFAULT NULL   COMMENT '客户职业',
  `working_state`                             varchar(32)        DEFAULT NULL   COMMENT '工作状态 "预定义字段：在职，失业"',
  `position`                                  varchar(32)        DEFAULT NULL   COMMENT '职务',
  `title`                                     varchar(32)        DEFAULT NULL   COMMENT '职称',
  `borrower_industry`                         varchar(255)       DEFAULT NULL   COMMENT '借款人行业-枚举\r\n借款人行业\r\nNIL--空\r\nA--农、林、牧、渔业\r\nB--采矿业\r\nC--制造业\r\nD--电力、热力、燃气及水生产和供应业\r\nE--建筑业\r\nF--批发和零售业\r\nG--交通运输、仓储和邮政业\r\nH--住宿和餐饮业\r\nI--信息传输、软件和信息技术服务业\r\nJ--金融业\r\nK--房地产业\r\nL--租赁和商务服务业\r\nM--科学研究和技术服务业\r\nN--水利、环境和公共设施管理业\r\nO--居民服务、修理和其他服务业\r\nP--教育\r\nQ--卫生和社会工作\r\nR--文化、体育和娱乐业\r\nS--公共管理、社会保障和社会组织\r\nT--国际组织\r\nZ--其他',
  `is_car`                                    varchar(16)        DEFAULT NULL   COMMENT '是否有车"预定义字段:是 否"',
  `is_mortgage_financing`                     varchar(16)        DEFAULT NULL   COMMENT '是否有按揭车贷"预定义字段:是 否"',
  `is_house`                                  varchar(16)        DEFAULT NULL   COMMENT '是否有房"预定义字段:是 否"',
  `is_mortgage_loans`                         varchar(16)        DEFAULT NULL   COMMENT '是否有按揭房贷"预定义字段:是 否"',
  `is_credit_card`                            varchar(16)        DEFAULT NULL   COMMENT '是否有信用卡"预定义字段:是 否"',
  `credit_limit`                              decimal(20,6)      DEFAULT NULL   COMMENT '信用卡额度',
  `annual_income`                             decimal(20,6)      DEFAULT NULL   COMMENT '年收入(元)',
  `internal_credit_rating`                    varchar(32)        DEFAULT NULL   COMMENT '内部信用等级',
  `blacklist_level`                           varchar(32)        DEFAULT NULL   COMMENT '黑名单等级',
  `unit_name`                                 varchar(100)       DEFAULT NULL   COMMENT '单位名称',
  `fixed_telephone`                           varchar(18)        DEFAULT NULL   COMMENT '固定电话',
  `zip_code`                                  varchar(18)        DEFAULT NULL   COMMENT '邮编',
  `unit_address`                              varchar(100)       DEFAULT NULL   COMMENT '单位详细地址',
  `create_time`                               timestamp                         COMMENT '创建时间',
  `update_time`                               timestamp                         COMMENT '更新时间',
  `import_id`                                 int(11)            DEFAULT NULL   COMMENT '导入Id',
  `data_source`                               tinyint(2)         DEFAULT NULL   COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `borrowerinfo_index` (`project_id`,`agency_id`,`serial_number`) USING BTREE,
  KEY `index2` (`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=99998553 DEFAULT CHARSET=utf8 COMMENT='主借款人信息表-文件二';

-- ----------------------------
-- Table structure for t_associatesinfo
-- ----------------------------
-- DROP TABLE IF EXISTS `t_associatesinfo`;
CREATE TABLE `t_associatesinfo` (
  `id`                                        int(32)            NOT NULL       COMMENT '关联人信息主键',
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `agency_id`                                 varchar(32)        DEFAULT NULL   COMMENT '机构编号',
  `serial_number`                             varchar(100)       DEFAULT NULL   COMMENT '借据号',
  `contract_role`                             varchar(32)        DEFAULT NULL   COMMENT '合同角色-枚举 预定义字段: 共同借款人, 担保人, 无',
  `borrower_name`                             varchar(100)       DEFAULT NULL   COMMENT '客户姓名',
  `certificate_type`                          varchar(32)        DEFAULT NULL   COMMENT '证件类型-预定义字段：身份证 护照 户口本 外国人护照',
  `document_num`                              varchar(100)       DEFAULT NULL   COMMENT '身份证号',
  `phone_num`                                 varchar(32)        DEFAULT NULL   COMMENT '手机号',
  `age`                                       int(4)             DEFAULT NULL   COMMENT '年龄',
  `sex`                                       varchar(16)        DEFAULT NULL   COMMENT '性别预定义字段：男，女',
  `relationship_with_borrowers`               varchar(32)        DEFAULT NULL   COMMENT '与借款人关系 预定义字段：配偶 父母 子女 亲戚 朋友 同事',
  `career`                                    varchar(32)        DEFAULT NULL   COMMENT '职业',
  `working_state`                             varchar(32)        DEFAULT NULL   COMMENT '工作状态 预定义字段：在职 失业',
  `annual_income`                             decimal(20,6)      DEFAULT NULL   COMMENT '年收入(元)',
  `mailing_address`                           varchar(128)       DEFAULT NULL   COMMENT '通讯地址',
  `unit_address`                              varchar(129)       DEFAULT NULL   COMMENT '单位详细地址',
  `unit_contact_mode`                         varchar(130)       DEFAULT NULL   COMMENT '单位联系方式',
  `create_time`                               timestamp                         COMMENT '创建时间',
  `update_time`                               timestamp                         COMMENT '更新时间',
  `import_id`                                 int(11)            DEFAULT NULL   COMMENT '导入Id',
  `data_source`                               tinyint(2)         DEFAULT NULL   COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `associatesinfo_index` (`project_id`,`agency_id`,`serial_number`,`document_num`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=203499 DEFAULT CHARSET=utf8 COMMENT='关联人信息表-文件三';

-- ----------------------------
-- Table structure for t_guarantycarinfo
-- ----------------------------
-- DROP TABLE IF EXISTS `t_guarantycarinfo`;
CREATE TABLE `t_guarantycarinfo` (
  `id`                                        int(32)            NOT NULL       COMMENT '抵押物(车)信息主键',
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `agency_id`                                 varchar(32)        DEFAULT NULL   COMMENT '机构编号',
  `serial_number`                             varchar(100)       DEFAULT NULL   COMMENT '借据号',
  `guaranty_code`                             varchar(100)       DEFAULT NULL   COMMENT '抵押物编号',
  `guaranty_handling_status`                  varchar(32)        DEFAULT NULL   COMMENT '抵押办理状态 预定义字段：办理中 办理完成 尚未办理',
  `guaranty_alignment`                        varchar(32)        DEFAULT NULL   COMMENT '抵押顺位 1-第一顺位 2-第二顺位 9-其他',
  `car_property`                              varchar(32)        DEFAULT NULL   COMMENT '车辆性质 预定义字段：非融资车分期非 融资车抵贷 融资租赁车分期 融资租赁车抵贷',
  `financing_type`                            varchar(32)        DEFAULT NULL   COMMENT '融资方式 预定义字段：正租 反租',
  `guarantee_type`                            varchar(32)        DEFAULT NULL   COMMENT '担保方式 预定义字段：质押担保，信用担保，保证担保，抵押担保',
  `pawn_value`                                decimal(20,6)      DEFAULT NULL   COMMENT '评估价格(元)',
  `car_sales_price`                           decimal(20,6)      DEFAULT NULL   COMMENT '车辆销售价格(元)',
  `car_new_price`                             decimal(20,6)      DEFAULT NULL   COMMENT '新车指导价(元)',
  `total_investment`                          decimal(20,6)      DEFAULT NULL   COMMENT '投资总额(元)',
  `purchase_tax_amouts`                       decimal(20,6)      DEFAULT NULL   COMMENT '购置税金额(元)',
  `insurance_type`                            varchar(100)       DEFAULT NULL   COMMENT '保险种类  交强险 第三者责任险 盗抢险 车损险 不计免赔 其他',
  `car_insurance_premium`                     decimal(20,6)      DEFAULT NULL   COMMENT '汽车保险总费用',
  `total_poundage`                            decimal(20,6)      DEFAULT NULL   COMMENT '手续总费用(元)',
  `cumulative_car_transfer_number`            int(11)            DEFAULT NULL   COMMENT '累计车辆过户次数',
  `one_year_car_transfer_number`              int(11)            DEFAULT NULL   COMMENT '一年内车辆过户次数',
  `liability_insurance_cost1`                 decimal(20,6)      DEFAULT NULL   COMMENT '责信保费用1',
  `liability_insurance_cost2`                 decimal(20,6)      DEFAULT NULL   COMMENT '责信保费用2',
  `car_type`                                  varchar(32)        DEFAULT NULL   COMMENT '车类型 预定义字段：新车 二手车',
  `frame_num`                                 varchar(100)       DEFAULT NULL   COMMENT '车架号',
  `engine_num`                                varchar(100)       DEFAULT NULL   COMMENT '发动机号',
  `gps_code`                                  varchar(100)       DEFAULT NULL   COMMENT 'GPS编号',
  `gps_cost`                                  decimal(20,6)      DEFAULT NULL   COMMENT 'GPS费用',
  `license_num`                               varchar(100)       DEFAULT NULL   COMMENT '车牌号码',
  `car_brand`                                 varchar(100)       DEFAULT NULL   COMMENT '车辆品牌',
  `car_system`                                varchar(100)       DEFAULT NULL   COMMENT '车系',
  `car_model`                                 varchar(100)       DEFAULT NULL   COMMENT '车型',
  `car_age`                                   decimal(20,6)      DEFAULT NULL   COMMENT '车龄',
  `car_energy_type`                           varchar(32)        DEFAULT NULL   COMMENT '车辆能源类型 预定义字段： 混合动力 纯电 非新能源车',
  `production_date`                           varchar(32)        DEFAULT NULL   COMMENT '生产日期',
  `mileage`                                   decimal(20,6)      DEFAULT NULL   COMMENT '里程数',
  `register_date`                             date               DEFAULT NULL   COMMENT '注册日期',
  `buy_car_address`                           varchar(255)       DEFAULT NULL   COMMENT '车辆购买地',
  `car_colour`                                varchar(100)       DEFAULT NULL   COMMENT '车辆颜色',
  `create_time`                               timestamp                         COMMENT '创建时间',
  `update_time`                               timestamp                         COMMENT '更新时间',
  `import_id`                                 int(11)            DEFAULT NULL   COMMENT '导入Id',
  `data_source`                               tinyint(2)         DEFAULT NULL   COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `guarantycarinfo_index` (`project_id`,`agency_id`,`serial_number`,`guaranty_code`) USING BTREE,
  KEY `guarantycarinfo_index1` (`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=100386270 DEFAULT CHARSET=utf8 COMMENT='抵押物(车)信息表-文件四';

-- ----------------------------
-- Table structure for t_repaymentplan
-- ----------------------------
-- DROP TABLE IF EXISTS `t_repaymentplan`;
CREATE TABLE `t_repaymentplan` (
  `id`                                        int(32)            NOT NULL       COMMENT '还款计划主键',
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `agency_id`                                 varchar(32)        DEFAULT NULL   COMMENT '机构编号',
  `serial_number`                             varchar(100)       DEFAULT NULL   COMMENT '借据号',
  `period`                                    int(8)             DEFAULT NULL   COMMENT '期次',
  `should_repay_date`                         date               DEFAULT NULL   COMMENT '应还款日',
  `should_repay_principal`                    decimal(20,6)      DEFAULT NULL   COMMENT '应还本金',
  `should_repay_interest`                     decimal(20,6)      DEFAULT NULL   COMMENT '应还利息',
  `should_repay_cost`                         decimal(20,6)      DEFAULT NULL   COMMENT '应还费用',
  `begin_principal_balance`                   decimal(20,6)      DEFAULT NULL   COMMENT '期初剩余本金',
  `end_principal_balance`                     decimal(20,6)      DEFAULT NULL   COMMENT '期末剩余本金',
  `effective_date`                            date               DEFAULT NULL   COMMENT '生效日期',
  `repay_status`                              tinyint(4)         DEFAULT NULL   COMMENT '还款状态 1:入池前已还,2:入池前未还',
  `create_time`                               timestamp                         COMMENT '创建时间',
  `update_time`                               timestamp                         COMMENT '更新时间',
  `import_id`                                 int(11)            DEFAULT NULL   COMMENT '导入Id',
  `data_source`                               tinyint(2)         DEFAULT NULL   COMMENT '数据来源 1:startLink,2:excelImport,3:systemgenerated',
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
  `id`                                        int(11)            NOT NULL       COMMENT '还款计划主键',
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `agency_id`                                 varchar(32)        DEFAULT NULL   COMMENT '机构编号',
  `serial_number`                             varchar(100)       DEFAULT NULL   COMMENT '借据号',
  `period`                                    tinyint(8)         DEFAULT NULL   COMMENT '期次',
  `should_repay_date`                         date               DEFAULT NULL   COMMENT '应还款日',
  `should_repay_principal`                    decimal(20,6)      DEFAULT NULL   COMMENT '应还本金',
  `should_repay_interest`                     decimal(20,6)      DEFAULT NULL   COMMENT '应还利息',
  `should_repay_cost`                         decimal(20,6)      DEFAULT NULL   COMMENT '应还费用',
  `begin_principal_balance`                   decimal(20,6)      DEFAULT NULL   COMMENT '期初剩余本金',
  `end_principal_balance`                     decimal(20,6)      DEFAULT NULL   COMMENT '期末剩余本金',
  `effective_date`                            date               DEFAULT NULL   COMMENT '生效日期',
  `repay_status`                              tinyint(4)         DEFAULT NULL   COMMENT '还款状态 1:入池前已还,2:入池前未还',
  `create_time`                               timestamp                         COMMENT '创建时间',
  `update_time`                               timestamp                         COMMENT '更新时间',
  `import_id`                                 int(11)            DEFAULT NULL   COMMENT '导入Id',
  `data_source`                               tinyint(2)         DEFAULT NULL   COMMENT '数据来源 1:startLink,2:excelImport,3:systemgenerated',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `repaymentplan_history_index` (`project_id`,`serial_number`,`agency_id`,`period`,`effective_date`) USING BTREE,
  KEY `IX_REPAY_HIS_SERIAL_EFFECTIVE` (`serial_number`,`effective_date`)
) ENGINE=InnoDB AUTO_INCREMENT=26950022 DEFAULT CHARSET=utf8 COMMENT='还款计划历史';


-- ----------------------------
-- Table structure for t_assettradeflow
-- ----------------------------
-- DROP TABLE IF EXISTS `t_assettradeflow`;
CREATE TABLE `t_assettradeflow` (
  `id`                                        int(32)            NOT NULL       COMMENT '资产交易支付流水信息',
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `agency_id`                                 varchar(32)        DEFAULT NULL   COMMENT '机构编号',
  `serial_number`                             varchar(100)       DEFAULT NULL   COMMENT '借据号',
  `trade_channel`                             varchar(32)        DEFAULT NULL   COMMENT '交易渠道',
  `trade_type`                                varchar(32)        DEFAULT NULL   COMMENT '交易类型',
  `order_number`                              varchar(100)       DEFAULT NULL   COMMENT '订单编号',
  `order_amount`                              decimal(20,6)      DEFAULT NULL   COMMENT '订单金额',
  `trade_currency`                            varchar(32)        DEFAULT NULL   COMMENT '币种',
  `name`                                      varchar(255)       DEFAULT NULL   COMMENT '姓名',
  `bank_account`                              varchar(100)       DEFAULT NULL   COMMENT '银行账号',
  `trade_time`                                date               DEFAULT NULL   COMMENT '交易日期',
  `trade_status`                              varchar(32)        DEFAULT NULL   COMMENT '交易状态',
  `trade_digest`                              varchar(128)       DEFAULT NULL   COMMENT '交易摘要',
  `confirm_repay_time`                        date               DEFAULT NULL   COMMENT '确认还款日期',
  `create_time`                               timestamp                         COMMENT '创建时间',
  `update_time`                               timestamp                         COMMENT '更新时间',
  `import_id`                                 int(11)            DEFAULT NULL   COMMENT '导入Id',
  `data_source`                               tinyint(2)         DEFAULT NULL   COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `assettradeflow_index` (`project_id`,`agency_id`,`serial_number`,`order_number`) USING BTREE,
  KEY `assettradeflow_index1` (`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=12573496 DEFAULT CHARSET=utf8 COMMENT='资产交易支付流水信息表-文件六';

-- ----------------------------
-- Table structure for t_actualrepayinfo
-- ----------------------------
-- DROP TABLE IF EXISTS `t_actualrepayinfo`;
CREATE TABLE `t_actualrepayinfo` (
  `id`                                        int(32)            NOT NULL       COMMENT '实际还款信息主键',
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `agency_id`                                 varchar(32)        DEFAULT NULL   COMMENT '机构编号',
  `serial_number`                             varchar(100)       DEFAULT NULL   COMMENT '借据号',
  `term`                                      int(4)             DEFAULT NULL   COMMENT '期次',
  `shoud_repay_date`                          date               DEFAULT NULL   COMMENT '应还款日',
  `shoud_repay_principal`                     decimal(20,6)      DEFAULT NULL   COMMENT '应还本金（元）',
  `shoud_repay_interest`                      decimal(20,6)      DEFAULT NULL   COMMENT '应还利息（元）',
  `shoud_repay_fee`                           decimal(20,6)      DEFAULT NULL   COMMENT '应还费用（元）',
  `repay_type`                                varchar(32)        DEFAULT NULL   COMMENT '还款类型 1，提前还款 2，正常还款 3，部分还款 4，逾期还款',
  `actual_work_interest_rate`                 decimal(20,6)      DEFAULT NULL   COMMENT '实际还款执行利率',
  `actual_repay_principal`                    decimal(20,6)      DEFAULT NULL   COMMENT '实还本金（元）',
  `actual_repay_interest`                     decimal(20,6)      DEFAULT NULL   COMMENT '实还利息（元）',
  `actual_repay_fee`                          decimal(20,6)      DEFAULT NULL   COMMENT '实还费用（元）',
  `actual_repay_time`                         date               DEFAULT NULL   COMMENT '实际还清日期 : ( 若没有还清传还款日期，若还清就传实际还清的日期  )',
  `current_period_loan_balance`               decimal(20,6)      DEFAULT NULL   COMMENT '当期贷款余额  （各期还款日（T+1）更新该字段，即截至当期还款日资产的剩余（未偿还）贷款本金余额）',
  `current_account_status`                    varchar(32)        DEFAULT NULL   COMMENT '当期账户状态： （各期还款日（T+1）更新该字段，\r\n     正常：在还款日前该期应还已还清\r\n     提前还清（早偿）：贷款在该期还款日前提前全部还清\r\n     逾期：贷款在该期还款日时，实还金额小于应还金额。\r\n     ）',
  `penalbond`                                 decimal(20,6)      DEFAULT NULL   COMMENT '违约金',
  `penalty_interest`                          decimal(20,6)      DEFAULT NULL   COMMENT '罚息',
  `compensation`                              decimal(20,6)      DEFAULT NULL   COMMENT '赔偿金（提前还款/逾期所产生的赔偿金）',
  `advanced_commission_charge`                decimal(20,6)      DEFAULT NULL   COMMENT '提前还款手续费',
  `other_fee`                                 decimal(20,6)      DEFAULT NULL   COMMENT '其他相关费用 （违约金、罚款、赔偿金和提前还款手续费以外的费用）',
  `is_borrowers_oneself_repayment`            varchar(16)        DEFAULT NULL   COMMENT '是否借款人本人还款 预定义字段 Y N',
  `create_time`                               timestamp                         COMMENT '创建时间',
  `update_time`                               timestamp                         COMMENT '更新时间',
  `import_id`                                 int(11)            DEFAULT NULL   COMMENT '导入Id',
  `data_source`                               tinyint(2)         DEFAULT NULL   COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `actualrepayinfo_index` (`project_id`,`agency_id`,`serial_number`,`term`,`shoud_repay_date`,`actual_repay_time`,`current_period_loan_balance`) USING BTREE,
  KEY `actualrepayinfo_index1` (`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=19889709 DEFAULT CHARSET=utf8 COMMENT='实际还款信息表-文件七';

-- ----------------------------
-- Table structure for t_assetdealprocessinfo
-- ----------------------------
-- DROP TABLE IF EXISTS `t_assetdealprocessinfo`;
CREATE TABLE `t_assetdealprocessinfo` (
  `id`                                        int(32)            NOT NULL       COMMENT '资产处置过程信息主键',
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `agency_id`                                 varchar(32)        DEFAULT NULL   COMMENT '机构编号',
  `serial_number`                             varchar(100)       DEFAULT NULL   COMMENT '借据号',
  `deal_status`                               varchar(32)        DEFAULT NULL   COMMENT '处置状态（处置中 已处置 ）',
  `deal_type`                                 varchar(32)        DEFAULT NULL   COMMENT '处置类型（SUSONG-诉讼 FEISONGSU-非诉讼 ）',
  `lawsuit_node`                              varchar(32)        DEFAULT NULL   COMMENT '诉讼节点 处置开始 诉讼准备 法院受理 执行拍卖 处置结束',
  `lawsuit_node_time`                         date               DEFAULT NULL   COMMENT '诉讼节点时间',
  `deal_result`                               varchar(32)        DEFAULT NULL   COMMENT '处置结果',
  `create_time`                               timestamp                         COMMENT '创建时间',
  `update_time`                               timestamp                         COMMENT '更新时间',
  `import_id`                                 int(11)            DEFAULT NULL   COMMENT '导入Id',
  `data_source`                               tinyint(2)         DEFAULT NULL   COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `assetdealprocessinfo_index` (`project_id`,`agency_id`,`serial_number`) USING BTREE,
  KEY `assetdealprocessinfo_index1` (`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=48 DEFAULT CHARSET=utf8 COMMENT='资产处置过程信息表-文件八';

-- ----------------------------
-- Table structure for t_assetaddtradeinfo
-- ----------------------------
-- DROP TABLE IF EXISTS `t_assetaddtradeinfo`;
CREATE TABLE `t_assetaddtradeinfo` (
  `id`                                        int(32)            NOT NULL       COMMENT '资产补充交易信息主键',
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `agency_id`                                 varchar(32)        DEFAULT NULL   COMMENT '机构编号',
  `serial_number`                             varchar(100)       DEFAULT NULL   COMMENT '借据号',
  `trade_type`                                varchar(32)        DEFAULT NULL   COMMENT '交易类型',
  `trade_reason`                              varchar(255)       DEFAULT NULL   COMMENT '交易原因',
  `trade_time`                                date               DEFAULT NULL   COMMENT '交易日期',
  `trade_total_amount`                        decimal(20,6)      DEFAULT NULL   COMMENT '交易总金额',
  `principal`                                 decimal(20,6)      DEFAULT NULL   COMMENT '本金',
  `interest`                                  decimal(20,6)      DEFAULT NULL   COMMENT '利息',
  `penalty_interest`                          decimal(20,6)      DEFAULT NULL   COMMENT '罚息',
  `other_fee`                                 decimal(20,6)      DEFAULT NULL   COMMENT '其他费用',
  `create_time`                               timestamp                         COMMENT '创建时间',
  `update_time`                               timestamp                         COMMENT '更新时间',
  `import_id`                                 int(11)            DEFAULT NULL   COMMENT '导入Id',
  `data_source`                               tinyint(2)         DEFAULT NULL   COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `assetaddtradeinfo_index` (`project_id`,`agency_id`,`serial_number`) USING BTREE,
  KEY `assetaddtradeinfo_index1` (`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=38 DEFAULT CHARSET=utf8 COMMENT='资产补充交易信息表-文件九';

-- ----------------------------
-- Table structure for t_assetaccountcheck
-- ----------------------------
-- DROP TABLE IF EXISTS `t_assetaccountcheck`;
CREATE TABLE `t_assetaccountcheck` (
  `id`                                        int(32)            NOT NULL       COMMENT '资产对账信息主键',
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `agency_id`                                 varchar(32)        DEFAULT NULL   COMMENT '机构编号',
  `serial_number`                             varchar(100)       DEFAULT NULL   COMMENT '借据号',
  `is_change_repay_schedule`                  varchar(16)        DEFAULT NULL   COMMENT '是否变更还款计划（YES - 是，NO - 否 ）',
  `change_time`                               date               DEFAULT NULL   COMMENT '变更时间',
  `total_loan_amount`                         decimal(20,6)      DEFAULT NULL   COMMENT '贷款总金额',
  `year_loan_rate`                            varchar(32)        DEFAULT NULL   COMMENT '贷款年利率',
  `total_term`                                int(4)             DEFAULT NULL   COMMENT '总期数',
  `already_repay_term`                        int(4)             DEFAULT NULL   COMMENT '已还期数',
  `remain_term`                               int(4)             DEFAULT NULL   COMMENT '剩余期数',
  `remain_principal`                          decimal(20,6)      DEFAULT NULL   COMMENT '剩余本金',
  `remain_interest`                           decimal(20,6)      DEFAULT NULL   COMMENT '剩余利息',
  `remain_other_fee`                          decimal(20,6)      DEFAULT NULL   COMMENT '剩余其他费用',
  `settle_payment_advance`                    decimal(20,6)      DEFAULT NULL   COMMENT '提前结清实还费用',
  `early_settlement_interest`                 decimal(20,6)      DEFAULT NULL   COMMENT '提前结清实还利息',
  `principal_settled_advance`                 decimal(20,6)      DEFAULT NULL   COMMENT '提前结清实还本金',
  `next_term_shouldrepay_time`                date               DEFAULT NULL   COMMENT '下一期应还款日',
  `asset_status`                              varchar(32)        DEFAULT NULL   COMMENT '资产状态 枚举 正常，逾期，已结清 ',
  `closeoff_reason`                           varchar(255)       DEFAULT NULL   COMMENT '结清原因 枚举 正常结清，提前结清，处置结束，资产回购，逾期结清，合同取消',
  `current_overdue_principal`                 decimal(20,6)      DEFAULT NULL   COMMENT '当前逾期本金',
  `current_overdue_interest`                  decimal(20,6)      DEFAULT NULL   COMMENT '当前逾期利息',
  `current_overdue_fee`                       decimal(20,6)      DEFAULT NULL   COMMENT '当前逾期费用',
  `current_overdue_daynum`                    int(4)             DEFAULT NULL   COMMENT '当前逾期天数',
  `total_overdue_daynum`                      int(4)             DEFAULT NULL   COMMENT '累计逾期天数',
  `history_most_overdue_daynum`               int(4)             DEFAULT NULL   COMMENT '历史最高逾期天数',
  `history_total_overdue_daynum`              int(4)             DEFAULT NULL   COMMENT '历史累计逾期天数',
  `current_overdue_termnum`                   int(4)             DEFAULT NULL   COMMENT '当前逾期期数',
  `total_overdue_termnum`                     int(4)             DEFAULT NULL   COMMENT '累计逾期期数',
  `history_longest_overdue_term`              int(4)             DEFAULT NULL   COMMENT '历史单次最长逾期期数',
  `history_top_overdue_principal`             decimal(20,6)      DEFAULT NULL   COMMENT '历史最大逾期本金',
  `data_extract_time`                         date               DEFAULT NULL   COMMENT '数据提取日',
  `create_time`                               timestamp                         COMMENT '创建时间',
  `update_time`                               timestamp                         COMMENT '更新时间',
  `import_id`                                 int(11)            DEFAULT NULL   COMMENT '导入Id',
  `data_source`                               tinyint(2)         DEFAULT NULL   COMMENT '数据来源 1:startLink,2:excelImport,3:systemgenerated',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `assetaccountcheck_index` (`project_id`,`agency_id`,`serial_number`,`data_extract_time`) USING BTREE,
  KEY `assetaccountcheck_index2` (`project_id`,`serial_number`,`data_extract_time`) USING BTREE,
  KEY `assetaccountcheck_index3` (`serial_number`,`data_extract_time`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=191291599 DEFAULT CHARSET=utf8 COMMENT='资产对账信息表-文件十';

-- ----------------------------
-- Table structure for t_projectaccountcheck
-- ----------------------------
-- DROP TABLE IF EXISTS `t_projectaccountcheck`;
CREATE TABLE `t_projectaccountcheck` (
  `id`                                        int(32)            NOT NULL       COMMENT '项目对账信息主键',
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `agency_id`                                 varchar(32)        DEFAULT NULL   COMMENT '机构编号',
  `loan_sum`                                  int(8)             DEFAULT NULL   COMMENT '贷款总数量',
  `loan_remaining_principal`                  decimal(20,6)      DEFAULT NULL   COMMENT '贷款剩余本金',
  `loan_totalamount`                          decimal(20,6)      DEFAULT NULL   COMMENT '贷款合同总金额',
  `overdue_assets_sum1`                       int(4)             DEFAULT NULL   COMMENT '1~7逾期资产数',
  `overdue_assets_outstanding_principal1`     decimal(20,6)      DEFAULT NULL   COMMENT '1~7逾期资产剩余未还本金',
  `overdue_assets_sum8`                       int(4)             DEFAULT NULL   COMMENT '8~30逾期资产数',
  `overdue_assets_outstanding_principal8`     decimal(20,6)      DEFAULT NULL   COMMENT '8~30逾期资产剩余未还本金',
  `overdue_assets_sum31`                      int(4)             DEFAULT NULL   COMMENT '31~60逾期资产数',
  `overdue_assets_outstanding_principal31`    decimal(20,6)      DEFAULT NULL   COMMENT '31~60逾期资产剩余未还本金',
  `overdue_assets_sum61`                      int(4)             DEFAULT NULL   COMMENT '61~90逾期资产数',
  `overdue_assets_outstanding_principal61`    decimal(20,6)      DEFAULT NULL   COMMENT '61~90逾期资产剩余未还本金',
  `overdue_assets_sum91`                      int(4)             DEFAULT NULL   COMMENT '91~180逾期资产数',
  `overdue_assets_outstanding_principal91`    decimal(20,6)      DEFAULT NULL   COMMENT '91~180逾期资产剩余未还本金',
  `overdue_assets_sum180`                     int(4)             DEFAULT NULL   COMMENT '180+逾期资产数',
  `overdue_assets_outstanding_principal180`   decimal(20,6)      DEFAULT NULL   COMMENT '180+逾期资产剩余未还本金',
  `add_loan_sum`                              int(4)             DEFAULT NULL   COMMENT '当日新增贷款笔数',
  `add_loan_totalamount`                      decimal(20,6)      DEFAULT NULL   COMMENT '当日新增贷款总金额',
  `actual_payment_sum`                        int(4)             DEFAULT NULL   COMMENT '当日实还资产笔数',
  `actual_payment_totalamount`                decimal(20,6)      DEFAULT NULL   COMMENT '当日实还总金额',
  `back_assets_sum`                           int(4)             DEFAULT NULL   COMMENT '当日回购资产笔数',
  `back_assets_totalamount`                   decimal(20,6)      DEFAULT NULL   COMMENT '当日回购总金额',
  `disposal_assets_sum`                       int(4)             DEFAULT NULL   COMMENT '当日处置资产笔数',
  `disposal_assets_totalamount`               decimal(20,6)      DEFAULT NULL   COMMENT '当日处置回收总金额',
  `differentia_complement_assets_sum`         int(4)             DEFAULT NULL   COMMENT '当日差额补足资产笔数',
  `differentia_complement_assets_totalamount` decimal(20,6)      DEFAULT NULL   COMMENT '当日差额补足总金额',
  `compensatory_assets_sum`                   int(4)             DEFAULT NULL   COMMENT '当日代偿资产笔数',
  `compensatory_assets_totalamount`           decimal(20,6)      DEFAULT NULL   COMMENT '当日代偿总金额',
  `data_extraction_day`                       date               DEFAULT NULL   COMMENT '数据提取日',
  `create_time`                               timestamp                         COMMENT '创建时间',
  `update_time`                               timestamp                         COMMENT '更新时间',
  `import_id`                                 int(11)            DEFAULT NULL   COMMENT '导入Id',
  `data_source`                               tinyint(2)         DEFAULT NULL   COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `projectaccountcheck_index` (`project_id`,`agency_id`,`data_extraction_day`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=91236 DEFAULT CHARSET=utf8 COMMENT='项目对账信息表-文件十一';

-- ----------------------------
-- Table structure for t_enterpriseinfo
-- ----------------------------
-- DROP TABLE IF EXISTS `t_enterpriseinfo`;
CREATE TABLE `t_enterpriseinfo` (
  `id`                                        int(32)            NOT NULL       COMMENT '企业名称信息主键',
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `agency_id`                                 varchar(32)        DEFAULT NULL   COMMENT '机构编号',
  `serial_number`                             varchar(100)       DEFAULT NULL   COMMENT '借据号',
  `contract_role`                             varchar(32)        DEFAULT NULL   COMMENT '合同角色 预定义字段: 主借款企业 共同借款企业 担保企业 无',
  `enterprise_name`                           varchar(100)       DEFAULT NULL   COMMENT '企业姓名',
  `registration_number`                       varchar(32)        DEFAULT NULL   COMMENT '工商注册号',
  `organization_code`                         varchar(32)        DEFAULT NULL   COMMENT '组织机构代码',
  `taxpayer_identification_number`            varchar(18)        DEFAULT NULL   COMMENT '纳税人识别号',
  `uniform_credit_code`                       varchar(18)        DEFAULT NULL   COMMENT '统一信用代码',
  `registered_address`                        varchar(130)       DEFAULT NULL   COMMENT '注册地址',
  `create_time`                               timestamp                         COMMENT '创建时间',
  `update_time`                               timestamp                         COMMENT '更新时间',
  `import_id`                                 int(11)            DEFAULT NULL   COMMENT '导入Id',
  `data_source`                               tinyint(2)         DEFAULT NULL   COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `enterpriseinfo_index` (`project_id`,`agency_id`,`serial_number`,`registration_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8 COMMENT='企业名称信息表-文件十二';

-- ----------------------------
-- Table structure for t_guarantyhouseinfo
-- ----------------------------
-- DROP TABLE IF EXISTS `t_guarantyhouseinfo`;
CREATE TABLE `t_guarantyhouseinfo` (
  `id`                                        int(32)            NOT NULL       COMMENT '房抵押物信息主键',
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `agency_id`                                 varchar(32)        DEFAULT NULL   COMMENT '机构编号',
  `serial_number`                             varchar(100)       DEFAULT NULL   COMMENT '借据号',
  `guaranty_number`                           varchar(100)       DEFAULT NULL   COMMENT '抵押物编号',
  `guaranty_name`                             varchar(100)       DEFAULT NULL   COMMENT '抵押物名称',
  `guaranty_describe`                         varchar(100)       DEFAULT NULL   COMMENT '抵押物描述',
  `guaranty_handle_status`                    varchar(32)        DEFAULT NULL   COMMENT '抵押办理状态 预定义字段 办理中 办理完成 尚未办理',
  `guaranty_alignment`                        varchar(32)        DEFAULT NULL   COMMENT '抵押顺位 第一顺位 第二顺位 其他',
  `guaranty_front_hand_balance`               decimal(20,6)      DEFAULT NULL   COMMENT '前手抵押余额',
  `guaranty_type`                             varchar(32)        DEFAULT NULL   COMMENT '抵押类型',
  `ownership_name`                            varchar(32)        DEFAULT NULL   COMMENT '所有权人姓名',
  `ownership_document_type`                   varchar(32)        DEFAULT NULL   COMMENT '所有权人证件类型 预定义字段：身份证 护照 户口本 外国人护照',
  `ownership_document_number`                 varchar(100)       DEFAULT NULL   COMMENT '所有权人证件号码',
  `ownership_job`                             varchar(16)        DEFAULT NULL   COMMENT '所有人职业 0 1 3 4 5 6 X Y Z',
  `is_guaranty_ownership_only_domicile`       varchar(16)        DEFAULT NULL   COMMENT '押品是否为所有权人/借款人名下唯一住所 1 2 3 9',
  `house_area`                                decimal(20,6)      DEFAULT NULL   COMMENT '房屋建筑面积 平米',
  `house_age`                                 decimal(20,6)      DEFAULT NULL   COMMENT '楼龄',
  `house_location_province`                   varchar(50)        DEFAULT NULL   COMMENT '房屋所在省',
  `house_location_city`                       varchar(50)        DEFAULT NULL   COMMENT '房屋所在城市',
  `house_location_district_county`            varchar(50)        DEFAULT NULL   COMMENT '房屋所在区县',
  `house_address`                             varchar(256)       DEFAULT NULL   COMMENT '房屋地址',
  `property_years`                            decimal(20,6)      DEFAULT NULL   COMMENT '产权年限',
  `purchase_contract_number`                  varchar(100)       DEFAULT NULL   COMMENT '购房合同编号',
  `warrant_type`                              varchar(32)        DEFAULT NULL   COMMENT '权证类型 房产证 房屋他项权证',
  `property_certificate_number`               varchar(100)       DEFAULT NULL   COMMENT '房产证编号',
  `house_warrant_number`                      varchar(100)       DEFAULT NULL   COMMENT '房屋他项权证编号',
  `house_type`                                varchar(32)        DEFAULT NULL   COMMENT '房屋类别 01 02 03 04 05 06 07 08 09 00',
  `is_property_right_co_owner`                varchar(32)        DEFAULT NULL   COMMENT '是否有产权共有人',
  `property_co_owner_informed_situation`      varchar(255)       DEFAULT NULL   COMMENT '产权共有人知情情况 1 2 3 9',
  `guaranty_registration`                     varchar(32)        DEFAULT NULL   COMMENT '抵押登记办理 已完成 已递交申请并取得回执 尚未办理',
  `enforcement_notarization`                  varchar(32)        DEFAULT NULL   COMMENT '强制执行公证 已完成 已递交申请并取得回执 尚未办理',
  `is_arbitration_prove`                      varchar(255)       DEFAULT NULL   COMMENT '网络仲裁办仲裁证明',
  `assessment_price_evaluation_company`       decimal(20,6)      DEFAULT NULL   COMMENT '评估价格-评估公司(元)',
  `assessment_price_letting_agent`            decimal(20,6)      DEFAULT NULL   COMMENT '评估价格-房屋中介(元)',
  `assessment_price_original_rights_day`      decimal(20,6)      DEFAULT NULL   COMMENT '评估价格-原始权益日内部评估(元)',
  `house_selling_price`                       decimal(20,6)      DEFAULT NULL   COMMENT '房屋销售价格(元)',
  `create_time`                               timestamp                         COMMENT '创建时间',
  `update_time`                               timestamp                         COMMENT '更新时间',
  `import_id`                                 int(11)            DEFAULT NULL   COMMENT '导入Id',
  `data_source`                               tinyint(2)         DEFAULT NULL   COMMENT '数据来源 1:startLink,2:excelImport',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `guarantyhouseinfo_index` (`project_id`,`agency_id`,`serial_number`,`guaranty_number`) USING BTREE,
  KEY `guarantyhouseinfo_index1` (`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=69 DEFAULT CHARSET=utf8 COMMENT='房抵押物信息表-文件十三';











-- ----------------------------
-- Table structure for t_basic_asset
-- ----------------------------
-- DROP TABLE IF EXISTS `t_basic_asset`;
CREATE TABLE `t_basic_asset` (
  `id`                                        int(11)            NOT NULL       COMMENT '主键id',
  `import_id`                                 int(11)            DEFAULT NULL   COMMENT '导入记录编号',
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `asset_bag_id`                              varchar(50)        DEFAULT NULL   COMMENT '资产包编号',
  `asset_type`                                varchar(64)        DEFAULT NULL   COMMENT '资产类型',
  `serial_number`                             varchar(100)       DEFAULT NULL   COMMENT '借据号',
  `contract_code`                             varchar(64)        DEFAULT NULL   COMMENT '贷款合同编号',
  `contract_amount`                           decimal(32,2)      DEFAULT NULL   COMMENT '贷款合同金额',
  `interest_rate_type`                        varchar(32)        DEFAULT NULL   COMMENT '利率类型',
  `contract_interest_rate`                    float(20,4)        DEFAULT NULL   COMMENT '合同利率',
  `base_interest_rate`                        float(20,4)        DEFAULT NULL   COMMENT '基准利率',
  `fixed_interest_rate`                       float(20,4)        DEFAULT NULL   COMMENT '固定利率',
  `fixed_interest_diff`                       varchar(45)        DEFAULT NULL   COMMENT '固定利差',
  `interest_rate_ajustment`                   varchar(64)        DEFAULT NULL   COMMENT '利率调整方式',
  `loan_issue_date`                           date               DEFAULT NULL   COMMENT '贷款发放日',
  `loan_expiry_date`                          date               DEFAULT NULL   COMMENT '贷款到期日',
  `frist_repayment_date`                      date               DEFAULT NULL   COMMENT '首次还款日',
  `repayment_type`                            varchar(32)        DEFAULT NULL   COMMENT '还款方式',
  `repayment_frequency`                       varchar(32)        DEFAULT NULL   COMMENT '还款频率',
  `loan_repay_date`                           int(11)            DEFAULT NULL   COMMENT '贷款还款日',
  `tail_amount`                               decimal(32,2)      DEFAULT NULL   COMMENT '尾款金额',
  `tail_amount_rate`                          float              DEFAULT NULL   COMMENT '尾款金额占比',
  `consume_use`                               varchar(32)        DEFAULT NULL   COMMENT '消费用途',
  `guarantee_type`                            varchar(32)        DEFAULT NULL   COMMENT '担保方式',
  `extract_date`                              date               DEFAULT NULL   COMMENT '提取日期',
  `extract_date_principal_amount`             decimal(32,2)      DEFAULT NULL   COMMENT '提取日本金金额',
  `loan_cur_interest_rate`                    float              DEFAULT NULL   COMMENT '贷款现行利率',
  `borrower_type`                             varchar(32)        DEFAULT NULL   COMMENT '借款人类型',
  `borrower_name`                             varchar(300)       DEFAULT NULL   COMMENT '借款人姓名',
  `document_type`                             varchar(32)        DEFAULT NULL   COMMENT '证件类型',
  `document_num`                              varchar(100)       DEFAULT NULL   COMMENT '证件号码',
  `borrower_rating`                           varchar(32)        DEFAULT NULL   COMMENT '借款人评级',
  `borrower_industry`                         varchar(32)        DEFAULT NULL   COMMENT '借款人行业',
  `phone_num`                                 varchar(100)       DEFAULT NULL   COMMENT '手机号码',
  `sex`                                       varchar(32)        DEFAULT NULL   COMMENT '性别',
  `birthday`                                  date               DEFAULT NULL   COMMENT '出生日期',
  `age`                                       int(11)            DEFAULT NULL   COMMENT '年龄',
  `province`                                  varchar(32)        DEFAULT NULL   COMMENT '所在省份',
  `city`                                      varchar(32)        DEFAULT NULL   COMMENT '所在城市',
  `marital_status`                            varchar(32)        DEFAULT NULL   COMMENT '婚姻状况',
  `country`                                   varchar(32)        DEFAULT NULL   COMMENT '国籍',
  `annual_income`                             decimal(32,2)      DEFAULT NULL   COMMENT '年收入',
  `income_debt_rate`                          float(32,4)        DEFAULT NULL   COMMENT '收入债务比',
  `education_level`                           varchar(32)        DEFAULT NULL   COMMENT '教育程度',
  `period_exp`                                int(11)            DEFAULT NULL   COMMENT '提取日剩余还款期数',
  `account_age`                               float(11,2)        DEFAULT NULL   COMMENT '帐龄',
  `residual_maturity_con`                     float(11,2)        DEFAULT NULL   COMMENT '合同期限',
  `residual_maturity_ext`                     float(11,2)        DEFAULT NULL   COMMENT '提取日剩余期限',
  `package_principal_balance`                 decimal(32,2)      DEFAULT '0.00' COMMENT '封包本金余额',
  `statistics_date`                           date               DEFAULT NULL   COMMENT '统计日期',
  `extract_interest_date`                     date               DEFAULT NULL   COMMENT '提取日计息日',
  `curr_over_days`                            int(11)            DEFAULT '0'    COMMENT '当前逾期天数',
  `remain_counts`                             int(11)            DEFAULT NULL   COMMENT '当前剩余期数',
  `remain_amounts`                            decimal(20,2)      DEFAULT NULL   COMMENT '当前剩余本金(元)',
  `remain_interest`                           decimal(20,2)      DEFAULT NULL   COMMENT '当前剩余利息(元)',
  `remain_other_amounts`                      decimal(20,2)      DEFAULT NULL   COMMENT '当前剩余费用(元)',
  `periods`                                   int(11)            DEFAULT NULL   COMMENT '总期数',
  `period_amounts`                            decimal(20,2)      DEFAULT NULL   COMMENT '每期固定费用',
  `bus_product_id`                            varchar(100)       DEFAULT NULL   COMMENT '业务产品编号',
  `bus_product_name`                          varchar(100)       DEFAULT NULL   COMMENT '业务产品名称',
  `shoufu_amount`                             decimal(32,2)      DEFAULT NULL   COMMENT '首付款金额',
  `selling_price`                             decimal(32,2)      DEFAULT NULL   COMMENT '销售价格',
  `contract_daily_interest_rate`              float              DEFAULT NULL   COMMENT '合同日利率',
  `repay_plan_cal_rule`                       varchar(32)        DEFAULT NULL   COMMENT '还款计划计算规则',
  `contract_daily_interest_rate_count`        int(11)            DEFAULT NULL   COMMENT '日利率计算基础',
  `total_investment_amount`                   decimal(32,2)      DEFAULT NULL   COMMENT '投资总额(元)',
  `contract_month_interest_rate`              float(20,4)        DEFAULT NULL   COMMENT '合同月利率',
  `status_change_log`                         text                              COMMENT '资产状态变更日志',
  `package_filter_id`                         varchar(50)        DEFAULT NULL   COMMENT '虚拟过滤包id',
  `virtual_asset_bag_id`                      varchar(32)        DEFAULT NULL   COMMENT '虚拟资产包id',
  `package_remain_principal`                  decimal(20,2)      DEFAULT NULL   COMMENT '封包时当前剩余本金(元)',
  `package_remain_periods`                    tinyint(4)         DEFAULT NULL   COMMENT '封包时当前剩余期数',
  `status`                                    int(4)             DEFAULT '0'    COMMENT '资产状态(-1征信异常 0准入 1在池 2封包 3发行 4历史',
  `wind_control_status`                       varchar(32)        DEFAULT NULL   COMMENT '当前风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
  `wind_control_status_pool`                  varchar(32)        DEFAULT NULL   COMMENT '入池时风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
  `cheat_level`                               varchar(10)        DEFAULT NULL   COMMENT '反欺诈等级:1~5,-1:null,其它:异常值',
  `score_range`                               varchar(10)        DEFAULT NULL   COMMENT '评分等级:1~20,-1:null,其它:异常值',
  `score_level`                               varchar(10)        DEFAULT NULL   COMMENT '评分区间:1~5,-1:null,其它:异常值',
  `create_time`                               timestamp                         COMMENT '创建记录时间',
  `update_time`                               timestamp                         COMMENT '修改时间',
  `data_source`                               tinyint(2)         DEFAULT NULL   COMMENT '数据来源 1:startLink,2:excelImport',
  `address`                                   varchar(255)       DEFAULT NULL   COMMENT '居住地址',
  `mortgage_rates`                            float(20,4)        DEFAULT NULL   COMMENT '抵押率(%)',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `basicasset_index` (`project_id`,`serial_number`,`contract_code`) USING BTREE,
  KEY `basicasset_index2` (`extract_date`) USING BTREE,
  KEY `basicasset_index3` (`asset_bag_id`) USING BTREE,
  KEY `basicasset_index4` (`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1167340 DEFAULT CHARSET=utf8 COMMENT='基础资产表';











-- ----------------------------
-- Table structure for t_asset_package_cash_flow
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_package_cash_flow`;
CREATE TABLE `t_asset_package_cash_flow` (
  `id`                                        int(11)            NOT NULL,
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `asset_bag_id`                              varchar(50)        DEFAULT NULL   COMMENT '资产包编号',
  `collection_date`                           date               NOT NULL       COMMENT '归集日',
  `begin_principal_balance`                   decimal(32,2)      DEFAULT NULL   COMMENT '期初本金余额(元)',
  `amount`                                    decimal(32,2)      DEFAULT NULL   COMMENT '金额(元)',
  `principal`                                 decimal(32,2)      DEFAULT NULL   COMMENT '本金(元)',
  `interest`                                  decimal(32,2)      DEFAULT NULL   COMMENT '利息(元)',
  `cost`                                      decimal(32,2)      DEFAULT NULL   COMMENT '费用(元)',
  `end_principal_balance`                     decimal(32,2)      DEFAULT NULL   COMMENT '期末本金余额(元)',
  `create_time`                               timestamp
  `update_time`                               timestamp,
  PRIMARY KEY (`id`),
  KEY `idx_cash_project_id_asset_bag_id` (`project_id`,`asset_bag_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=95466 DEFAULT CHARSET=utf8 COMMENT='封包资产现金流';

-- ----------------------------
-- Table structure for t_asset_package
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_package`;
CREATE TABLE `t_asset_package` (
  `id`                                        int(11)            NOT NULL       COMMENT '主键id',
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `asset_bag_id`                              varchar(50)        DEFAULT NULL   COMMENT '资产包编号',
  `asset_type`                                varchar(64)        DEFAULT NULL   COMMENT '资产类型',
  `serial_number`                             varchar(100)       DEFAULT NULL   COMMENT '借据号',
  `contract_code`                             varchar(64)        DEFAULT NULL   COMMENT '贷款合同编号',
  `contract_amount`                           decimal(32,2)      DEFAULT NULL   COMMENT '贷款合同金额',
  `interest_rate_type`                        varchar(32)        DEFAULT NULL   COMMENT '利率类型',
  `contract_interest_rate`                    float(20,4)        DEFAULT NULL   COMMENT '合同利率',
  `repayment_type`                            varchar(32)        DEFAULT NULL   COMMENT '还款方式',
  `guarantee_type`                            varchar(32)        DEFAULT NULL   COMMENT '担保方式',
  `borrower_type`                             varchar(32)        DEFAULT NULL   COMMENT '借款人类型',
  `borrower_name`                             varchar(100)       DEFAULT NULL   COMMENT '借款人姓名',
  `borrower_industry`                         varchar(32)        DEFAULT NULL   COMMENT '借款人行业',
  `birthday`                                  date               DEFAULT NULL   COMMENT '出生日期',
  `age`                                       int(11)            DEFAULT NULL   COMMENT '年龄',
  `province`                                  varchar(32)        DEFAULT NULL   COMMENT '所在省份',
  `city`                                      varchar(32)        DEFAULT NULL   COMMENT '所在城市',
  `annual_income`                             decimal(32,2)      DEFAULT NULL   COMMENT '年收入',
  `account_age`                               float(11,2)        DEFAULT NULL   COMMENT '帐龄',
  `loan_issue_date`                           date               DEFAULT NULL   COMMENT '贷款发放日',
  `loan_expiry_date`                          date               DEFAULT NULL   COMMENT '贷款到期日',
  `frist_repayment_date`                      date               DEFAULT NULL   COMMENT '首次还款日',
  `bag_date`                                  date               DEFAULT NULL   COMMENT '封包日期',
  `package_principal_balance`                 decimal(32,2)      DEFAULT '0.00' COMMENT '封包本金余额',
  `periods`                                   int(11)            DEFAULT NULL   COMMENT '总期数',
  `package_remain_principal`                  decimal(20,2)      DEFAULT NULL   COMMENT '封包时当前剩余本金(元)',
  `package_remain_periods`                    tinyint(4)         DEFAULT NULL   COMMENT '封包时当前剩余期数',
  `wind_control_status`                       varchar(32)        DEFAULT NULL   COMMENT '风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
  `cheat_level`                               varchar(10)        DEFAULT NULL   COMMENT '反欺诈等级:1~5,-1:null,其它:异常值',
  `score_range`                               varchar(10)        DEFAULT NULL   COMMENT '评分等级:1~20,-1:null,其它:异常值',
  `score_level`                               varchar(10)        DEFAULT NULL   COMMENT '评分区间:1~5,-1:null,其它:异常值',
  `create_time`                               timestamp                         COMMENT '创建记录时间',
  `update_time`                               timestamp                         COMMENT '修改时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `ix_asset_package_project_id_bag_id` (`project_id`,`asset_bag_id`,`serial_number`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=161922 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='封包时基础资产信息表';

-- ----------------------------
-- Table structure for t_asset_package_profile
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_package_profile`;
CREATE TABLE `t_asset_package_profile` (
  `id`                                        int(11)            NOT NULL,
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `asset_bag_id`                              varchar(50)        DEFAULT NULL   COMMENT '资产包编号',
  `asset_count`                               int(11)            DEFAULT NULL   COMMENT '资产数量(条)',
  `principal_balance`                         decimal(32,2)      DEFAULT NULL   COMMENT '本金余额(元)',
  `principal_interest_balance`                decimal(32,2)      DEFAULT NULL   COMMENT '本息余额(元)',
  `borrower_count`                            int(11)            DEFAULT NULL   COMMENT '借款人数量(户)',
  `borrower_avg_principal_balance`            decimal(32,2)      DEFAULT NULL   COMMENT '借款人平均本金余额(元)',
  `single_max_principal_balance`              decimal(32,2)      DEFAULT NULL   COMMENT '单笔最大本金余额(元)',
  `single_min_principal_balance`              decimal(32,2)      DEFAULT NULL   COMMENT '单笔最小本金余额(元)',
  `avg_principal_balance`                     decimal(32,2)      DEFAULT NULL   COMMENT '平均本金余额(元)',
  `single_max_ontract_amount`                 decimal(32,2)      DEFAULT NULL   COMMENT '单笔最大合同金额(元)',
  `single_min_ontract_amount`                 decimal(32,2)      DEFAULT NULL   COMMENT '单笔最小合同金额(元)',
  `avg_contract_amount`                       decimal(32,2)      DEFAULT NULL   COMMENT '平均合同金额(元)',
  `single_max_contract_periods`               int(5)             DEFAULT NULL   COMMENT '单笔最大合同期数(月)',
  `single_min_contract_periods`               int(5)             DEFAULT NULL   COMMENT '单笔最小合同期数(月)',
  `avg_contract_periods`                      decimal(10,2)      DEFAULT NULL   COMMENT '平均合同期数(月)',
  `weight_avg_contract_periods`               decimal(10,2)      DEFAULT NULL   COMMENT '加权平均合同期数(月)',
  `single_max_remain_periods`                 int(5)             DEFAULT NULL   COMMENT '单笔最大剩余期数(月)',
  `single_min_remain_periods`                 int(5)             DEFAULT NULL   COMMENT '单笔最小剩余期数(月)',
  `avg_remain_periods`                        decimal(10,2)      DEFAULT NULL   COMMENT '平均剩余期数(月)',
  `weight_avg_remain_periods`                 decimal(10,2)      DEFAULT NULL   COMMENT '加权平均剩余期数(月)',
  `single_max_replay_periods`                 int(5)             DEFAULT NULL   COMMENT '单笔最大已还期数(月)',
  `single_min_replay_periods`                 int(5)             DEFAULT NULL   COMMENT '单笔最小已还期数(月)',
  `avg_replay_periods`                        decimal(10,2)      DEFAULT NULL   COMMENT '平均已还期数(月)',
  `weight_avg_replay_periods`                 decimal(10,2)      DEFAULT NULL   COMMENT '加权平均已还期数(月)',
  `single_max_interest_rate`                  decimal(10,2)      DEFAULT NULL   COMMENT '单笔最大年利率(%)',
  `single_min_interest_rate`                  decimal(10,2)      DEFAULT NULL   COMMENT '单笔最小年利率(%)',
  `avg_interest_rate`                         decimal(10,2)      DEFAULT NULL   COMMENT '平均年利率(%)',
  `weight_avg_interest_rate`                  decimal(10,2)      DEFAULT NULL   COMMENT '加权平均年利率(%)',
  `max_borrower_age`                          decimal(5,2)       DEFAULT NULL   COMMENT '借款人最大年龄(岁)',
  `min_borrower_age`                          decimal(5,2)       DEFAULT NULL   COMMENT '借款人最小年龄(岁)',
  `avg_borrower_age`                          decimal(5,2)       DEFAULT NULL   COMMENT '借款人平均年龄(岁)',
  `weight_avg_borrower_age`                   decimal(5,2)       DEFAULT NULL   COMMENT '借款人加权平均年龄(岁)',
  `max_borrower_annual_income`                decimal(32,2)      DEFAULT NULL   COMMENT '借款人最大年收入(元)',
  `min_borrower_annual_income`                decimal(32,2)      DEFAULT NULL   COMMENT '借款人最小年收入(元)',
  `avg_borrower_annual_income`                decimal(32,2)      DEFAULT NULL   COMMENT '借款人平均年收入(元)',
  `weight_avg_borrower_annual_income`         decimal(32,2)      DEFAULT NULL   COMMENT '借款人加权平均年收入(元)',
  `max_income_debt_ratio`                     decimal(10,2)      DEFAULT NULL   COMMENT '最大收入债务比(%)',
  `min_income_debt_ratio`                     decimal(10,2)      DEFAULT NULL   COMMENT '最小收入债务比(%)',
  `avg_income_debt_ratio`                     decimal(10,2)      DEFAULT NULL   COMMENT '平均收入债务比(%)',
  `weight_avg_income_debt_ratio`              decimal(10,2)      DEFAULT NULL   COMMENT '加权平均收入债务比(%)',
  `mortgage_asset_balance`                    decimal(32,2)      DEFAULT NULL   COMMENT '抵押的资产余额(元)',
  `mortgage_asset_count`                      int(11)            DEFAULT NULL   COMMENT '抵押的资产笔数(笔)',
  `mortgage_init_valuation`                   decimal(32,2)      DEFAULT NULL   COMMENT '抵押初始评估价值(元)',
  `weight_avg_mortgage_rate`                  decimal(10,2)      DEFAULT NULL   COMMENT '加权平均抵押率(%)',
  `mortgage_asset_balance_ratio`              decimal(10,2)      DEFAULT NULL   COMMENT '抵押资产余额占比(%)',
  `mortgage_asset_count_ratio`                decimal(10,2)      DEFAULT NULL   COMMENT '抵押资产笔数占比(%)',
  `min_aging`                                 decimal(10,2)      DEFAULT NULL   COMMENT '最小账龄',
  `max_aging`                                 decimal(10,2)      DEFAULT NULL   COMMENT '最大账龄',
  `avg_aging`                                 decimal(10,2)      DEFAULT NULL   COMMENT '平均账龄',
  `weighted_aging`                            decimal(10,2)      DEFAULT NULL   COMMENT '加权平均账龄',
  `min_surplus_term`                          decimal(10,2)      DEFAULT NULL   COMMENT '最小合同剩余期限',
  `max_surplus_term`                          decimal(10,2)      DEFAULT NULL   COMMENT '最大合同剩余期限',
  `avg_surplus_term`                          decimal(10,2)      DEFAULT NULL   COMMENT '平均合同剩余期限',
  `weighted_surplus_term`                     decimal(10,2)      DEFAULT NULL   COMMENT '加权平均剩余期限',
  `create_time`                               timestamp
  `update_time`                               timestamp,
  PRIMARY KEY (`id`),
  KEY `idx_profile_project_id_asset_bag_id` (`project_id`,`asset_bag_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1075 DEFAULT CHARSET=utf8 COMMENT='封包资产总览';

-- ----------------------------
-- Table structure for t_asset_package_guaranty
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_package_guaranty`;
CREATE TABLE `t_asset_package_guaranty` (
  `id`                                        int(11)            NOT NULL       COMMENT '主键id',
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `serial_number`                             varchar(100)       DEFAULT NULL   COMMENT '借据号',
  `asset_bag_id`                              varchar(50)        DEFAULT NULL   COMMENT '资产包编号',
  `contract_code`                             varchar(64)        DEFAULT NULL   COMMENT '贷款合同编号',
  `guaranty_code`                             varchar(100)       DEFAULT NULL   COMMENT '抵押物编号',
  `pawn_value`                                decimal(20,6)      DEFAULT NULL   COMMENT '评估价格(元)',
  `car_sales_price`                           decimal(20,6)      DEFAULT NULL   COMMENT '车辆销售价格(元)',
  `car_new_price`                             decimal(20,6)      DEFAULT NULL   COMMENT '新车指导价(元)',
  `total_investment`                          decimal(20,6)      DEFAULT NULL   COMMENT '投资总额(元)',
  `purchase_tax_amouts`                       decimal(20,6)      DEFAULT NULL   COMMENT '购置税金额(元)',
  `car_brand`                                 varchar(100)       DEFAULT NULL   COMMENT '车辆品牌',
  `car_type`                                  varchar(32)        DEFAULT NULL   COMMENT '车类型 预定义字段：新车 二手车',
  `create_time`                               timestamp                         COMMENT '创建记录时间',
  `update_time`                               timestamp                         COMMENT '修改时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `ix_asset_package_guaranty` (`project_id`,`asset_bag_id`,`serial_number`,`guaranty_code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=25715 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='封包时基础资产抵押物信息表';











-- ----------------------------
-- Table structure for t_asset_bag
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_bag`;
CREATE TABLE `t_asset_bag` (
  `id`                                        int(11)            NOT NULL,
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `asset_bag_id`                              varchar(50)        DEFAULT NULL   COMMENT '资产包编号',
  `asset_bag_name`                            varchar(300)       DEFAULT NULL   COMMENT '资产包名称',
  `asset_count`                               int(11)            DEFAULT NULL   COMMENT '资产包资产数量',
  `package_principal_balance`                 decimal(20,2)      DEFAULT NULL   COMMENT '封包总本金余额',
  `bag_date`                                  date               DEFAULT NULL   COMMENT '封包日期',
  `bus_product_id`                            varchar(32)        DEFAULT NULL   COMMENT '业务产品编号',
  `bag_conditions`                            varchar(2048)      DEFAULT NULL   COMMENT '封包条件',
  `weight_avg_remain_term`                    decimal(10,2)      DEFAULT NULL   COMMENT '加权平均剩余期限(月)',
  `bag_rate`                                  decimal(20,2)      DEFAULT NULL   COMMENT '封包平均利率',
  `bag_weight_rate`                           decimal(20,2)      DEFAULT NULL   COMMENT '加权平均利率',
  `asset_bag_calc_rule`                       tinyint(2)         DEFAULT NULL   COMMENT '封包规模计算规则 1:按实际剩余本金,2:按还款计划',
  `asset_bag_type`                            tinyint(2)         DEFAULT NULL   COMMENT '资产包类型 1:初始包,2:循环包',
  `status`                                    tinyint(2)         DEFAULT NULL   COMMENT '包状态, 1:未封包,2:已封包,3:已解包,4:封包中,5:封包失败',
  `package_filter_id`                         varchar(50)        DEFAULT NULL   COMMENT '虚拟过滤包id',
  `create_time`                               timestamp                         COMMENT '创建时间',
  `update_time`                               timestamp                         COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_bag_project_id_asset_bag_id` (`project_id`,`asset_bag_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1017 DEFAULT CHARSET=utf8 COMMENT='资产包';











-- ----------------------------
-- Table structure for t_account_cash_flow
-- ----------------------------
-- DROP TABLE IF EXISTS `t_account_cash_flow`;
CREATE TABLE `t_account_cash_flow` (
  `id`                                        int(11)            NOT NULL,
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `product_id`                                int(11)            DEFAULT NULL   COMMENT '产品Id',
  `period`                                    int(11)            DEFAULT NULL   COMMENT '期数',
  `begin_interest_date`                       date               DEFAULT NULL   COMMENT '起息日',
  `pay_interest_date`                         date               DEFAULT NULL   COMMENT '付息日',
  `redemption_date`                           date               DEFAULT NULL   COMMENT '收益兑付日',
  `begin_principal_account_balance`           decimal(20,2)      DEFAULT NULL   COMMENT '本金账户期初余额',
  `end_principal_account_balance`             decimal(20,2)      DEFAULT NULL   COMMENT '本金账户期末余额',
  `begin_interest_account_balance`            decimal(20,2)      DEFAULT NULL   COMMENT '利息账户期初余额',
  `end_interest_account_balance`              decimal(20,2)      DEFAULT NULL   COMMENT '利息账户期末余额',
  `begin_account_balance`                     decimal(20,2)      DEFAULT NULL   COMMENT '本息账户期初余额',
  `end_account_balance`                       decimal(20,2)      DEFAULT NULL   COMMENT '本息账户期末余额',
  `available_amount`                          decimal(32,2)      DEFAULT NULL   COMMENT '当期可动用金额',
  `total_available_amount`                    decimal(32,2)      DEFAULT NULL   COMMENT '累计可动用金额',
  `create_time`                               timestamp
  `update_time`                               timestamp,
  PRIMARY KEY (`id`),
  KEY `idx_account_cash_flow_project_id_product_id` (`project_id`,`product_id`)
) ENGINE=InnoDB AUTO_INCREMENT=709 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='账户现金流';

-- ----------------------------
-- Table structure for t_asset_cash_flow
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_cash_flow`;
CREATE TABLE `t_asset_cash_flow` (
  `id`                                        int(11)            NOT NULL,
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `product_id`                                int(11)            DEFAULT NULL   COMMENT '产品Id',
  `period`                                    int(11)            DEFAULT NULL   COMMENT '期数',
  `period_begin_date`                         date               DEFAULT NULL   COMMENT '本期起始日',
  `period_end_date`                           date               DEFAULT NULL   COMMENT '本期终止日',
  `income_report_date`                        date               DEFAULT NULL   COMMENT '收益报告日',
  `income_transfer_date`                      date               DEFAULT NULL   COMMENT '收益转付日',
  `asset_principal_amount`                    decimal(20,2)      DEFAULT NULL   COMMENT '资产端本金流入金额',
  `asset_interest_amount`                     decimal(20,2)      DEFAULT NULL   COMMENT '资产端利息流入金额',
  `total_principal_amount`                    decimal(20,2)      DEFAULT NULL   COMMENT '累积本金金额',
  `period_begin_principal_balance`            decimal(20,2)      DEFAULT NULL   COMMENT '期初未尝本金余额',
  `period_end_principal_balance`              decimal(20,2)      DEFAULT NULL   COMMENT '期末未尝本金余额',
  `period_begin_total_should_repay_principal` decimal(20,2)      DEFAULT NULL   COMMENT '期初累积应收本金',
  `period_end_total_should_repay_principal`   decimal(20,2)      DEFAULT NULL   COMMENT '期末累积应收本金',
  `period_begin_total_should_repay_interest`  decimal(20,2)      DEFAULT NULL   COMMENT '期初累积应收利息',
  `period_end_total_should_repay_interest`    decimal(20,2)      DEFAULT NULL   COMMENT '期末累积应收利息',
  `create_time`                               timestamp
  `update_time`                               timestamp,
  PRIMARY KEY (`id`),
  KEY `idx_asset_cash_flow_project_id_product_id` (`project_id`,`product_id`)
) ENGINE=InnoDB AUTO_INCREMENT=709 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='资产端现金流';











-- ----------------------------
-- Table structure for t_asset_wind_control_history
-- ----------------------------
-- DROP TABLE IF EXISTS `t_asset_wind_control_history`;
CREATE TABLE `t_asset_wind_control_history` (
  `id`                                        int(11)            NOT NULL       COMMENT '主键id',
  `project_id`                                varchar(32)        DEFAULT NULL   COMMENT '项目编号',
  `asset_bag_id`                              varchar(50)        DEFAULT NULL   COMMENT '资产包编号',
  `asset_type`                                varchar(64)        DEFAULT NULL   COMMENT '资产类型',
  `serial_number`                             varchar(100)       DEFAULT NULL   COMMENT '借据号',
  `contract_code`                             varchar(64)        DEFAULT NULL   COMMENT '贷款合同编号',
  `statistics_date`                           date               DEFAULT NULL   COMMENT '统计日期',
  `status`                                    int(4)             DEFAULT '0'    COMMENT '资产状态(-1征信异常 0准入 1在池 2封包 3发行 4历史',
  `wind_control_status`                       varchar(32)        DEFAULT NULL   COMMENT '当前风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
  `wind_control_status_pool`                  varchar(32)        DEFAULT NULL   COMMENT '入池时风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
  `cheat_level`                               varchar(10)        DEFAULT NULL   COMMENT '反欺诈等级:1~5,-1:null,其它:异常值',
  `score_range`                               varchar(10)        DEFAULT NULL   COMMENT '评分等级:1~20,-1:null,其它:异常值',
  `score_level`                               varchar(10)        DEFAULT NULL   COMMENT '评分区间:1~5,-1:null,其它:异常值',
  `data_source`                               tinyint(2)         DEFAULT NULL   COMMENT '数据来源 1:startLink,2:excelImport',
  `create_time`                               timestamp                         COMMENT '创建记录时间',
  `update_time`                               timestamp                         COMMENT '修改时间',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `basicasset_index` (`project_id`),
  KEY `basicasset_index1` (`serial_number`),
  KEY `basicasset_index2` (`contract_code`),
  KEY `basicasset_index3` (`statistics_date`)
) ENGINE=InnoDB AUTO_INCREMENT=2813968 DEFAULT CHARSET=utf8 COMMENT='基础资产风控评分历史表';

-- ----------------------------
-- Table structure for t_duration_risk_control_result
-- ----------------------------
-- DROP TABLE IF EXISTS `t_duration_risk_control_result`;
CREATE TABLE `t_duration_risk_control_result` (
  `id`                                        int(11)            NOT NULL       COMMENT '主键id',
  `project_id`                                varchar(50)        NOT NULL,
  `monitor_date`                              date               NOT NULL       COMMENT '监控日期',
  `serial_number`                             varchar(50)        NOT NULL       COMMENT '借据号',
  `asset_level`                               int(4)             NOT NULL       COMMENT '资产等级',
  `credit_level`                              int(4)             NOT NULL       COMMENT '信用等级',
  `antifraud_level`                           int(4)             NOT NULL       COMMENT '反欺诈等级',
  `is_black_list`                             tinyint(2)         NOT NULL       COMMENT '是否命中黑名单',
  `create_time`                               timestamp                         COMMENT '创建时间',
  `update_time`                               timestamp                         COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=9555 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT COMMENT='存续风控结果记录表';

-- ----------------------------
-- Table structure for t_wind_control_req_log
-- ----------------------------
-- DROP TABLE IF EXISTS `t_wind_control_req_log`;
CREATE TABLE `t_wind_control_req_log` (
  `id`                                        int(11)            NOT NULL,
  `task_id`                                   varchar(32)        NOT NULL       COMMENT '任务流水号',
  `ent_no`                                    varchar(32)        NOT NULL       COMMENT '商户号',
  `project_id`                                varchar(32)        NOT NULL       COMMENT '项目编号',
  `apply_no`                                  varchar(32)        NOT NULL       COMMENT '申请编号(借据号)',
  `timestamp`                                 varchar(20)        NOT NULL       COMMENT '时间戳',
  `req_url`                                   varchar(100)       NOT NULL       COMMENT '请求url',
  `req_content`                               text               NOT NULL       COMMENT '请求报文',
  `req_step`                                  tinyint(2)         NOT NULL       COMMENT '调用链 1:星连调星云,2:星云调风控,3:内部调用',
  `req_status`                                tinyint(2)         NOT NULL       COMMENT '请求状态 1:成功,2:失败',
  `req_interface`                             tinyint(2)         NOT NULL       COMMENT '请求接口 1:风控审核4号接口,2:风控结果查询新分享本地接口,3:风控结果查询5号接口,4:风控查询6号接口',
  `wind_moment`                               tinyint(10)        DEFAULT NULL   COMMENT '风控时刻（0：入池时，1：资产跟踪)',
  `error_msg`                                 varchar(100)       DEFAULT NULL   COMMENT '错误信息',
  `create_time`                               timestamp
  `update_time`                               timestamp,
  PRIMARY KEY (`id`)                                                          USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=147276 DEFAULT CHARSET=utf8 COMMENT='风控请求日志';

-- ----------------------------
-- Table structure for t_wind_control_resp_log
-- ----------------------------
-- DROP TABLE IF EXISTS `t_wind_control_resp_log`;
CREATE TABLE `t_wind_control_resp_log` (
  `id`                                        int(11)            NOT NULL       COMMENT '主键Id',
  `req_id`                                    int(11)            NOT NULL       COMMENT '请求Id',
  `project_id`                                varchar(32)        NOT NULL       COMMENT '项目编号',
  `apply_no`                                  varchar(32)        NOT NULL       COMMENT '申请编号（借据号）',
  `creid_no`                                  varchar(100)       DEFAULT NULL   COMMENT '身份证号',
  `name`                                      varchar(300)       DEFAULT NULL   COMMENT '姓名',
  `mobile_phone`                              varchar(100)       DEFAULT NULL   COMMENT '手机号',
  `credit_line`                               int(255)           DEFAULT NULL   COMMENT '申请额度，以分为单位，取值范围（0，1000000000）',
  `ret_code`                                  varchar(10)        NOT NULL       COMMENT '应答码',
  `ret_msg`                                   varchar(100)       NOT NULL       COMMENT '应答信息',
  `status`                                    varchar(10)        DEFAULT NULL   COMMENT '查询结果状态（0：未查得，1：查得）',
  `rating_result`                             varchar(10)        DEFAULT NULL   COMMENT '风控状态（Yes：风控通过，No：风控未通过，NA：风控未查得，Error：风控异常，Fail：调用风控失败）',
  `cheat_level`                               varchar(10)        DEFAULT NULL   COMMENT '反欺诈等级（1到5，—1：null，其它：异常值）',
  `score_range`                               varchar(10)        DEFAULT NULL   COMMENT '评分等级（1到20，—1：null，其它：异常值）',
  `score_level`                               varchar(10)        DEFAULT NULL   COMMENT '评分区间（1到5，—1：null，其它：异常值）',
  `ret_content`                               text                              COMMENT '风控结果报文',
  `wind_moment`                               tinyint(10)        DEFAULT NULL   COMMENT '风控时刻（0：入池时，1：资产跟踪）',
  `wind_interface_type`                       tinyint(10)        DEFAULT NULL   COMMENT '查询的风控接口类型（1：风控审核4号接口，2：风控结果查询新分享本地接口，3：风控结果查询5号接口，4：风控查询6号接口)',
  `create_time`                               timestamp
  `update_time`                               timestamp,
  PRIMARY KEY (`id`),
  KEY `IX_WIND_RESP_PROJECTID` (`project_id`,`apply_no`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=956408 DEFAULT CHARSET=utf8 COMMENT='风控结果日志';

DROP DATABASE IF EXISTS `dwb`;
CREATE DATABASE IF NOT EXISTS `dwb`;

--用信申请表
DROP TABLE IF EXISTS `dwb.dwb_loan_apply`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_loan_apply`(
  `org`                           string        COMMENT '机构号',
  `ecif_id`                       string        COMMENT '客户ecif号',
  `credit_id`                     string        COMMENT '授信id',
  `apply_no`                      string        COMMENT '申请号',
  `loan_amt`                      decimal(15,2) COMMENT '贷款金额',
  `loan_term`                     int           COMMENT '贷款期数',
  `repay_type`                    string        COMMENT '1：等额本金，2：等额本息',
  `interest_rate`                 decimal(15,6) COMMENT '利息利率（年化）',
  `penalty_rate`                  decimal(15,6) COMMENT '罚息利率（年化）',
  `user_name`                     string        COMMENT '用户姓名',
  `id_no`                         string        COMMENT '证件号',
  `bank_card_no`                  string        COMMENT '银行卡号',
  `mobile_no`                     string        COMMENT '银行预留手机号',
  `loan_usage`                    string        COMMENT '1：日常消费，2：汽车加油，3：修车保养，4：购买汽车，5：医疗支出，6：教育深造，7：房屋装修，8：旅游出行，9：其他消费',
  `process_date`                  string        COMMENT '处理时间',
  `apply_date`                    string        COMMENT '用信申请时间',
  `apply_time`                    bigint        COMMENT '用信申请日期',
  `product_code`                  string        COMMENT '产品编码',
  `due_bill_no`                   string        COMMENT '借据号',
  `status`                        int           COMMENT '借款状态（1: 放款成功，2: 放款失败，3: 处理中）',
  `message`                       string        COMMENT '借款信息：包括失败原因',
  `issueTime`                     bigint        COMMENT '放款时间，放款成功后必填',
  `loanAmount`                    decimal(15,6) COMMENT '贷款金额/分',
  `channel_id`                    string        COMMENT '渠道号'
) COMMENT 'DWB用信申请表'
PARTITIONED BY (p_type string COMMENT '项目类型')
STORED AS PARQUET;


DROP TABLE IF EXISTS `dwb.dwb_risk_control_feature`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_risk_control_feature`(
  `org`                           string        COMMENT '机构号',
  `ecif_id`                       string        COMMENT '客户ecif号',
  `credit_id`                     string        COMMENT '授信id',
  `apply_no`                      string        COMMENT '申请号',
  `risk_score`                    string        COMMENT '曼指数评分',
  `order_start_stability_level`   string        COMMENT '打车地点稳定性等级',
  `consume_stability_level`       string        COMMENT '打车消费稳定性等级',
  `imei_freq_change_3m`           string        COMMENT '近3个月设备是否频繁更换',
  `phone_imei_level_3m`           string        COMMENT '3个月内同一手机imei一级关联的手机号数量等级',
  `imei_phone_level_3m`           string        COMMENT '3个月内同一手机号一级关联的imei数量等级',
  `device_stability_1year`        string        COMMENT '近1年移动设备是否频繁更换',
  `regist_month_level`            string        COMMENT '注册时长等级',
  `night_ordernum_level_1m`       string        COMMENT '近1个月内深夜打车订单数占比等级',
  `cost_level_1m`                 string        COMMENT '近1个月平台消费等级',
  `cost_level_3m`                 string        COMMENT '近3个月平台消费等级',
  `is_often_used_dev`             string        COMMENT '是否滴滴常用设备',
  `is_often_used_addr`            string        COMMENT '是否为滴滴常用地址',
  `idcard_frontphoto_uploadtime`  string        COMMENT '身份证正面拍摄/上传时间（上传时间）（毫秒时间戳）',
  `idcard_backphoto_uploadtime`   string        COMMENT '身份证反面拍摄/上传时间（上传时间）（毫秒时间戳）',
  `idcard_frontphoto_shoottimes`  string        COMMENT '身份证正面拍摄次数',
  `photo_match_fail_times`        string        COMMENT '大头照比对失败次数',
  `company_address_inputtime`     string        COMMENT '工作单位名称输入时长（毫秒）',
  `contact_mobile_inputtime`      string        COMMENT '联系人电话输入时长（毫秒）',
  `home_address_inputtime`        string        COMMENT '家庭地址输入时长（毫秒）',
  `product_page_staytime`         string        COMMENT '产品介绍页逗留时长（毫秒）',
  `product_page_first_viewtime`   string        COMMENT '首次进入产品介绍页时间（毫秒时间戳）',
  `access_channel`                string        COMMENT '进件渠道',
  `bind_card_time`                string        COMMENT '银行卡签约时间（毫秒时间戳）',
  `repayplan_page_staytime`       string        COMMENT '还款计划表页面停留时长（毫秒）',
  `loan_term_modifytimes`         string        COMMENT '借款期限修改次数',
  `is_same_apply_imei`            string        COMMENT '本次提现使用imei与申请时是否一致',
  `is_sanme_aplly_city`           string        COMMENT '本次提现所在城市区与申请时是否一致',
  `acount_id`                     string        COMMENT '用户账号',
  `longtitude_md5`                string        COMMENT 'gps经度',
  `latitude_md5`                  string        COMMENT 'gps纬度',
  `gps_apply_city_md5`            string        COMMENT '申请GPS城市',
  `device_id`                     string        COMMENT '设备指纹',
  `open_id_md5`                   string        COMMENT 'openid加密',
  `deal_date`                     string        COMMENT '处理时间',
  `process_date`                  string        COMMENT '批量时间',
  `create_time`                   string        COMMENT '创建时间'
) COMMENT 'DWB风控用户特征信息'
PARTITIONED BY (p_type string COMMENT '项目类型')
STORED AS PARQUET;


DROP TABLE IF EXISTS `dwb.dwb_loan`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_loan`(
  `ecif_id`                       string        COMMENT 'ecif号',
  `loan_id`                       string        COMMENT '借据ID',
  `channel_id`                    string        COMMENT '渠道ID',
  `capital_id`                    string        COMMENT '资金方ID',
  `product_code`                  string        COMMENT '产品编码',
  `due_bill_no`                   string        COMMENT '借据号',
  `register_date`                 string        COMMENT '放款日',
  `loan_init_principal`           decimal(15,2) COMMENT '贷款本金',
  `loan_init_term`                int           COMMENT '贷款总期数',
  `loan_init_interest`            decimal(15,2) COMMENT '贷款总利息',
  `loan_init_fee`                 decimal(15,2) COMMENT '贷款总费用',
  `remain_principal`              decimal(15,2) COMMENT '剩余本金',
  `remain_interest`               decimal(15,2) COMMENT '剩余利息',
  `loan_principal`                decimal(15,2) COMMENT '应还本金',
  `loan_interest`                 decimal(15,2) COMMENT '应还利息',
  `loan_penalty`                  decimal(15,2) COMMENT '应还罚息',
  `loan_fee`                      decimal(15,2) COMMENT '应还费用',
  `paid_principal`                decimal(15,2) COMMENT '已还本金',
  `paid_interest`                 decimal(15,2) COMMENT '已还利息',
  `paid_penalty`                  decimal(15,2) COMMENT '已还罚息',
  `paid_fee`                      decimal(15,2) COMMENT '已还费用',
  `overdue_principal`             decimal(15,2) COMMENT '逾期本金',
  `overdue_interest`              decimal(15,2) COMMENT '逾期利息',
  `overdue_fee`                   decimal(15,2) COMMENT '逾期费用',
  `curr_term`                     int           COMMENT '当前期数',
  `paid_out_date`                 string        COMMENT '贷款还款日期',
  `overdue_date`                  string        COMMENT '逾期起始日期',
  `overdue_days`                  int           COMMENT '逾期天数',
  `cpd_begin_date`                string        COMMENT 'cpd开始时间',
  `loan_expire_date`              string        COMMENT '贷款到期日',
  `interest_rate`                 decimal(15,6) COMMENT '利息利率',
  `fee_rate`                      decimal(15,6) COMMENT '费用利率',
  `penalty_rate`                  decimal(15,6) COMMENT '罚息利率',
  `contr_nbr`                     string        COMMENT '合同号',
  `loan_usage`                    string        COMMENT '贷款用途',
  `application_no`                string        COMMENT '进件号',
  `loan_status`                   string        COMMENT '借据状态',
  `create_time`                   bigint        COMMENT '业务借据创建时间',
  `loan_terminal_code`            string        COMMENT '分期终止原因代码 P|提前还款 D|逾期自动终止 T|退货终止 F|强制结清终止',
  `paid_out_type`                 string        COMMENT '还清类型',
  `flag`                          string        COMMENT '状态 1新增，2非新增',
  `cycle_day`                     string        COMMENT '',
  `user_field1`                   string        COMMENT '备用字段'
) COMMENT 'DWB借据表'
PARTITIONED BY (d_date string COMMENT '分区日期',p_type string COMMENT '项目类型')
STORED AS PARQUET;


DROP TABLE IF EXISTS `dwb.dwb_repay_schedule`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_repay_schedule`(
  `schedule_id`                   string        COMMENT '还款计划ID',
  `loan_id`                       string        COMMENT '借据ID',
  `channel_id`                    string        COMMENT '渠道ID',
  `capital_id`                    string        COMMENT '资金方ID',
  `term`                          int           COMMENT '当前期数',
  `due_bill_no`                   string        COMMENT '借据号',
  `loan_pmt_due_date`             string        COMMENT '到期还款日',
  `register_date`                 string        COMMENT '放款日',
  `remain_principal`              decimal(15,2) COMMENT '剩余本金',
  `remain_interest`               decimal(15,2) COMMENT '剩余利息',
  `loan_principal`                decimal(15,2) COMMENT '应还本金',
  `loan_interest`                 decimal(15,2) COMMENT '应还利息',
  `loan_penalty`                  decimal(15,2) COMMENT '应还罚息',
  `loan_fee`                      decimal(15,2) COMMENT '应还费用',
  `paid_principal`                decimal(15,2) COMMENT '已还本金',
  `paid_interest`                 decimal(15,2) COMMENT '已还利息',
  `paid_penalty`                  decimal(15,2) COMMENT '已还罚息',
  `paid_fee`                      decimal(15,2) COMMENT '已还费用',
  `paid_mult`                     decimal(15,2) COMMENT '已还滞纳金',
  `reduce_principal`              decimal(15,2) COMMENT '减免本金',
  `reduce_interest`               decimal(15,2) COMMENT '减免利息',
  `reduce_fee`                    decimal(15,2) COMMENT '减免费用',
  `reduce_penalty`                decimal(15,2) COMMENT '减免罚息',
  `reduce_mult`                   decimal(15,2) COMMENT '减免滞纳金',
  `penalty_acru`                  decimal(15,2) COMMENT '罚息累计金额',
  `paid_out_date`                 string        COMMENT '还清日期',
  `paid_out_type`                 string        COMMENT '还清类型',
  `schedule_status`               string        COMMENT '还款计划状态 N|正常 O|逾期 F|已还清',
  `grace_date`                    string        COMMENT '宽限日',
  `flag`                          string        COMMENT '状态 1新增，2非新增'
) COMMENT 'dwb还款计划表'
PARTITIONED BY (d_date string COMMENT '分区日期',p_type string COMMENT '项目类型')
STORED AS PARQUET;


DROP TABLE IF EXISTS `dwb.dwb_repay_hst`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_repay_hst`(
  `payment_id`                    string        COMMENT '主键',
  `loan_id`                       string        COMMENT 'load_id',
  `due_bill_no`                   string        COMMENT '借据号',
  `bnp_type`                      string        COMMENT 'Pricinpal：本金，Interest：利息，Penalty：罚息，Mulct：罚金，Compound：复利，CardFee：年费，OverLimitFee：超限费，LatePaymentCharge：滞纳金，NSFCharge：资金不足罚金，TXNFee：交易费，TERMFee：手续费，SVCFee：服务费',
  `repay_amt`                     decimal(15,2) COMMENT '实还金额',
  `batch_date`                    string        COMMENT '批量日期',
  `term`                          int           COMMENT '期数',
  `order_id`                      string        COMMENT '订单ID',
  `txn_seq`                       string        COMMENT '交易序列号',
  `txn_date`                      string        COMMENT '交易日期',
  `loan_status`                   string        COMMENT '借据状态'
) COMMENT 'DWB还款分配表'
PARTITIONED BY (p_type string COMMENT '项目类型')
STORED AS PARQUET;


DROP TABLE IF EXISTS `dwb.dwb_order`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_order`(
  `ecif_id`                       string        COMMENT 'ecif_id',
  `channel_id`                    string        COMMENT '渠道ID',
  `capital_id`                    string        COMMENT '资金方ID',
  `order_id`                      string        COMMENT '订单ID',
  `loan_id`                       string        COMMENT '借据ID',
  `due_bill_no`                   string        COMMENT '借据号',
  `apply_no`                      string        COMMENT '申请件编号',
  `product_code`                  string        COMMENT '产品号',
  `service_id`                    string        COMMENT 'service_id',
  `command_type`                  string        COMMENT '指令类型 SPA|单笔代付 SDB|单笔代扣 BDB|批量代扣 BDA|批量代付',
  `purpose`                       string        COMMENT '支付用途',
  `business_date`                 string        COMMENT '业务时间',
  `term`                          int           COMMENT '期数',
  `order_time`                    bigint        COMMENT '订单时间',
  `order_status`                  string        COMMENT '订单状态',
  `loan_usage`                    string        COMMENT '订单类型',
  `repay_way`                     string        COMMENT '还款方式 N|正常 O|逾期 T|提前还款 P|提前结清',
  `txn_amt`                       decimal(15,2) COMMENT '交易金额',
  `bank_trade_no`                 string        COMMENT '交易流水号',
  `bank_acct`                     string        COMMENT '银行卡账户号',
  `bank_acct_name`                string        COMMENT '银行卡账户名称',
  `memo`                          string        COMMENT '备注',
  `code`                          string        COMMENT '支付状态码',
  `message`                       string        COMMENT '支付状态描述',
  `flag`                          string        COMMENT '状态 1新增，2非新增'
) COMMENT 'DWB订单表'
PARTITIONED BY (d_date string COMMENT '分区日期',p_type string COMMENT '项目类型')
STORED AS PARQUET;


--业务数据库客户信息表
DROP TABLE IF EXISTS `dwb.dwb_bussiness_customer_info`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_bussiness_customer_info`(
  `org`                           string        COMMENT '机构号',
  `channel`                       string        COMMENT '渠道',
  `cust_id`                       string        COMMENT '客户号 : ///@uuidseq',
  `outer_cust_id`                 string        COMMENT '外部客户号',
  `id_no`                         string        COMMENT '证件号码',
  `id_type`                       string        COMMENT '证件类型',
  `name`                          string        COMMENT '姓名',
  `mobie`                         string        COMMENT '移动电话',
  `cust_lmt_id`                   string        COMMENT '客户额度id',
  `create_time`                   string        COMMENT '创建时间',
  `create_user`                   string        COMMENT '创建人',
  `jpa_version`                   int           COMMENT 'jpa_version',
  `overflow_amt`                  decimal(20,6) COMMENT '溢缴款',
  `lst_upd_time`                  string        COMMENT '最后一次更新时间',
  `lst_upd_user`                  string        COMMENT '最后一次更新人',
  `gender`                        string        COMMENT '',
  `bir_date`                      string        COMMENT '',
  `marital_status`                string        COMMENT '',
  `permanent_address`             string        COMMENT '',
  `now_address`                   string        COMMENT '',
  `bank_no`                       string        COMMENT '',
  `apply_no`                      string        COMMENT '申请编号',
  `city`                          string        COMMENT '',
  `job_type`                      string        COMMENT '',
  `province`                      string        COMMENT '',
  `country`                       string        COMMENT '',
  `ecif_id`                       string        COMMENT ''
) COMMENT '业务数据库客户表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


---dwb 联系人信息表
DROP TABLE IF EXISTS `dwb.dwb_customer_linkman_info`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_customer_linkman_info`(
  `linkman_name`                  string        COMMENT '联系人名称',
  `linkman_phone`                 string        COMMENT '联系人电话',
  `ecif`                          string        COMMENT 'ecif',
  `cust_name`                     string        COMMENT '客户姓名',
  `relation_ship`                 string        COMMENT '联系人与申请人关系',
  `linkman_addr`                  string        COMMENT '联系人地址',
  `linkman_idcard`                string        COMMENT '联系人身份证号',
  `org`                           string        COMMENT '租户号',
  `create_time`                   string        COMMENT '创建时间',
  `update_time`                   string        COMMENT '更新时间'
) COMMENT '客户联系人信息表'
PARTITIONED BY (d_date string COMMENT '分区日期',p_type string COMMENT '项目类型')
STORED AS PARQUET;


-- 银行卡信息表
DROP TABLE IF EXISTS `dwb.dwb_bank_card_info`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_bank_card_info`(
  `card_id`                       string        COMMENT '银行卡id',
  `CUST_ID`                       string        COMMENT '客户id',
  `due_bill_no`                   string        COMMENT '借据号',
  `bank_card_id_no`               string        COMMENT '证件号码',
  `bank_card_name`                string        COMMENT '姓名',
  `bank_card_phone`               string        COMMENT '手机号',
  `pay_channel`                   int           COMMENT '支付渠道 1 宝付  2 通联',
  `card_flag`                     string        COMMENT '绑卡标志',
  `agreement_no`                  string        COMMENT '绑卡协议编号',
  `bank_cardNo`                   string        COMMENT '银行卡号',
  `bank_no`                       string        COMMENT '银行编号',
  `bank_name`                     string        COMMENT '银行名称',
  `province`                      string        COMMENT '银行分行-省',
  `city`                          string        COMMENT '银行分行-市',
  `tied_card_time`                string        COMMENT '绑卡时间',
  `ecif`                          string        COMMENT 'ecif',
  `org`                           string        COMMENT 'org'
) COMMENT '银行卡信息表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


---dwb 绑卡信息变更变
DROP TABLE IF EXISTS `dwb.dwb_bind_card_change`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_bind_card_change`(
  `CHANGE_ID`                     string        COMMENT '主键',
  `ORG`                           string        COMMENT '租户号',
  `CUST_ID`                       string        COMMENT '客户id',
  `DUE_BILL_NO`                   string        COMMENT '借据号',
  `OLD_BANK_CARD_ID_NO`           string        COMMENT '旧证件号码',
  `OLD_BANK_CARD_NAME`            string        COMMENT '旧姓名',
  `OLD_BANK_CARD_PHONE`           string        COMMENT '旧手机号',
  `OLD_BANK_CARD_NO`              string        COMMENT '旧银行卡号',
  `OLD_PAY_CHANNEL`               string        COMMENT '旧支付渠道:1-宝付,2-通联',
  `OLD_AGREEMENT_NO`              string        COMMENT '旧绑卡协议编号',
  `OLD_CARD_FLAG`                 string        COMMENT '旧标识:N|正常,F|非客户本人,共同借款人,配偶',
  `NEW_BANK_CARD_ID_NO`           string        COMMENT '新证件号码',
  `NEW_BANK_CARD_NAME`            string        COMMENT '新姓名',
  `NEW_BANK_CARD_PHONE`           string        COMMENT '旧手机号',
  `NEW_BANK_CARD_NO`              string        COMMENT '新银行卡号',
  `NEW_PAY_CHANNEL`               string        COMMENT '新支付渠道:1-宝付,2-通联',
  `NEW_AGREEMENT_NO`              string        COMMENT '新绑卡协议编号',
  `NEW_CARD_FLAG`                 string        COMMENT '新标识:N|正常,F|非客户本人,共同借款人,配偶',
  `CREATE_TIME`                   string        COMMENT '',
  `LST_UPD_TIME`                  string        COMMENT '',
  `JPA_VERSION`                   string        COMMENT '',
  `old_ecif`                      string        COMMENT '旧身份证号对应的ecif',
  `new_ecif`                      string        COMMENT '新身份证号对应的ecif'
) COMMENT '绑卡信息更新信息表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


--授信申请表
DROP TABLE IF EXISTS `dwb.dwb_credit_apply`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_credit_apply`(
  `org`                           string        COMMENT '租户号(FK)',
  `APPLY_ID`                      string        COMMENT 'APPLY_ID',
  `channel_id`                    string        COMMENT '渠道ID',
  `ecif_id`                       string        COMMENT 'ecif_id',
  `product_code`                  string        COMMENT '产品编号',
  `apply_no`                      string        COMMENT '进件号',
  `apply_date`                    string        COMMENT '授信申请日期',
  `apply_time`                    bigint        COMMENT '授信申请时间',
  `apply_amt`                     decimal(15,2) COMMENT '申请金额',
  `apply_status`                  string        COMMENT '申请结果',
  `apply_type`                    string        COMMENT '申请类型',
  `contr_no`                      string        COMMENT '合同号',
  `resp_code`                     string        COMMENT '授信结果码',
  `resp_msg`                      string        COMMENT '结果描述',
  `credit_time`                   bigint        COMMENT '授信时间',
  `process_date`                  string        COMMENT '处理时间',
  `start_date`                    string        COMMENT '授信开始时间',
  `end_date`                      string        COMMENT '授信结束时间',
  `limit_amt`                     decimal(15,2) COMMENT '初始授信额度'
) COMMENT 'DWB授信申请表'
PARTITIONED BY (p_type string COMMENT '项目类型')
STORED AS PARQUET;


--授信结果表
DROP TABLE IF EXISTS `dwb.dwb_credit_result`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_credit_result`(
  `org`                           string        COMMENT '租户号',
  `credit_result`                 string        COMMENT '授信状态|1:通过 2:不通过 3:已受理处理中',
  `credit_id`                     string        COMMENT '资金方申请单号',
  `apply_id`                      string        COMMENT '滴滴授信申请id',
  `ecif_id`                       string        COMMENT 'ecif_id',
  `product_code`                  string        COMMENT '产品编号',
  `amount`                        decimal(15,2) COMMENT '授信额度',
  `interest_rate`                 decimal(15,6) COMMENT '利率',
  `interest_penalty_rate`         decimal(15,6) COMMENT '罚息率',
  `start_date`                    string        COMMENT '授信有效起始日期',
  `end_date`                      string        COMMENT '授信有效截止日期',
  `lockdown_end_time`             bigint        COMMENT '禁闭期截止日期',
  `reject_code`                   string        COMMENT '授信拒绝码',
  `reject_reason`                 string        COMMENT '授信拒绝原因',
  `credit_time`                   bigint        COMMENT '授信时间'
) COMMENT 'DWB授信结果表'
PARTITIONED BY (d_date string COMMENT '分区日期',p_type string COMMENT '项目类型')
STORED AS PARQUET;


--额度流水表
DROP TABLE IF EXISTS `dwb.dwb_credit_change_record`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_credit_change_record`(
  `org`                           string        COMMENT '租户号',
  `apply_id`                      string        COMMENT '进件ID',
  `op_type`                       string        COMMENT '变更类型|0:提高额度 1:降低利息 9:调整有效期',
  `op_step`                       string        COMMENT '0—预提交1—执行当 opType 为 1 即降息情况下，opStep 分两步，先预提交后执行当 opType 为 0 即提高额度情况下，opStep 直接传 1 即可',
  `op_date`                       string        COMMENT '操作日期',
  `op_time`                       bigint        COMMENT '操作时间',
  `reqSn`                         string        COMMENT '额度变更流水号',
  `ecif_id`                       string        COMMENT 'ecif_id',
  `product_code`                  string        COMMENT '产品编号',
  `oldAmount`                     decimal(15,2) COMMENT '变更前授信额度',
  `newAmount`                     decimal(15,2) COMMENT '变更后授信额度',
  `oldInterestRate`               decimal(15,6) COMMENT '变更前利率',
  `newInterestRate`               decimal(15,6) COMMENT '变更后利率',
  `odsInterestPenaltyRate`        decimal(15,6) COMMENT '变更前罚息率',
  `newInterestPenaltyRate`        decimal(15,6) COMMENT '变更后罚息率',
  `oldEndDate`                    string        COMMENT '变更前有效期截止日期',
  `newEndDate`                    string        COMMENT '变更后有效期截止日期',
  `remark`                        string        COMMENT '变更原因',
  `error_code`                    string        COMMENT '授信变更结果',
  `error_msg`                     string        COMMENT '授信变更结果描述',
  `create_time`                   bigint        COMMENT '创建时间'
) COMMENT 'DWB额度流水变更表'
PARTITIONED BY (d_date string COMMENT '分区日期',p_type string COMMENT '项目类型')
STORED AS PARQUET;


--风控结果表
DROP TABLE IF EXISTS `dwb.dwb_risk_control_result`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_risk_control_result`(
  `channel_id`                    string        COMMENT '渠道ID',
  `capital_id`                    string        COMMENT '资金方ID',
  `application_no`                string        COMMENT '进件号',
  `contr_nbr`                     string        COMMENT '合同号',
  `assess_date`                   string        COMMENT '评估日期',
  `process_result`                string        COMMENT '评估结果',
  `amt`                           decimal(15,2) COMMENT '金额',
  `outside_reject_reason`         string        COMMENT '对外拒绝原因',
  `code`                          string        COMMENT '拒绝码',
  `reason`                        string        COMMENT '拒绝原因',
  `process_date`                  string        COMMENT '处理时间',
  `ecif_id`                       string        COMMENT 'ecif',
  `org`                           string        COMMENT 'org',
  `product_code`                  string        COMMENT '产品编码'
) COMMENT 'DWB风控结果表'
PARTITIONED BY (p_type string COMMENT '项目类型')
STORED AS PARQUET;


--dwb催收案件信息表
DROP TABLE IF EXISTS `dwb.dwb_case_main`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_case_main`(
  `org`                           string        COMMENT '机构号',
  `ecif_id`                       string        COMMENT 'ecif号',
  `case_main_id`                  string        COMMENT '案件编号',
  `cust_no`                       string        COMMENT '客户编号',
  `id_type`                       string        COMMENT '证件类型',
  `id_no`                         string        COMMENT '证件号码',
  `cust_name`                     string        COMMENT '客户姓名',
  `mobile_no`                     string        COMMENT '移动电话',
  `pre_function_code`             string        COMMENT '上一逾期阶段',
  `function_code`                 string        COMMENT '逾期阶段',
  `risk_rank`                     int           COMMENT '风险等级',
  `status_code`                   string        COMMENT '案件状态代码',
  `coll_state`                    string        COMMENT '催收状态',
  `is_close`                      string        COMMENT '是否结案',
  `is_wo`                         string        COMMENT '是否结清',
  `deal_stat`                     string        COMMENT '处理状态',
  `over_due_date`                 string        COMMENT '逾期起始日',
  `over_due_days`                 int           COMMENT '逾期天数',
  `shd_amt`                       decimal(17,2) COMMENT '应还款额',
  `shd_principal`                 decimal(17,2) COMMENT '应还本金',
  `shd_interest`                  decimal(17,2) COMMENT '应还利息',
  `shd_fee`                       decimal(17,2) COMMENT '应还手续费',
  `overdue_principal`             decimal(17,2) COMMENT '逾期未还本金',
  `overdue_comp_inst`             decimal(17,2) COMMENT '逾期未还复利',
  `overdue_penalty`               decimal(17,2) COMMENT '逾期未还罚息',
  `overdue_fee`                   decimal(17,2) COMMENT '逾期未还手续费',
  `overdue_insterent`             decimal(17,2) COMMENT '逾期未还利息',
  `sex`                           string        COMMENT '性别',
  `is_stop_coll`                  string        COMMENT '是否停催',
  `task_pool`                     string        COMMENT '催收任务池',
  `create_user`                   string        COMMENT '创建人',
  `create_time`                   string        COMMENT '创建时间',
  `lst_upd_user`                  string        COMMENT '最后修改人',
  `lst_upd_time`                  string        COMMENT '最后修改时间',
  `channel_id`                    string        COMMENT '渠道Id',
  `channel_name`                  string        COMMENT '渠道名称',
  `product_name`                  string        COMMENT '产品名称',
  `org_code`                      string        COMMENT '所属机构',
  `loan_fee`                      decimal(15,2) COMMENT '放款金额',
  `shd_penalty`                   decimal(17,2) COMMENT '应还罚息',
  `shd_management_fee`            decimal(17,2) COMMENT '应还账户管理费',
  `overdue_amt`                   decimal(17,2) COMMENT '剩余应还总额',
  `overdue_management_fee`        decimal(17,2) COMMENT '逾期应还账户管理费'
) COMMENT 'dwb催收案件信息表'
PARTITIONED BY (d_date string COMMENT '分区日期',p_type string COMMENT '项目类型')
STORED AS PARQUET;


--dwb催收子案件信息表
DROP TABLE IF EXISTS `dwb.dwb_sub_case_info`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_sub_case_info`(
  `org`                           string        COMMENT '机构号',
  `case_sub_id`                   string        COMMENT '子案件编号',
  `ecif_id`                       string        COMMENT 'ecif号',
  `org_code`                      string        COMMENT '所属机构',
  `case_main_id`                  string        COMMENT '案件编号',
  `cust_no`                       string        COMMENT '客户号',
  `id_type`                       string        COMMENT '证件类型',
  `id_no`                         string        COMMENT '证件号码',
  `cust_name`                     string        COMMENT '客户姓名',
  `mobile_no`                     string        COMMENT '移动电话',
  `function_code`                 string        COMMENT '逾期阶段',
  `risk_rank`                     string        COMMENT '风险等级',
  `status_code`                   string        COMMENT '案件状态代码',
  `is_close`                      string        COMMENT '是否结案',
  `is_wo`                         string        COMMENT '是否结清',
  `collector`                     string        COMMENT '催收员',
  `deal_stat`                     string        COMMENT '处理状态',
  `over_due_days`                 int           COMMENT '逾期天数',
  `overdue_principal`             decimal(15,2) COMMENT '逾期未还本金',
  `overdue_penalty`               decimal(15,2) COMMENT '逾期未还罚息',
  `overdue_fee`                   decimal(15,2) COMMENT '逾期未还费用',
  `overdue_insterent`             decimal(15,2) COMMENT '逾期未还利息',
  `sex`                           string        COMMENT '性别',
  `collect_out_date`              string        COMMENT '出催日期',
  `collect_assign_status`         string        COMMENT '分配状态',
  `create_time`                   bigint        COMMENT '创建时间',
  `overdue_amt`                   decimal(15,2) COMMENT '逾期未还金额',
  `over_due_nper`                 int           COMMENT '逾期期数',
  `coll_collection`               string        COMMENT '催收组',
  `loan_id`                       string        COMMENT '借据号',
  `channel_id`                    string        COMMENT '渠道Id',
  `channel_name`                  string        COMMENT '渠道名称',
  `product_name`                  string        COMMENT '产品名称',
  `product_id`                    string        COMMENT '产品ID',
  `oa_state`                      string        COMMENT '委外状态',
  `contr_nbr`                     string        COMMENT '合同编号',
  `last_deal_date`                string        COMMENT '上次处理日期',
  `repay_date`                    string        COMMENT '还款日期',
  `input_coll_date`               string        COMMENT '入催时间',
  `oa_allot_state`                string        COMMENT '委外分案状态',
  `allot_case_batch_no`           string        COMMENT '分案批次号',
  `pre_allot_case_batch_no`       string        COMMENT '上一次分案批次号',
  `input_coll_amount`             decimal(15,2) COMMENT '入催金额',
  `loan_period`                   string        COMMENT '分期总期数',
  `is_grace`                      string        COMMENT '是否在宽限期内',
  `scene_id`                      string        COMMENT '场景方ID',
  `capital_id`                    string        COMMENT '资金方ID',
  `input_coll_overdue_principal`  decimal(15,2) COMMENT '入催时逾期本金',
  `input_coll_overdue_interest`   decimal(15,2) COMMENT '入催时逾期利息',
  `input_coll_overdue_penalty`    decimal(15,2) COMMENT '入催时逾期罚息',
  `input_coll_overdue_fee`        decimal(15,2) COMMENT '入催时逾期费用',
  `input_coll_term`               int           COMMENT '入催时所在期次',
  `input_coll_overdue_days`       int           COMMENT '入催时逾期天数',
  `curr_remainder_principal`      decimal(15,2) COMMENT '当前剩余本金',
  `curr_term`                     int           COMMENT '当前所在期次',
  `curr_input_coll_days`          int           COMMENT '当前入催天数'
) COMMENT '子案件信息表'
PARTITIONED BY (d_date string COMMENT '分区日期',p_type string COMMENT '项目类型')
STORED AS PARQUET;


--子案件出催信息表
DROP TABLE IF EXISTS `dwb.dwb_sub_case_out_info`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_sub_case_out_info`(
  `id`                            string        COMMENT '出催ID',
  `ecif_id`                       string        COMMENT 'ecif号',
  `org`                           string        COMMENT '机构号',
  `case_sub_id`                   string        COMMENT '子案件编号',
  `case_main_id`                  string        COMMENT '案件编号',
  `org_code`                      string        COMMENT '所属机构',
  `cust_no`                       string        COMMENT '客户号',
  `id_type`                       string        COMMENT '证件类型',
  `id_no`                         string        COMMENT '证件号码',
  `cust_name`                     string        COMMENT '客户姓名',
  `mobile_no`                     string        COMMENT '移动电话',
  `function_code`                 string        COMMENT '逾期阶段',
  `risk_rank`                     string        COMMENT '风险等级',
  `status_code`                   string        COMMENT '案件状态代码',
  `coll_state`                    string        COMMENT '催收状态',
  `is_close`                      string        COMMENT '是否结案',
  `is_wo`                         string        COMMENT '是否结清',
  `collector`                     string        COMMENT '催收员',
  `deal_stat`                     string        COMMENT '处理状态',
  `over_due_days`                 int           COMMENT '逾期天数',
  `overdue_principal`             decimal(15,2) COMMENT '逾期未还本金',
  `overdue_penalty`               decimal(15,2) COMMENT '逾期未还罚息',
  `overdue_fee`                   decimal(15,2) COMMENT '逾期未还费用',
  `overdue_insterent`             decimal(15,2) COMMENT '逾期未还利息',
  `sex`                           string        COMMENT '性别',
  `is_stop_coll`                  string        COMMENT '是否停催',
  `collect_out_date`              string        COMMENT '出催日期',
  `collect_assign_status`         string        COMMENT '分配状态',
  `create_time`                   bigint        COMMENT '创建时间',
  `overdue_amt`                   decimal(15,2) COMMENT '逾期未还金额',
  `over_due_nper`                 int           COMMENT '逾期期数',
  `coll_collection`               string        COMMENT '催收组',
  `loan_id`                       string        COMMENT '借据号',
  `channel_id`                    string        COMMENT '渠道Id',
  `channel_name`                  string        COMMENT '渠道名称',
  `product_name`                  string        COMMENT '产品名称',
  `product_id`                    string        COMMENT '产品ID',
  `oa_state`                      string        COMMENT '委外状态',
  `contr_nbr`                     string        COMMENT '合同编号',
  `last_deal_date`                string        COMMENT '上次处理日期',
  `repay_date`                    string        COMMENT '还款日期',
  `input_coll_date`               string        COMMENT '入催时间',
  `oa_allot_state`                string        COMMENT '委外分案状态',
  `allot_case_batch_no`           string        COMMENT '分案批次号',
  `pre_allot_case_batch_no`       string        COMMENT '上一次分案批次号',
  `input_coll_amount`             decimal(15,2) COMMENT '入催金额',
  `loan_period`                   string        COMMENT '分期总期数',
  `is_grace`                      string        COMMENT '是否在宽限期内',
  `scene_id`                      string        COMMENT '场景方ID',
  `capital_id`                    string        COMMENT '资金方ID',
  `input_coll_overdue_principal`  decimal(15,2) COMMENT '入催时逾期本金',
  `input_coll_overdue_interest`   decimal(15,2) COMMENT '入催时逾期利息',
  `input_coll_overdue_penalty`    decimal(15,2) COMMENT '入催时逾期罚息',
  `input_coll_overdue_fee`        decimal(15,2) COMMENT '入催时逾期费用',
  `input_coll_term`               int           COMMENT '入催时所在期次',
  `input_coll_overdue_days`       int           COMMENT '入催时逾期天数',
  `curr_remainder_principal`      decimal(15,2) COMMENT '当前剩余本金',
  `curr_term`                     int           COMMENT '当前所在期次',
  `curr_input_coll_days`          int           COMMENT '当前入催天数'
) COMMENT '子案件出催信息表'
PARTITIONED BY (d_date string COMMENT '分区日期',p_type string COMMENT '项目类型')
STORED AS PARQUET;


--催收记录表
DROP TABLE IF EXISTS `dwb.dwb_coll_rec`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_coll_rec`(
  `coll_rec_no`                   string        COMMENT '催记流水号',
  `case_sub_id`                   string        COMMENT '子案件编号',
  `coll_time`                     bigint        COMMENT '催收时间',
  `coll_date`                     string        COMMENT '催收日期',
  `action_code`                   string        COMMENT '催收动作',
  `coll_result`                   string        COMMENT '催收结果',
  `contact_name`                  string        COMMENT '催收对象',
  `tel_no`                        string        COMMENT '联系电话',
  `collector`                     string        COMMENT '催收员',
  `promise_amt`                   decimal(15,2) COMMENT '承诺金额',
  `promise_date`                  string        COMMENT '承诺日期',
  `create_time`                   bigint        COMMENT '创建时间',
  `loan_id`                       string        COMMENT '借据号',
  `called_ref_type`               string        COMMENT '引用类型',
  `call_status`                   string        COMMENT '通话状态',
  `call_result`                   string        COMMENT '通话结果',
  `cust_name`                     string        COMMENT '客户姓名',
  `coll_collection`               string        COMMENT '催收组',
  `oa_org_no`                     string        COMMENT '委外机构编号'
) COMMENT '催收记录表'
PARTITIONED BY (d_date string COMMENT '分区日期',p_type string COMMENT '项目类型')
STORED AS PARQUET;


--外呼消息作业表
DROP TABLE IF EXISTS `dwb.dwb_call_message_work`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_call_message_work`(
  `call_message_id`               string        COMMENT '流水号',
  `case_sub_id`                   string        COMMENT '子案件编号',
  `send_state`                    string        COMMENT '发送状态',
  `send_type`                     string        COMMENT '发送类型',
  `call_create_time`              bigint        COMMENT '通话创建时间',
  `call_start_time`               bigint        COMMENT '通话开始时间',
  `call_end_time`                 bigint        COMMENT '通话结束时间',
  `call_duration`                 int           COMMENT '总通话时长，单位s',
  `handle_date`                   bigint        COMMENT '下游平台开始处理时间',
  `call_cost`                     decimal(15,2) COMMENT '总费用',
  `call_status`                   int           COMMENT '是否应答 0：未应答 1.应答',
  `msg`                           string        COMMENT '应答信息',
  `tel_no`                        string        COMMENT '发送号码',
  `send_date`                     bigint        COMMENT '发送时间',
  `collector`                     string        COMMENT '操作员',
  `create_time`                   bigint        COMMENT '发送时间',
  `cust_name`                     string        COMMENT '客户姓名',
  `contact_name`                  string        COMMENT '联系人姓名',
  `relationship`                  string        COMMENT '联系人关系',
  `contact_id`                    string        COMMENT '联系人ID',
  `record_name`                   string        COMMENT '录音',
  `oa_org_no`                     string        COMMENT '委外机构编号',
  `coll_collection`               string        COMMENT '催收组',
  `loan_id`                       string        COMMENT '借据号'
) COMMENT '外呼消息作业表'
PARTITIONED BY (d_date string COMMENT '分区日期',p_type string COMMENT '项目类型')
STORED AS PARQUET;


--消息作业表
DROP TABLE IF EXISTS `dwb.dwb_message_work`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_message_work`(
  `message_id`                    string        COMMENT '流水号',
  `case_sub_id`                   string        COMMENT '子案件编号',
  `message_type`                  string        COMMENT '信息类型',
  `send_stat`                     string        COMMENT '发送状态',
  `send_type`                     string        COMMENT '发送类型',
  `content`                       string        COMMENT '信息内容',
  `task_pool`                     string        COMMENT '任务池',
  `tel_no`                        string        COMMENT '发送号码',
  `send_date`                     bigint        COMMENT '发送时间',
  `collector`                     string        COMMENT '操作员',
  `create_user`                   string        COMMENT '创建人',
  `lp_corpno`                     string        COMMENT '法人代码',
  `tranus`                        string        COMMENT '录入人',
  `brchno`                        string        COMMENT '录入机构',
  `upbrchno`                      string        COMMENT '最后修改机构',
  `create_time`                   bigint        COMMENT '创建时间',
  `lst_upd_user`                  string        COMMENT '最后修改人',
  `lst_upd_time`                  bigint        COMMENT '最后修改时间',
  `oa_org_no`                     string        COMMENT '委外机构编号',
  `coll_collection`               string        COMMENT '催收组'
) COMMENT '消息作业表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


--付款承诺表
DROP TABLE IF EXISTS `dwb.dwb_coll_promise`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_coll_promise`(
  `promise_id`                    string        COMMENT '承诺付款流水号',
  `promise_amt`                   decimal(15,2) COMMENT '承诺金额',
  `promise_date`                  string        COMMENT '承诺日期',
  `shd_pmt_amt`                   decimal(15,2) COMMENT '还款金额',
  `collector`                     string        COMMENT '催收员',
  `promise_status`                string        COMMENT '承诺状态',
  `case_sub_id`                   string        COMMENT '子案件编号',
  `create_time`                   bigint        COMMENT '创建时间',
  `loan_id`                       string        COMMENT '借据号'
) COMMENT '付款承诺表'
PARTITIONED BY (d_date string COMMENT '分区日期',p_type string COMMENT '项目类型')
STORED AS PARQUET;


--代扣申请表
DROP TABLE IF EXISTS `dwb.dwb_early_repay_check`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_early_repay_check`(
  `legal_apply_num`               string        COMMENT '还款代扣申请流水',
  `org`                           string        COMMENT '机构号',
  `sub_case_main_id`              string        COMMENT '子案件编号',
  `loan_id`                       string        COMMENT '借据编号',
  `cust_name`                     string        COMMENT '客户姓名',
  `id_type`                       string        COMMENT '证件类型',
  `id_no`                         string        COMMENT '证件号码',
  `total_amount_money`            decimal(18,2) COMMENT '代扣金额',
  `real_debit_result`             string        COMMENT '代扣结果',
  `payment_type`                  string        COMMENT '代扣类型',
  `repay_channel`                 string        COMMENT '代扣申请渠道',
  `payment_time`                  string        COMMENT '代扣时间',
  `operator`                      string        COMMENT '操作人',
  `operat_date`                   bigint        COMMENT '操作时间',
  `manual_repay_term`             decimal(18,0) COMMENT '手工还款期次',
  `manual_capital`                decimal(18,2) COMMENT '手工本金',
  `manual_int`                    decimal(18,2) COMMENT '手工利息',
  `manual_oint`                   decimal(18,2) COMMENT '手工罚息',
  `manual_paycompound`            decimal(18,2) COMMENT '手工复利',
  `manual_fee`                    decimal(18,2) COMMENT '手工费用',
  `manual_fee_details`            string        COMMENT '还款明细',
  `repayment_account_type`        string        COMMENT '还款账户类型',
  `order_id`                      string        COMMENT '还款订单编号',
  `active_repay_date`             string        COMMENT '还款时间',
  `coll_rec_no`                   int           COMMENT '催记流水号',
  `trans_code`                    string        COMMENT '交易类型',
  `trans_serial_no`               string        COMMENT '交易流水号',
  `due_bill_id`                   string        COMMENT '借据号',
  `total_amt`                     decimal(18,2) COMMENT '需冻结金额',
  `repay_principal`               decimal(17,2) COMMENT '归还本金',
  `repay_interest`                decimal(17,2) COMMENT '归还利息',
  `repay_penalty_amount`          decimal(17,2) COMMENT '归还逾期罚息',
  `repay_fee`                     decimal(17,2) COMMENT '归还费用',
  `failure_message`               string        COMMENT '失败原因',
  `actual_repay_amount`           decimal(18,2) COMMENT '还款金额',
  `apply_datetime`                bigint        COMMENT '代扣申请时间'
) COMMENT '代扣申请表'
PARTITIONED BY (d_date string COMMENT '分区日期',p_type string COMMENT '项目类型')
STORED AS PARQUET;


--减免申请表
DROP TABLE IF EXISTS `dwb.dwb_derate_apply`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_derate_apply`(
  `id`                            string        COMMENT 'ID',
  `der_apply_num`                 string        COMMENT '减免申请流水号',
  `org`                           string        COMMENT '机构号',
  `cust_name`                     string        COMMENT '客户姓名',
  `loan_id`                       string        COMMENT '借据号',
  `apply_date`                    string        COMMENT '减免申请日期',
  `der_reason`                    string        COMMENT '减免原因',
  `reduction_amt`                 decimal(15,2) COMMENT '减免总额',
  `reduction_fee`                 decimal(15,2) COMMENT '减免费用',
  `reduction_penalty`             decimal(15,2) COMMENT '减免罚息',
  `reduction_int`                 decimal(15,2) COMMENT '减免利息',
  `reduction_principal`           decimal(15,2) COMMENT '减免本金',
  `derate_trans_date`             string        COMMENT '减免交易时间',
  `derate_result`                 string        COMMENT '减免结果',
  `reduction_type`                string        COMMENT '减免方式',
  `case_sub_id`                   string        COMMENT '子案件编号',
  `check_status`                  string        COMMENT '审批状态',
  `txn_status`                    string        COMMENT '扣款结果',
  `if_payment`                    string        COMMENT '是否扣款',
  `actual_repay_amount`           decimal(15,2) COMMENT '还款金额',
  `repay_principal`               decimal(15,2) COMMENT '归还本金',
  `repay_interest`                decimal(15,2) COMMENT '归还利息',
  `repay_penalty_amount`          decimal(15,2) COMMENT '归还逾期罚息',
  `repay_fee`                     decimal(15,2) COMMENT '还款费用',
  `failure_message`               string        COMMENT '失败原因',
  `cust_no`                       string        COMMENT '客户号',
  `txn_seq`                       string        COMMENT '交易流水号',
  `apply_datetime`                bigint        COMMENT '减免申请时间'
) COMMENT '减免申请表'
PARTITIONED BY (d_date string COMMENT '分区日期',p_type string COMMENT '项目类型')
STORED AS PARQUET;


DROP TABLE IF EXISTS `dwb.dwb_customer_info`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_customer_info`(
  `ecif`                          string        COMMENT '客户ECIF ID',
  `cutomer_type`                  string        COMMENT '客戶类型',
  `name`                          string        COMMENT '姓名',
  `idCard`                        string        COMMENT '身份证号',
  `age`                           int           COMMENT '年龄',
  `sex`                           string        COMMENT '性别',
  `credential_due_date`           string        COMMENT '证件到期日期',
  `face_detect_url`               string        COMMENT '人脸识别素材地址',
  `cell_phone`                    string        COMMENT '手机号',
  `init_phone`                    string        COMMENT '初始手机号',
  `marry_status`                  string        COMMENT '婚姻状态',
  `education`                     string        COMMENT '学历',
  `init_education`                string        COMMENT '初始学历',
  `private_owners`                string        COMMENT '是否私营业主',
  `position`                      string        COMMENT '社会身份（职位）',
  `industry_type`                 string        COMMENT '行业类别',
  `department`                    string        COMMENT '任职部门',
  `duties`                        string        COMMENT '职务',
  `employ_year`                   int           COMMENT '工作年限',
  `month_income`                  decimal(15,2) COMMENT '月收入',
  `year_income`                   decimal(15,2) COMMENT '年收入',
  `family_month_income`           decimal(15,2) COMMENT '家庭月收入',
  `family_nums`                   int           COMMENT '家庭人数',
  `idcard_location`               string        COMMENT '身份证所在地',
  `idcard_provice`                string        COMMENT '身份证对应省',
  `idcard_city`                   string        COMMENT '身份证对应市',
  `idcard_area`                   string        COMMENT '身份证对应区',
  `apply_loaction`                string        COMMENT 'apply_loaction',
  `mobile_home`                   string        COMMENT '手机所在地',
  `live_addr`                     string        COMMENT '居住地址',
  `init_live_addr`                string        COMMENT '初始居住地址',
  `live_status`                   string        COMMENT '居住状态',
  `live_prov`                     string        COMMENT '居住地址-省',
  `init_live_prov`                string        COMMENT '初始居住地址-省',
  `live_city`                     string        COMMENT '居住地址-市',
  `init_live_city`                string        COMMENT '初始居住地址-市',
  `live_area`                     string        COMMENT '居住地址-区',
  `live_road`                     string        COMMENT '居住地址-街道',
  `live_longitude`                string        COMMENT '居住地址-经度',
  `live_latitude`                 string        COMMENT '居住地址-纬度',
  `company_name`                  string        COMMENT '工作单位名称',
  `company_phone`                 string        COMMENT '工作单位电话',
  `company_addr`                  string        COMMENT '工作单位地址',
  `init_company_addr`             string        COMMENT '初始工作单位地址',
  `company_prov`                  string        COMMENT '工作单位地址-省',
  `init_company_prov`             string        COMMENT '初始工作单位地址-省',
  `company_Ctiy`                  string        COMMENT '工作单位地址-市',
  `init_company_city`             string        COMMENT '初始工作单位地址-市',
  `company_area`                  string        COMMENT '工作单位地址-区',
  `company_road`                  string        COMMENT '工作单位地址-街道',
  `company_longitude`             string        COMMENT '工作单位地址-经度',
  `marks`                         string        COMMENT '标志滴滴(DD)汇通(HT)盒子(HZ)',
  `org`                           string        COMMENT '租户号',
  `id_type`                       string        COMMENT '证件类型',
  `product_code`                  string        COMMENT '产品编号',
  `channel_id`                    string        COMMENT '渠道号'
) COMMENT 'ods客户信息表'
PARTITIONED BY (d_date string COMMENT '分区日期',p_type string COMMENT '项目类型')
STORED AS PARQUET;


--DWB滴滴日志详情表
DROP TABLE IF EXISTS `dwb.dwb_dd_log_detail`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_dd_log_detail`(
  `ecif`                          string        COMMENT 'ECIF',
  `name`                          string        COMMENT '用户姓名',
  `cardNo`                        string        COMMENT '身份证ID',
  `idType`                        string        COMMENT '证件类型',
  `phone`                         string        COMMENT '银行卡绑定手机号',
  `telephone`                     string        COMMENT '滴滴登陆手机号',
  `bankCardNo`                    string        COMMENT '银行卡号',
  `userRole`                      string        COMMENT '职业',
  `idCardValidDate`               string        COMMENT '有效期结束日期',
  `address`                       string        COMMENT '地址',
  `ocrInfo`                       string        COMMENT 'ocr解析结果',
  `idCardBackInfo`                string        COMMENT '',
  `imageType`                     int           COMMENT '图片类型',
  `imageStatus`                   string        COMMENT '',
  `imageUrl`                      string        COMMENT 'gift 地址',
  `livingImageInfo`               string        COMMENT '',
  `sftpDir`                       string        COMMENT '',
  `sftp`                          string        COMMENT '',
  `amount`                        decimal(15,6) COMMENT '授信额度',
  `interestRate`                  decimal(15,6) COMMENT '利率',
  `interestPenaltyRate`           decimal(15,6) COMMENT '罚息利率',
  `startDate`                     string        COMMENT '授信有效起始日期',
  `endDate`                       string        COMMENT '授信有效截止日期',
  `lockDownEndTime`               string        COMMENT '禁闭期截止日期',
  `didiRcFeatureJson`             string        COMMENT 'didi 数据',
  `flowNo`                        string        COMMENT '',
  `signType`                      string        COMMENT '',
  `applicationId`                 string        COMMENT '申请唯一标示',
  `creditResultStatus`            string        COMMENT '授信状态',
  `applySource`                   string        COMMENT '',
  `deal_date`                     string        COMMENT '处理时间',
  `create_time`                   string        COMMENT '创建时间',
  `update_time`                   string        COMMENT '更新时间',
  `linkman_info`                  string        COMMENT '联系人信息',
  `org`                           string        COMMENT '租户org'
) COMMENT 'DWB滴滴日志详情表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


--DWB凤金日志详情表
DROP TABLE IF EXISTS `dwb.dwb_fj_log_detail`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_fj_log_detail`(
  `org`                           string        COMMENT '',
  `id`                            string        COMMENT '自增主键',
  `project_id`                    string        COMMENT '项目编号',
  `agency_id`                     string        COMMENT '机构号',
  `partition_key`                 string        COMMENT '数据库里的分区时间',
  `create_time`                   string        COMMENT '创建时间',
  `update_time`                   string        COMMENT '更新时间',
  `ecif`                          string        COMMENT 'ecif 编号',
  `product_no`                    string        COMMENT '产品编号',
  `request_no`                    string        COMMENT '进件流水号',
  `loan_apply_use`                string        COMMENT '申请贷款用途',
  `loan_rate_type`                string        COMMENT '利率类型',
  `currency_type`                 string        COMMENT '申请币种',
  `contract_no`                   string        COMMENT '合同编号',
  `contract_amount`               decimal(15,2) COMMENT '贷款总金额',
  `company_loan_bool`             string        COMMENT '是否是企业贷款',
  `repay_type`                    string        COMMENT '还款方式',
  `repay_frequency`               string        COMMENT '还款频率',
  `terms`                         int           COMMENT '还款期数',
  `deduction_date`                int           COMMENT '扣款日',
  `loan_rate`                     decimal(15,6) COMMENT '贷款年利率',
  `year_rate_base`                decimal(15,6) COMMENT '年利率基础',
  `loan_date`                     string        COMMENT '贷款开始日',
  `loan_end_date`                 string        COMMENT '贷款结束日',
  `open_id`                       string        COMMENT 'Borrower用户唯一标识',
  `name`                          string        COMMENT '姓名',
  `id_type`                       string        COMMENT '证件类型',
  `id_no`                         string        COMMENT '证件号码',
  `mobile_phone`                  string        COMMENT '手机号码',
  `sex`                           string        COMMENT '性别',
  `age`                           int           COMMENT '年龄',
  `province`                      string        COMMENT '居住地址（省）',
  `city`                          string        COMMENT '居住地址(市)',
  `area`                          string        COMMENT '居住地址（区/县）',
  `address`                       string        COMMENT '详细地址',
  `marital_status`                string        COMMENT '婚姻状态',
  `education`                     string        COMMENT '学历',
  `industry`                      string        COMMENT '借款人行业',
  `annual_income`                 decimal(15,2) COMMENT '年收入',
  `have_house`                    string        COMMENT '是否有房',
  `housing_area`                  string        COMMENT '住房面积',
  `housing_value`                 string        COMMENT '住房价值',
  `family_worth`                  string        COMMENT '家庭净资产',
  `front_url`                     string        COMMENT '身份证正面照片地址',
  `back_url`                      string        COMMENT '身份证反面照片地址',
  `private_owners`                string        COMMENT '是否私营业主',
  `income_m1`                     decimal(15,2) COMMENT '盒伙人1个月累计佣金',
  `income_m3`                     decimal(15,2) COMMENT '盒伙人3个月累计佣金',
  `income_m6`                     decimal(15,2) COMMENT '盒伙人6个月累计佣金',
  `income_m12`                    decimal(15,2) COMMENT '盒伙人12个月累计佣金',
  `social_credit_code`            string        COMMENT '统一社会信用代码',
  `company_name`                  string        COMMENT '企业名称',
  `company_industry`              string        COMMENT '企业行业',
  `company_province`              string        COMMENT '注册省份',
  `company_city`                  string        COMMENT '注册城市',
  `company_address`               string        COMMENT '注册地址',
  `company_legal_person_name`     string        COMMENT '法人代表名字',
  `legal_person_id_type`          string        COMMENT '法人证件类型',
  `legal_person_id_no`            string        COMMENT '法人证件号码',
  `company_legal_person_phone`    string        COMMENT '法人手机',
  `company_phone`                 string        COMMENT '企业联系电话',
  `company_operate_years`         int           COMMENT '经营年限',
  `relational_humans`             string        COMMENT '关联人信息json',
  `guaranties`                    string        COMMENT '抵押物列表json',
  `carJson`                       string        COMMENT '',
  `houseJson`                     string        COMMENT '房子信息json',
  `humanjson`                     string        COMMENT '',
  `apply_request_no`              string        COMMENT '外部交易流水号',
  `acct_setup_ind`                string        COMMENT '是否开户成功',
  `loan_accountJson`              string        COMMENT '放款账户信息',
  `repayment_accountJson`         string        COMMENT '还款账户信息'
) COMMENT 'DWB凤金日志详情表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


--DWB汇通报文解析详情表
DROP TABLE IF EXISTS `dwb.dwb_ht_log_detail`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_ht_log_detail`(
  `id`                            string        COMMENT '接口响应日志ID',
  `req_log_id`                    string        COMMENT '请求日志ID',
  `org`                           string        COMMENT '机构号',
  `deal_date`                     string        COMMENT '请求处理时间',
  `create_time`                   bigint        COMMENT '创建时间',
  `update_time`                   bigint        COMMENT '修改时间',
  `ecif`                          string        COMMENT 'ecif',
  `pre_apply_no`                  string        COMMENT '预申请编号',
  `apply_no`                      string        COMMENT '申请件编号',
  `company_loan_bool`             string        COMMENT '是否是企业贷款',
  `product_no`                    string        COMMENT '产品编号',
  `currency_type`                 string        COMMENT '申请币种',
  `currency_amt`                  decimal(15,2) COMMENT '申请金额',
  `loan_amt`                      decimal(15,2) COMMENT '贷款总金额',
  `loan_terms`                    int           COMMENT '贷款期数',
  `repay_type`                    string        COMMENT '还款方式',
  `loan_rate_type`                string        COMMENT '利率类型',
  `agreement_rate_ind`            string        COMMENT '是否使用协议费/利率',
  `loan_rate`                     decimal(15,6) COMMENT '贷款年利率',
  `loan_fee_rate`                 decimal(15,6) COMMENT '贷款手续费比例',
  `loan_svc_fee_rate`             decimal(15,6) COMMENT '贷款服务费比例',
  `loan_penalty_rate`             decimal(15,6) COMMENT '贷款罚息年利率',
  `guarantee_type`                string        COMMENT '担保方式',
  `loan_apply_use`                string        COMMENT '申请贷款用途',
  `open_id`                       string        COMMENT 'Borrower用户唯一标识',
  `name`                          string        COMMENT 'Borrower借款人姓名',
  `id_type`                       string        COMMENT '证件类型',
  `id_no`                         string        COMMENT 'Borrower借款人证件号码',
  `mobile_phone`                  string        COMMENT 'Borrower',
  `sex`                           string        COMMENT 'Borrower性别',
  `age`                           int           COMMENT 'Borrower年龄',
  `province`                      string        COMMENT '居住地址(省）',
  `city`                          string        COMMENT 'Borrower居住地址(市)',
  `area`                          string        COMMENT 'Borrower居住地址（区/县',
  `address`                       string        COMMENT 'Borrower详细地址',
  `marital_status`                string        COMMENT 'Borrower婚姻状态',
  `education`                     string        COMMENT 'Borrower学历',
  `industry`                      string        COMMENT 'Borrower借款人行业',
  `annual_income_min`             string        COMMENT 'Borrower年收入区间下限',
  `annual_income_max`             string        COMMENT 'Borrower年收入区间上限',
  `have_house`                    string        COMMENT 'Borrower是否有房',
  `housing_area`                  string        COMMENT 'Borrower  住房面积',
  `housing_value`                 string        COMMENT 'Borrower 住房价值',
  `drivr_licen_no`                string        COMMENT 'Borrower 驾驶证号',
  `driving_expr`                  string        COMMENT 'Borrower驾龄',
  `company_social_credit_code`    string        COMMENT '统一社会信用代码',
  `company_name`                  string        COMMENT 'Borrower 企业名称',
  `company_industry`              string        COMMENT ' Borrower 企业行业',
  `company_province`              string        COMMENT 'Borrower 注册省份',
  `company_city`                  string        COMMENT 'Borrower 注册城市',
  `company_address`               string        COMMENT 'Borrower 注册地址',
  `legal_person_name`             string        COMMENT '法人代表名字',
  `legal_person_id_type`          string        COMMENT '法人证件类型',
  `legal_person_id_no`            string        COMMENT '法人证件号码',
  `legal_person_phone`            string        COMMENT '法人手机',
  `company_phone`                 string        COMMENT '企业联系电话',
  `company_operate_years`         string        COMMENT '经营年限',
  `relational_humans`             string        COMMENT '关联人关系',
  `guaranties`                    string        COMMENT '抵押物列表json',
  `car`                           string        COMMENT 'car',
  `account_type`                  string        COMMENT '账户类型',
  `account_num`                   string        COMMENT '账户号码（银行卡号）',
  `account_name`                  string        COMMENT '账户户名',
  `bank_name`                     string        COMMENT '开户银行',
  `branch_name`                   string        COMMENT '开户支行名称',
  `branch_mobile_phone`           string        COMMENT '银行卡在银行预留的手机号码',
  `acct_setup_ind`                string        COMMENT '是否开户成功',
  `reject_msg`                    string        COMMENT '结果信息',
  `cust_no`                       string        COMMENT '内部客户号',
  `code`                          string        COMMENT '请求失败的状态码',
  `channel_id`                    string        COMMENT '',
  `status`                        string        COMMENT '请求成功或者失败'
) COMMENT 'DWB汇通报文解析详情表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


--DWB乐信报文解析详情表
DROP TABLE IF EXISTS `dwb.dwb_lx_log_detail`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_lx_log_detail`(
  `authNo`                        string        COMMENT '授权协议编号',
  `applyNo`                       string        COMMENT '申请编号',
  `proCode`                       string        COMMENT '产品编号',
  `proName`                       string        COMMENT '产品名称',
  `dbBankCode`                    string        COMMENT '银行代码',
  `dbBankName`                    string        COMMENT '银行总行名',
  `dbOpenBankName`                string        COMMENT '开户行银行名',
  `dbBankAccount`                 string        COMMENT '放款账户卡号',
  `dbAccountName`                 string        COMMENT '放款账户名',
  `custName`                      string        COMMENT '客户名称',
  `customerId`                    string        COMMENT '客户号',
  `creditLimit`                   decimal(15,2) COMMENT '客户授信额度',
  `availableCreditLimit`          decimal(15,2) COMMENT '客户当前可用额度',
  `idType`                        string        COMMENT '证件类型',
  `idNo`                          string        COMMENT '证件号码',
  `idPreDate`                     string        COMMENT '证件起始日期',
  `idEndDate`                     string        COMMENT '证件截止日期',
  `sex`                           string        COMMENT '性别',
  `birth`                         string        COMMENT '',
  `age`                           int           COMMENT '年龄',
  `country`                       string        COMMENT '国籍',
  `edu`                           string        COMMENT '最高学历',
  `degree`                        string        COMMENT '最高学位',
  `phoneNo`                       string        COMMENT '手机号码',
  `telNo`                         string        COMMENT '联系电话',
  `postCode`                      string        COMMENT '通讯邮编',
  `postAddr`                      string        COMMENT '通讯地址',
  `homeArea`                      string        COMMENT '户籍归属地',
  `homeCode`                      string        COMMENT '住宅邮编',
  `homeAddr`                      string        COMMENT '住宅地址',
  `homeTel`                       string        COMMENT '住宅电话',
  `homeSts`                       string        COMMENT '居住状况',
  `marriage`                      string        COMMENT '婚姻状况',
  `mateName`                      string        COMMENT 'mateName',
  `mateIdtype`                    string        COMMENT '配偶证件类型',
  `mateIdno`                      string        COMMENT '配偶证件号码',
  `mateWork`                      string        COMMENT '配偶工作单位',
  `mateTel`                       string        COMMENT '配偶联系电话',
  `children`                      string        COMMENT '是否有子女',
  `mincome`                       string        COMMENT '个人月收入（元）',
  `income`                        string        COMMENT '个人年收入',
  `homeIncome`                    string        COMMENT '家庭年收入（反洗钱）',
  `zxhomeIncome`                  string        COMMENT '家庭年收入（征信）',
  `custType`                      string        COMMENT '客户类型',
  `workSts`                       string        COMMENT '工作状态',
  `workName`                      string        COMMENT '工作单位名称',
  `workWay`                       string        COMMENT '工作单位所属行业',
  `workCode`                      string        COMMENT '工作单位邮编',
  `workAddr`                      string        COMMENT '工作单位地址',
  `workType`                      string        COMMENT '职业',
  `workDuty`                      string        COMMENT '职务',
  `workTitle`                     string        COMMENT '职称',
  `workYear`                      string        COMMENT '四位年份',
  `pactAmt`                       decimal(15,2) COMMENT '贷款金额',
  `lnRate`                        decimal(15,6) COMMENT '利率（月）',
  `loanTerm`                      int           COMMENT '贷款期数',
  `appArea`                       string        COMMENT '《行政区划》默认',
  `appUse`                        string        COMMENT '申请用途',
  `vouType`                       string        COMMENT '担保方式',
  `loanDate`                      string        COMMENT '申请日期',
  `endDate`                       string        COMMENT '到期日期',
  `rpyMethod`                     string        COMMENT '还款方式',
  `payType`                       string        COMMENT '',
  `payDay`                        string        COMMENT '扣款日期',
  `riskLevel`                     string        COMMENT '风险等级',
  `loanTime`                      string        COMMENT '放款订单时间',
  `trade`                         string        COMMENT '投向行业（反洗钱）',
  `ifCar`                         string        COMMENT '',
  `ifCarCred`                     string        COMMENT '是否有按揭车贷',
  `ifRoom`                        string        COMMENT '是否有房',
  `ifMort`                        string        COMMENT '是否有按揭房贷',
  `ifCard`                        string        COMMENT '是否有贷记卡',
  `cardAmt`                       decimal(15,2) COMMENT '贷记卡最低额度',
  `ifApp`                         string        COMMENT '是否填写申请表',
  `ifId`                          string        COMMENT '是否有身份证信息',
  `ifPact`                        string        COMMENT '是否已签订借款合同',
  `czPactNo`                      string        COMMENT '查证流水号',
  `ifLaunder`                     string        COMMENT '是否具有洗钱风险',
  `launder`                       string        COMMENT '反洗钱风险关联度',
  `ifAgent`                       string        COMMENT '是否有代理人',
  `profession`                    string        COMMENT '职业（反洗钱）',
  `isBelowRisk`                   string        COMMENT '借款人风险等级是否在贷款服务机构指定等级及以下',
  `hasOverdueLoan`                string        COMMENT '借款人是否存在其他未结清的逾期贷款',
  `sales`                         string        COMMENT '销售渠道',
  `resultCode`                    string        COMMENT '请求状态码',
  `resultMsg`                     string        COMMENT '请求结果信息',
  `pass`                          string        COMMENT '',
  `deal_date`                     string        COMMENT '处理时间',
  `partner`                       string        COMMENT '',
  `ecif_id`                       string        COMMENT 'ecif_id',
  `org`                           string        COMMENT '',
  `update_time`                   bigint        COMMENT '修改时间',
  `create_time`                   bigint        COMMENT '',
  `req_timestamp`                 bigint        COMMENT '请求报文里的时间',
  `flag`                          string        COMMENT '区分乐信1期还是乐信2期'
) COMMENT 'DWB乐信日志详情表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


--DWB瓜子报文解析详情表
DROP TABLE IF EXISTS `dwb.dwb_gz_log_detail`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_gz_log_detail`(
  `service`                       string        COMMENT '接口名称' ,
  `partner`                       string        COMMENT '合作方身份' ,
  `projectNo`                     string        COMMENT '项目编号' ,
  `serviceSn`                     string        COMMENT '请求流水号' ,
  `projectName`                   string        COMMENT '项目名称' ,
  `timeStamp`                     string        COMMENT '接口请求时间戳' ,
  `preApplyNo`                    string        COMMENT '预申请编号  ' ,
  `applyNo`                       string        COMMENT '申请件编号' ,
  `companyLoanBool`               string        COMMENT '是否是企业贷款' ,
  `rentalDate`                    string        COMMENT '申请时间' ,
  `currencyType`                  string        COMMENT '申请币种' ,
  `applyCity`                     string        COMMENT '进件城市' ,
  `currencyAmt`                   decimal(15,2) COMMENT '申请金额' ,
  `loanAmt`                       decimal(15,2) COMMENT '贷款总金额' ,
  `loanTerms`                     int           COMMENT '贷款期数' ,
  `currencyTerms`                 int           COMMENT '申请期限' ,
  `repayType`                     string        COMMENT '还款方式' ,
  `loanRateType`                  string        COMMENT '利率类型' ,
  `agreementRateInd`              string        COMMENT '是否使用协议费/利率' ,
  `loanRate`                      decimal(15,2) COMMENT '贷款年利率' ,
  `loanFeeRate`                   decimal(15,2) COMMENT '贷款手续费比例' ,
  `loanSvcFeeRate`                decimal(15,2) COMMENT '贷款服务费比例' ,
  `loanPenaltyRate`               decimal(15,2) COMMENT '贷款罚息年利率' ,
  `guaranteeType`                 string        COMMENT '担保方式' ,
  `loanApplyUse`                  string        COMMENT '申请贷款用途' ,
  `name`                          string        COMMENT '姓名' ,
  `idType`                        string        COMMENT '证件类型' ,
  `idNo`                          string        COMMENT '证件号码' ,
  `idCountry`                     string        COMMENT '国家' ,
  `idProvince`                    string        COMMENT '省' ,
  `idCity`                        string        COMMENT '市' ,
  `idAddress`                     string        COMMENT '地址' ,
  `mobilePhone`                   string        COMMENT '手机号码' ,
  `tel`                           string        COMMENT '联系电话' ,
  `sex`                           string        COMMENT '性别' ,
  `homeProvince`                  string        COMMENT '居住地址（省）' ,
  `homeCity`                      string        COMMENT '居住地址(市)' ,
  `homeArea`                      string        COMMENT '居住地址（区/县）' ,
  `homeAddress`                   string        COMMENT '居住详细地址' ,
  `homeTel`                       string        COMMENT '家庭电话' ,
  `maritalStatus`                 string        COMMENT '婚姻状态' ,
  `education`                     string        COMMENT '学历' ,
  `industry`                      string        COMMENT '借款人行业' ,
  `occpType`                      string        COMMENT '职业' ,
  `companyName`                   string        COMMENT '公司名称' ,
  `companyProvince`               string        COMMENT '公司地址-省' ,
  `companyCity`                   string        COMMENT '公司地址-市' ,
  `companyArea`                   string        COMMENT '公司地址-区' ,
  `companyAddress`                string        COMMENT '公司详细地址' ,
  `annualIncomeMin`               decimal(15,2) COMMENT '年收入区间下限' ,
  `annualIncomeMax`               decimal(15,2) COMMENT '年收入区间上限' ,
  `haveHouse`                     string        COMMENT '是否有房' ,
  `housingArea`                   string        COMMENT '住房面积' ,
  `housingValue`                  decimal(15,2) COMMENT '住房价值' ,
  `drivrLicenNo`                  string        COMMENT '驾驶证号' ,
  `drivingExpr`                   int           COMMENT '驾龄' ,
  `relationalHumans`              string        COMMENT '关联人列表' ,
  `guaranties`                    string        COMMENT '抵押物列表' ,
  `loanAccount`                   string        COMMENT '放款账户' ,
  `payAccount`                    string        COMMENT '还款账户' ,
  `auditInfo`                     string        COMMENT '审核信息' ,
  `bank_card_no`                  string        COMMENT '放款账户的银行卡号' ,
  `deal_date`                     string        COMMENT '' ,
  `status`                        int           COMMENT '申请结果 string    0-进件失败 1-进件通过 2-处理中' ,
  `message`                       string        COMMENT '错误信息' ,
  `ecif_id`                       string        COMMENT '' ,
  `org`                           string        COMMENT '' ,
  `create_time`                   bigint        COMMENT '' ,
  `update_time`                   bigint        COMMENT '' ,
  `bank_card_mobile_phone`        string        COMMENT '银行预留手机号' ,
  `flag`                          string        COMMENT '预留一个字段'
) COMMENT 'DWb瓜子日志详情表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


-- 乐信2期银行卡信息表
DROP TABLE IF EXISTS `dwb.dwb_bank_card_info_asset`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_bank_card_info_asset`(
  `card_id`                       string        COMMENT '银行卡id',
  `CUST_ID`                       string        COMMENT '客户id',
  `due_bill_no`                   string        COMMENT '借据号',
  `bank_card_id_no`               string        COMMENT '证件号码',
  `bank_card_name`                string        COMMENT '姓名',
  `bank_card_phone`               string        COMMENT '手机号',
  `pay_channel`                   int           COMMENT '支付渠道 1 宝付  2 通联',
  `card_flag`                     string        COMMENT '绑卡标志',
  `agreement_no`                  string        COMMENT '绑卡协议编号',
  `bank_cardNo`                   string        COMMENT '银行卡号',
  `bank_no`                       string        COMMENT '银行编号',
  `bank_name`                     string        COMMENT '银行名称',
  `province`                      string        COMMENT '银行分行-省',
  `city`                          string        COMMENT '银行分行-市',
  `tied_card_time`                string        COMMENT '绑卡时间',
  `ecif`                          string        COMMENT 'ecif',
  `org`                           string        COMMENT 'org'
) COMMENT '乐信2期银行卡信息表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


---dwb 乐信2期绑卡信息变更变
DROP TABLE IF EXISTS `dwb.dwb_bind_card_change_asset`;
CREATE TABLE IF NOT EXISTS `dwb.dwb_bind_card_change_asset`(
  `CHANGE_ID`                     string        COMMENT '主键',
  `ORG`                           string        COMMENT '租户号',
  `CUST_ID`                       string        COMMENT '客户id',
  `DUE_BILL_NO`                   string        COMMENT '借据号',
  `OLD_BANK_CARD_ID_NO`           string        COMMENT '旧证件号码',
  `OLD_BANK_CARD_NAME`            string        COMMENT '旧姓名',
  `OLD_BANK_CARD_PHONE`           string        COMMENT '旧手机号',
  `OLD_BANK_CARD_NO`              string        COMMENT '旧银行卡号',
  `OLD_PAY_CHANNEL`               string        COMMENT '旧支付渠道:1-宝付,2-通联',
  `OLD_AGREEMENT_NO`              string        COMMENT '旧绑卡协议编号',
  `OLD_CARD_FLAG`                 string        COMMENT '旧标识:N|正常,F|非客户本人,共同借款人,配偶',
  `NEW_BANK_CARD_ID_NO`           string        COMMENT '新证件号码',
  `NEW_BANK_CARD_NAME`            string        COMMENT '新姓名',
  `NEW_BANK_CARD_PHONE`           string        COMMENT '旧手机号',
  `NEW_BANK_CARD_NO`              string        COMMENT '新银行卡号',
  `NEW_PAY_CHANNEL`               string        COMMENT '新支付渠道:1-宝付,2-通联',
  `NEW_AGREEMENT_NO`              string        COMMENT '新绑卡协议编号',
  `NEW_CARD_FLAG`                 string        COMMENT '新标识:N|正常,F|非客户本人,共同借款人,配偶',
  `CREATE_TIME`                   string        COMMENT '',
  `LST_UPD_TIME`                  string        COMMENT '',
  `JPA_VERSION`                   string        COMMENT '',
  `old_ecif`                      string        COMMENT '旧身份证号对应的ecif',
  `new_ecif`                      string        COMMENT '新身份证号对应的ecif'
) COMMENT '乐信2期绑卡信息更新信息表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;

-- ----------------------------
-- Table structure for eagle.t_bill_snapshot
-- ----------------------------

--ALTER TABLE eagle.t_bill_snapshot RENAME TO eagle.t_bill_snapshot_v3;
--ALTER TABLE eagle.t_bill_snapshot_v2 RENAME TO eagle.t_bill_snapshot;
drop table if exists eagle.t_bill_snapshot;
create table if not exists eagle.t_bill_snapshot (
  `ecif_no`                                   string        comment 'ecif号',
  `partner_code`                              string        comment '合作方渠道编号',
  `product_code`                              string        comment '产品编号',
  `bill_no`                                   string        comment '借据编号',
  `contract_code`                             string        comment '合同编号',
  `loan_date`                                 string        comment '放款日',
  `loan_amount`                               decimal(15,4) comment '放款金额',
  `loan_term`                                 int           comment '贷款期数',
  `bill_status`                               string        comment '借据状态',
  `current_remain_principal`                  decimal(15,4) comment '当前剩余本金',
  `current_repaid_principal`                  decimal(15,4) comment '当前已还本金',
  `should_repay_principal`                    decimal(15,4) comment '当前应还本金',
  `should_repay_month`                        string        comment '应还款月(yyyy-MM)',
  `should_repay_date`                         string        comment '应还款日(yyyy-MM-dd)',
  `current_overdue_days`                      int           comment '当前逾期天数',
  `current_overdue_principal`                 decimal(15,4) comment '当前逾期本金',
  `current_overdue_term`                      int           comment '当前逾期期次',
  `current_overdue_stage`                     string        comment '当前逾期阶段',
  `accumulate_overdue_days`                   int           comment '累计逾期天数',
  `accumulate_overdue_num`                    int           comment '累计逾期次数',
  `loan_settlement_date`                      string        comment '借款结清日',
  `snapshot_month`                            string        comment '快照月(yyyy-MM)',
  `effective_date`                            string        comment '合同生效日',
  `expiry_date`                               string        comment '合同到期日',
  `contract_interest_rate`                    decimal(10,2) comment '合同利率',
  `rate_type`                                 string        comment '利率类型',
  `repayment_frequency`                       string        comment '还款频率',
  `repayment_type`                            string        comment '还款方式',
  `repayment_day`                             int           comment '还款日(day)',
  `nominal_interest_rate`                     decimal(10,2) comment '名义利率',
  `nominal_fee_rate`                          decimal(10,2) comment '名义费率',
  `daily_penalty_interest_rate`               decimal(10,2) comment '日罚息率',
  `guaranty_type`                             string        comment '担保方式',
  `loan_purpose`                              string        comment '贷款用途',
  `borrower_type`                             string        comment '借款方类型',
  `borrower_name`                             string        comment '借款人姓名',
  `card_type`                                 string        comment '证件类型',
  `card_no`                                   string        comment '证件号码',
  `sex`                                       string        comment '性别',
  `age`                                       string        comment '年龄',
  `id_card_area`                              string        comment '身份证地区',
  `apply_area`                                string        comment '申请地地区',
  `education`                                 string        comment '学历',
  `annual_income`                             decimal(15,4) comment '年收入',
  `current_term`                              int           comment '当前期数',
  `repaid_term`                               int           comment '已还期数',
  `remain_term`                               int           comment '剩余期数',
  `total_interest`                            decimal(15,4) comment '利息总额',
  `total_fee`                                 decimal(15,4) comment '费用总额',
  `accumulate_penalty_interest`               decimal(15,4) comment '累计罚息',
  `current_remain_interest`                   decimal(15,4) comment '当前剩余利息',
  `current_remain_fee`                        decimal(15,4) comment '当前剩余费用',
  `current_remain_penalty_interest`           decimal(15,4) comment '当前剩余罚息',
  `history_most_overdue_days`                 int           comment '历史最大逾期天数',
  `history_most_overdue_principal`            decimal(15,4) comment '历史最大逾期本金',
  `accumulate_overdue_principal`              decimal(15,4) comment '累计逾期本金',
  `first_term_overdue`                        string        comment '首期是否逾期',
  `current_risk_control_status`               string        comment '当前风控状态',
  `first_overdue_current_remain_principal`    decimal(15,4) comment '首次逾期当天剩余本金'
)COMMENT 'dm全量借据表'
partitioned by (snapshot_date String comment '快照日(yyyy-MM-dd)')
row format delimited
fields terminated by '\001'
STORED AS parquet;


drop table if exists eagle.t_bill_snapshot_v2;
create table if not exists eagle.t_bill_snapshot_v2 (
  `ecif_no`                                   string        comment 'ecif号',
  `partner_code`                              string        comment '合作方渠道编号',
  `product_code`                              string        comment '产品编号',
  `bill_no`                                   string        comment '借据编号',
  `contract_code`                             string        comment '合同编号',
  `loan_date`                                 string        comment '放款日',
  `loan_amount`                               decimal(15,4) comment '放款金额',
  `loan_term`                                 int           comment '贷款期数',
  `bill_status`                               string        comment '借据状态',
  `current_remain_principal`                  decimal(15,4) comment '当前剩余本金',
  `current_repaid_principal`                  decimal(15,4) comment '当前已还本金',
  `should_repay_principal`                    decimal(15,4) comment '当前应还本金',
  `should_repay_month`                        string        comment '应还款月(yyyy-MM)',
  `should_repay_date`                         string        comment '应还款日(yyyy-MM-dd)',
  `current_overdue_days`                      int           comment '当前逾期天数',
  `current_overdue_principal`                 decimal(15,4) comment '当前逾期本金',
  `current_overdue_term`                      int           comment '当前逾期期次',
  `current_overdue_stage`                     string        comment '当前逾期阶段',
  `accumulate_overdue_days`                   int           comment '累计逾期天数',
  `accumulate_overdue_num`                    int           comment '累计逾期次数',
  `loan_settlement_date`                      string        comment '借款结清日',
  `snapshot_month`                            string        comment '快照月(yyyy-MM)',

  `effective_date`                            string        comment '合同生效日',
  `expiry_date`                               string        comment '合同到期日',
  `contract_interest_rate`                    decimal(10,2) comment '合同利率',
  `rate_type`                                 string        comment '利率类型',
  `repayment_frequency`                       string        comment '还款频率',
  `repayment_type`                            string        comment '还款方式',
  `repayment_day`                             int           comment '还款日(day)',
  `nominal_interest_rate`                     decimal(10,2) comment '名义利率',
  `nominal_fee_rate`                          decimal(10,2) comment '名义费率',
  `daily_penalty_interest_rate`               decimal(10,2) comment '日罚息率',
  `guaranty_type`                             string        comment '担保方式',
  `loan_purpose`                              string        comment '贷款用途',
  `borrower_type`                             string        comment '借款方类型',
  `borrower_name`                             string        comment '借款人姓名',
  `card_type`                                 string        comment '证件类型',
  `card_no`                                   string        comment '证件号码',
  `sex`                                       string        comment '性别',
  `age`                                       string        comment '年龄',
  `id_card_area`                              string        comment '身份证地区',
  `apply_area`                                string        comment '申请地地区',
  `education`                                 string        comment '学历',
  `annual_income`                             decimal(15,4) comment '年收入',
  `current_term`                              int           comment '当前期数',
  `repaid_term`                               int           comment '已还期数',
  `remain_term`                               int           comment '剩余期数',
  `total_interest`                            decimal(15,4) comment '利息总额',
  `total_fee`                                 decimal(15,4) comment '费用总额',
  `accumulate_penalty_interest`               decimal(15,4) comment '累计罚息',
  `current_remain_interest`                   decimal(15,4) comment '当前剩余利息',
  `current_remain_fee`                        decimal(15,4) comment '当前剩余费用',
  `current_remain_penalty_interest`           decimal(15,4) comment '当前剩余罚息',
  `history_most_overdue_days`                 int           comment '历史最大逾期天数',
  `history_most_overdue_principal`            decimal(15,4) comment '历史最大逾期本金',
  `accumulate_overdue_principal`              decimal(15,4) comment '累计逾期本金',
  `first_term_overdue`                        string        comment '首期是否逾期',
  `current_risk_control_status`               string        comment '当前风控状态',
  `abs_statistics_date`                       string        comment 'abs临时统计日',
  `snapshot_date`                             string        comment '快照日'
)COMMENT 'dm全量借据临时表'
row format delimited
fields terminated by '\001'
STORED AS parquet;

-- ----------------------------
-- Table structure for eagle.t_repayment_info
-- ----------------------------
drop table if exists eagle.t_repayment_info;
create table if not exists eagle.t_repayment_info (
  `bill_no`                                   string        comment '借据编号',
  `repaid_term`                               int           comment '还款期次',
  `actual_repaid_date`                        string        comment '实还日期',
  `actual_repaid_amount`                      decimal(15,4) comment '已还金额',
  `actual_repaid_principal`                   decimal(15,4) comment '已还本金',
  `actual_repaid_interest`                    decimal(15,4) comment '已还利息',
  `actual_repaid_fee`                         decimal(15,4) comment '已还费用',
  `actual_repaid_penalty_interest`            decimal(15,4) comment '已还罚息',
  `principal_balance`                         decimal(15,4) comment '本金余额',
  `account_status`                            string        comment '账户状态',
  `update_time`                               string        comment '更新时间（yyyy-MM-ddHH:mm:ss）',
  `create_time`                               string        comment '创建时间（yyyy-MM-ddHH:mm:ss）'
)COMMENT 'dm实际还款信息表'
row format delimited
fields terminated by '\001'
STORED AS parquet;

-- ----------------------------
-- Table structure for eagle.t_asset_pay_flow
-- ----------------------------
drop table if exists eagle.t_asset_pay_flow;
create table if not exists eagle.t_asset_pay_flow (
  `bill_no`                                   string        comment '借据编号',
  `trade_type`                                string        comment '交易类型',
  `trade_channel`                             string        comment '交易渠道',
  `order_no`                                  string        comment '订单号',
  `trade_time`                                string        comment '交易时间',
  `order_amount`                              decimal(15,4) comment '订单金额',
  `trade_status`                              string        comment '交易状态',
  `update_time`                               string        comment '更新时间（yyyy-MM-ddHH:mm:ss）',
  `create_time`                               string        comment '创建时间（yyyy-MM-ddHH:mm:ss）'
)COMMENT 'dm资产支付流水信息表'
row format delimited
fields terminated by '\001'
STORED AS parquet;

-- ----------------------------
-- Table structure for eagle.t_guaranty_car
-- ----------------------------
drop table if exists eagle.t_guaranty_car;
create table if not exists eagle.t_guaranty_car (
  `bill_no`                                   string        comment '借据编号',
  `frame_num`                                 string        comment '车架号',
  `car_brand`                                 string        comment '车辆品牌',
  `car_model`                                 string        comment '车辆型号',
  `car_colour`                                string        comment '车辆颜色',
  `license_num`                               string        comment '车辆号码',
  `register_date`                             string        comment '注册日期',
  `pawn_value`                                decimal(15,4) comment '评估价值',
  `update_time`                               string        comment '更新时间',
  `create_time`                               string        comment '创建时间'
)COMMENT 'dm抵押物车信息表'
row format delimited
fields terminated by '\001'
STORED AS parquet;

-- ----------------------------
-- Table structure for eagle.t_repayment_schedule
-- ----------------------------
drop table if exists eagle.t_repayment_schedule;
create table if not exists eagle.t_repayment_schedule (
  `bill_no`                                   string        comment '借据编号',
  `term`                                      int           comment '期数',
  `bill_date`                                 string        comment '账单日',
  `should_repay_date`                         string        comment '还款日',
  `grace_date`                                string        comment '宽限日',
  `should_repay_amount`                       decimal(15,4) comment '应还金额',
  `should_repay_principal`                    decimal(15,4) comment '应还本金',
  `should_repay_interest`                     decimal(15,4) comment '应还利息',
  `should_repay_fee`                          decimal(15,4) comment '应还费用',
  `accumulate_penalty_interest`               decimal(15,4) comment '累计罚息',
  `repaid_amount`                             decimal(15,4) comment '已还金额',
  `repaid_pincipal`                           decimal(15,4) comment '已还本金',
  `repaid_interest`                           decimal(15,4) comment '已还利息',
  `repaid_fee`                                decimal(15,4) comment '已还费用',
  `repaid_penalty_interest`                   decimal(15,4) comment '已还罚息',
  `repaid_count`                              int           comment '还款笔数',
  `settlement_status`                         string        comment '结清标志',
  `update_time`                               string        comment '更新时间（yyyy-MM-ddHH:mm:ss）',
  `create_time`                               string        comment '创建时间（yyyy-MM-ddHH:mm:ss）'
)COMMENT 'dm还款计划表'
row format delimited
fields terminated by '\001'
STORED AS parquet;


drop table if exists eagle.t_credit_info;
create table if not exists eagle.t_credit_info (
  `apply_no`                                  string        comment '申请号',
  `bill_no`                                   string        comment '借据编号',
  `assessment_date`                           string        comment '评估日期',
  `credit_result`                             string        comment '授信结果',
  `credit_amount`                             decimal(15,4) comment '授信额度',
  `credit_validity`                           string        comment '授信有效期（yyyy-MM-dd）',
  `refuse_reason`                             string        comment '拒绝原因',
  `current_remain_credit_amount`              decimal(15,4) comment '当前剩余额度',
  `current_credit_amount_utilization_rate`    decimal(10,2) comment '当前额度使用率',
  `accumulate_credit_amount_utilization_rate` decimal(10,2) comment '累计额度使用率',
  `snapshot_date`                             string        comment '快照日期(yyyy-MM-dd)',
  `update_time`                               string        comment '更新时间（yyyy-MM-ddHH:mm:ss）',
  `create_time`                               string        comment '创建时间（yyyy-MM-ddHH:mm:ss）'
)COMMENT 'dm授信信息快照表'
row format delimited
fields terminated by '\001'
STORED AS parquet;

drop table if exists eagle.t_credit_results;
create table if not exists eagle.t_credit_results(
  `bill_no`                                   string        comment '借据编号',
  `risk_control_type`                         string        comment '风控类型',
  `assessment_date`                           string        comment '评估日期',
  `risk_control_result`                       string        comment '风控结果',
  `asset_level`                               string        comment '资产等级',
  `credit_level`                              string        comment '信用等级',
  `anti_fraud_level`                          string        comment '反欺诈等级',
  `credit_amount`                             decimal(15,4) comment '金额',
  `credit_validity`                           string        comment '有效期',
  `refuse_reason`                             string        comment '拒绝原因',
  `update_time`                               string        comment '更新时间（yyyy-MM-ddHH:mm:ss）',
  `create_time`                               string        comment '创建时间（yyyy-MM-ddHH:mm:ss）'
)COMMENT 'dm用信风控结果表'
row format delimited
fields terminated by '\001'
STORED AS parquet;

drop table if exists eagle.t_contact_person;
create table if not exists eagle.t_contact_person(
  `ecif_no`                                   string        comment 'ecifNo',
  `bill_no`                                   string        comment '借据号',
  `relationship`                              string        comment '与借款人关系',
  `name`                                      string        comment '姓名',
  `phone_num`                                 string        comment '手机号',
  `update_time`                               string        comment '更新时间',
  `create_time`                               string        comment '创建时间'
)COMMENT 'dm联系人表'
row format delimited
fields terminated by '\001'
STORED AS parquet;

drop table if exists eagle.t_collection_info;
create table if not exists eagle.t_collection_info(
  `ecif_no`                                   string,
  `bill_no`                                   string        comment '借据编号',
  `acquisition_time`                          string        comment '催收时间',
  `acquisition_group`                         string        comment '催收组',
  `acquisition_person`                        string        comment '催收员',
  `customer_name`                             string        comment '客户姓名',
  `contacts_name`                             string        comment '联系人姓名',
  `contacts_relation`                         string        comment '联系人关系',
  `contact_number`                            string        comment '联系电话',
  `contact_result`                            string        comment '联系结果',
  `acquisition_result`                        string        comment '催收结果',
  `result_detail`                             string        comment '结果详情',
  `promise_amount`                            decimal(15,4) comment '承诺金额',
  `promise_date`                              string        comment '承诺日期',
  `update_time`                               string        comment '更新时间（yyyy-MM-ddHH:mm:ss）',
  `create_time`                               string        comment '创建时间（yyyy-MM-ddHH:mm:ss）'
)COMMENT 'dm催记信息表'
PARTITIONED BY (`snapshot_date` string)
row format delimited
fields terminated by '\001'
STORED AS parquet;

drop table if exists eagle.t_commitment_info;
create table if not exists eagle.t_commitment_info(
  `ecif_no`                                   string,
  `bill_no`                                   string        comment '借据编号',
  `acquisition_time`                          string        comment '催收时间',
  `promise_amount`                            decimal(15,4) comment '承诺金额',
  `promise_date`                              string        comment '承诺日期',
  `repaid_amount`                             decimal(15,4) comment '还款金额',
  `promise_status`                            string        comment '承诺状态',
  `update_time`                               string        comment '更新时间（yyyy-MM-ddHH:mm:ss）',
  `create_time`                               string        comment '创建时间（yyyy-MM-ddHH:mm:ss）'
)COMMENT 'dm承诺信息表'
PARTITIONED BY (`snapshot_date` string)
row format delimited
fields terminated by '\001'
STORED AS parquet;

drop table if exists eagle.t_sound_record_info;
create table if not exists eagle.t_sound_record_info(
  `ecif_no`                                   string,
  `bill_no`                                   string        comment '借据编号',
  `contacts_name`                             string        comment '联系人姓名',
  `contacts_relation`                         string        comment '联系人关系',
  `outbound_call_number`                      string        comment '外呼号码',
  `send_status`                               string        comment '发送状态',
  `is_answer`                                 string        comment '是否应答',
  `talk_time`                                 string        comment '通话时长',
  `acquisition_group`                         string        comment '催收组',
  `acquisition_person`                        string        comment '催收员',
  `acquisition_record_document_name`          string        comment '催收录音文件名称',
  `acquisition_record_document_path`          string        comment '催收录音文件下载地址',
  `outbound_call_time`                        string        COMMENT '外呼时间',
  `update_time`                               string        comment '更新时间（yyyy-MM-ddHH:mm:ss）',
  `create_time`                               string        comment '创建时间（yyyy-MM-ddHH:mm:ss）'
)COMMENT 'dm录音表'
PARTITIONED BY (`snapshot_date` string)
row format delimited
fields terminated by '\001'
STORED AS parquet;

drop table if exists eagle.t_recovery_detail;
create table if not exists eagle.t_recovery_detail(
  `ecif_no`                                   string        COMMENT 'ecif号',
  `case_sub_id`                               string        COMMENT '子案件编号',
  `partner_code`                              string        COMMENT '合作方渠道编号',
  `product_code`                              string        COMMENT '产品编号',
  `bill_no`                                   string        COMMENT '借据编号',
  `contract_code`                             string        COMMENT '合同编号',
  `loan_term`                                 int           COMMENT '贷款期数',
  `input_coll_date`                           string        COMMENT '入催日期(yyyy-MM-dd)',
  `input_coll_month`                          string        COMMENT '入催月(yyyy-MM)',
  `input_coll_amount`                         decimal(15,4) COMMENT '入催金额',
  `is_close`                                  string        COMMENT '是否结案',
  `is_contact`                                string        COMMENT '是否可联',
  `last_deal_date`                            string        COMMENT '上次触碰日期(yyyy-MM-dd)',
  `promise_amount`                            decimal(15,4) COMMENT '承诺金额',
  `cash_promise_amount`                       decimal(15,4) COMMENT '承诺兑现金额',
  `snapshot_month`                            string        COMMENT '快照月(yyyy-MM)'
  )COMMENT                                    'dm催收回收表'
PARTITIONED BY (`snapshot_date` string)
row format delimited
fields terminated by '\001'
STORED AS parquet;

drop table if exists eagle.t_credit_detail;
create table if not exists eagle.t_credit_detail(
  `ecif_no`                                   string        comment 'ecif号',
  `channel_code`                              string        comment '合作方渠道编号',
  `product_code`                              string        comment '产品编号',
  `is_credit_success`                         tinyint       comment '是否授信通过，1-通过，0-不通过',
  `failure_msg`                               string        comment '授信失败原因',
  `approval_time`                             string        comment '授信通过时间（YYYY-MM-DDHH:mm:ss）',
  `approval_amount`                           decimal(15,4) comment '授信通过金额',
  `apply_amount`                              decimal(15,4) comment '授信申请金额',
  `apply_time`                                string        comment '授信申请时间（YYYY-MM-DDHH:mm:ss）',
  `credit_id`                                 string        comment '授信id',
  `credit_validity`                           string        comment '授信有效期',
  `ext`                                       string        comment '扩展字段'
)COMMENT 'dm授信详情表'
row format delimited
fields terminated by '\001'
STORED AS parquet;

drop table if exists eagle.t_on_loan_detail;
create table if not exists eagle.t_on_loan_detail(
  `ecif_no`                                   string        comment 'ecif号',
  `loan_id`                                   string        comment '借据号',
  `channel_code`                              string        comment '合作方渠道编号',
  `product_code`                              string        comment '产品编号',
  `apply_amt`                                 decimal(15,4) comment '申请用信金额',
  `approval_amt`                              decimal(15,4) comment '用信审批通过金额',
  `refusal_cause`                             string        comment '拒绝原因（编码）',
  `refusal_cause_cn`                          string        comment '拒绝原因（详情）',
  `snapshot_date`                             string        comment '快照日期（YYYY-MM-DD）',
  `data_time`                                 string        comment '数据时间：未审批时为申请时间，审批后为审批时间（YYYY-MM-DD）',
  `credit_id`                                 string        comment '授信id',
  `apply_date`                                string        comment '申请用信时间（YYYY-MM-DD）',
  `approval_date`                             string        comment '审批时间（YYYY-MM-DD）',
  `approval_status`                           int           comment '审批状态1-通过，0-不通过',
  `first_use_credit`                          int           comment '是否是第一次用信：1-是，0-否',
  `product_terms`                             int           comment '申请用信的产品的期数',
  `ext`                                       string        comment '扩展字段'
)COMMENT 'dm用信详情表'
row format delimited
fields terminated by '\001'
STORED AS parquet;

drop table if exists eagle.t_register_summary;
create table if not exists eagle.t_register_summary(
  `channel_code`                              string        comment '合作方渠道编号',
  `product_code`                              string        comment '产品编号',
  `record_date`                               string        comment '记录时间',
  `reg_page_view_cnt`                         int           comment '注册页面浏览人数',
  `reg_success_cnt`                           int           comment '注册人数',
  `ext`                                       string        comment '扩展字段'
)COMMENT 'dm注册统计表'
row format delimited
fields terminated by '\001'
STORED AS parquet;

drop table if exists eagle.t_product_summary;
create table if not exists eagle.t_product_summary(
  `channel_code`                              string        comment '渠道编号',
  `product_code`                              string        comment '产品编号',
  `loan_count_daily`                          int           comment '放款笔数',
  `loan_daily`                                decimal(15,4) comment '放款金额',
  `repay_count_daily`                         int           comment '还款笔数',
  `repay_daily`                               decimal(15,4) comment '还款金额',
  `date_time`                                 string        comment '日期',
  `pass_rate`                                 decimal(15,4) comment '通过率',
  `bad_rate`                                  decimal(15,4) comment '坏账率',
  `loan_sum`                                  decimal(15,4) comment '累计放款金额',
  `repay_sum`                                 decimal(15,4) comment '累计还款金额',
  `loan_avg`                                  decimal(15,4) comment '平均放款金额',
  `repay_avg`                                 decimal(15,4) comment '平均还款金额',
  `loan_request_amount`                       int,
  `over_due_remain`                           decimal(15,4),
  `over_due_loan_sum`                         decimal(15,4)
)COMMENT 'dm产品统计信息表'
row format delimited
fields terminated by '\001'
STORED AS parquet;


-- ----------------------------
-- dw层表
-- ----------------------------
-- ----------------------------
-- Table structure for ws_dw.t_bill_loan_detail
-- ----------------------------
drop table if exists ws_dw.t_bill_loan_detail;
create table if not exists ws_dw.t_bill_loan_detail (
  `ecif_no`                                   string        comment 'ecif号',
  `project_id`                                string        comment '项目id',
  `agency_id`                                 string        comment '机构编号',
  `asset_id`                                  string        comment '资产借据号',
  `contract_code`                             string        comment '贷款合同编号',
  `loan_total_amount`                         decimal(16,2) comment '贷款总金额',
  `periods`                                   int           comment '总期数',
  `repay_type`                                string        comment '还款方式,0-等额本息,1-等额本金,2-等本等息3-先息后本4-一次性还本付息,5-气球贷,6-自定义还款计划',
  `interest_rate_type`                        string        comment '0-固定利率,1-浮动利率',
  `loan_interest_rate`                        decimal(8,6)  comment '贷款年利率，单位:%',
  `contract_data_status`                      int           comment '合同数据与还款计划中的总额是否一致，0-一致，1-不一致',
  `contract_status`                           varchar(8)    comment '0-生效,1-不生效',
  `first_repay_date`                          string        comment '首次还款日期，yyyy-mm-dd',
  `first_loan_end_date`                       string        comment '合同结束时间',
  `per_loan_end_date`                         string        comment '上一次合同结束时间',
  `cur_loan_end_date`                         string        comment '当前合同结束时间',
  `loan_begin_date`                           string        comment '合同开始时间',
  `loan_total_interest`                       decimal(16,2) comment '贷款总利息',
  `loan_total_fee`                            decimal(16,2) comment '贷款总费用',
  `loan_penalty_rate`                         decimal(8,6)  comment '贷款罚息利率',
  `repay_frequency`                           string        comment '还款频率',
  `nominal_rate`                              decimal(16,4) comment '名义费率',
  `daily_penalty_rate`                        decimal(16,4) comment '日罚息率',
  `loan_use`                                  string        comment '贷款用途',
  `guarantee_type`                            string        comment '担保方式',

  `customer_name`                             string        comment '客户姓名',
  `document_num`                              string        comment '身份证号码',
  `phone_num`                                 string        comment '手机号码',
  `age`                                       int           comment '年龄',
  `sex`                                       string        comment '性别，0-男，2-女',
  `marital_status`                            string        comment '婚姻状态,0-已婚，1-未婚，2-离异，3-丧偶',
  `degree`                                    string        comment '学位，0-小学，1-初中，2-高中/职高/技校，3-大专，4-本科,5-硕士,6-博士，7-文盲和半文盲',
  `province`                                  string        comment '客户所在省',
  `city`                                      string        comment '客户所在市',
  `address`                                   string        comment '客户所在地区',
  `card_no`                                   string        comment '身份证号码(脱敏处理)',
  `phone_no`                                  string        comment '手机号码(脱敏处理)',
  `imei`                                      string        comment 'imei号',
  `education`                                 string        comment '学位',
  `annual_income`                             decimal(16,2) comment '年收入(元)'
)COMMENT 'dw借据合同详情表'
row format delimited
fields terminated by '\001'
STORED AS parquet;


-- ----------------------------
-- Table structure for eagle.t_repayment_info
-- ----------------------------
drop table if exists ws_dw.t_repayment_info;
create table if not exists ws_dw.t_repayment_info (
  `id`                                        int           comment '实际还款信息表主键',
  `project_id`                                string        comment '项目id',
  `agency_id`                                 string        comment '机构编号',
  `asset_id`                                  string        comment '资产借据号',
  `repay_date`                                string        comment '应还款日期',
  `repay_principal`                           decimal(16,2),
  `repay_interest`                            decimal(16,2),
  `repay_fee`                                 decimal(16,2),
  `rel_pay_date`                              string        comment '实际还清日期',
  `rel_principal`                             decimal(16,2) comment '实还本金',
  `rel_interest`                              decimal(16,2) comment '实还利息',
  `rel_fee`                                   decimal(16,2) comment '实还费用',
  `period`                                    int           comment '期次',
  `timestamp`                                 string,
  `create_time`                               string        comment '创建时间',
  `update_time`                               string        comment '更新时间',
  `free_amount`                               decimal(16,2) comment '免息金额',
  `remainder_principal`                       decimal(16,2) comment '剩余本金',
  `remainder_interest`                        decimal(16,2) comment '剩余利息',
  `remainder_fee`                             decimal(16,2) comment '剩余费用',
  `remainder_periods`                         int           comment '剩余期数',
  `repay_type`                                string        comment '还款类型',
  `current_loan_balance`                      decimal(16,2) comment '当期贷款余额',
  `account_status`                            string        comment '当期账户状态',
  `overdue_day`                               int           comment '逾期天数',
  `finish_periods`                            int           comment '已还期数',
  `plan_begin_loan_principal`                 decimal(16,2) comment '期初剩余本金',
  `plan_end_loan_principal`                   decimal(16,2) comment '期末剩余本金',
  `plan_begin_loan_interest`                  decimal(16,2) comment '期初剩余利息',
  `plan_end_loan_interest`                    decimal(16,2) comment '期末剩余利息',
  `plan_begin_loan_fee`                       decimal(16,2) comment '期初剩余费用',
  `plan_end_loan_fee`                         decimal(16,2) comment '期末剩余费用',
  `plan_remainder_periods`                    int           comment '剩余期数',
  `plan_next_repay_date`                      string        comment '下次应还日期',
  `real_interest_rate`                        decimal(16,2) comment '实际执行利率',
  `current_status`                            string        comment '当期状态',
  `repay_penalty`                             decimal(16,2) comment '应还罚息',
  `rel_penalty`                               decimal(16,2) comment '应还罚息'
)COMMENT 'dm实际还款信息表'
row format delimited
fields terminated by '\001'
STORED AS parquet;


CREATE TABLE if not exists id_service.mapping (
  `ecif_no`                                   STRING        COMMENT 'ECIF号',
  `age`                                       STRING        COMMENT '年龄段标签',
  `region`                                    STRING        COMMENT '区域标签',
  `province`                                  STRING        COMMENT '省份标签',
  `gender`                                    STRING        COMMENT '性别标签',
  `education`                                 STRING        COMMENT '教育标签',
  `income`                                    STRING        COMMENT '收入标签'
)
row format delimited
fields terminated by '\001'
STORED AS PARQUET;


CREATE TABLE if not exists id_service.mapping_cache (
  `ecif_no`                                   STRING        COMMENT 'ECIF号'
)
PARTITIONED BY (strategy_code STRING,attribute STRING )
row format delimited
fields terminated by '\001'
STORED AS PARQUET;


CREATE TABLE if not exists ws_dw.dw1_original_features(
  `id`                                        string        COMMENT 'id',
  `idtype`                                    int           COMMENT 'id类型',
  `ecifid`                                    string        COMMENT 'ecifid',
  `key`                                       string        COMMENT '键',
  `value`                                     string        COMMENT '值',
  `spark_deal_time`                           int           COMMENT 'spark处理时间戳')
COMMENT '原始特征表'
PARTITIONED BY (`have_ecifid` string)
row format delimited
fields terminated by '\001'
STORED AS PARQUET;

CREATE TABLE if not exists ws_dw.dw2_derivative_features(
  `ecifid`                                    string        COMMENT 'ecifid',
  `key`                                       string        COMMENT '键',
  `value`                                     string        COMMENT '值',
  `spark_deal_time`                           int           COMMENT 'spark处理时间戳')
COMMENT '衍生特征表'
PARTITIONED BY (`dt` string)
row format delimited
fields terminated by '\001'
STORED AS PARQUET;

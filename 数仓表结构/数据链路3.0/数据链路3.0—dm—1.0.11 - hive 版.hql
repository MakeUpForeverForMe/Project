DROP DATABASE IF EXISTS dm;
CREATE DATABASE IF NOT EXISTS dm;

-- 看管还款计划表
DROP TABLE IF EXISTS `dm.dm_watch_repayment_schedule`;
CREATE TABLE IF NOT EXISTS `dm.dm_watch_repayment_schedule`(
  `bill_no`                                   string        COMMENT '借据编号',
  `term`                                      int           COMMENT '期数',
  `bill_date`                                 string        COMMENT '账单日',
  `should_repay_date`                         string        COMMENT '还款日',
  `grace_date`                                string        COMMENT '宽限日',
  `should_repay_amount`                       decimal(15,4) COMMENT '应还金额',
  `should_repay_principal`                    decimal(15,4) COMMENT '应还本金',
  `should_repay_interest`                     decimal(15,4) COMMENT '应还利息',
  `should_repay_fee`                          decimal(15,4) COMMENT '应还费用',
  `accumulate_penalty_interest`               decimal(15,4) COMMENT '累计罚息',
  `repaid_amount`                             decimal(15,4) COMMENT '已还金额',
  `repaid_pincipal`                           decimal(15,4) COMMENT '已还本金',
  `repaid_interest`                           decimal(15,4) COMMENT '已还利息',
  `repaid_fee`                                decimal(15,4) COMMENT '已还费用',
  `repaid_penalty_interest`                   decimal(15,4) COMMENT '已还罚息',
  `repaid_count`                              int           COMMENT '还款笔数',
  `settlement_status`                         string        COMMENT '还款计划状态',
  `create_time`                               string        COMMENT '创建时间',
  `update_time`                               string        COMMENT '更新时间'
) COMMENT '还款计划表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


-- 看管全量借据表
DROP TABLE IF EXISTS `dm.dm_watch_bill_snapshot`;
CREATE TABLE IF NOT EXISTS `dm.dm_watch_bill_snapshot`(
  `ecif_no`                                   string        COMMENT 'ecif号',
  `partner_code`                              string        COMMENT '合作方渠道编号',
  `product_code`                              string        COMMENT '产品编号',
  `bill_no`                                   string        COMMENT '借据编号',
  `contract_code`                             string        COMMENT '合同编号',
  `loan_date`                                 string        COMMENT '放款日',
  `loan_amount`                               decimal(15,2) COMMENT '放款金额',
  `loan_term`                                 int           COMMENT '贷款期数',
  `bill_status`                               string        COMMENT '借据状态',
  `current_remain_principal`                  decimal(15,2) COMMENT '当前剩余本金',
  `current_repaid_principal`                  decimal(15,2) COMMENT '当前已还本金',
  `should_repay_principal`                    decimal(15,2) COMMENT '当前应还本金',
  `should_repay_month`                        string        COMMENT '应还款月（yyyy-MM）',
  `should_repay_date`                         string        COMMENT '应还款日（yyyy-MM-dd）',
  `current_overdue_days`                      int           COMMENT '当前逾期天数',
  `current_overdue_principal`                 decimal(15,2) COMMENT '当前逾期本金',
  `current_overdue_term`                      int           COMMENT '当前逾期期次',
  `current_overdue_stage`                     string        COMMENT '当前逾期阶段',
  `accumulate_overdue_days`                   int           COMMENT '累计逾期天数',
  `accumulate_overdue_num`                    int           COMMENT '累计逾期次数',
  `loan_settlement_date`                      string        COMMENT '借款结清日',
  `snapshot_month`                            string        COMMENT '快照月（yyyy-MM）',
  `effective_date`                            string        COMMENT '合同生效日',
  `expiry_date`                               string        COMMENT '合同到期日',
  `contract_interest_rate`                    decimal(15,6) COMMENT '合同利率',
  `rate_type`                                 string        COMMENT '利率类型',
  `repayment_frequency`                       string        COMMENT '还款频率',
  `repayment_type`                            string        COMMENT '还款方式',
  `repayment_day`                             int           COMMENT '还款日（day）',
  `nominal_interest_rate`                     decimal(15,6) COMMENT '名义利率',
  `nominal_fee_rate`                          decimal(15,6) COMMENT '名义费率',
  `daily_penalty_interest_rate`               decimal(15,6) COMMENT '日罚息率',
  `guaranty_type`                             string        COMMENT '担保方式',
  `loan_purpose`                              string        COMMENT '贷款用途',
  `borrower_type`                             string        COMMENT '借款方类型',
  `borrower_name`                             string        COMMENT '借款人姓名',
  `card_type`                                 string        COMMENT '证件类型',
  `card_no`                                   string        COMMENT '证件号码',
  `sex`                                       string        COMMENT '性别',
  `age`                                       string        COMMENT '年龄',
  `id_card_area`                              string        COMMENT '身份证地区',
  `apply_area`                                string        COMMENT '申请地地区',
  `education`                                 string        COMMENT '学历',
  `annual_income`                             decimal(15,2) COMMENT '年收入',
  `current_term`                              int           COMMENT '当前期数',
  `repaid_term`                               int           COMMENT '已还期数',
  `remain_term`                               int           COMMENT '剩余期数',
  `total_interest`                            decimal(15,2) COMMENT '利息总额',
  `total_fee`                                 decimal(15,2) COMMENT '费用总额',
  `accumulate_penalty_interest`               decimal(15,2) COMMENT '累计罚息',
  `current_remain_interest`                   decimal(15,2) COMMENT '当前剩余利息',
  `current_remain_fee`                        decimal(15,2) COMMENT '当前剩余费用',
  `current_remain_penalty_interest`           decimal(15,2) COMMENT '当前剩余罚息',
  `history_most_overdue_days`                 int           COMMENT '历史最大逾期天数',
  `history_most_overdue_principal`            decimal(15,2) COMMENT '历史最大逾期本金',
  `accumulate_overdue_principal`              decimal(15,2) COMMENT '累计逾期本金',
  `first_term_overdue`                        string        COMMENT '首期是否逾期',
  `current_risk_control_status`               string        COMMENT '当前风控状态',
  `first_overdue_current_remain_principal`    decimal(15,2) COMMENT '首次逾期当天剩余本金',
  `snapshot_date`                             string        COMMENT '快照日期（yyyy-MM-dd）'
) COMMENT '看管全量借据表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


-------实际还款信息表-----
DROP TABLE IF EXISTS `dm.dm_watch_repayment_info`;
CREATE TABLE IF NOT EXISTS `dm.dm_watch_repayment_info`(
  `bill_no`                                   string        COMMENT '借据号',
  `repaid_term`                               int           COMMENT '还款期次',
  `actual_repaid_date`                        string        COMMENT '实还日期',
  `actual_repaid_amount`                      decimal(20,2) COMMENT '已还金额',
  `actual_repaid_principal`                   decimal(20,2) COMMENT '已还本金',
  `actual_repaid_interest`                    decimal(20,2) COMMENT '已还利息',
  `actual_repaid_fee`                         decimal(20,2) COMMENT '已还费用',
  `actual_repaid_penalty_interest`            decimal(20,2) COMMENT '已还罚息',
  `principal_balance`                         decimal(20,2) COMMENT '还款后剩余本金',
  `account_status`                            string        COMMENT '还款类型',
  `update_time`                               string        COMMENT '更新时间（yyyy-MM-ddHH:mm:ss）',
  `create_time`                               string        COMMENT '创建时间（yyyy-MM-ddHH:mm:ss）'
) COMMENT '实际还款信息表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


--授信详情表
DROP TABLE IF EXISTS `dm.dm_watch_credit_detail`;
CREATE TABLE IF NOT EXISTS `dm.dm_watch_credit_detail`(
  `ecif_no`                                   string        COMMENT 'ecif号',
  `channel_code`                              string        COMMENT '合作方渠道编号',
  `product_code`                              string        COMMENT '产品编号',
  `is_credit_success`                         int           COMMENT '是否授信通过（1：通过，0：不通过）',
  `failure_msg`                               string        COMMENT '授信失败原因',
  `approval_time`                             string        COMMENT '授信通过时间（YYYY-MM-DDHH:mm:ss）',
  `approval_amount`                           decimal(15,4) COMMENT '授信通过金额',
  `apply_amount`                              decimal(15,4) COMMENT '授信申请金额',
  `apply_time`                                string        COMMENT '授信申请时间（YYYY-MM-DDHH:mm:ss）',
  `credit_id`                                 string        COMMENT '授信id',
  `credit_validity`                           string        COMMENT '授信有效期',
  `ext`                                       string        COMMENT '扩展字段'
) COMMENT '授信详情表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


--授信信息快照表
DROP TABLE IF EXISTS `dm.dm_watch_credit_info`;
CREATE TABLE IF NOT EXISTS `dm.dm_watch_credit_info`(
  `apply_no`                                  string        COMMENT '申请号',
  `bill_no`                                   string        COMMENT '借据编号',
  `assessment_date`                           string        COMMENT '评估日期',
  `credit_result`                             string        COMMENT '授信结果',
  `credit_amount`                             decimal(15,4) COMMENT '授信额度',
  `credit_validity`                           string        COMMENT '授信有效期（yyyy-MM-dd）',
  `refuse_reason`                             string        COMMENT '拒绝原因',
  `current_remain_credit_amount`              decimal(15,4) COMMENT '当前剩余额度',
  `current_credit_amount_utilization_rate`    decimal(10,2) COMMENT '当前额度使用率',
  `accumulate_credit_amount_utilization_rate` decimal(10,2) COMMENT '累计额度使用率',
  `snapshot_date`                             string        COMMENT '快照日期（yyyy-MM-dd）',
  `update_time`                               string        COMMENT '更新时间',
  `create_time`                               string        COMMENT '创建时间'
) COMMENT '授信信息快照表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


--用信风控结果表
DROP TABLE IF EXISTS `dm.dm_watch_credit_results`;
CREATE TABLE IF NOT EXISTS `dm.dm_watch_credit_results`(
  `bill_no`                                   string        COMMENT '借据编号',
  `risk_control_type`                         string        COMMENT '风控类型',
  `assessment_date`                           string        COMMENT '评估日期',
  `risk_control_result`                       string        COMMENT '风控结果',
  `asset_level`                               string        COMMENT '资产等级',
  `credit_level`                              string        COMMENT '信用等级',
  `anti_fraud_level`                          string        COMMENT '反欺诈等级',
  `credit_amount`                             decimal(15,4) COMMENT '金额',
  `credit_validity`                           string        COMMENT '有效期',
  `refuse_reason`                             string        COMMENT '拒绝原因',
  `update_time`                               string        COMMENT '更新时间',
  `create_time`                               string        COMMENT '创建时间'
) COMMENT '用信风控结果表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


--用信详情表
DROP TABLE IF EXISTS `dm.dm_watch_on_loan_detail`;
CREATE TABLE IF NOT EXISTS `dm.dm_watch_on_loan_detail`(
  `ecif_no`                                   string        COMMENT 'ecif号',
  `loan_id`                                   string        COMMENT '借据号',
  `channel_code`                              string        COMMENT '合作方渠道编号',
  `product_code`                              string        COMMENT '产品编号',
  `apply_amt`                                 decimal(15,4) COMMENT '申请用信金额',
  `approval_amt`                              decimal(15,4) COMMENT '用信审批通过金额',
  `refusal_cause`                             string        COMMENT '拒绝原因（编码）',
  `refusal_cause_cn`                          string        COMMENT '拒绝原因（详情）',
  `snapshot_date`                             string        COMMENT '快照日期（YYYY-MM-DD）',
  `data_time`                                 string        COMMENT '数据时间：未审批时为申请时间，审批后为审批时间（YYYY-MM-DD）',
  `credit_id`                                 string        COMMENT '授信id',
  `apply_date`                                string        COMMENT '申请用信时间（YYYY-MM-DD）',
  `approval_date`                             string        COMMENT '审批时间（YYYY-MM-DD）',
  `approval_status`                           int           COMMENT '审批状态（1：通过，0：不通过）',
  `first_use_credit`                          int           COMMENT '是否是第一次用信（1：是，0：否）',
  `product_terms`                             int           COMMENT '申请用信的产品的期数',
  `ext`                                       string        COMMENT '扩展字段'
) COMMENT '用信详情表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


---联系人
DROP TABLE IF EXISTS `dm.dm_watch_contact_person`;
CREATE TABLE IF NOT EXISTS `dm.dm_watch_contact_person`(
  `ecif_no`                                   string        COMMENT 'ecif',
  `bill_no`                                   string        COMMENT '借据号',
  `relationship`                              string        COMMENT '与借款人关系（维度-客户联系人信息表，与申请人关系）',
  `name`                                      string        COMMENT '姓名',
  `phone_num`                                 string        COMMENT '手机号',
  `update_time`                               string        COMMENT '更新时间',
  `create_time`                               string        COMMENT '创建时间'
) COMMENT '联系人表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


--资产支付流水信息表
DROP TABLE IF EXISTS `dm.dm_watch_asset_pay_flow`;
CREATE TABLE IF NOT EXISTS `dm.dm_watch_asset_pay_flow`(
  `bill_no`                                   string        COMMENT '借据号',
  `trade_type`                                string        COMMENT '交易类型',
  `trade_channel`                             string        COMMENT '交易渠道',
  `order_no`                                  string        COMMENT '订单号',
  `trade_time`                                string        COMMENT '交易时间',
  `order_amount`                              decimal(15,4) COMMENT '订单金额',
  `trade_status`                              string        COMMENT '批次日',
  `update_time`                               string        COMMENT '更新时间',
  `create_time`                               string        COMMENT '创建时间'
) COMMENT '资产支付流水信息表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


-- 产品统计信息表
DROP TABLE IF EXISTS `dm.dm_watch_product_summary`;
CREATE TABLE IF NOT EXISTS `dm.dm_watch_product_summary`(
  `channel_code`                              string        COMMENT '渠道编号',
  `product_code`                              string        COMMENT '产品编号',
  `loan_count_daily`                          int           COMMENT '放款笔数',
  `loan_daily`                                decimal(15,4) COMMENT '放款金额',
  `repay_count_daily`                         int           COMMENT '还款笔数',
  `repay_daily`                               decimal(15,4) COMMENT '还款金额',
  `date_time`                                 string        COMMENT '统计日期',
  `pass_rate`                                 decimal(15,4) COMMENT '通过率：授信通过人数/授信申请人数',
  `bad_rate`                                  decimal(15,4) COMMENT '坏账率：逾期超过180天的贷款所有剩余未还本金/累计放款金额',
  `loan_sum`                                  decimal(15,4) COMMENT '累计放款金额',
  `repay_sum`                                 decimal(15,4) COMMENT '累计还款金额',
  `loan_avg`                                  decimal(15,4) COMMENT '平均放款金额',
  `repay_avg`                                 decimal(15,4) COMMENT '平均还款金额：累计还款金额/还款笔数',
  `loan_request_amount`                       int           COMMENT '贷款申请笔数',
  `over_due_remain`                           decimal(15,4) COMMENT '剩余逾期金额',
  `over_due_loan_sum`                         decimal(15,4) COMMENT '逾期总额'
) COMMENT '产品统计信息表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;




















--dm催收采集表
DROP TABLE IF EXISTS `dm.dm_watch_collection_info`;
CREATE TABLE IF NOT EXISTS `dm.dm_watch_collection_info`(
  `ecif_no`                                   string        COMMENT '客户要素',
  `bill_no`                                   string        COMMENT '借据编号',
  `acquisition_time`                          string        COMMENT '催收时间',
  `acquisition_group`                         string        COMMENT '催收组',
  `acquisition_person`                        string        COMMENT '催收员',
  `customer_name`                             string        COMMENT '客户姓名',
  `contacts_name`                             string        COMMENT '联系人姓名',
  `contacts_relation`                         string        COMMENT '联系人关系',
  `contact_number`                            string        COMMENT '联系电话',
  `contact_result`                            string        COMMENT '联系结果',
  `acquisition_result`                        string        COMMENT '催收结果',
  `result_detail`                             string        COMMENT '结果详情',
  `promise_amount`                            decimal(15,4) COMMENT '承诺金额',
  `promise_date`                              string        COMMENT '承诺日期',
  `update_time`                               string        COMMENT '更新时间（yyyy-MM-ddHH:mm:ss）',
  `create_time`                               string        COMMENT '创建时间（yyyy-MM-ddHH:mm:ss）',
  `snapshot_date`                             string        COMMENT '快照日'
) COMMENT 'dm催收采集表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


--dm催收回收信息表
DROP TABLE IF EXISTS `dm.dm_watch_recovery_detail`;
CREATE TABLE IF NOT EXISTS `dm.dm_watch_recovery_detail`(
  `ecif_no`                                   string        COMMENT 'ecif号',
  `case_sub_id`                               string        COMMENT '子案件编号',
  `partner_code`                              string        COMMENT '合作方渠道编号',
  `product_code`                              string        COMMENT '产品编号',
  `bill_no`                                   string        COMMENT '借据编号',
  `contract_code`                             string        COMMENT '合同编号',
  `loan_term`                                 int           COMMENT '贷款期数',
  `input_coll_date`                           string        COMMENT '入催日期(yyyy-MM-dd)',
  `coll_out_date`                             string        COMMENT '出催日期',
  `input_coll_month`                          string        COMMENT '入催月(yyyy-MM)',
  `input_coll_amount`                         decimal(15,4) COMMENT '入催金额',
  `is_close`                                  string        COMMENT '是否结案',
  `is_contact`                                string        COMMENT '是否可联',
  `last_deal_date`                            string        COMMENT '上次触碰日期(yyyy-MM-dd)',
  `promise_amount`                            decimal(15,4) COMMENT '承诺金额',
  `cash_promise_amount`                       decimal(15,4) COMMENT '承诺兑现金额',
  `snapshot_month`                            string        COMMENT '快照月(yyyy-MM)',
  `snapshot_date`                             string        COMMENT '快照日'
) COMMENT 'dm催收回收信息表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


--dm催收承诺表
DROP TABLE IF EXISTS `dm.dm_watch_commitment_info`;
CREATE TABLE IF NOT EXISTS `dm.dm_watch_commitment_info`(
  `ecif_no`                                   string        COMMENT '客户要素',
  `bill_no`                                   string        COMMENT '借据编号',
  `acquisition_time`                          string        COMMENT '催收时间',
  `promise_amount`                            decimal(15,4) COMMENT '承诺金额',
  `promise_date`                              string        COMMENT '承诺日期',
  `repaid_amount`                             decimal(15,4) COMMENT '还款金额',
  `promise_status`                            string        COMMENT '承诺状态',
  `update_time`                               string        COMMENT '更新时间（yyyy-MM-ddHH:mm:ss）',
  `create_time`                               string        COMMENT '创建时间（yyyy-MM-ddHH:mm:ss）',
  `snapshot_date`                             string        COMMENT '快照日'
) COMMENT 'dm催收承诺表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;


--dm录音记录表
DROP TABLE IF EXISTS `dm.dm_watch_sound_record_info`;
CREATE TABLE IF NOT EXISTS `dm.dm_watch_sound_record_info`(
  `ecif_no`                                   string        COMMENT '客户要素',
  `bill_no`                                   string        COMMENT '借据编号',
  `contacts_name`                             string        COMMENT '联系人姓名',
  `contacts_relation`                         string        COMMENT '联系人关系',
  `outbound_call_number`                      string        COMMENT '外呼号码',
  `send_status`                               string        COMMENT '发送状态',
  `is_answer`                                 string        COMMENT '是否应答',
  `talk_time`                                 string        COMMENT '通话时长',
  `acquisition_group`                         string        COMMENT '催收组',
  `acquisition_person`                        string        COMMENT '催收员',
  `acquisition_record_document_name`          string        COMMENT '催收录音文件名称',
  `acquisition_record_document_path`          string        COMMENT '催收录音文件下载地址',
  `outbound_call_time`                        string        COMMENT '外呼时间',
  `update_time`                               string        COMMENT '更新时间（yyyy-MM-ddHH:mm:ss）',
  `create_time`                               string        COMMENT '创建时间（yyyy-MM-ddHH:mm:ss）',
  `snapshot_date`                             string        COMMENT '快照日'
) COMMENT 'dm录音记录表'
PARTITIONED BY (d_date string COMMENT '分区日期')
STORED AS PARQUET;

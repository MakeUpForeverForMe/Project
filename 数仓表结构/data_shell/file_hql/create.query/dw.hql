set hivevar:warehouse=cosn://bigdatacenter-sit-1253824322/user/hadoop/warehouse;   -- 测试
-- set hivevar:warehouse=cosn://bigdata-center-prod-1253824322/user/hadoop/warehouse; -- 生产

-- DROP DATABASE IF EXISTS `dw`;
CREATE DATABASE IF NOT EXISTS `dw` COMMENT 'dw层数据（代偿前）' location '${warehouse}/dw.db';

-- DROP DATABASE IF EXISTS `dw_cps`;
CREATE DATABASE IF NOT EXISTS `dw_cps` COMMENT 'dw层数据（代偿后）' location '${warehouse}/dw_cps.db';


-- DROP TABLE IF EXISTS `dw.dw_credit_apply_stat_day`;
CREATE TABLE IF NOT EXISTS `dw.dw_credit_apply_stat_day`(
  `credit_apply_date`                     string         COMMENT '授信申请日期',
  `credit_apply_num`                      decimal(20,0)  COMMENT '授信申请笔数',
  `credit_apply_num_person`               decimal(20,0)  COMMENT '授信申请人数',
  `credit_apply_amount`                   decimal(25,5)  COMMENT '授信申请金额',

  `credit_approval_date`                  string         COMMENT '授信通过日期',
  `credit_approval_num`                   decimal(20,0)  COMMENT '授信通过笔数',
  `credit_approval_num_person`            decimal(20,0)  COMMENT '授信通过人数',
  `credit_approval_amount`                decimal(25,5)  COMMENT '授信通过金额'
) COMMENT '轻度授信统计（申请）'
PARTITIONED BY (`biz_date` string  COMMENT '授信申请日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- DROP TABLE IF EXISTS `dw.dw_loan_apply_stat_day`;
CREATE TABLE IF NOT EXISTS `dw.dw_loan_apply_stat_day`(
  `loan_terms`                            decimal(3,0)   COMMENT '贷款期数',

  `loan_apply_date`                       string         COMMENT '用信申请日期',
  `loan_apply_num`                        decimal(20,0)  COMMENT '用信申请笔数',
  `loan_apply_num_person`                 decimal(20,0)  COMMENT '用信申请人数',
  `loan_apply_amount`                     decimal(25,5)  COMMENT '用信申请金额',

  `loan_approval_date`                    string         COMMENT '用信通过日期',
  `loan_approval_num`                     decimal(20,0)  COMMENT '用信通过笔数',
  `loan_approval_num_person`              decimal(20,0)  COMMENT '用信通过人数',
  `loan_approval_amount`                  decimal(25,5)  COMMENT '用信通过金额'
) COMMENT '轻度用信统计（申请）'
PARTITIONED BY (`biz_date` string COMMENT '用信申请日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- DROP TABLE IF EXISTS `dw.dw_credit_approval_stat_day`;
CREATE TABLE IF NOT EXISTS `dw.dw_credit_approval_stat_day`(
  `credit_approval_date`                  string         COMMENT '授信通过日期',
  `credit_approval_num`                   decimal(20,0)  COMMENT '授信通过笔数',
  `credit_approval_num_count`             decimal(20,0)  COMMENT '累计授信通过笔数',
  `credit_approval_num_person`            decimal(20,0)  COMMENT '授信通过人数',
  `credit_approval_num_person_count`      decimal(20,0)  COMMENT '累计授信通过人数',
  `credit_approval_amount`                decimal(25,5)  COMMENT '授信通过金额',
  `credit_approval_amount_count`          decimal(25,5)  COMMENT '累计授信通过金额',

  `loan_approval_date`                    string         COMMENT '用信通过日期',
  `credit_loan_approval_num`              decimal(20,0)  COMMENT '用信通过笔数',
  `credit_loan_approval_person`           decimal(20,0)  COMMENT '用信通过人数',
  `credit_loan_approval_num_amount`       decimal(25,5)  COMMENT '用信通过金额'
) COMMENT '轻度授信统计（通过）'
PARTITIONED BY (`biz_date` string  COMMENT '授信通过日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- DROP TABLE IF EXISTS `dw.dw_loan_approval_stat_day`;
CREATE TABLE IF NOT EXISTS `dw.dw_loan_apply_stat_day`(
  `loan_terms`                            decimal(3,0)   COMMENT '贷款期数',

  `loan_apply_date`                       string         COMMENT '用信申请日期',
  `loan_apply_num`                        decimal(20,0)  COMMENT '用信申请笔数',
  `loan_apply_num_count`                  decimal(20,0)  COMMENT '累计申请笔数',
  `loan_apply_num_person`                 decimal(20,0)  COMMENT '用信申请人数',
  `loan_apply_num_person_count`           decimal(20,0)  COMMENT '累计用信申请人数',
  `loan_apply_amount`                     decimal(25,5)  COMMENT '用信申请金额',
  `loan_apply_amount_count`               decimal(25,5)  COMMENT '累计用信申请金额',
  `loan_approval_date`                    string         COMMENT '用信通过日期',
  `loan_approval_num`                     decimal(20,0)  COMMENT '用信通过笔数',
  `loan_approval_num_person`              decimal(20,0)  COMMENT '用信通过人数',
  `loan_approval_amount`                  decimal(25,5)  COMMENT '用信通过金额'
) COMMENT '轻度用信统计（申请）'
PARTITIONED BY (`biz_date` string COMMENT '用信申请日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;



-- DROP TABLE IF EXISTS `dw.dw_credit_ret_msg_day`;
CREATE TABLE IF NOT EXISTS `dw.dw_credit_ret_msg_day`(
  `ret_msg`                               string         COMMENT '拒绝原因',
  `ret_msg_num`                           decimal(20,0)  COMMENT '拒绝原因笔数',
  `ret_msg_rate`                          decimal(30,10) COMMENT '拒绝原因占比'
) COMMENT '授信拒绝原因分布'
PARTITIONED by (`biz_date` string COMMENT '风控处理日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- DROP TABLE IF EXISTS `dw.dw_loan_ret_msg_day`;
CREATE TABLE IF NOT EXISTS `dw.dw_loan_ret_msg_day`(
  `loan_terms`                            decimal(3,0)   COMMENT '贷款期数',

  `ret_msg`                               string         COMMENT '拒绝原因',
  `ret_msg_num`                           decimal(20,0)  COMMENT '拒绝原因笔数',
  `ret_msg_rate`                          decimal(30,10) COMMENT '拒绝原因占比'
) COMMENT '用信拒绝原因分布'
PARTITIONED by (`biz_date` string COMMENT '风控处理日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;







set hivevar:db_suffix=;

set hivevar:db_suffix=_cps;



-- DROP TABLE IF EXISTS `dw${db_suffix}.dw_loan_base_stat_loan_num_day`;
CREATE TABLE IF NOT EXISTS `dw${db_suffix}.dw_loan_base_stat_loan_num_day`(
  `loan_terms`                            decimal(3,0)   COMMENT '贷款期数',

  `loan_num`                              decimal(20,0)  COMMENT '放款借据数',
  `loan_num_count`                        decimal(20,0)  COMMENT '累计放款借据数',
  `loan_principal`                        decimal(25,5)  COMMENT '放款本金',
  `loan_principal_count`                  decimal(25,5)  COMMENT '累计放款本金'
) COMMENT '放款统计 - 日级'
PARTITIONED BY (`biz_date` string COMMENT '放款日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- DROP TABLE IF EXISTS `dw${db_suffix}.dw_loan_base_stat_overdue_num_day`;
CREATE TABLE IF NOT EXISTS `dw${db_suffix}.dw_loan_base_stat_overdue_num_day`(
  `loan_terms`                            decimal(3,0)   COMMENT '贷款期数',

  `loan_active_date`                      string         COMMENT '放款日期',
  `overdue_mob`                           decimal(2,0)   COMMENT '进展月',

  `is_first_term_overdue`                 string         COMMENT '是否首期逾期（0：否，1：是）',
  `overdue_dob`                           decimal(5,0)   COMMENT '进展日（DOB1————＞DOB30，DOB1为应还日。取数字1—>30）',

  `should_repay_date_curr`                string         COMMENT '当期应还日期',
  `should_repay_date_history_curr`        string         COMMENT '当期应还日期（历史）',
  `should_repay_principal_curr`           decimal(25,5)  COMMENT '计划当期应还日应还本金',
  `should_repay_loan_num_curr`            decimal(20,0)  COMMENT '计划当期应还日应还借据数',
  `should_repay_paid_principal_curr`      decimal(20,0)  COMMENT '当期应还日已还本金',
  `should_repay_principal_curr_actual`    decimal(25,5)  COMMENT '实际当期应还日应还本金',
  `should_repay_loan_num_curr_actual`     decimal(20,0)  COMMENT '实际当期应还日应还借据数',

  `should_repay_date`                     string         COMMENT '上期应还日期',
  `should_repay_date_history`             string         COMMENT '上期应还日期（历史）',
  `should_repay_principal`                decimal(25,5)  COMMENT '计划上期应还日应还本金',
  `should_repay_loan_num`                 decimal(20,0)  COMMENT '计划上期应还日应还借据数',

  `overdue_date_first`                    string         COMMENT '首次逾期日期',
  `overdue_date_start`                    string         COMMENT '逾期起始日期',
  `is_first_overdue_day`                  decimal(2,0)   COMMENT '此逾期天数是否是首次逾期到这个天数（0：否，1：是）',
  `overdue_days`                          decimal(5,0)   COMMENT '逾期天数',
  `overdue_stage_previous`                decimal(5,0)   COMMENT '上期逾期阶段（按期数，M1—>M6+）',
  `overdue_stage_curr`                    decimal(5,0)   COMMENT '当前逾期阶段（按期数，M1—>M6+）',
  `overdue_stage`                         decimal(2,0)   COMMENT '当前逾期阶段（按30天，C、M1—>M6、M6+，取数字格式：0—>7）',
  `unposted_principal`                    decimal(25,5)  COMMENT '未出账本金',
  `remain_principal`                      decimal(25,5)  COMMENT '本金余额',
  `paid_principal`                        decimal(25,5)  COMMENT '已还本金',
  `overdue_principal`                     decimal(25,5)  COMMENT '逾期本金',
  `overdue_loan_num`                      decimal(20,0)  COMMENT '逾期借据数',
  `overdue_remain_principal`              decimal(25,5)  COMMENT '逾期借据本金余额'
) COMMENT '逾期统计 - 日级'
PARTITIONED BY (`biz_date` string COMMENT '观察日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- DROP TABLE IF EXISTS `dw${db_suffix}.dw_loan_base_stat_should_repay_day`;
CREATE TABLE IF NOT EXISTS `dw${db_suffix}.dw_loan_base_stat_should_repay_day`(
  `loan_terms`                            decimal(3,0)   COMMENT '贷款期数',

  `loan_active_date`                      string         COMMENT '放款日期',
  `should_repay_date`                     string         COMMENT '应还日期',
  `loan_principal`                        decimal(25,5)  COMMENT '放款本金',

  `should_repay_amount`                   decimal(25,5)  COMMENT '当期计划应还金额（应还本金+应还息费）',
  `should_repay_principal`                decimal(25,5)  COMMENT '当期计划应还本金',
  `should_repay_interest_fee`             decimal(25,5)  COMMENT '当期计划应还息费罚（应还利息+应还费用+应还罚息）',
  `should_repay_interest`                 decimal(25,5)  COMMENT '当期计划应还利息',
  `should_repay_svc_term`                 decimal(25,5)  COMMENT '当期计划应还费用（应还手续费+应还服务费）',
  `should_repay_term_fee`                 decimal(25,5)  COMMENT '当期计划应还手续费',
  `should_repay_svc_fee`                  decimal(25,5)  COMMENT '当期计划应还服务费',
  `should_repay_penalty`                  decimal(25,5)  COMMENT '当期计划应还罚息',

  `should_repay_paid_amount`              decimal(25,5)  COMMENT 'T-1当期已还金额（应还本金+应还息费）',
  `should_repay_paid_principal`           decimal(25,5)  COMMENT 'T-1当期已还本金',
  `should_repay_paid_interest_fee`        decimal(25,5)  COMMENT 'T-1当期已还息费罚（应还利息+应还费用+应还罚息）',
  `should_repay_paid_interest`            decimal(25,5)  COMMENT 'T-1当期已还利息',
  `should_repay_paid_svc_term`            decimal(25,5)  COMMENT 'T-1当期已还费用（应还手续费+应还服务费）',
  `should_repay_paid_term_fee`            decimal(25,5)  COMMENT 'T-1当期已还手续费',
  `should_repay_paid_svc_fee`             decimal(25,5)  COMMENT 'T-1当期已还服务费',
  `should_repay_paid_penalty`             decimal(25,5)  COMMENT 'T-1当期已还罚息',
  `should_repay_paid_mult`                decimal(25,5)  COMMENT 'T-1当期已还滞纳金',

  `overdue_should_repay_amount`           decimal(25,5)  COMMENT 'T-1逾期应还金额（应还本金+应还息费）',
  `overdue_should_repay_interest`         decimal(25,5)  COMMENT 'T-1逾期应还利息（应还息费）',
  `overdue_should_repay_principal`        decimal(25,5)  COMMENT 'T-1逾期应还本金（应还本金）',
  `overdue_should_repay_interest_fee`     decimal(25,5)  COMMENT 'T-1逾期已还息费罚（应还利息+应还费用+应还罚息）',
  `overdue_should_repay_svc_term`         decimal(25,5)  COMMENT 'T-1逾期应还费用（应还手续费+应还服务费）',
  `overdue_should_repay_term_fee`         decimal(25,5)  COMMENT 'T-1逾期应还手续费（手续费）',
  `overdue_should_repay_svc_fee`          decimal(25,5)  COMMENT 'T-1逾期应还服务费（服务费）',
  `overdue_should_repay_penalty`          decimal(25,5)  COMMENT 'T-1逾期应还罚息（罚息）',

  `should_repay_amount_actual`            decimal(25,5)  COMMENT '实际应还金额（当期应还金额+T日应还罚息+T-1逾期金额-当期T-1已还金额-T-1日已还罚息息费）',
  `should_repay_principal_actual`         decimal(25,5)  COMMENT '实际应还本金（当期应还本金+T-1逾期罚息-当期T-1已还本金）',
  `should_repay_interest_fee_actual`      decimal(25,5)  COMMENT '实际应还息费罚（当期应还息费+T日应还罚息+T-1逾期息费罚-当期T-1已还息费- T-1日已还罚息息费）',
  `should_repay_interest_actual`          decimal(25,5)  COMMENT '实际应还利息（当期应还利息+T-1逾期利息-当期T-1已还利息）',
  `should_repay_svc_term_actual`          decimal(25,5)  COMMENT '实际应还费用（当期应还手续费+应还服务费）',
  `should_repay_term_fee_actual`          decimal(25,5)  COMMENT '实际应还手续费',
  `should_repay_svc_fee_actual`           decimal(25,5)  COMMENT '实际应还服务费',
  `should_repay_penalty_actual`           decimal(25,5)  COMMENT '实际应还罚息'
) COMMENT '应还统计 - 日级'
PARTITIONED BY (`biz_date` string COMMENT '观察日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- DROP TABLE IF EXISTS `dw${db_suffix}.dw_loan_base_stat_repay_detail_day`;
CREATE TABLE IF NOT EXISTS `dw${db_suffix}.dw_loan_base_stat_repay_detail_day`(
  `loan_terms`                            decimal(3,0)  COMMENT '贷款期数',
  `loan_active_date`                      string        COMMENT '放款日',

  `repaid_num`                            decimal(11,0) COMMENT '实还借据数',
  `repaid_num_count`                      decimal(11,0) COMMENT '累计实还借据数',

  `repaid_amount`                         decimal(15,4) COMMENT '实还金额（实还本金+实还息费）',
  `repaid_principal`                      decimal(15,4) COMMENT '实还本金',
  `repaid_interest_penalty_svc_fee`       decimal(15,4) COMMENT '实还息费（实还利息+实还费用+实还罚金）',
  `repaid_interest`                       decimal(15,4) COMMENT '实还利息',
  `repaid_repay_svc_term`                 decimal(15,4) COMMENT '实还费用（实还手续费+实还服务费）',
  `repaid_repay_term_fee`                 decimal(15,4) COMMENT '实还手续费',
  `repaid_repay_svc_fee`                  decimal(15,4) COMMENT '实还服务费',
  `repaid_penalty`                        decimal(15,4) COMMENT '实还罚金',
  `repaid_amount_count`                   decimal(15,4) COMMENT '累计回款金额（累计回款本金+累计回款息费）',
  `repaid_principal_count`                decimal(15,4) COMMENT '累计回款本金',
  `repaid_interest_penalty_svc_fee_count` decimal(15,4) COMMENT '累计回款息费（累计回款利息+累计回款费用+累计回款罚金）',
  `repaid_interest_count`                 decimal(15,4) COMMENT '累计回款利息',
  `repaid_repay_svc_term_count`           decimal(15,4) COMMENT '累计回款费用（累计回款手续费+累计回款服务费）',
  `repaid_repay_term_fee_count`           decimal(15,4) COMMENT '累计回款手续费',
  `repaid_repay_svc_fee_count`            decimal(15,4) COMMENT '累计回款服务费',
  `repaid_penalty_count`                  decimal(15,4) COMMENT '累计回款罚金',

  `settle_num`                           decimal(15,0) COMMENT '当日结清借据数',
  `settle_count`                         decimal(15,0) COMMENT '累计结清借据数',
  `settle_loan_days`                     decimal(15,0) COMMENT '当天结清借据总用信天数(从放款到结清天数)',
  `settle_loan_days_count`               decimal(15,0) COMMENT '累计用信天数(从放款到结清天数)',
  `prepay_principal`                     decimal(15,4) COMMENT '当日提前还款金额'
) COMMENT '实还统计 - 日级'
PARTITIONED BY (`biz_date` string COMMENT '实还日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;














-- 3.0 的时候是这张表  4.0 表移动到了 dim
-- DROP TABLE IF EXISTS `dw.dw_static_overdue_bill`;
CREATE TABLE IF NOT EXISTS `dw.dw_static_overdue_bill`(
  `product_id`                            string         COMMENT '产品编号',
  `due_bill_no`                           string         COMMENT '借据编号',
  `loan_term`                             decimal(3,0)   COMMENT '当前期数',
  `loan_active_date`                      string         COMMENT '放款日期',
  `loan_init_principal`                   decimal(25,5)  COMMENT '贷款本金',
  `loan_init_term`                        decimal(3,0)   COMMENT '贷款期数（3、6、9等）',
  `remain_term`                           decimal(3,0)   COMMENT '截止当前剩余未还期数，不做更新',
  `remain_principal`                      decimal(25,5)  COMMENT '截止当前本金余额，不做更新',
  `remain_interest`                       decimal(25,5)  COMMENT '截止当前剩余应还利息，不做更新',
  `paid_term`                             decimal(3,0)   COMMENT '截止当前已还期数不做更新',
  `paid_principal`                        decimal(25,5)  COMMENT '截止当前已还本金，不做更新',
  `paid_interest`                         decimal(25,5)  COMMENT '截止当前已还利息，不做更新',
  `overdue_date_start`                    string         COMMENT '逾期起始日期，不更新',
  `overdue_days`                          decimal(5,0)   COMMENT '逾期天数，不更新，只取：31、61、91',
  `loan_status`                           string         COMMENT '借据状态，不更新（N：正常，O：逾期，F：已还清）'
) COMMENT '借据静态逾期表，只记录第一次逾期31天、61天、91天的数据，只统计代偿前'
PARTITIONED BY (`biz_date` string COMMENT '观察日期',`prj_type` string COMMENT '项目类别：DD、HT、LXGM、LXZT')
STORED AS PARQUET;















-- DROP TABLE IF EXISTS `dw.dw_transaction_blend_record`;
CREATE TABLE IF NOT EXISTS `dw.dw_transaction_blend_record`(
  `blend_serial_no`                       string         COMMENT '流水勾兑编号',
  `record_type`                           string         COMMENT '记录类型  D:借方 C:贷方 M:手工调整值 F:结果值 G:看管值',
  `loan_amt`                              decimal(18,2)  COMMENT '放款金额',
  `cust_repay_amt`                        decimal(18,2)  COMMENT '客户还款',
  `comp_bak_amt`                          decimal(18,2)  COMMENT '代偿回款',
  `buy_bak_amt`                           decimal(18,2)  COMMENT '回购回款',
  `deduct_sve_fee`                        decimal(18,2)  COMMENT '划扣手续费',
  `invest_amt`                            decimal(18,2)  COMMENT '投资金额',
  `invest_redeem_amt`                     decimal(18,2)  COMMENT '投资赎回金额',
  `invest_earning`                        decimal(18,2)  COMMENT '投资收益',
  `acct_int`                              decimal(18,2)  COMMENT '账户利息',
  `acct_fee`                              decimal(18,2)  COMMENT '账户费用',
  `tax_amt`                               decimal(18,2)  COMMENT '税费支付',
  `invest_cash`                           decimal(18,2)  COMMENT '投资兑付',
  `ci_fund`                               decimal(18,2)  COMMENT '信保基金',
  `ci_redeem_amt`                         decimal(18,2)  COMMENT '信保赎回',
  `ci_earning`                            decimal(18,2)  COMMENT '信保收益',
  `other_amt`                             decimal(18,2)  COMMENT '其他金额',
  `trade_day_bal`                         decimal(18,2)  COMMENT 'T日余额',
  `trade_yesterday_bal`                   decimal(18,2)  COMMENT 'T-1日余额',
  `trade_day__bal_diff`                   decimal(18,2)  COMMENT 'T日余额差异',
  `remark`                                string         COMMENT '备注',
  `create_date`                           bigint         COMMENT '创建时间',
  `update_date`                           bigint         COMMENT '创建时间',
  `calc_date`                             string         COMMENT '勾兑记录日期',
  `product_code`                          string         COMMENT '信托产品编号',
  `product_name`                          string         COMMENT '信托产品名称',
  `return_ticket_bak_amt`                 decimal(18,2)  COMMENT '退票回款',
  `ch_diff_explain`                       string         COMMENT '',
  `en_diff_explain`                       string         COMMENT ''
) COMMENT '资金打标流水表'
PARTITIONED BY (`biz_date` string COMMENT '计算日期')
STORED AS PARQUET;


-- DROP TABLE IF EXISTS `dw.dw_asset_info_day`;
CREATE TABLE IF NOT EXISTS `dw.dw_asset_info_day`(
  `remain_principal`                      decimal(15,2)  COMMENT '日终资产余额',
  `loan_sum_daily`                        decimal(15,2)  COMMENT '放款金额',
  `repay_sum_daily`                       decimal(15,2)  COMMENT '还款总金额',
  `repay_principal`                       decimal(15,2)  COMMENT '还款本金',
  `repay_interest`                        decimal(15,2)  COMMENT '还款利息',
  `repay_penalty`                         decimal(15,2)  COMMENT '还款罚息',
  `repay_fee`                             decimal(15,2)  COMMENT '还款费用',
  `cust_repay_sum`                        decimal(15,2)  COMMENT '客户还款总本金',
  `cust_repay_principal`                  decimal(15,2)  COMMENT '客户还款本金',
  `cust_repay_interest`                   decimal(15,2)  COMMENT '客户还款利息',
  `cust_repay_penalty`                    decimal(15,2)  COMMENT '客户还款罚息',
  `cust_repay_fee`                        decimal(15,2)  COMMENT '客户还款费用',
  `back_repay_sum`                        decimal(15,2)  COMMENT '催回回款还款总金额',
  `back_repay_principal`                  decimal(15,2)  COMMENT '催回回款还款本金',
  `back_repay_interest`                   decimal(15,2)  COMMENT '催回回款还款利息',
  `back_repay_penalty`                    decimal(15,2)  COMMENT '催回回款还款罚息',
  `back_repay_fee`                        decimal(15,2)  COMMENT '催回回款还款费用'
) COMMENT '资产每日统计-代偿前'
PARTITIONED BY (`biz_date` string COMMENT '观察日期',`project_id` string COMMENT '项目编号')
STORED AS PARQUET;


-- DROP TABLE IF EXISTS `dw_cps.dw_asset_info_day`;
CREATE TABLE IF NOT EXISTS `dw_cps.dw_asset_info_day`(
  `remain_principal`                      decimal(15,2)  COMMENT '日终资产余额',
  `loan_sum_daily`                        decimal(15,2)  COMMENT '放款金额',
  `repay_sum_daily`                       decimal(15,2)  COMMENT '还款总金额',
  `repay_principal`                       decimal(15,2)  COMMENT '还款本金',
  `repay_interest`                        decimal(15,2)  COMMENT '还款利息',
  `repay_penalty`                         decimal(15,2)  COMMENT '还款罚息',
  `repay_fee`                             decimal(15,2)  COMMENT '还款费用',
  `cust_repay_sum`                        decimal(15,2)  COMMENT '客户还款总本金',
  `cust_repay_principal`                  decimal(15,2)  COMMENT '客户还款本金',
  `cust_repay_interest`                   decimal(15,2)  COMMENT '客户还款利息',
  `cust_repay_penalty`                    decimal(15,2)  COMMENT '客户还款罚息',
  `cust_repay_fee`                        decimal(15,2)  COMMENT '客户还款费用',
  `comp_repay_sum`                        decimal(15,2)  COMMENT '代偿回款还款总金额',
  `comp_repay_principal`                  decimal(15,2)  COMMENT '代偿回款还款本金',
  `comp_repay_interest`                   decimal(15,2)  COMMENT '代偿回款还款利息',
  `comp_repay_penalty`                    decimal(15,2)  COMMENT '代偿回款还款罚息',
  `comp_repay_fee`                        decimal(15,2)  COMMENT '代偿回款还款费用',
  `repo_repay_sum`                        decimal(15,2)  COMMENT '回购还款总金额',
  `repo_repay_principal`                  decimal(15,2)  COMMENT '回购还款本金',
  `repo_repay_interest`                   decimal(15,2)  COMMENT '回购还款利息',
  `repo_repay_penalty`                    decimal(15,2)  COMMENT '回购还款罚息',
  `repo_repay_fee`                        decimal(15,2)  COMMENT '回购还款费用',
  `return_repay_sum`                      decimal(15,2)  COMMENT '银行退票总金额',
  `return_repay_principal`                decimal(15,2)  COMMENT '银行退票本金',
  `return_repay_interest`                 decimal(15,2)  COMMENT '银行退票利息',
  `return_repay_penalty`                  decimal(15,2)  COMMENT '银行退票罚息',
  `return_repay_fee`                      decimal(15,2)  COMMENT '银行退票费用'
) COMMENT '资产每日统计-代偿后'
PARTITIONED BY (`biz_date` string COMMENT '观察日期',`project_id` string COMMENT '项目编号')
STORED AS PARQUET;


--- 乐信资产服务报告
-- DROP TABLE IF EXISTS `dw.dw_report_cal_day`;
CREATE TABLE IF NOT EXISTS `dw.dw_report_cal_day`(
  `project_id`                            string         COMMENT '项目id',
  `m2plus_recover_amount`                 decimal(25,5)  COMMENT '当前违约30天+回收金额',
  `m2plus_recover_prin`                   decimal(25,5)  COMMENT '当前违约30天+回收本金',
  `m2plus_recover_inter`                  decimal(25,5)  COMMENT '当前违约30天+回收息费',
  `m4plus_recover_amount`                 decimal(25,5)  COMMENT '当前违约90天+回收金额',
  `m4plus_recover_prin`                   decimal(25,5)  COMMENT '当前违约90天+回收本金',
  `m4plus_recover_inter`                  decimal(25,5)  COMMENT '当前违约90天+回收息费',
  `m2plus_recover_amount_acc`             decimal(25,5)  COMMENT '累计违约30天+回收金额',
  `m2plus_recover_prin_acc`               decimal(25,5)  COMMENT '累计违约30天+回收本金',
  `m2plus_recover_inter_acc`              decimal(25,5)  COMMENT '累计违约30天+回收息费',
  `m4plus_recover_amount_acc`             decimal(25,5)  COMMENT '累计违约90天+回收金额',
  `m4plus_recover_prin_acc`               decimal(25,5)  COMMENT '累计违约90天+回收本金',
  `m4plus_recover_inter_acc`              decimal(25,5)  COMMENT '累计违约90天+回收息费',
  `m2plus_num`                            int            COMMENT '新增违约30天+笔数',
  `m2plus_amount`                         decimal(25,5)  COMMENT '新增违约30天+金额',
  `m2plus_prin`                           decimal(25,5)  COMMENT '新增违约30天+本金',
  `m2plus_inter`                          decimal(25,5)  COMMENT '新增违约30天+利息',
  `m4plus_num`                            int            COMMENT '新增新增违约90天+笔数',
  `m4plus_amount`                         decimal(25,5)  COMMENT '新增违约90天+金额',
  `m4plus_prin`                           decimal(25,5)  COMMENT '新增违约90天+本金',
  `m4plus_inter`                          decimal(25,5)  COMMENT '新增违约90天+利息',
  `static_30_new_amount`                  decimal(25,5)  COMMENT '新增30+逾期资产金额',
  `static_30_new_prin`                    decimal(25,5)  COMMENT '新增30+逾期资产本金',
  `static_30_new_inter`                   decimal(25,5)  COMMENT '新增30+逾期资产利息',
  `static_90_new_amount`                  decimal(25,5)  COMMENT '新增90+逾期资产金额',
  `static_90_new_prin`                    decimal(25,5)  COMMENT '新增90+逾期资产本金',
  `static_90_new_inter`                   decimal(25,5)  COMMENT '新增90+逾期资产利息',
  `static_30_acc_amount`                  decimal(25,5)  COMMENT '累计30+逾期资产金额',
  `static_acc_30_prin`                    decimal(25,5)  COMMENT '累计30+逾期资产本金',
  `static_acc_30_iner`                    decimal(25,5)  COMMENT '累计30+逾期资产利息',
  `static_acc_90_amount`                  decimal(25,5)  COMMENT '累计90+逾期资产金额',
  `static_acc_90_prin`                    decimal(25,5)  COMMENT '累计90+逾期资产本金',
  `static_acc_90_iner`                    decimal(25,5)  COMMENT '累计90+逾期资产利息',
  `overdue_remain_principal`              decimal(25,5)  COMMENT '逾期1+剩余本金',
  `acc_buy_back_amount`                   decimal(25,5)  COMMENT '累计180+回购金额'
) COMMENT '服务报告乐信国民、乐信中铁、乐信国民二期'
PARTITIONED BY (`biz_date` string COMMENT '快照日')
STORED AS PARQUET;





CREATE EXTERNAL TABLE `dw.dw_transaction_blend_record`(
  `blend_serial_no` string COMMENT '流水勾兑编号',
  `record_type` string COMMENT '记录类型  D:借方 C:贷方 M:手工调整值 F:结果值 G:看管值',
  `loan_amt` decimal(18,2) COMMENT '放款金额',
  `cust_repay_amt` decimal(18,2) COMMENT '客户还款',
  `comp_bak_amt` decimal(18,2) COMMENT '代偿回款',
  `buy_bak_amt` decimal(18,2) COMMENT '回购回款',
  `deduct_sve_fee` decimal(18,2) COMMENT '划扣手续费',
  `invest_amt` decimal(18,2) COMMENT '投资金额',
  `invest_redeem_amt` decimal(18,2) COMMENT '投资赎回金额',
  `invest_earning` decimal(18,2) COMMENT '投资收益',
  `acct_int` decimal(18,2) COMMENT '账户利息',
  `acct_fee` decimal(18,2) COMMENT '账户费用',
  `tax_amt` decimal(18,2) COMMENT '税费支付',
  `invest_cash` decimal(18,2) COMMENT '投资兑付',
  `ci_fund` decimal(18,2) COMMENT '信保基金',
  `ci_redeem_amt` decimal(18,2) COMMENT '信保赎回',
  `ci_earning` decimal(18,2) COMMENT '信保收益',
  `other_amt` decimal(18,2) COMMENT '其他金额',
  `trade_day_bal` decimal(18,2) COMMENT 'T日余额',
  `trade_yesterday_bal` decimal(18,2) COMMENT 'T-1日余额',
  `trade_day__bal_diff` decimal(18,2) COMMENT 'T日余额差异',
  `remark` string COMMENT '备注',
  `create_date` bigint COMMENT '创建时间',
  `update_date` bigint COMMENT '创建时间',
  `calc_date` string COMMENT '勾兑记录日期',
  `product_code` string COMMENT '信托产品编号',
  `product_name` string COMMENT '信托产品名称',
  `return_ticket_bak_amt` decimal(18,2) COMMENT '退票回款',
  `ch_diff_explain` string COMMENT '',
  `en_diff_explain` string COMMENT '')
COMMENT '资金打标流水表'
PARTITIONED BY (`biz_date` string COMMENT '计算日期')
STORED AS PARQUET
LOCATION 'cosn://bigdatacenter-sit-1253824322/user/hadoop/warehouse/dw.db/dw_transaction_blend_record';













-- 星云宽表（借据级）
-- DROP TABLE IF EXISTS `dw.abs_due_info_day`;
CREATE TABLE IF NOT EXISTS `dw.abs_due_info_day`(
  -- 借据级
  `due_bill_no`                  string         COMMENT '借据编号',
  `loan_init_term`               decimal(3,0)   COMMENT '贷款期数（3、6、9等）',
  `loan_init_principal`          decimal(20,5)  COMMENT '贷款本金',
  `account_age`                  decimal(3,0)   COMMENT '账龄（取当前期数）',
  `loan_term_remain`             decimal(3,0)   COMMENT '已还期数',
  `loan_term_repaid`             decimal(3,0)   COMMENT '剩余期数',
  `remain_principal`             decimal(20,5)  COMMENT '剩余本金',
  `remain_interest`              decimal(20,5)  COMMENT '剩余利息',
  `remain_principal_yesterday`   decimal(20,5)  COMMENT '昨日剩余本金',
  `loan_status`                  string         COMMENT '借据状态（英文原值）（N：正常，O：逾期，F：已还清）',
  `paid_out_date`                string         COMMENT '还清日期',
  `paid_out_type`                string         COMMENT '结清类型',
  `overdue_due_bill_no`          string         COMMENT '逾期借据借据编号',
  `overdue_user_hash_no`         string         COMMENT '逾期借据用户编号',
  `overdue_date_start`           string         COMMENT '逾期起始日期',
  `overdue_days`                 decimal(5,0)   COMMENT '逾期天数',
  `dpd_days_max`                 decimal(4,0)   COMMENT '历史最大DPD天数',
  `overdue_days_dpd`             array<string>  COMMENT 'DPD时期（1+、7+、14+、30+、60+、90+、120+、150+、180+、1_7、8_15、15_30、31_60、61_90、91_120、121_150、151_180）',
  `dpd_map`                  map<string,string> COMMENT '曾有逾期的借据逾期开始日期及剩余本金汇总（格式：{"2021-01-01":1200}：{"开始逾期日期":逾期借据剩余本金}）',
  `overdue_principal`            decimal(20,5)  COMMENT '逾期本金',
  `overdue_interest`             decimal(20,5)  COMMENT '逾期利息',
  `overdue_svc_fee`              decimal(20,5)  COMMENT '逾期服务费',
  `overdue_term_fee`             decimal(20,5)  COMMENT '逾期手续费',
  `overdue_penalty`              decimal(20,5)  COMMENT '逾期罚息',
  `overdue_mult_amt`             decimal(20,5)  COMMENT '逾期滞纳金',
  `overdue_remain_principal`     decimal(20,5)  COMMENT '逾期借据剩余本金',

  -- 合同级
  `contract_no`                  string         COMMENT '借据编号',
  `loan_init_interest_rate`      decimal(25,10) COMMENT '利息利率',
  `loan_type_cn`                 string         COMMENT '分期类型（汉语解释）',
  `contract_term`                decimal(3,0)   COMMENT '合同期限（按照月份计算）',
  `mortgage_rate`                decimal(20,5)  COMMENT '抵押率',
  `shoufu_amount`                decimal(20,5)  COMMENT '首付款金额（元）',

  -- 客户级
  `user_hash_no`                 string         COMMENT '用户编号',
  `age`                          decimal(3,0)   COMMENT '客户年龄',
  `job_type`                     decimal(5,0)   COMMENT '工作年限',
  `income_year`                  decimal(25,5)  COMMENT '年收入',
  `income_year_max`              decimal(25,5)  COMMENT '年收入上限',
  `income_year_min`              decimal(25,5)  COMMENT '年收入下限',
  `idcard_area`                  string         COMMENT '身份证大区（东北地区、华北地区、西北地区、西南地区、华南地区、华东地区、华中地区、港澳台地区）',

  -- 抵押物级
  `due_bill_no_guaranty`         string         COMMENT '抵押物借据编号',
  `pawn_value`                   decimal(20,6)  COMMENT '评估价格(元)',
  `guarantee_type`               string         COMMENT '担保方式 预定义字段：质押担保，信用担保，保证担保，抵押担保',

  -- 分布
  `distribution_array`           array<string>  COMMENT '分布数组'
) COMMENT '星云宽表（借据级）'
PARTITIONED BY (`biz_date` string COMMENT '观察日期',`project_id` string COMMENT '项目编号')
STORED AS PARQUET;




-- DROP VIEW IF EXISTS `dw.abs_due_info_day_abs`;
CREATE VIEW IF NOT EXISTS `dw.abs_due_info_day_abs`(
  -- 借据级
  `due_bill_no`                COMMENT '借据编号',
  `loan_init_term`             COMMENT '贷款期数（3、6、9等）',
  `loan_init_principal`        COMMENT '贷款本金',
  `account_age`                COMMENT '账龄（取当前期数）',
  `loan_term_remain`           COMMENT '已还期数',
  `loan_term_repaid`           COMMENT '剩余期数',
  `remain_principal`           COMMENT '剩余本金',
  `remain_interest`            COMMENT '剩余利息',
  `remain_principal_yesterday` COMMENT '昨日剩余本金',
  `loan_status`                COMMENT '借据状态（英文原值）（N：正常，O：逾期，F：已还清）',
  `paid_out_date`              COMMENT '还清日期',
  `paid_out_type`              COMMENT '结清类型',
  `overdue_due_bill_no`        COMMENT '逾期借据借据编号',
  `overdue_user_hash_no`       COMMENT '逾期借据用户编号',
  `overdue_date_start`         COMMENT '逾期起始日期',
  `overdue_days`               COMMENT '逾期天数',
  `dpd_days_max`               COMMENT '历史最大DPD天数',
  `overdue_days_dpd`           COMMENT 'DPD时期（1+、7+、14+、30+、60+、90+、120+、150+、180+、1_7、8_15、15_30、31_60、61_90、91_120、121_150、151_180）',
  `dpd_map`                    COMMENT '曾有逾期的借据逾期开始日期及剩余本金汇总（格式：{"2021-01-01":1200}：{"开始逾期日期":逾期借据剩余本金}）',
  `overdue_principal`          COMMENT '逾期本金',
  `overdue_interest`           COMMENT '逾期利息',
  `overdue_svc_fee`            COMMENT '逾期服务费',
  `overdue_term_fee`           COMMENT '逾期手续费',
  `overdue_penalty`            COMMENT '逾期罚息',
  `overdue_mult_amt`           COMMENT '逾期滞纳金',
  `overdue_remain_principal`   COMMENT '逾期借据剩余本金',

  -- 合同级
  `contract_no`                COMMENT '借据编号',
  `loan_init_interest_rate`    COMMENT '利息利率',
  `loan_type_cn`               COMMENT '分期类型（汉语解释）',
  `contract_term`              COMMENT '合同期限（按照月份计算）',
  `mortgage_rate`              COMMENT '抵押率',
  `shoufu_amount`              COMMENT '首付款金额（元）',

  -- 客户级
  `user_hash_no`               COMMENT '用户编号',
  `age`                        COMMENT '客户年龄',
  `job_type`                   COMMENT '工作年限',
  `income_year`                COMMENT '年收入',
  `income_year_max`            COMMENT '年收入上限',
  `income_year_min`            COMMENT '年收入下限',
  `idcard_area`                COMMENT '身份证大区（东北地区、华北地区、西北地区、西南地区、华南地区、华东地区、华中地区、港澳台地区）',

  -- 抵押物级
  `due_bill_no_guaranty`       COMMENT '抵押物借据编号',
  `pawn_value`                 COMMENT '评估价格(元)',
  `guarantee_type`             COMMENT '担保方式 预定义字段：质押担保，信用担保，保证担保，抵押担保',

  -- 分布
  `distribution_array`         COMMENT '分布数组',

  -- 分区
  `biz_date`                   COMMENT '观察日期',
  `project_id`                 COMMENT '项目编号'
) COMMENT '星云宽表（借据级）' AS select
  -- 借据级
  t1.due_bill_no,
  t1.loan_init_term,
  t1.loan_init_principal,
  t1.account_age,
  t1.loan_term_remain,
  t1.loan_term_repaid,
  t1.remain_principal,
  t1.remain_interest,
  t1.remain_principal_yesterday,
  t1.loan_status,
  t1.paid_out_date,
  t1.paid_out_type,
  t1.overdue_due_bill_no,
  t1.overdue_user_hash_no,
  t1.overdue_date_start,
  t1.overdue_days,
  t1.dpd_days_max,
  t1.overdue_days_dpd,
  t1.dpd_map,
  t1.overdue_principal,
  t1.overdue_interest,
  t1.overdue_svc_fee,
  t1.overdue_term_fee,
  t1.overdue_penalty,
  t1.overdue_mult_amt,
  t1.overdue_remain_principal,

  -- 合同级
  t1.contract_no,
  t1.loan_init_interest_rate,
  t1.loan_type_cn,
  t1.contract_term,
  t1.mortgage_rate,
  t1.shoufu_amount,

  -- 客户级
  t1.user_hash_no,
  t1.age,
  t1.job_type,
  t1.income_year,
  t1.income_year_max,
  t1.income_year_min,
  t1.idcard_area,

  -- 抵押物级
  t1.due_bill_no_guaranty,
  t1.pawn_value,
  t1.guarantee_type,

  -- 分布
  t1.distribution_array,

  -- 日期项目
  t1.biz_date,
  t2.project_id
from dw.abs_due_info_day as t1
join dim.project_due_bill_no as t2
on  t1.project_id  = nvl(t2.related_project_id,t2.project_id)
and t1.due_bill_no = t2.due_bill_no;

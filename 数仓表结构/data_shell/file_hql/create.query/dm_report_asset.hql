-- /**
--  * 库名 dm_report_asset 资产服务报告
--  */
-- DROP DATABASE IF EXISTS dm_report_asset;
-- CREATE DATABASE IF NOT EXISTS dm_report_asset;



-- /**
--  * 资产池统计
--  *
--  * 分区字段 biz_date
--  */
-- DROP TABLE IF EXISTS `dm_report_asset.asset_pool_sum`;
CREATE TABLE IF NOT EXISTS `dm_report_asset.asset_pool_sum`(
  `capital_id`                      string        COMMENT '资金方编号',
  `channel_id`                      string        COMMENT '渠道方编号',
  `project_id`                      string        COMMENT '项目编号',
  `total_remain_principal`          decimal(15,4) COMMENT '总本金余额',
  `total_remain_num`                decimal(11,0) COMMENT '未结清笔数',
  `weighted_average_rate`           decimal(15,8) COMMENT '加权平均利率',
  `weighted_average_remain_tentor`  decimal(3,0)  COMMENT '加权平均剩余期数(账龄)'
) COMMENT '资产池统计'
PARTITIONED BY (`d_date` string  COMMENT '快照日（yyyy—MM—dd）')
STORED AS PARQUET;


-- /**
--  * 资产池回款
--  *
--  * 分区字段 biz_date
--  */
-- DROP TABLE IF EXISTS `dm_report_asset.asset_pool_payment`;
CREATE TABLE IF NOT EXISTS `dm_report_asset.asset_pool_payment`(
  `capital_id`                      string        COMMENT '资金方编号',
  `channel_id`                      string        COMMENT '渠道方编号',
  `project_id`                      string        COMMENT '项目编号',
  `total_collection_amount`         decimal(15,4) COMMENT '总回收款-金额',
  `total_collection_num`            decimal(11,0) COMMENT '总回收款-笔数',
  `scheduled_amount`                decimal(15,4) COMMENT '计划内还款-金额',
  `scheduled_num`                   decimal(11,0) COMMENT '计划内还款-笔数',
  `repayment_amount`                decimal(15,4) COMMENT '提前结清-金额',
  `repayment_num`                   decimal(11,0) COMMENT '提前结清-笔数',
  `redeem_amount`                   decimal(15,4) COMMENT '赎回回款-金额',
  `redeem_num`                      decimal(11,0) COMMENT '赎回回款-笔数'
) COMMENT '资产池回款'
PARTITIONED BY (`d_date` string  COMMENT '快照日（yyyy—MM—dd）')
STORED AS PARQUET;


-- /**
--  * 资产池特征统计
--  *
--  * 分区字段 biz_date
--  */
-- DROP TABLE IF EXISTS `dm_report_asset.asset_pool_feature_sum`;
CREATE TABLE IF NOT EXISTS `dm_report_asset.asset_pool_feature_sum`(
  `capital_id`                      string        COMMENT '资金方编号',
  `channel_id`                      string        COMMENT '渠道方编号',
  `project_id`                      string        COMMENT '项目编号',
  `total_num`                       decimal(11,0) COMMENT '合计-笔数',
  `total_amount`                    decimal(15,4) COMMENT '合计-金额',
  `current_num`                     decimal(11,0) COMMENT '正常-笔数',
  `current_num_percent`             decimal(15,8) COMMENT '正常-笔数-占比',
  `current_amount`                  decimal(15,4) COMMENT '正常-金额',
  `current_amount_amount`           decimal(15,8) COMMENT '正常-金额-占比',
  `m1_num`                          decimal(11,0) COMMENT '逾期1~30天-笔数',
  `m1_num_percent`                  decimal(15,8) COMMENT '逾期1~30天-笔数-占比',
  `m1_amount`                       decimal(15,4) COMMENT '逾期1~30天-金额',
  `m1_amount_amount`                decimal(15,8) COMMENT '逾期1~30天-金额-占比',
  `m2_num`                          decimal(11,0) COMMENT '逾期31~60天-笔数',
  `m2_num_percent`                  decimal(15,8) COMMENT '逾期31~60天-笔数-占比',
  `m2_amount`                       decimal(15,4) COMMENT '逾期31~60天-金额',
  `m2_amount_amount`                decimal(15,8) COMMENT '逾期31~60天-金额-占比',
  `m3_num`                          decimal(11,0) COMMENT '逾期61-90天-笔数',
  `m3_num_percent`                  decimal(15,8) COMMENT '逾期61-90天-笔数-占比',
  `m3_amount`                       decimal(15,4) COMMENT '逾期61-90天-金额',
  `m3_amount_amount`                decimal(15,8) COMMENT '逾期61-90天-金额-占比',
  `m4plus_num`                      decimal(11,0) COMMENT '逾期90+天-笔数',
  `m4plus_num_percent`              decimal(15,8) COMMENT '逾期90+天-笔数-占比',
  `m4plus_amount`                   decimal(15,4) COMMENT '逾期90+天-金额',
  `m4plus_amount_amount`            decimal(15,8) COMMENT '逾期90+天-金额-占比'
) COMMENT '资产池特征统计'
PARTITIONED BY (`d_date` string  COMMENT '快照日（yyyy—MM—dd）')
STORED AS PARQUET;


-- /**
--  * 违约资产情况
--  *
--  * 分区字段 biz_date
--  */
-- DROP TABLE IF EXISTS `dm_report_asset.overdue_assets`;
CREATE TABLE IF NOT EXISTS `dm_report_asset.overdue_assets`(
  `capital_id`                      string        COMMENT '资金方编号',
  `channel_id`                      string        COMMENT '渠道方编号',
  `project_id`                      string        COMMENT '项目编号',
  `m2plus_num`                      decimal(11,0) COMMENT '违约30天+笔数',
  `m2plus_amount`                   decimal(15,4) COMMENT '违约30天+金额',
  `m7plus_num`                      decimal(11,0) COMMENT '违约180天+笔数',
  `m7plus_amount`                   decimal(15,4) COMMENT '违约180天+金额',
  `cumulative_recover_amount`       decimal(15,4) COMMENT '累计回收租金',
  `cumulative_delinque_rate`        decimal(15,8) COMMENT '累计违约率'
) COMMENT '违约资产情况'
PARTITIONED BY (`d_date` string  COMMENT '快照日（yyyy—MM—dd）')
STORED AS PARQUET;


-- /**
--  * 逾期90+情况
--  *
--  * 分区字段 biz_date
--  */
-- DROP TABLE IF EXISTS `dm_report_asset.overdue_assets_90plus`;
CREATE TABLE IF NOT EXISTS `dm_report_asset.overdue_assets_90plus`(
  `capital_id`                      string        COMMENT '资金方编号',
  `channel_id`                      string        COMMENT '渠道方编号',
  `project_id`                      string        COMMENT '项目编号',
  `static_m3plus_num`               decimal(11,0) COMMENT '静态90+笔数',
  `static_m3plus_amount`            decimal(15,4) COMMENT '静态90+金额',
  `static_recover_amount`           decimal(15,4) COMMENT '累计回收本金',
  `static_delinque_rate`            decimal(15,8) COMMENT '累计违约率'
) COMMENT '逾期90+情况'
PARTITIONED BY (`d_date` string  COMMENT '快照日（yyyy—MM—dd）')
STORED AS PARQUET;


-- /**
--  * 项目-借据（都是第一次）
--  *
--  * 分区字段 biz_date
--  */
-- DROP TABLE IF EXISTS `dm_report_asset.overdue_assets_static`;
CREATE TABLE IF NOT EXISTS `dm_report_asset.overdue_assets_static`(
  `capital_id`                      string        COMMENT '资金方编号',
  `channel_id`                      string        COMMENT '渠道方编号',
  `project_id`                      string        COMMENT '项目编号',
  `due_bill_no`                     string        COMMENT '借据ID',
  `project_start_date`              string        COMMENT '借据入项目日期',
  `project_start_remain_principal`  decimal(15,4) COMMENT '借据入项目时剩余本金',
  `project_start_remain_interest`   decimal(15,4) COMMENT '借据入项目时剩余利息',
  `overdue_day31_date`              string        COMMENT '静态逾期31时日期',
  `overdue_day31_principal`         decimal(15,4) COMMENT '静态逾期31时本金',
  `overdue_day31_interest`          decimal(15,4) COMMENT '静态逾期31时利息',
  `overdue_day61_date`              string        COMMENT '静态逾期61时日期',
  `overdue_day61_principal`         decimal(15,4) COMMENT '静态逾期61时本金',
  `overdue_day61_interest`          decimal(15,4) COMMENT '静态逾期61时利息',
  `overdue_day91_date`              string        COMMENT '静态逾期91时日期',
  `overdue_day91_principal`         decimal(15,4) COMMENT '静态逾期91时本金',
  `overdue_day91_interest`          decimal(15,4) COMMENT '静态逾期91时利息'
) COMMENT '项目-借据'
STORED AS PARQUET;


-- /**
--  * 逾期资产明细
--  *
--  * 分区字段 biz_date
--  */
-- DROP TABLE IF EXISTS `dm_report_asset.overdue_assets_detail`;
CREATE TABLE IF NOT EXISTS `dm_report_asset.overdue_assets_detail`(
  `capital_id`                      string        COMMENT '资金方编号',
  `channel_id`                      string        COMMENT '渠道方编号',
  `project_id`                      string        COMMENT '项目编号',
  `due_bill_no`                     string        COMMENT '借据号',
  `borrower_name`                   string        COMMENT '客户姓名',
  `overdue_start_date`              string        COMMENT '逾期开始日期',
  `overdue_amount`                  decimal(15,4) COMMENT '逾期本金+逾期利息'
) COMMENT '逾期资产明细'
PARTITIONED BY (`d_date` string  COMMENT '快照日（yyyy—MM—dd）')
STORED AS PARQUET;


-- /**
--  * 早偿
--  *
--  * 分区字段 biz_date
--  */
-- DROP TABLE IF EXISTS `dm_report_asset.early_repayment`;
CREATE TABLE IF NOT EXISTS `dm_report_asset.early_repayment`(
  `capital_id`                      string        COMMENT '资金方编号',
  `channel_id`                      string        COMMENT '渠道方编号',
  `project_id`                      string        COMMENT '项目编号',
  `due_bill_no`                     string        COMMENT '借据号',
  `borrower_name`                   string        COMMENT '客户姓名',
  `should_pay_date`                 string        COMMENT '应还日期',
  `should_pay_amount`               decimal(15,4) COMMENT '应还金额',
  `repayment_date`                  string        COMMENT '提前结清时间',
  `repayment_amount`                decimal(15,4) COMMENT '提前结清金额'
) COMMENT '早偿'
PARTITIONED BY (`d_date` string  COMMENT '快照日（yyyy—MM—dd）')
STORED AS PARQUET;


-- /**
--  * 借据状态
--  *
--  * 分区字段 biz_date
--  */
-- DROP TABLE IF EXISTS `dm_report_asset.loan_status_overdue`;
CREATE TABLE IF NOT EXISTS `dm_report_asset.loan_status_overdue`(
  `capital_id`                      string        COMMENT '资金方编号',
  `channel_id`                      string        COMMENT '渠道方编号',
  `project_id`                      string        COMMENT '项目编号',
  `due_bill_no`                     string        COMMENT '借据号',
  `borrower_name`                   string        COMMENT '客户姓名',
  `remain_principal`                decimal(15,4) COMMENT '本金余额',
  `remain_interest`                 decimal(15,4) COMMENT '利息余额',
  `loan_init_interest_rate`         decimal(15,8) COMMENT '年利率',
  `loan_init_term`                  decimal(3,0)  COMMENT '总期数',
  `loan_term_repaid`                decimal(3,0)  COMMENT '已还期数',
  `should_repay_term`               decimal(3,0)  COMMENT '未还期数（含逾期）',
  `loan_term_repaid`                decimal(3,0)  COMMENT '已过期数',
  `unbilled_terms`                  decimal(3,0)  COMMENT '未出账期数',
  `loan_status`                     string        COMMENT '资产状态-借据状态',
  `paid_out_type`                   string        COMMENT '结清原因',
  `paid_out_date`                   string        COMMENT '结清日期',
  `paid_out_amount`                 decimal(15,4) COMMENT '提前结清金额',
  `overdue_begin_date`              string        COMMENT '逾期开始日期',
  `overdue_days`                    decimal(5,0)  COMMENT '逾期天数',
  `overdue_principal`               decimal(15,4) COMMENT '逾期本金',
  `overdue_interest`                decimal(15,4) COMMENT '逾期利息'
) COMMENT '借据状态'
PARTITIONED BY (`d_date` string  COMMENT '快照日（yyyy—MM—dd）')
STORED AS PARQUET;


-- /**
--  * 借据回款
--  *
--  * 分区字段 biz_date
--  */
-- DROP TABLE IF EXISTS `dm_report_asset.loan_rebate`;
CREATE TABLE IF NOT EXISTS `dm_report_asset.loan_rebate`(
  `capital_id`                      string        COMMENT '资金方编号',
  `channel_id`                      string        COMMENT '渠道方编号',
  `project_id`                      string        COMMENT '项目编号',
  `due_bill_no`                     string        COMMENT '借据号',
  `repaid_date`                     string        COMMENT '实还日期',
  `paid_type`                       string        COMMENT '还款方式',
  `repaid_type`                     string        COMMENT '实还类型',
  `repaid_pincipal`                 decimal(15,4) COMMENT '实还本金',
  `repaid_interest`                 decimal(15,4) COMMENT '实还利息',
  `repaid_fee`                      decimal(15,4) COMMENT '实还费用',
  `repaid_penalty_interest`         decimal(15,4) COMMENT '实还罚息'
) COMMENT '借据回款'
PARTITIONED BY (`d_date` string  COMMENT '快照日（yyyy—MM—dd）')
STORED AS PARQUET;

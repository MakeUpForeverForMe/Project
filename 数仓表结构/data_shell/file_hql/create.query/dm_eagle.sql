-- 看管系统


-- 库名 dm_eagle 数据指标
-- DROP DATABASE IF EXISTS dm_eagle;
CREATE DATABASE IF NOT EXISTS dm_eagle COMMENT 'dm_eagle层数据（代偿前）';

-- DROP DATABASE IF EXISTS dm_eagle_cps;
CREATE DATABASE IF NOT EXISTS dm_eagle_cps COMMENT 'dm_eagle层数据（代偿后）';

use dm_eagle;
use dm_eagle_cps;



-- 资金相关
-- 无在途版-乐信资产变动信息(代偿版)(还款本金取T-2)
-- drop table if exists `eagle_asset_change_comp_t1`;
create table if not exists `eagle_asset_change_comp_t1`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `yesterday_remain_sum`                  decimal(15,4) COMMENT '当日初资产余额:前一日的资产余额，空则为0',
  `loan_sum_daily`                        decimal(15,4) COMMENT '当日新增放款金额',
  `repay_sum_daily`                       decimal(15,4) COMMENT '前一日还款本金',
  `repay_other_sum_daily`                 decimal(15,4) COMMENT '前一日资产还款息费',
  `today_remain_sum`                      decimal(15,4) COMMENT '日终资产余额',
  `biz_date`                              date          COMMENT '观察日期'
) COMMENT '资产变动信息(代偿版)'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- 无在途版-乐信资产变动信息(剔除代偿版)(还款本金取T-2)
-- drop table if exists `eagle_asset_change_t1`;
create table if not exists `eagle_asset_change_t1`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `yesterday_remain_sum`                  decimal(15,4) COMMENT '当日初资产余额:前一日的资产余额，空则为0',
  `loan_sum_daily`                        decimal(15,4) COMMENT '当日新增放款金额',
  `repay_sum_daily`                       decimal(15,4) COMMENT '前一日还款本金',
  `repay_other_sum_daily`                 decimal(15,4) COMMENT '前一日资产还款息费',
  `today_remain_sum`                      decimal(15,4) COMMENT '日终资产余额',
  `biz_date`                              date          COMMENT '观察日期'
) COMMENT '资产变动信息(剔除代偿版)'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- 有在途版-非乐信资产变动信息(代偿版)
-- drop table if exists `eagle_asset_change_comp`;
create table if not exists `eagle_asset_change_comp`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `yesterday_remain_sum`                  decimal(15,4) COMMENT '当日初资产余额:前一日的资产余额，空则为0',
  `loan_sum_daily`                        decimal(15,4) COMMENT '当日新增放款金额',
  `repay_sum_daily`                       decimal(15,4) COMMENT '当日还款本金',
  `repay_other_sum_daily`                 decimal(15,4) COMMENT '当日资产还款息费',
  `today_remain_sum`                      decimal(15,4) COMMENT '日终资产余额',
  `biz_date`                              date          COMMENT '观察日期'
) COMMENT '资产变动信息(代偿版)'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- 有在途版-非乐信资产变动信息(剔除代偿版)
-- drop table if exists `eagle_asset_change`;
create table if not exists `eagle_asset_change`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `yesterday_remain_sum`                  decimal(15,4) COMMENT '当日初资产余额:前一日的资产余额，空则为0',
  `loan_sum_daily`                        decimal(15,4) COMMENT '当日新增放款金额',
  `repay_sum_daily`                       decimal(15,4) COMMENT '当日还款本金',
  `repay_other_sum_daily`                 decimal(15,4) COMMENT '当日资产还款息费',
  `today_remain_sum`                      decimal(15,4) COMMENT '日终资产余额',
  `biz_date`                              date          COMMENT '观察日期'
) COMMENT '资产变动信息(剔除代偿版)'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;



-- 在途资金-代偿版
-- drop table if exists `eagle_unreach_funds`;
create table if not exists `eagle_unreach_funds`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `trade_yesterday_bal`                   decimal(15,4) COMMENT '前一日末在途金额',
  `repay_today_bal`                       decimal(15,4) COMMENT '当日到账在途金额',
  `repay_sum_daily`                       decimal(15,4) COMMENT '当日新增在途金额',
  `trade_today_bal`                       decimal(15,4) COMMENT '当日终在途金额',
  `biz_date`                              date          COMMENT '观察日期'
) COMMENT '在途资金-代偿版'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;



-- 拓展信息-到账信息(代偿版)
-- drop table if exists `eagle_asset_comp_info`;
create table if not exists `eagle_asset_comp_info`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `repay_sum_daily`                       decimal(15,4) COMMENT '当日还款本金',
  `repay_way`                             varchar(255)  COMMENT '到账方式:1-客户还款2-代偿回款3-回购回款4-代偿催回5-退票回款',
  `amount_type`                           varchar(255)  COMMENT '金额类型:1-本金2-利息3-罚息4-费用',
  `amount`                                decimal(15,4) COMMENT '金额',
  `biz_date`                              date          COMMENT '观察日期'
) COMMENT '拓展信息-到账信息(代偿版)'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;



-- 当日费用支出-代偿版
-- drop table if exists `eagle_acct_cost`;
create table if not exists `eagle_acct_cost`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `acct_total_fee`                        decimal(15,4) COMMENT '当日费用支出金额',
  `fee_type`                              varchar(255)  COMMENT '费用类型：1-账户费用2-扣划手续费3-其他4-税费支持',
  `amount`                                decimal(15,4) COMMENT '对应项的类型及金额',
  `biz_date`                              date          COMMENT '观察日期'
) COMMENT '当日费用支出-代偿版'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;



-- 资金相关
-- 资金账户信息
-- drop table if exists `eagle_funds`;
create table if not exists `eagle_funds`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `trade_yesterday_bal`                   decimal(15,4) COMMENT '当日初账户余额',
  `loan_today_bal`                        decimal(15,4) COMMENT '当日放款金额',
  `repay_today_bal`                       decimal(15,4) COMMENT '当日到账回款金额:客户还款+代偿回款+回购回购+退票回款',
  `acct_int`                              decimal(15,4) COMMENT '当日账户利息',
  `acct_total_fee`                        decimal(15,4) COMMENT '当日费用支出',
  `invest_cash`                           decimal(15,4) COMMENT '当日投资兑付金额',
  `trade_today_bal`                       decimal(15,4) COMMENT 'T日余额',
  `biz_date`                              date          COMMENT '观察日期'
) COMMENT '资金账户信息'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;



-- 回款明细
-- drop table if exists `eagle_repayment_detail`;
create table if not exists `eagle_repayment_detail`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `repay_today_bal`                       decimal(15,4) COMMENT '当日回款总金额:客户还款+代偿回款+回购回购+退票回款',
  `repay_type`                            varchar(255)  COMMENT '费用类型：1-客户还款2-代偿回款3-回购回款4-退票回款',
  `amount`                                decimal(15,4) COMMENT '对应项的金额',
  `biz_date`                              date          COMMENT '观察日期'
) COMMENT '回款明细'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;





-- /**
-- * 额度通过率分析
-- *
-- * 分区字段 biz_date
-- */
-- drop table if exists `eagle_credit_loan_approval_rate_day`;
create table if not exists `eagle_credit_loan_approval_rate_day`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `loan_terms`                            decimal(3,0)  COMMENT '贷款期数',

  `credit_apply_num`                      decimal(11,0) COMMENT '授信申请笔数',
  `credit_apply_num_accumulate`           decimal(11,0) COMMENT '累计授信申请笔数',
  `credit_approval_num`                   decimal(11,0) COMMENT '当日授信申请并授信成功笔数',
  `credit_approval_num_rate`              decimal(15,8) COMMENT '授信通过笔数通过率（当日授信申请并授信成功笔数/当日授信申请笔数×100%）',
  `credit_approval_num_rate_dod_ratio`    decimal(15,8) COMMENT '授信通过笔数通过率环比（(当日授信通过笔数-昨日授信通过笔数)/昨日授信通过笔数）',

  `loan_apply_num`                        decimal(11,0) COMMENT '用信申请笔数',
  `loan_apply_num_accumulate`             decimal(11,0) COMMENT '累计用信申请笔数',
  `loan_approval_num`                     decimal(11,0) COMMENT '当日用信申请并用信成功笔数',
  `loan_approval_num_accumulate`          decimal(11,0) COMMENT '累计用信申请并用信成功笔数',
  `loan_approval_rate`                    decimal(15,8) COMMENT '用信通过笔数通过率（当日用信申请并用信成功笔数/当日用信申请笔数×100%）',
  `loan_approval_num_rate_dod_ratio`      decimal(15,8) COMMENT '用信通过笔数通过率环比',

  `credit_apply_amount`                   decimal(15,4) COMMENT '授信申请金额',
  `credit_apply_amount_accumulate`        decimal(15,4) COMMENT '累计授信申请金额',
  `credit_approval_amount`                decimal(15,4) COMMENT '授信通过金额',
  `credit_approval_amount_rate`           decimal(15,8) COMMENT '授信通过金额通过率（授信通过金额/授信申请金额×100%）',
  `loan_apply_amount`                     decimal(15,4) COMMENT '用信申请金额',
  `loan_apply_amount_accumulate`          decimal(15,4) COMMENT '累计用信申请金额',
  `loan_approval_amount`                  decimal(15,4) COMMENT '用信通过金额',
  `loan_approval_amount_accumulate`       decimal(15,4) COMMENT '累计用信通过金额',
  `loan_approval_amount_rate`             decimal(15,8) COMMENT '用信通过金额通过率（用信通过金额/用信申请金额×100%）',

  `credit_apply_num_person`               decimal(11,0) COMMENT '授信申请人数',
  `credit_apply_num_person_accumulate`    decimal(11,0) COMMENT '累计授信申请人数',
  `credit_approval_num_person`            decimal(11,0) COMMENT '授信通过人数',
  `credit_approval_person_rate`           decimal(15,8) COMMENT '授信通过人数通过率（授信通过人数/授信申请人数×100%）',
  `loan_apply_num_person`                 decimal(11,0) COMMENT '用信申请人数',
  `loan_apply_num_person_accumulate`      decimal(11,0) COMMENT '累计用信申请人数',
  `loan_approval_num_person_accumulate`   decimal(11,0) COMMENT '累计用信通过人数',
  `loan_approval_person_rate`             decimal(15,8) COMMENT '用信通过人数通过率（用信通过人数/用信申请人数×100%）',
  `biz_date`                              date          COMMENT '授信、用信申请日期',
  `product_id`                            varchar(255)  COMMENT '产品编号'


) COMMENT '额度通过率分析'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- /**
-- * 额度统计
-- *
-- * 分区字段 biz_date
-- */
-- drop table if exists `eagle_credit_loan_approval_amount_sum_day`;
create table if not exists `eagle_credit_loan_approval_amount_sum_day`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `loan_terms`                            decimal(3,0)  COMMENT '贷款期数',

  `credit_approval_amount`                decimal(15,4) COMMENT '授信通过金额',
  `credit_approval_amount_accumulate`     decimal(15,4) COMMENT '累计授信通过金额',
  `credit_approval_amount_dod_ratio`      decimal(15,8) COMMENT '授信通过金额环比（(当日授信金额-昨日授信金额)/昨日授信金额）',

  `credit_approval_num`                   decimal(11,0) COMMENT '授信通过笔数',
  `credit_approval_num_accumulate`        decimal(11,0) COMMENT '累计授信通过笔数',
  `credit_approval_num_dod_ratio`         decimal(15,8) COMMENT '授信通过笔数环比',
  `credit_approval_num_num_avg`           decimal(15,4) COMMENT '授信通过件均（授信通过金额/授信通过笔数）',
  `credit_approval_num_num_avg_dod_ratio` decimal(15,4) COMMENT '授信通过件均环比',

  `loan_approval_amount`                  decimal(15,4) COMMENT '用信通过金额',
  `loan_approval_amount_accumulate`       decimal(15,4) COMMENT '累计用信通过金额',
  `loan_approval_amount_dod_ratio`        decimal(15,8) COMMENT '用信通过金额环比（(当日用信金额-昨日用信金额)/昨日用信金额）',

  `loan_approval_num`                     decimal(11,0) COMMENT '用信通过笔数',
  `loan_approval_num_accumulate`          decimal(11,0) COMMENT '累计用信通过笔数',
  `loan_approval_num_dod_ratio`           decimal(15,8) COMMENT '用信通过笔数环比',
  `loan_approval_num_num_avg`             decimal(15,4) COMMENT '用信通过件均（用信通过金额/用信通过笔数）',
  `loan_approval_num_num_avg_dod_ratio`   decimal(15,4) COMMENT '用信通过件均环比',

  `credit_approval_num_person`            decimal(11,0) COMMENT '授信通过人数',
  `credit_approval_num_person_accumulate` decimal(11,0) COMMENT '累计授信通过人数',
  `credit_approval_num_person_dod_ratio`  decimal(15,8) COMMENT '授信通过人数环比',
  `credit_approval_num_person_num_avg`    decimal(15,4) COMMENT '授信通过人均（授信通过金额/授信通过人数）',

  `loan_approval_num_person`              decimal(11,0) COMMENT '用信通过人数',
  `loan_approval_num_person_accumulate`   decimal(11,0) COMMENT '累计用信通过人数',
  `loan_approval_num_person_dod_ratio`    decimal(15,8) COMMENT '用信通过人数环比',
  `loan_approval_num_person_num_avg`      decimal(15,4) COMMENT '用信通过人均（用信通过金额/用信通过人数）',
  `biz_date`                              date          COMMENT '授信、用信通过日期',
  `product_id`                            varchar(255)  COMMENT '产品编号'
) COMMENT '额度统计'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- 进件分析
-- /**
-- * 动支率
-- *
-- * 分区字段 biz_date
-- */
-- drop table if exists `eagle_credit_loan_approval_amount_rate_day`;
create table if not exists `eagle_credit_loan_approval_amount_rate_day`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',

  `credit_approval_amount`                decimal(15,4) COMMENT '授信通过金额',
  `loan_approval_amount`                  decimal(15,4) COMMENT '用信通过金额（T+3内）',
  `credit_loan_approval_amount_rate`      decimal(15,8) COMMENT '动支率（用信通过金额/授信通过金额×100%）',
  `biz_date`                              date          COMMENT '授信通过日期',
  `product_id`                            varchar(255)  COMMENT '产品编号'
) COMMENT '动支率'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- /**
-- * 拒绝原因分布
-- *
-- * 分区字段 biz_date
-- */
-- drop table if exists `eagle_ret_msg_day`;
create table if not exists `eagle_ret_msg_day`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `loan_terms`                            decimal(3,0)  COMMENT '贷款期数',

  `ret_msg_stage`                         varchar(255)  COMMENT '拒绝阶段（01：授信、02：用信）',
  `ret_msg`                               varchar(255)  COMMENT '拒绝原因',
  `ret_msg_num`                           decimal(9,0)  COMMENT '拒绝原因笔数',
  `ret_msg_rate`                          decimal(9,0)  COMMENT '拒绝原因占比',
  `biz_date`                              date          COMMENT '风控处理日期',
  `product_id`                            varchar(255)  COMMENT '产品编号'
) COMMENT '拒绝原因分布'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;










-- /**
-- * 标题头信息
-- */
-- drop table if exists `eagle_title_info`;
create table if not exists `eagle_title_info`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `product_id`                            varchar(255)  COMMENT '产品编号',
  `project_init_amount`                   decimal(15,4) COMMENT '初始放款规模（项目级金额）',
  `loan_month_map`                        varchar(5000) COMMENT '放款月范围（含期数。结构：{"2020-07":[3,6,9],"2020-06":[9]}）',
  `biz_month_list`                        varchar(5000) COMMENT '快照月范围（结构：["2020-01","2020-02"]）',
  `loan_terms_list`                       varchar(255)  COMMENT '期数范围（结构：[3,6,9]）',
  `create_date`                           date          COMMENT '插入日期'
) COMMENT '标题头信息'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- 应还实还
-- /**
-- * 应还实还对比
-- *
-- * 分区字段 biz_date
-- */
-- drop table if exists `eagle_should_repay_repaid_amount_day`;
create table if not exists `eagle_should_repay_repaid_amount_day`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `loan_terms`                            decimal(3,0)  COMMENT '贷款期数',
  `should_repay_amount_plan`              decimal(15,4) COMMENT '计划应还金额',
  `should_repay_amount_actual`            decimal(15,4) COMMENT '实际应还金额',
  `repaid_amount`                         decimal(15,4) COMMENT '实还金额',
  `biz_date`                              date          COMMENT '放款日期',
  `project_id`                            varchar(255)  COMMENT '项目编号'
) COMMENT '应还实还对比'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- 放款规模
-- /**
-- * 放款规模
-- *
-- * 分区字段 biz_date
-- */
-- drop table if exists `eagle_loan_amount_day`;
create table if not exists `eagle_loan_amount_day`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `loan_terms`                            decimal(3,0)  COMMENT '贷款期数',

  `loan_amount`                           decimal(15,4) COMMENT '放款本金',
  `loan_amount_accumulate`                decimal(15,4) COMMENT '累计放款本金',
  `biz_date`                              date          COMMENT '放款日期',
  `product_id`                            varchar(255)  COMMENT '产品编号'
) COMMENT '放款规模'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- /**
-- * 资产规模（本金）
-- *
-- * 分区字段 biz_date
-- */
-- drop table if exists `eagle_asset_scale_principal_day`;
create table if not exists `eagle_asset_scale_principal_day`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `loan_terms`                            decimal(3,0)  COMMENT '贷款期数',

  `remain_principal`                      decimal(15,4) COMMENT '本金余额',

  `overdue_principal`                     decimal(15,4) COMMENT '逾期本金',
  `unposted_principal`                    decimal(15,4) COMMENT '未出账本金',

  `overdue_principal_rate`                decimal(15,8) COMMENT '逾期本金占比（逾期本金/本金余额×100%）',
  `unposted_principal_rate`               decimal(15,8) COMMENT '未出账本金占比（未出账本金/本金余额×100%）',
  `biz_date`                              date          COMMENT '观察日期',
  `product_id`                            varchar(255)  COMMENT '产品编号'
) COMMENT '资产规模（本金）'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- /**
-- * 资产规模（回款）
-- *
-- * 分区字段 biz_date
-- */
-- drop table if exists `eagle_asset_scale_repaid_day`;
create table if not exists `eagle_asset_scale_repaid_day`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `loan_terms`                            decimal(3,0)  COMMENT '贷款期数',

  `paid_amount`                           decimal(15,4) COMMENT '累计回款金额',
  `paid_principal`                        decimal(15,4) COMMENT '累计回款本金',
  `paid_interest_penalty_svc_fee`         decimal(15,4) COMMENT '累计回款息费',
  `paid_interest`                         decimal(15,4) COMMENT '累计回款利息',
  `paid_svc_fee`                          decimal(15,4) COMMENT '累计回款费用',
  `paid_penalty`                          decimal(15,4) COMMENT '累计回款罚金',

  `paid_principal_rate`                   decimal(15,8) COMMENT '累计回款本金占比（累计回款本金/累计回款金额×100%）',
  `paid_interest_svc_fee_rate`            decimal(15,8) COMMENT '累计回款费息占比（累计回款费息/累计回款金额×100%）',

  `repay_amount`                          decimal(15,4) COMMENT '当日实还金额',
  `repay_principal`                       decimal(15,4) COMMENT '当日实还本金',
  `repay_interest_penalty_svc_fee`        decimal(15,4) COMMENT '当日实还息费',
  `repay_interest`                        decimal(15,4) COMMENT '当日实还利息',
  `repay_svc_fee`                         decimal(15,4) COMMENT '当日实还费用',
  `repay_penalty`                         decimal(15,4) COMMENT '当日实还罚金',

  `repay_principal_rate`                  decimal(15,8) COMMENT '当日实还本金占比（当日实还本金/当日实还金额×100%）',
  `repay_interest_svc_fee_rate`           decimal(15,8) COMMENT '当日实还费息占比（当日实还费息/当日实还金额×100%）',
  `biz_date`                              date          COMMENT '还款日期',
  `product_id`                            varchar(255)  COMMENT '产品编号'
) COMMENT '资产规模（回款）'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- /**
-- * 逾期率表现（曾有取第一次的剩余本金）
-- *
-- * 分区字段 biz_month
-- */
-- drop table if exists `eagle_overdue_rate_month`;
create table if not exists `eagle_overdue_rate_month`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `loan_terms`                            decimal(3,0)  COMMENT '贷款期数',

  `dpd`                                   varchar(255)  COMMENT 'DPD时期（1+、3+、7+、14+、30+、60+、90+、120+、150+、180+）',
  `mob`                                   varchar(255)  COMMENT '进展月（MOB0—MOB9放款月后的自然月）',

  `loan_month`                            varchar(255)  COMMENT '放款月',
  `loan_amount`                           decimal(15,4) COMMENT '放款本金',
  `remain_principal`                      decimal(15,4) COMMENT '当前本金余额',

  `overdue_principal`                     decimal(15,4) COMMENT '当前逾期借据逾期本金',
  `overdue_remain_principal`              decimal(15,4) COMMENT '当前逾期借据本金余额',
  `overdue_rate`                          decimal(15,8) COMMENT '当前逾期借据逾期率表现（当前逾期借据本金余额/放款本金×100%）',
  `overdue_principal_once`                decimal(15,4) COMMENT '曾有逾期借据逾期本金（取第一次）',
  `overdue_remain_principal_once`         decimal(15,4) COMMENT '曾有逾期借据本金余额（取第一次）',
  `overdue_rate_once`                     decimal(15,8) COMMENT '曾有逾期借据逾期率表现（曾有逾期借据本金余额/放款本金×100%）',
  `biz_date`                              date          COMMENT '观察日期',
  `product_id`                            varchar(255)  COMMENT '产品编号'
) COMMENT '逾期率表现'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- /**
-- * 递延逾期率（全部放款月、产品级）
-- *
-- * 分区字段 biz_date
-- */
-- drop table if exists `eagle_deferred_overdue_rate_full_month_product_day`;
create table if not exists `eagle_deferred_overdue_rate_full_month_product_day`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `dpd`                                   varchar(255)  COMMENT 'DPD时期（1+、3+、7+、14+、30+、60+、90+、120+、150+、180+）',
  `loan_principal_deferred`               decimal(15,4) COMMENT '递延放款本金',
  `overdue_principal`                     decimal(15,4) COMMENT '逾期借据逾期本金',
  `overdue_remain_principal`              decimal(15,4) COMMENT '逾期借据本金余额',
  `overdue_rate_overdue_principal`        decimal(15,8) COMMENT '逾期借据递延逾期率（逾期本金）（逾期借据逾期本金/递延放款本金×100%）',
  `overdue_rate_remain_principal`         decimal(15,8) COMMENT '逾期借据递延逾期率（本金余额）（逾期借据本金余额/递延放款本金×100%）',
  `biz_date`                              varchar(255)  COMMENT '观察日期',
  `product_id`                            varchar(255)  COMMENT '产品编号'
) COMMENT '递延逾期率（全部放款月、产品级）'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- /**
-- * 递延逾期率（单个放款月、产品级）
-- *
-- * 分区字段 biz_date
-- */
-- drop table if exists `eagle_deferred_overdue_rate_sigle_month_product_day`;
create table if not exists `eagle_deferred_overdue_rate_sigle_month_product_day`(
  `capital_id`                            string        COMMENT '资金方编号',
  `channel_id`                            string        COMMENT '渠道方编号',
  `project_id`                            string        COMMENT '项目编号',
  `dpd`                                   string        COMMENT 'DPD时期（1+、3+、7+、14+、30+、60+、90+、120+、150+、180+）',
  `loan_month`                            string        COMMENT '放款月',
  `loan_principal_deferred`               decimal(15,4) COMMENT '递延放款本金',
  `overdue_principal`                     decimal(15,4) COMMENT '逾期借据逾期本金',
  `overdue_remain_principal`              decimal(15,4) COMMENT '逾期借据本金余额',
  `overdue_rate_overdue_principal`        decimal(15,8) COMMENT '逾期借据递延逾期率（逾期本金）（逾期借据逾期本金/递延放款本金×100%）',
  `overdue_rate_remain_principal`         decimal(15,8) COMMENT '逾期借据递延逾期率（本金余额）（逾期借据本金余额/递延放款本金×100%）',
  `biz_date`                              string        COMMENT '观察日期',
  `product_id`                            string        COMMENT '产品编号'
) COMMENT '递延逾期率（单个放款月、产品级）'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- /**
-- * 首期流入率（取进展日30天的逾期数据）
-- *
-- * 分区字段 biz_date
-- */
-- drop table if exists `eagle_inflow_rate_first_term_day`;
create table if not exists `eagle_inflow_rate_first_term_day`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `loan_terms`                            decimal(3,0)  COMMENT '贷款期数',

  `dob`                                   varchar(255)  COMMENT '进展日（DOB1————＞DOB30，DOB1为应还日）',

  `loan_num`                              decimal(11,0) COMMENT '放款借据数',
  `overdue_loan_num`                      decimal(11,0) COMMENT '逾期借据数',
  `overdue_loan_inflow_rate`              decimal(15,8) COMMENT '首期案件逾期率（逾期借据数/放款借据数×100%）',

  `loan_active_date`                      varchar(255)  COMMENT '放款日期',
  `loan_amount`                           decimal(15,4) COMMENT '放款本金',
  `overdue_principal`                     decimal(15,4) COMMENT '逾期本金',
  `overdue_principal_inflow_rate`         decimal(15,8) COMMENT '首期流入率（逾期本金）（逾期本金/放款本金×100%）',
  `overdue_remain_principal`              decimal(15,4) COMMENT '逾期借据本金余额',
  `remain_principal_inflow_rate`          decimal(15,8) COMMENT '首期流入率（逾期借据本金余额）（逾期借据本金余额/放款本金×100%）',
  `biz_date`                              date          COMMENT '观察日期',
  `product_id`                            varchar(255)  COMMENT '产品编号'
) COMMENT '首期流入率（取进展日30天的逾期数据）'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- /**
-- * 逾期流入率（取进展日30天的逾期数据）
-- *
-- * 分区字段 biz_date
-- */
-- drop table if exists `eagle_inflow_rate_day`;
create table if not exists `eagle_inflow_rate_day`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `loan_terms`                            decimal(3,0)  COMMENT '贷款期数',

  `dob`                                   varchar(255)  COMMENT '进展日（DOB1—DOB30，DOB1为应还日）',
  `is_first`                              varchar(255)  COMMENT '是否首次逾期（y表示首次，n表示非首次）',

  `should_repay_loan_num`                 decimal(11,0) COMMENT '应还日应还借据数',
  `overdue_loan_num`                      decimal(11,0) COMMENT '逾期借据数',
  `overdue_loan_inflow_rate`              decimal(15,8) COMMENT '案件逾期率（逾期借据数/应还日应还借据数×100%）',

  `loan_active_month`                     varchar(255)  COMMENT '放款月份',
  `should_repay_date`                     varchar(255)  COMMENT '应还日期',
  `remain_principal`                      decimal(15,4) COMMENT '应还日本金余额',
  `should_repay_principal`                decimal(15,4) COMMENT '应还日应还本金',
  `overdue_principal`                     decimal(15,4) COMMENT '逾期借据逾期本金',
  `overdue_remain_principal`              decimal(15,4) COMMENT '逾期借据本金余额',
  `overdue_principal_inflow_rate`         decimal(15,8) COMMENT '逾期借据逾期本金逾期率（逾期借据逾期本金/应还日应还本金×100%）',
  `remain_principal_inflow_rate`          decimal(15,8) COMMENT '逾期借据本金余额逾期率（逾期借据本金余额/应还日本金余额×100%）',
  `biz_date`                              date          COMMENT '观察日期',
  `product_id`                            varchar(255)  COMMENT '产品编号'
) COMMENT '逾期流入率（取进展日30天的逾期数据）'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- /**
-- * 迁徙率（12个月）
-- *
-- * 分区字段 biz_month
-- */
-- drop table if exists `eagle_migration_rate_month`;
create table if not exists `eagle_migration_rate_month`(
  `capital_id`                            varchar(255)  COMMENT '资金方编号',
  `channel_id`                            varchar(255)  COMMENT '渠道方编号',
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `loan_terms`                            decimal(3,0)  COMMENT '贷款期数',

  `overdue_stage`                         varchar(255)  COMMENT '逾期阶段（C、M1—>M6、M6+）',
  -- `overdue_stage`                                 varchar(255)  COMMENT '逾期阶段（C—>M1至M5—>M6）',
  `mob`                                   varchar(255)  COMMENT '进展月（MOB0—MOB9放款月后的自然月）',

  `loan_month`                            varchar(255)  COMMENT '放款月',
  `loan_amount`                           decimal(15,4) COMMENT '月放款本金',

  `remain_principal`                      decimal(15,4) COMMENT '当月末本金余额',
  -- `remain_principal_l`                            decimal(15,4) COMMENT '上月末本金余额',
  -- `remain_principal_t`                            decimal(15,4) COMMENT '本月末本金余额',
  -- `migration_rate`                                decimal(15,8) COMMENT '月迁徙率（MOB3的M1—M2迁徙率：MOB3月末状态为M2本金余额/MOB2月末状态为M1本金余额×100%）'
  `biz_month`                             varchar(255)  COMMENT '观察月',
  `product_id`                            varchar(255)  COMMENT '产品编号'
) COMMENT '迁徙率（12个月）'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
















-- ABS系统

-- uabs-core 库
-- 资产总体信息（项目——所有包）
-- drop table if exists `abs_asset_information_project`;
create table if not exists `abs_asset_information_project`(
  -- basicAssetInformationVo
  `asset_count`                           decimal(12,0) COMMENT '资产数量',
  `customer_count`                        decimal(12,0) COMMENT '借款人数量',
  `remain_principal`                      decimal(20,8) COMMENT '本金余额',
  `remain_interest`                       decimal(20,8) COMMENT '本息余额',
  `customer_remain_principal_avg`         decimal(20,8) COMMENT '借款人平均本金余额',
  -- basicAmountFeaturesVo
  `remain_principal_max`                  decimal(20,8) COMMENT '单笔最大本金余额',
  `remain_principal_min`                  decimal(20,8) COMMENT '单笔最小本金余额',
  `remain_principal_avg`                  decimal(20,8) COMMENT '平均本金余额',
  `loan_principal_max`                    decimal(20,8) COMMENT '单笔最大合同金额',
  `loan_principal_min`                    decimal(20,8) COMMENT '单笔最小合同金额',
  `loan_principal_avg`                    decimal(20,8) COMMENT '平均合同金额',
  -- basicCharacteristicsVo
  `loan_terms_max`                        decimal(3,0)  COMMENT '单笔最大合同期数',
  `loan_terms_min`                        decimal(3,0)  COMMENT '单笔最小合同期数',
  `loan_terms_avg`                        decimal(11,8) COMMENT '平均合同期数',
  `loan_terms_avg_weighted`               decimal(11,8) COMMENT '加权平均合同期数',
  `remain_term_max`                       decimal(3,0)  COMMENT '单笔最大剩余期数',
  `remain_term_min`                       decimal(3,0)  COMMENT '单笔最小剩余期数',
  `remain_term_avg`                       decimal(11,8) COMMENT '平均剩余期数',
  `remain_term_avg_weighted`              decimal(11,8) COMMENT '加权平均剩余期数', -- basicAssetInformationVo 中也要
  `repaid_term_max`                       decimal(3,0)  COMMENT '单笔最大已还期数',
  `repaid_term_min`                       decimal(3,0)  COMMENT '单笔最小已还期数',
  `repaid_term_avg`                       decimal(11,8) COMMENT '平均已还期数',
  `repaid_term_avg_weighted`              decimal(11,8) COMMENT '加权平均已还期数',
  `aging_max`                             decimal(3,0)  COMMENT '单笔最大账龄',
  `aging_min`                             decimal(3,0)  COMMENT '单笔最小账龄',
  `aging_avg`                             decimal(11,8) COMMENT '平均账龄',
  `aging_avg_weighted`                    decimal(11,8) COMMENT '加权平均账龄',
  `remain_period_max`                     decimal(3,0)  COMMENT '单笔最大合同剩余期限（账龄相关）',
  `remain_period_min`                     decimal(3,0)  COMMENT '单笔最小合同剩余期限（账龄相关）',
  `remain_period_avg`                     decimal(11,8) COMMENT '平均合同剩余期限（账龄相关）',
  `remain_period_avg_weighted`            decimal(11,8) COMMENT '加权平均合同剩余期限（账龄相关）',
  -- basicInterestRateVo
  `interest_rate_max`                     decimal(20,8) COMMENT '单笔最大年利率',
  `interest_rate_min`                     decimal(20,8) COMMENT '单笔最小年利率',
  `interest_rate_avg`                     decimal(20,8) COMMENT '平均年利率',
  `interest_rate_avg_weighted`            decimal(20,8) COMMENT '加权平均年利率', -- basicAssetInformationVo 中也要
  -- basicBorrowerVo
  `age_max`                               decimal(3,0)  COMMENT '最大年龄',
  `age_min`                               decimal(3,0)  COMMENT '最小年龄',
  `age_avg`                               decimal(11,8) COMMENT '平均年龄',
  `age_avg_weighted`                      decimal(11,8) COMMENT '加权平均年龄',
  `income_year_max`                       decimal(20,8) COMMENT '最大年收入',
  `income_year_min`                       decimal(20,8) COMMENT '最小年收入',
  `income_year_avg`                       decimal(20,8) COMMENT '平均年收入',
  `income_year_avg_weighted`              decimal(20,8) COMMENT '加权平均年收入',
  `income_debt_ratio_max`                 decimal(20,8) COMMENT '最大收入债务比',
  `income_debt_ratio_min`                 decimal(20,8) COMMENT '最小收入债务比',
  `income_debt_ratio_avg`                 decimal(20,8) COMMENT '平均收入债务比',
  `income_debt_ratio_avg_weighted`        decimal(20,8) COMMENT '加权平均收入债务比',
  -- basicMortgagedVo
  `pledged_asset_balance`                 decimal(20,8) COMMENT '抵押的资产余额',
  `pledged_asset_count`                   decimal(12,0) COMMENT '抵押的资产笔数',
  `pledged_asset_balance_ratio`           decimal(20,8) COMMENT '抵押资产余额占比',
  `pledged_asset_count_ratio`             decimal(20,8) COMMENT '抵押资产笔数占比',
  `pawn_value`                            decimal(20,8) COMMENT '抵押初始评估价值',
  `pledged_asset_rate_avg_weighted`       decimal(20,8) COMMENT '加权平均抵押率', -- basicAssetInformationVo 中也要
  `is_allbag`                             varchar(255)  COMMENT '是否是所有包(y: 是 , n : 否)',
  `biz_date`                              varchar(255)  COMMENT '观察日期',
  `project_id`                            varchar(255)  COMMENT '项目编号'
) COMMENT '资产总体信息（项目——所有包）'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- 资产总体信息（包）
-- drop table if exists `abs_asset_information_bag`;
create table if not exists `abs_asset_information_bag`(
  `project_id`                            varchar(255)  COMMENT '项目编号',
  -- basicAssetInformationVo
  `asset_count`                           decimal(12,0) COMMENT '资产数量',
  `customer_count`                        decimal(12,0) COMMENT '借款人数量',
  `remain_principal`                      decimal(20,8) COMMENT '本金余额',
  `remain_interest`                       decimal(20,8) COMMENT '本息余额',
  `customer_remain_principal_avg`         decimal(20,8) COMMENT '借款人平均本金余额',
  -- basicAmountFeaturesVo
  `remain_principal_max`                  decimal(20,8) COMMENT '单笔最大本金余额',
  `remain_principal_min`                  decimal(20,8) COMMENT '单笔最小本金余额',
  `remain_principal_avg`                  decimal(20,8) COMMENT '平均本金余额',
  `loan_principal_max`                    decimal(20,8) COMMENT '单笔最大合同金额',
  `loan_principal_min`                    decimal(20,8) COMMENT '单笔最小合同金额',
  `loan_principal_avg`                    decimal(20,8) COMMENT '平均合同金额',
  -- basicCharacteristicsVo
  `loan_terms_max`                        decimal(3,0)  COMMENT '单笔最大合同期数',
  `loan_terms_min`                        decimal(3,0)  COMMENT '单笔最小合同期数',
  `loan_terms_avg`                        decimal(11,8) COMMENT '平均合同期数',
  `loan_terms_avg_weighted`               decimal(11,8) COMMENT '加权平均合同期数',
  `remain_term_max`                       decimal(3,0)  COMMENT '单笔最大剩余期数',
  `remain_term_min`                       decimal(3,0)  COMMENT '单笔最小剩余期数',
  `remain_term_avg`                       decimal(11,8) COMMENT '平均剩余期数',
  `remain_term_avg_weighted`              decimal(11,8) COMMENT '加权平均剩余期数', -- basicAssetInformationVo 中也要
  `repaid_term_max`                       decimal(3,0)  COMMENT '单笔最大已还期数',
  `repaid_term_min`                       decimal(3,0)  COMMENT '单笔最小已还期数',
  `repaid_term_avg`                       decimal(11,8) COMMENT '平均已还期数',
  `repaid_term_avg_weighted`              decimal(11,8) COMMENT '加权平均已还期数',
  `aging_max`                             decimal(3,0)  COMMENT '单笔最大账龄',
  `aging_min`                             decimal(3,0)  COMMENT '单笔最小账龄',
  `aging_avg`                             decimal(11,8) COMMENT '平均账龄',
  `aging_avg_weighted`                    decimal(11,8) COMMENT '加权平均账龄',
  `remain_period_max`                     decimal(3,0)  COMMENT '单笔最大合同剩余期限（账龄相关）',
  `remain_period_min`                     decimal(3,0)  COMMENT '单笔最小合同剩余期限（账龄相关）',
  `remain_period_avg`                     decimal(11,8) COMMENT '平均合同剩余期限（账龄相关）',
  `remain_period_avg_weighted`            decimal(11,8) COMMENT '加权平均合同剩余期限（账龄相关）',
  -- basicInterestRateVo
  `interest_rate_max`                     decimal(20,8) COMMENT '单笔最大年利率',
  `interest_rate_min`                     decimal(20,8) COMMENT '单笔最小年利率',
  `interest_rate_avg`                     decimal(20,8) COMMENT '平均年利率',
  `interest_rate_avg_weighted`            decimal(20,8) COMMENT '加权平均年利率', -- basicAssetInformationVo 中也要
  -- basicBorrowerVo
  `age_max`                               decimal(3,0)  COMMENT '最大年龄',
  `age_min`                               decimal(3,0)  COMMENT '最小年龄',
  `age_avg`                               decimal(11,8) COMMENT '平均年龄',
  `age_avg_weighted`                      decimal(11,8) COMMENT '加权平均年龄',
  `income_year_max`                       decimal(20,8) COMMENT '最大年收入',
  `income_year_min`                       decimal(20,8) COMMENT '最小年收入',
  `income_year_avg`                       decimal(20,8) COMMENT '平均年收入',
  `income_year_avg_weighted`              decimal(20,8) COMMENT '加权平均年收入',
  `income_debt_ratio_max`                 decimal(20,8) COMMENT '最大收入债务比',
  `income_debt_ratio_min`                 decimal(20,8) COMMENT '最小收入债务比',
  `income_debt_ratio_avg`                 decimal(20,8) COMMENT '平均收入债务比',
  `income_debt_ratio_avg_weighted`        decimal(20,8) COMMENT '加权平均收入债务比',
  -- basicMortgagedVo
  `pledged_asset_balance`                 decimal(20,8) COMMENT '抵押的资产余额',
  `pledged_asset_count`                   decimal(12,0) COMMENT '抵押的资产笔数',
  `pledged_asset_balance_ratio`           decimal(20,8) COMMENT '抵押资产余额占比',
  `pledged_asset_count_ratio`             decimal(20,8) COMMENT '抵押资产笔数占比',
  `pawn_value`                            decimal(20,8) COMMENT '抵押初始评估价值',
  `pledged_asset_rate_avg_weighted`       decimal(20,8) COMMENT '加权平均抵押率', -- basicAssetInformationVo 中也要
  `biz_date`                              varchar(255)  COMMENT '观察日期',
  `bag_id`                                varchar(255)  COMMENT '包编号'
) COMMENT '资产总体信息（包）'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- 资产总体信息（包）
-- drop table if exists `abs_asset_information_bag_snapshot`;
create table if not exists `abs_asset_information_bag_snapshot`(
  `project_id`                            varchar(255)  COMMENT '项目编号',
  -- basicAssetInformationVo
  `asset_count`                           decimal(12,0) COMMENT '资产数量',
  `customer_count`                        decimal(12,0) COMMENT '借款人数量',
  `remain_principal`                      decimal(20,8) COMMENT '本金余额',
  `remain_interest`                       decimal(20,8) COMMENT '本息余额',
  `customer_remain_principal_avg`         decimal(20,8) COMMENT '借款人平均本金余额',
  -- basicAmountFeaturesVo
  `remain_principal_max`                  decimal(20,8) COMMENT '单笔最大本金余额',
  `remain_principal_min`                  decimal(20,8) COMMENT '单笔最小本金余额',
  `remain_principal_avg`                  decimal(20,8) COMMENT '平均本金余额',
  `loan_principal_max`                    decimal(20,8) COMMENT '单笔最大合同金额',
  `loan_principal_min`                    decimal(20,8) COMMENT '单笔最小合同金额',
  `loan_principal_avg`                    decimal(20,8) COMMENT '平均合同金额',
  -- basicCharacteristicsVo
  `loan_terms_max`                        decimal(3,0)  COMMENT '单笔最大合同期数',
  `loan_terms_min`                        decimal(3,0)  COMMENT '单笔最小合同期数',
  `loan_terms_avg`                        decimal(11,8) COMMENT '平均合同期数',
  `loan_terms_avg_weighted`               decimal(11,8) COMMENT '加权平均合同期数',
  `remain_term_max`                       decimal(3,0)  COMMENT '单笔最大剩余期数',
  `remain_term_min`                       decimal(3,0)  COMMENT '单笔最小剩余期数',
  `remain_term_avg`                       decimal(11,8) COMMENT '平均剩余期数',
  `remain_term_avg_weighted`              decimal(11,8) COMMENT '加权平均剩余期数', -- basicAssetInformationVo 中也要
  `repaid_term_max`                       decimal(3,0)  COMMENT '单笔最大已还期数',
  `repaid_term_min`                       decimal(3,0)  COMMENT '单笔最小已还期数',
  `repaid_term_avg`                       decimal(11,8) COMMENT '平均已还期数',
  `repaid_term_avg_weighted`              decimal(11,8) COMMENT '加权平均已还期数',
  `aging_max`                             decimal(3,0)  COMMENT '单笔最大账龄',
  `aging_min`                             decimal(3,0)  COMMENT '单笔最小账龄',
  `aging_avg`                             decimal(11,8) COMMENT '平均账龄',
  `aging_avg_weighted`                    decimal(11,8) COMMENT '加权平均账龄',
  `remain_period_max`                     decimal(3,0)  COMMENT '单笔最大合同剩余期限（账龄相关）',
  `remain_period_min`                     decimal(3,0)  COMMENT '单笔最小合同剩余期限（账龄相关）',
  `remain_period_avg`                     decimal(11,8) COMMENT '平均合同剩余期限（账龄相关）',
  `remain_period_avg_weighted`            decimal(11,8) COMMENT '加权平均合同剩余期限（账龄相关）',
  -- basicInterestRateVo
  `interest_rate_max`                     decimal(20,8) COMMENT '单笔最大年利率',
  `interest_rate_min`                     decimal(20,8) COMMENT '单笔最小年利率',
  `interest_rate_avg`                     decimal(20,8) COMMENT '平均年利率',
  `interest_rate_avg_weighted`            decimal(20,8) COMMENT '加权平均年利率', -- basicAssetInformationVo 中也要
  -- basicBorrowerVo
  `age_max`                               decimal(3,0)  COMMENT '最大年龄',
  `age_min`                               decimal(3,0)  COMMENT '最小年龄',
  `age_avg`                               decimal(11,8) COMMENT '平均年龄',
  `age_avg_weighted`                      decimal(11,8) COMMENT '加权平均年龄',
  `income_year_max`                       decimal(20,8) COMMENT '最大年收入',
  `income_year_min`                       decimal(20,8) COMMENT '最小年收入',
  `income_year_avg`                       decimal(20,8) COMMENT '平均年收入',
  `income_year_avg_weighted`              decimal(20,8) COMMENT '加权平均年收入',
  `income_debt_ratio_max`                 decimal(20,8) COMMENT '最大收入债务比',
  `income_debt_ratio_min`                 decimal(20,8) COMMENT '最小收入债务比',
  `income_debt_ratio_avg`                 decimal(20,8) COMMENT '平均收入债务比',
  `income_debt_ratio_avg_weighted`        decimal(20,8) COMMENT '加权平均收入债务比',
  -- basicMortgagedVo
  `pledged_asset_balance`                 decimal(20,8) COMMENT '抵押的资产余额',
  `pledged_asset_count`                   decimal(12,0) COMMENT '抵押的资产笔数',
  `pledged_asset_balance_ratio`           decimal(20,8) COMMENT '抵押资产余额占比',
  `pledged_asset_count_ratio`             decimal(20,8) COMMENT '抵押资产笔数占比',
  `pawn_value`                            decimal(20,8) COMMENT '抵押初始评估价值',
  `pledged_asset_rate_avg_weighted`       decimal(20,8) COMMENT '加权平均抵押率', -- basicAssetInformationVo 中也要
  `bag_id`                                varchar(255)  COMMENT '包编号'
) COMMENT '资产总体信息（包、封包时）'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- 现金流分析（包、封包时）
-- drop table if exists `abs_asset_information_cash_flow_bag_snapshot`;
create table if not exists `abs_asset_information_cash_flow_bag_snapshot`(
  `project_id`                            varchar(255)  COMMENT '项目编号',
  `should_repay_date`                     varchar(255)  COMMENT '应还日期',
  `remain_principal_term_begin`           decimal(20,8) COMMENT '期初本金余额',
  `remain_principal_term_end`             decimal(20,8) COMMENT '期末本金余额',
  `should_repay_amount`                   decimal(20,8) COMMENT '应还金额',
  `should_repay_principal`                decimal(20,8) COMMENT '应还本金',
  `should_repay_interest`                 decimal(20,8) COMMENT '应还利息',
  `should_repay_cost`                     decimal(20,8) COMMENT '应还费用',
  `bag_id`                                varchar(255)  COMMENT '包编号'
) COMMENT '现金流分析（包、封包时）'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- 现金流分析（项目、所有包、包）
-- drop table if exists `abs_asset_information_cash_flow_bag_day`;
create table if not exists `abs_asset_information_cash_flow_bag_day`(
  `bag_date`                                      varchar(255)   COMMENT '封包日期',
  `data_extraction_day`                           varchar(255)   COMMENT '最新数据提取日',

  `remain_principal_term_begin`                   decimal(30,10) COMMENT '期初本金余额',
  `remain_principal_term_end`                     decimal(30,10) COMMENT '期末本金余额',

  `should_repay_amount`                           decimal(30,10) COMMENT '应收金额',
  `should_repay_principal`                        decimal(30,10) COMMENT '应收本金',
  `should_repay_interest`                         decimal(30,10) COMMENT '应收利息',
  `should_repay_cost`                             decimal(30,10) COMMENT '应收费用',

  `paid_amount`                                   decimal(30,10) COMMENT '实收金额',
  `paid_principal`                                decimal(30,10) COMMENT '实收本金',
  `paid_interest`                                 decimal(30,10) COMMENT '实收利息',
  `paid_cost`                                     decimal(30,10) COMMENT '实收费用',

  `overdue_paid_amount`                           decimal(30,10) COMMENT '逾期还款金额',
  `overdue_paid_principal`                        decimal(30,10) COMMENT '逾期还款本金',
  `overdue_paid_interest`                         decimal(30,10) COMMENT '逾期还款利息',
  `overdue_paid_cost`                             decimal(30,10) COMMENT '逾期还款费用',

  `prepayment_amount`                             decimal(30,10) COMMENT '提前还款金额',
  `prepayment_principal`                          decimal(30,10) COMMENT '提前还款本金',
  `prepayment_interest`                           decimal(30,10) COMMENT '提前还款利息',
  `prepayment_cost`                               decimal(30,10) COMMENT '提前还款费用',

  `normal_paid_amount`                            decimal(30,10) COMMENT '正常还款金额',
  `normal_paid_principal`                         decimal(30,10) COMMENT '正常还款本金',
  `normal_paid_interest`                          decimal(30,10) COMMENT '正常还款利息',
  `normal_paid_cost`                              decimal(30,10) COMMENT '正常还款费用',

  `pmml_should_repayamount`                       decimal(30,10) COMMENT '预测应收金额',
  `pmml_should_repayprincipal`                    decimal(30,10) COMMENT '预测应收本金',
  `pmml_should_repayinterest`                     decimal(30,10) COMMENT '预测应收利息',

  `pmml_paid_amount`                              decimal(30,10) COMMENT '预测实收金额',
  `pmml_paid_principal`                           decimal(30,10) COMMENT '预测实收本金',
  `pmml_paid_interest`                            decimal(30,10) COMMENT '预测实收利息',

  `collect_date`                                  varchar(255)   COMMENT '统计日期'

  `biz_date`                                      varchar(255)   COMMENT '观察日期',
  `project_id`                                    varchar(255)   COMMENT '项目编号',
  `bag_id`                                        varchar(255)   COMMENT '包编号（包编号、default_project、default_all_bag）'
) COMMENT '现金流分析（项目、所有包、包）'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- abs分布表（项目——所有包）
-- drop table if exists `abs_asset_distribution_day`;
create table if not exists `abs_asset_distribution_day`(
  `is_allbag`                     varchar(255)   COMMENT '是否是所有包(y: 是 , n : 否)',
  `asset_tab_name`                decimal(4,0)   COMMENT '分布标签名称（1：未偿本金余额分布，2：资产利率分布，3：资产合同期限分布，4：资产剩余期限分布，5：资产已还期数分布，6：账龄分布，7：合同剩余期限分布（账龄相关），8：还款方式分布，9：借款人年龄分布，10：借款人行业分布，11：借款人年收入分布，12：借款人风控结果分布，13：借款人信用等级分布，14：借款人反欺诈等级分布，15：借款人资产等级分布，16：借款人地区分布，17：抵押率分布，18：车辆品牌分布，19：新旧车辆分布）',
  `asset_name`                    varchar(255)   COMMENT '分布项名称',
  `asset_name_order`              decimal(4,0)   COMMENT '分布项名称排序',
  `remain_principal`              decimal(20,5)  COMMENT '本金余额',
  `remain_principal_ratio`        decimal(25,10) COMMENT '本金余额占比（分布项本金余额/本金余额）',
  `loan_num`                      decimal(15,0)  COMMENT '借据笔数',
  `loan_numratio`                 decimal(25,10) COMMENT '借据笔数占比（分布项借据笔数/借据笔数）',
  `remain_principal_loan_num_avg` decimal(20,5)  COMMENT '平均每笔余额（本金余额/借据笔数）',
  `biz_date`                      varchar(255)   COMMENT '观察日期',
  `project_id`                    varchar(255)   COMMENT '项目编号'
) COMMENT 'abs分布表（项目——所有包）'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- abs分布表（包）
-- drop table if exists `abs_asset_distribution_bag_day`;
create table if not exists `abs_asset_distribution_bag_day`(
  `project_id`                    varchar(255)   COMMENT '项目编号',
  `asset_tab_name`                decimal(4,0)   COMMENT '分布标签名称（1：未偿本金余额分布，2：资产利率分布，3：资产合同期限分布，4：资产剩余期限分布，5：资产已还期数分布，6：账龄分布，7：合同剩余期限分布（账龄相关），8：还款方式分布，9：借款人年龄分布，10：借款人行业分布，11：借款人年收入分布，12：借款人风控结果分布，13：借款人信用等级分布，14：借款人反欺诈等级分布，15：借款人资产等级分布，16：借款人地区分布，17：抵押率分布，18：车辆品牌分布，19：新旧车辆分布）',
  `asset_name`                    varchar(255)   COMMENT '分布项名称',
  `asset_name_order`              decimal(4,0)   COMMENT '分布项名称排序',
  `remain_principal`              decimal(20,5)  COMMENT '本金余额',
  `remain_principal_ratio`        decimal(25,10) COMMENT '本金余额占比（分布项本金余额/本金余额）',
  `loan_num`                      decimal(15,0)  COMMENT '借据笔数',
  `loan_numratio`                 decimal(25,10) COMMENT '借据笔数占比（分布项借据笔数/借据笔数）',
  `remain_principal_loan_num_avg` decimal(20,5)  COMMENT '平均每笔余额（本金余额/借据笔数）',
  `biz_date`                      varchar(255)   COMMENT '观察日期',
  `bag_id`                        varchar(255)   COMMENT '包编号'
) COMMENT 'abs分布表（包）'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- abs分布表（包、封包时）
-- drop table if exists `abs_asset_distribution_bag_snapshot_day`;
create table if not exists `abs_asset_distribution_bag_snapshot_day`(
  `project_id`                    varchar(255)   COMMENT '项目编号',
  `asset_tab_name`                decimal(4,0)   COMMENT '分布标签名称（1：未偿本金余额分布，2：资产利率分布，3：资产合同期限分布，4：资产剩余期限分布，5：资产已还期数分布，6：账龄分布，7：合同剩余期限分布（账龄相关），8：还款方式分布，9：借款人年龄分布，10：借款人行业分布，11：借款人年收入分布，12：借款人风控结果分布，13：借款人信用等级分布，14：借款人反欺诈等级分布，15：借款人资产等级分布，16：借款人地区分布，17：抵押率分布，18：车辆品牌分布，19：新旧车辆分布）',
  `asset_name`                    varchar(255)   COMMENT '分布项名称',
  `asset_name_order`              decimal(4,0)   COMMENT '分布项名称排序',
  `remain_principal`              decimal(20,5)  COMMENT '本金余额',
  `remain_principal_ratio`        decimal(25,10) COMMENT '本金余额占比（分布项本金余额/本金余额）',
  `loan_num`                      decimal(15,0)  COMMENT '借据笔数',
  `loan_numratio`                 decimal(25,10) COMMENT '借据笔数占比（分布项借据笔数/借据笔数）',
  `remain_principal_loan_num_avg` decimal(20,5)  COMMENT '平均每笔余额（本金余额/借据笔数）',
  `bag_id`                        varchar(255)   COMMENT '包编号'
) COMMENT 'abs分布表（包、封包时）'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- 逾期情况监控
-- 各阶段本金余额/封包时本金余额
-- DROP TABLE IF EXISTS `abs_overdue_rate_day`;
CREATE TABLE IF NOT EXISTS `abs_overdue_rate_day`(
  `is_allbag`                                     varchar(255)  COMMENT '是否是所有包（y：是，n：否）',
  `dpd`                                           varchar(255)  COMMENT 'DPD时期（1+、7+、14+、30+、60+、90+、120+、150+、180+、1_7、8_15、15_30、31_60、61_90、91_120、121_150、151_180）',

  `remain_principal`                              decimal(20,5) COMMENT '封包时本金余额',
  `overdue_remain_principal`                      decimal(20,5) COMMENT '当前逾期借据本金余额',
  `overdue_remain_principal_new`                  decimal(20,5) COMMENT '新增逾期借据本金余额',
  `overdue_remain_principal_once`                 decimal(20,5) COMMENT '累计逾期借据本金余额',

  `bag_due_num`                                   decimal(15,0) COMMENT '封包时借据笔数',
  `overdue_num`                                   decimal(15,0) COMMENT '当前逾期借据笔数',
  `overdue_num_new`                               decimal(15,0) COMMENT '新增逾期借据笔数',
  `overdue_num_once`                              decimal(15,0) COMMENT '累计逾期借据笔数',

  `bag_due_person_num`                            decimal(15,0) COMMENT '封包时借据人数',
  `overdue_person_num`                            decimal(15,0) COMMENT '当前逾期借据人数',
  `overdue_person_num_new`                        decimal(15,0) COMMENT '新增逾期借据人数',
  `overdue_person_num_once`                       decimal(15,0) COMMENT '累计逾期借据人数',
  `biz_date`                                      varchar(255)  COMMENT '观察日期',
  `project_id`                                    varchar(255)  COMMENT '项目编号',
  `bag_id`                                        varchar(255)  COMMENT '包编号'
) COMMENT '逾期情况监控（各阶段本金余额/封包时本金余额）'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

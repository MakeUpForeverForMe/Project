-- DROP DATABASE IF EXISTS `dm_eagle`;
CREATE DATABASE IF NOT EXISTS `dm_eagle` COMMENT 'dm_eagle层数据（代偿前）' location 'cosn://bigdata-center-prod-1253824322/user/hadoop/warehouse/dm_eagle.db';

-- DROP DATABASE IF EXISTS `dm_eagle_cps`;
CREATE DATABASE IF NOT EXISTS `dm_eagle_cps` COMMENT 'dm_eagle层数据（代偿后）' location 'cosn://bigdata-center-prod-1253824322/user/hadoop/warehouse/dm_eagle_cps.db';



-- 资金相关
-- 无在途版-乐信资产变动信息(代偿版)(还款本金取T-2)
-- DROP TABLE IF EXISTS `dm_eagle.eagle_asset_change_comp_t1`;
CREATE TABLE IF NOT EXISTS `dm_eagle.eagle_asset_change_comp_t1`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `yesterday_remain_sum`                          decimal(25,5)  COMMENT '当日初资产余额:前一日的资产余额，空则为0',
  `loan_sum_daily`                                decimal(25,5)  COMMENT '当日新增放款金额',
  `repay_sum_daily`                               decimal(25,5)  COMMENT '前一日还款本金',
  `repay_other_sum_daily`                         decimal(25,5)  COMMENT '前一日资产还款息费',
  `today_remain_sum`                              decimal(25,5)  COMMENT '日终资产余额'
) COMMENT '资产变动信息(代偿版)'
PARTITIONED BY(`biz_date` string COMMENT '观察日期')
STORED AS PARQUET;


-- 无在途版-乐信资产变动信息(剔除代偿版)(还款本金取T-2)
-- DROP TABLE IF EXISTS `dm_eagle.eagle_asset_change_t1`;
CREATE TABLE IF NOT EXISTS `dm_eagle.eagle_asset_change_t1`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `yesterday_remain_sum`                          decimal(25,5)  COMMENT '当日初资产余额:前一日的资产余额，空则为0',
  `loan_sum_daily`                                decimal(25,5)  COMMENT '当日新增放款金额',
  `repay_sum_daily`                               decimal(25,5)  COMMENT '前一日还款本金',
  `repay_other_sum_daily`                         decimal(25,5)  COMMENT '前一日资产还款息费',
  `today_remain_sum`                              decimal(25,5)  COMMENT '日终资产余额'
) COMMENT '资产变动信息(剔除代偿版)'
PARTITIONED BY(`biz_date` string COMMENT '观察日期')
STORED AS PARQUET;


-- 有在途版-非乐信资产变动信息(代偿版)
-- DROP TABLE IF EXISTS `dm_eagle.eagle_asset_change_comp`;
CREATE TABLE IF NOT EXISTS `dm_eagle.eagle_asset_change_comp`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `yesterday_remain_sum`                          decimal(25,5)  COMMENT '当日初资产余额:前一日的资产余额，空则为0',
  `loan_sum_daily`                                decimal(25,5)  COMMENT '当日新增放款金额',
  `repay_sum_daily`                               decimal(25,5)  COMMENT '当日还款本金',
  `repay_other_sum_daily`                         decimal(25,5)  COMMENT '当日资产还款息费',
  `today_remain_sum`                              decimal(25,5)  COMMENT '日终资产余额'
) COMMENT '资产变动信息(代偿版)'
PARTITIONED BY(`biz_date` string COMMENT '观察日期')
STORED AS PARQUET;


-- 有在途版-非乐信资产变动信息(剔除代偿版)
-- DROP TABLE IF EXISTS `dm_eagle.eagle_asset_change`;
CREATE TABLE IF NOT EXISTS `dm_eagle.eagle_asset_change`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `yesterday_remain_sum`                          decimal(25,5)  COMMENT '当日初资产余额:前一日的资产余额，空则为0',
  `loan_sum_daily`                                decimal(25,5)  COMMENT '当日新增放款金额',
  `repay_sum_daily`                               decimal(25,5)  COMMENT '当日还款本金',
  `repay_other_sum_daily`                         decimal(25,5)  COMMENT '当日资产还款息费',
  `today_remain_sum`                              decimal(25,5)  COMMENT '日终资产余额'
) COMMENT '资产变动信息(剔除代偿版)'
PARTITIONED BY(`biz_date` string COMMENT '观察日期')
STORED AS PARQUET;


-- 在途资金-代偿版
-- DROP TABLE IF EXISTS `dm_eagle.eagle_unreach_funds`;
CREATE TABLE IF NOT EXISTS `dm.eagle_unreach_funds`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `trade_yesterday_bal`                           decimal(15,4)  COMMENT '前一日末在途金额',
  `repay_today_bal`                               decimal(15,4)  COMMENT '当日到账在途金额',
  `repay_sum_daily`                               decimal(15,4)  COMMENT '当日新增在途金额',
  `trade_today_bal`                               decimal(15,4)  COMMENT '当日终在途金额'
) COMMENT '在途资金-代偿版'
PARTITIONED BY(`biz_date` string COMMENT '观察日期')
STORED AS PARQUET;


-- 拓展信息-到账信息(代偿版)
-- DROP TABLE IF EXISTS `dm_eagle.eagle_asset_comp_info`;
CREATE TABLE IF NOT EXISTS `dm_eagle.eagle_asset_comp_info`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `repay_sum_daily`                               decimal(25,5)  COMMENT '当日还款本金',
  `repay_way`                                     string         COMMENT '到账方式:1-客户还款2-代偿回款3-回购回款4-代偿催回5-退票回款',
  `amount_type`                                   string         COMMENT '金额类型:1-本金2-利息3-罚息4-费用',
  `amount`                                        decimal(25,5)  COMMENT '金额'
) COMMENT '拓展信息-到账信息(代偿版)'
PARTITIONED BY(`biz_date` string COMMENT '观察日期')
STORED AS PARQUET;


-- 当日费用支出-代偿版
-- DROP TABLE IF EXISTS `dm_eagle.eagle_acct_cost`;
create  table if not exists `dm.eagle_acct_cost`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `acct_total_fee`                                decimal(15,4)  COMMENT '当日费用支出金额',
  `fee_type`                                      string         COMMENT '费用类型：1-账户费用2-扣划手续费3-其他4-税费支持',
  `amount`                                        decimal(15,4)  COMMENT '对应项的类型及金额'
) COMMENT '当日费用支出-代偿版'
PARTITIONED BY(`biz_date` string COMMENT '观察日期')
STORED AS PARQUET;



-- 资金相关
-- 资金账户信息
-- DROP TABLE IF EXISTS `dm_eagle.eagle_funds`;
CREATE TABLE IF NOT EXISTS `dm.eagle_funds`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `trade_yesterday_bal`                           decimal(15,4)  COMMENT '当日初账户余额',
  `loan_today_bal`                                decimal(15,4)  COMMENT '当日放款金额',
  `repay_today_bal`                               decimal(15,4)  COMMENT '当日到账回款金额:客户还款+代偿回款+回购回购+退票回款',
  `acct_int`                                      decimal(15,4)  COMMENT '当日账户利息',
  `acct_total_fee`                                decimal(15,4)  COMMENT '当日费用支出',
  `invest_cash`                                   decimal(15,4)  COMMENT '当日投资兑付金额',
  `trade_today_bal`                               decimal(15,4)  COMMENT 'T日余额'
) COMMENT '资金账户信息'
PARTITIONED BY(`biz_date` string COMMENT '观察日期')
STORED AS PARQUET;


-- 回款明细
-- DROP TABLE IF EXISTS `dm_eagle.eagle_repayment_detail`;
CREATE TABLE IF NOT EXISTS `dm.eagle_repayment_detail`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `repay_today_bal`                               decimal(15,4)  COMMENT '当日回款总金额:客户还款+代偿回款+回购回购+退票回款',
  `repay_type`                                    string         COMMENT '费用类型：1-客户还款2-代偿回款3-回购回款4-退票回款',
  `amount`                                        decimal(15,4)  COMMENT '对应项的金额'
) COMMENT '回款明细'
PARTITIONED BY(`biz_date` string COMMENT '观察日期')
STORED AS PARQUET;





-- 额度通过率分析
-- 分区字段 biz_date
-- DROP TABLE IF EXISTS `dm_eagle.eagle_credit_loan_approval_rate_day`;
CREATE TABLE IF NOT EXISTS `dm_eagle.eagle_credit_loan_approval_rate_day`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `loan_terms`                                    decimal(3,0)   COMMENT '贷款期数',
  `credit_apply_num`                              decimal(12,0)  COMMENT '授信申请笔数',
  `credit_apply_num_accumulate`                   decimal(12,0)  COMMENT '累计授信申请笔数',
  `credit_approval_num`                           decimal(12,0)  COMMENT '当日授信申请并授信成功笔数',
  `credit_approval_num_rate`                      decimal(30,10) COMMENT '授信通过笔数通过率（当日授信申请并授信成功笔数/当日授信申请笔数×100%）',
  `credit_approval_num_rate_dod_ratio`            decimal(30,10) COMMENT '授信通过笔数通过率环比（(当日授信通过笔数-昨日授信通过笔数)/昨日授信通过笔数）',
  `loan_apply_num`                                decimal(12,0)  COMMENT '用信申请笔数',
  `loan_apply_num_accumulate`                     decimal(12,0)  COMMENT '累计用信申请笔数',
  `loan_approval_num`                             decimal(12,0)  COMMENT '当日用信申请并用信成功笔数',
  `loan_approval_num_accumulate`                  decimal(12,0)  COMMENT '累计用信申请并用信成功笔数',
  `loan_approval_rate`                            decimal(30,10) COMMENT '用信通过笔数通过率（当日用信申请并用信成功笔数/当日用信申请笔数×100%）',
  `loan_approval_num_rate_dod_ratio`              decimal(30,10) COMMENT '用信通过笔数通过率环比',
  `credit_apply_amount`                           decimal(25,5)  COMMENT '授信申请金额',
  `credit_apply_amount_accumulate`                decimal(25,5)  COMMENT '累计授信申请金额',
  `credit_approval_amount`                        decimal(25,5)  COMMENT '授信通过金额',
  `credit_approval_amount_rate`                   decimal(30,10) COMMENT '授信通过金额通过率（授信通过金额/授信申请金额×100%）',
  `loan_apply_amount`                             decimal(25,5)  COMMENT '用信申请金额',
  `loan_apply_amount_accumulate`                  decimal(25,5)  COMMENT '累计用信申请金额',
  `loan_approval_amount`                          decimal(25,5)  COMMENT '用信通过金额',
   `loan_approval_amount_accumulate`              decimal(25,5)  COMMENT '累计用信通过金额',
  `loan_approval_amount_rate`                     decimal(30,10) COMMENT '用信通过金额通过率（用信通过金额/用信申请金额×100%）',
  `credit_apply_num_person`                       decimal(12,0)  COMMENT '授信申请人数',
  `credit_apply_num_person_accumulate`            decimal(12,0)  COMMENT '累计授信申请人数',
  `credit_approval_num_person`                    decimal(12,0)  COMMENT '授信通过人数',
  `credit_approval_person_rate`                   decimal(30,10) COMMENT '授信通过人数通过率（授信通过人数/授信申请人数×100%）',
  `loan_apply_num_person`                         decimal(12,0)  COMMENT '用信申请人数',
  `loan_apply_num_person_accumulate`              decimal(12,0)  COMMENT '累计用信申请人数',
  `loan_approval_num_person`                      decimal(12,0)  COMMENT '用信通过人数',
  `loan_approval_num_person_accumulate`           decimal(12,0)  COMMENT '累计用信通过人数',
  `loan_approval_person_rate`                     decimal(30,10) COMMENT '用信通过人数通过率（用信通过人数/用信申请人数×100%）'
) COMMENT '额度通过率分析'
PARTITIONED BY(`biz_date` string COMMENT '授信、用信申请日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 额度统计
-- 分区字段 biz_date
-- DROP TABLE IF EXISTS `dm_eagle.eagle_credit_loan_approval_amount_sum_day`;
CREATE TABLE IF NOT EXISTS `dm_eagle.eagle_credit_loan_approval_amount_sum_day`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `loan_terms`                                    decimal(3,0)   COMMENT '贷款期数',
  `credit_approval_amount`                        decimal(25,5)  COMMENT '授信通过金额',
  `credit_approval_amount_accumulate`             decimal(25,5)  COMMENT '累计授信通过金额',
  `credit_approval_amount_dod_ratio`              decimal(30,10) COMMENT '授信通过金额环比（(当日授信金额-昨日授信金额)/昨日授信金额）',
  `credit_approval_num`                           decimal(12,0)  COMMENT '授信通过笔数',
  `credit_approval_num_accumulate`                decimal(12,0)  COMMENT '累计授信通过笔数',
  `credit_approval_num_dod_ratio`                 decimal(30,10) COMMENT '授信通过笔数环比',
  `credit_approval_num_num_avg`                   decimal(25,5)  COMMENT '授信通过件均（授信通过金额/授信通过笔数）',
  `credit_approval_num_num_avg_dod_ratio`         decimal(25,5)  COMMENT '授信通过件均环比',
  `loan_approval_amount`                          decimal(25,5)  COMMENT '用信通过金额',
  `loan_approval_amount_accumulate`               decimal(25,5)  COMMENT '累计用信通过金额',
  `loan_approval_amount_dod_ratio`                decimal(30,10) COMMENT '用信通过金额环比（(当日用信金额-昨日用信金额)/昨日用信金额）',
  `loan_approval_num`                             decimal(12,0)  COMMENT '用信通过笔数',
  `loan_approval_num_accumulate`                  decimal(12,0)  COMMENT '累计用信通过笔数',
  `loan_approval_num_dod_ratio`                   decimal(30,10) COMMENT '用信通过笔数环比',
  `loan_approval_num_num_avg`                     decimal(25,5)  COMMENT '用信通过件均（用信通过金额/用信通过笔数）',
  `loan_approval_num_num_avg_dod_ratio`           decimal(25,5)  COMMENT '用信通过件均环比',
  `credit_approval_num_person`                    decimal(12,0)  COMMENT '授信通过人数',
  `credit_approval_num_person_accumulate`         decimal(12,0)  COMMENT '累计授信通过人数',
  `credit_approval_num_person_dod_ratio`          decimal(30,10) COMMENT '授信通过人数环比',
  `credit_approval_num_person_num_avg`            decimal(25,5)  COMMENT '授信通过人均（授信通过金额/授信通过人数）',
  `loan_approval_num_person`                      decimal(12,0)  COMMENT '用信通过人数',
  `loan_approval_num_person_accumulate`           decimal(12,0)  COMMENT '累计用信通过人数',
  `loan_approval_num_person_dod_ratio`            decimal(30,10) COMMENT '用信通过人数环比',
  `loan_approval_num_person_num_avg`              decimal(25,5)  COMMENT '用信通过人均（用信通过金额/用信通过人数）'
) COMMENT '额度统计'
PARTITIONED BY(`biz_date` string COMMENT '授信、用信通过日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 进件分析
-- 动支率
-- 分区字段 biz_date
-- DROP TABLE IF EXISTS `dm_eagle.eagle_credit_loan_approval_amount_rate_day`;
CREATE TABLE IF NOT EXISTS `dm_eagle.eagle_credit_loan_approval_amount_rate_day`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `credit_approval_amount`                        decimal(25,5)  COMMENT '授信通过金额',
  `loan_approval_amount`                          decimal(25,5)  COMMENT '用信通过金额（T+3内）',
  `credit_loan_approval_amount_rate`              decimal(30,10) COMMENT '动支率（用信通过金额/授信通过金额×100%）'
) COMMENT '动支率'
PARTITIONED BY(`biz_date` string COMMENT '授信通过日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 拒绝原因分布
-- 分区字段 biz_date
-- DROP TABLE IF EXISTS `dm_eagle.eagle_ret_msg_day`;
CREATE TABLE IF NOT EXISTS `dm_eagle.eagle_ret_msg_day`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `loan_terms`                                    decimal(3,0)   COMMENT '贷款期数',
  `ret_msg_stage`                                 string         COMMENT '拒绝阶段（01：授信、02：用信）',
  `ret_msg`                                       string         COMMENT '拒绝原因',
  `ret_msg_num`                                   decimal(12,0)  COMMENT '拒绝原因笔数',
  `ret_msg_rate`                                  decimal(12,0)  COMMENT '拒绝原因占比（各个拒绝原因笔数/总的拒绝笔数）'
) COMMENT '拒绝原因分布'
PARTITIONED BY(`biz_date` string COMMENT '风控处理日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- DROP TABLE IF EXISTS `dm_eagle.assets_distribution`;
CREATE TABLE IF NOT EXISTS `dm_eagle.assets_distribution`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `bill_no`                                       string         COMMENT 'no-借据编号',
  `apply_no`                                      string         COMMENT 'no-用信申请编号',
  `credit_id`                                     string         COMMENT 'id-授信id',
  `use_credit_id`                                 string         COMMENT 'id-用信id',
  `age`                                           string         COMMENT 'old-年龄',
  `gender`                                        string         COMMENT 'old-性别',
  `income`                                        string         COMMENT 'old-收入',
  `education`                                     string         COMMENT 'old-学历',
  `region`                                        string         COMMENT 'old-地区',
  `province`                                      string         COMMENT 'old-省份',
  `bill_credit_line`                              decimal(20,0)  COMMENT 'bill-贷款额度',
  `bill_term`                                     decimal(5,0)   COMMENT 'bill-贷款总期数',
  `bill_interest_rate`                            decimal(30,10) COMMENT 'bill-利率',
  `credit_coef`                                   decimal(30,10) COMMENT '综合融资成本（8d/%）',
  `bill_sex`                                      string         COMMENT 'bill-性别',
  `bill_age`                                      decimal(5,0)   COMMENT 'bill-年龄',
  `bill_id_card_area`                             string         COMMENT 'bill-身份证地区',
  `bill_province`                                 string         COMMENT 'bill-身份证省份',
  `bill_education`                                string         COMMENT 'bill-学历',
  `bill_education_ws`                             string         COMMENT 'bill-学历级别',
  `bill_annual_income`                            decimal(25,5)  COMMENT 'bill-年收入',
  `score_level`                                   string         COMMENT '资产等级   a~e',
  `city_level`                                    string         COMMENT '城市等级标签 1-其他 2-五线城市 3-四线城市 4-三线城市 5-二线城市 6-新一线城市 7-一线城市 99-未知',
  `financ_ability`                                string         COMMENT '理财能力标签 1-低 2-中 3-高 99-未知',
  `consumption_level`                             string         COMMENT '消费水平标签 1-低 2-中 3-高 99-未知',
  `marriage_status`                               string         COMMENT '婚姻状态标签 1-单身 2-已婚  99-未知',
  `edu_level`                                     string         COMMENT '学历水平标签 1-低 2-中 3-高 99-未知',
  `income_level`                                  string         COMMENT '收入标签   1-低 2-中 3-高 99-未知',
  `apply_by_id_last_year`                         decimal(5,0)   COMMENT '跨平台数过去一年申请数量按身份证号码统计   1:(-,  7],     2:(7,   11],     3:(11,   15],     4:(15,   22],     5:(22,   +)',
  `apps_of_con_fin_pro_last_two_year`             decimal(5,0)   COMMENT '消费金融类过去两年申请产品数 1:(-,  0],     2:(0,   3],     3:(3,   7],      4:(7,   12],     5:(12,   +)',
  `usage_rate_of_compus_loan_app`                 decimal(5,0)   COMMENT '校园类贷款app过去30天内使用强度 1:(-,  1],    2:(1,   3],     3:(3,   +)',
  `apps_of_con_fin_org_last_two_year`             decimal(5,0)   COMMENT '消费金融类机构过去两年申请数量    1:(-,  2],    2:(2,   4],   3:(4,   7],   4:(7,   +)',
  `reject_times_of_credit_apply_last4_m`          decimal(5,0)   COMMENT '过去四个月授信失败次数    1:(-,  1],    2:(1,   3],     3:(3,   +)',
  `overdue_times_of_credit_apply_last4_m`         decimal(5,0)   COMMENT '过去四个月逾期次数  1:(-,  1],    2:(1,   5],     3:(5,   +)',
  `sum_credit_of_web_loan`                        decimal(5,0)   COMMENT '网络贷款平台总授信额度    1:(-,  0],    2:(0,   1000],     3:(1000,   2000],     4:(2000,   4000],     5:(4000,   +)',
  `count_of_register_apps_of_fin_last_m`          decimal(5,0)   COMMENT '过去一个月内金融类app注册总数量  1:(-,  0],    2:(0,   2],     3:(2,   +)',
  `count_of_uninstall_apps_of_loan_last3_m`       decimal(5,0)   COMMENT '过去三个月贷款类app卸载总数量   1:(-,  0],    2:(0,   2],     3:(2,   +)',
  `days_of_location_upload_lats9_m`               decimal(5,0)   COMMENT '过去九个月地理位置上报天数  1:(-,  5],     2:(5,   24],     3:(24,   61],     4:(61,   128],     5:(128,   +)',
  `account_for_in_town_in_work_day_last_m`        decimal(5,0)   COMMENT '最近一个月工作日出现在村镇占比    1: (-,0.1],   2:  (0.1, 0.2],  3: (0.2, 0.3], 4: (0.3, 0.5], 5: (0.5,1]',
  `count_of_installition_of_loan_app_last2_m`     decimal(5,0)   COMMENT '过去两个月金融贷款类app安装总个数 1: (-,1],   2:  (1, 3],  3: (3, 5], 4: (5, +)',
  `risk_of_device`                                decimal(5,0)   COMMENT '综合评估设备风险程度 1: (-, 600],   2:  (600, 660],  3: (660, 720], 4: (720, +)',
  `account_for_wifi_use_time_span_last5_m`        decimal(5,0)   COMMENT '过去五个月wifi连接时间长度占比  1: (-, 0.25],   2:  (0.25, 0.5],  3: (0.5, 0.8], 4: (0.8, 1]',
  `count_of_notices_of_fin_message_last9_m`       decimal(5,0)   COMMENT '过去九个月金融类通知接收总数量    1: (-, 80],   2:  (80, 300],  3: (300, 500], 4: (500, 800], 5: (800,+)',
  `days_of_earliest_register_of_loan_app_last9_m` decimal(5,0)   COMMENT '过去九个月最早注册贷款应用距今天数  1: (-, 60],   2:  (60, 150],  3: (150, 270]',
  `loan_amt_last3_m`                              decimal(5,0)   COMMENT '过去三个月贷款总金额 1:(-, 0],  2: (0, 3000],   3:  (3000, 30000],  4: (30000, +)',
  `overdue_loans_of_more_than1_day_last6_m`       decimal(5,0)   COMMENT '过去六个月发生一天以上的逾期贷款总笔数    1:(-, 0],  2: (0, 1],   3:  (1, 2],  4: (2, 3], 5: (5. +)',
  `amt_of_perfermance_loans_last3_m`              decimal(5,0)   COMMENT '过去三个月履约贷款总金额   1:(-, 0],  2: (0, 3000],   3:  (3000, 20000],  4: (20000, +)',
  `last_financial_query`                          decimal(5,0)   COMMENT '最近一次金融类查询距今时间(月)   1:(-, 1], 2:(1, 3], 3:(3, 6], 4: (6, 12], 5:(12,+)',
  `average_daily_open_times_of_fin_apps_last_m`   decimal(5,0)   COMMENT '过去一个月金融理财类app每天打开次数平均值 1: (-, 0.2],   2:  (0.2, 0.6],  3: (0.6, 3], 4: (3, +)',
  `times_of_uninstall_fin_apps_last15_d`          decimal(5,0)   COMMENT '过去半个月金融理财类app卸载总次数 1: (-, 2],   2:  (2, 20],  3: (20, 50], 4: (50, +)',
  `account_for_install_bussiness_apps_last4_m`    decimal(5,0)   COMMENT '过去四个月商务类app安装数量占比  1: (-, 0.01],   2:  (0.01, 0.02],  3: (0.02, 0.03], 4: (0.03, +)',
  `blk_list1`                                     decimal(5,0)   COMMENT '外部机构1黑名单    0：未命中，1：命中，-1：未知',
  `blk_list2`                                     decimal(5,0)   COMMENT '外部机构2黑名单    0：未命中，1：命中，-1：未知',
  `blk_list_loc`                                  decimal(5,0)   COMMENT '自有黑名单    0：未命中，1：命中',
  `virtual_malicious_status`                      decimal(5,0)   COMMENT '疑似涉黄涉恐   0：未命中，1：命中',
  `counterfeit_agency_status`                     decimal(5,0)   COMMENT '疑似资料伪造包装    0：未命中，1：命中',
  `forged_id_status`                              decimal(5,0)   COMMENT '疑似资料伪冒行为 0：未命中，1：命中',
  `gamer_arbitrage_status`                        decimal(5,0)   COMMENT '疑似营销活动欺诈   0：未命中，1：命中',
  `id_theft_status`                               decimal(5,0)   COMMENT '疑似资料被盗    0：未命中，1：命中',
  `hit_deadbeat_list`                             decimal(5,0)   COMMENT '疑似公开信息失信    0：未命中，1：命中',
  `fraud_industry`                                decimal(5,0)   COMMENT '疑似金融黑产相关   0：未命中，1：命中',
  `cat_pool`                                      decimal(5,0)   COMMENT '疑似手机猫池欺诈 0：未命中，1：命中',
  `suspicious_device`                             decimal(5,0)   COMMENT '疑似风险设备环境    0：未命中，1：命中',
  `abnormal_payment`                              decimal(5,0)   COMMENT '疑似异常支付行为 0：未命中，1：命中',
  `abnormal_account`                              decimal(5,0)   COMMENT '疑似线上养号   0：未命中，1：命中',
  `account_hacked`                                decimal(5,0)   COMMENT '疑似账号被盗风向   0：未命中，1：命中',
  `ret_msg`                                       string         COMMENT '拒绝原因',
  `associated_partner_evaluation_rating`          decimal(5,0)   COMMENT '关联方评估等级',
  `vendor_attributes`                             decimal(5,0)   COMMENT '场景方评分',
  `multi_borrowing`                               decimal(5,0)   COMMENT '多头借贷综合指数',
  `gambling_preference`                           decimal(5,0)   COMMENT '博彩偏好综合指数',
  `gaming_preference`                             decimal(5,0)   COMMENT '游戏偏好综合指数',
  `multimedia_preference`                         decimal(5,0)   COMMENT '视频偏好综合指数',
  `social_preference`                             decimal(5,0)   COMMENT '交友偏好综合指数'
) COMMENT 'dm客群分布维度表'
PARTITIONED BY(`product_id` string COMMENT '产品编号')
STORED AS PARQUET;






set hivevar:db_suffix=;

set hivevar:db_suffix=_cps;


-- 标题头信息
-- DROP TABLE IF EXISTS `dm_eagle${db_suffix}.eagle_title_info`;
CREATE TABLE IF NOT EXISTS `dm_eagle${db_suffix}.eagle_title_info`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `product_id`                                    string         COMMENT '产品编号',
  `project_init_amount`                           decimal(25,5)  COMMENT '初始放款规模（项目级金额）',
  `loan_month_map`                                string         COMMENT '放款月范围（含期数。结构：{"2020-07":[3,6,9],"2020-06":[9]}）',
  `biz_month_list`                                string         COMMENT '快照月范围（结构：["2020-01","2020-02"]）',
  `loan_terms_list`                               string         COMMENT '期数范围（结构：[3,6,9]）',
  `create_date`                                   string         COMMENT '插入日期'
) COMMENT '标题头信息'
STORED AS PARQUET;


-- 应还实还
-- 应还实还对比
-- 分区字段 biz_date
-- DROP TABLE IF EXISTS `dm_eagle${db_suffix}.eagle_should_repay_repaid_amount_day`;
CREATE TABLE IF NOT EXISTS `dm_eagle${db_suffix}.eagle_should_repay_repaid_amount_day`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `loan_terms`                                    decimal(3,0)   COMMENT '贷款期数',
  `should_repay_amount_plan`                      decimal(25,5)  COMMENT '计划应还金额',
  `should_repay_amount_actual`                    decimal(25,5)  COMMENT '实际应还金额',
  `repaid_amount`                                 decimal(25,5)  COMMENT '实还金额'
) COMMENT '应还实还对比'
PARTITIONED BY(`biz_date` string COMMENT '放款日期',`project_id` string COMMENT '项目编号')
STORED AS PARQUET;


-- 放款规模
-- 放款规模
-- 分区字段 biz_date
-- DROP TABLE IF EXISTS `dm_eagle${db_suffix}.eagle_loan_amount_day`;
CREATE TABLE IF NOT EXISTS `dm_eagle${db_suffix}.eagle_loan_amount_day`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `loan_terms`                                    decimal(3,0)   COMMENT '贷款期数',
  `loan_amount`                                   decimal(25,5)  COMMENT '放款本金',
  `loan_amount_accumulate`                        decimal(25,5)  COMMENT '累计放款本金'
) COMMENT '放款规模'
PARTITIONED BY(`biz_date` string COMMENT '放款日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 资产规模
-- 分区字段 biz_date
-- DROP TABLE IF EXISTS `dm_eagle${db_suffix}.eagle_asset_scale_principal_day`;
CREATE TABLE IF NOT EXISTS `dm_eagle${db_suffix}.eagle_asset_scale_principal_day`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `loan_terms`                                    decimal(3,0)   COMMENT '贷款期数',
  `remain_principal`                              decimal(25,5)  COMMENT '本金余额',
  `overdue_principal`                             decimal(25,5)  COMMENT '逾期本金',
  `unposted_principal`                            decimal(25,5)  COMMENT '未出账本金',
  `overdue_principal_rate`                        decimal(30,10) COMMENT '逾期本金占比（逾期本金/本金余额×100%）',
  `unposted_principal_rate`                       decimal(30,10) COMMENT '未出账本金占比（未出账本金/本金余额×100%）'
) COMMENT '资产规模'
PARTITIONED BY(`biz_date` string COMMENT '观察日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 回款规模
-- 分区字段 biz_date
-- DROP TABLE IF EXISTS `dm_eagle${db_suffix}.eagle_asset_scale_repaid_day`;
CREATE TABLE IF NOT EXISTS `dm_eagle${db_suffix}.eagle_asset_scale_repaid_day`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `loan_terms`                                    decimal(3,0)   COMMENT '贷款期数',
  `paid_amount`                                   decimal(25,5)  COMMENT '累计回款金额',
  `paid_principal`                                decimal(25,5)  COMMENT '累计回款本金',
  `paid_interest_penalty_svc_fee`                 decimal(25,5)  COMMENT '累计回款息费',
  `paid_interest`                                 decimal(25,5)  COMMENT '累计回款利息',
  `paid_svc_fee`                                  decimal(25,5)  COMMENT '累计回款费用',
  `paid_penalty`                                  decimal(25,5)  COMMENT '累计回款罚金',
  `paid_principal_rate`                           decimal(30,10) COMMENT '累计回款本金占比（累计回款本金/累计回款金额×100%）',
  `paid_interest_svc_fee_rate`                    decimal(30,10) COMMENT '累计回款费息占比（累计回款费息/累计回款金额×100%）',
  `repay_amount`                                  decimal(25,5)  COMMENT '当日实还金额',
  `repay_principal`                               decimal(25,5)  COMMENT '当日实还本金',
  `repay_interest_penalty_svc_fee`                decimal(25,5)  COMMENT '当日实还息费',
  `repay_interest`                                decimal(25,5)  COMMENT '当日实还利息',
  `repay_svc_fee`                                 decimal(25,5)  COMMENT '当日实还费用',
  `repay_penalty`                                 decimal(25,5)  COMMENT '当日实还罚金',
  `repay_principal_rate`                          decimal(30,10) COMMENT '当日实还本金占比（当日实还本金/当日实还金额×100%）',
  `repay_interest_svc_fee_rate`                   decimal(30,10) COMMENT '当日实还费息占比（当日实还费息/当日实还金额×100%）'
) COMMENT '回款规模'
PARTITIONED BY(`biz_date` string COMMENT '还款日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 逾期率表现（曾有取第一次的剩余本金）
-- 分区字段 biz_date
-- DROP TABLE IF EXISTS `dm_eagle${db_suffix}.eagle_overdue_rate_month`;
CREATE TABLE IF NOT EXISTS `dm_eagle${db_suffix}.eagle_overdue_rate_month`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `loan_terms`                                    decimal(3,0)   COMMENT '贷款期数',
  `dpd`                                           string         COMMENT 'DPD时期（1+、3+、7+、14+、30+、60+、90+、120+、150+、180+）',
  `mob`                                           string         COMMENT '进展月（MOB0—MOB9放款月后的自然月）',
  `loan_month`                                    string         COMMENT '放款月',
  `loan_amount`                                   decimal(25,5)  COMMENT '放款本金',
  `remain_principal`                              decimal(25,5)  COMMENT '当前本金余额',
  `overdue_principal`                             decimal(25,5)  COMMENT '当前逾期借据逾期本金',
  `overdue_remain_principal`                      decimal(25,5)  COMMENT '当前逾期借据本金余额',
  `overdue_rate`                                  decimal(30,10) COMMENT '当前逾期借据逾期率表现（当前逾期借据本金余额/放款本金×100%）',
  `overdue_principal_once`                        decimal(25,5)  COMMENT '曾有逾期借据逾期本金（取第一次）',
  `overdue_remain_principal_once`                 decimal(25,5)  COMMENT '曾有逾期借据本金余额（取第一次）',
  `overdue_rate_once`                             decimal(30,10) COMMENT '曾有逾期借据逾期率表现（曾有逾期借据本金余额/放款本金×100%）'
) COMMENT '逾期率表现'
PARTITIONED BY(`biz_date` string COMMENT '观察日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 递延逾期率（全部放款月、产品级）
-- 分区字段 biz_date
-- DROP TABLE IF EXISTS `dm_eagle${db_suffix}.eagle_deferred_overdue_rate_full_month_product_day`;
CREATE TABLE IF NOT EXISTS `dm_eagle${db_suffix}.eagle_deferred_overdue_rate_full_month_product_day`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `dpd`                                           string         COMMENT 'DPD时期（1+、3+、7+、14+、30+、60+、90+、120+、150+、180+）',
  `loan_principal_deferred`                       decimal(25,5)  COMMENT '递延放款本金',
  `overdue_principal`                             decimal(25,5)  COMMENT '逾期借据逾期本金',
  `overdue_remain_principal`                      decimal(25,5)  COMMENT '逾期借据本金余额',
  `overdue_rate_overdue_principal`                decimal(30,10) COMMENT '逾期借据递延逾期率（逾期本金）（逾期借据逾期本金/递延放款本金×100%）',
  `overdue_rate_remain_principal`                 decimal(30,10) COMMENT '逾期借据递延逾期率（本金余额）（逾期借据本金余额/递延放款本金×100%）'
) COMMENT '递延逾期率（全部放款月、产品级）'
PARTITIONED BY(`biz_date` string COMMENT '观察日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 递延逾期率（单个放款月、产品级）
-- 分区字段 biz_date
-- DROP TABLE IF EXISTS `dm_eagle${db_suffix}.eagle_deferred_overdue_rate_sigle_month_product_day`;
CREATE TABLE IF NOT EXISTS `dm_eagle${db_suffix}.eagle_deferred_overdue_rate_sigle_month_product_day`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `dpd`                                           string         COMMENT 'DPD时期（1+、3+、7+、14+、30+、60+、90+、120+、150+、180+）',
  `loan_month`                                    string         COMMENT '放款月',
  `loan_principal_deferred`                       decimal(25,5)  COMMENT '递延放款本金',
  `overdue_principal`                             decimal(25,5)  COMMENT '逾期借据逾期本金',
  `overdue_remain_principal`                      decimal(25,5)  COMMENT '逾期借据本金余额',
  `overdue_rate_overdue_principal`                decimal(30,10) COMMENT '逾期借据递延逾期率（逾期本金）（逾期借据逾期本金/递延放款本金×100%）',
  `overdue_rate_remain_principal`                 decimal(30,10) COMMENT '逾期借据递延逾期率（本金余额）（逾期借据本金余额/递延放款本金×100%）'
) COMMENT '递延逾期率（单个放款月、产品级）'
PARTITIONED BY(`biz_date` string COMMENT '观察日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 首期流入率（取进展日30天的逾期数据）
-- 分区字段 biz_date
-- DROP TABLE IF EXISTS `dm_eagle${db_suffix}.eagle_inflow_rate_first_term_day`;
CREATE TABLE IF NOT EXISTS `dm_eagle${db_suffix}.eagle_inflow_rate_first_term_day`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `loan_terms`                                    decimal(3,0)   COMMENT '贷款期数',
  `dob`                                           string         COMMENT '进展日（DOB1————＞DOB30，DOB1为应还日）',
  `loan_num`                                      decimal(12,0)  COMMENT '放款借据数',
  `overdue_loan_num`                              decimal(12,0)  COMMENT '逾期借据数',
  `overdue_loan_inflow_rate`                      decimal(30,10) COMMENT '首期案件逾期率（逾期借据数/放款借据数×100%）',
  `loan_active_date`                              string         COMMENT '放款日期',
  `loan_amount`                                   decimal(25,5)  COMMENT '放款本金',
  `overdue_principal`                             decimal(25,5)  COMMENT '逾期本金',
  `overdue_principal_inflow_rate`                 decimal(30,10) COMMENT '首期流入率（逾期本金）（逾期本金/放款本金×100%）',
  `overdue_remain_principal`                      decimal(25,5)  COMMENT '逾期借据本金余额',
  `remain_principal_inflow_rate`                  decimal(30,10) COMMENT '首期流入率（逾期借据本金余额）（逾期借据本金余额/放款本金×100%）'
) COMMENT '首期流入率（取进展日30天的逾期数据）'
PARTITIONED BY(`biz_date` string COMMENT '观察日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;



-- 逾期流入率（取进展日30天的逾期数据）
-- 分区字段 biz_date
-- DROP TABLE IF EXISTS `dm_eagle${db_suffix}.eagle_inflow_rate_day`;
CREATE TABLE IF NOT EXISTS `dm_eagle${db_suffix}.eagle_inflow_rate_day`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `loan_terms`                                    decimal(3,0)   COMMENT '贷款期数',
  `dob`                                           string         COMMENT '进展日（DOB1—DOB30，DOB1为应还日）',
  `is_first`                                      string         COMMENT '是否首次逾期（y表示首次，n表示非首次）',
  `should_repay_loan_num`                         decimal(12,0)  COMMENT '应还日应还借据数',
  `overdue_loan_num`                              decimal(12,0)  COMMENT '逾期借据数',
  `overdue_loan_inflow_rate`                      decimal(30,10) COMMENT '案件逾期率（逾期借据数/应还日为T，T-1为C的T-1日应还借据数×100%）',
  `loan_active_month`                             string         COMMENT '放款月份',
  `should_repay_date`                             string         COMMENT '应还日期',
  `remain_principal`                              decimal(25,5)  COMMENT '应还日本金余额（应还日为T，T-1为C的T-1日实际应还本金）',
  `should_repay_principal`                        decimal(25,5)  COMMENT '应还日应还本金（应还日为T，T-1为C的T-1日实际本金余额）',
  `overdue_principal`                             decimal(25,5)  COMMENT '逾期借据逾期本金',
  `overdue_remain_principal`                      decimal(25,5)  COMMENT '逾期借据本金余额',
  `overdue_principal_inflow_rate`                 decimal(30,10) COMMENT '逾期借据逾期本金逾期率（逾期借据逾期本金/应还日为T，T-1为C的T-1日实际应还本金×100%）',
  `remain_principal_inflow_rate`                  decimal(30,10) COMMENT '逾期借据本金余额逾期率（逾期借据本金余额/应还日为T，T-1为C的T-1日实际本金余额×100%）'
) COMMENT '逾期流入率（取进展日30天的逾期数据）'
PARTITIONED BY(`biz_date` string COMMENT '观察日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 迁徙率（12个月）
-- 分区字段 biz_month
-- DROP TABLE IF EXISTS `dm_eagle${db_suffix}.eagle_migration_rate_month`;
CREATE TABLE IF NOT EXISTS `dm_eagle${db_suffix}.eagle_migration_rate_month`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `loan_terms`                                    decimal(3,0)   COMMENT '贷款期数',
  `overdue_stage`                                 string         COMMENT '逾期阶段（C、M1—>M6、M6+）',
  -- `overdue_stage`                                 string  COMMENT '逾期阶段（C—>M1至M5—>M6）',
  `mob`                                           string         COMMENT '进展月（MOB0—MOB9放款月后的自然月）',
  `loan_month`                                    string         COMMENT '放款月',
  `loan_amount`                                   decimal(25,5)  COMMENT '月放款本金',
  `remain_principal`                              decimal(25,5)  COMMENT '当月末本金余额'
  -- `remain_principal_l`                            decimal(25,5) COMMENT '上月末本金余额',
  -- `remain_principal_t`                            decimal(25,5) COMMENT '本月末本金余额',
  -- `migration_rate`                                decimal(30,10) COMMENT '月迁徙率（MOB3的M1—M2迁徙率：MOB3月末状态为M2本金余额/MOB2月末状态为M1本金余额×100%）'
) COMMENT '迁徙率（12个月）'
PARTITIONED BY(`biz_month` string COMMENT '观察月',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;






-- DROP TABLE IF EXISTS `dm_eagle${db_suffix}.eagle_loan_info`;
CREATE TABLE IF NOT EXISTS `dm_eagle${db_suffix}.eagle_loan_info`(
  `user_hash_no`                                  string         COMMENT '用户编号',
  `cust_id`                                       string         COMMENT '客户编号',
  `due_bill_no`                                   string         COMMENT '借据编号',
  `contract_no`                                   string         COMMENT '合同编号',
  `apply_no`                                      string         COMMENT '进件编号',
  `loan_usage`                                    string         COMMENT '贷款用途',
  `loan_active_date`                              string         COMMENT '放款日期',
  `cycle_day`                                     decimal(2,0)   COMMENT '账单日',
  `loan_expire_date`                              string         COMMENT '贷款到期日期',
  `loan_type`                                     string         COMMENT '分期类型（英文原值）（MCEP：等额本金，MCEI：等额本息，R：消费转分期，C：现金分期，B：账单分期，P：POS分期，M：大额分期（专项分期），MCAT：随借随还）',
  `loan_type_cn`                                  string         COMMENT '分期类型（汉语解释）',
  `loan_init_term`                                decimal(3,0)   COMMENT '贷款期数（3、6、9等）',
  `loan_term`                                     decimal(3,0)   COMMENT '当前期数',
  `loan_term_repaid`                              decimal(3,0)   COMMENT '已还期数',
  `loan_term_remain`                              decimal(3,0)   COMMENT '剩余期数',
  `loan_status`                                   string         COMMENT '借据状态（英文原值）（N：正常，O：逾期，F：已还清）',
  `loan_status_cn`                                string         COMMENT '借据状态（汉语解释）',
  `loan_out_reason`                               string         COMMENT '借据终止原因（P：提前还款，M：银行业务人员手工终止（manual），D：逾期自动终止（delinquency），R：锁定码终止（Refund），V：持卡人手动终止，C：理赔终止，T：退货终止，U：重组结清终止，F：强制结清终止，B：免息转分期）',
  `paid_out_type`                                 string         COMMENT '结清类型（英文原值）（D：代偿结清，H：回购结清，T：退货（车）结清，P：提前结清，C：强制结清，F：正常到期结清）',
  `paid_out_type_cn`                              string         COMMENT '结清类型（汉语解释）',
  `paid_out_date`                                 string         COMMENT '还款日期',
  `terminal_date`                                 string         COMMENT '提前终止日期',
  `loan_init_principal`                           decimal(25,5)  COMMENT '贷款本金',
  `loan_init_interest_rate`                       decimal(30,10) COMMENT '利息利率',
  `credit_coef`                                   decimal(30,10) COMMENT '综合融资成本（8d/%）',
  `loan_init_interest`                            decimal(25,5)  COMMENT '贷款利息',
  `loan_init_term_fee_rate`                       decimal(30,10) COMMENT '手续费费率',
  `loan_init_term_fee`                            decimal(25,5)  COMMENT '贷款手续费',
  `loan_init_svc_fee_rate`                        decimal(30,10) COMMENT '服务费费率',
  `loan_init_svc_fee`                             decimal(25,5)  COMMENT '贷款服务费',
  `loan_init_penalty_rate`                        decimal(30,10) COMMENT '罚息利率',
  `loan_penalty_accumulate`                       decimal(25,5)  COMMENT '累计罚息',
  `paid_amount`                                   decimal(25,5)  COMMENT '已还金额',
  `paid_principal`                                decimal(25,5)  COMMENT '已还本金',
  `paid_interest`                                 decimal(25,5)  COMMENT '已还利息',
  `paid_penalty`                                  decimal(25,5)  COMMENT '已还罚息',
  `paid_svc_fee`                                  decimal(25,5)  COMMENT '已还服务费',
  `paid_term_fee`                                 decimal(25,5)  COMMENT '已还手续费',
  `paid_mult`                                     decimal(25,5)  COMMENT '已还滞纳金',
  `remain_amount`                                 decimal(25,5)  COMMENT '剩余金额：本息费',
  `remain_principal`                              decimal(25,5)  COMMENT '剩余本金',
  `remain_interest`                               decimal(25,5)  COMMENT '剩余利息',
  `remain_svc_fee`                                decimal(25,5)  COMMENT '剩余服务费',
  `remain_term_fee`                               decimal(25,5)  COMMENT '剩余手续费',
  `remain_penalty`                                decimal(25,5)  COMMENT '剩余罚息',
  `overdue_principal`                             decimal(25,5)  COMMENT '逾期本金',
  `overdue_interest`                              decimal(25,5)  COMMENT '逾期利息',
  `overdue_svc_fee`                               decimal(25,5)  COMMENT '逾期服务费',
  `overdue_term_fee`                              decimal(25,5)  COMMENT '逾期手续费',
  `overdue_penalty`                               decimal(25,5)  COMMENT '逾期罚息',
  `overdue_mult_amt`                              decimal(25,5)  COMMENT '逾期滞纳金',
  `overdue_date_first`                            string         COMMENT '首次逾期日期',
  `overdue_date_start`                            string         COMMENT '逾期起始日期',
  `overdue_days`                                  decimal(5,0)   COMMENT '逾期天数',
  `overdue_date`                                  string         COMMENT '逾期日期',
  `dpd_begin_date`                                string         COMMENT 'DPD起始日期',
  `dpd_days`                                      decimal(4,0)   COMMENT 'DPD天数',
  `dpd_days_count`                                decimal(4,0)   COMMENT '累计DPD天数',
  `dpd_days_max`                                  decimal(4,0)   COMMENT '历史最大DPD天数',
  `collect_out_date`                              string         COMMENT '出催日期',
  `overdue_term`                                  decimal(3,0)   COMMENT '当前逾期期数',
  `overdue_terms_count`                           decimal(3,0)   COMMENT '累计逾期期数',
  `overdue_terms_max`                             decimal(3,0)   COMMENT '历史单次最长逾期期数',
  `overdue_principal_accumulate`                  decimal(25,5)  COMMENT '累计逾期本金',
  `overdue_principal_max`                         decimal(25,5)  COMMENT '历史最大逾期本金'
) COMMENT '借据信息表'
PARTITIONED BY(`biz_date` string COMMENT '观察日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- DROP TABLE IF EXISTS `dm_eagle${db_suffix}.eagle_repay_schedule`;
CREATE TABLE IF NOT EXISTS `dm_eagle${db_suffix}.eagle_repay_schedule`(
  `due_bill_no`                                   string         COMMENT '借据编号',
  `loan_active_date`                              string         COMMENT '激活日期—借据生成时间',
  `loan_init_principal`                           decimal(25,5)  COMMENT '贷款本金',
  `loan_init_term`                                decimal(3,0)   COMMENT '贷款期数（3、6、9等）',
  `loan_term`                                     decimal(3,0)   COMMENT '当前期数',
  `start_interest_date`                           string         COMMENT '起息日期',
  `should_repay_date`                             string         COMMENT '应还日期',         -- 对应 pmt_due_date 字段
  `should_repay_date_history`                     string         COMMENT '修改前的应还日期', -- 对应 pmt_due_date 的上一次日期 字段
  `grace_date`                                    string         COMMENT '宽限日期',
  `should_repay_amount`                           decimal(25,5)  COMMENT '应还金额',
  `should_repay_principal`                        decimal(25,5)  COMMENT '应还本金',
  `should_repay_interest`                         decimal(25,5)  COMMENT '应还利息',
  `should_repay_term_fee`                         decimal(25,5)  COMMENT '应还手续费',
  `should_repay_svc_fee`                          decimal(25,5)  COMMENT '应还服务费',
  `should_repay_penalty`                          decimal(25,5)  COMMENT '应还罚息',
  `should_repay_mult_amt`                         decimal(25,5)  COMMENT '应还滞纳金',
  `paid_out_type`                                 string         COMMENT '还款计划状态（英文原值）（N：正常，O：逾期，F：已还清）',
  `repaid_num`                                    decimal(20,0)  COMMENT '实还笔数',
  `paid_amount`                                   decimal(25,5)  COMMENT '已还金额',
  `paid_principal`                                decimal(25,5)  COMMENT '已还本金',
  `paid_interest`                                 decimal(25,5)  COMMENT '已还利息',
  `paid_term_fee`                                 decimal(25,5)  COMMENT '已还手续费',
  `paid_svc_fee`                                  decimal(25,5)  COMMENT '已还服务费',
  `paid_penalty`                                  decimal(25,5)  COMMENT '已还罚息',
  `paid_mult`                                     decimal(25,5)  COMMENT '已还滞纳金',
  `reduce_amount`                                 decimal(25,5)  COMMENT '减免金额',
  `reduce_principal`                              decimal(25,5)  COMMENT '减免本金',
  `reduce_interest`                               decimal(25,5)  COMMENT '减免利息',
  `reduce_term_fee`                               decimal(25,5)  COMMENT '减免手续费',
  `reduce_svc_fee`                                decimal(25,5)  COMMENT '减免服务费',
  `reduce_penalty`                                decimal(25,5)  COMMENT '减免罚息',
  `reduce_mult_amt`                               decimal(25,5)  COMMENT '减免滞纳金',
  `schedule_id`                                   string         COMMENT '还款计划编号',
  `s_d_date`                                      string         COMMENT 'ods层起始日期',
  `e_d_date`                                      string         COMMENT 'ods层结束日期'
) COMMENT '还款计划表'
PARTITIONED BY(`product_id` string COMMENT '产品编号')
STORED AS PARQUET
TBLPROPERTIES('parquet.compression'='SNAPPY');


-- DROP TABLE IF EXISTS `dm_eagle${db_suffix}.eagle_repay_detail`;
CREATE TABLE IF NOT EXISTS `dm_eagle${db_suffix}.eagle_repay_detail`(
  `due_bill_no`                                   string         COMMENT '借据号',
  `loan_active_date`                              string         COMMENT '放款日期',
  `loan_init_term`                                decimal(3,0)   COMMENT '贷款期数（3、6、9等）',
  `repay_term`                                    decimal(3,0)   COMMENT '实还期数',
  `order_id`                                      string         COMMENT '订单号',
  `loan_status`                                   string         COMMENT '借据状态（英文原值）（N：正常，O：逾期，F：已还清）',
  `loan_status_cn`                                string         COMMENT '借据状态（汉语解释）',
  `overdue_days`                                  decimal(4,0)   COMMENT '逾期天数',
  `payment_id`                                    string         COMMENT '实还流水号',
  `txn_time`                                      timestamp      COMMENT '交易时间',
  `post_time`                                     timestamp      COMMENT '入账时间',
  `bnp_type`                                      string         COMMENT '还款成分（英文原值）（Pricinpal：本金，Interest：利息，Penalty：罚息，Mulct：罚金，Compound：复利，CardFee：年费，OverLimitFee：超限费，LatePaymentCharge：滞纳金，NSFCharge：资金不足罚金，TXNFee：交易费，TERMFee：手续费，SVCFee：服务费，LifeInsuFee：寿险计划包费）',
  `bnp_type_cn`                                   string         COMMENT '还款成分（汉语解释）',
  `repay_amount`                                  decimal(25,5)  COMMENT '还款金额',
  `batch_date`                                    string         COMMENT '批量日期',
  `create_time`                                   timestamp      COMMENT '创建时间',
  `update_time`                                   timestamp      COMMENT '更新时间'
) COMMENT '实还明细表'
PARTITIONED BY(`biz_date` string COMMENT '实还日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- DROP TABLE IF EXISTS `dm_eagle${db_suffix}.eagle_order_info`;
CREATE TABLE IF NOT EXISTS `dm_eagle${db_suffix}.eagle_order_info`(
  `order_id`                                      string         COMMENT '订单编号',
  `ori_order_id`                                  string         COMMENT '原订单编号',
  `apply_no`                                      string         COMMENT '申请件编号',
  `due_bill_no`                                   string         COMMENT '借据号',
  `term`                                          decimal(3,0)   COMMENT '处理期数',
  `channel_id`                                    string         COMMENT '服务渠道编号（VISA：VISA，MC：MC，JCB：JCB，CUP：CUP，AMEX：AMEX，BANK：本行，ICL：ic系统，THIR：第三方，SUNS：阳光，AG：客服）',
  `pay_channel`                                   string         COMMENT '支付渠道',
  `command_type`                                  string         COMMENT '支付指令类型（SPA：单笔代付，SDB：单笔代扣，QSP：单笔代付查询，QSD：单笔代扣查询，BDB：批量代扣，BDA：批量代付）',
  `order_status`                                  string         COMMENT '订单状态（C：已提交，P：待提交，Q：待审批，W：处理中，S：已完成，V：已失效，E：失败，T：超时，R：已重提，G：拆分处理中，D：拆分已完成，B：撤销，X：已受理待入账）',
  `order_time`                                    timestamp      COMMENT '订单时间',
  `repay_serial_no`                               string         COMMENT '还款流水号',
  `service_id`                                    string         COMMENT '交易服务码',
  `assign_repay_ind`                              string         COMMENT '指定余额成分还款标志（Y：是，N：否）',
  `repay_way`                                     string         COMMENT '还款方式（ONLINE：线上，OFFLINE：线下）',
  `txn_type`                                      string         COMMENT '交易类型（Inq：查询，Cash：取现，AgentDebit：付款，Loan：分期，Auth：消费，PreAuth：预授权，PAComp：预授权完成，Load：圈存，Credit：存款，AgentCredit：收款，TransferCredit：转入，TransferDeditDepos：转出，AdviceSettle：结算通知，BigAmountLoan：大）',
  `txn_amt`                                       decimal(25,5)  COMMENT '交易金额',
  `original_txn_amt`                              decimal(25,5)  COMMENT '原始交易金额',
  `success_amt`                                   decimal(25,5)  COMMENT '成功金额',
  `currency`                                      string         COMMENT '币种',
  `code`                                          string         COMMENT '状态码',
  `message`                                       string         COMMENT '描述',
  `response_code`                                 string         COMMENT '对外返回码',
  `response_message`                              string         COMMENT '对外返回描述',
  `business_date`                                 string         COMMENT '业务日期',
  `send_time`                                     string         COMMENT '发送时间',
  `opt_datetime`                                  timestamp      COMMENT '更新时间',
  `setup_date`                                    string         COMMENT '创建日期',
  `loan_usage`                                    string         COMMENT '贷款用途（M：预约提前结清扣款，L：放款申请，R：退货，O：逾期扣款，W：赎回结清，I：强制结清扣款，X：账务调整，N：提前还当期）',
  `purpose`                                       string         COMMENT '支付用途',
  `online_flag`                                   string         COMMENT '联机标识（Y：是，N：否）',
  `online_allow`                                  string         COMMENT '允许联机标识（Y：是，N：否）',
  `order_pay_no`                                  string         COMMENT '支付流水号',
  `bank_trade_no`                                 string         COMMENT '银行交易流水号',
  `bank_trade_time`                               string         COMMENT '线下银行订单交易时间',
  `bank_trade_act_no`                             string         COMMENT '银行付款账号',
  `bank_trade_act_name`                           string         COMMENT '银行付款账户名称',
  `bank_trade_act_phone`                          string         COMMENT '银行预留手机号',
  `service_sn`                                    string         COMMENT '流水号',
  `outer_no`                                      string         COMMENT '外部凭证号',
  `confirm_flag`                                  string         COMMENT '确认标志',
  `txn_time`                                      timestamp      COMMENT '交易时间',
  `txn_date`                                      string         COMMENT '交易日期',
  `capital_plan_no`                               string         COMMENT '资金计划编号',
  `memo`                                          string         COMMENT '备注',
  `create_time`                                   timestamp      COMMENT '创建时间',
  `update_time`                                   timestamp      COMMENT '更新时间'
) COMMENT '订单流水表'
PARTITIONED BY(`biz_date` string COMMENT '交易日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- DROP TABLE IF EXISTS `dm_eagle${db_suffix}.eagle_repayment_record_day`;
CREATE TABLE IF NOT EXISTS `dm_eagle${db_suffix}.eagle_repayment_record_day`(
  `capital_id`                                    string         COMMENT '资金方编号',
  `channel_id`                                    string         COMMENT '渠道方编号',
  `project_id`                                    string         COMMENT '项目编号',
  `loan_terms`                                    decimal(3,0)   COMMENT '贷款期数',
  `due_bill_no`                                   string         COMMENT '借据编号',
  `repaid_term`                                   decimal(3,0)   COMMENT '还款期次',
  `txn_date`                                      string         COMMENT '实还日期',
  `paid_amount`                                   decimal(25,5)  COMMENT '实还金额',
  `paid_principal`                                decimal(25,5)  COMMENT '实还本金',
  `paid_interest`                                 decimal(25,5)  COMMENT '实还利息',
  `paid_term_svc_fee`                             decimal(25,5)  COMMENT '实还费用',
  `paid_penalty`                                  decimal(25,5)  COMMENT '实还罚息',
  `remain_principal`                              decimal(25,5)  COMMENT '本金余额',
  `paid_out_type`                                 string         COMMENT '账户状态'
) COMMENT '还款记录'
PARTITIONED BY (`biz_date` string COMMENT '实还日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;



















-- 运营平台报表建表语句
-- 5张明细表
-- 借据台账-明细 （取最新的数据）
-- DROP TABLE IF EXISTS `dm_eagle.operation_loan_ledger_detail`;
CREATE TABLE IF NOT EXISTS `dm_eagle.operation_loan_ledger_detail`(
  `channel_id`                                    string         COMMENT '渠道编号',
  `project_id`                                    string         COMMENT '项目编号',
  `contract_no`                                   string         COMMENT '合同编号',
  `due_bill_no`                                   string         COMMENT '借据编号',
  `customer_name`                                 string         COMMENT '客户姓名',
  `loan_init_principal`                           decimal(25,5)  COMMENT '合同金额',
  `loan_active_date`                              string         COMMENT '借款生效日',
  `debt_conversion_date`                          string         COMMENT '债转日期',
  `cycle_day`                                     decimal(2,0)   COMMENT '还款日',
  `loan_type`                                     string         COMMENT '还款方式',
  `loan_init_interest_rate`                       decimal(30,10) COMMENT '利率',
  `loan_init_penalty_rate`                        decimal(30,10) COMMENT '罚息率',
  `loan_init_fee_rate`                            decimal(30,10) COMMENT '费率',
  `loan_expire_date`                              string         COMMENT '借款到期日',
  `loan_status`                                   string         COMMENT '借据状态',
  `paid_out_date`                                 string         COMMENT '借款结清日',
  `loan_init_term`                                decimal(3,0)   COMMENT '借款期数',
  `loan_term_remain`                              decimal(3,0)   COMMENT '剩余期数',
  `overdue_days`                                  decimal(5,0)   COMMENT '逾期天数',
  `remain_amount`                                 decimal(25,5)  COMMENT '剩余未还金额',
  `remain_principal`                              decimal(25,5)  COMMENT '剩余未还本金',
  `remain_interest`                               decimal(25,5)  COMMENT '剩余未还利息',
  `remain_fee`                                    decimal(25,5)  COMMENT '剩余未还费用',
  `remain_penalty_interest`                       decimal(25,5)  COMMENT '剩余未还罚息',
  `overdue_amount`                                decimal(25,5)  COMMENT '逾期金额',
  `overdue_principal`                             decimal(25,5)  COMMENT '逾期本金',
  `overdue_interest`                              decimal(25,5)  COMMENT '逾期利息',
  `overdue_fee`                                   decimal(25,5)  COMMENT '逾期费用',
  `overdue_penalty`                               decimal(25,5)  COMMENT '逾期罚息',
  `paid_amount`                                   decimal(25,5)  COMMENT '已还金额',
  `paid_principal`                                decimal(25,5)  COMMENT '已还本金',
  `paid_interest`                                 decimal(25,5)  COMMENT '已还利息',
  `paid_fee`                                      decimal(25,5)  COMMENT '已还费用',
  `paid_penalty`                                  decimal(25,5)  COMMENT '已还罚息',
  `execution_date`                                string         COMMENT '跑批日期'
) COMMENT '借据台账-明细'
PARTITIONED BY(product_id string COMMENT '产品编号')
STORED AS PARQUET;


-- 应还款明细报表
-- DROP TABLE IF EXISTS `dm_eagle.operation_should_repay_detail`;
CREATE TABLE IF NOT EXISTS `dm_eagle.operation_should_repay_detail`(
  `channel_id`                                    string         COMMENT '合同渠道方',
  `project_id`                                    string         COMMENT '项目名称',
  `contract_no`                                   string         COMMENT '合同编号',
  `due_bill_no`                                   string         COMMENT '借据编号',
  `loan_active_date`                              string         COMMENT '借据生效日',
  `customer_name`                                 string         COMMENT '客户姓名',
  `loan_init_term`                                decimal(3,0)   COMMENT '总期数',
  `should_repay_date`                             string         COMMENT '应还日期',
  `loan_term`                                     decimal(3,0)   COMMENT '期次',
  `should_repay_amount`                           decimal(25,5)  COMMENT '应还金额',
  `should_repay_principal`                        decimal(25,5)  COMMENT '应还本金',
  `should_repay_interest`                         decimal(25,5)  COMMENT '应还利息',
  `should_repay_fee`                              decimal(25,5)  COMMENT '应还费用',
  `should_repay_penalty`                          decimal(25,5)  COMMENT '应还罚息',
  `execution_date`                                string         COMMENT '跑批日期'
) COMMENT '应还款明细报表'
PARTITIONED BY(product_id string COMMENT '产品编号')
STORED AS PARQUET;


-- 日还款明细报表
-- DROP TABLE IF EXISTS `dm_eagle.operation_daily_repay_detail`;
CREATE TABLE IF NOT EXISTS `dm_eagle.operation_daily_repay_detail`(
  `channel_id`                                    string         COMMENT '合同渠道方',
  `project_id`                                    string         COMMENT '项目名称',
  `contract_no`                                   string         COMMENT '合同编号',
  `due_bill_no`                                   string         COMMENT '借据编号',
  `schedule_status`                               string         COMMENT '借据状态',
  `loan_active_date`                              string         COMMENT '借据生效日',
  `loan_expire_date`                              string         COMMENT '借据到期日',
  `customer_name`                                 string         COMMENT '客户姓名',
  `loan_init_principal`                           decimal(25,5)  COMMENT '借款金额',
  `loan_init_term`                                decimal(3,0)   COMMENT '总期数',
  `loan_term`                                     decimal(3,0)   COMMENT '还款期次',
  `should_repay_date`                             string         COMMENT '应还日期',
  `biz_date`                                      string         COMMENT '实还日期',
  `paid_out_date`                                 string         COMMENT '核销日期',
  `fund_flow_date`                                string         COMMENT '资金流日期',
  `fund_flow_status`                              string         COMMENT '资金流状态',
  `overdue_days`                                  decimal(5,0)   COMMENT '逾期天数',
  `repay_way`                                     string         COMMENT '还款类型',
  `paid_amount`                                   decimal(25,5)  COMMENT '还款金额',
  `paid_principal`                                decimal(25,5)  COMMENT '还款本金',
  `paid_interest`                                 decimal(25,5)  COMMENT '还款利息',
  `paid_fee`                                      decimal(25,5)  COMMENT '还款费用',
  `paid_penalty`                                  decimal(25,5)  COMMENT '还款罚息',
  `execution_date`                                string         COMMENT '跑批日期'
) COMMENT '日还款明细报表'
PARTITIONED BY(product_id string COMMENT '产品编号')
STORED AS PARQUET;


--运营报表实还明细临时表
-- DROP TABLE IF EXISTS `dm_eagle.operation_daily_repay_detail_temp`;
CREATE TABLE IF NOT EXISTS `dm_eagle.operation_daily_repay_detail_temp`(
  `channel_id`                                    string         COMMENT '合同渠道方',
  `project_id`                                    string         COMMENT '项目名称',
  `contract_no`                                   string         COMMENT '合同编号',
  `due_bill_no`                                   string         COMMENT '借据编号',
  `schedule_status`                               string         COMMENT '借据状态',
  `loan_active_date`                              string         COMMENT '借据生效日',
  `loan_expire_date`                              string         COMMENT '借据到期日',
  `customer_name`                                 string         COMMENT '客户姓名',
  `loan_init_principal`                           decimal(25,5)  COMMENT '借款金额',
  `loan_init_term`                                decimal(3,0)   COMMENT '总期数',
  `loan_term`                                     decimal(3,0)   COMMENT '还款期次',
  `should_repay_date`                             string         COMMENT '应还日期',
  `biz_date`                                      string         COMMENT '实还日期',
  `paid_out_date`                                 string         COMMENT '核销日期',
  `fund_flow_date`                                string         COMMENT '资金流日期',
  `fund_flow_status`                              string         COMMENT '资金流状态',
  `overdue_days`                                  decimal(5,0)   COMMENT '逾期天数',
  `repay_way`                                     string         COMMENT '还款类型',
  `paid_amount`                                   decimal(25,5)  COMMENT '还款金额',
  `paid_principal`                                decimal(25,5)  COMMENT '还款本金',
  `paid_interest`                                 decimal(25,5)  COMMENT '还款利息',
  `paid_fee`                                      decimal(25,5)  COMMENT '还款费用',
  `paid_penalty`                                  decimal(25,5)  COMMENT '还款罚息',
  `execution_date`                                string         COMMENT '跑批日期',
  `product_id`                                    string         COMMENT '产品编号'
) COMMENT '运营报表实还明细临时表'
STORED AS PARQUET;


-- 逾期明细报表  Overdue details
-- DROP TABLE IF EXISTS `dm_eagle.operation_overdue_datail`;
CREATE TABLE IF NOT EXISTS `dm_eagle.operation_overdue_datail`(
  `channel_id`                                    string         COMMENT '合作渠道方',
  `project_id`                                    string         COMMENT '项目名称',
  `due_bill_no`                                   string         COMMENT '借据编号',
  `contract_no`                                   string         COMMENT '合同编号',
  `loan_active_date`                              string         COMMENT '借据生效日',
  `loan_expire_date`                              string         COMMENT '借据到期日',
  `loan_init_principal`                           decimal(25,5)  COMMENT '借款金额',
  `cycle_day`                                     decimal(2,0)   COMMENT '还款日',
  `curr_overdue_stage`                            string         COMMENT '当前逾期阶段',
  `overdue_date_first`                            string         COMMENT '首次逾期起始日期',
  `overdue_date_start`                            string         COMMENT '当前逾期起始日期',
  `overdue_terms_count`                           decimal(3,0)   COMMENT '累计逾期期数',
  `overdue_term`                                  decimal(3,0)   COMMENT '当前逾期期数',
  `overdue_days`                                  decimal(3,0)   COMMENT '当前逾期天数',
  `overdue_amount`                                decimal(25,5)  COMMENT '逾期金额',
  `overdue_principal`                             decimal(25,5)  COMMENT '逾期本金',
  `overdue_interest`                              decimal(25,5)  COMMENT '逾期利息',
  `overdue_fee`                                   decimal(25,5)  COMMENT '逾期费用',
  `overdue_penalty`                               decimal(25,5)  COMMENT '逾期罚息',
  `remain_amount`                                 decimal(25,5)  COMMENT '剩余未还金额',
  `remain_principal`                              decimal(25,5)  COMMENT '剩余未还本金',
  `remain_interest`                               decimal(25,5)  COMMENT '剩余未还利息',
  `remain_fee`                                    decimal(25,5)  COMMENT '剩余未还费用',
  `remain_penalty_interest`                       decimal(25,5)  COMMENT '剩余未还罚息'
) COMMENT '逾期明细报表'
PARTITIONED BY(product_id string COMMENT '产品编号',snapshot_day string COMMENT '快照日')
STORED AS PARQUET;

-- DROP TABLE IF EXISTS `dm_eagle.operation_overdue_base_datail`;
CREATE TABLE IF NOT EXISTS `dm_eagle.operation_overdue_base_datail`(
  `channel_id`                                    string         COMMENT '合作渠道方',
  `project_id`                                    string         COMMENT '项目id',
  `product_name`                                  string         COMMENT '产品名称',
  `project_name`                                  string         COMMENT '项目名称',
  `due_bill_no`                                   string         COMMENT '借据编号',
  `contract_no`                                   string         COMMENT '合同编号',
  `loan_active_date`                              string         COMMENT '借据生效日',
  `loan_expire_date`                              string         COMMENT '借据到期日',
  `loan_init_principal`                           decimal(25,5)  COMMENT '借款金额',
  `cycle_day`                                     decimal(2,0)   COMMENT '还款日',
  `curr_overdue_stage`                            string         COMMENT '当前逾期阶段',
  `overdue_date_first`                            string         COMMENT '首次逾期起始日期',
  `overdue_date_start`                            string         COMMENT '当前逾期起始日期',
  `overdue_terms_count`                           decimal(3,0)   COMMENT '累计逾期期数',
  `overdue_term`                                  decimal(3,0)   COMMENT '当前逾期期数',
  `overdue_days`                                  decimal(3,0)   COMMENT '当前逾期天数',
  `overdue_amount`                                decimal(25,5)  COMMENT '逾期金额',
  `overdue_principal`                             decimal(25,5)  COMMENT '逾期本金',
  `overdue_interest`                              decimal(25,5)  COMMENT '逾期利息',
  `overdue_fee`                                   decimal(25,5)  COMMENT '逾期费用',
  `overdue_penalty`                               decimal(25,5)  COMMENT '逾期罚息',
  `remain_amount`                                 decimal(25,5)  COMMENT '剩余未还金额',
  `remain_principal`                              decimal(25,5)  COMMENT '剩余未还本金',
  `remain_interest`                               decimal(25,5)  COMMENT '剩余未还利息',
  `remain_fee`                                    decimal(25,5)  COMMENT '剩余未还费用',
  `remain_penalty_interest`                       decimal(25,5)  COMMENT '剩余未还罚息',
  `remain_amount_cps`                             decimal(25,5)  COMMENT '剩余未还金额(代偿后)',
  `remain_principal_cps`                          decimal(25,5)  COMMENT '剩余未还本金(代偿后)',
  `remain_interest_cps`                           decimal(25,5)  COMMENT '剩余未还利息(代偿后)',
  `remain_fee_cps`                                decimal(25,5)  COMMENT '剩余未还费用(代偿后)',
  `remain_penalty_interest_cps`                   decimal(25,5)  COMMENT '剩余未还罚息(代偿后)',
  `is_buy_back`                                   string         COMMENT '是否回购:Y - 是 N - 否'
) COMMENT '逾期底层报表(场景方数据)'
PARTITIONED BY(product_id string COMMENT '产品编号',snapshot_day string COMMENT '快照日')
STORED AS PARQUET;


-- 退车退票明细表
-- DROP TABLE IF EXISTS `dm_eagle.operation_return_ticket_detail`;
CREATE TABLE IF NOT EXISTS `dm_eagle.operation_return_ticket_detail`(
  `channel_id`                                    string         COMMENT '合同渠道方',
  `project_id`                                    string         COMMENT '项目名称',
  `due_bill_no`                                   string         COMMENT '借据编号',
  `contract_no`                                   string         COMMENT '合同编号',
  `loan_active_date`                              string         COMMENT '借据生效日期',
  `debt_conversion_date`                          string         COMMENT '债转日期',
  `txn_date`                                      string         COMMENT '发生日期',
  `information_flow_date`                         string         COMMENT '信息流日期',
  `cash_flow_date`                                string         COMMENT '资金流日期',
  `cash_flow_status`                              string         COMMENT '资金流状态',
  `customer_name`                                 string         COMMENT '客户姓名',
  `loan_init_principal`                           decimal(25,5)  COMMENT '放款金额',
  `success_amt`                                   decimal(25,5)  COMMENT '退票/退车金额',
  `loan_usage`                                    string         COMMENT '退票/退车状态',
  `execution_date`                                string         COMMENT '跑批日期'
) COMMENT '退车退票明细表'
PARTITIONED BY(product_id string COMMENT '产品编号')
STORED AS PARQUET;


-- 汇总表#
-- 产品管理报表
-- DROP TABLE IF EXISTS `dm_eagle.operation_product_management_report_agg`;
CREATE TABLE IF NOT EXISTS `dm_eagle.operation_product_management_report_agg`(
  `channel_id`                                    string         COMMENT '合作渠道方',
  `project_id`                                    string         COMMENT '项目名称',
  `loan_num_count`                                decimal(12,0)  COMMENT '累计放款笔数',
  `loan_principal_count`                          decimal(25,5)  COMMENT '累计放款金额',
  `cumulative_repaid_amount`                      decimal(25,5)  COMMENT '累计已还金额',
  `cumulative_repaid_principal`                   decimal(25,5)  COMMENT '累计已还本金',
  `cumulative_repaid_interest`                    decimal(25,5)  COMMENT '累计已还利息',
  `cumulative_repaid_fee`                         decimal(25,5)  COMMENT '累计已还费用',
  `cumulative_repaid_penalty`                     decimal(25,5)  COMMENT '累计已还罚息',
  `assets_number_on_loan`                         decimal(12,0)  COMMENT '在贷资产笔数',
  `remain_amount_sum`                             decimal(25,5)  COMMENT '在贷余额',
  `remain_principal_sum`                          decimal(25,5)  COMMENT '在贷本金余额',
  `remain_interest_sum`                           decimal(25,5)  COMMENT '在贷利息余额',
  `overdue_assets_number`                         decimal(12,0)  COMMENT '逾期资产笔数',
  `current_overdue_amount`                        decimal(25,5)  COMMENT '当前逾期金额',
  `current_overdue_principal`                     decimal(25,5)  COMMENT '当前逾期本金',
  `current_overdue_interest`                      decimal(25,5)  COMMENT '当前逾期利息',
  `current_overdue_fee`                           decimal(25,5)  COMMENT '当前逾期费用',
  `current_overdue_penalty`                       decimal(25,5)  COMMENT '当前逾期罚息'
) COMMENT '产品管理报表'
PARTITIONED BY(`biz_date` string COMMENT '观察日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 放款日报
-- DROP TABLE IF EXISTS `dm_eagle.operation_lending_daily_agg`;
CREATE TABLE IF NOT EXISTS `dm_eagle.operation_lending_daily_agg` (
  `channel_id`                                    string         COMMENT '合作渠道方',
  `project_id`                                    string         COMMENT '项目名称',
  `biz_date`                                      string         COMMENT '放款日期',
  `loan_num`                                      decimal(25,5)  COMMENT '放款日放款笔数',
  `loan_principal`                                decimal(25,5)  COMMENT '放款日放款金额',
  `loan_num_count`                                decimal(12,0)  COMMENT '累计放款笔数',
  `loan_principal_count`                          decimal(25,5)  COMMENT '累计放款金额',
  `execution_date`                                string         COMMENT '跑批日期'
) COMMENT '放款日报'
PARTITIONED BY(`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 应还日报表
-- DROP TABLE IF EXISTS `dm_eagle.operation_should_repay_day_agg`;
CREATE TABLE IF NOT EXISTS `dm_eagle.operation_should_repay_day_agg`(
  `channel_id`                                    string         COMMENT '合同渠道方',
  `project_id`                                    string         COMMENT '项目名称',
  `should_repay_date`                             string         COMMENT '应还日期',
  `should_repay_amount`                           decimal(25,5)  COMMENT '应还金额',
  `should_repay_principal`                        decimal(25,5)  COMMENT '应还本金',
  `should_repay_interest`                         decimal(25,5)  COMMENT '应还利息',
  `should_repay_fee`                              decimal(25,5)  COMMENT '应还费用',
  `should_repay_penalty`                          decimal(25,5)  COMMENT '应还罚息',
  `should_repay_amount_unpaid`                    decimal(25,5)  COMMENT '应还金额(未结清)',
  `should_repay_principal_unpaid`                 decimal(25,5)  COMMENT '应还本金(未结清)',
  `should_repay_interest_unpaid`                  decimal(25,5)  COMMENT '应还利息(未结清)',
  `should_repay_fee_unpaid`                       decimal(25,5)  COMMENT '应还费用(未结清)',
  `should_repay_penalty_unpaid`                   decimal(25,5)  COMMENT '应还罚息(未结清)'
) COMMENT '应还日报表'
PARTITIONED BY(`biz_date` string COMMENT '观察日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 还款日报
-- DROP TABLE IF EXISTS `dm_eagle.operation_daily_repay_agg`;
CREATE TABLE IF NOT EXISTS `dm_eagle.operation_daily_repay_agg`(
  `channel_id`                                    string         COMMENT '合同渠道方',
  `project_id`                                    string         COMMENT '项目名称',
  `biz_date`                                      string         COMMENT '实还日期',
  `paid_out_date`                                 string         COMMENT '核销日期',
  `repay_way`                                     string         COMMENT '还款类型',
  `repay_num`                                     decimal(12,0)  COMMENT '还款笔数',
  `paid_amount`                                   decimal(25,5)  COMMENT '还款金额',
  `paid_principal`                                decimal(25,5)  COMMENT '还款本金',
  `paid_interest`                                 decimal(25,5)  COMMENT '还款利息',
  `paid_fee`                                      decimal(25,5)  COMMENT '还款费用',
  `paid_penalty`                                  decimal(25,5)  COMMENT '还款罚息',
  `execution_date`                                string         COMMENT '跑批日期'
) COMMENT '还款日报'
PARTITIONED BY(`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 退票报表
-- DROP TABLE IF EXISTS `dm_eagle.operation_return_ticket_agg`;
CREATE TABLE IF NOT EXISTS `dm_eagle.operation_return_ticket_agg`(
  `channel_id`                                    string         COMMENT '合同渠道方',
  `project_id`                                    string         COMMENT '项目名称',
  `txn_date`                                      string         COMMENT '发生日期',
  `refund_amount`                                 decimal(25,5)  COMMENT '退票/退车金额',
  `refund_num`                                    decimal(12,0)  COMMENT '退票/退车笔数',
  `refund_amount_accumul`                         decimal(25,5)  COMMENT '累计退票/退车金额',
  `refund_num_accumul`                            decimal(12,0)  COMMENT '累计退票/退车笔数'
) COMMENT '退票报表'
PARTITIONED BY(`product_id` string COMMENT '产品编号')
STORED AS PARQUET;

















-- ABS系统

-- 资产总体信息（项目——所有包）
-- DROP TABLE IF EXISTS `dm_eagle.abs_asset_information_project`;
CREATE TABLE IF NOT EXISTS `dm_eagle.abs_asset_information_project`(
  -- basicAssetInformationVo
  `asset_count`                                   decimal(12,0)  COMMENT '资产数量',
  `customer_count`                                decimal(12,0)  COMMENT '借款人数量',
  `remain_principal`                              decimal(30,10) COMMENT '本金余额',
  `remain_interest`                               decimal(30,10) COMMENT '本息余额',
  `customer_remain_principal_avg`                 decimal(30,10) COMMENT '借款人平均本金余额',
  -- basicAmountFeaturesVo
  `remain_principal_max`                          decimal(30,10) COMMENT '单笔最大本金余额',
  `remain_principal_min`                          decimal(30,10) COMMENT '单笔最小本金余额',
  `remain_principal_avg`                          decimal(30,10) COMMENT '平均本金余额',
  `loan_principal_max`                            decimal(30,10) COMMENT '单笔最大合同金额',
  `loan_principal_min`                            decimal(30,10) COMMENT '单笔最小合同金额',
  `loan_principal_avg`                            decimal(30,10) COMMENT '平均合同金额',
  -- basicCharacteristicsVo
  `loan_terms_max`                                decimal(3,0)   COMMENT '单笔最大合同期数',
  `loan_terms_min`                                decimal(3,0)   COMMENT '单笔最小合同期数',
  `loan_terms_avg`                                decimal(30,10) COMMENT '平均合同期数',
  `loan_terms_avg_weighted`                       decimal(30,10) COMMENT '加权平均合同期数',
  `remain_term_max`                               decimal(3,0)   COMMENT '单笔最大剩余期数',
  `remain_term_min`                               decimal(3,0)   COMMENT '单笔最小剩余期数',
  `remain_term_avg`                               decimal(30,10) COMMENT '平均剩余期数',
  `remain_term_avg_weighted`                      decimal(30,10) COMMENT '加权平均剩余期数', -- basicAssetInformationVo 中也要
  `repaid_term_max`                               decimal(3,0)   COMMENT '单笔最大已还期数',
  `repaid_term_min`                               decimal(3,0)   COMMENT '单笔最小已还期数',
  `repaid_term_avg`                               decimal(30,10) COMMENT '平均已还期数',
  `repaid_term_avg_weighted`                      decimal(30,10) COMMENT '加权平均已还期数',
  `aging_max`                                     decimal(3,0)   COMMENT '单笔最大账龄',
  `aging_min`                                     decimal(3,0)   COMMENT '单笔最小账龄',
  `aging_avg`                                     decimal(30,10) COMMENT '平均账龄',
  `aging_avg_weighted`                            decimal(30,10) COMMENT '加权平均账龄',
  `remain_period_max`                             decimal(3,0)   COMMENT '单笔最大合同剩余期限（账龄相关）',
  `remain_period_min`                             decimal(3,0)   COMMENT '单笔最小合同剩余期限（账龄相关）',
  `remain_period_avg`                             decimal(30,10) COMMENT '平均合同剩余期限（账龄相关）',
  `remain_period_avg_weighted`                    decimal(30,10) COMMENT '加权平均合同剩余期限（账龄相关）',
  -- basicInterestRateVo
  `interest_rate_max`                             decimal(30,10) COMMENT '单笔最大年利率',
  `interest_rate_min`                             decimal(30,10) COMMENT '单笔最小年利率',
  `interest_rate_avg`                             decimal(30,10) COMMENT '平均年利率',
  `interest_rate_avg_weighted`                    decimal(30,10) COMMENT '加权平均年利率', -- basicAssetInformationVo 中也要
  -- basicBorrowerVo
  `age_max`                                       decimal(3,0)   COMMENT '最大年龄',
  `age_min`                                       decimal(3,0)   COMMENT '最小年龄',
  `age_avg`                                       decimal(30,10) COMMENT '平均年龄',
  `age_avg_weighted`                              decimal(30,10) COMMENT '加权平均年龄',
  `income_year_max`                               decimal(30,10) COMMENT '最大年收入',
  `income_year_min`                               decimal(30,10) COMMENT '最小年收入',
  `income_year_avg`                               decimal(30,10) COMMENT '平均年收入',
  `income_year_avg_weighted`                      decimal(30,10) COMMENT '加权平均年收入',
  `income_debt_ratio_max`                         decimal(30,10) COMMENT '最大收入债务比',
  `income_debt_ratio_min`                         decimal(30,10) COMMENT '最小收入债务比',
  `income_debt_ratio_avg`                         decimal(30,10) COMMENT '平均收入债务比',
  `income_debt_ratio_avg_weighted`                decimal(30,10) COMMENT '加权平均收入债务比',
  -- basicMortgagedVo
  `pledged_asset_balance`                         decimal(30,10) COMMENT '抵押的资产余额',
  `pledged_asset_count`                           decimal(12,0)  COMMENT '抵押的资产笔数',
  `pledged_asset_balance_ratio`                   decimal(30,10) COMMENT '抵押资产余额占比',
  `pledged_asset_count_ratio`                     decimal(30,10) COMMENT '抵押资产笔数占比',
  `pawn_value`                                    decimal(30,10) COMMENT '抵押初始评估价值',
  `pledged_asset_rate_avg_weighted`               decimal(30,10) COMMENT '加权平均抵押率', -- basicAssetInformationVo 中也要
  `is_allbag`                                     string         COMMENT '是否是所有包(y: 是 , n : 否)'
) COMMENT '资产总体信息（项目——所有包）'
PARTITIONED BY(`biz_date` string COMMENT '观察日期',`project_id` string COMMENT '项目编号')
STORED AS PARQUET;


-- 资产总体信息（包）
-- DROP TABLE IF EXISTS `dm_eagle.abs_asset_information_bag`;
CREATE TABLE IF NOT EXISTS `dm_eagle.abs_asset_information_bag`(
  `project_id`                                    string         COMMENT '项目编号',
  -- basicAssetInformationVo
  `asset_count`                                   decimal(12,0)  COMMENT '资产数量',
  `customer_count`                                decimal(12,0)  COMMENT '借款人数量',
  `remain_principal`                              decimal(30,10) COMMENT '本金余额',
  `remain_interest`                               decimal(30,10) COMMENT '本息余额',
  `customer_remain_principal_avg`                 decimal(30,10) COMMENT '借款人平均本金余额',
  -- basicAmountFeaturesVo
  `remain_principal_max`                          decimal(30,10) COMMENT '单笔最大本金余额',
  `remain_principal_min`                          decimal(30,10) COMMENT '单笔最小本金余额',
  `remain_principal_avg`                          decimal(30,10) COMMENT '平均本金余额',
  `loan_principal_max`                            decimal(30,10) COMMENT '单笔最大合同金额',
  `loan_principal_min`                            decimal(30,10) COMMENT '单笔最小合同金额',
  `loan_principal_avg`                            decimal(30,10) COMMENT '平均合同金额',
  -- basicCharacteristicsVo
  `loan_terms_max`                                decimal(3,0)   COMMENT '单笔最大合同期数',
  `loan_terms_min`                                decimal(3,0)   COMMENT '单笔最小合同期数',
  `loan_terms_avg`                                decimal(30,10) COMMENT '平均合同期数',
  `loan_terms_avg_weighted`                       decimal(30,10) COMMENT '加权平均合同期数',
  `remain_term_max`                               decimal(3,0)   COMMENT '单笔最大剩余期数',
  `remain_term_min`                               decimal(3,0)   COMMENT '单笔最小剩余期数',
  `remain_term_avg`                               decimal(30,10) COMMENT '平均剩余期数',
  `remain_term_avg_weighted`                      decimal(30,10) COMMENT '加权平均剩余期数', -- basicAssetInformationVo 中也要
  `repaid_term_max`                               decimal(3,0)   COMMENT '单笔最大已还期数',
  `repaid_term_min`                               decimal(3,0)   COMMENT '单笔最小已还期数',
  `repaid_term_avg`                               decimal(30,10) COMMENT '平均已还期数',
  `repaid_term_avg_weighted`                      decimal(30,10) COMMENT '加权平均已还期数',
  `aging_max`                                     decimal(3,0)   COMMENT '单笔最大账龄',
  `aging_min`                                     decimal(3,0)   COMMENT '单笔最小账龄',
  `aging_avg`                                     decimal(30,10) COMMENT '平均账龄',
  `aging_avg_weighted`                            decimal(30,10) COMMENT '加权平均账龄',
  `remain_period_max`                             decimal(3,0)   COMMENT '单笔最大合同剩余期限（账龄相关）',
  `remain_period_min`                             decimal(3,0)   COMMENT '单笔最小合同剩余期限（账龄相关）',
  `remain_period_avg`                             decimal(30,10) COMMENT '平均合同剩余期限（账龄相关）',
  `remain_period_avg_weighted`                    decimal(30,10) COMMENT '加权平均合同剩余期限（账龄相关）',
  -- basicInterestRateVo
  `interest_rate_max`                             decimal(30,10) COMMENT '单笔最大年利率',
  `interest_rate_min`                             decimal(30,10) COMMENT '单笔最小年利率',
  `interest_rate_avg`                             decimal(30,10) COMMENT '平均年利率',
  `interest_rate_avg_weighted`                    decimal(30,10) COMMENT '加权平均年利率', -- basicAssetInformationVo 中也要
  -- basicBorrowerVo
  `age_max`                                       decimal(3,0)   COMMENT '最大年龄',
  `age_min`                                       decimal(3,0)   COMMENT '最小年龄',
  `age_avg`                                       decimal(30,10) COMMENT '平均年龄',
  `age_avg_weighted`                              decimal(30,10) COMMENT '加权平均年龄',
  `income_year_max`                               decimal(30,10) COMMENT '最大年收入',
  `income_year_min`                               decimal(30,10) COMMENT '最小年收入',
  `income_year_avg`                               decimal(30,10) COMMENT '平均年收入',
  `income_year_avg_weighted`                      decimal(30,10) COMMENT '加权平均年收入',
  `income_debt_ratio_max`                         decimal(30,10) COMMENT '最大收入债务比',
  `income_debt_ratio_min`                         decimal(30,10) COMMENT '最小收入债务比',
  `income_debt_ratio_avg`                         decimal(30,10) COMMENT '平均收入债务比',
  `income_debt_ratio_avg_weighted`                decimal(30,10) COMMENT '加权平均收入债务比',
  -- basicMortgagedVo
  `pledged_asset_balance`                         decimal(30,10) COMMENT '抵押的资产余额',
  `pledged_asset_count`                           decimal(12,0)  COMMENT '抵押的资产笔数',
  `pledged_asset_balance_ratio`                   decimal(30,10) COMMENT '抵押资产余额占比',
  `pledged_asset_count_ratio`                     decimal(30,10) COMMENT '抵押资产笔数占比',
  `pawn_value`                                    decimal(30,10) COMMENT '抵押初始评估价值',
  `pledged_asset_rate_avg_weighted`               decimal(30,10) COMMENT '加权平均抵押率' -- basicAssetInformationVo 中也要
) COMMENT '资产总体信息（包）'
PARTITIONED BY(`biz_date` string COMMENT '观察日期',`bag_id` string COMMENT '包编号')
STORED AS PARQUET;


-- 资产总体信息（包、封包时）
-- DROP TABLE IF EXISTS `dm_eagle.abs_asset_information_bag_snapshot`;
CREATE TABLE IF NOT EXISTS `dm_eagle.abs_asset_information_bag_snapshot`(
  `project_id`                                    string         COMMENT '项目编号',
  -- basicAssetInformationVo
  `asset_count`                                   decimal(12,0)  COMMENT '资产数量',
  `customer_count`                                decimal(12,0)  COMMENT '借款人数量',
  `remain_principal`                              decimal(30,10) COMMENT '本金余额',
  `remain_interest`                               decimal(30,10) COMMENT '本息余额',
  `customer_remain_principal_avg`                 decimal(30,10) COMMENT '借款人平均本金余额',
  -- basicAmountFeaturesVo
  `remain_principal_max`                          decimal(30,10) COMMENT '单笔最大本金余额',
  `remain_principal_min`                          decimal(30,10) COMMENT '单笔最小本金余额',
  `remain_principal_avg`                          decimal(30,10) COMMENT '平均本金余额',
  `loan_principal_max`                            decimal(30,10) COMMENT '单笔最大合同金额',
  `loan_principal_min`                            decimal(30,10) COMMENT '单笔最小合同金额',
  `loan_principal_avg`                            decimal(30,10) COMMENT '平均合同金额',
  -- basicCharacteristicsVo
  `loan_terms_max`                                decimal(3,0)   COMMENT '单笔最大合同期数',
  `loan_terms_min`                                decimal(3,0)   COMMENT '单笔最小合同期数',
  `loan_terms_avg`                                decimal(30,10) COMMENT '平均合同期数',
  `loan_terms_avg_weighted`                       decimal(30,10) COMMENT '加权平均合同期数',
  `remain_term_max`                               decimal(3,0)   COMMENT '单笔最大剩余期数',
  `remain_term_min`                               decimal(3,0)   COMMENT '单笔最小剩余期数',
  `remain_term_avg`                               decimal(30,10) COMMENT '平均剩余期数',
  `remain_term_avg_weighted`                      decimal(30,10) COMMENT '加权平均剩余期数', -- basicAssetInformationVo 中也要
  `repaid_term_max`                               decimal(3,0)   COMMENT '单笔最大已还期数',
  `repaid_term_min`                               decimal(3,0)   COMMENT '单笔最小已还期数',
  `repaid_term_avg`                               decimal(30,10) COMMENT '平均已还期数',
  `repaid_term_avg_weighted`                      decimal(30,10) COMMENT '加权平均已还期数',
  `aging_max`                                     decimal(3,0)   COMMENT '单笔最大账龄',
  `aging_min`                                     decimal(3,0)   COMMENT '单笔最小账龄',
  `aging_avg`                                     decimal(30,10) COMMENT '平均账龄',
  `aging_avg_weighted`                            decimal(30,10) COMMENT '加权平均账龄',
  `remain_period_max`                             decimal(3,0)   COMMENT '单笔最大合同剩余期限（账龄相关）',
  `remain_period_min`                             decimal(3,0)   COMMENT '单笔最小合同剩余期限（账龄相关）',
  `remain_period_avg`                             decimal(30,10) COMMENT '平均合同剩余期限（账龄相关）',
  `remain_period_avg_weighted`                    decimal(30,10) COMMENT '加权平均合同剩余期限（账龄相关）',
  -- basicInterestRateVo
  `interest_rate_max`                             decimal(30,10) COMMENT '单笔最大年利率',
  `interest_rate_min`                             decimal(30,10) COMMENT '单笔最小年利率',
  `interest_rate_avg`                             decimal(30,10) COMMENT '平均年利率',
  `interest_rate_avg_weighted`                    decimal(30,10) COMMENT '加权平均年利率', -- basicAssetInformationVo 中也要
  -- basicBorrowerVo
  `age_max`                                       decimal(3,0)   COMMENT '最大年龄',
  `age_min`                                       decimal(3,0)   COMMENT '最小年龄',
  `age_avg`                                       decimal(30,10) COMMENT '平均年龄',
  `age_avg_weighted`                              decimal(30,10) COMMENT '加权平均年龄',
  `income_year_max`                               decimal(30,10) COMMENT '最大年收入',
  `income_year_min`                               decimal(30,10) COMMENT '最小年收入',
  `income_year_avg`                               decimal(30,10) COMMENT '平均年收入',
  `income_year_avg_weighted`                      decimal(30,10) COMMENT '加权平均年收入',
  `income_debt_ratio_max`                         decimal(30,10) COMMENT '最大收入债务比',
  `income_debt_ratio_min`                         decimal(30,10) COMMENT '最小收入债务比',
  `income_debt_ratio_avg`                         decimal(30,10) COMMENT '平均收入债务比',
  `income_debt_ratio_avg_weighted`                decimal(30,10) COMMENT '加权平均收入债务比',
  -- basicMortgagedVo
  `pledged_asset_balance`                         decimal(30,10) COMMENT '抵押的资产余额',
  `pledged_asset_count`                           decimal(12,0)  COMMENT '抵押的资产笔数',
  `pledged_asset_balance_ratio`                   decimal(30,10) COMMENT '抵押资产余额占比',
  `pledged_asset_count_ratio`                     decimal(30,10) COMMENT '抵押资产笔数占比',
  `pawn_value`                                    decimal(30,10) COMMENT '抵押初始评估价值',
  `pledged_asset_rate_avg_weighted`               decimal(30,10) COMMENT '加权平均抵押率' -- basicAssetInformationVo 中也要
) COMMENT '资产总体信息（包、封包时）'
PARTITIONED BY(`biz_date` string COMMENT '观察日期',`bag_id` string COMMENT '包编号')
STORED AS PARQUET;





-- 现金流分析（包、封包时）
-- DROP TABLE IF EXISTS `dm_eagle.abs_asset_information_cash_flow_bag_snapshot`;
CREATE TABLE IF NOT EXISTS `dm_eagle.abs_asset_information_cash_flow_bag_snapshot`(
  `project_id`                                    string         COMMENT '项目编号',
  `should_repay_date`                             string         COMMENT '应还日期',
  `remain_principal_term_begin`                   decimal(30,10) COMMENT '期初本金余额',
  `remain_principal_term_end`                     decimal(30,10) COMMENT '期末本金余额',
  `should_repay_amount`                           decimal(30,10) COMMENT '应还金额',
  `should_repay_principal`                        decimal(30,10) COMMENT '应还本金',
  `should_repay_interest`                         decimal(30,10) COMMENT '应还利息',
  `should_repay_cost`                             decimal(30,10) COMMENT '应还费用'
) COMMENT '现金流分析（包、封包时）'
PARTITIONED BY(`bag_id` string COMMENT '包编号')
STORED AS PARQUET;


-- 现金流分析（项目、所有包、包）
-- DROP TABLE IF EXISTS `dm_eagle.abs_asset_information_cash_flow_bag_day`;
CREATE TABLE IF NOT EXISTS `dm_eagle.abs_asset_information_cash_flow_bag_day`(
  `bag_date`                                      string         COMMENT '封包日期',
  `data_extraction_day`                           string         COMMENT '最新数据提取日',

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

  `collect_date`                                  string         COMMENT '统计日期'
) COMMENT '现金流分析（项目、所有包、包）'
PARTITIONED BY(`biz_date` string COMMENT '观察日期',`project_id` string COMMENT '项目编号',`bag_id` string COMMENT '包编号（包编号、default_project、default_all_bag）')
STORED AS PARQUET;





-- abs分布表（项目——所有包）
-- DROP TABLE IF EXISTS `dm_eagle.abs_asset_distribution_day`;
CREATE TABLE IF NOT EXISTS `dm_eagle.abs_asset_distribution_day`(
  `is_allbag`                                     string         COMMENT '是否是所有包(y: 是 , n : 否)',
  `asset_tab_name`                                decimal(4,0)   COMMENT '分布标签名称（1：未偿本金余额分布，2：资产利率分布，3：资产合同期限分布，4：资产剩余期限分布，5：资产已还期数分布，6：账龄分布，7：合同剩余期限分布（账龄相关），8：还款方式分布，9：借款人年龄分布，10：借款人行业分布，11：借款人年收入分布，12：借款人风控结果分布，13：借款人信用等级分布，14：借款人反欺诈等级分布，15：借款人资产等级分布，16：借款人地区分布，17：抵押率分布，18：车辆品牌分布，19：新旧车辆分布）',
  `asset_name`                                    string         COMMENT '分布项名称',
  `asset_name_order`                              decimal(4,0)   COMMENT '分布项名称排序',
  `remain_principal`                              decimal(25,5)  COMMENT '本金余额',
  `remain_principal_ratio`                        decimal(30,10) COMMENT '本金余额占比（分布项本金余额/本金余额）',
  `loan_num`                                      decimal(20,0)  COMMENT '借据笔数',
  `loan_numratio`                                 decimal(30,10) COMMENT '借据笔数占比（分布项借据笔数/借据笔数）',
  `remain_principal_loan_num_avg`                 decimal(25,5)  COMMENT '平均每笔余额（本金余额/借据笔数）'
) COMMENT 'abs分布表（项目——所有包）'
PARTITIONED BY(`biz_date` string COMMENT '观察日期',`project_id` string COMMENT '项目编号')
STORED AS PARQUET;


-- abs分布表（包）
-- DROP TABLE IF EXISTS `dm_eagle.abs_asset_distribution_bag_day`;
CREATE TABLE IF NOT EXISTS `dm_eagle.abs_asset_distribution_bag_day`(
  `project_id`                                    string         COMMENT '项目编号',
  `asset_tab_name`                                decimal(4,0)   COMMENT '分布标签名称（1：未偿本金余额分布，2：资产利率分布，3：资产合同期限分布，4：资产剩余期限分布，5：资产已还期数分布，6：账龄分布，7：合同剩余期限分布（账龄相关），8：还款方式分布，9：借款人年龄分布，10：借款人行业分布，11：借款人年收入分布，12：借款人风控结果分布，13：借款人信用等级分布，14：借款人反欺诈等级分布，15：借款人资产等级分布，16：借款人地区分布，17：抵押率分布，18：车辆品牌分布，19：新旧车辆分布）',
  `asset_name`                                    string         COMMENT '分布项名称',
  `asset_name_order`                              decimal(4,0)   COMMENT '分布项名称排序',
  `remain_principal`                              decimal(25,5)  COMMENT '本金余额',
  `remain_principal_ratio`                        decimal(30,10) COMMENT '本金余额占比（分布项本金余额/本金余额）',
  `loan_num`                                      decimal(20,0)  COMMENT '借据笔数',
  `loan_numratio`                                 decimal(30,10) COMMENT '借据笔数占比（分布项借据笔数/借据笔数）',
  `remain_principal_loan_num_avg`                 decimal(25,5)  COMMENT '平均每笔余额（本金余额/借据笔数）'
) COMMENT 'abs分布表（包）'
PARTITIONED BY(`biz_date` string COMMENT '观察日期',`bag_id` string COMMENT '包编号')
STORED AS PARQUET;


-- abs分布表（包、封包时）
-- DROP TABLE IF EXISTS `dm_eagle.abs_asset_distribution_snapshot_day`;
CREATE TABLE IF NOT EXISTS `dm_eagle.abs_asset_distribution_bag_snapshot_day`(
  `project_id`                                    string         COMMENT '项目编号',
  `asset_tab_name`                                decimal(4,0)   COMMENT '分布标签名称（1：未偿本金余额分布，2：资产利率分布，3：资产合同期限分布，4：资产剩余期限分布，5：资产已还期数分布，6：账龄分布，7：合同剩余期限分布（账龄相关），8：还款方式分布，9：借款人年龄分布，10：借款人行业分布，11：借款人年收入分布，12：借款人风控结果分布，13：借款人信用等级分布，14：借款人反欺诈等级分布，15：借款人资产等级分布，16：借款人地区分布，17：抵押率分布，18：车辆品牌分布，19：新旧车辆分布）',
  `asset_name`                                    string         COMMENT '分布项名称',
  `asset_name_order`                              decimal(4,0)   COMMENT '分布项名称排序',
  `remain_principal`                              decimal(25,5)  COMMENT '本金余额',
  `remain_principal_ratio`                        decimal(30,10) COMMENT '本金余额占比（分布项本金余额/本金余额）',
  `loan_num`                                      decimal(20,0)  COMMENT '借据笔数',
  `loan_numratio`                                 decimal(30,10) COMMENT '借据笔数占比（分布项借据笔数/借据笔数）',
  `remain_principal_loan_num_avg`                 decimal(25,5)  COMMENT '平均每笔余额（本金余额/借据笔数）'
) COMMENT 'abs分布表（包、封包时）'
PARTITIONED BY(`biz_date` string COMMENT '观察日期',`bag_id` string COMMENT '包编号')
STORED AS PARQUET;





-- 逾期情况监控
-- 各阶段本金余额/封包时本金余额
-- DROP TABLE IF EXISTS `dm_eagle.abs_overdue_rate_day`;
CREATE TABLE IF NOT EXISTS `dm_eagle.abs_overdue_rate_day`(
  `is_allbag`                                     string         COMMENT '是否是所有包（y：是，n：否）',
  `dpd`                                           string         COMMENT 'DPD时期（1+、7+、14+、30+、60+、90+、120+、150+、180+、1_7、8_15、15_30、31_60、61_90、91_120、121_150、151_180）',

  `remain_principal`                              decimal(25,5)  COMMENT '封包时本金余额',
  `overdue_remain_principal`                      decimal(25,5)  COMMENT '当前逾期借据本金余额',
  `overdue_remain_principal_new`                  decimal(25,5)  COMMENT '新增逾期借据本金余额',
  `overdue_remain_principal_once`                 decimal(25,5)  COMMENT '累计逾期借据本金余额',

  `bag_due_num`                                   decimal(20,0)  COMMENT '封包时借据笔数',
  `overdue_num`                                   decimal(20,0)  COMMENT '当前逾期借据笔数',
  `overdue_num_new`                               decimal(20,0)  COMMENT '新增逾期借据笔数',
  `overdue_num_once`                              decimal(20,0)  COMMENT '累计逾期借据笔数',

  `bag_due_person_num`                            decimal(20,0)  COMMENT '封包时借据人数',
  `overdue_person_num`                            decimal(20,0)  COMMENT '当前逾期借据人数',
  `overdue_person_num_new`                        decimal(20,0)  COMMENT '新增逾期借据人数',
  `overdue_person_num_once`                       decimal(20,0)  COMMENT '累计逾期借据人数'
) COMMENT '逾期情况监控（各阶段本金余额/封包时本金余额）'
PARTITIONED BY(`biz_date` string COMMENT '观察日期',`project_id` string COMMENT '项目编号',`bag_id` string COMMENT '包编号')
STORED AS PARQUET;


-- 逾期情况监控
-- 借据详情
-- DROP TABLE IF EXISTS `dm_eagle.abs_overdue_rate_details_day`;
CREATE TABLE IF NOT EXISTS `dm_eagle.abs_overdue_rate_details_day`(
  `serial_number`                                 string         COMMENT '借据号',
  `contract_no`                                   string         COMMENT '合同编号',
  `overdue_days`                                  decimal(5,0)   COMMENT '逾期天数',
  `type_curr`                                     string         COMMENT '是否命中当前（y：是，n：否）',
  `type_once`                                     string         COMMENT '是否命中累计（y：是，n：否）',
  `type_new`                                      string         COMMENT '是否命中新增（y：是，n：否）',
  `loan_terms`                                    decimal(3,0)   COMMENT '贷款期数',
  `loan_principal`                                decimal(25,5)  COMMENT '合同金额',
  `remain_term`                                   decimal(3,0)   COMMENT '剩余期数',
  `overdue_days_max`                              decimal(4,0)   COMMENT '最长单期逾期天数',
  `remain_principal`                              decimal(25,5)  COMMENT '当前本金余额（元）',
  `overdue_principal`                             decimal(25,5)  COMMENT '当前逾期本金（元）',
  `overdue_interest`                              decimal(25,5)  COMMENT '当前逾期利息（元）',
  `overdue_cost`                                  decimal(25,5)  COMMENT '当前逾期费用（元）'
) COMMENT '逾期情况详情（借据详情）'
PARTITIONED BY(`biz_date` string COMMENT '快照日',`project_id` string COMMENT '项目编号',`bag_id` string COMMENT '包编号')
STORED AS PARQUET;





-- 早偿监控汇总表
-- DROP TABLE IF EXISTS `dm_eagle.abs_early_payment_asset_statistic`;
CREATE TABLE IF NOT EXISTS `dm_eagle.abs_early_payment_asset_statistic`(
  `is_allBag`                                     string         COMMENT '是否所有包',
  `early_payment_principal`                       decimal(25,5)  COMMENT '当日早偿金额',
  `early_payment_asset_count`                     decimal(12,0)  COMMENT '当日早偿笔数',
  `early_payment_cust_count`                      decimal(12,0)  COMMENT '当日早偿客户数',
  `early_payment_principal_accu`                  decimal(25,5)  COMMENT '累计早偿金额',
  `early_payment_asset_count_accu`                decimal(12,0)  COMMENT '累计早偿笔数',
  `early_payment_cust_count_accu`                 decimal(12,0)  COMMENT '累计早偿客户数',
  `remain_principal`                              decimal(25,5)  COMMENT '未结清资产剩余本金',
  `asset_count`                                   decimal(12,0)  COMMENT '未结清资产笔数',
  `cust_count`                                    decimal(12,0)  COMMENT '未结清资产客户数',
  `bag_date`                                      string         COMMENT '封包日',
  `package_remain_principal`                      decimal(25,5)  COMMENT '封包时金额',
  `package_asset_count`                           decimal(12,0)  COMMENT '封包时资产笔数',
  `package_cust_count`                            decimal(12,0)  COMMENT '封包时客户数'
) COMMENT '早偿监控汇总表'
PARTITIONED BY(`biz_date` string COMMENT '快照日',`project_id` string COMMENT '项目编号',`bag_id` string COMMENT '包id')
STORED AS PARQUET;


-- 早偿监控资产详情表
-- DROP TABLE IF EXISTS `dm_eagle.abs_early_payment_asset_details`;
CREATE TABLE IF NOT EXISTS `dm_eagle.abs_early_payment_asset_details`(
  `serial_number`                                 string         COMMENT '借据号',
  `contract_no`                                   string         COMMENT '合同编号',
  `remain_principal_before_payment`               decimal(25,5)  COMMENT '还款前剩余本金',
  `early_payment_date`                            string         COMMENT '提前还款日期',
  `early_payment_amount`                          decimal(25,5)  COMMENT '总金额',
  `early_payment_principal`                       decimal(25,5)  COMMENT '提前结清实还本金',
  `early_payment_interest`                        decimal(25,5)  COMMENT '提前结清实还利息',
  `early_payment_fee`                             decimal(25,5)  COMMENT '提前结清实还费用'
) COMMENT '早偿监控资产详情表'
PARTITIONED BY(`biz_date` string COMMENT '快照日',`project_id` string COMMENT '项目编号',`bag_id` string COMMENT '包编号')
STORED AS PARQUET;

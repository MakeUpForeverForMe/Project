-- DROP DATABASE IF EXISTS `stage`;
CREATE DATABASE IF NOT EXISTS `stage` COMMENT '数据缓冲层' location 'cosn://bigdata-center-prod-1253824322/user/hadoop/warehouse/stage.db';






-- 核心报文日志表
-- DROP TABLE IF EXISTS `stage.ecas_msg_log`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.ecas_msg_log`(
  `msg_log_id`                                        string        COMMENT '报文日志ID',
  `foreign_id`                                        string        COMMENT '关联ID',
  `msg_type`                                          string        COMMENT '报文类型',
  `original_msg`                                      string        COMMENT '原始报文',
  `target_msg`                                        string        COMMENT '目标报文',
  `deal_date`                                         string        COMMENT '处理时间', -- 业务时间
  `org`                                               string        COMMENT '机构号',
  `create_time`                                       bigint        COMMENT '创建时间',
  `update_time`                                       bigint        COMMENT '更新时间', -- 批量时间
  `jpa_version`                                       int           COMMENT '乐观锁版本号'
) COMMENT '报文日志表'
STORED as PARQUET;


-- 星连报文日志表
-- DROP TABLE IF EXISTS `stage.nms_interface_resp_log`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.nms_interface_resp_log`(
  `id`                                                string        COMMENT '接口响应日志ID',
  `req_log_id`                                        string        COMMENT '请求日志ID',
  `sta_service_method_name`                           string        COMMENT '标准请求模板编号',
  `standard_req_msg`                                  string        COMMENT '标准请求报文',
  `standard_resp_msg`                                 string        COMMENT '标准响应报文',
  `resp_msg`                                          string        COMMENT '响应报文',
  `resp_code`                                         string        COMMENT '响应码',
  `resp_desc`                                         string        COMMENT '响应描述',
  `deal_date`                                         string        COMMENT '请求处理时间',
  `status`                                            string        COMMENT '请求状态（SUC：成功，FAIL：失败）',
  `org`                                               string        COMMENT '机构号',
  `create_time`                                       bigint        COMMENT '创建时间',
  `update_time`                                       bigint        COMMENT '更新时间',
  `jpa_version`                                       decimal(8,0)  COMMENT '乐观锁版本号：乐观锁版本号'
) COMMENT '报文日志表'
STORED as PARQUET;


-- 风控用户标签表
-- DROP TABLE IF EXISTS `stage.t_personas`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.t_personas`(
  `id`                                                int           COMMENT '主键id',
  `swift_no`                                          string        COMMENT '贷中流水号',
  `project_name`                                      string        COMMENT '项目名',
  `project_id`                                        string        COMMENT '项目ID',
  `order_id`                                          string        COMMENT '申请号',
  `pro_code`                                          string        COMMENT '产品编码',
  `strategy`                                          string        COMMENT '分单策略',
  `model_version`                                     string        COMMENT '版本号',
  `apply_by_id_last_year_fit`                         decimal(12,8) COMMENT '跨平台数过去一年申请数量按身份证号码统计fit',
  `apply_by_id_last_year`                             int           COMMENT '跨平台数过去一年申请数量按身份证号码统计',
  `apps_of_con_fin_pro_last_two_year_fit`             decimal(12,8) COMMENT '消费金融类过去两年申请产品数fit',
  `apps_of_con_fin_pro_last_two_year`                 int           COMMENT '消费金融类过去两年申请产品数',
  `usage_rate_of_compus_loan_app_fit`                 decimal(12,8) COMMENT '校园类贷款app过去30天内使用强度fit',
  `usage_rate_of_compus_loan_app`                     int           COMMENT '校园类贷款app过去30天内使用强度',
  `apps_of_con_fin_org_last_two_year_fit`             decimal(12,8) COMMENT '消费金融类机构过去两年申请数量fit',
  `apps_of_con_fin_org_last_two_year`                 int           COMMENT '消费金融类机构过去两年申请数量',
  `reject_times_of_credit_apply_last4_m_fit`          decimal(12,8) COMMENT '过去四个月授信失败次数fit',
  `reject_times_of_credit_apply_last4_m`              int           COMMENT '过去四个月授信失败次数',
  `overdue_times_of_credit_apply_last4_m_fit`         decimal(12,8) COMMENT '过去四个月逾期次数fit',
  `overdue_times_of_credit_apply_last4_m`             int           COMMENT '过去四个月逾期次数',
  `sum_credit_of_web_loan_fit`                        decimal(12,8) COMMENT '网络贷款平台总授信额度fit',
  `sum_credit_of_web_loan`                            int           COMMENT '网络贷款平台总授信额度',
  `count_of_register_apps_of_fin_last_m_fit`          decimal(12,8) COMMENT '过去一个月内金融类app注册总数量fit',
  `count_of_register_apps_of_fin_last_m`              int           COMMENT '过去一个月内金融类app注册总数量',
  `count_of_uninstall_apps_of_loan_last3_m_fit`       decimal(12,8) COMMENT '过去三个月贷款类APP卸载总数量fit',
  `count_of_uninstall_apps_of_loan_last3_m`           int           COMMENT '过去三个月贷款类APP卸载总数量',
  `days_of_location_upload_lats9_m_fit`               decimal(12,8) COMMENT '过去九个月地理位置上报天数fit',
  `days_of_location_upload_lats9_m`                   int           COMMENT '过去九个月地理位置上报天数',
  `account_for_in_town_in_work_day_last_m_fit`        decimal(12,8) COMMENT '最近一个月工作日出现在村镇占比fit',
  `account_for_in_town_in_work_day_last_m`            int           COMMENT '最近一个月工作日出现在村镇占比',
  `count_of_installition_of_loan_app_last2_m_fit`     decimal(12,8) COMMENT '过去两个月金融贷款类APP安装总个数fit',
  `count_of_installition_of_loan_app_last2_m`         int           COMMENT '过去两个月金融贷款类APP安装总个数',
  `risk_of_device_fit`                                decimal(12,8) COMMENT '综合评估设备风险程度fit',
  `risk_of_device`                                    int           COMMENT '综合评估设备风险程度',
  `account_for_wifi_use_time_span_last5_m_fit`        decimal(12,8) COMMENT '过去五个月wifi连接时间长度占比fit',
  `account_for_wifi_use_time_span_last5_m`            int           COMMENT '过去五个月wifi连接时间长度占比',
  `count_of_notices_of_fin_message_last9_m_fit`       decimal(12,8) COMMENT '过去九个月金融类通知接收总数量fit',
  `count_of_notices_of_fin_message_last9_m`           int           COMMENT '过去九个月金融类通知接收总数量',
  `days_of_earliest_register_of_loan_app_last9_m_fit` decimal(12,8) COMMENT '过去九个月最早注册贷款应用距今天数fit',
  `days_of_earliest_register_of_loan_app_last9_m`     int           COMMENT '过去九个月最早注册贷款应用距今天数',
  `loan_amt_last3_m_fit`                              decimal(12,8) COMMENT '过去三个月贷款总金额fit',
  `loan_amt_last3_m`                                  int           COMMENT '过去三个月贷款总金额',
  `overdue_loans_of_more_than1_day_last6_m_fit`       decimal(12,8) COMMENT '过去六个月发生一天以上的逾期贷款总笔数fit',
  `overdue_loans_of_more_than1_day_last6_m`           int           COMMENT '过去六个月发生一天以上的逾期贷款总笔数',
  `amt_of_perfermance_loans_last3_m_fit`              decimal(12,8) COMMENT '过去三个月履约贷款总金额fit',
  `amt_of_perfermance_loans_last3_m`                  int           COMMENT '过去三个月履约贷款总金额',
  `last_financial_query_fit`                          decimal(12,8) COMMENT '最近一次金融类查询距今时间(月)fit',
  `last_financial_query`                              int           COMMENT '最近一次金融类查询距今时间(月)',
  `average_daily_open_times_of_fin_apps_last_m_fit`   decimal(12,8) COMMENT '过去一个月金融理财类APP每天打开次数平均值fit',
  `average_daily_open_times_of_fin_apps_last_m`       int           COMMENT '过去一个月金融理财类APP每天打开次数平均值',
  `times_of_uninstall_fin_apps_last15_d_fit`          decimal(12,8) COMMENT '过去半个月金融理财类APP卸载总次数fit',
  `times_of_uninstall_fin_apps_last15_d`              int           COMMENT '过去半个月金融理财类APP卸载总次数',
  `account_for_install_bussiness_apps_last4_m_fit`    decimal(12,8) COMMENT '过去四个月商务类APP安装数量占比fit',
  `account_for_install_bussiness_apps_last4_m`        int           COMMENT '过去四个月商务类APP安装数量占比',
  `city_level_model`                                  int           COMMENT '城市模型标签',
  `consume_model`                                     int           COMMENT '消费模型标签',
  `education_model`                                   int           COMMENT '学历模型标签',
  `marriage_model`                                    int           COMMENT '婚姻模型标签',
  `financial_model`                                   int           COMMENT '理财模型标签',
  `income_level`                                      string        COMMENT '收入等级',
  `blk_list1`                                         int           COMMENT '外部机构1黑名单',
  `blk_list2`                                         int           COMMENT '外部机构2黑名单',
  `blk_list_loc`                                      int           COMMENT '自有黑名单',
  `virtual_malicious_status`                          int           COMMENT '疑似涉黄涉恐',
  `counterfeit_agency_status`                         int           COMMENT '疑似资料伪造包装',
  `forgedid_status`                                   int           COMMENT '疑似资料伪冒行为',
  `gamer_arbitrage_status`                            int           COMMENT '疑似营销活动欺诈',
  `id_theft_status`                                   int           COMMENT '疑似资料被盗',
  `hit_deadbeat_list`                                 int           COMMENT '疑似公开信息失信',
  `fraud_industry`                                    int           COMMENT '疑似金融黑产相关',
  `cat_pool`                                          int           COMMENT '疑似手机猫池欺诈',
  `suspicious_device`                                 int           COMMENT '疑似风险设备环境',
  `abnormal_payment`                                  int           COMMENT '疑似异常支付行为',
  `abnormal_account`                                  int           COMMENT '疑似线上养号',
  `account_hacked`                                    int           COMMENT '疑似账号被盗风向',
  `score_level`                                       string        COMMENT '资产等级（A~E）',
  `pass`                                              string        COMMENT '是否通过（Yes，No）',
  `ret_msg`                                           string        COMMENT '拒绝原因（当前仅有以下三种拒绝原因：1、信用风险，2、欺诈风险，3、黑名单）',
  `associated_partner_evaluation_rating`              int           COMMENT '关联方评估等级',
  `vendor_attributes`                                 int           COMMENT '场景方评分',
  `multi_borrowing`                                   int           COMMENT '多头借贷综合指数',
  `gambling_preference`                               int           COMMENT '博彩偏好综合指数',
  `gaming_preference`                                 int           COMMENT '游戏偏好综合指数',
  `multimedia_preference`                             int           COMMENT '视频偏好综合指数',
  `social_preference`                                 int           COMMENT '交友偏好综合指数',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       bigint        COMMENT '修改时间'
) COMMENT '风控用户标签表'
STORED as PARQUET;


set hivevar:tb_suffix=_asset; -- 代偿前
set hivevar:tb_suffix=;       -- 代偿后

-- 借据表
-- DROP TABLE IF EXISTS `stage.ecas_loan${tb_suffix}`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.ecas_loan${tb_suffix}`(
  `org`                                               string        COMMENT '机构号',
  `loan_id`                                           string        COMMENT '分期计划ID（与 DUE_BILL_NO 相关联的借据号）',
  `acct_nbr`                                          string        COMMENT '账户编号',
  `acct_type`                                         string        COMMENT '账户类型：A：人民币独立基本信用账户，B：美元独立基本信用账户，C：人民币共享基本信用账户，D：美元共享基本信用账户，E：人民币独立小额贷款账户，F：人民币活期借记账户，G：人民币共享小额贷款账户，L：人民币存款账户',
  `cust_id`                                           string        COMMENT '客户号',
  `due_bill_no`                                       string        COMMENT '借据号',
  `apply_no`                                          string        COMMENT '申请编号',
  `register_date`                                     string        COMMENT '注册日期',
  `request_time`                                      bigint        COMMENT '请求日期时间',
  `loan_type`                                         string        COMMENT '分期类型（R：消费转分期，C：现金分期，B：账单分期，P：POS分期，M：大额分期（专项分期），MCAT：随借随还，MCEP：等额本金，MCEI：等额本息）',
  `loan_status`                                       string        COMMENT '分期状态（N：正常，O：逾期，F：已还清）',
  `loan_init_term`                                    int           COMMENT '分期总期数',
  `curr_term`                                         int           COMMENT '当前期数',
  `remain_term`                                       int           COMMENT '剩余期数',
  `loan_init_prin`                                    decimal(15,2) COMMENT '分期总本金',
  `totle_int`                                         decimal(15,2) COMMENT '贷款总利息',
  `totle_term_fee`                                    decimal(15,2) COMMENT '总分期手续费',
  `totle_svc_fee`                                     decimal(15,2) COMMENT '总服务费',
  `unstmt_prin`                                       decimal(15,2) COMMENT '未出账单的本金',
  `paid_principal`                                    decimal(15,2) COMMENT '已偿还本金',
  `paid_interest`                                     decimal(15,2) COMMENT '已偿还利息',
  `paid_svc_fee`                                      decimal(15,2) COMMENT '已还服务费',
  `paid_term_fee`                                     decimal(15,2) COMMENT '已还手续费',
  `paid_penalty`                                      decimal(15,2) COMMENT '已还罚息',
  `paid_mult`                                         decimal(15,2) COMMENT '已还滞纳金',
  `active_date`                                       string        COMMENT '激活日期',
  `paid_out_date`                                     string        COMMENT '还清日期',
  `terminal_date`                                     string        COMMENT '提前终止日期',
  `terminal_reason_cd`                                string        COMMENT '分期终止原因代码（P：提前还款，M：银行业务人员手工终止（manual），D：逾期自动终止（delinquency），R：锁定码终止（Refund），V：持卡人手动终止，C：理赔终止，T：退货终止，U：重组结清终止，F：强制结清终止，B：免息转分期）',
  `loan_code`                                         string        COMMENT '分期计划代码',
  `register_id`                                       string        COMMENT '分期申请顺序号',
  `interest_rate`                                     decimal(12,8) COMMENT '基础利率',
  `penalty_rate`                                      decimal(12,8) COMMENT '罚息利率',
  `loan_expire_date`                                  string        COMMENT '贷款到期日期',
  `loan_age_code`                                     string        COMMENT '贷款逾期最大期数',
  `past_extend_cnt`                                   int           COMMENT '已展期次数',
  `past_shorten_cnt`                                  int           COMMENT '已缩期次数',
  `contract_no`                                       string        COMMENT '合同号',
  `overdue_date`                                      string        COMMENT '逾期起始日期',
  `max_cpd`                                           int           COMMENT 'CPD最大值',
  `max_cpd_date`                                      string        COMMENT '最大CPD日期',
  `max_dpd`                                           int           COMMENT 'DPD最大值',
  `max_dpd_date`                                      string        COMMENT '最大DPD日期',
  `cpd_begin_date`                                    string        COMMENT 'CPD起始日期',
  `loan_fee_def_id`                                   string        COMMENT '贷款子产品编号',
  `purpose`                                           string        COMMENT '客户贷款用途',
  `product_code`                                      string        COMMENT '产品代码',
  `pre_age_cd_gl`                                     string        COMMENT '上一会计账龄',
  `age_code_gl`                                       string        COMMENT '总账账龄',
  `normal_int_acru`                                   decimal(15,2) COMMENT '计提正常利息',
  `totle_mult_fee`                                    decimal(15,2) COMMENT '总应收滞纳金',
  `totle_penalty`                                     decimal(15,2) COMMENT '应还罚息',
  `is_int_accural_ind`                                string        COMMENT '是否免息（Y：是，N：否）',
  `collect_out_date`                                  string        COMMENT '出催收队列时间',
  `create_time`                                       bigint        COMMENT '创建时间',
  `create_user`                                       string        COMMENT '创建人',
  `loan_settle_reason`                                string        COMMENT '借据结清原因（NORMAL_SETTLE：正常结清，OVERDUE_SETTLE：逾期结清，PRE_SETTLE：提前结清，REFUND：退车，REDEMPTION：赎回）',
  `repay_term`                                        int           COMMENT '已还期数',
  `overdue_term`                                      int           COMMENT '当前逾期期数',
  `count_overdue_term`                                int           COMMENT '累计逾期期数',
  `max_overdue_term`                                  int           COMMENT '历史单次最长逾期期数',
  `max_overdue_prin`                                  decimal(15,2) COMMENT '历史最大逾期本金',
  `overdue_days`                                      int           COMMENT '逾期天数',
  `count_overdue_days`                                int           COMMENT '历史累计逾期天数',
  `max_overdue_days`                                  int           COMMENT '历史最大逾期天数',
  `reduce_prin`                                       decimal(15,2) COMMENT '减免本金',
  `reduce_interest`                                   decimal(15,2) COMMENT '减免利息',
  `reduce_svc_fee`                                    decimal(15,2) COMMENT '减免服务费',
  `reduce_term_fee`                                   decimal(15,2) COMMENT '减免手续费',
  `reduce_penalty`                                    decimal(15,2) COMMENT '减免罚息',
  `reduce_mult_amt`                                   decimal(15,2) COMMENT '减免滞纳金',
  `overdue_prin`                                      decimal(15,2) COMMENT '逾期本金',
  `overdue_interest`                                  decimal(15,2) COMMENT '逾期利息',
  `overdue_svc_fee`                                   decimal(15,2) COMMENT '逾期服务费',
  `overdue_term_fee`                                  decimal(15,2) COMMENT '逾期手续费',
  `overdue_penalty`                                   decimal(15,2) COMMENT '逾期罚息',
  `overdue_mult_amt`                                  decimal(15,2) COMMENT '逾期滞纳金',
  `svc_fee_rate`                                      decimal(12,8) COMMENT '服务费费率',
  `term_fee_rate`                                     decimal(12,8) COMMENT '手续费费率',
  `acq_id`                                            string        COMMENT '合作方机构',
  `cycle_day`                                         string        COMMENT '账户的账单日期',
  `goods_princ`                                       decimal(12,2) COMMENT '关联金额',
  `sync_date`                                         string        COMMENT '同步日期',
  `capital_plan_no`                                   string        COMMENT '资金计划编号',
  `lst_upd_time`                                      bigint        COMMENT '最后一次更新时间',
  `lst_upd_user`                                      string        COMMENT '最后一次更新人',
  `capital_type`                                      string        COMMENT '资金方类型'
) COMMENT '借据表'
PARTITIONED BY (`d_date` string COMMENT '快照日期',`p_type` string COMMENT '数据类型')
STORED as PARQUET;


-- 还款计划表
-- DROP TABLE IF EXISTS `stage.ecas_repay_schedule${tb_suffix}`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.ecas_repay_schedule${tb_suffix}`(
  `org`                                               string        COMMENT '机构号',
  `schedule_id`                                       string        COMMENT '还款计划ID',
  `due_bill_no`                                       string        COMMENT '借据号',
  `curr_bal`                                          decimal(15,2) COMMENT '当前余额（当前欠款）',
  `loan_init_prin`                                    decimal(15,2) COMMENT '分期总本金',
  `loan_init_term`                                    int           COMMENT '分期总期数',
  `curr_term`                                         int           COMMENT '当前期数',
  `due_term_prin`                                     decimal(15,2) COMMENT '应还本金',
  `due_term_int`                                      decimal(15,2) COMMENT '应还利息',
  `due_term_fee`                                      decimal(15,2) COMMENT '应还手续费',
  `due_svc_fee`                                       decimal(15,2) COMMENT '应还服务费',
  `due_penalty`                                       decimal(15,2) COMMENT '应还罚息',
  `due_mult_amt`                                      decimal(15,2) COMMENT '应还滞纳金',
  `paid_term_pric`                                    decimal(15,2) COMMENT '已还本金',
  `paid_term_int`                                     decimal(15,2) COMMENT '已还利息',
  `paid_term_fee`                                     decimal(15,2) COMMENT '已还手续费',
  `paid_svc_fee`                                      decimal(15,2) COMMENT '已还服务费',
  `paid_penalty`                                      decimal(15,2) COMMENT '已还罚息',
  `paid_mult_amt`                                     decimal(15,2) COMMENT '已还滞纳金',
  `reduced_amt`                                       decimal(15,2) COMMENT '减免金额',
  `reduce_term_prin`                                  decimal(15,2) COMMENT '减免本金',
  `reduce_term_int`                                   decimal(15,2) COMMENT '减免利息',
  `reduce_term_fee`                                   decimal(15,2) COMMENT '减免手续费',
  `reduce_svc_fee`                                    decimal(15,2) COMMENT '减免服务费',
  `reduce_penalty`                                    decimal(15,2) COMMENT '减免罚息',
  `reduce_mult_amt`                                   decimal(15,2) COMMENT '减免滞纳金',
  `penalty_acru`                                      decimal(15,2) COMMENT '罚息累计金额',
  `paid_out_date`                                     string        COMMENT '还清日期',
  `paid_out_type`                                     string        COMMENT '还清类型',
  `start_interest_date`                               string        COMMENT '起息日',
  `pmt_due_date`                                      string        COMMENT '到期还款日期',
  `origin_pmt_due_date`                               string        COMMENT '原到期还款日',
  `product_code`                                      string        COMMENT '产品代码',
  `schedule_status`                                   string        COMMENT '还款计划状态（N：正常，O：逾期，F：已还清）',
  `grace_date`                                        string        COMMENT '宽限日期（宽限日）',
  `create_time`                                       bigint        COMMENT '创建时间',
  `create_user`                                       string        COMMENT '创建人',
  `lst_upd_time`                                      bigint        COMMENT '最后一次更新时间',
  `lst_upd_user`                                      string        COMMENT '最后一次更新人',
  `jpa_version`                                       int           COMMENT '乐观锁版本号',
  `out_side_schedule_no`                              string        COMMENT '外部还款计划编号'
) COMMENT '还款计划表'
PARTITIONED BY (`d_date` string COMMENT '快照日期',`p_type` string COMMENT '数据类型')
STORED as PARQUET;


-- 还款分配历史表
-- DROP TABLE IF EXISTS `stage.ecas_repay_hst${tb_suffix}`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.ecas_repay_hst${tb_suffix}`(
  `payment_id`                                        string        COMMENT '流水号',
  `due_bill_no`                                       string        COMMENT '借据号',
  `acct_nbr`                                          string        COMMENT '账户编号',
  `acct_type`                                         string        COMMENT '账户类型（A：人民币独立基本信用账户，B：美元独立基本信用账户，C：人民币共享基本信用账户，D：美元共享基本信用账户，E：人民币独立小额贷款账户，F：人民币活期借记账户，G：人民币共享小额贷款账户，L：人民币存款账户）',
  `bnp_type`                                          string        COMMENT '余额成分（Pricinpal：本金，Interest：利息，Penalty：罚息，Mulct：罚金，Compound：复利，CardFee：年费，OverLimitFee：超限费，LatePaymentCharge：滞纳金，NSFCharge：资金不足罚金，TXNFee：交易费，TERMFee：手续费，SVCFee：服务费，LifeInsuFee：寿险计划包费，S）',
  `repay_amt`                                         decimal(15,2) COMMENT '还款金额',
  `batch_date`                                        string        COMMENT '批量日期',
  `create_time`                                       bigint        COMMENT '创建时间',
  `create_user`                                       string        COMMENT '创建人',
  `lst_upd_time`                                      bigint        COMMENT '最后一次更新时间',
  `lst_upd_user`                                      string        COMMENT '最后一次更新人',
  `jpa_version`                                       int           COMMENT '乐观锁版本号',
  `term`                                              int           COMMENT '期数',
  `org`                                               string        COMMENT '机构号',
  `order_id`                                          string        COMMENT '订单编号',
  `txn_seq`                                           string        COMMENT '交易流水号',
  `txn_date`                                          string        COMMENT '交易日期',
  `overdue_days`                                      int           COMMENT '逾期天数',
  `loan_status`                                       string        COMMENT '借据状态'
) COMMENT '还款分配历史表'
PARTITIONED BY (`d_date` string COMMENT '快照日期',`p_type` string COMMENT '数据类型')
STORED as PARQUET;


-- 订单流水表
-- DROP TABLE IF EXISTS `stage.ecas_order${tb_suffix}`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.ecas_order${tb_suffix}`(
  `org`                                               string        COMMENT '机构号',
  `order_id`                                          string        COMMENT '订单编号',
  `channel_id`                                        string        COMMENT '服务渠道编号（VISA：VISA，MC：MC，JCB：JCB，CUP：CUP，AMEX：AMEX，BANK：本行，ICL：ic系统，THIR：第三方，SUNS：阳光，AG：客服）',
  `acct_nbr`                                          string        COMMENT '账户编号',
  `acct_type`                                         string        COMMENT '账户类型（A：人民币独立基本信用账户，B：美元独立基本信用账户，C：人民币共享基本信用账户，D：美元共享基本信用账户，E：人民币独立小额贷款账户，F：人民币活期借记账户，G：人民币共享小额贷款账户，L：人民币存款账户）',
  `command_type`                                      string        COMMENT '支付指令类型（SPA：单笔代付，SDB：单笔代扣，QSP：单笔代付查询，QSD：单笔代扣查询，BDB：批量代扣，BDA：批量代付）',
  `order_status`                                      string        COMMENT '订单状态（C：已提交，P：待提交，Q：待审批，W：处理中，S：已完成，V：已失效，E：失败，T：超时，R：已重提，G：拆分处理中，D：拆分已完成，B：撤销，X：已受理待入账）',
  `order_time`                                        bigint        COMMENT '订单时间',
  `mer_id`                                            string        COMMENT '商户编号',
  `txn_type`                                          string        COMMENT '交易类型（Inq：查询，Cash：取现，AgentDebit：付款，Loan：分期，Auth：消费，PreAuth：预授权，PAComp：预授权完成，Load：圈存，Credit：存款，AgentCredit：收款，TransferCredit：转入，TransferDeditDepos：转出，AdviceSettle：结算通知，BigAmountLoan：大）',
  `txn_amt`                                           decimal(15,2) COMMENT '交易金额',
  `currency`                                          string        COMMENT '币种',
  `purpose`                                           string        COMMENT '支付用途',
  `status`                                            string        COMMENT '交易状态（支付状态为SUCCESS成功，其他为失败）',
  `code`                                              string        COMMENT '状态码',
  `message`                                           string        COMMENT '描述',
  `due_bill_no`                                       string        COMMENT '借据号',
  `business_date`                                     string        COMMENT '业务日期',
  `send_time`                                         string        COMMENT '发送时间',
  `ori_order_id`                                      string        COMMENT '原订单编号',
  `setup_date`                                        string        COMMENT '创建日期',
  `opt_datetime`                                      bigint        COMMENT '更新时间',
  `loan_usage`                                        string        COMMENT '贷款用途（M：预约提前结清扣款，L：放款申请，R：退货，O：逾期扣款，W：赎回结清，I：强制结清扣款，X：账务调整，N：提前还当期，T：退票）',
  `online_flag`                                       string        COMMENT '联机标识（Y：是，N：否）',
  `memo`                                              string        COMMENT '备注',
  `create_time`                                       bigint        COMMENT '创建时间',
  `create_user`                                       string        COMMENT '创建人',
  `lst_upd_time`                                      bigint        COMMENT '最后一次更新时间',
  `lst_upd_user`                                      string        COMMENT '最后一次更新人',
  `contr_nbr`                                         string        COMMENT '合同号',
  `ref_nbr`                                           string        COMMENT '交易参考号（内部交易参考号，生成规则：(yyMMddhhmmddSSS) + 4位随机数 + 3位外部流水号末尾）',
  `service_id`                                        string        COMMENT '交易服务码',
  `jpa_version`                                       int           COMMENT '乐观锁版本号',
  `original_txn_amt`                                  decimal(15,2) COMMENT '原始交易金额',
  `online_allow`                                      string        COMMENT '允许联机标识（Y：是，N：否）',
  `assign_repay_ind`                                  string        COMMENT '指定余额成分还款标志（Y：是，N：否）',
  `repay_way`                                         string        COMMENT '还款方式（ONLINE：线上，OFFLINE：线下）',
  `term`                                              int           COMMENT '处理期数',
  `order_pay_no`                                      string        COMMENT '支付流水号',
  `outer_no`                                          string        COMMENT '外部凭证号',
  `pay_channel`                                       string        COMMENT '支付渠道',
  `batch_seq`                                         string        COMMENT '批次号',
  `apply_no`                                          string        COMMENT '申请件编号',
  `response_code`                                     string        COMMENT '对外返回码',
  `response_message`                                  string        COMMENT '对外返回描述',
  `repay_items`                                       string        COMMENT '还款余额成分详情',
  `bank_trade_time`                                   string        COMMENT '线下银行订单交易时间',
  `bank_trade_no`                                     string        COMMENT '银行交易流水号',
  `bank_trade_act_no`                                 string        COMMENT '银行付款账号',
  `bank_trade_act_name`                               string        COMMENT '银行付款账户名称',
  `txn_time`                                          bigint        COMMENT '交易时间',
  `service_sn`                                        string        COMMENT '流水号',
  `real_bank_trade_no`                                string        COMMENT '实际银行流水号',
  `real_bank_trade_time`                              bigint        COMMENT '实际银行流水时间',
  `confirm_flag`                                      string        COMMENT '确认标志',
  `txn_date`                                          string        COMMENT '交易日期',
  `bank_trade_act_phone`                              string        COMMENT '银行预留手机号',
  `capital_plan_no`                                   string        COMMENT '资金计划编号',
  `capital_type`                                      string        COMMENT '资金方类型',
  `extend_info`                                       string        COMMENT '扩展信息（json对象）',
  `repay_serial_no`                                   string        COMMENT '还款流水号',
  `success_amt`                                       decimal(15,2) COMMENT '成功金额'
) COMMENT '订单流水表'
PARTITIONED BY (`d_date` string COMMENT '快照日期',`p_type` string COMMENT '数据类型')
STORED as PARQUET;


-- 订单流水历史表
-- DROP TABLE IF EXISTS `stage.ecas_order_hst${tb_suffix}`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.ecas_order_hst${tb_suffix}`(
  `org`                                               string        COMMENT '机构号',
  `order_hst_id`                                      string        COMMENT '订单历史ID',
  `channel_id`                                        string        COMMENT '服务渠道编号（VISA：VISA，MC：MC，JCB：JCB，CUP：CUP，AMEX：AMEX，BANK：本行，ICL：ic系统，THIR：第三方，SUNS：阳光，AG：客服）',
  `acct_nbr`                                          string        COMMENT '账户编号',
  `acct_type`                                         string        COMMENT '账户类型（A：人民币独立基本信用账户，B：美元独立基本信用账户，C：人民币共享基本信用账户，D：美元共享基本信用账户，E：人民币独立小额贷款账户，F：人民币活期借记账户，G：人民币共享小额贷款账户，L：人民币存款账户）',
  `command_type`                                      string        COMMENT '支付指令类型（SPA：单笔代付，SDB：单笔代扣，QSP：单笔代付查询，QSD：单笔代扣查询，BDB：批量代扣，BDA：批量代付）',
  `order_status`                                      string        COMMENT '订单状态（C：已提交，P：待提交，Q：待审批，W：处理中，S：已完成，V：已失效，E：失败，T：超时，R：已重提，G：拆分处理中，D：拆分已完成，B：撤销，X：已受理待入账）',
  `order_time`                                        bigint        COMMENT '订单时间',
  `mer_id`                                            string        COMMENT '商户编号',
  `txn_type`                                          string        COMMENT '交易类型（Inq：查询，Cash：取现，AgentDebit：付款，Loan：分期，Auth：消费，PreAuth：预授权，PAComp：预授权完成，Load：圈存，Credit：存款，AgentCredit：收款，TransferCredit：转入，TransferDeditDepos：转出，AdviceSettle：结算通知，BigAmountLoan：大）',
  `txn_amt`                                           decimal(15,2) COMMENT '交易金额',
  `currency`                                          string        COMMENT '币种',
  `purpose`                                           string        COMMENT '支付用途',
  `status`                                            string        COMMENT '交易状态（支付状态为SUCCESS成功，其他为失败）',
  `code`                                              string        COMMENT '状态码',
  `message`                                           string        COMMENT '描述',
  `due_bill_no`                                       string        COMMENT '借据号',
  `business_date`                                     string        COMMENT '业务日期',
  `send_time`                                         string        COMMENT '发送时间',
  `ori_order_id`                                      string        COMMENT '原订单编号',
  `setup_date`                                        string        COMMENT '创建日期',
  `opt_datetime`                                      bigint        COMMENT '更新时间',
  `loan_usage`                                        string        COMMENT '贷款用途（M：预约提前结清扣款，L：放款申请，R：退货，O：逾期扣款，W：赎回结清，I：强制结清扣款，X：账务调整，N：提前还当期）',
  `online_flag`                                       string        COMMENT '联机标识（Y：是，N：否）',
  `memo`                                              string        COMMENT '备注',
  `create_time`                                       bigint        COMMENT '创建时间',
  `create_user`                                       string        COMMENT '创建人',
  `lst_upd_time`                                      bigint        COMMENT '最后一次更新时间',
  `lst_upd_user`                                      string        COMMENT '最后一次更新人',
  `contr_nbr`                                         string        COMMENT '合同号',
  `ref_nbr`                                           string        COMMENT '交易参考号（内部交易参考号，生成规则：(yyMMddhhmmddSSS)+4位随机数+3位外部流水号末尾）',
  `service_id`                                        string        COMMENT '交易服务码',
  `jpa_version`                                       int           COMMENT '乐观锁版本号',
  `original_txn_amt`                                  decimal(15,2) COMMENT '原始交易金额',
  `online_allow`                                      string        COMMENT '允许联机标识（Y：是，N：否）',
  `assign_repay_ind`                                  string        COMMENT '指定余额成分还款标志（Y：是，N：否）',
  `repay_way`                                         string        COMMENT '还款方式（ONLINE：线上，OFFLINE：线下）',
  `term`                                              int           COMMENT '处理期数',
  `order_pay_no`                                      string        COMMENT '支付流水号',
  `outer_no`                                          string        COMMENT '外部凭证号',
  `pay_channel`                                       string        COMMENT '支付渠道',
  `batch_seq`                                         string        COMMENT '批次号',
  `apply_no`                                          string        COMMENT '申请件编号',
  `response_code`                                     string        COMMENT '对外返回码',
  `response_message`                                  string        COMMENT '对外返回描述',
  `repay_items`                                       string        COMMENT '还款余额成分详情',
  `bank_trade_time`                                   string        COMMENT '线下银行订单交易时间',
  `bank_trade_no`                                     string        COMMENT '银行交易流水号',
  `bank_trade_act_no`                                 string        COMMENT '银行付款账号',
  `bank_trade_act_name`                               string        COMMENT '银行付款账户名称',
  `order_id`                                          string        COMMENT '订单ID',
  `txn_time`                                          bigint        COMMENT '交易时间',
  `service_sn`                                        string        COMMENT '流水号',
  `real_bank_trade_no`                                string        COMMENT '实际银行流水号',
  `real_bank_trade_time`                              bigint        COMMENT '实际银行流水时间',
  `confirm_flag`                                      string        COMMENT '确认标志',
  `txn_date`                                          string        COMMENT '交易日期',
  `bank_trade_act_phone`                              string        COMMENT '银行预留手机号',
  `capital_plan_no`                                   string        COMMENT '资金计划编号',
  `capital_type`                                      string        COMMENT '资金方类型',
  `extend_info`                                       string        COMMENT '扩展信息（json对象）',
  `repay_serial_no`                                   string        COMMENT '还款流水号',
  `success_amt`                                       decimal(15,2) COMMENT '成功金额'
) COMMENT '订单流水历史表'
PARTITIONED BY (`d_date` string COMMENT '快照日期',`p_type` string COMMENT '数据类型')
STORED as PARQUET;


-- 汇通修数表
-- DROP TABLE IF EXISTS `stage.schedule_repay_order_info_ddht`;
CREATE TABLE IF NOT EXISTS `stage.schedule_repay_order_info_ddht`(
  `due_bill_no`                                       string        COMMENT '借据号',
  `order_id`                                          string        COMMENT '订单编号',
  `term`                                              decimal(5,0)  COMMENT '期数',
  `paid_out_date`                                     string        COMMENT '还款计划上结清日期',
  `txn_date`                                          string        COMMENT '实还明细上实还日期',
  `txn_amt`                                           decimal(10,4) COMMENT '实还明细上交易金额'
) COMMENT '还款计划和实还明细实还时间对照临时表'
PARTITIONED BY(biz_date string COMMENT '观察日期',product_id string COMMENT '产品编号')
STORED AS PARQUET;


-- DROP TABLE IF EXISTS `stage.ecas_loan_ht_repair`;
CREATE EXTERNAL TABLE `stage.ecas_loan_ht_repair`(
  `org`                                               string        COMMENT '',
  `loan_id`                                           string        COMMENT '///@UuidSeq',
  `acct_nbr`                                          string        COMMENT '',
  `acct_type`                                         string        COMMENT '///@EnumRef:cn.sunline.ppy.dictionary.enums.AccountType\r\n\r\nA人民币独立基本信用账户\r\nB美元独立基本信用账户\r\nC人民币共享基本信用账户\r\nD美元共享基本信用账户\r\nE人民币独立小额贷款账户\r\nF人民币活期借记账户\r\nG人民币共享小额贷款账户\r\nL人民币存款账户',
  `cust_id`                                           string        COMMENT '',
  `due_bill_no`                                       string        COMMENT '',
  `apply_no`                                          string        COMMENT '',
  `register_date`                                     string        COMMENT '',
  `request_time`                                      bigint        COMMENT '请求日期时间',
  `loan_type`                                         string        COMMENT '///@EnumRef:cn.sunline.ppy.dictionary.enums.LoanType\r\nR消费转分期\r\nC现金分期\r\nB账单分期\r\nPPOS分期\r\nM大额分期（专项分期）\r\nMCAT随借随还\r\nMCEP等额本金\r\nMCEI等额本息',
  `loan_status`                                       string        COMMENT '///@EnumRef:cn.sunline.ecas.definition.enums.LoanStatus\r\nN正常\r\nO逾期\r\nF已还清',
  `loan_init_term`                                    int           COMMENT '',
  `curr_term`                                         int           COMMENT '',
  `remain_term`                                       int           COMMENT '',
  `loan_init_prin`                                    decimal(15,2) COMMENT '',
  `totle_int`                                         decimal(15,2) COMMENT '',
  `totle_term_fee`                                    decimal(15,2) COMMENT '',
  `totle_svc_fee`                                     decimal(15,2) COMMENT '',
  `unstmt_prin`                                       decimal(15,2) COMMENT '',
  `paid_principal`                                    decimal(15,2) COMMENT '',
  `paid_interest`                                     decimal(15,2) COMMENT '',
  `paid_svc_fee`                                      decimal(15,2) COMMENT '',
  `paid_term_fee`                                     decimal(15,2) COMMENT '',
  `paid_penalty`                                      decimal(15,2) COMMENT '',
  `paid_mult`                                         decimal(15,2) COMMENT '',
  `active_date`                                       string        COMMENT '',
  `paid_out_date`                                     string        COMMENT '',
  `terminal_date`                                     string        COMMENT '',
  `terminal_reason_cd`                                string        COMMENT '///@EnumRef:cn.sunline.ecas.definition.enums.LoanTerminateReason\r\nP提前还款\r\nM银行业务人员手工终止（manual）\r\nD逾期自动终止（delinquency）\r\nR锁定码终止(Refund)\r\nV持卡人手动终止\r\nC理赔终止\r\nT退货终止\r\nU重组结清终止\r\nF强制结清终止\r\nB免息转分期',
  `loan_code`                                         string        COMMENT '',
  `register_id`                                       string        COMMENT '',
  `interest_rate`                                     decimal(12,8) COMMENT '',
  `penalty_rate`                                      decimal(12,8) COMMENT '',
  `loan_expire_date`                                  string        COMMENT '',
  `loan_age_code`                                     string        COMMENT '',
  `past_extend_cnt`                                   int           COMMENT '',
  `past_shorten_cnt`                                  int           COMMENT '',
  `contract_no`                                       string        COMMENT '',
  `overdue_date`                                      string        COMMENT '',
  `max_cpd`                                           int           COMMENT '',
  `max_cpd_date`                                      string        COMMENT '',
  `max_dpd`                                           int           COMMENT '',
  `max_dpd_date`                                      string        COMMENT '',
  `cpd_begin_date`                                    string        COMMENT '',
  `loan_fee_def_id`                                   string        COMMENT '',
  `purpose`                                           string        COMMENT '',
  `product_code`                                      string        COMMENT '产品代码',
  `pre_age_cd_gl`                                     string        COMMENT '',
  `age_code_gl`                                       string        COMMENT '',
  `normal_int_acru`                                   decimal(15,2) COMMENT '',
  `totle_mult_fee`                                    decimal(15,2) COMMENT '',
  `totle_penalty`                                     decimal(15,2) COMMENT '',
  `is_int_accural_ind`                                string        COMMENT '///@EnumRef:cn.sunline.common.enums.Indicator\r\nY是\r\nN否',
  `collect_out_date`                                  string        COMMENT '出催收队列时间',
  `create_time`                                       bigint        COMMENT '',
  `create_user`                                       string        COMMENT '',
  `loan_settle_reason`                                string        COMMENT '///@EnumRef:cn.sunline.ecas.definition.enums.LoanSettleReason\r\nNORMAL_SETTLE正常结清\r\nOVERDUE_SETTLE逾期结清\r\nPRE_SETTLE提前结清\r\nREFUND退车\r\nREDEMPTION赎回',
  `repay_term`                                        int           COMMENT '',
  `overdue_term`                                      int           COMMENT '',
  `count_overdue_term`                                int           COMMENT '',
  `max_overdue_term`                                  int           COMMENT '',
  `max_overdue_prin`                                  decimal(15,2) COMMENT '',
  `overdue_days`                                      int           COMMENT '',
  `count_overdue_days`                                int           COMMENT '',
  `max_overdue_days`                                  int           COMMENT '',
  `reduce_prin`                                       decimal(15,2) COMMENT '减免本金',
  `reduce_interest`                                   decimal(15,2) COMMENT '减免利息',
  `reduce_svc_fee`                                    decimal(15,2) COMMENT '减免服务费',
  `reduce_term_fee`                                   decimal(15,2) COMMENT '减免手续费',
  `reduce_penalty`                                    decimal(15,2) COMMENT '减免罚息',
  `reduce_mult_amt`                                   decimal(15,2) COMMENT '减免滞纳金',
  `overdue_prin`                                      decimal(15,2) COMMENT '',
  `overdue_interest`                                  decimal(15,2) COMMENT '',
  `overdue_svc_fee`                                   decimal(15,2) COMMENT '',
  `overdue_term_fee`                                  decimal(15,2) COMMENT '',
  `overdue_penalty`                                   decimal(15,2) COMMENT '',
  `overdue_mult_amt`                                  decimal(15,2) COMMENT '',
  `svc_fee_rate`                                      decimal(12,8) COMMENT '服务费费率',
  `term_fee_rate`                                     decimal(12,8) COMMENT '',
  `acq_id`                                            string        COMMENT '',
  `cycle_day`                                         string        COMMENT '账户的账单日期',
  `goods_princ`                                       decimal(12,2) COMMENT '关联金额',
  `sync_date`                                         string        COMMENT '同步日期',
  `capital_plan_no`                                   string        COMMENT '资金计划编号',
  `lst_upd_time`                                      bigint        COMMENT '最后一次更新时间',
  `lst_upd_user`                                      string        COMMENT '最后一次更新人',
  `capital_type`                                      string        COMMENT '资金方类型'
) COMMENT '汇通借据修数表'
PARTITIONED BY (`d_date` string COMMENT '观察日期',`p_type` string COMMENT '数据类型')
STORED as PARQUET;


-- DROP TABLE IF EXISTS `stage.ecas_repay_schedule_ht_repair`;
CREATE EXTERNAL TABLE `stage.ecas_repay_schedule_ht_repair`(
  `org`                                               string        COMMENT '机构号',
  `schedule_id`                                       string        COMMENT '还款计划ID : ///@UuidSeq',
  `due_bill_no`                                       string        COMMENT '借据号',
  `curr_bal`                                          decimal(15,2) COMMENT '当前余额 : 当前欠款',
  `loan_init_prin`                                    decimal(15,2) COMMENT '分期总本金',
  `loan_init_term`                                    int           COMMENT '分期总期数',
  `curr_term`                                         int           COMMENT '当前期数',
  `due_term_prin`                                     decimal(15,2) COMMENT '应还本金',
  `due_term_int`                                      decimal(15,2) COMMENT '应还利息',
  `due_term_fee`                                      decimal(15,2) COMMENT '应还手续费',
  `due_svc_fee`                                       decimal(15,2) COMMENT '应还服务费',
  `due_penalty`                                       decimal(15,2) COMMENT '应还罚息',
  `due_mult_amt`                                      decimal(15,2) COMMENT '应还滞纳金',
  `paid_term_pric`                                    decimal(15,2) COMMENT '已还本金',
  `paid_term_int`                                     decimal(15,2) COMMENT '已还利息',
  `paid_term_fee`                                     decimal(15,2) COMMENT '已还手续费',
  `paid_svc_fee`                                      decimal(15,2) COMMENT '已还服务费',
  `paid_penalty`                                      decimal(15,2) COMMENT '已还罚息',
  `paid_mult_amt`                                     decimal(15,2) COMMENT '已还滞纳金',
  `reduced_amt`                                       decimal(15,2) COMMENT '减免金额',
  `reduce_term_prin`                                  decimal(15,2) COMMENT '减免本金',
  `reduce_term_int`                                   decimal(15,2) COMMENT '减免利息',
  `reduce_term_fee`                                   decimal(15,2) COMMENT '减免手续费',
  `reduce_svc_fee`                                    decimal(15,2) COMMENT '减免服务费',
  `reduce_penalty`                                    decimal(15,2) COMMENT '减免罚息',
  `reduce_mult_amt`                                   decimal(15,2) COMMENT '减免滞纳金',
  `penalty_acru`                                      decimal(15,2) COMMENT '罚息累计金额',
  `paid_out_date`                                     string        COMMENT '还清日期',
  `paid_out_type`                                     string        COMMENT '还清类型',
  `start_interest_date`                               string        COMMENT '起息日',
  `pmt_due_date`                                      string        COMMENT '到期还款日期',
  `origin_pmt_due_date`                               string        COMMENT '原到期还款日',
  `product_code`                                      string        COMMENT '产品代码',
  `schedule_status`                                   string        COMMENT '还款计划状态 : ///@EnumRef:cn.sunline.ecas.definition.enums.ScheduleStatus\r\nN正常\r\nO逾期\r\nF已还清',
  `grace_date`                                        string        COMMENT '宽限日期 : 宽限日',
  `create_time`                                       bigint        COMMENT '创建时间',
  `create_user`                                       string        COMMENT '创建人',
  `lst_upd_time`                                      bigint        COMMENT '最后一次更新时间',
  `lst_upd_user`                                      string        COMMENT '最后一次更新人',
  `jpa_version`                                       int           COMMENT '乐观锁版本号',
  `out_side_schedule_no`                              string        COMMENT '外部还款计划编号'
) COMMENT '汇通还款计划修数表'
PARTITIONED BY (`d_date` string COMMENT '观察日期',`p_type` string COMMENT '数据类型')
STORED as PARQUET;


-- DROP TABLE IF EXISTS `stage.t_transaction_blend_record`;
CREATE EXTERNAL TABLE `stage.t_transaction_blend_record`(
  `id`                                                int           COMMENT '',
  `blend_serial_no`                                   string        COMMENT '流水勾兑编号',
  `record_type`                                       string        COMMENT '记录类型  D:借方 C:贷方 M:手工调整值 F:结果值 G:看管值',
  `loan_amt`                                          decimal(18,2) COMMENT '放款金额',
  `cust_repay_amt`                                    decimal(18,2) COMMENT '客户还款',
  `comp_bak_amt`                                      decimal(18,2) COMMENT '代偿回款',
  `buy_bak_amt`                                       decimal(18,2) COMMENT '回购回款',
  `deduct_sve_fee`                                    decimal(18,2) COMMENT '划扣手续费',
  `return_ticket_bak_amt`                             decimal(18,2) COMMENT '退票回款',
  `invest_amt`                                        decimal(18,2) COMMENT '投资金额',
  `invest_redeem_amt`                                 decimal(18,2) COMMENT '投资赎回金额',
  `invest_earning`                                    decimal(18,2) COMMENT '投资收益',
  `acct_int`                                          decimal(18,2) COMMENT '账户利息',
  `acct_fee`                                          decimal(18,2) COMMENT '账户费用',
  `tax_amt`                                           decimal(18,2) COMMENT '税费支付',
  `invest_cash`                                       decimal(18,2) COMMENT '投资兑付',
  `ci_fund`                                           decimal(18,2) COMMENT '信保基金',
  `ci_redeem_amt`                                     decimal(18,2) COMMENT '信保赎回',
  `ci_earning`                                        decimal(18,2) COMMENT '信保收益',
  `other_amt`                                         decimal(18,2) COMMENT '其他金额',
  `trade_day_bal`                                     decimal(18,2) COMMENT 'T日余额',
  `trade_yesterday_bal`                               decimal(18,2) COMMENT 'T-1日余额',
  `trade_day__bal_diff`                               decimal(18,2) COMMENT 'T日余额差异',
  `remark`                                            string        COMMENT '备注',
  `create_date`                                       bigint        COMMENT '创建时间',
  `update_date`                                       bigint        COMMENT '创建时间',
  `calc_date`                                         string        COMMENT '勾兑记录日期',
  `product_code`                                      string        COMMENT '信托产品编号',
  `product_name`                                      string        COMMENT '信托产品名称',
  `ch_diff_explain`                                   string        COMMENT '',
  `en_diff_explain`                                   string        COMMENT ''
) COMMENT '资金表'
PARTITIONED BY (`d_date` string COMMENT '观察日期')
STORED AS PARQUET;















-- 文件01-贷款合同信息表
-- DROP TABLE IF EXISTS `stage.asset_01_t_loan_contract_info`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.asset_01_t_loan_contract_info`(
  `id`                                                int           COMMENT '贷款合同信息主键',
  `import_id`                                         string        COMMENT '数据批次号-由接入系统生产',
  `project_id`                                        string        COMMENT '项目id',
  `agency_id`                                         string        COMMENT '机构编号',
  `asset_id`                                          string        COMMENT '资产借据号',
  `contract_code`                                     string        COMMENT '贷款合同编号',
  `loan_total_amount`                                 decimal(16,2) COMMENT '贷款总金额',
  `periods`                                           int           COMMENT '总期数',
  `repay_type`                                        string        COMMENT '还款方式,0-等额本息,1-等额本金,2-等本等息3-先息后本4-一次性还本付息,5-气球贷,6-自定义还款计划',
  `interest_rate_type`                                string        COMMENT '0-固定利率,1-浮动利率',
  `loan_interest_rate`                                decimal(8,6)  COMMENT '贷款年利率，单位:%',
  `contract_data_status`                              int           COMMENT '合同数据与还款计划中的总额是否一致，0-一致，1-不一致',
  `contract_status`                                   string        COMMENT '0-生效,1-不生效',
  `first_repay_date`                                  string        COMMENT '首次还款日期，yyyy-MM-dd',
  `extra_info`                                        string        COMMENT '扩展信息',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `verify_status`                                     string        COMMENT '数据校验状态',
  `verify_mark`                                       string        COMMENT '校验标志',
  `first_loan_end_date`                               string        COMMENT '合同结束时间',
  `per_loan_end_date`                                 string        COMMENT '上一次合同结束时间',
  `cur_loan_end_date`                                 string        COMMENT '当前合同结束时间',
  `loan_begin_date`                                   string        COMMENT '合同开始时间',
  `abs_push_flag`                                     string        COMMENT 'abs推送标志',
  `repay_frequency`                                   string        COMMENT '还款频率',
  `nominal_rate`                                      decimal(16,4) COMMENT '名义费率',
  `daily_penalty_rate`                                decimal(16,4) COMMENT '日罚息率',
  `loan_use`                                          string        COMMENT '贷款用途',
  `guarantee_type`                                    string        COMMENT '担保方式',
  `loan_type`                                         string        COMMENT '借款方类型',
  `loan_total_interest`                               decimal(16,2) COMMENT '贷款总利息',
  `loan_total_fee`                                    decimal(16,2) COMMENT '贷款总费用',
  `loan_penalty_rate`                                 decimal(8,6)  COMMENT '贷款罚息利率'
) COMMENT '贷款合同信息表'
STORED as PARQUET;


-- 文件02-主借款人信息表
-- DROP TABLE IF EXISTS `stage.asset_02_t_principal_borrower_info`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.asset_02_t_principal_borrower_info`(
  `id`                                                int           COMMENT '主借款人信息主键',
  `project_id`                                        string        COMMENT '项目id',
  `agency_id`                                         string        COMMENT '机构编号',
  `asset_id`                                          string        COMMENT '资产借据号',
  `customer_name`                                     string        COMMENT '客户姓名',
  `document_num`                                      string        COMMENT '身份证号码',
  `phone_num`                                         string        COMMENT '手机号码',
  `age`                                               int           COMMENT '年龄',
  `sex`                                               string        COMMENT '性别，0-男，2-女',
  `marital_status`                                    string        COMMENT '婚姻状态,0-已婚，1-未婚，2-离异，3-丧偶',
  `degree`                                            string        COMMENT '学位，0-小学，1-初中，2-高中/职高/技校，3-大专，4-本科,5-硕士,6-博士，7-文盲和半文盲',
  `province`                                          string        COMMENT '客户所在省',
  `city`                                              string        COMMENT '客户所在市',
  `address`                                           string        COMMENT '客户所在地区',
  `extra_info`                                        string        COMMENT '扩展信息',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `ecif_no`                                           string        COMMENT 'ecifNo',
  `card_no`                                           string        COMMENT '身份证号码(脱敏处理)',
  `phone_no`                                          string        COMMENT '手机号码(脱敏处理)',
  `imei`                                              string        COMMENT 'imei号',
  `education`                                         string        COMMENT '学位',
  `annual_income`                                     decimal(16,2) COMMENT '年收入(元)',
  `processed`                                         int           COMMENT '是否已处理'
) COMMENT '主借款人信息表'
STORED as PARQUET;


-- 文件03-主借款联系人信息表
-- DROP TABLE IF EXISTS `stage.asset_03_t_contact_person_info`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.asset_03_t_contact_person_info`(
  `id`                                                int           COMMENT '主借款人信息主键',
  `project_id`                                        string        COMMENT '项目id',
  `agency_id`                                         string        COMMENT '机构编号',
  `asset_id`                                          string        COMMENT '资产借据号',
  `customer_name`                                     string        COMMENT '客户姓名',
  `document_num`                                      string        COMMENT '身份证号码',
  `phone_num`                                         string        COMMENT '手机号码',
  `age`                                               int           COMMENT '年龄',
  `sex`                                               string        COMMENT '性别，0-男，2-女',
  `mainborrower_relationship`                         string        COMMENT '与主借款人的关系，0-配偶,1-父母,2-子女,3-亲戚,4-朋友,5-同事',
  `occupation`                                        string        COMMENT '职业',
  `work_status`                                       string        COMMENT '工作状态，0-在职，1-失业',
  `annual_income`                                     decimal(16,2) COMMENT '年收入',
  `communication_address`                             string        COMMENT '通讯地址',
  `unit_address`                                      string        COMMENT '单位地址',
  `unit_phone_number`                                 string        COMMENT '单位联系方式',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `ecif_no`                                           string        COMMENT 'ecifNo',
  `card_no`                                           string        COMMENT '身份证号码(脱敏处理)',
  `phone_no`                                          string        COMMENT '手机号码(脱敏处理)',
  `imei`                                              string        COMMENT 'imei号'
) COMMENT '主借款联系人信息表'
STORED as PARQUET;


-- 文件04-抵押物信息表
-- DROP TABLE IF EXISTS `stage.asset_04_t_guaranty_info`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.asset_04_t_guaranty_info`(
  `id`                                                int           COMMENT '抵押物信息主键',
  `project_id`                                        string        COMMENT '项目id',
  `agency_id`                                         string        COMMENT '机构编号',
  `asset_id`                                          string        COMMENT '资产借据号',
  `guaranty_type`                                     string        COMMENT '抵押物类型（CAR_MORTGAGE：汽车抵押贷款，HOUSE_MORTGAGE：住房抵押贷款，PERSONAL_CONSUME：个人消费贷款）',
  `guaranty_umber`                                    string        COMMENT '抵押物编号',
  `mortgage_handle_status`                            string        COMMENT '抵押办理状态，0-办理中，1-办理完成，2-尚未办理',
  `mortgage_alignment`                                string        COMMENT '抵押顺位0-第一顺位,2-第二顺位,3-其他',
  `extra_info`                                        string        COMMENT '扩展信息',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `processed`                                         int           COMMENT '是否已处理'
) COMMENT '抵押物信息表'
STORED as PARQUET;


-- 文件05-还款计划信息表
-- DROP TABLE IF EXISTS `stage.asset_05_t_repayment_schedule`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.asset_05_t_repayment_schedule`(
  `id`                                                int           COMMENT '实际还款信息表主键',
  `import_id`                                         string        COMMENT '数据批次号-由接入系统生产',
  `project_id`                                        string        COMMENT '项目id',
  `agency_id`                                         string        COMMENT '机构编号',
  `asset_id`                                          string        COMMENT '资产借据号',
  `period`                                            int           COMMENT '期次',
  `repay_date`                                        string        COMMENT '应还款日期',
  `repay_principal`                                   decimal(16,2) COMMENT '应还本金(元)',
  `repay_interest`                                    decimal(16,2) COMMENT '应还利息(元)',
  `repay_fee`                                         decimal(16,2) COMMENT '应还费用(元)',
  `begin_loan_principal`                              decimal(16,2) COMMENT '期初剩余本金',
  `end_loan_principal`                                decimal(16,2) COMMENT '期末剩余本金',
  `execute_date`                                      string        COMMENT '生效日期',
  `timestamp`                                         string        COMMENT '时间戳信息，区分还款计划的批次问题',
  `extra_info`                                        string        COMMENT '扩展信息',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `begin_loan_interest`                               decimal(16,2) COMMENT '期初剩余利息',
  `end_loan_interest`                                 decimal(16,2) COMMENT '期末剩余利息',
  `begin_loan_fee`                                    decimal(16,2) COMMENT '期初剩余费用',
  `end_loan_fee`                                      decimal(16,2) COMMENT '期末剩余费用',
  `remainder_periods`                                 decimal(16,2) COMMENT '剩余期数',
  `next_repay_date`                                   string        COMMENT '下次应还日期',
  `repay_penalty`                                     decimal(16,2) COMMENT '应还罚息'
) COMMENT '还款计划信息表'
PARTITIONED BY(d_date string comment '快照日期')
STORED as PARQUET;


-- 文件06-资产支付流水信息表
-- DROP TABLE IF EXISTS `stage.asset_06_t_asset_pay_flow`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.asset_06_t_asset_pay_flow`(
  `id`                                                int           COMMENT '抵押物信息主键',
  `project_id`                                        string        COMMENT '项目id',
  `agency_id`                                         string        COMMENT '机构编号',
  `asset_id`                                          string        COMMENT '资产借据号',
  `trade_channel`                                     string        COMMENT '交易渠道',
  `trade_type`                                        string        COMMENT '0-放款,1-代扣,2-主动还款,3-代偿,4-回购,5-差额补足,6-处置回收',
  `order_id`                                          string        COMMENT '订单id',
  `order_amount`                                      decimal(16,2) COMMENT '订单金额',
  `trade_currency`                                    string        COMMENT '交易币种，默认人民币',
  `name`                                              string        COMMENT '姓名-银行户名',
  `bank_account`                                      string        COMMENT '银行账号',
  `trade_time`                                        string        COMMENT '交易时间',
  `trade_status`                                      string        COMMENT '交易状态，0-成功，1-失败',
  `extra_info`                                        string        COMMENT '扩展字段',
  `trad_desc`                                         string        COMMENT '交易摘要',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间'
) COMMENT '资产支付流水信息表'
STORED as PARQUET;


-- 文件07-实际还款信息表
-- DROP TABLE IF EXISTS `stage.asset_07_t_repayment_info`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.asset_07_t_repayment_info`(
  `id`                                                int           COMMENT '实际还款信息表主键',
  `import_id`                                         string        COMMENT '数据批次号-由接入系统生产',
  `project_id`                                        string        COMMENT '项目id',
  `agency_id`                                         string        COMMENT '机构编号',
  `asset_id`                                          string        COMMENT '资产借据号',
  `repay_date`                                        string        COMMENT '应还款日期',
  `repay_principal`                                   decimal(16,2) COMMENT '应还款本金',
  `repay_interest`                                    decimal(16,2) COMMENT '应还款利息',
  `repay_fee`                                         decimal(16,2) COMMENT '应还款费用',
  `rel_pay_date`                                      string        COMMENT '实际还清日期',
  `rel_principal`                                     decimal(16,2) COMMENT '实还本金',
  `rel_interest`                                      decimal(16,2) COMMENT '实还利息',
  `rel_fee`                                           decimal(16,2) COMMENT '实还费用',
  `period`                                            int           COMMENT '期次',
  `timestamp`                                         string        COMMENT '时间戳',
  `extra_info`                                        string        COMMENT '扩展信息',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `free_amount`                                       decimal(16,2) COMMENT '免息金额',
  `remainder_principal`                               decimal(16,2) COMMENT '剩余本金',
  `remainder_interest`                                decimal(16,2) COMMENT '剩余利息',
  `remainder_fee`                                     decimal(16,2) COMMENT '剩余费用',
  `remainder_periods`                                 int           COMMENT '剩余期数',
  `repay_type`                                        string        COMMENT '还款类型',
  `current_loan_balance`                              decimal(16,2) COMMENT '当期贷款余额',
  `account_status`                                    string        COMMENT '当期账户状态',
  `current_status`                                    string        COMMENT '当期状态',
  `overdue_day`                                       int           COMMENT '逾期天数',
  `finish_periods`                                    int           COMMENT '已还期数',
  `plan_begin_loan_principal`                         decimal(16,2) COMMENT '期初剩余本金',
  `plan_end_loan_principal`                           decimal(16,2) COMMENT '期末剩余本金',
  `plan_begin_loan_interest`                          decimal(16,2) COMMENT '期初剩余利息',
  `plan_end_loan_interest`                            decimal(16,2) COMMENT '期末剩余利息',
  `plan_begin_loan_fee`                               decimal(16,2) COMMENT '期初剩余费用',
  `plan_end_loan_fee`                                 decimal(16,2) COMMENT '期末剩余费用',
  `plan_remainder_periods`                            int           COMMENT '剩余期数',
  `plan_next_repay_date`                              string        COMMENT '下次应还日期',
  `real_interest_rate`                                decimal(16,2) COMMENT '实际执行利率',
  `repay_penalty`                                     decimal(16,2) COMMENT '应还罚息',
  `rel_penalty`                                       decimal(16,2) COMMENT '实还罚息'
) COMMENT '实际还款信息表'
STORED as PARQUET;


-- 文件08-资产处置过程信息表
-- DROP TABLE IF EXISTS `stage.asset_08_t_asset_disposal`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.asset_08_t_asset_disposal`(
  `id`                                                int           COMMENT '抵押物信息主键',
  `project_id`                                        string        COMMENT '项目id',
  `agency_id`                                         string        COMMENT '机构编号',
  `asset_id`                                          string        COMMENT '资产借据号',
  `disposi_status`                                    string        COMMENT '处置状态,0-已处置，1-未处置',
  `disposi_type`                                      string        COMMENT '处置类型，0-诉讼，1-非诉讼',
  `litigate_node`                                     string        COMMENT '诉讼节点，0-处置开始，1-诉讼准备，2-法院受理，3-执行拍卖，4-处置结束',
  `litigate_node_time`                                string        COMMENT '诉讼节点时间',
  `disposi_esult`                                     string        COMMENT '处置结果,0-经处置无拖欠,1-经处置已结清,2-经处置已核销',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间'
) COMMENT '资产处置过程信息表'
STORED as PARQUET;


-- 文件09-资产补充交易信息表
-- DROP TABLE IF EXISTS `stage.asset_09_t_asset_supplement`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.asset_09_t_asset_supplement`(
  `id`                                                int           COMMENT '抵押物信息主键',
  `project_id`                                        string        COMMENT '项目id',
  `agency_id`                                         string        COMMENT '机构编号',
  `asset_id`                                          string        COMMENT '资产借据号',
  `tradeType`                                         string        COMMENT '交易类型,0-回购,1-处置回收,2-代偿,3-差额补足',
  `tradeReason`                                       string        COMMENT '交易原因',
  `tradeDate`                                         string        COMMENT '交易日期',
  `trade_tol_amounts`                                 decimal(16,2) COMMENT '交易总金额',
  `principal`                                         decimal(16,2) COMMENT '本金',
  `interest`                                          decimal(16,2) COMMENT '利息',
  `punish_interest`                                   decimal(16,2) COMMENT '罚息',
  `oth_fee`                                           decimal(16,2) COMMENT '其他费用',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间'
) COMMENT '资产补充交易信息表'
STORED as PARQUET;


-- 文件10-资产对账信息表
-- DROP TABLE IF EXISTS `stage.asset_10_t_asset_check`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.asset_10_t_asset_check`(
  `id`                                                int           COMMENT '资产对账信息主键',
  `project_id`                                        string        COMMENT '项目id',
  `agency_id`                                         string        COMMENT '机构编号',
  `asset_id`                                          string        COMMENT '资产借据号',
  `account_date`                                      string        COMMENT '记账时间',
  `repayedPeriod`                                     int           COMMENT '已还期数',
  `remain_period`                                     int           COMMENT '剩余期数',
  `remain_principal`                                  decimal(16,2) COMMENT '剩余本金(元)',
  `remain_interest`                                   decimal(16,2) COMMENT '剩余利息(元)',
  `remain_othAmounts`                                 decimal(16,2) COMMENT '剩余其他费用(元)',
  `next_pay_date`                                     string        COMMENT '下一个还款日期',
  `assets_status`                                     string        COMMENT '资产状态，0-正常，1-逾期，2-已结清',
  `settle_reason`                                     string        COMMENT '结清原因,0-正常结清,1-提前结清,2-处置结束,3-资产回购',
  `current_overdue_principal`                         decimal(16,2) COMMENT '当前逾期本金',
  `current_overdue_interest`                          decimal(16,2) COMMENT '当前逾期利息',
  `current_overdue_fee`                               decimal(16,2) COMMENT '当前逾期费用',
  `current_overdue_days`                              int           COMMENT '当前逾期天数(天)',
  `accum_overdue_days`                                int           COMMENT '累计逾期天数',
  `his_accum_overdue_days`                            int           COMMENT '历史累计逾期天数',
  `his_overdue_mdays`                                 int           COMMENT '历史单次最长逾期天数',
  `current_overdue_period`                            int           COMMENT '当前逾期期数',
  `accum_overdue_period`                              int           COMMENT '累计逾期期数',
  `his_overdue_mperiods`                              int           COMMENT '历史单次最长逾期期数',
  `his_overdue_mprincipal`                            decimal(16,2) COMMENT '历史最大逾期本金',
  `extra_info`                                        string        COMMENT '扩展信息',
  `remark`                                            string        COMMENT '备注信息',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `loan_total_amount`                                 decimal(16,2) COMMENT '贷款总金额',
  `loan_interest_rate`                                decimal(16,2) COMMENT '贷款年利率(%)',
  `periods`                                           int           COMMENT '总期数',
  `recovered_total_amount`                            decimal(16,2) COMMENT '实还当天回收款总金额',
  `rel_principal`                                     decimal(16,2) COMMENT '实还本金',
  `rel_interest`                                      decimal(16,2) COMMENT '实还利息',
  `rel_fee`                                           decimal(16,2) COMMENT '实还费用',
  `current_continuity_overdays`                       int           COMMENT '当前连续逾期天数',
  `max_single_overduedays`                            int           COMMENT '最长单期逾期天数',
  `max_continuity_overdays`                           int           COMMENT '最长连续逾期天数',
  `accum_overdue_principal`                           decimal(16,2) COMMENT '历史累计逾期本金',
  `remain_penalty`                                    decimal(16,2) COMMENT '剩余罚息',
  `loan_settlement_date`                              string        COMMENT '结清日期',
  `loss_principal`                                    decimal(16,2) COMMENT '损失本金',
  `total_rel_amount`                                  decimal(16,2) COMMENT '累计实还金额',
  `total_rel_principal`                               decimal(16,2) COMMENT '累计实还本金',
  `total_rel_interest`                                decimal(16,2) COMMENT '累计实还利息',
  `total_rel_fee`                                     decimal(16,2) COMMENT '累计实还费用',
  `total_rel_penalty`                                 decimal(16,2) COMMENT '累计实还罚息',
  `rel_penalty`                                       decimal(16,2) COMMENT '当天实还罚息',
  `curr_period`                                       int           COMMENT '当前期次',
  `first_term_overdue`                                string        COMMENT '首期是否逾期'
) COMMENT '资产对账信息表'
STORED as PARQUET;


-- 文件11-项目对账信息表
-- DROP TABLE IF EXISTS `stage.asset_11_t_project_check`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.asset_11_t_project_check`(
  `id`                                                int           COMMENT '抵押物信息主键',
  `project_id`                                        string        COMMENT '项目id',
  `agency_id`                                         string        COMMENT '机构编号',
  `loan_sums`                                         int           COMMENT '贷款总笔数',
  `loan_remain_principal`                             decimal(16,2) COMMENT '贷款剩余本金',
  `loan_contract_tol_amounts`                         decimal(16,2) COMMENT '贷款合同总金额',
  `extra_info`                                        string        COMMENT '扩展信息',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `extract_date`                                      string        COMMENT '数据提取日'
) COMMENT '项目对账信息表'
STORED as PARQUET;


-- 文件12-企业信息表
-- DROP TABLE IF EXISTS `stage.asset_12_t_enterprise_info`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.asset_12_t_enterprise_info`(
  `id`                                                int           COMMENT '抵押物信息主键',
  `project_id`                                        string        COMMENT '项目id',
  `agency_id`                                         string        COMMENT '机构编号',
  `asset_id`                                          string        COMMENT '资产借据号',
  `contract_role`                                     string        COMMENT '合同角色 0-主借款企业，1-共同借款企业,2-担保企业,3-无',
  `enterprise_name`                                   string        COMMENT '企业姓名',
  `business_number`                                   string        COMMENT '工商注册号',
  `organizate_code`                                   string        COMMENT '组织机构代码',
  `taxpayer_number`                                   string        COMMENT '纳税人识别号',
  `unified_credit_code`                               string        COMMENT '统一信用代码',
  `registered_address`                                string        COMMENT '注册地址',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `loan_type`                                         string        COMMENT '借款方类型',
  `industry`                                          string        COMMENT '企业行业',
  `legal_person_name`                                 string        COMMENT '法人姓名',
  `id_type`                                           string        COMMENT '法人证件类型',
  `id_no`                                             string        COMMENT '法人证件号',
  `legal_person_phone`                                string        COMMENT '法人手机号码',
  `phone`                                             string        COMMENT '企业联系电话',
  `operate_years`                                     int           COMMENT '企业运营年限',
  `is_linked`                                         string        COMMENT '是否挂靠企业',
  `province`                                          string        COMMENT '企业省份'
) COMMENT '企业信息表'
STORED as PARQUET;


-- 校验平台授信表（取浦发用）
-- DROP TABLE IF EXISTS `stage.asset_12_t_enterprise_info`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.asset_t_ods_credit`(
  `credit_id`                                         string        COMMENT '授信编号',
  `assessment_date`                                   string        COMMENT '评估日期',
  `credit_result`                                     string        COMMENT '授信结果',
  `credit_amount`                                     decimal(16,2) COMMENT '授信金额',
  `credit_validity`                                   string        COMMENT '授信有效期',
  `refuse_reason`                                     string        COMMENT '授信拒绝原因',
  `current_remain_credit_amount`                      decimal(16,2) COMMENT '当前剩余额度',
  `current_credit_amount_utilization_rate`            decimal(16,4) COMMENT '当前额度使用率',
  `accumulate_credit_amount_utilization_rate`         decimal(16,4) COMMENT '累计额度使用率',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `project_id`                                        string        COMMENT '项目编号',
  `agency_id`                                         string        COMMENT '机构编号',
  `ecif_no`                                           string        COMMENT 'ecifNo',
  `card_no`                                           string        COMMENT '身份证号码(脱敏处理)',
  `phone_no`                                          string        COMMENT '手机号码(脱敏处理)',
  `imei`                                              string        COMMENT 'imei号'
) COMMENT '校验平台授信表（取浦发用）'
STORED AS PARQUET;


-- 校验平台用信表（取浦发用）
-- DROP TABLE IF EXISTS `stage.asset_12_t_enterprise_info`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.asset_t_credit_loan`(
  `id`                                                int           COMMENT '主键',
  `credit_id`                                         string        COMMENT '授信编号',
  `loan_id`                                           string        COMMENT '借据编号',
  `project_id`                                        string        COMMENT '项目编号',
  `agency_id`                                         string        COMMENT '机构编号',
  `assessment_date`                                   string        COMMENT '评估日期',
  `asset_level`                                       string        COMMENT '资产等级',
  `credit_level`                                      string        COMMENT '信用等级',
  `anti_fraud_level`                                  string        COMMENT '反欺诈等级',
  `risk_control_result`                               string        COMMENT '风控结果',
  `refuse_reason`                                     string        COMMENT '风控结果',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `product_terms`                                     int           COMMENT '产品总期数',
  `ecif_no`                                           string        COMMENT 'ecifNo',
  `card_no`                                           string        COMMENT '身份证号码(脱敏处理)',
  `phone_no`                                          string        COMMENT '手机号码(脱敏处理)',
  `imei`                                              string        COMMENT 'imei号',
  `apply_amt`                                         decimal(16,2) COMMENT '申请用信金额',
  `approval_amt`                                      decimal(16,2) COMMENT '用信审批通过金额',
  `first_use_credit`                                  int           COMMENT 'credit_id是否存在'
) COMMENT '校验平台用信表（取浦发用）'
STORED AS PARQUET;


-- 星云风控数据
-- DROP TABLE IF EXISTS `stage.duration_result`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.duration_result`(
  `id`                                                int           COMMENT '主键',
  `request_id`                                        int           COMMENT '存续期数据跑批申请表主键',
  `swift_no`                                          string        COMMENT '流水号',
  `apply_no`                                          string        COMMENT '借据号',
  `project_name`                                      string        COMMENT '项目编号',
  `name`                                              string        COMMENT '姓名',
  `card_no`                                           string        COMMENT '身份证号码',
  `mobile`                                            string        COMMENT '手机号',
  `is_settle`                                         string        COMMENT '是否已结清',
  `execute_month`                                     string        COMMENT '执行月份（YYYY-MM）',
  `score_range_t1`                                    string        COMMENT 'T-1月资产等级',
  `score_range_t2`                                    string        COMMENT 'T-2月资产等级',
  `score_range`                                       string        COMMENT '资产等级',
  `inner_black`                                       string        COMMENT '内部黑名单（1：命中，2：未命中）',
  `focus`                                             string        COMMENT '关注名单（1：关注，0：非关注）',
  `state`                                             string        COMMENT '数据状态（0：无效，1：处理中，2：处理成功，3：处理失败）',
  `error_msg`                                         string        COMMENT '失败原因',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '修改时间'
) COMMENT '存续期数据跑批结果表'
STORED AS PARQUET;










-- 文件01-借款合同信息表
-- DROP TABLE IF EXISTS `stage.abs_01_t_loancontractinfo`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.abs_01_t_loancontractinfo`(
  `id`                                                int           COMMENT '借款合同信息主键',
  `project_id`                                        string        COMMENT '项目编号',
  `agency_id`                                         string        COMMENT '机构编号',
  `serial_number`                                     string        COMMENT '借据号',
  `asset_type`                                        string        COMMENT '资产类型-枚举预定义字段：个人消费贷款 汽车抵押贷款 住房抵押贷款',
  `contract_code`                                     string        COMMENT '贷款合同编号',
  `product_id`                                        string        COMMENT '产品编号',
  `product_scheme_name`                               string        COMMENT '产品方案名称',
  `contract_amount`                                   decimal(20,6) COMMENT '贷款总金额(元)',
  `interest_rate_type`                                string        COMMENT '利率类型-枚举 固定利率 浮动利率',
  `contract_interest_rate`                            decimal(20,6) COMMENT '贷款年利率(%)',
  `contract_daily_rate`                               decimal(20,6) COMMENT '贷款日利率(%)',
  `contract_an_rate`                                  decimal(20,6) COMMENT '贷款月利率(%)',
  `daily_calculation_basis`                           string        COMMENT '日利率计算基础-枚举预定义字段： 360， 365',
  `repayment_type`                                    string        COMMENT '还款方式-枚举预定义字段： 等额本息， 等额本金， 等本等息， 先息后本， 一次性还本付息 气球贷 自定义还款计划',
  `repayment_frequency`                               string        COMMENT '还款频率-枚举预定义字段：月，季，半年，年，到期一次付清 自定义还款频率',
  `periods`                                           int           COMMENT '总期数',
  `profit_tol_rate`                                   decimal(20,6) COMMENT '总收益率(%)',
  `rest_principal`                                    decimal(20,6) COMMENT '剩余本金(元)',
  `rest_other_cost`                                   decimal(20,6) COMMENT '剩余费用其他(元)',
  `rest_interest`                                     decimal(20,6) COMMENT '剩余利息(元)',
  `shoufu_amount`                                     decimal(20,6) COMMENT '首付款金额(元)',
  `shoufu_proportion`                                 decimal(20,6) COMMENT '首付比例(%)',
  `tail_amount`                                       decimal(20,6) COMMENT '尾付款金额(元)',
  `tail_amount_rate`                                  decimal(20,6) COMMENT '尾付比例(%)',
  `poundage`                                          decimal(20,6) COMMENT '手续费',
  `poundage_rate`                                     decimal(20,6) COMMENT '手续费利率',
  `poundage_deduction_type`                           string        COMMENT '手续费扣款方式-枚举 预定义字段： 一次性 分期扣款',
  `settlement_poundage_rate`                          decimal(20,6) COMMENT '结算手续费率',
  `subsidie_poundage`                                 decimal(20,6) COMMENT '补贴手续费',
  `discount_amount`                                   decimal(20,6) COMMENT '贴息金额',
  `margin_rate`                                       decimal(20,6) COMMENT '保证金比例',
  `margin`                                            decimal(20,6) COMMENT '保证金',
  `margin_offset_type`                                string        COMMENT '保证金冲抵方式-枚举预定义字段：不冲抵 冲抵',
  `account_management_expense`                        decimal(20,6) COMMENT '帐户管理费',
  `mortgage_rate`                                     decimal(20,6) COMMENT '抵押率(%)',
  `loan_issue_date`                                   string        COMMENT '合同开始时间',
  `loan_expiry_date`                                  string        COMMENT '合同结束时间',
  `actual_loan_date`                                  string        COMMENT '实际放款时间',
  `frist_repayment_date`                              string        COMMENT '首次还款时间',
  `loan_repay_date`                                   string        COMMENT '贷款还款日',
  `last_estimated_deduction_date`                     string        COMMENT '最后一次预计扣款时间',
  `month_repay_amount`                                decimal(20,6) COMMENT '月还款额',
  `loan_use`                                          string        COMMENT '贷款用途-车抵押,旅游，教育，医美，经营类 其他 农业，个人综合消费',
  `loan_application`                                  string        COMMENT '申请用途',
  `guarantee_type`                                    string        COMMENT '担保方式-枚举 预定义字段：质押担保，信用担保，保证担保，抵押担保',
  `contract_term`                                     decimal(20,6) COMMENT '合同期限(月)',
  `total_overdue_daynum`                              int           COMMENT '累计逾期期数',
  `history_most_overdue_daynum`                       int           COMMENT '历史最高逾期天数',
  `history_total_overdue_daynum`                      int           COMMENT '历史累计逾期天数',
  `current_overdue_daynum`                            int           COMMENT '当前逾期天数',
  `contract_status`                                   string        COMMENT '合同状态-枚举 预定义字段：生效、不生效',
  `apply_status_code`                                 string        COMMENT '申请状态代码-枚举 预定义字段：已放款、放款失败',
  `apply_channel`                                     string        COMMENT '申请渠道',
  `apply_place`                                       string        COMMENT '申请地点',
  `borrowerp_status`                                  string        COMMENT '借款人状态 1-首次申请、2-内部续贷、3-其他机构转单、9-其他',
  `dealer_name`                                       string        COMMENT '经销商名称',
  `dealer_sale_address`                               string        COMMENT '经销商卖场地址',
  `store_provinces`                                   string        COMMENT '店面省份',
  `store_cities`                                      string        COMMENT '店面城市',
  `data_extraction_day`                               string        COMMENT '数据提取日',
  `remaining_life_extraction`                         decimal(20,6) COMMENT '提取日剩余期限(月)',
  `borrower_type`                                     string        COMMENT '客户类型',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `import_id`                                         int           COMMENT '导入Id',
  `data_source`                                       int           COMMENT '数据来源 1:startLink,2:excelImport'
) COMMENT '借款合同信息表'
STORED as PARQUET;


-- 文件02-主借款人信息表
-- DROP TABLE IF EXISTS `stage.abs_02_t_borrowerinfo`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.abs_02_t_borrowerinfo`(
  `id`                                                int           COMMENT '主借款人信息主键',
  `project_id`                                        string        COMMENT '项目编号',
  `agency_id`                                         string        COMMENT '机构编号',
  `serial_number`                                     string        COMMENT '借据号',
  `borrower_name`                                     string        COMMENT '客户姓名',
  `certificate_type`                                  string        COMMENT '证件类型 预定义字段：身份证 护照 户口本 外国人护照',
  `document_num`                                      string        COMMENT '身份证号',
  `phone_num`                                         string        COMMENT '手机号',
  `age`                                               int           COMMENT '年龄',
  `sex`                                               string        COMMENT '性别-枚举 男，女',
  `marital_status`                                    string        COMMENT '婚姻状况-枚举 预定义字段：已婚，未婚，离异，丧偶',
  `children_number`                                   int           COMMENT '子女数量',
  `work_years`                                        string        COMMENT '工作年限',
  `customer_type`                                     string        COMMENT '客户类型-枚举 "01-农户 02-工薪 03-个体工商户 04-学生 99-其他"',
  `education_level`                                   string        COMMENT '学历-枚举 预定义字段： 小学， 初中， 高中/职高/技校， 大专， 本科, 硕士, 博士， 文盲和半文盲',
  `degree`                                            string        COMMENT '学位',
  `is_capacity_civil_conduct`                         string        COMMENT '是否具有完全民事行为能力"预定义字段：是 否"',
  `living_state`                                      string        COMMENT '居住状态',
  `province`                                          string        COMMENT '客户所在省 传输省份城市代码则提供对应关系',
  `city`                                              string        COMMENT '客户所在市 传输省份城市代码则提供对应关系',
  `address`                                           string        COMMENT '客户居住地址',
  `house_province`                                    string        COMMENT '客户户籍所在省',
  `house_city`                                        string        COMMENT '客户户籍所在市',
  `house_address`                                     string        COMMENT '客户户籍地址',
  `communications_zip_code`                           string        COMMENT '通讯邮编',
  `mailing_address`                                   string        COMMENT '客户通讯地址',
  `career`                                            string        COMMENT '客户职业',
  `working_state`                                     string        COMMENT '工作状态 "预定义字段：在职，失业"',
  `position`                                          string        COMMENT '职务',
  `title`                                             string        COMMENT '职称',
  `borrower_industry`                                 string        COMMENT '借款人行业-枚举 借款人行业、NIL：空、A：农、林、牧、渔业、B：采矿业、C：制造业、D：电力、热力、燃气及水生产和供应业、E：建筑业、F：批发和零售业、G：交通运输、仓储和邮政业、H：住宿和餐饮业、I：信息传输、软件和信息技术服务业、J：金融业、K：房地产业、L：租赁和商务服务业、M：科学研究和技术服务业、N：水利、环境和公共设施管理业、O：居民服务、修理和其他服务业、P：教育、Q：卫生和社会工作、R：文化、体育和娱乐业、S：公共管理、社会保障和社会组织、T：国际组织、Z：其他',
  `is_car`                                            string        COMMENT '是否有车"预定义字段:是 否"',
  `is_mortgage_financing`                             string        COMMENT '是否有按揭车贷"预定义字段:是 否"',
  `is_house`                                          string        COMMENT '是否有房"预定义字段:是 否"',
  `is_mortgage_loans`                                 string        COMMENT '是否有按揭房贷"预定义字段:是 否"',
  `is_credit_card`                                    string        COMMENT '是否有信用卡"预定义字段:是 否"',
  `credit_limit`                                      decimal(20,6) COMMENT '信用卡额度',
  `annual_income`                                     decimal(20,6) COMMENT '年收入(元)',
  `internal_credit_rating`                            string        COMMENT '内部信用等级',
  `blacklist_level`                                   string        COMMENT '黑名单等级',
  `unit_name`                                         string        COMMENT '单位名称',
  `fixed_telephone`                                   string        COMMENT '固定电话',
  `zip_code`                                          string        COMMENT '邮编',
  `unit_address`                                      string        COMMENT '单位详细地址',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `import_id`                                         int           COMMENT '导入Id',
  `data_source`                                       int           COMMENT '数据来源 1:startLink,2:excelImport'
) COMMENT '主借款人信息表'
STORED as PARQUET;


-- 文件03-关联人信息表
-- DROP TABLE IF EXISTS `stage.abs_03_t_associatesinfo`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.abs_03_t_associatesinfo`(
  `id`                                                int           COMMENT '关联人信息主键',
  `project_id`                                        string        COMMENT '项目编号',
  `agency_id`                                         string        COMMENT '机构编号',
  `serial_number`                                     string        COMMENT '借据号',
  `contract_role`                                     string        COMMENT '合同角色-枚举 预定义字段: 共同借款人, 担保人, 无',
  `borrower_name`                                     string        COMMENT '客户姓名',
  `certificate_type`                                  string        COMMENT '证件类型-预定义字段：身份证 护照 户口本 外国人护照',
  `document_num`                                      string        COMMENT '身份证号',
  `phone_num`                                         string        COMMENT '手机号',
  `age`                                               int           COMMENT '年龄',
  `sex`                                               string        COMMENT '性别预定义字段：男，女',
  `relationship_with_borrowers`                       string        COMMENT '与借款人关系 预定义字段：配偶 父母 子女 亲戚 朋友 同事',
  `career`                                            string        COMMENT '职业',
  `working_state`                                     string        COMMENT '工作状态 预定义字段：在职 失业',
  `annual_income`                                     decimal(20,6) COMMENT '年收入(元)',
  `mailing_address`                                   string        COMMENT '通讯地址',
  `unit_address`                                      string        COMMENT '单位详细地址',
  `unit_contact_mode`                                 string        COMMENT '单位联系方式',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `import_id`                                         int           COMMENT '导入Id',
  `data_source`                                       int           COMMENT '数据来源 1:startLink,2:excelImport'
) COMMENT '关联人信息表'
STORED as PARQUET;


-- 文件04-抵押物(车)信息表
-- DROP TABLE IF EXISTS `stage.abs_04_t_guarantycarinfo`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.abs_04_t_guarantycarinfo`(
  `id`                                                int           COMMENT '抵押物(车)信息主键',
  `project_id`                                        string        COMMENT '项目编号',
  `agency_id`                                         string        COMMENT '机构编号',
  `serial_number`                                     string        COMMENT '借据号',
  `guaranty_code`                                     string        COMMENT '抵押物编号',
  `guaranty_handling_status`                          string        COMMENT '抵押办理状态 预定义字段：办理中 办理完成 尚未办理',
  `guaranty_alignment`                                string        COMMENT '抵押顺位 1-第一顺位 2-第二顺位 9-其他',
  `car_property`                                      string        COMMENT '车辆性质 预定义字段：非融资车分期非 融资车抵贷 融资租赁车分期 融资租赁车抵贷',
  `financing_type`                                    string        COMMENT '融资方式 预定义字段：正租 反租',
  `guarantee_type`                                    string        COMMENT '担保方式 预定义字段：质押担保，信用担保，保证担保，抵押担保',
  `pawn_value`                                        decimal(20,6) COMMENT '评估价格(元)',
  `car_sales_price`                                   decimal(20,6) COMMENT '车辆销售价格(元)',
  `car_new_price`                                     decimal(20,6) COMMENT '新车指导价(元)',
  `total_investment`                                  decimal(20,6) COMMENT '投资总额(元)',
  `purchase_tax_amouts`                               decimal(20,6) COMMENT '购置税金额(元)',
  `insurance_type`                                    string        COMMENT '保险种类  交强险 第三者责任险 盗抢险 车损险 不计免赔 其他',
  `car_insurance_premium`                             decimal(20,6) COMMENT '汽车保险总费用',
  `total_poundage`                                    decimal(20,6) COMMENT '手续总费用(元)',
  `cumulative_car_transfer_number`                    int           COMMENT '累计车辆过户次数',
  `one_year_car_transfer_number`                      int           COMMENT '一年内车辆过户次数',
  `liability_insurance_cost1`                         decimal(20,6) COMMENT '责信保费用1',
  `liability_insurance_cost2`                         decimal(20,6) COMMENT '责信保费用2',
  `car_type`                                          string        COMMENT '车类型 预定义字段：新车 二手车',
  `frame_num`                                         string        COMMENT '车架号',
  `engine_num`                                        string        COMMENT '发动机号',
  `gps_code`                                          string        COMMENT 'GPS编号',
  `gps_cost`                                          decimal(20,6) COMMENT 'GPS费用',
  `license_num`                                       string        COMMENT '车牌号码',
  `car_brand`                                         string        COMMENT '车辆品牌',
  `car_system`                                        string        COMMENT '车系',
  `car_model`                                         string        COMMENT '车型',
  `car_age`                                           decimal(20,6) COMMENT '车龄',
  `car_energy_type`                                   string        COMMENT '车辆能源类型 预定义字段： 混合动力 纯电 非新能源车',
  `production_date`                                   string        COMMENT '生产日期',
  `mileage`                                           decimal(20,6) COMMENT '里程数',
  `register_date`                                     string        COMMENT '注册日期',
  `buy_car_address`                                   string        COMMENT '车辆购买地',
  `car_colour`                                        string        COMMENT '车辆颜色',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `import_id`                                         int           COMMENT '导入Id',
  `data_source`                                       int           COMMENT '数据来源 1:startLink,2:excelImport'
) COMMENT '抵押物(车)信息表'
STORED as PARQUET;


-- 文件05-还款计划历史
-- DROP TABLE IF EXISTS `stage.abs_05_t_repaymentplan_history`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.abs_05_t_repaymentplan_history`(
  `id`                                                int           COMMENT '还款计划主键',
  `project_id`                                        string        COMMENT '项目编号',
  `agency_id`                                         string        COMMENT '机构编号',
  `serial_number`                                     string        COMMENT '借据号',
  `period`                                            int           COMMENT '期次',
  `should_repay_date`                                 string        COMMENT '应还款日',
  `should_repay_principal`                            decimal(20,6) COMMENT '应还本金',
  `should_repay_interest`                             decimal(20,6) COMMENT '应还利息',
  `should_repay_cost`                                 decimal(20,6) COMMENT '应还费用',
  `begin_principal_balance`                           decimal(20,6) COMMENT '期初剩余本金',
  `end_principal_balance`                             decimal(20,6) COMMENT '期末剩余本金',
  `effective_date`                                    string        COMMENT '生效日期',
  `repay_status`                                      int           COMMENT '还款状态 1:入池前已还,2:入池前未还',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `import_id`                                         int           COMMENT '导入Id',
  `data_source`                                       int           COMMENT '数据来源 1:startLink,2:excelImport,3:systemgenerated'
) COMMENT '还款计划历史'
STORED as PARQUET;


-- 文件06-资产交易支付流水信息表
-- DROP TABLE IF EXISTS `stage.abs_06_t_assettradeflow`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.abs_06_t_assettradeflow`(
  `id`                                                int           COMMENT '资产交易支付流水信息',
  `project_id`                                        string        COMMENT '项目编号',
  `agency_id`                                         string        COMMENT '机构编号',
  `serial_number`                                     string        COMMENT '借据号',
  `trade_channel`                                     string        COMMENT '交易渠道',
  `trade_type`                                        string        COMMENT '交易类型',
  `order_number`                                      string        COMMENT '订单编号',
  `order_amount`                                      decimal(20,6) COMMENT '订单金额',
  `trade_currency`                                    string        COMMENT '币种',
  `name`                                              string        COMMENT '姓名',
  `bank_account`                                      string        COMMENT '银行账号',
  `trade_time`                                        string        COMMENT '交易日期',
  `trade_status`                                      string        COMMENT '交易状态',
  `trade_digest`                                      string        COMMENT '交易摘要',
  `confirm_repay_time`                                string        COMMENT '确认还款日期',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `import_id`                                         int           COMMENT '导入Id',
  `data_source`                                       int           COMMENT '数据来源 1:startLink,2:excelImport'
) COMMENT '资产交易支付流水信息表'
STORED as PARQUET;


-- 文件07-实际还款信息表
-- DROP TABLE IF EXISTS `stage.abs_07_t_actualrepayinfo`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.abs_07_t_actualrepayinfo`(
  `id`                                                int           COMMENT '实际还款信息主键',
  `project_id`                                        string        COMMENT '项目编号',
  `agency_id`                                         string        COMMENT '机构编号',
  `serial_number`                                     string        COMMENT '借据号',
  `term`                                              int           COMMENT '期次',
  `shoud_repay_date`                                  string        COMMENT '应还款日',
  `shoud_repay_principal`                             decimal(20,6) COMMENT '应还本金（元）',
  `shoud_repay_interest`                              decimal(20,6) COMMENT '应还利息（元）',
  `shoud_repay_fee`                                   decimal(20,6) COMMENT '应还费用（元）',
  `repay_type`                                        string        COMMENT '还款类型 1，提前还款 2，正常还款 3，部分还款 4，逾期还款',
  `actual_work_interest_rate`                         decimal(20,6) COMMENT '实际还款执行利率',
  `actual_repay_principal`                            decimal(20,6) COMMENT '实还本金（元）',
  `actual_repay_interest`                             decimal(20,6) COMMENT '实还利息（元）',
  `actual_repay_fee`                                  decimal(20,6) COMMENT '实还费用（元）',
  `actual_repay_time`                                 string        COMMENT '实际还清日期：（若没有还清传还款日期，若还清就传实际还清的日期）',
  `current_period_loan_balance`                       decimal(20,6) COMMENT '当期贷款余额：（各期还款日（T+1）更新该字段，即截至当期还款日资产的剩余（未偿还）贷款本金余额）',
  `current_account_status`                            string        COMMENT '当期账户状态：（各期还款日（T+1）更新该字段；正常：在还款日前该期应还已还清，提前还清（早偿）：贷款在该期还款日前提前全部还清，逾期：贷款在该期还款日时，实还金额小于应还金额）',
  `penalbond`                                         decimal(20,6) COMMENT '违约金',
  `penalty_interest`                                  decimal(20,6) COMMENT '罚息',
  `compensation`                                      decimal(20,6) COMMENT '赔偿金（提前还款/逾期所产生的赔偿金）',
  `advanced_commission_charge`                        decimal(20,6) COMMENT '提前还款手续费',
  `other_fee`                                         decimal(20,6) COMMENT '其他相关费用 （违约金、罚款、赔偿金和提前还款手续费以外的费用）',
  `is_borrowers_oneself_repayment`                    string        COMMENT '是否借款人本人还款 预定义字段 Y N',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `import_id`                                         int           COMMENT '导入Id',
  `data_source`                                       int           COMMENT '数据来源 1:startLink,2:excelImport'
) COMMENT '实际还款信息表'
STORED as PARQUET;


-- 文件08-资产处置过程信息表
-- DROP TABLE IF EXISTS `stage.abs_08_t_assetdealprocessinfo`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.abs_08_t_assetdealprocessinfo`(
  `id`                                                int           COMMENT '资产处置过程信息主键',
  `project_id`                                        string        COMMENT '项目编号',
  `agency_id`                                         string        COMMENT '机构编号',
  `serial_number`                                     string        COMMENT '借据号',
  `deal_status`                                       string        COMMENT '处置状态（处置中 已处置 ）',
  `deal_type`                                         string        COMMENT '处置类型（SUSONG-诉讼 FEISONGSU-非诉讼 ）',
  `lawsuit_node`                                      string        COMMENT '诉讼节点 处置开始 诉讼准备 法院受理 执行拍卖 处置结束',
  `lawsuit_node_time`                                 string        COMMENT '诉讼节点时间',
  `deal_result`                                       string        COMMENT '处置结果',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `import_id`                                         int           COMMENT '导入Id',
  `data_source`                                       int           COMMENT '数据来源 1:startLink,2:excelImport'
) COMMENT '资产处置过程信息表'
STORED as PARQUET;


-- 文件09-资产补充交易信息表
-- DROP TABLE IF EXISTS `stage.abs_09_t_assetaddtradeinfo`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.abs_09_t_assetaddtradeinfo`(
  `id`                                                int           COMMENT '资产补充交易信息主键',
  `project_id`                                        string        COMMENT '项目编号',
  `agency_id`                                         string        COMMENT '机构编号',
  `serial_number`                                     string        COMMENT '借据号',
  `trade_type`                                        string        COMMENT '交易类型',
  `trade_reason`                                      string        COMMENT '交易原因',
  `trade_time`                                        string        COMMENT '交易日期',
  `trade_total_amount`                                decimal(20,6) COMMENT '交易总金额',
  `principal`                                         decimal(20,6) COMMENT '本金',
  `interest`                                          decimal(20,6) COMMENT '利息',
  `penalty_interest`                                  decimal(20,6) COMMENT '罚息',
  `other_fee`                                         decimal(20,6) COMMENT '其他费用',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `import_id`                                         int           COMMENT '导入Id',
  `data_source`                                       int           COMMENT '数据来源 1:startLink,2:excelImport'
) COMMENT '资产补充交易信息表'
STORED as PARQUET;


-- 文件10-资产对账信息表
-- DROP TABLE IF EXISTS `stage.abs_10_t_assetaccountcheck`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.abs_10_t_assetaccountcheck`(
  `id`                                                int           COMMENT '资产对账信息主键',
  `project_id`                                        string        COMMENT '项目编号',
  `agency_id`                                         string        COMMENT '机构编号',
  `serial_number`                                     string        COMMENT '借据号',
  `is_change_repay_schedule`                          string        COMMENT '是否变更还款计划（YES - 是，NO - 否 ）',
  `change_time`                                       string        COMMENT '变更时间',
  `total_loan_amount`                                 decimal(20,6) COMMENT '贷款总金额',
  `year_loan_rate`                                    string        COMMENT '贷款年利率',
  `total_term`                                        int           COMMENT '总期数',
  `already_repay_term`                                int           COMMENT '已还期数',
  `remain_term`                                       int           COMMENT '剩余期数',
  `remain_principal`                                  decimal(20,6) COMMENT '剩余本金',
  `remain_interest`                                   decimal(20,6) COMMENT '剩余利息',
  `remain_other_fee`                                  decimal(20,6) COMMENT '剩余其他费用',
  `settle_payment_advance`                            decimal(20,6) COMMENT '提前结清实还费用',
  `early_settlement_interest`                         decimal(20,6) COMMENT '提前结清实还利息',
  `principal_settled_advance`                         decimal(20,6) COMMENT '提前结清实还本金',
  `next_term_shouldrepay_time`                        string        COMMENT '下一期应还款日',
  `asset_status`                                      string        COMMENT '资产状态',
  `closeoff_reason`                                   string        COMMENT '结清原因',
  `current_overdue_principal`                         decimal(20,6) COMMENT '当前逾期本金',
  `current_overdue_interest`                          decimal(20,6) COMMENT '当前逾期利息',
  `current_overdue_fee`                               decimal(20,6) COMMENT '当前逾期费用',
  `current_overdue_daynum`                            int           COMMENT '当前逾期天数',
  `total_overdue_daynum`                              int           COMMENT '累计逾期天数',
  `history_most_overdue_daynum`                       int           COMMENT '历史最高逾期天数',
  `history_total_overdue_daynum`                      int           COMMENT '历史累计逾期天数',
  `current_overdue_termnum`                           int           COMMENT '当前逾期期数',
  `total_overdue_termnum`                             int           COMMENT '累计逾期期数',
  `history_longest_overdue_term`                      int           COMMENT '历史单次最长逾期期数',
  `history_top_overdue_principal`                     decimal(20,6) COMMENT '历史最大逾期本金',
  `data_extract_time`                                 string        COMMENT '数据提取日',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `import_id`                                         int           COMMENT '导入Id',
  `data_source`                                       int           COMMENT '数据来源 1:startLink,2:excelImport,3:systemgenerated'
) COMMENT '资产对账信息表'
STORED as PARQUET;


-- 文件11-项目对账信息表
-- DROP TABLE IF EXISTS `stage.abs_11_t_projectaccountcheck`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.abs_11_t_projectaccountcheck`(
  `id`                                                int           COMMENT '项目对账信息主键',
  `project_id`                                        string        COMMENT '项目编号',
  `agency_id`                                         string        COMMENT '机构编号',
  `loan_sum`                                          int           COMMENT '贷款总数量',
  `loan_remaining_principal`                          decimal(20,6) COMMENT '贷款剩余本金',
  `loan_totalamount`                                  decimal(20,6) COMMENT '贷款合同总金额',
  `overdue_assets_sum1`                               int           COMMENT '1~7逾期资产数',
  `overdue_assets_outstanding_principal1`             decimal(20,6) COMMENT '1~7逾期资产剩余未还本金',
  `overdue_assets_sum8`                               int           COMMENT '8~30逾期资产数',
  `overdue_assets_outstanding_principal8`             decimal(20,6) COMMENT '8~30逾期资产剩余未还本金',
  `overdue_assets_sum31`                              int           COMMENT '31~60逾期资产数',
  `overdue_assets_outstanding_principal31`            decimal(20,6) COMMENT '31~60逾期资产剩余未还本金',
  `overdue_assets_sum61`                              int           COMMENT '61~90逾期资产数',
  `overdue_assets_outstanding_principal61`            decimal(20,6) COMMENT '61~90逾期资产剩余未还本金',
  `overdue_assets_sum91`                              int           COMMENT '91~180逾期资产数',
  `overdue_assets_outstanding_principal91`            decimal(20,6) COMMENT '91~180逾期资产剩余未还本金',
  `overdue_assets_sum180`                             int           COMMENT '180+逾期资产数',
  `overdue_assets_outstanding_principal180`           decimal(20,6) COMMENT '180+逾期资产剩余未还本金',
  `add_loan_sum`                                      int           COMMENT '当日新增贷款笔数',
  `add_loan_totalamount`                              decimal(20,6) COMMENT '当日新增贷款总金额',
  `actual_payment_sum`                                int           COMMENT '当日实还资产笔数',
  `actual_payment_totalamount`                        decimal(20,6) COMMENT '当日实还总金额',
  `back_assets_sum`                                   int           COMMENT '当日回购资产笔数',
  `back_assets_totalamount`                           decimal(20,6) COMMENT '当日回购总金额',
  `disposal_assets_sum`                               int           COMMENT '当日处置资产笔数',
  `disposal_assets_totalamount`                       decimal(20,6) COMMENT '当日处置回收总金额',
  `differentia_complement_assets_sum`                 int           COMMENT '当日差额补足资产笔数',
  `differentia_complement_assets_totalamount`         decimal(20,6) COMMENT '当日差额补足总金额',
  `compensatory_assets_sum`                           int           COMMENT '当日代偿资产笔数',
  `compensatory_assets_totalamount`                   decimal(20,6) COMMENT '当日代偿总金额',
  `data_extraction_day`                               string        COMMENT '数据提取日',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `import_id`                                         int           COMMENT '导入Id',
  `data_source`                                       int           COMMENT '数据来源 1:startLink,2:excelImport'
) COMMENT '项目对账信息表'
STORED as PARQUET;


-- 文件12-企业名称信息表
-- DROP TABLE IF EXISTS `stage.abs_12_t_enterpriseinfo`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.abs_12_t_enterpriseinfo`(
  `id`                                                int           COMMENT '企业名称信息主键',
  `project_id`                                        string        COMMENT '项目编号',
  `agency_id`                                         string        COMMENT '机构编号',
  `serial_number`                                     string        COMMENT '借据号',
  `contract_role`                                     string        COMMENT '合同角色 预定义字段: 主借款企业 共同借款企业 担保企业 无',
  `enterprise_name`                                   string        COMMENT '企业姓名',
  `registration_number`                               string        COMMENT '工商注册号',
  `organization_code`                                 string        COMMENT '组织机构代码',
  `taxpayer_identification_number`                    string        COMMENT '纳税人识别号',
  `uniform_credit_code`                               string        COMMENT '统一信用代码',
  `registered_address`                                string        COMMENT '注册地址',
  `borrower_type`                                     string        COMMENT '借款人类别',
  `enterprise_industry`                               string        COMMENT '企业行业',
  `enterprise_phoneno`                                string        COMMENT '企业联系方式',
  `operation_years`                                   int           COMMENT '经营年限',
  `is_affiliated`                                     string        COMMENT '是否挂靠',
  `legal_person_name`                                 string        COMMENT '法人姓名',
  `legal_person_card_type`                            string        COMMENT '法人证件类型',
  `legal_person_card_no`                              string        COMMENT '法人证件号',
  `legal_person_phoneno`                              string        COMMENT '法人手机号',
  `registered_province`                               string        COMMENT '注册省份',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `import_id`                                         int           COMMENT '导入Id',
  `data_source`                                       int           COMMENT '数据来源 1:startLink,2:excelImport'
) COMMENT '企业名称信息表'
STORED as PARQUET;


-- 文件13-房抵押物信息表
-- DROP TABLE IF EXISTS `stage.abs_13_t_guarantyhouseinfo`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.abs_13_t_guarantyhouseinfo`(
  `id`                                                int           COMMENT '房抵押物信息主键',
  `project_id`                                        string        COMMENT '项目编号',
  `agency_id`                                         string        COMMENT '机构编号',
  `serial_number`                                     string        COMMENT '借据号',
  `guaranty_number`                                   string        COMMENT '抵押物编号',
  `guaranty_name`                                     string        COMMENT '抵押物名称',
  `guaranty_describe`                                 string        COMMENT '抵押物描述',
  `guaranty_handle_status`                            string        COMMENT '抵押办理状态 预定义字段 办理中 办理完成 尚未办理',
  `guaranty_alignment`                                string        COMMENT '抵押顺位 第一顺位 第二顺位 其他',
  `guaranty_front_hand_balance`                       decimal(20,6) COMMENT '前手抵押余额',
  `guaranty_type`                                     string        COMMENT '抵押类型',
  `ownership_name`                                    string        COMMENT '所有权人姓名',
  `ownership_document_type`                           string        COMMENT '所有权人证件类型 预定义字段：身份证 护照 户口本 外国人护照',
  `ownership_document_number`                         string        COMMENT '所有权人证件号码',
  `ownership_job`                                     string        COMMENT '所有人职业 0 1 3 4 5 6 X Y Z',
  `is_guaranty_ownership_only_domicile`               string        COMMENT '押品是否为所有权人/借款人名下唯一住所 1 2 3 9',
  `house_area`                                        decimal(20,6) COMMENT '房屋建筑面积 平米',
  `house_age`                                         decimal(20,6) COMMENT '楼龄',
  `house_location_province`                           string        COMMENT '房屋所在省',
  `house_location_city`                               string        COMMENT '房屋所在城市',
  `house_location_district_county`                    string        COMMENT '房屋所在区县',
  `house_address`                                     string        COMMENT '房屋地址',
  `property_years`                                    decimal(20,6) COMMENT '产权年限',
  `purchase_contract_number`                          string        COMMENT '购房合同编号',
  `warrant_type`                                      string        COMMENT '权证类型 房产证 房屋他项权证',
  `property_certificate_number`                       string        COMMENT '房产证编号',
  `house_warrant_number`                              string        COMMENT '房屋他项权证编号',
  `house_type`                                        string        COMMENT '房屋类别 01 02 03 04 05 06 07 08 09 00',
  `is_property_right_co_owner`                        string        COMMENT '是否有产权共有人',
  `property_co_owner_informed_situation`              string        COMMENT '产权共有人知情情况 1 2 3 9',
  `guaranty_registration`                             string        COMMENT '抵押登记办理 已完成 已递交申请并取得回执 尚未办理',
  `enforcement_notarization`                          string        COMMENT '强制执行公证 已完成 已递交申请并取得回执 尚未办理',
  `is_arbitration_prove`                              string        COMMENT '网络仲裁办仲裁证明',
  `assessment_price_evaluation_company`               decimal(20,6) COMMENT '评估价格-评估公司(元)',
  `assessment_price_letting_agent`                    decimal(20,6) COMMENT '评估价格-房屋中介(元)',
  `assessment_price_original_rights_day`              decimal(20,6) COMMENT '评估价格-原始权益日内部评估(元)',
  `house_selling_price`                               decimal(20,6) COMMENT '房屋销售价格(元)',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间',
  `import_id`                                         int           COMMENT '导入Id',
  `data_source`                                       int           COMMENT '数据来源 1:startLink,2:excelImport'
) COMMENT '房抵押物信息表'
STORED as PARQUET;


-- 基础资产表
-- DROP TABLE IF EXISTS `stage.abs_t_basic_asset`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.abs_t_basic_asset` (
  `id`                                 int            COMMENT '主键id',
  `import_id`                          int            COMMENT '导入记录编号',
  `project_id`                         string         COMMENT '项目编号',
  `asset_bag_id`                       string         COMMENT '资产包编号',
  `asset_type`                         string         COMMENT '资产类型',
  `serial_number`                      string         COMMENT '借据号',
  `contract_code`                      string         COMMENT '贷款合同编号',
  `contract_amount`                    decimal(32,2)  COMMENT '贷款合同金额',
  `interest_rate_type`                 string         COMMENT '利率类型',
  `contract_interest_rate`             decimal(20,4)  COMMENT '合同利率',
  `base_interest_rate`                 decimal(20,4)  COMMENT '基准利率',
  `fixed_interest_rate`                decimal(20,4)  COMMENT '固定利率',
  `fixed_interest_diff`                string         COMMENT '固定利差',
  `interest_rate_ajustment`            string         COMMENT '利率调整方式',
  `loan_issue_date`                    string         COMMENT '贷款发放日',
  `loan_expiry_date`                   string         COMMENT '贷款到期日',
  `frist_repayment_date`               string         COMMENT '首次还款日',
  `repayment_type`                     string         COMMENT '还款方式',
  `repayment_frequency`                string         COMMENT '还款频率',
  `loan_repay_date`                    int            COMMENT '贷款还款日',
  `tail_amount`                        decimal(32,2)  COMMENT '尾款金额',
  `tail_amount_rate`                   decimal(30,10) COMMENT '尾款金额占比',
  `consume_use`                        string         COMMENT '消费用途',
  `guarantee_type`                     string         COMMENT '担保方式',
  `extract_date`                       string         COMMENT '提取日期',
  `extract_date_principal_amount`      decimal(32,2)  COMMENT '提取日本金金额',
  `loan_cur_interest_rate`             decimal(30,10) COMMENT '贷款现行利率',
  `borrower_type`                      string         COMMENT '借款人类型',
  `borrower_name`                      string         COMMENT '借款人姓名',
  `document_type`                      string         COMMENT '证件类型',
  `document_num`                       string         COMMENT '证件号码',
  `borrower_rating`                    string         COMMENT '借款人评级',
  `borrower_industry`                  string         COMMENT '借款人行业',
  `phone_num`                          string         COMMENT '手机号码',
  `sex`                                string         COMMENT '性别',
  `birthday`                           string         COMMENT '出生日期',
  `age`                                int            COMMENT '年龄',
  `province`                           string         COMMENT '所在省份',
  `city`                               string         COMMENT '所在城市',
  `marital_status`                     string         COMMENT '婚姻状况',
  `country`                            string         COMMENT '国籍',
  `annual_income`                      decimal(32,2)  COMMENT '年收入',
  `income_debt_rate`                   decimal(32,4)  COMMENT '收入债务比',
  `education_level`                    string         COMMENT '教育程度',
  `period_exp`                         int            COMMENT '提取日剩余还款期数',
  `account_age`                        decimal(11,2)  COMMENT '帐龄',
  `residual_maturity_con`              decimal(11,2)  COMMENT '合同期限',
  `residual_maturity_ext`              decimal(11,2)  COMMENT '提取日剩余期限',
  `package_principal_balance`          decimal(32,2)  COMMENT '封包本金余额',
  `statistics_date`                    string         COMMENT '统计日期',
  `extract_interest_date`              string         COMMENT '提取日计息日',
  `curr_over_days`                     int            COMMENT '当前逾期天数',
  `remain_counts`                      int            COMMENT '当前剩余期数',
  `remain_amounts`                     decimal(20,2)  COMMENT '当前剩余本金（元）',
  `remain_interest`                    decimal(20,2)  COMMENT '当前剩余利息（元）',
  `remain_other_amounts`               decimal(20,2)  COMMENT '当前剩余费用（元）',
  `periods`                            int            COMMENT '总期数',
  `period_amounts`                     decimal(20,2)  COMMENT '每期固定费用',
  `bus_product_id`                     string         COMMENT '业务产品编号',
  `bus_product_name`                   string         COMMENT '业务产品名称',
  `shoufu_amount`                      decimal(32,2)  COMMENT '首付款金额',
  `selling_price`                      decimal(32,2)  COMMENT '销售价格',
  `contract_daily_interest_rate`       decimal(30,10) COMMENT '合同日利率',
  `repay_plan_cal_rule`                string         COMMENT '还款计划计算规则',
  `contract_daily_interest_rate_count` int            COMMENT '日利率计算基础',
  `total_investment_amount`            decimal(32,2)  COMMENT '投资总额（元）',
  `contract_month_interest_rate`       decimal(20,4)  COMMENT '合同月利率',
  `status_change_log`                  string         COMMENT '资产状态变更日志',
  `package_filter_id`                  string         COMMENT '虚拟过滤包id',
  `virtual_asset_bag_id`               string         COMMENT '虚拟资产包id',
  `package_remain_principal`           decimal(20,2)  COMMENT '封包时当前剩余本金（元）',
  `package_remain_periods`             int            COMMENT '封包时当前剩余期数',
  `status`                             int            COMMENT '当前资产状态（-1：征信异常，0：准入，1：在池，2：封包，3：发行，4：历史）',
  `wind_control_status`                string         COMMENT '当前风控状态（Yes：风控通过，No：风控未通过，NA：风控未查得，Without：未跑风控）',
  `wind_control_status_pool`           string         COMMENT '入池时风控状态（Yes：风控通过，No：风控未通过，NA：风控未查得，Without：未跑风控）',
  `cheat_level`                        string         COMMENT '反欺诈等级（1~5，-1：null，其它：异常值）',
  `score_range`                        string         COMMENT '评分等级（1~20，-1：null，其它：异常值）',
  `score_level`                        string         COMMENT '评分区间（1~5，-1：null，其它：异常值）',
  `create_time`                        string         COMMENT '创建记录时间',
  `update_time`                        string         COMMENT '修改时间',
  `data_source`                        int            COMMENT '数据来源（1：startLink，2：excelImport）',
  `address`                            string         COMMENT '居住地址',
  `mortgage_rates`                     decimal(20,4)  COMMENT '抵押率（%）'
) COMMENT '基础资产表'
STORED as PARQUET;





-- 基础资产风控评分历史表
-- DROP TABLE IF EXISTS `stage.abs_t_asset_wind_control_history`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.abs_t_asset_wind_control_history`(
  `id`                                                int           COMMENT '主键id',
  `project_id`                                        string        COMMENT '项目编号',
  `asset_bag_id`                                      string        COMMENT '资产包编号',
  `asset_type`                                        string        COMMENT '资产类型',
  `serial_number`                                     string        COMMENT '借据号',
  `contract_code`                                     string        COMMENT '贷款合同编号',
  `statistics_date`                                   string        COMMENT '统计日期',
  `status`                                            int           COMMENT '资产状态(-1征信异常 0准入 1在池 2封包 3发行 4历史',
  `wind_control_status`                               string        COMMENT '当前风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
  `wind_control_status_pool`                          string        COMMENT '入池时风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
  `cheat_level`                                       string        COMMENT '反欺诈等级:1~5,-1:null,其它:异常值',
  `score_range`                                       string        COMMENT '评分等级:1~20,-1:null,其它:异常值',
  `score_level`                                       string        COMMENT '评分区间:1~5,-1:null,其它:异常值',
  `data_source`                                       int           COMMENT '数据来源 1:startLink,2:excelImport',
  `create_time`                                       string        COMMENT '创建记录时间',
  `update_time`                                       string        COMMENT '修改时间'
) COMMENT '基础资产风控评分历史表'
STORED as PARQUET;


-- 风控结果日志
-- DROP TABLE IF EXISTS `stage.abs_t_wind_control_resp_log`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.abs_t_wind_control_resp_log`(
  `id`                                                int           COMMENT '主键id',
  `req_id`                                            int           COMMENT '请求Id',
  `project_id`                                        string        COMMENT '项目编号',
  `apply_no`                                          string        COMMENT '申请编号(借据号)',
  `creid_no`                                          string        COMMENT '身份证号',
  `name`                                              string        COMMENT '姓名',
  `mobile_phone`                                      string        COMMENT '手机号',
  `credit_line`                                       int           COMMENT '申请额度  以分为单位，取值范围（0,1000000000）',
  `ret_code`                                          string        COMMENT '应答码',
  `ret_msg`                                           string        COMMENT '应答信息',
  `status`                                            string        COMMENT '查询结果状态 0:未查得,1:查得',
  `rating_result`                                     string        COMMENT '风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Error:风控异常,Fail:调用风控失败)',
  `cheat_level`                                       string        COMMENT '反欺诈等级:1~5,-1:null,其它:异常值',
  `score_range`                                       string        COMMENT '评分等级:1~20,-1:null,其它:异常值',
  `score_level`                                       string        COMMENT '评分区间:1~5,-1:null,其它:异常值',
  `ret_content`                                       string        COMMENT '风控结果报文',
  `wind_moment`                                       int           COMMENT '风控时刻（0：入池时，1：资产跟踪)',
  `wind_interface_type`                               int           COMMENT '查询的风控接口类型(1:风控审核4号接口,2:风控结果查询新分享本地接口,3:风控结果查询5号接口,4:风控查询6号接口)',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '修改时间'
) COMMENT '风控结果日志'
STORED as PARQUET;





-- 项目表
-- DROP TABLE IF EXISTS `stage.abs_t_project`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.abs_t_project`(
  `id`                                                int           COMMENT '主键id',
  `project_id`                                        string        COMMENT '项目编号',
  `project_stage`                                     int           COMMENT '项目阶段 1:项目立项,2:资产管理,3:产品设计,4:存续期,5:已结束',
  `project_progress`                                  int           COMMENT '项目进展情况',
  `client_name`                                       string        COMMENT '客户名称',
  `project_name`                                      string        COMMENT '项目名称',
  `project_full_name`                                 string        COMMENT '项目名全称',
  `asset_type`                                        int           COMMENT '资产类别 1:汽车贷,2:房贷,3:消费贷',
  `project_type`                                      int           COMMENT '业务模式 1:存量,2:增量',
  `channel`                                           int           COMMENT '渠道',
  `mode`                                              int           COMMENT '模型归属：1-新分享，2-联合',
  `fee_type`                                          int           COMMENT '费率类型',
  `team`                                              int           COMMENT '团队',
  `trading_structure`                                 string        COMMENT '交易结构',
  `exp_coop_scale`                                    string        COMMENT '预计合作规模',
  `origination`                                       int           COMMENT 'whos origination (WeShare or Tencent)',
  `exp_borrow_time`                                   bigint        COMMENT '预计出借时间',
  `guarantee`                                         string        COMMENT '担保',
  `asset_yield_rate`                                  decimal(10,5) COMMENT '资产端收益率',
  `current_stage`                                     string        COMMENT '目前进展',
  `next_stage`                                        string        COMMENT '下一步工作安排',
  `set_up_time`                                       bigint        COMMENT '目标产品成立时间',
  `collaboration`                                     string        COMMENT '外部协作机构',
  `project_time`                                      string        COMMENT '立项时间',
  `project_begin_date`                                string        COMMENT '项目开始时间',
  `project_end_date`                                  string        COMMENT '项目结束时间',
  `image_show_type`                                   int           COMMENT '影像入口类型',
  `asset_pool_type`                                   int           COMMENT '资产池类型 1:静态池,2:动态池',
  `data_source`                                       int           COMMENT '数据来源 1:接口导入,2:excel导入',
  `public_offer`                                      string        COMMENT '公募名称',
  `create_user`                                       string        COMMENT '创建人',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '修改时间'
) COMMENT '项目表'
STORED as PARQUET;


-- 项目表关联表
-- DROP TABLE IF EXISTS `stage.abs_t_related_assets`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.abs_t_related_assets`(
  `id`                                                int           COMMENT '主键id',
  `import_id`                                         int           COMMENT '导入id',
  `serial_number`                                     string        COMMENT '借据号',
  `project_id`                                        string        COMMENT '项目编号',
  `related_serial_number`                             string        COMMENT '关联借据号',
  `related_project_id`                                string        COMMENT '关联项目编号',
  `related_status`                                    int           COMMENT '关联状态（1：正常，2：解除关联）',
  `create_time`                                       string        COMMENT '创建时间',
  `update_time`                                       string        COMMENT '更新时间'
) COMMENT '项目表关联表'
STORED as PARQUET;


-- 资产包信息表
-- DROP TABLE IF EXISTS `stage.abs_t_related_assets`;
CREATE EXTERNAL TABLE IF NOT EXISTS `stage.abs_t_asset_bag`(
  `id`                        int           COMMENT '主键id',
  `project_id`                string        COMMENT '项目编号',
  `asset_bag_id`              string        COMMENT '资产包编号',
  `asset_bag_name`            string        COMMENT '资产包名称',
  `asset_count`               int           COMMENT '资产包资产数量',
  `package_principal_balance` decimal(20,2) COMMENT '封包总本金余额',
  `bag_date`                  string        COMMENT '封包日期',
  `bus_product_id`            string        COMMENT '业务产品编号',
  `bag_conditions`            string        COMMENT '封包条件',
  `weight_avg_remain_term`    decimal(10,2) COMMENT '加权平均剩余期限（月）',
  `bag_rate`                  decimal(20,2) COMMENT '封包平均利率',
  `bag_weight_rate`           decimal(20,2) COMMENT '加权平均利率',
  `asset_bag_calc_rule`       int           COMMENT '封包规模计算规则（1：按实际剩余本金，2：按还款计划）',
  `asset_bag_type`            int           COMMENT '资产包类型（1：初始包，2：循环包）',
  `status`                    int           COMMENT '包状态（1：未封包，2：已封包，3：已解包，4：封包中，5：封包失败）',
  `package_filter_id`         string        COMMENT '虚拟过滤包id',
  `create_time`               string        COMMENT '创建时间',
  `update_time`               string        COMMENT '更新时间'
) COMMENT '资产包信息表'
STORED as PARQUET;


CREATE external TABLE stage.duration_result(
  `id` int  COMMENT '主键',
  `request_id` int  COMMENT '存续期数据跑批申请表主键',
  `swift_no` string COMMENT '流水号',
  `apply_no` string  COMMENT '借据号',
  `project_name` string COMMENT '项目编号',
  `name` string COMMENT '姓名',
  `card_no` string COMMENT '身份证号码',
  `mobile` string COMMENT '手机号',
  `is_settle` string COMMENT '是否已结清',
  `execute_month` string COMMENT '执行日期(YYYY-MM)',
  `score_range_t1` string COMMENT 'T-1 月资产等级',
  `score_range_t2` string COMMENT 'T-2月资产等级',
  `score_range` string COMMENT '资产等级',
  `inner_black` string COMMENT '内部黑名单 1：命中 2：未命中',
  `focus` string COMMENT '关注名单 1-关注 0-非关注',
  `state` string COMMENT '数据状态 0-无效 1-处理中 2-处理成功 3-处理失败',
  `error_msg` string COMMENT '失败原因',
  `create_time` bigint  COMMENT '创建时间',
  `update_time` bigint  COMMENT '修改时间'
) COMMENT '存续期数据跑批结果表'
partitioned by (d_date string comment '批量日期',project_id string comment '项目id')
stored as parquet;




CREATE TABLE `stage.duration_request` (
  `id` int COMMENT '主键',
  `swift_no` string COMMENT '流水号',
  `apply_no` string COMMENT '唯一请求号',
  `req_data` string COMMENT '请求信息',
  `file_path` string COMMENT '文件路径',
  `file_name` string COMMENT '文件名',
  `state` string COMMENT '数据状态 0-无效 1-处理中 2-处理成功 3-处理失败',
  `error_msg` string COMMENT '失败原因',
  `create_time` bigint   COMMENT '创建时间',
  `update_time` bigint   COMMENT '修改时间'
) COMMENT '存续期数据跑批申请表'
partitioned by(d_date string comment '批量日期')
stored as parquet;
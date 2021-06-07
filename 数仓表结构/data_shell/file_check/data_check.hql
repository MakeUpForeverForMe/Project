CREATE DATABASE IF NOT EXISTS `data_check` COMMENT '老集群数据库' location 'cosn://bigdata-center-prod-1253824322/user/hadoop/warehouse/data_check.db';

CREATE TABLE IF NOT EXISTS `data_check.loan_apply`(
  `cust_id`                            string        COMMENT '客户编号',
  `user_hash_no`                       string        COMMENT '用户编号',
  `birthday`                           string        COMMENT '出生日期',
  `age`                                decimal(3,0)  COMMENT '年龄',
  `pre_apply_no`                       string        COMMENT '预审申请编号',
  `apply_id`                           string        COMMENT '申请id',
  `due_bill_no`                        string        COMMENT '借据编号',
  `loan_apply_time`                    timestamp     COMMENT '用信申请时间',
  `loan_amount_apply`                  decimal(15,4) COMMENT '用信申请金额',
  `loan_terms`                         decimal(3,0)  COMMENT '贷款期数（3、6、9等）',
  `loan_usage`                         string        COMMENT '贷款用途（英文原值）（1：日常消费，2：汽车加油，3：修车保养，4：购买汽车，5：医疗支出，6：教育深造，7：房屋装修，8：旅游出行，9：其他消费）',
  `loan_usage_cn`                      string        COMMENT '贷款用途（汉语解释）',
  `repay_type`                         string        COMMENT '还款方式（英文原值）（1：等额本金，2：等额本息、等额等息等）',
  `repay_type_cn`                      string        COMMENT '还款方式（汉语解释）',
  `interest_rate`                      decimal(15,8) COMMENT '利息利率（8d/%）',
  `credit_coef`                        decimal(15,8) COMMENT '综合融资成本（8d/%）',
  `penalty_rate`                       decimal(15,8) COMMENT '罚息利率（8d/%）',
  `apply_status`                       decimal(2,0)  COMMENT '申请状态（1: 放款成功，2: 放款失败，3: 处理中，4：用信成功，5：用信失败）',
  `apply_resut_msg`                    string        COMMENT '申请结果信息',
  `issue_time`                         timestamp     COMMENT '放款时间，放款成功后必填',
  `loan_amount_approval`               decimal(15,4) COMMENT '用信通过金额',
  `loan_amount`                        decimal(15,4) COMMENT '放款金额',
  `risk_level`                         string        COMMENT '风控等级',
  `risk_score`                         string        COMMENT '风控评分',
  `ori_request`                        string        COMMENT '原始请求',
  `ori_response`                       string        COMMENT '原始应答',
  `create_time`                        string        COMMENT '创建时间',
  `update_time`                        string        COMMENT '更新时间'
) COMMENT '用信申请表'
PARTITIONED BY (`biz_date` string COMMENT '用信申请日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;




CREATE TABLE IF NOT EXISTS `data_check.loan_lending${tb_suffix}`(
  `apply_no`                           string        COMMENT '进件编号',
  `contract_no`                        string        COMMENT '合同编号',
  `due_bill_no`                        string        COMMENT '借据编号',
  `loan_usage`                         string        COMMENT '贷款用途',
  `loan_active_date`                   string        COMMENT '放款日期',
  `cycle_day`                          decimal(2,0)  COMMENT '账单日',
  `loan_expire_date`                   string        COMMENT '贷款到期日期',
  `loan_type`                          string        COMMENT '分期类型（英文原值）（MCEP：等额本金，MCEI：等额本息，R：消费转分期，C：现金分期，B：账单分期，P：POS分期，M：大额分期（专项分期），MCAT：随借随还，STAIR：阶梯还款）',
  `loan_type_cn`                       string        COMMENT '分期类型（汉语解释）',
  `loan_init_term`                     decimal(3,0)  COMMENT '贷款期数（3、6、9等）',
  `loan_init_principal`                decimal(15,4) COMMENT '贷款本金',
  `loan_init_interest_rate`            decimal(15,8) COMMENT '利息利率',
  `loan_init_term_fee_rate`            decimal(15,8) COMMENT '手续费费率',
  `loan_init_svc_fee_rate`             decimal(15,8) COMMENT '服务费费率',
  `loan_init_penalty_rate`             decimal(15,8) COMMENT '罚息利率'
) COMMENT '放款表'
PARTITIONED BY (`biz_date` string COMMENT '放款日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS `data_check.loan_info${tb_suffix}`(
  `due_bill_no`                        string        COMMENT '借据编号',
  `apply_no`                           string        COMMENT '进件编号',
  `loan_active_date`                   string        COMMENT '放款日期',
  `loan_init_principal`                decimal(15,4) COMMENT '贷款本金',
  `loan_init_term`                     decimal(3,0)  COMMENT '贷款期数（3、6、9等）',
  `loan_term`                          decimal(3,0)  COMMENT '当前期数（按应还日算）',
  `should_repay_date`                  string        COMMENT '应还日期',
  `loan_term_repaid`                   decimal(3,0)  COMMENT '已还期数',
  `loan_term_remain`                   decimal(3,0)  COMMENT '剩余期数',
  `loan_init_interest`                 decimal(15,4) COMMENT '贷款利息',
  `loan_init_term_fee`                 decimal(15,4) COMMENT '贷款手续费',
  `loan_init_svc_fee`                  decimal(15,4) COMMENT '贷款服务费',
  `loan_status`                        string        COMMENT '借据状态（英文原值）（N：正常，O：逾期，F：已还清）',
  `loan_status_cn`                     string        COMMENT '借据状态（汉语解释）',
  `loan_out_reason`                    string        COMMENT '借据终止原因（P：提前还款，M：银行业务人员手工终止（manual），D：逾期自动终止（delinquency），R：锁定码终止（Refund），V：持卡人手动终止，C：理赔终止，T：退货终止，U：重组结清终止，F：强制结清终止，B：免息转分期）',
  `paid_out_type`                      string        COMMENT '结清类型（英文原值）（BANK_REF：退票结清，BUY_BACK：资产回购，CAPITAL_VERI：资产核销，DISPOSAL：处置结束，NORMAL_SETTLE：正常结清，OVER_COMP：逾期代偿，OVERDUE_SETTLE：逾期结清，PRE_SETTLE：提前结清，REDEMPTION：赎回，REFUND：退车，REFUND_SETTLEMENT：退票结清）',
  `paid_out_type_cn`                   string        COMMENT '结清类型（汉语解释）',
  `paid_out_date`                      string        COMMENT '还款日期',
  `terminal_date`                      string        COMMENT '提前终止日期',
  `paid_amount`                        decimal(15,4) COMMENT '已还金额',
  `paid_principal`                     decimal(15,4) COMMENT '已还本金',
  `paid_interest`                      decimal(15,4) COMMENT '已还利息',
  `paid_penalty`                       decimal(15,4) COMMENT '已还罚息',
  `paid_svc_fee`                       decimal(15,4) COMMENT '已还服务费',
  `paid_term_fee`                      decimal(15,4) COMMENT '已还手续费',
  `paid_mult`                          decimal(15,4) COMMENT '已还滞纳金',
  `remain_amount`                      decimal(15,4) COMMENT '剩余金额：本息费',
  `remain_principal`                   decimal(15,4) COMMENT '剩余本金',
  `remain_interest`                    decimal(15,4) COMMENT '剩余利息',
  `remain_svc_fee`                     decimal(15,4) COMMENT '剩余服务费',
  `remain_term_fee`                    decimal(15,4) COMMENT '剩余手续费',
  `overdue_principal`                  decimal(15,4) COMMENT '逾期本金',
  `overdue_interest`                   decimal(15,4) COMMENT '逾期利息',
  `overdue_svc_fee`                    decimal(15,4) COMMENT '逾期服务费',
  `overdue_term_fee`                   decimal(15,4) COMMENT '逾期手续费',
  `overdue_penalty`                    decimal(15,4) COMMENT '逾期罚息',
  `overdue_mult_amt`                   decimal(15,4) COMMENT '逾期滞纳金',
  `overdue_date_first`                 string        COMMENT '首次逾期日期',
  `overdue_date_start`                 string        COMMENT '逾期起始日期',
  `overdue_days`                       decimal(5,0)  COMMENT '逾期天数',
  `overdue_date`                       string        COMMENT '逾期日期',
  `dpd_begin_date`                     string        COMMENT 'DPD起始日期',
  `dpd_days`                           decimal(4,0)  COMMENT 'DPD天数',
  `dpd_days_count`                     decimal(4,0)  COMMENT '累计DPD天数',
  `dpd_days_max`                       decimal(4,0)  COMMENT '历史最大DPD天数',
  `collect_out_date`                   string        COMMENT '出催日期',
  `overdue_term`                       decimal(3,0)  COMMENT '当前逾期期数',
  `overdue_terms_count`                decimal(3,0)  COMMENT '累计逾期期数',
  `overdue_terms_max`                  decimal(3,0)  COMMENT '历史单次最长连续逾期期数',
  `overdue_principal_accumulate`       decimal(15,4) COMMENT '累计逾期本金',
  `overdue_principal_max`              decimal(15,4) COMMENT '历史最大逾期本金',
  `s_d_date`                           string        COMMENT 'ods层起始日期',
  `e_d_date`                           string        COMMENT 'ods层结束日期',
  `effective_time`                     timestamp     COMMENT '生效日期',
  `expire_time`                        timestamp     COMMENT '失效日期'
) COMMENT '借据信息表'
PARTITIONED BY (`is_settled` string COMMENT '是否已结清',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;





CREATE TABLE IF NOT EXISTS `data_check.repay_schedule${tb_suffix}`(
  `schedule_id`                        string        COMMENT '还款计划编号',
  `due_bill_no`                        string        COMMENT '借据编号',
  `loan_active_date`                   string        COMMENT '放款日期',
  `loan_init_principal`                decimal(15,4) COMMENT '贷款本金',
  `loan_init_term`                     decimal(3,0)  COMMENT '贷款期数（3、6、9等）',
  `loan_term`                          decimal(3,0)  COMMENT '当前期数',
  `start_interest_date`                string        COMMENT '起息日期',
  `curr_bal`                           decimal(15,4) COMMENT '当前余额（当前欠款）',
  `should_repay_date`                  string        COMMENT '应还日期',         -- 对应 pmt_due_date 字段
  `should_repay_date_history`          string        COMMENT '修改前的应还日期', -- 对应 pmt_due_date 的上一次日期 字段
  `grace_date`                         string        COMMENT '宽限日期',
  `should_repay_amount`                decimal(15,4) COMMENT '应还金额',
  `should_repay_principal`             decimal(15,4) COMMENT '应还本金',
  `should_repay_interest`              decimal(15,4) COMMENT '应还利息',
  `should_repay_term_fee`              decimal(15,4) COMMENT '应还手续费',
  `should_repay_svc_fee`               decimal(15,4) COMMENT '应还服务费',
  `should_repay_penalty`               decimal(15,4) COMMENT '应还罚息',
  `should_repay_mult_amt`              decimal(15,4) COMMENT '应还滞纳金',
  `should_repay_penalty_acru`          decimal(15,4) COMMENT '应还累计罚息金额',
  `schedule_status`                    string        COMMENT '还款计划状态（英文原值）（N：正常，O：逾期，F：已还清）',
  `schedule_status_cn`                 string        COMMENT '还款计划状态（汉语解释）',
  `paid_out_date`                      string        COMMENT '还清日期',
  `paid_out_type`                      string        COMMENT '结清类型（英文原值）（BANK_REF：退票结清，BUY_BACK：资产回购，CAPITAL_VERI：资产核销，DISPOSAL：处置结束，NORMAL_SETTLE：正常结清，OVER_COMP：逾期代偿，OVERDUE_SETTLE：逾期结清，PRE_SETTLE：提前结清，REDEMPTION：赎回，REFUND：退车，REFUND_SETTLEMENT：退票结清）',
  `paid_out_type_cn`                   string        COMMENT '结清类型（汉语解释）',
  `paid_amount`                        decimal(15,4) COMMENT '已还金额',
  `paid_principal`                     decimal(15,4) COMMENT '已还本金',
  `paid_interest`                      decimal(15,4) COMMENT '已还利息',
  `paid_term_fee`                      decimal(15,4) COMMENT '已还手续费',
  `paid_svc_fee`                       decimal(15,4) COMMENT '已还服务费',
  `paid_penalty`                       decimal(15,4) COMMENT '已还罚息',
  `paid_mult`                          decimal(15,4) COMMENT '已还滞纳金',
  `reduce_amount`                      decimal(15,4) COMMENT '减免金额',
  `reduce_principal`                   decimal(15,4) COMMENT '减免本金',
  `reduce_interest`                    decimal(15,4) COMMENT '减免利息',
  `reduce_term_fee`                    decimal(15,4) COMMENT '减免手续费',
  `reduce_svc_fee`                     decimal(15,4) COMMENT '减免服务费',
  `reduce_penalty`                     decimal(15,4) COMMENT '减免罚息',
  `reduce_mult_amt`                    decimal(15,4) COMMENT '减免滞纳金',
  `s_d_date`                           string        COMMENT 'ods层起始日期',
  `e_d_date`                           string        COMMENT 'ods层结束日期',
  `effective_time`                     timestamp     COMMENT '生效时间',
  `expire_time`                        timestamp     COMMENT '失效时间'
) COMMENT '还款计划表'
PARTITIONED BY (`is_settled` string COMMENT '是否已结清',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;



CREATE TABLE IF NOT EXISTS `data_check.repay_detail${tb_suffix}`(
  `due_bill_no`                        string        COMMENT '借据号',
  `loan_active_date`                   string        COMMENT '放款日期',
  `loan_init_term`                     decimal(3,0)  COMMENT '贷款期数（3、6、9等）',
  `repay_term`                         decimal(3,0)  COMMENT '实还期数',
  `order_id`                           string        COMMENT '订单号',
  `loan_status`                        string        COMMENT '借据状态（英文原值）（N：正常，O：逾期，F：已还清，NORMAL_SETTLE：正常结清，OVERDUE_SETTLE：逾期结清，PRE_SETTLE：提前结清）',
  `loan_status_cn`                     string        COMMENT '借据状态（汉语解释）',
  `overdue_days`                       decimal(4,0)  COMMENT '逾期天数',
  `payment_id`                         string        COMMENT '实还流水号',
  `txn_time`                           timestamp     COMMENT '交易时间',
  `post_time`                          timestamp     COMMENT '入账时间',
  `bnp_type`                           string        COMMENT '还款成分（英文原值）（Pricinpal：本金，Interest：利息，Penalty：罚息，Mulct：罚金，Compound：复利，CardFee：年费，OverLimitFee：超限费，LatePaymentCharge：滞纳金，NSFCharge：资金不足罚金，TXNFee：交易费，TERMFee：手续费，SVCFee：服务费，LifeInsuFee：寿险计划包费）',
  `bnp_type_cn`                        string        COMMENT '还款成分（汉语解释）',
  `repay_amount`                       decimal(15,4) COMMENT '还款金额',
  `batch_date`                         string        COMMENT '批量日期',
  `create_time`                        timestamp     COMMENT '创建时间',
  `update_time`                        timestamp     COMMENT '更新时间'
) COMMENT '实还明细表'
PARTITIONED BY (`biz_date` string COMMENT '实还日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 订单流水表
-- 数据库主键 order_id
-- 按照 biz_date 分区
-- DROP TABLE IF EXISTS `ods_new_s${db_suffix}.order_info`;
CREATE TABLE IF NOT EXISTS `data_check.order_info${tb_suffix}`(
  `order_id`                           string        COMMENT '订单编号',
  `ori_order_id`                       string        COMMENT '原订单编号',
  `apply_no`                           string        COMMENT '申请件编号',
  `due_bill_no`                        string        COMMENT '借据号',
  `term`                               decimal(3,0)  COMMENT '处理期数',
  `channel_id`                         string        COMMENT '服务渠道编号（VISA：VISA，MC：MC，JCB：JCB，CUP：CUP，AMEX：AMEX，BANK：本行，ICL：ic系统，THIR：第三方，SUNS：阳光，AG：客服）',
  `pay_channel`                        string        COMMENT '支付渠道',
  `command_type`                       string        COMMENT '支付指令类型（SPA：单笔代付，SDB：单笔代扣，QSP：单笔代付查询，QSD：单笔代扣查询，BDB：批量代扣，BDA：批量代付）',
  `order_status`                       string        COMMENT '订单状态（C：已提交，P：待提交，Q：待审批，W：处理中，S：已完成，V：已失效，E：失败，T：超时，R：已重提，G：拆分处理中，D：拆分已完成，B：撤销，X：已受理待入账）',
  `order_time`                         timestamp     COMMENT '订单时间',
  `repay_serial_no`                    string        COMMENT '还款流水号',
  `service_id`                         string        COMMENT '交易服务码',
  `assign_repay_ind`                   string        COMMENT '指定余额成分还款标志（Y：是，N：否）',
  `repay_way`                          string        COMMENT '还款方式（ONLINE：线上，OFFLINE：线下）',
  `txn_type`                           string        COMMENT '交易类型（Inq：查询，Cash：取现，AgentDebit：付款，Loan：分期，Auth：消费，PreAuth：预授权，PAComp：预授权完成，Load：圈存，Credit：存款，AgentCredit：收款，TransferCredit：转入，TransferDeditDepos：转出，AdviceSettle：结算通知，BigAmountLoan：大）',
  `txn_amt`                            decimal(15,4) COMMENT '交易金额',
  `original_txn_amt`                   decimal(15,4) COMMENT '原始交易金额',
  `success_amt`                        decimal(15,4) COMMENT '成功金额',
  `currency`                           string        COMMENT '币种',
  `code`                               string        COMMENT '状态码',
  `message`                            string        COMMENT '描述',
  `response_code`                      string        COMMENT '对外返回码',
  `response_message`                   string        COMMENT '对外返回描述',
  `business_date`                      string        COMMENT '业务日期',
  `send_time`                          string        COMMENT '发送时间',
  `opt_datetime`                       timestamp     COMMENT '更新时间',
  `setup_date`                         string        COMMENT '创建日期',
  `loan_usage`                         string        COMMENT '贷款用途（B：回购，C：差额补足，D：代偿，F：追偿代扣，H：处置回收，I：强制结清扣款，L：放款申请，M：预约提前结清扣款，N：提前还当期，O：逾期扣款，P：打款通知，R：退货，T：退票，W：赎回结清，X：账务调整，Z：委托转付）',
  `purpose`                            string        COMMENT '支付用途',
  `online_flag`                        string        COMMENT '联机标识（Y：是，N：否）',
  `online_allow`                       string        COMMENT '允许联机标识（Y：是，N：否）',
  `order_pay_no`                       string        COMMENT '支付流水号',
  `bank_trade_no`                      string        COMMENT '银行交易流水号',
  `bank_trade_time`                    string        COMMENT '线下银行订单交易时间',
  `bank_trade_act_no`                  string        COMMENT '银行付款账号',
  `bank_trade_act_name`                string        COMMENT '银行付款账户名称',
  `bank_trade_act_phone`               string        COMMENT '银行预留手机号',
  `service_sn`                         string        COMMENT '流水号',
  `outer_no`                           string        COMMENT '外部凭证号',
  `confirm_flag`                       string        COMMENT '确认标志',
  `txn_time`                           timestamp     COMMENT '交易时间',
  `txn_date`                           string        COMMENT '交易日期',
  `capital_plan_no`                    string        COMMENT '资金计划编号',
  `memo`                               string        COMMENT '备注',
  `create_time`                        timestamp     COMMENT '创建时间',
  `update_time`                        timestamp     COMMENT '更新时间'
) COMMENT '订单流水表'
PARTITIONED BY (`biz_date` string COMMENT '交易日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;

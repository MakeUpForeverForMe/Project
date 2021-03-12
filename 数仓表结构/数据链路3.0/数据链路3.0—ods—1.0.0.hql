----------------ods.ecas_loan-----------------------------
create table if not exists ods.ecas_loan(ORG string comment '' ,
LOAN_ID string comment '///@UuidSeq' ,
ACCT_NBR string comment '' ,
ACCT_TYPE string comment '///@EnumRef:cn.sunline.ppy.dictionary.enums.AccountType

A|人民币独立基本信用账户
B|美元独立基本信用账户
C|人民币共享基本信用账户
D|美元共享基本信用账户
E|人民币独立小额贷款账户
F|人民币活期借记账户
G|人民币共享小额贷款账户
L|人民币存款账户' ,
CUST_ID string comment '' ,
DUE_BILL_NO string comment '' ,
APPLY_NO string comment '' ,
REGISTER_DATE string comment '' ,
REQUEST_TIME bigint comment '请求日期时间' ,
LOAN_TYPE string comment '///@EnumRef:cn.sunline.ppy.dictionary.enums.LoanType
R|消费转分期
C|现金分期
B|账单分期
P|POS分期
M|大额分期（专项分期）
MCAT|随借随还
MCEP|等额本金
MCEI|等额本息' ,
LOAN_STATUS string comment '///@EnumRef:cn.sunline.ecas.definition.enums.LoanStatus
N|正常
O|逾期
F|已还清' ,
LOAN_INIT_TERM int comment '' ,
CURR_TERM int comment '' ,
REMAIN_TERM int comment '' ,
LOAN_INIT_PRIN decimal(15,2) comment '' ,
TOTLE_INT decimal(15,2) comment '' ,
TOTLE_TERM_FEE decimal(15,2) comment '' ,
TOTLE_SVC_FEE decimal(15,2) comment '' ,
UNSTMT_PRIN decimal(15,2) comment '' ,
PAID_PRINCIPAL decimal(15,2) comment '' ,
PAID_INTEREST decimal(15,2) comment '' ,
PAID_SVC_FEE decimal(15,2) comment '' ,
PAID_TERM_FEE decimal(15,2) comment '' ,
PAID_PENALTY decimal(15,2) comment '' ,
PAID_MULT decimal(15,2) comment '' ,
ACTIVE_DATE string comment '' ,
PAID_OUT_DATE string comment '' ,
TERMINAL_DATE string comment '' ,
TERMINAL_REASON_CD string comment '///@EnumRef:cn.sunline.ecas.definition.enums.LoanTerminateReason
P|提前还款
M|银行业务人员手工终止（manual）
D|逾期自动终止（delinquency）
R|锁定码终止(Refund)
V|持卡人手动终止
C|理赔终止
T|退货终止
U|重组结清终止
F|强制结清终止
B|免息转分期' ,
LOAN_CODE string comment '' ,
REGISTER_ID string comment '' ,
INTEREST_RATE decimal(12,8) comment '' ,
PENALTY_RATE decimal(12,8) comment '' ,
LOAN_EXPIRE_DATE string comment '' ,
LOAN_AGE_CODE string comment '' ,
PAST_EXTEND_CNT int comment '' ,
PAST_SHORTEN_CNT int comment '' ,
CONTRACT_NO string comment '' ,
OVERDUE_DATE string comment '' ,
MAX_CPD int comment '' ,
MAX_CPD_DATE string comment '' ,
MAX_DPD int comment '' ,
MAX_DPD_DATE string comment '' ,
CPD_BEGIN_DATE string comment '' ,
LOAN_FEE_DEF_ID string comment '' ,
PURPOSE string comment '' ,
PRODUCT_CODE string comment '产品代码' ,
PRE_AGE_CD_GL string comment '' ,
AGE_CODE_GL string comment '' ,
NORMAL_INT_ACRU decimal(15,2) comment '' ,
TOTLE_MULT_FEE decimal(15,2) comment '' ,
TOTLE_PENALTY decimal(15,2) comment '' ,
IS_INT_ACCURAL_IND string comment '///@EnumRef:cn.sunline.common.enums.Indicator
Y|是
N|否' ,
COLLECT_OUT_DATE string comment '出催收队列时间' ,
CREATE_TIME bigint comment '' ,
CREATE_USER string comment '' ,
LOAN_SETTLE_REASON string comment '///@EnumRef:cn.sunline.ecas.definition.enums.LoanSettleReason
NORMAL_SETTLE|正常结清
OVERDUE_SETTLE|逾期结清
PRE_SETTLE|提前结清
REFUND|退车
REDEMPTION|赎回' ,
REPAY_TERM int comment '' ,
OVERDUE_TERM int comment '' ,
COUNT_OVERDUE_TERM int comment '' ,
MAX_OVERDUE_TERM int comment '' ,
MAX_OVERDUE_PRIN decimal(15,2) comment '' ,
OVERDUE_DAYS int comment '' ,
COUNT_OVERDUE_DAYS int comment '' ,
MAX_OVERDUE_DAYS int comment '' ,
REDUCE_PRIN decimal(15,2) comment '减免本金' ,
REDUCE_INTEREST decimal(15,2) comment '减免利息' ,
REDUCE_SVC_FEE decimal(15,2) comment '减免服务费' ,
REDUCE_TERM_FEE decimal(15,2) comment '减免手续费' ,
REDUCE_PENALTY decimal(15,2) comment '减免罚息' ,
REDUCE_MULT_AMT decimal(15,2) comment '减免滞纳金' ,
OVERDUE_PRIN decimal(15,2) comment '' ,
OVERDUE_INTEREST decimal(15,2) comment '' ,
OVERDUE_SVC_FEE decimal(15,2) comment '' ,
OVERDUE_TERM_FEE decimal(15,2) comment '' ,
OVERDUE_PENALTY decimal(15,2) comment '' ,
OVERDUE_MULT_AMT decimal(15,2) comment '' ,
SVC_FEE_RATE decimal(12,8) comment '服务费费率' ,
TERM_FEE_RATE decimal(12,8) comment '' ,
ACQ_ID string comment '' ,
CYCLE_DAY string comment '账户的账单日期' ,
GOODS_PRINC decimal(12,2) comment '关联金额' ,
CAPITAL_PLAN_NO string comment '资金计划编号' ,
CAPITAL_TYPE string comment '资金方类型' ,
LST_UPD_TIME bigint comment '最后一次更新时间' ,
LST_UPD_USER string comment '最后一次更新人' ,
SYNC_DATE string comment '同步日期' 
)partitioned by(d_date string) stored as parquet;
-----------------ods.ecas_order--------------------
create table if not exists ods.ecas_order(ORG string comment '机构号' ,
ORDER_ID string comment '订单编号 : ///@UuidSeq' ,
CHANNEL_ID string comment '服务渠道编号 : ///@EnumRef:cn.sunline.ppy.dictionary.enums.InputSource
VISA|VISA
MC|MC
JCB|JCB
CUP|CUP
AMEX|AMEX
BANK|本行
ICL|ic系统
THIR|第三方
SUNS|阳光
AG|客服' ,
ACCT_NBR string comment '账户编号' ,
ACCT_TYPE string comment '账户类型 : ///@EnumRef:cn.sunline.ppy.dictionary.enums.AccountType

A|人民币独立基本信用账户
B|美元独立基本信用账户
C|人民币共享基本信用账户
D|美元共享基本信用账户
E|人民币独立小额贷款账户
F|人民币活期借记账户
G|人民币共享小额贷款账户
L|人民币存款账户' ,
COMMAND_TYPE string comment '支付指令类型 : ///@EnumRef:cn.sunline.ppy.dictionary.enums.CommandType
SPA|单笔代付
SDB|单笔代扣
QSP|单笔代付查询
QSD|单笔代扣查询
BDB|批量代扣
BDA|批量代付' ,
ORDER_STATUS string comment '订单状态 : ///@EnumRef:cn.sunline.ppy.dictionary.enums.OrderStatus
C|已提交
P|待提交
Q|待审批
W|处理中
S|已完成
V|已失效
E|失败
T|超时
R|已重提
G|拆分处理中
D|拆分已完成
B|撤销
X|已受理待入账' ,
ORDER_TIME bigint comment '订单时间' ,
MER_ID string comment '商户编号' ,
TXN_TYPE string comment '交易类型 : ///@EnumRef:cn.sunline.ppy.dictionary.enums.AuthTransType
Inq|查询
Cash|取现
AgentDebit|付款
Loan|分期
Auth|消费
PreAuth|预授权
PAComp|预授权完成
Load|圈存
Credit|存款
AgentCredit|收款
TransferCredit|转入
TransferDeditDepos|转出
AdviceSettle|结算通知
BigAmountLoan|大' ,
TXN_AMT decimal(15,2) comment '交易金额' ,
CURRENCY string comment '币种' ,
PURPOSE string comment '支付用途' ,
STATUS string comment '交易状态 : 支付状态为SUCCESS成功，其他为失败' ,
CODE string comment '状态码' ,
MESSAGE string comment '状态描述' ,
DUE_BILL_NO string comment '借据号' ,
BUSINESS_DATE string comment '业务日期' ,
SEND_TIME string comment '发送时间' ,
ORI_ORDER_ID string comment '原订单编号' ,
SETUP_DATE string comment '创建日期' ,
OPT_DATETIME bigint comment '更新时间' ,
LOAN_USAGE string comment '贷款用途 : ///@EnumRef:cn.sunline.ecas.definition.enums.LoanUsage
M|预约提前结清扣款
L|放款申请
R|退货
O|逾期扣款
W|赎回结清
I|强制结清扣款
X|账务调整
N|提前还当期' ,
ONLINE_FLAG string comment '联机标识 : ///@EnumRef:cn.sunline.common.enums.Indicator
Y|是
N|否' ,
MEMO string comment '备注' ,
CREATE_TIME bigint comment '创建时间' ,
CREATE_USER string comment '创建人' ,
LST_UPD_TIME bigint comment '最后一次更新时间' ,
LST_UPD_USER string comment '最后一次更新人' ,
CONTR_NBR string comment '合同号' ,
REF_NBR string comment '交易参考号 : 内部交易参考号，生成规则：（yyMMddhhmmddSSS）+4位随机数+3位外部流水号末尾' ,
SERVICE_ID string comment '交易服务码' ,
JPA_VERSION int comment '乐观锁版本号' ,
ORIGINAL_TXN_AMT decimal(15,2) comment '原始交易金额' ,
ONLINE_ALLOW string comment '允许联机标识 : ///@EnumRef:cn.sunline.common.enums.Indicator
Y|是
N|否' ,
ASSIGN_REPAY_IND string comment '指定余额成分还款标志 : ///@EnumRef:cn.sunline.common.enums.Indicator
Y|是
N|否' ,
REPAY_WAY string comment '还款方式 : ///@EnumRef:cn.sunline.ecas.definition.enums.RepayWay
ONLINE|线上
OFFLINE|线下' ,
TERM int comment '处理期数' ,
ORDER_PAY_NO string comment '支付流水号' ,
OUTER_NO string comment '外部凭证号' ,
PAY_CHANNEL string comment '支付渠道' ,
BATCH_SEQ string comment '批次号' ,
APPLY_NO string comment '申请件编号' ,
RESPONSE_CODE string comment '对外返回码 : 对外返回码' ,
RESPONSE_MESSAGE string comment '对外返回描述' ,
REPAY_ITEMS string comment '还款余额成分详情 : 还款余额成分详情' ,
BANK_TRADE_TIME string comment '线下银行订单交易时间' ,
BANK_TRADE_NO string comment '银行交易流水号 : 银行交易流水号' ,
BANK_TRADE_ACT_NO string comment '银行付款账号 : 银行付款账号' ,
BANK_TRADE_ACT_NAME string comment '银行付款账户名称 : 银行付款账户名称' ,
BANK_TRADE_ACT_PHONE string comment '银行预留手机号' ,
TXN_TIME bigint comment '交易时间' ,
SERVICE_SN string comment '流水号' ,
CAPITAL_PLAN_NO string comment '资金计划编号' ,
CAPITAL_TYPE string comment '资金方类型' ,
EXTEND_INFO string comment '扩展信息-json对象' ,
REPAY_SERIAL_NO string comment '还款流水号' ,
SUCCESS_AMT decimal(15,2) comment '成功金额' ,
REAL_BANK_TRADE_NO string comment '实际银行流水号' ,
REAL_BANK_TRADE_TIME bigint comment '实际银行流水时间' ,
CONFIRM_FLAG string comment '确认标志' ,
TXN_DATE string comment '交易日期' 
)partitioned by(d_date string) stored as parquet;
-------------ods.ecas_repay_schedue------------
create table if not exists ods.ecas_repay_schedule(ORG string comment '机构号' ,
SCHEDULE_ID string comment '还款计划ID : ///@UuidSeq' ,
DUE_BILL_NO string comment '借据号' ,
CURR_BAL decimal(15,2) comment '当前余额 : 当前欠款' ,
LOAN_INIT_PRIN decimal(15,2) comment '分期总本金' ,
LOAN_INIT_TERM int comment '分期总期数' ,
CURR_TERM int comment '当前期数' ,
DUE_TERM_PRIN decimal(15,2) comment '应还本金' ,
DUE_TERM_INT decimal(15,2) comment '应还利息' ,
DUE_TERM_FEE decimal(15,2) comment '应还手续费' ,
DUE_SVC_FEE decimal(15,2) comment '应还服务费' ,
DUE_PENALTY decimal(15,2) comment '应还罚息' ,
DUE_MULT_AMT decimal(15,2) comment '应还滞纳金' ,
PAID_TERM_PRIC decimal(15,2) comment '已还本金' ,
PAID_TERM_INT decimal(15,2) comment '已还利息' ,
PAID_TERM_FEE decimal(15,2) comment '已还手续费' ,
PAID_SVC_FEE decimal(15,2) comment '已还服务费' ,
PAID_PENALTY decimal(15,2) comment '已还罚息' ,
PAID_MULT_AMT decimal(15,2) comment '已还滞纳金' ,
REDUCED_AMT decimal(15,2) comment '减免金额' ,
REDUCE_TERM_PRIN decimal(15,2) comment '减免本金' ,
REDUCE_TERM_INT decimal(15,2) comment '减免利息' ,
REDUCE_TERM_FEE decimal(15,2) comment '减免手续费' ,
REDUCE_SVC_FEE decimal(15,2) comment '减免服务费' ,
REDUCE_PENALTY decimal(15,2) comment '减免罚息' ,
REDUCE_MULT_AMT decimal(15,2) comment '减免滞纳金' ,
PENALTY_ACRU decimal(15,6) comment '罚息累计值' ,
PAID_OUT_DATE string comment '还清日期' ,
PAID_OUT_TYPE string comment '还清类型' ,
START_INTEREST_DATE string comment '起息日' ,
PMT_DUE_DATE string comment '到期还款日期' ,
ORIGIN_PMT_DUE_DATE string comment '原到期还款日' ,
PRODUCT_CODE string comment '产品代码' ,
SCHEDULE_STATUS string comment '还款计划状态 : ///@EnumRef:cn.sunline.ecas.definition.enums.ScheduleStatus
N|正常
O|逾期
F|已还清' ,
GRACE_DATE string comment '宽限日期 : 宽限日' ,
OUT_SIDE_SCHEDULE_NO string comment '外部还款计划编号' ,
CREATE_TIME bigint comment '创建时间' ,
CREATE_USER string comment '创建人' ,
LST_UPD_TIME bigint comment '最后一次更新时间' ,
LST_UPD_USER string comment '最后一次更新人' ,
JPA_VERSION int comment '乐观锁版本号' 
)partitioned by(d_date string) stored as parquet;
-----------ods.ecas_repay_hst-----------
create table if not exists ods.ecas_repay_hst(PAYMENT_ID string comment '流水号 : ///@UuidSeq' ,
DUE_BILL_NO string comment '借据号' ,
ACCT_NBR string comment '账户编号' ,
ACCT_TYPE string comment '账户类型 : ///@EnumRef:cn.sunline.ppy.dictionary.enums.AccountType

A|人民币独立基本信用账户
B|美元独立基本信用账户
C|人民币共享基本信用账户
D|美元共享基本信用账户
E|人民币独立小额贷款账户
F|人民币活期借记账户
G|人民币共享小额贷款账户
L|人民币存款账户' ,
BNP_TYPE string comment '余额成分 : ///@EnumRef:cn.sunline.ecas.definition.enums.BucketType
Pricinpal|本金
Interest|利息
Penalty|罚息
Mulct|罚金
Compound|复利
CardFee|年费
OverLimitFee|超限费
LatePaymentCharge|滞纳金
NSFCharge|资金不足罚金
TXNFee|交易费
TERMFee|手续费
SVCFee|服务费
LifeInsuFee|寿险计划包费
S' ,
REPAY_AMT decimal(15,2) comment '还款金额' ,
BATCH_DATE string comment '批量日期' ,
CREATE_TIME bigint comment '创建时间' ,
CREATE_USER string comment '创建人' ,
LST_UPD_TIME bigint comment '最后一次更新时间' ,
LST_UPD_USER string comment '最后一次更新人' ,
JPA_VERSION int comment '乐观锁版本号' ,
TERM int comment '期数' ,
ORG string comment '机构号' ,
ORDER_ID string comment '订单编号' ,
TXN_SEQ string comment '交易流水号 : ///@UuidSeq' ,
TXN_DATE string comment '交易日期' ,
OVERDUE_DAYS int comment '逾期天数' ,
LOAN_STATUS string comment '借据状态' 
)partitioned by(d_date string) stored as parquet;
----------------ods.ecas_customer_lmt---------------
create table if not exists ods.ecas_customer_lmt(ORG string comment '机构号' ,
CUST_LMT_ID string comment '客户额度ID : ///@UuidSeq' ,
CHANNEL string comment '渠道' ,
INIT_LIMIT decimal(12,2) comment '初始额度' ,
EFFECTIVE_LIMIT decimal(12,2) comment '可用额度' ,
LIMIT_CODE string comment '额度编号' ,
LMT_START_EXPIRE_DATE string comment '额度起始有效期' ,
LMT_END_EXPIRE_DATE string comment '额度截止有效期' ,
CREATE_TIME bigint comment '创建时间' ,
CREATE_USER string comment '创建人' ,
JPA_VERSION int comment '乐观锁版本号' ,
CUST_ID string comment '' ,
LST_UPD_TIME bigint comment '最后一次更新时间' ,
LST_UPD_USER string comment '最后一次更新人' 
)partitioned by(d_date string) stored as parquet;
-----------ods.ecas_msg_log----------
create table if not exists ods.ecas_msg_log(MSG_LOG_ID string comment '报文日志ID : ///@UUIDSeq' ,
FOREIGN_ID string comment '关联ID' ,
MSG_TYPE string comment '报文类型' ,
ORIGINAL_MSG string comment '原始报文' ,
TARGET_MSG string comment '目标报文' ,
DEAL_DATE string comment '处理时间' ,
ORG string comment '机构号' ,
CREATE_TIME bigint comment '创建时间' ,
UPDATE_TIME bigint comment '更新时间' ,
JPA_VERSION int comment '乐观锁版本号' 
)partitioned by(d_date string) stored as parquet;
-----------ods.rmas_case_main--------------
create table if not exists ods.rmas_case_main(ORG string comment '机构号' ,
CASE_MAIN_ID string comment '案件编号' ,
CUST_NO string comment '客户编号' ,
ID_TYPE string comment '证件类型' ,
ID_NO string comment '证件号码' ,
CUST_NAME string comment '客户姓名' ,
MOBILE_NO string comment '移动电话' ,
ADDRESS string comment '居住地址' ,
PRE_FUNCTION_CODE string comment '上一逾期阶段' ,
FUNCTION_CODE string comment '逾期阶段' ,
RISK_RANK int comment '风险等级' ,
STATUS_CODE string comment '案件状态代码' ,
COLL_STATE string comment '催收状态' ,
IS_CLOSE string comment '是否结案' ,
IS_WO string comment '是否结清' ,
DEAL_STAT string comment '处理状态' ,
OVER_DUE_DATE string comment '逾期起始日' ,
OVER_DUE_DAYS int comment '逾期天数' ,
SHD_AMT decimal(17,2) comment '应还款额' ,
SHD_PRINCIPAL decimal(17,2) comment '应还本金' ,
SHD_INTEREST decimal(17,2) comment '应还利息' ,
SHD_FEE decimal(17,2) comment '应还手续费' ,
OVERDUE_PRINCIPAL decimal(17,2) comment '逾期未还本金' ,
OVERDUE_COMP_INST decimal(17,2) comment '逾期未还复利' ,
OVERDUE_PENALTY decimal(17,2) comment '逾期未还罚息' ,
OVERDUE_FEE decimal(17,2) comment '逾期未还手续费' ,
OVERDUE_INSTERENT decimal(17,2) comment '逾期未还利息' ,
SEX string comment '性别' ,
REMARK string comment '备注' ,
IS_STOP_COLL string comment '是否停催' ,
TASK_POOL string comment '催收任务池' ,
CREATE_USER string comment '创建人' ,
CREATE_TIME bigint comment '创建时间' ,
LST_UPD_USER string comment '最后修改人' ,
LST_UPD_TIME bigint comment '最后修改时间' ,
CHANNEL_ID string comment '渠道Id' ,
CHANNEL_NAME string comment '渠道名称' ,
PRODUCT_NAME string comment '产品名称' ,
CUSTOMER_MANAGE_NO string comment '客户经理编号' ,
CUSTOMER_MANAGE_NAME string comment '客户经理名称' ,
JPA_VERSION int comment '乐观锁' ,
ORG_CODE string comment '所属机构' ,
LOAN_FEE decimal(15,2) comment '放款金额' ,
SHD_PENALTY decimal(17,2) comment '应还罚息' ,
SHD_MANAGEMENT_FEE decimal(17,2) comment '应还账户管理费' ,
OVERDUE_AMT decimal(17,2) comment '剩余应还总额' ,
OVERDUE_MANAGEMENT_FEE decimal(17,2) comment '逾期应还账户管理费' 
)partitioned by(d_date string) stored as parquet;
-----------ods.rmas_sub_case_info-----------
create table if not exists ods.rmas_sub_case_info(ORG string comment '机构号' ,
CASE_SUB_ID string comment '子案件编号' ,
ORG_CODE string comment '所属机构' ,
ASSURE_COMPANY_NUM string comment '担保公司编号' ,
CASE_MAIN_ID string comment '案件编号' ,
CUST_NO string comment '客户号' ,
ID_TYPE string comment '证件类型' ,
ID_NO string comment '证件号码' ,
CUST_NAME string comment '客户姓名' ,
MOBILE_NO string comment '移动电话' ,
ADDRESS string comment '居住地址' ,
PRE_FUNCTION_CODE string comment '上一逾期阶段' ,
FUNCTION_CODE string comment '逾期阶段' ,
RISK_RANK int comment '风险等级' ,
STATUS_CODE string comment '案件状态' ,
COLL_STATE string comment '催收状态' ,
IS_CLOSE string comment '是否结案' ,
IS_WO string comment '是否结清' ,
COLLECTOR string comment '催收员' ,
DEAL_STAT string comment '处理状态' ,
OA_STATUS string comment '委外申请状态-委外申请逻辑相关' ,
HOLD_DATE bigint comment '跟催日期' ,
OVER_DUE_DATE string comment '逾期起始日' ,
OVER_DUE_DAYS int comment '逾期天数' ,
SHD_AMT decimal(17,2) comment '应还款额' ,
SHD_PRINCIPAL decimal(17,2) comment '应还本金' ,
SHD_INTEREST decimal(17,2) comment '应还利息' ,
SHD_FEE decimal(17,2) comment '应还手续费' ,
OVERDUE_PRINCIPAL decimal(17,2) comment '逾期未还本金' ,
OVERDUE_COMP_INST decimal(17,2) comment '逾期未还复利' ,
OVERDUE_PENALTY decimal(17,2) comment '逾期未还罚息' ,
OVERDUE_FEE decimal(17,2) comment '逾期未还手续费' ,
OVERDUE_INSTERENT decimal(17,2) comment '逾期未还利息' ,
SEX string comment '性别' ,
REMARK string comment '备注' ,
IS_STOP_COLL string comment '是否停催' ,
IS_FREEZE string comment '是否冻结' ,
PRE_TASK_POOL string comment '上一个催收任务池' ,
CASE_TYPE string comment '类型' ,
TASK_POOL string comment '催收任务池' ,
PROMISE_STATUS string comment '承诺状态' ,
PRE_COLLECTOR string comment '上一个催收员' ,
CREATE_USER string comment '创建人' ,
COLLECT_OUT_DATE bigint comment '出催日期' ,
COLLECT_ASSIGN_STATUS string comment '分配状态' ,
ASSIGN_TYPE string comment '分案方式' ,
ASSIGN_DATE bigint comment '分配日期' ,
ASSIGN_CASE_DATE bigint comment '分配案件日期' ,
JPA_VERSION int comment '乐观锁' ,
LP_CORPNO string comment '法人代码' ,
TRANUS string comment '录入人' ,
BRCHNO string comment '录入机构' ,
UPBRCHNO string comment '最后修改机构' ,
CREATE_TIME bigint comment '创建时间' ,
LST_UPD_USER string comment '最后修改人' ,
LST_UPD_TIME bigint comment '最后修改时间' ,
APPLY_STATUS string comment '申请状态' ,
CHANNEL_ID string comment '渠道Id' ,
CHANNEL_NAME string comment '渠道名称' ,
PRODUCT_NAME string comment '产品名称' ,
CUSTOMER_MANAGE_NO string comment '客户经理编号' ,
CUSTOMER_MANAGE_NAME string comment '客户经理名称' ,
COLL_TIME bigint comment '催收时间' ,
OVERDUE_AMT decimal(18,2) comment '逾期未还金额' ,
SHD_PENALTY decimal(18,2) comment '应还罚息' ,
OVER_DUE_NPER decimal(10,0) comment '逾期期数' ,
SHD_REPAY_DATE string comment '账单日' ,
PRODUCT_TYPE string comment '产品类型' ,
COLL_COLLECTION string comment '催收组' ,
LOAN_ID string comment '借据号' ,
PRODUCT_ID string comment '产品ID' ,
IS_CALLED string comment '是否呼叫' ,
LOAN_FEE decimal(17,2) comment '放款金额' ,
OVER_MANAGEMENT_FEE decimal(17,2) comment '逾期应还账户管理费' ,
SHD_MANAGEMENT_FEE decimal(17,2) comment '应还账户管理费' ,
SHD_AMT_PRE decimal(17,2) comment '昨日应还金额' ,
OA_ORG_NO string comment '委外机构编号' ,
CONTR_NBR string comment '合同编号' ,
LAST_DEAL_DATE bigint comment '上次处理日期' ,
OA_ORG_NAME string comment '委外机构名称' ,
BATCH_NUM string comment '批次号' ,
OA_STATE string comment '委外状态' ,
REPAY_DATE string comment '还款日期' ,
ALLOT_CASE_BATCH_NO string comment '分案批次号' ,
PRE_ALLOT_CASE_BATCH_NO string comment '上一次分案批次号' ,
OVERDUE_FALSIFY decimal(18,2) comment '逾期违约金' ,
INPUT_COLL_DATE bigint comment '入催时间' ,
APP_NO string comment '申请编号' ,
OA_ALLOT_STATE string comment '委外分配状态' ,
REPAY_DAY string comment '还款日' ,
LOCK_STATUS string comment '锁定状态' ,
INPUT_COLL_AMOUNT decimal(17,2) comment '入催金额' ,
LOAN_PERIOD string comment '分期总期数' ,
IS_GRACE string comment '是否在宽限期内 0-否   1-是' ,
SCENE_ID string comment '场景方ID' ,
CAPITAL_ID string comment '资金方ID' ,
INPUT_COLL_OVERDUE_PRINCIPAL decimal(17,2) comment '入催时逾期本金' ,
INPUT_COLL_OVERDUE_INTEREST decimal(17,2) comment '入催时逾期利息' ,
INPUT_COLL_OVERDUE_PENALTY decimal(17,2) comment '入催时逾期罚息' ,
INPUT_COLL_OVERDUE_FEE decimal(17,2) comment '入催时逾期费用' ,
CURR_REMAINDER_PRINCIPAL decimal(17,2) comment '当前剩余本金' ,
INPUT_COLL_TERM int comment '入催时所在期次' ,
INPUT_COLL_OVERDUE_DAYS int comment '入催时逾期天数' ,
CURR_TERM int comment '当前所在期次' ,
CURR_INPUT_COLL_DAYS int comment '当前入催天数' 
)partitioned by(d_date string) stored as parquet;
------------ods.rmas_sub_case_out_info-------------
create table if not exists ods.rmas_sub_case_out_info(ID string comment '出催ID' ,
ORG string comment '机构号' ,
CASE_SUB_ID string comment '子案件编号' ,
ORG_CODE string comment '所属机构' ,
ASSURE_COMPANY_NUM string comment '担保公司编号' ,
CASE_MAIN_ID string comment '案件编号' ,
CUST_NO string comment '客户号' ,
ID_TYPE string comment '证件类型' ,
ID_NO string comment '证件号码' ,
CUST_NAME string comment '客户姓名' ,
MOBILE_NO string comment '移动电话' ,
ADDRESS string comment '居住地址' ,
PRE_FUNCTION_CODE string comment '上一逾期阶段' ,
FUNCTION_CODE string comment '逾期阶段' ,
RISK_RANK int comment '风险等级' ,
STATUS_CODE string comment '案件状态' ,
COLL_STATE string comment '催收状态' ,
IS_CLOSE string comment '是否结案' ,
IS_WO string comment '是否结清' ,
COLLECTOR string comment '催收员' ,
DEAL_STAT string comment '处理状态' ,
OA_STATUS string comment '委外申请状态-委外申请逻辑相关' ,
HOLD_DATE bigint comment '跟催日期' ,
OVER_DUE_DATE string comment '逾期起始日' ,
OVER_DUE_DAYS int comment '逾期天数' ,
SHD_AMT decimal(17,2) comment '应还款额' ,
SHD_PRINCIPAL decimal(17,2) comment '应还本金' ,
SHD_INTEREST decimal(17,2) comment '应还利息' ,
SHD_FEE decimal(17,2) comment '应还手续费' ,
OVERDUE_PRINCIPAL decimal(17,2) comment '逾期未还本金' ,
OVERDUE_COMP_INST decimal(17,2) comment '逾期未还复利' ,
OVERDUE_PENALTY decimal(17,2) comment '逾期未还罚息' ,
OVERDUE_FEE decimal(17,2) comment '逾期未还手续费' ,
OVERDUE_INSTERENT decimal(17,2) comment '逾期未还利息' ,
SEX string comment '性别' ,
REMARK string comment '备注' ,
IS_STOP_COLL string comment '是否停催' ,
IS_FREEZE string comment '是否冻结' ,
PRE_TASK_POOL string comment '上一个催收任务池' ,
CASE_TYPE string comment '类型' ,
TASK_POOL string comment '催收任务池' ,
PROMISE_STATUS string comment '承诺状态' ,
PRE_COLLECTOR string comment '上一个催收员' ,
CREATE_USER string comment '创建人' ,
COLLECT_OUT_DATE bigint comment '出催日期' ,
COLLECT_ASSIGN_STATUS string comment '分配状态' ,
ASSIGN_TYPE string comment '分案方式' ,
ASSIGN_DATE bigint comment '分配日期' ,
ASSIGN_CASE_DATE bigint comment '分配案件日期' ,
JPA_VERSION int comment '乐观锁' ,
LP_CORPNO string comment '法人代码' ,
TRANUS string comment '录入人' ,
BRCHNO string comment '录入机构' ,
UPBRCHNO string comment '最后修改机构' ,
CREATE_TIME bigint comment '创建时间' ,
LST_UPD_USER string comment '最后修改人' ,
LST_UPD_TIME bigint comment '最后修改时间' ,
APPLY_STATUS string comment '申请状态' ,
CHANNEL_ID string comment '渠道Id' ,
CHANNEL_NAME string comment '渠道名称' ,
PRODUCT_NAME string comment '产品名称' ,
CUSTOMER_MANAGE_NO string comment '客户经理编号' ,
CUSTOMER_MANAGE_NAME string comment '客户经理名称' ,
COLL_TIME bigint comment '催收时间' ,
OVERDUE_AMT decimal(18,2) comment '逾期未还金额' ,
SHD_PENALTY decimal(18,2) comment '应还罚息' ,
OVER_DUE_NPER decimal(10,0) comment '逾期期数' ,
SHD_REPAY_DATE string comment '账单日' ,
PRODUCT_TYPE string comment '产品类型' ,
COLL_COLLECTION string comment '催收组' ,
LOAN_ID string comment '借据号' ,
PRODUCT_ID string comment '产品ID' ,
IS_CALLED string comment '是否呼叫' ,
LOAN_FEE decimal(17,2) comment '放款金额' ,
OVER_MANAGEMENT_FEE decimal(17,2) comment '逾期应还账户管理费' ,
SHD_MANAGEMENT_FEE decimal(17,2) comment '应还账户管理费' ,
SHD_AMT_PRE decimal(17,2) comment '昨日应还金额' ,
OA_ORG_NO string comment '委外机构编号' ,
CONTR_NBR string comment '合同编号' ,
LAST_DEAL_DATE bigint comment '上次处理日期' ,
OA_ORG_NAME string comment '委外机构名称' ,
BATCH_NUM string comment '批次号' ,
OA_STATE string comment '委外状态' ,
REPAY_DATE string comment '还款日期' ,
ALLOT_CASE_BATCH_NO string comment '分案批次号' ,
PRE_ALLOT_CASE_BATCH_NO string comment '上一次分案批次号' ,
OVERDUE_FALSIFY decimal(18,2) comment '逾期违约金' ,
INPUT_COLL_DATE bigint comment '入催时间' ,
APP_NO string comment '申请编号' ,
OA_ALLOT_STATE string comment '委外分配状态' ,
REPAY_DAY string comment '还款日' ,
LOCK_STATUS string comment '锁定状态' ,
INPUT_COLL_AMOUNT decimal(17,2) comment '入催金额' ,
LOAN_PERIOD string comment '分期总期数' ,
IS_GRACE string comment '是否在宽限期内 0-否   1-是' ,
SCENE_ID string comment '场景方ID' ,
CAPITAL_ID string comment '资金方ID' ,
INPUT_COLL_OVERDUE_PRINCIPAL decimal(17,2) comment '入催时逾期本金' ,
INPUT_COLL_OVERDUE_INTEREST decimal(17,2) comment '入催时逾期利息' ,
INPUT_COLL_OVERDUE_PENALTY decimal(17,2) comment '入催时逾期罚息' ,
INPUT_COLL_OVERDUE_FEE decimal(17,2) comment '入催时逾期费用' ,
CURR_REMAINDER_PRINCIPAL decimal(17,2) comment '当前剩余本金' ,
INPUT_COLL_TERM int comment '入催时所在期次' ,
INPUT_COLL_OVERDUE_DAYS int comment '入催时逾期天数' ,
CURR_TERM int comment '当前所在期次' ,
CURR_INPUT_COLL_DAYS int comment '当前入催天数' 
)partitioned by(d_date string) stored as parquet;
-----------ods.rmas_coll_rec-----------------
create table if not exists ods.rmas_coll_rec(ORG_CODE string comment '所属机构' ,
ORG string comment '机构号' ,
COLL_REC_NO string comment '催记流水号' ,
CASE_SUB_ID string comment '子案件编号' ,
COLL_TIME bigint comment '催收时间' ,
ACTION_CODE string comment '催收结果编码' ,
COLL_RESULT string comment '催收结果详情' ,
CONTACT_NAME string comment '催收对象' ,
TEL_NO string comment '联系电话' ,
COLLECTOR string comment '催收员' ,
PROMISE_AMT decimal(17,2) comment '承诺金额' ,
PROMISE_DATE string comment '承诺日期' ,
BELONG_COLL_COMPANY string comment '所属催收公司' ,
REMARK string comment '备注' ,
JPA_VERSION int comment '乐观锁' ,
LP_CORPNO string comment '法人代码' ,
TRANUS string comment '录入人' ,
BRCHNO string comment '录入机构' ,
UPBRCHNO string comment '最后修改机构' ,
CREATE_TIME bigint comment '创建时间' ,
LST_UPD_TIME bigint comment '最后修改时间' ,
LST_UPD_USER string comment '最后修改人' ,
CUST_NAME string comment '客户姓名' ,
ID_NO string comment '身份证号码' ,
RMAS_ORG string comment '催收机构' ,
BUSINESS_RESULT string comment '业务结果' ,
DUE_BILL_ID string comment '借据号' ,
CONTACT_ID string comment '联系人id' ,
CALLED_REF_TYPE string comment '引用类型' ,
AGENT_ID string comment '操作员号' ,
DEVICE_ID string comment '分机号' ,
CALL_STATUS string comment '通话状态' ,
CALL_RESULT string comment '通话结果' ,
OVERDUE_REASON string comment '逾期原因' ,
OPERATOR_NAME string comment '操作人姓名' ,
DATA_SOURCE string comment '数据源' ,
CONTR_NBR string comment '合同编号' ,
COLL_COLLECTION string comment '催收组' ,
OA_ORG_NO string comment '委外机构编号' 
)partitioned by(d_date string) stored as parquet;
-------------ods.rmas_call_message_work-----------
create table if not exists ods.rmas_call_message_work(ORG_CODE string comment '所属机构' ,
ORG string comment '机构号' ,
CALL_MESSAGE_ID string comment '流水号' ,
CASE_SUB_ID string comment '子案件编号' ,
SEND_STATE string comment '发送状态' ,
SEND_TYPE string comment '发送类型' ,
CALL_CREATE_TIME bigint comment '通话创建时间' ,
CALL_START_TIME bigint comment '通话开始时间' ,
CALL_END_TIME bigint comment '通话结束时间' ,
CALL_DURATION int comment '总通话时长，单位s' ,
HANDLE_DATE bigint comment '下游平台开始处理时间' ,
CALL_COST float comment '总费用' ,
CALL_STATUS int comment '是否应答 0：未应答 1.应答' ,
MSG string comment '应答信息' ,
TEL_NO string comment '发送号码' ,
SEND_DATE bigint comment '发送时间' ,
COLLECTOR string comment '操作员' ,
CREATE_USER string comment '创建人' ,
JPA_VERSION int comment '乐观锁' ,
CREATE_TIME bigint comment '创建时间' ,
LST_UPD_USER string comment '最后修改人' ,
LST_UPD_TIME bigint comment '最后修改时间' ,
EMAIL_ADDRESS string comment '邮件地址' ,
CUST_NAME string comment '客户姓名' ,
CONTACT_NAME string comment '联系人姓名' ,
RELATIONSHIP string comment '联系人关系' ,
CONTACT_ID string comment '联系人ID' ,
RECORD_NAME string comment '录音' ,
RES_ID string comment '呼叫中心ID' ,
OA_ORG_NO string comment '委外机构编号' ,
COLL_COLLECTION string comment '催收组' ,
LOAN_ID string comment '借据号' 
)partitioned by(d_date string) stored as parquet;
------------ods.rmas_message_work-----------
create table if not exists ods.rmas_message_work(ORG_CODE string comment '所属机构' ,
ORG string comment '机构号' ,
MESSAGE_ID string comment '流水号' ,
CASE_SUB_ID string comment '子案件编号' ,
TEMPLATE_CODE string comment '消息模板代码' ,
MESSAGE_TYPE string comment '信息类型' ,
SEND_STAT string comment '发送状态' ,
SEND_TYPE string comment '发送类型' ,
CONTENT string comment '信息内容' ,
TASK_POOL string comment '任务池' ,
TEL_NO string comment '发送号码' ,
SEND_DATE bigint comment '发送时间' ,
COLLECTOR string comment '操作员' ,
CREATE_USER string comment '创建人' ,
REMARK string comment '备注' ,
JPA_VERSION int comment '乐观锁' ,
LP_CORPNO string comment '法人代码' ,
TRANUS string comment '录入人' ,
BRCHNO string comment '录入机构' ,
UPBRCHNO string comment '最后修改机构' ,
CREATE_TIME bigint comment '创建时间' ,
LST_UPD_USER string comment '最后修改人' ,
LST_UPD_TIME bigint comment '最后修改时间' ,
EMAIL_ADDRESS string comment '邮件地址' ,
TEMPLATE_PARAM string comment '模板参数' ,
OA_ORG_NO string comment '委外机构编号' ,
COLL_COLLECTION string comment '催收组' 
)partitioned by(d_date string) stored as parquet;
----------ods.rmas_coll_promise----------
create table if not exists ods.rmas_coll_promise(ORG_CODE string comment '所属机构' ,
ORG string comment '机构号' ,
PROMISE_ID string comment '承诺付款流水号' ,
PROMISE_AMT decimal(17,2) comment '承诺金额' ,
PROMISE_DATE string comment '承诺日期' ,
SHD_PMT_AMT decimal(17,2) comment '还款金额' ,
COLLECTOR string comment '催收员' ,
PROMISE_STATUS string comment '承诺状态' ,
JPA_VERSION int comment '乐观锁' ,
CASE_SUB_ID string comment '子案件编号' ,
LP_CORPNO string comment '法人代码' ,
TRANUS string comment '录入人' ,
BRCHNO string comment '录入机构' ,
UPBRCHNO string comment '最后修改机构' ,
CREATE_TIME bigint comment '创建时间' ,
LST_UPD_USER string comment '最后修改人' ,
LST_UPD_TIME bigint comment '最后修改时间' ,
CONTR_NBR string comment '合同编号' ,
LOAN_ID string comment '借据号' 
)partitioned by(d_date string) stored as parquet;
-----------ods.rmas_early_repay_check------------
create table if not exists ods.rmas_early_repay_check(LEGAL_APPLY_NUM string comment '还款代扣申请流水' ,
ORG string comment '机构号' ,
SUB_CASE_MAIN_ID string comment '子案件编号' ,
LOAN_ID string comment '借据编号' ,
CUST_NAME string comment '客户姓名' ,
ID_TYPE string comment '证件类型' ,
ID_NO string comment '证件号码' ,
TOTAL_AMOUNT_MONEY decimal(18,2) comment '代扣金额' ,
APPLY_USER string comment '申请人' ,
APPLY_DATE string comment '申请时间' ,
APPLY_DESC string comment '申请说明' ,
REAL_DEBIT_RESULT string comment '代扣结果' ,
PAYMENT_TYPE string comment '代扣类型' ,
REPAY_CHANNEL string comment '代扣申请渠道' ,
PAYMENT_TIME string comment '代扣时间' ,
OPERATOR string comment '操作人' ,
OPERAT_DATE string comment '操作时间' ,
MANUAL_REPAY_TERM decimal(18,0) comment '手工还款期次' ,
MANUAL_CAPITAL decimal(18,2) comment '手工本金' ,
MANUAL_INT decimal(18,2) comment '手工利息' ,
MANUAL_OINT decimal(18,2) comment '手工罚息' ,
MANUAL_PAYCOMPOUND decimal(18,2) comment '手工复利' ,
MANUAL_FEE decimal(18,2) comment '手工费用' ,
MANUAL_FEE_DETAILS string comment '还款明细' ,
REPAYMENT_ACCOUNT_TYPE string comment '还款账户类型' ,
BANK_CARD_NO string comment '还款银行卡号' ,
ORDER_ID string comment '还款订单编号' ,
ACTIVE_REPAY_DATE string comment '还款时间' ,
COLL_REC_NO int comment '催记流水号' ,
TRANS_CODE string comment '交易类型' ,
TRANS_SERIAL_NO string comment '交易流水号' ,
DUE_BILL_ID string comment '借据号' ,
CHECK_USER string comment '审批人' ,
CHECK_DATE bigint comment '审批日期' ,
CHECK_DESC string comment '审批说明' ,
CHECK_STAT string comment '审批状态' ,
TOTAL_AMT decimal(18,2) comment '需冻结金额' ,
SERIAL_NO string comment '流水号' ,
JPA_VERSION int comment '乐观锁' ,
REPAY_PRINCIPAL decimal(17,2) comment '归还本金' ,
REPAY_INTEREST decimal(17,2) comment '归还利息' ,
REPAY_PENALTY_AMOUNT decimal(17,2) comment '归还逾期罚息' ,
REPAY_FEE decimal(17,2) comment '归还费用' ,
ACTUAL_REPAY_AMOUNT decimal(18,2) comment '还款金额' ,
FAILURE_MESSAGE string comment '失败原因' ,
APPLY_DATETIME bigint comment '代扣申请时间' 
)partitioned by(d_date string) stored as parquet;
---------ods.rmas_derate_apply-----------
create table if not exists ods.rmas_derate_apply(ID string comment 'ID' ,
DER_APPLY_NUM string comment '减免申请流水号' ,
ORG string comment '机构号' ,
CUST_NAME string comment '客户姓名' ,
DUE_BILL_NO string comment '借据号' ,
APPLY_USER string comment '减免申请人' ,
APPLY_DATE string comment '减免申请时间' ,
DER_REASON string comment '减免原因' ,
DER_DESC string comment '减免说明' ,
CHECK_USER string comment '减免审批人员' ,
CHECK_DATE bigint comment '减免审批时间' ,
CHECK_DESC string comment '减免审批说明' ,
DER_AMT decimal(18,2) comment '减免总额' ,
DER_TOTAL_AMT decimal(18,2) comment '减免承诺还款金额' ,
DER_MANA_EXPENSE decimal(18,2) comment '减免费用' ,
DER_PAYCOMPOUND decimal(18,2) comment '减免复利' ,
DER_OINT decimal(18,2) comment '减免罚息' ,
DER_PENALTY decimal(18,2) comment '减免违约金' ,
DER_INT decimal(18,2) comment '减免利息' ,
DER_CAPITAL decimal(18,2) comment '减免本金' ,
JPA_VERSION int comment '乐观锁' ,
DER_STAT string comment '减免进度' ,
CONTRACT_NO string comment '合同号' ,
OVER_DUE_DAYS decimal(10,0) comment '逾期天数' ,
OVER_DUE_AMT decimal(15,2) comment '逾期金额' ,
DERATE_TRANS_DATE string comment '减免交易时间' ,
DERATE_RESULT string comment '减免结果' ,
CHECK_END_DATE bigint comment '减免审批结束时间' ,
CHECK_LEVEL string comment '审批级别' ,
NEXT_CHECK_USER string comment '下一审批人' ,
NEXT_CHECK_LEVEL string comment '下一审批级' ,
REDUCTION_TYPE string comment '减免方式' ,
CASE_SUB_ID string comment '子案件编号' ,
CHECK_STATUS string comment '审批状态' ,
REDUCTION_PROPORTION decimal(10,2) comment '减免比例%' ,
TXN_STATUS string comment '扣款结果' ,
IF_PAYMENT string comment '是否扣款' ,
ACTUAL_REPAY_AMOUNT decimal(15,2) comment '还款金额' ,
REPAY_PRINCIPAL decimal(15,2) comment '归还本金' ,
REPAY_INTEREST decimal(15,2) comment '归还利息' ,
REPAY_PENALTY_AMOUNT decimal(15,2) comment '归还逾期罚息' ,
REPAY_FEE decimal(15,2) comment '还款费用' ,
FAILURE_MESSAGE string comment '失败原因' ,
FUNCTION_CODE string comment '逾期阶段' ,
CUST_NO string comment '客户号' ,
TXN_SEQ string comment '交易流水号' ,
OA_ORG_NO string comment '委外机构编号' ,
ACQ_ID string comment '合作机构号' ,
APPLY_DATETIME bigint comment '减免申请时间' 
)partitioned by(d_date string) stored as parquet;
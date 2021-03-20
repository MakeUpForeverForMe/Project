create database dm;

-- 看管还款计划表
drop table dm.dm_watch_repayment_schedule;
CREATE TABLE dm.dm_watch_repayment_schedule(
	id							STRING 			NOT NULL comment '主键',
	d_date						STRING			NOT NULL comment '分区时间',
	bill_no						STRING			NOT NULL comment '借据编号',
	term						INT				DEFAULT NULL comment '期数',
	bill_date					STRING			DEFAULT NULL comment '账单日',
	should_repay_date			STRING			DEFAULT NULL comment '还款日',
	grace_date					STRING			DEFAULT NULL comment '宽限日',
	should_repay_amount			DECIMAL(15,4)	DEFAULT NULL comment '应还金额',
	should_repay_principal		DECIMAL(15,4)	DEFAULT NULL comment '应还本金',
	should_repay_interest		DECIMAL(15,4)	DEFAULT NULL comment '应还利息',
	should_repay_fee			DECIMAL(15,4)	DEFAULT NULL comment '应还费用',
	accumulate_penalty_interest	DECIMAL(15,4)	DEFAULT NULL comment '累计罚息',
	repaid_amount				DECIMAL(15,4)	DEFAULT NULL comment '已还金额',
	repaid_pincipal				DECIMAL(15,4)	DEFAULT NULL comment '已还本金',
	repaid_interest				DECIMAL(15,4)	DEFAULT NULL comment '已还利息',
	repaid_fee					DECIMAL(15,4)	DEFAULT NULL comment '已还费用',
	repaid_penalty_interest		DECIMAL(15,4)	DEFAULT NULL comment '已还罚息',
	repaid_count				INT				DEFAULT NULL comment '还款笔数',
	settlement_status			STRING			DEFAULT NULL comment '还款计划状态',
	create_time STRING 			DEFAULT NULL comment '创建时间',
	update_time STRING 			DEFAULT NULL comment '更新时间',
	PRIMARY KEY(ID)
)
PARTITION BY HASH PARTITIONS 8
COMMENT '还款计划表'
STORED AS KUDU;



-- 看管全量借据表
drop table dm.dm_watch_bill_snapshot;
CREATE TABLE dm.dm_watch_bill_snapshot(
	id											STRING			NOT NULL			comment '主键',
	d_date										STRING			NOT NULL			comment '分区日期',
	ecif_no                               		STRING          DEFAULT NULL        comment 'ecif号',
	partner_code                          		STRING          DEFAULT NULL        comment '合作方渠道编号',
	product_code                          		STRING          DEFAULT NULL        comment '产品编号',
	bill_no                               		STRING          DEFAULT NULL        comment '借据编号',
	contract_code                         		STRING          DEFAULT NULL        comment '合同编号',
	loan_date                             		STRING          DEFAULT NULL        comment '放款日',
	loan_amount                           		DECIMAL(15,2)   DEFAULT NULL        comment '放款金额',
	loan_term                             		INT             DEFAULT NULL     	comment '贷款期数',
	bill_status                           		STRING          DEFAULT NULL        comment '借据状态',
	current_remain_principal              		DECIMAL(15,2)   DEFAULT NULL        comment '当前剩余本金',
	current_repaid_principal              		DECIMAL(15,2)   DEFAULT NULL        comment '当前已还本金',
	should_repay_principal                		DECIMAL(15,2)   DEFAULT NULL        comment '当前应还本金',
	should_repay_month                    		STRING          DEFAULT NULL        comment '应还款月(yyyy-MM)',
	should_repay_date                     		STRING          DEFAULT NULL        comment '应还款日(yyyy-MM-dd)',
	current_overdue_days                  		INT             DEFAULT NULL     	comment '当前逾期天数',
	current_overdue_principal             		DECIMAL(15,2)   DEFAULT NULL        comment '当前逾期本金',
	current_overdue_term                  		INT             DEFAULT NULL     	comment '当前逾期期次',
	current_overdue_stage                 		STRING          DEFAULT NULL        comment '当前逾期阶段',
	accumulate_overdue_days               		INT             DEFAULT NULL    	comment '累计逾期天数',
	accumulate_overdue_num                		INT             DEFAULT NULL     	comment '累计逾期次数',
	loan_settlement_date                  		STRING          DEFAULT NULL        comment '借款结清日',
	snapshot_month                        		STRING          DEFAULT NULL        comment '快照月(yyyy-MM)',
	effective_date                        		STRING          DEFAULT NULL        comment '合同生效日',
	expiry_date                           		STRING          DEFAULT NULL        comment '合同到期日',
	contract_interest_rate                		DECIMAL(15,6)   DEFAULT NULL        comment '合同利率',
	rate_type                             		STRING          DEFAULT NULL        comment '利率类型',
	repayment_frequency                   		STRING          DEFAULT NULL        comment '还款频率',
	repayment_type                        		STRING          DEFAULT NULL        comment '还款方式',
	repayment_day                         		INT             DEFAULT NULL     	comment '还款日(day)',
	nominal_interest_rate                 		DECIMAL(15,6)   DEFAULT NULL        comment '名义利率',
	nominal_fee_rate                      		DECIMAL(15,6)   DEFAULT NULL        comment '名义费率',
	daily_penalty_interest_rate           		DECIMAL(15,6)   DEFAULT NULL        comment '日罚息率',
	guaranty_type                         		STRING          DEFAULT NULL        comment '担保方式',
	loan_purpose                          		STRING          DEFAULT NULL        comment '贷款用途',
	borrower_type                         		STRING          DEFAULT NULL        comment '借款方类型',
	borrower_name                         		STRING          DEFAULT NULL        comment '借款人姓名',
	card_type                             		STRING          DEFAULT NULL        comment '证件类型',
	card_no                               		STRING          DEFAULT NULL        comment '证件号码',
	sex                               	  		STRING          DEFAULT NULL        comment '性别',
	age                                   		STRING          DEFAULT NULL        comment '年龄',
	id_card_area                          		STRING          DEFAULT NULL        comment '身份证地区',
	apply_area                            		STRING          DEFAULT NULL        comment '申请地地区',
	education                             		STRING          DEFAULT NULL        comment '学历',
	annual_income                         		DECIMAL(15,2)   DEFAULT NULL        comment '年收入',
	current_term                          		INT             DEFAULT NULL     	comment '当前期数',
	repaid_term                           		INT             DEFAULT NULL     	comment '已还期数',
	remain_term                           		INT             DEFAULT NULL     	comment '剩余期数',
	total_interest                        		DECIMAL(15,2)   DEFAULT NULL        comment '利息总额',
	total_fee                             		DECIMAL(15,2)   DEFAULT NULL        comment '费用总额',
	accumulate_penalty_interest           		DECIMAL(15,2)   DEFAULT NULL        comment '累计罚息',
	current_remain_interest               		DECIMAL(15,2)   DEFAULT NULL        comment '当前剩余利息',
	current_remain_fee                    		DECIMAL(15,2)   DEFAULT NULL        comment '当前剩余费用',
	current_remain_penalty_interest       		DECIMAL(15,2)   DEFAULT NULL        comment '当前剩余罚息',
	history_most_overdue_days             		INT             DEFAULT NULL        comment '历史最大逾期天数',
	history_most_overdue_principal        		DECIMAL(15,2)   DEFAULT NULL        comment '历史最大逾期本金',
	accumulate_overdue_principal          		DECIMAL(15,2)   DEFAULT NULL        comment '累计逾期本金',
	first_term_overdue                    		STRING          DEFAULT NULL        comment '首期是否逾期',
	current_risk_control_status           		STRING          DEFAULT NULL        comment '当前风控状态',
	first_overdue_current_remain_principal      DECIMAL(15,2)   DEFAULT NULL        comment '首次逾期当天剩余本金',
	snapshot_date                               STRING          DEFAULT NULL        comment '快照日(yyyy-MM-dd)',
	PRIMARY KEY(ID)
)PARTITION BY HASH PARTITIONS 8
COMMENT '看管全量借据表'
STORED AS KUDU;

-------实际还款信息表-----
drop table dm.dm_watch_repayment_info;
CREATE TABLE dm.dm_watch_repayment_info(
 id STRING not null comment 'id',
 bill_no STRING default null comment '借据号',
 d_date STRING default null comment '快照日',
 repaid_term int default null comment '还款期次',
 actual_repaid_date STRING default null comment '实还日期',
 actual_repaid_amount DECIMAL(20,2) default null comment '已还金额',
 actual_repaid_principal DECIMAL(20,2) default null comment '已还本金',
 actual_repaid_interest DECIMAL(20,2) default null comment '已还利息',
 actual_repaid_fee DECIMAL(20,2) default null comment '已还费用',
 actual_repaid_penalty_interest DECIMAL(20,2) default null comment '已还罚息',
 principal_balance DECIMAL(20,2) default null comment '还款后剩余本金',
 account_status STRING default null comment '还款类型',
 update_time STRING default null comment '更新时间（yyyy-MM-ddHH:mm:ss）',
 create_time STRING default null comment '创建时间（yyyy-MM-ddHH:mm:ss）',
 PRIMARY KEY(id)
)PARTITION BY HASH PARTITIONS 8
STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '1');

--授信详情表
drop table if exists dm.dm_watch_credit_detail
CREATE TABLE dm.dm_watch_credit_detail
(
  ID                    string not null comment '主键id:申请号+日期'
  ecif_no               string default null comment 'ecif号',
  channel_code          string default null comment '合作方渠道编号',
  product_code          string default null comment '产品编号',
  is_credit_success     int    default null comment '是否授信通过，1-通过，0-不通过',
  failure_msg           string default null comment '授信失败原因',
  approval_time         string default null comment '授信通过时间（YYYY-MM-DDHH:mm:ss）',
  approval_amount       decimal(15,4) default null comment '授信通过金额',
  apply_amount          decimal(15,4) default null comment '授信申请金额',
  apply_time            string default null comment '授信申请时间（YYYY-MM-DDHH:mm:ss）',
  credit_id             string default null comment '授信id',
  credit_validity       string default null comment '授信有效期',
  ext                   string default null comment '扩展字段',
  d_date         string comment '快照日期(yyyy-MM-dd)',
  PRIMARY KEY(ID)
)
PARTITION BY HASH PARTITIONS 8
COMMENT '授信详情表'
STORED AS KUDU;

--授信信息快照表
drop table if exists dm.dm_watch_credit_info
CREATE TABLE dm.dm_watch_credit_info
(
  ID                string  not null comment '主键id:申请号+日期',
  apply_no          string  not null comment '申请号',
  bill_no           string  default null comment '借据编号',
  assessment_date   string  default null comment '评估日期',
  credit_result     string  default null comment '授信结果',
  credit_amount     decimal(15,4) default null  comment '授信额度',
  credit_validity   string  default null comment '授信有效期（yyyy-MM-dd）',
  refuse_reason     string  default null comment '拒绝原因',
  current_remain_credit_amount                decimal(15,4) default null comment '当前剩余额度',
  current_credit_amount_utilization_rate      decimal(10,2) default null comment '当前额度使用率',
  accumulate_credit_amount_utilization_rate   decimal(10,2) default null comment '累计额度使用率',
  snapshot_date     string comment '快照日期(yyyy-MM-dd)',
  update_time       string default null comment '更新时间',
  create_time       string default null comment '创建时间',
  d_date            string comment '快照日期(yyyy-MM-dd)',
  PRIMARY KEY(ID)
)
PARTITION BY HASH PARTITIONS 8
COMMENT '授信信息快照表'
STORED AS KUDU;

--用信风控结果表
drop table if exists dm.dm_watch_credit_results
CREATE TABLE dm.dm_watch_credit_results
(
  ID                    string not null comment '主键id:借据号+日期',
  bill_no               string default null comment '借据编号',
  risk_control_type     string default null comment '风控类型',
  assessment_date       string default null comment '评估日期',
  risk_control_result   string default null comment '风控结果',
  asset_level           string default null comment '资产等级',
  credit_level          string default null comment '信用等级',
  anti_fraud_level      string default null comment '反欺诈等级',
  credit_amount         decimal(15,4) default null comment '金额',
  credit_validity       string default null comment '有效期',
  refuse_reason         string default null comment '拒绝原因',
  update_time           string default null comment '更新时间',
  create_time           string default null comment '创建时间',
  d_date            string comment '快照日期(yyyy-MM-dd)',
  PRIMARY KEY(ID)
)
PARTITION BY HASH PARTITIONS 8
COMMENT '用信风控结果表'
STORED AS KUDU;

--用信详情表
drop table if exists dm.dm_watch_on_loan_detail
CREATE TABLE dm.dm_watch_on_loan_detail
(
  ID                string not null comment '主键id:借据号+日期'
  ecif_no           string default null comment 'ecif号',
  loan_id           string default null comment '借据号',
  channel_code      string default null comment '合作方渠道编号',
  product_code      string default null comment '产品编号',
  apply_amt         decimal(15,4) default null comment '申请用信金额',
  approval_amt      decimal(15,4) default null comment '用信审批通过金额',
  refusal_cause     string default null comment '拒绝原因（编码）',
  refusal_cause_cn  string default null comment '拒绝原因（详情）',
  snapshot_date     string default null comment '快照日期（YYYY-MM-DD）',
  data_time         string default null comment '数据时间：未审批时为申请时间，审批后为审批时间（YYYY-MM-DD）',
  credit_id         string default null comment '授信id',
  apply_date        string default null comment '申请用信时间（YYYY-MM-DD）',
  approval_date     string default null comment '审批时间（YYYY-MM-DD）',
  approval_status   int    default null comment '审批状态1-通过，0-不通过',
  first_use_credit  int    default null comment '是否是第一次用信：1-是，0-否',
  product_terms     int    default null comment '申请用信的产品的期数',
  ext               string default null comment '扩展字段',
  d_date            string comment '快照日期(yyyy-MM-dd)',
  PRIMARY KEY(ID)
)
PARTITION BY HASH PARTITIONS 8
COMMENT '用信详情表'
STORED AS KUDU;

---联系人
drop table if exists dm.dm_watch_contact_person;
CREATE TABLE dm.dm_watch_contact_person(
 `id` STRING not null comment 'id',
  `ecif_no` STRING default null comment 'ecif',
 `bill_no` STRING default null comment '借据号',
 `relationship` STRING default null  comment '与借款人关系		维度-客户联系人信息表	与申请人关系',
 `name` STRING default null comment '姓名',
 `phone_num` STRING default null comment '手机号',
 `update_time` STRING default null  comment '更新时间',
 `create_time` STRING default null comment '创建时间',
 `d_date` STRING comment '批次日',
 PRIMARY KEY(id)
)PARTITION BY HASH PARTITIONS 8
COMMENT '联系人表'
STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '1');

--资产支付流水信息表
drop table  dm.dm_watch_asset_pay_flow;
CREATE TABLE dm.dm_watch_asset_pay_flow(
 `id` STRING not null comment 'id',
 `bill_no` STRING default null comment '借据号',
 `trade_type` STRING default null  comment '交易类型',
 `trade_channel` STRING default null comment '交易渠道',
 `order_no` STRING default null comment '订单号',
 `trade_time` STRING default null comment '交易时间',
 `order_amount` DECIMAL(15,4) default null comment '订单金额',
 `trade_status` STRING default null comment '批次日',
 `update_time` STRING default null  comment '更新时间',
 `create_time` STRING default null comment '创建时间',
  `d_date` STRING  COMMENT '批次日',
 PRIMARY KEY(id)
)PARTITION BY HASH PARTITIONS 8
comment '资产支付流水信息表'
STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '1');

-- 产品统计信息表
drop table if exists dm.dm_watch_product_summary;
CREATE TABLE dm.dm_watch_product_summary(
 `id` STRING not null comment 'id',
 `channel_code` STRING default null comment '渠道编号',
 `product_code` STRING default null comment '产品编号',
 `loan_count_daily` INT default null  comment '放款笔数',
 `loan_daily` DECIMAL(15,4) default null comment '放款金额',
 `repay_count_daily` INT default null comment '还款笔数',
 `repay_daily` DECIMAL(15,4) default null  comment '还款金额',
 `date_time` STRING default null comment '统计日期',
 `pass_rate` DECIMAL(15,4) default null  comment '通过率		授信通过人数/授信申请人数',
 `bad_rate` DECIMAL(15,4) default null comment '坏账率		逾期超过180天的贷款所有剩余未还本金/累计放款金额',
 `loan_sum` DECIMAL(15,4) default null comment '累计放款金额',
 `repay_sum` DECIMAL(15,4) default null  comment '累计还款金额',
 `loan_avg` DECIMAL(15,4) default null comment '平均放款金额',
 `repay_avg` DECIMAL(15,4) default null comment '平均还款金额		累计还款金额/还款笔数',
 `loan_request_amount` INT default null comment '贷款申请笔数',
 `over_due_remain` DECIMAL(15,4) default null comment '剩余逾期金额',
 `over_due_loan_sum` DECIMAL(15,4) default null comment '逾期总额',
  `d_date` STRING comment '批次日',
 PRIMARY KEY(id)
)PARTITION BY HASH PARTITIONS 8
comment '产品统计信息表'
STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '1');



--dm催收采集表

drop table if exists dm.dm_watch_collection_info;
create table if not exists dm.dm_watch_collection_info(
    id string not null COMMENT  '',
    ecif_no string default null comment  '客户要素',
    bill_no string default null comment  '借据编号',
    acquisition_time string default null comment  '催收时间',
    acquisition_group string default null comment  '催收组',
    acquisition_person string default null comment  '催收员',
    customer_name string default null comment  '客户姓名',
    contacts_name string default null comment  '联系人姓名',
    contacts_relation string default null comment  '联系人关系',
    contact_number string default null comment  '联系电话',
    contact_result string default null comment  '联系结果',
    acquisition_result string default null comment  '催收结果',
    result_detail string default null comment  '结果详情',
    promise_amount decimal(15,4) default null comment  '承诺金额',
    promise_date string default null comment  '承诺日期',
    update_time string default null comment  '更新时间（yyyy-MM-ddHH:mm:ss）',
    create_time string default null comment  '创建时间（yyyy-MM-ddHH:mm:ss）',
    snapshot_date string default null comment  '快照日',
    PRIMARY KEY(id)
)PARTITION BY HASH PARTITIONS 8
STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '1');

--dm催收回收信息表

drop table if exists dm.dm_watch_recovery_detail;
create table if not exists dm.dm_watch_recovery_detail(
    id string not null comment  '',
    ecif_no string  default null comment  'ecif号',
    case_sub_id string  default null comment  '子案件编号',
    partner_code    string  default null comment  '合作方渠道编号',
    product_code    string  default null comment  '产品编号',
    bill_no string  default null comment  '借据编号',
    contract_code   string  default null comment  '合同编号',
    loan_term   int default null comment  '贷款期数',
    input_coll_date string  default null comment  '入催日期(yyyy-MM-dd)',
	coll_out_date   string  comment '出催日期',
    input_coll_month    string  default null comment  '入催月(yyyy-MM)',
    input_coll_amount   decimal(15,4)   default null comment  '入催金额',
    is_close    string  default null comment  '是否结案',
    is_contact  string  default null comment  '是否可联',
    last_deal_date  string  default null comment  '上次触碰日期(yyyy-MM-dd)',
    promise_amount  decimal(15,4)   default null comment  '承诺金额',
    cash_promise_amount decimal(15,4)   default null comment  '承诺兑现金额',
    snapshot_month  string  default null comment  '快照月(yyyy-MM)',
    snapshot_date string default null comment  '快照日',
    PRIMARY KEY(id)
    )PARTITION BY HASH PARTITIONS 8
STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '1');

--dm催收承诺表

drop table if exists dm.dm_watch_commitment_info;
create table if not exists dm.dm_watch_commitment_info(
    id string not null comment  '',
    ecif_no string default null comment  '客户要素',
    bill_no string default null comment  '借据编号',
    acquisition_time string default null comment  '催收时间',
    promise_amount decimal(15,4) default null comment  '承诺金额',
    promise_date string default null comment  '承诺日期',
    repaid_amount decimal(15,4) default null comment  '还款金额',
    promise_status string default null comment  '承诺状态',
    update_time string default null comment  '更新时间（yyyy-MM-ddHH:mm:ss）',
    create_time string default null comment  '创建时间（yyyy-MM-ddHH:mm:ss）',
    snapshot_date string default null comment  '快照日',
    PRIMARY KEY(id)
)PARTITION BY HASH PARTITIONS 8
STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '1');

--dm录音记录表

drop table if exists dm.dm_watch_sound_record_info;
create table if not exists dm.dm_watch_sound_record_info(
    id string not null comment  '',
    ecif_no string default null comment  '客户要素',
    bill_no string default null comment  '借据编号',
    contacts_name string default null comment  '联系人姓名',
    contacts_relation string default null comment  '联系人关系',
    outbound_call_number string default null comment  '外呼号码',
    send_status string default null comment  '发送状态',
    is_answer string default null comment  '是否应答',
    talk_time string default null comment  '通话时长',
    acquisition_group string default null comment  '催收组',
    acquisition_person string default null comment  '催收员',
    acquisition_record_document_name string default null comment  '催收录音文件名称',
    acquisition_record_document_path string default null comment  '催收录音文件下载地址',
    outbound_call_time string default null COMMENT  '外呼时间',--
    update_time string default null comment  '更新时间（yyyy-MM-ddHH:mm:ss）',
    create_time string default null comment  '创建时间（yyyy-MM-ddHH:mm:ss）',
    snapshot_date string default null comment  '快照日',
    PRIMARY KEY(id)
)PARTITION BY HASH PARTITIONS 8
STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '1');

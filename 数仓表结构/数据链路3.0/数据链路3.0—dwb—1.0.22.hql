create database dwb;

--用信申请表
drop table `dwb.dwb_loan_apply`;
create table if not exists `dwb.dwb_loan_apply`(
`org`				string 			comment '机构号',
`ecif_id`			string			comment '客户ecif号',
`credit_id`		string 			comment '授信id',
`apply_no`		string 			comment '申请号',
`loan_amt`		decimal(15,2) 	comment '贷款金额',
`loan_term`		int				comment '贷款期数',
`repay_type`		string 			comment '1 等额本金 2等额本息',
`interest_rate`	decimal(15,6)	comment '利息利率（年化）',
`penalty_rate`	decimal(15,6)	comment '罚息利率（年化）',
`user_name`		string			comment '用户姓名',
`id_no`			string 			comment '证件号',
`bank_card_no`	string 			comment '银行卡号',
`mobile_no`		string			comment '银行预留手机号',
`loan_usage`		string			comment '1日常消费 2汽车加油 3修车保养 4购买汽车 5医疗支出 6教育深造 7房屋装修 8旅游出行 9其他消费',
`process_date`	string			comment '处理时间',
`apply_date`		string			comment '用信申请时间',
`apply_time`		bigint			comment '用信申请日期',
`product_code`    string 			comment '产品编码',
`due_bill_no`  string       comment '借据号',
`status`  	int 			comment '借款状态 1: 放款成功 2: 放款失败 3: 处理中',
`message`  	string 			comment '借款信息：包括失败原因',
`issueTime`  	bigint 			comment '放款时间,放款成功后必填',
`loanAmount`  	decimal(15,6) 			comment '贷款金额，单位分',
`channel_id`  string  comment '渠道号'
) comment'DWB用信申请表'
PARTITIONED BY (p_type string)
stored as parquet;

drop table `dwb.dwb_risk_control_feature`;
create table  if not exists `dwb.dwb_risk_control_feature`(
`org`								string 			comment '机构号',
`ecif_id`							string			comment '客户ecif号',
`credit_id`						string 			comment '授信id',
`apply_no`						string 			comment '申请号',
`risk_score`						string 			comment '曼指数评分',
`order_start_stability_level`		string			comment '打车地点稳定性等级',
`consume_stability_level`			string			comment '打车消费稳定性等级',
`imei_freq_change_3m`				string			comment '近3个月设备是否频繁更换',
`phone_imei_level_3m`				string			comment '3个月内同一手机imei一级关联的手机号数量等级',
`imei_phone_level_3m`				string			comment '3个月内同一手机号一级关联的imei数量等级',
`device_stability_1year`			string			comment '近1年移动设备是否频繁更换',
`regist_month_level`				string			comment '注册时长等级',
`night_ordernum_level_1m`			string			comment '近1个月内深夜打车订单数占比等级',
`cost_level_1m`					string 			comment '近1个月平台消费等级',
`cost_level_3m`					string			comment '近3个月平台消费等级',
`is_often_used_dev`				string 			comment '是否滴滴常用设备',
`is_often_used_addr`				string			comment '是否为滴滴常用地址',
`idcard_frontphoto_uploadtime`	string			comment '身份证正面拍摄/上传时间（上传时间）（毫秒时间戳）',
`idcard_backphoto_uploadtime`		string 			comment '身份证反面拍摄/上传时间（上传时间）（毫秒时间戳）',
`idcard_frontphoto_shoottimes`	string			comment '身份证正面拍摄次数',
`photo_match_fail_times`			string			comment '大头照比对失败次数',
`company_address_inputtime`		string			comment '工作单位名称输入时长（毫秒）',
`contact_mobile_inputtime`		string			comment '联系人电话输入时长（毫秒）',
`home_address_inputtime`			string			comment '家庭地址输入时长（毫秒）',
`product_page_staytime`			string			comment '产品介绍页逗留时长（毫秒）',
`product_page_first_viewtime`		string			comment '首次进入产品介绍页时间（毫秒时间戳）',
`access_channel`					string			comment '进件渠道',
`bind_card_time`					string			comment '银行卡签约时间（毫秒时间戳）',
`repayplan_page_staytime`			string 			comment '还款计划表页面停留时长（毫秒）',
`loan_term_modifytimes`			string			comment '借款期限修改次数',
`is_same_apply_imei`				string			comment '本次提现使用imei与申请时是否一致',
`is_sanme_aplly_city`				string			comment '本次提现所在城市区与申请时是否一致',
`acount_id`						string			comment '用户账号',
`longtitude_md5`					string			comment 'gps经度',
`latitude_md5`					string			comment 'gps纬度',
`gps_apply_city_md5`				string			comment '申请GPS城市',
`device_id`						string			comment '设备指纹',
`open_id_md5`						string 			comment 'openid加密',
`deal_date`					string			comment '处理时间',
`process_date`					string 			comment '批量时间',
`create_time`						string			comment '创建时间'
)comment'DWB风控用户特征信息'
PARTITIONED BY (p_type string)
stored as parquet;

drop table dwb.dwb_loan;
create table if not exists  dwb.dwb_loan(
  ecif_id 				string 			COMMENT 'ecif号', 
  loan_id 				string 			COMMENT '借据ID', 
  channel_id 			string 			COMMENT '渠道ID', 
  capital_id 			string 			COMMENT '资金方ID', 
  product_code 			string 			COMMENT '产品编码', 
  due_bill_no 			string 			COMMENT '借据号', 
  register_date 		string 			COMMENT '放款日', 
  loan_init_principal 	decimal(15,2) 	COMMENT '贷款本金', 
  loan_init_term 		int 			COMMENT '贷款总期数', 
  loan_init_interest 	decimal(15,2) 	COMMENT '贷款总利息', 
  loan_init_fee 		decimal(15,2) 	COMMENT '贷款总费用', 
  remain_principal 		decimal(15,2) 	COMMENT '剩余本金', 
  remain_interest 		decimal(15,2) 	COMMENT '剩余利息', 
  loan_principal 		decimal(15,2) 	COMMENT '应还本金', 
  loan_interest 		decimal(15,2) 	COMMENT '应还利息', 
  loan_penalty 			decimal(15,2) 	COMMENT '应还罚息', 
  loan_fee 				decimal(15,2) 	COMMENT '应还费用', 
  paid_principal 		decimal(15,2) 	COMMENT '已还本金', 
  paid_interest 		decimal(15,2) 	COMMENT '已还利息', 
  paid_penalty 			decimal(15,2) 	COMMENT '已还罚息', 
  paid_fee 				decimal(15,2) 	COMMENT '已还费用', 
  overdue_principal 	decimal(15,2) 	COMMENT '逾期本金', 
  overdue_interest 		decimal(15,2) 	COMMENT '逾期利息', 
  overdue_fee			decimal(15,2)  	COMMENT '逾期费用',
  curr_term 			int 			COMMENT '当前期数', 
  paid_out_date 		string 			COMMENT '贷款还款日期', 
  overdue_date 			string 			COMMENT '逾期起始日期', 
  overdue_days 			int 			COMMENT '逾期天数', 
  cpd_begin_date 		string 			COMMENT 'cpd开始时间', 
  loan_expire_date 		string 			COMMENT '贷款到期日', 
  interest_rate 		decimal(15,6) 	COMMENT '利息利率', 
  fee_rate 				decimal(15,6) 	COMMENT '费用利率', 
  penalty_rate			decimal(15,6) 	COMMENT '罚息利率', 
  contr_nbr 			string 			COMMENT '合同号',  
  loan_usage			string 			COMMENT '贷款用途', 
  application_no 		string 			COMMENT '进件号', 
  loan_status 			string 			COMMENT '借据状态', 
  create_time 			bigint 			COMMENT '业务借据创建时间', 
  loan_terminal_code 	string 			COMMENT '分期终止原因代码 P|提前还款 D|逾期自动终止 T|退货终止 F|强制结清终止',
  paid_out_type			string			COMMENT '还清类型',
  flag 					string 			COMMENT '状态 1新增，2非新增',
  cycle_day				string			COMMENT '',
  user_field1 			string 			COMMENT '备用字段'
)COMMENT 'DWB借据表'
PARTITIONED BY (d_date string,p_type string)
STORED AS PARQUET;

drop table dwb.dwb_repay_schedule;
create table if not exists dwb.dwb_repay_schedule(
	schedule_id				STRING comment '还款计划ID',
	loan_id           		STRING comment '借据ID',
	channel_id        		STRING comment '渠道ID',
	capital_id        		STRING comment '资金方ID',
	term           			int comment '当前期数',
	due_bill_no          	STRING comment '借据号',
	loan_pmt_due_date      	STRING comment '到期还款日',
	register_date			STRING comment '放款日',
	remain_principal     	decimal(15,2) comment '剩余本金',
	remain_interest     	decimal(15,2) comment '剩余利息',
	loan_principal     		decimal(15,2) comment '应还本金',
	loan_interest     		decimal(15,2) comment '应还利息',
	loan_penalty     		decimal(15,2) comment '应还罚息',
	loan_fee     			decimal(15,2) comment '应还费用',
	paid_principal     		decimal(15,2) comment '已还本金',
	paid_interest     		decimal(15,2) comment '已还利息',
	paid_penalty       		decimal(15,2) comment '已还罚息',
	paid_fee        		decimal(15,2) comment '已还费用',
	paid_mult				decimal(15,2) comment '已还滞纳金',
	reduce_principal		decimal(15,2) comment '减免本金',
	reduce_interest			decimal(15,2) comment '减免利息',
	reduce_fee				decimal(15,2) comment '减免费用',
	reduce_penalty			decimal(15,2) comment '减免罚息',
	reduce_mult				decimal(15,2) comment '减免滞纳金',
	penalty_acru			decimal(15,2) comment '罚息累计金额',
	paid_out_date           STRING	comment '还清日期',
	paid_out_type         	STRING comment '还清类型',
	schedule_status      	STRING comment '还款计划状态 N|正常 O|逾期 F|已还清',
	grace_date       		STRING comment '宽限日',
	flag					STRING comment '状态 1新增，2非新增'
)COMMENT 'dwb还款计划表'
partitioned by (d_date STRING,p_type string)
STORED AS PARQUET;

drop table dwb.dwb_repay_hst;
create table if not exists dwb.dwb_repay_hst(
	payment_id 	STRING 			comment '主键',
	loan_id		STRING    		comment 'load_id',
	due_bill_no STRING 			comment '借据号',
	bnp_type 	STRING 			comment '
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
									SVCFee|服务费',
	repay_amt 	decimal(15,2) 	comment '实还金额',
	batch_date 	STRING 			comment'批量日期',
	term 		int 			comment '期数',
	order_id 	STRING 			comment '订单ID',
	txn_seq 	STRING 			comment '交易序列号',
	txn_date 	STRING  		comment '交易日期',
	loan_status STRING			comment '借据状态'
)COMMENT 'DWB还款分配表'
partitioned by (p_type STRING)
STORED AS PARQUET;

drop table dwb.dwb_order;
create table if not exists dwb.dwb_order(
	ecif_id					STRING 	comment 'ecif_id',
	channel_id        		STRING 	comment '渠道ID',
	capital_id        		STRING 	comment '资金方ID',
	order_id        		STRING 	comment '订单ID',
	loan_id  				STRING 	comment '借据ID',
	due_bill_no 			STRING 	comment '借据号',
	apply_no				STRING 	comment '申请件编号',
	product_code			STRING 	comment '产品号',
	service_id				STRING	comment 'service_id',
	command_type 			STRING  comment '指令类型 SPA|单笔代付 SDB|单笔代扣 BDB|批量代扣 BDA|批量代付',
	purpose					STRING	comment '支付用途',
	business_date			STRING 	comment '业务时间',
	term					int     comment '期数',
	order_time 				bigint  comment '订单时间',
	order_status 			STRING  comment '订单状态',
	loan_usage				STRING  comment '订单类型',
	repay_way				STRING  comment '还款方式 N|正常 O|逾期 T|提前还款 P|提前结清',
	txn_amt 				decimal(15,2) comment '交易金额',
	bank_trade_no			STRING	comment '交易流水号',
	bank_acct				STRING	comment '银行卡账户号',
	bank_acct_name			STRING	comment '银行卡账户名称',
	memo 					STRING  comment '备注',
	code					STRING	comment '支付状态码',
	message					STRING	comment '支付状态描述',
	flag					STRING  comment '状态 1新增，2非新增'
)COMMENT 'DWB订单表'
partitioned by (d_date STRING,p_type STRING)
STORED AS PARQUET;






--业务数据库客户信息表
drop table `dwb.dwb_bussiness_customer_info`;
create table if not exists `dwb.dwb_bussiness_customer_info`(
`org` string  comment '机构号',
`channel` string  comment '渠道',
`cust_id` string comment '客户号 : ///@uuidseq',
`outer_cust_id`string  comment '外部客户号',
`id_no` string  comment '证件号码',
`id_type` string  comment '证件类型',
`name` string  comment '姓名',
`mobie` string  comment '移动电话',
`cust_lmt_id` string  comment '客户额度id',
`create_time` string  comment '创建时间',
`create_user` string  comment '创建人',
`jpa_version` int  comment 'jpa_version',
`overflow_amt` decimal(20,6)   comment '溢缴款',
`lst_upd_time` string  comment '最后一次更新时间',
`lst_upd_user` string   comment '最后一次更新人',
`gender` string  ,
`bir_date` string  ,
`marital_status` string  ,
`permanent_address` string  ,
`now_address` string  ,
`bank_no` string  ,
`apply_no` string   comment '申请编号',
`city` string  ,
`job_type` string  ,
`province` string  ,
`country` string,
`ecif_id`string
)comment '业务数据库客户表'
partitioned by (d_date string)
stored as parquet;



---dwb 联系人信息表

drop table `dwb.dwb_customer_linkman_info`;
create table if not exists `dwb.dwb_customer_linkman_info`(
`linkman_name` string comment '联系人名称',
`linkman_phone` string comment '联系人电话',
`ecif` string comment 'ecif',
`cust_name` string comment '客户姓名',
`relation_ship` string comment '联系人与申请人关系',
`linkman_addr` string comment '联系人地址',
`linkman_idcard` string comment '联系人身份证号',
   `org`   string comment '租户号',
   `create_time`   string comment '创建时间',
    `update_time`   string comment '更新时间'
)comment '客户联系人信息表'
partitioned by (d_date string,p_type string)
stored as parquet;

-- 银行卡信息表
drop table `dwb.dwb_bank_card_info`;
create table if not exists `dwb.dwb_bank_card_info`(
`card_id` string COMMENT '银行卡id',
  `CUST_ID` string comment '客户id',
`due_bill_no` string COMMENT '借据号',
`bank_card_id_no` string comment '证件号码',
`bank_card_name` string comment '姓名',
`bank_card_phone` string comment '手机号',
`pay_channel` int comment '支付渠道 1 宝付  2 通联',
`card_flag` string comment '绑卡标志',
`agreement_no` string comment '绑卡协议编号',
`bank_cardNo` string comment '银行卡号',
`bank_no` string comment '银行编号',
`bank_name` string comment '银行名称',
`province` string comment '银行分行-省',
`city` string comment '银行分行-市',
`tied_card_time` string comment '绑卡时间',
`ecif` string comment 'ecif',
`org` string comment 'org'
)comment '银行卡信息表'
partitioned by (d_date string)
stored as parquet;

---dwb 绑卡信息变更变
drop table  `dwb.dwb_bind_card_change` ;
create table if not exists `dwb.dwb_bind_card_change` (
  `CHANGE_ID` string COMMENT '主键',
  `ORG` string comment '租户号',
    `CUST_ID` string comment '客户id',
  `DUE_BILL_NO`string COMMENT '借据号',
  `OLD_BANK_CARD_ID_NO` string COMMENT '旧证件号码',
  `OLD_BANK_CARD_NAME` string COMMENT '旧姓名',
  `OLD_BANK_CARD_PHONE` string COMMENT '旧手机号',
  `OLD_BANK_CARD_NO` string COMMENT '旧银行卡号',
  `OLD_PAY_CHANNEL` string COMMENT '旧支付渠道:1-宝付,2-通联',
  `OLD_AGREEMENT_NO` string COMMENT '旧绑卡协议编号',
  `OLD_CARD_FLAG` string COMMENT '旧标识:N|正常,F|非客户本人,共同借款人,配偶',
  `NEW_BANK_CARD_ID_NO` string COMMENT '新证件号码',
  `NEW_BANK_CARD_NAME` string COMMENT '新姓名',
  `NEW_BANK_CARD_PHONE`  string COMMENT '旧手机号',
  `NEW_BANK_CARD_NO` string COMMENT '新银行卡号',
  `NEW_PAY_CHANNEL` string COMMENT '新支付渠道:1-宝付,2-通联',
  `NEW_AGREEMENT_NO` string COMMENT '新绑卡协议编号',
  `NEW_CARD_FLAG` string COMMENT '新标识:N|正常,F|非客户本人,共同借款人,配偶',
  `CREATE_TIME` string,
  `LST_UPD_TIME` string,
  `JPA_VERSION` string,
  `old_ecif` string comment '旧身份证号对应的ecif',
  `new_ecif` string comment '新身份证号对应的ecif'
  )comment '绑卡信息更新信息表'
  partitioned by (d_date string)
stored as parquet;


--授信申请表

drop table `dwb.dwb_credit_apply`;
create table if not exists `dwb.dwb_credit_apply`(
  `org`              STRING COMMENT '租户号(FK)',
  `APPLY_ID`         STRING COMMENT 'APPLY_ID',
  `channel_id`       STRING COMMENT '渠道ID',
  `ecif_id`          STRING COMMENT 'ecif_id',
  `product_code`     STRING COMMENT '产品编号',
  `apply_no`         STRING COMMENT '进件号',
  `apply_date`       STRING COMMENT '授信申请日期',
  `apply_time`       bigint COMMENT '授信申请时间',
  `apply_amt`        decimal(15,2) COMMENT '申请金额',
  `apply_status`     STRING COMMENT '申请结果',
  `apply_type`       STRING COMMENT '申请类型',
  `contr_no`         STRING COMMENT '合同号',
  `resp_code`        STRING COMMENT '授信结果码',
  `resp_msg`         STRING COMMENT '结果描述',
  `credit_time`      bigint COMMENT '授信时间',
  `process_date`     STRING COMMENT '处理时间',
  `start_date`     STRING COMMENT '授信开始时间',
  `end_date`     STRING COMMENT '授信结束时间',
  `limit_amt`        decimal(15,2) COMMENT '初始授信额度'
) COMMENT 'DWB授信申请表'
  partitioned by (p_type string)
STORED AS PARQUET;

--授信结果表
drop table `dwb.dwb_credit_result`;
create table if not exists `dwb.dwb_credit_result`(
    `org`                     STRING comment '租户号',
    `credit_result`           STRING comment '授信状态|1:通过 2:不通过 3:已受理处理中',
    `credit_id`               STRING comment '资金方申请单号',
    `apply_id`                STRING comment '滴滴授信申请id',
    `ecif_id`                 STRING comment 'ecif_id',
    `product_code`            STRING comment '产品编号',
    `amount`                  decimal(15,2) comment '授信额度',
    `interest_rate`           decimal(15,6) comment '利率',
    `interest_penalty_rate`   decimal(15,6) comment '罚息率',
    `start_date`              STRING comment '授信有效起始日期',
    `end_date`                STRING comment '授信有效截止日期',
    `lockdown_end_time`       bigint comment '禁闭期截止日期',
    `reject_code`             STRING comment '授信拒绝码',
    `reject_reason`           STRING comment '授信拒绝原因',
    `credit_time`             bigint COMMENT  '授信时间'
)COMMENT 'DWB授信结果表'
partitioned by (d_date STRING,p_type string)
STORED AS PARQUET;

--额度流水表
drop table `dwb.dwb_credit_change_record`;
create table if not exists `dwb.dwb_credit_change_record`(
    `org`                         STRING comment '租户号',
    `apply_id`                   STRING comment '进件ID',
    `op_type`                     STRING comment '变更类型|0:提高额度 1:降低利息 9:调整有效期',
	`op_step`                    STRING comment '0—预提交1—执行当 opType 为 1 即降息情况下，opStep 分两步，先预提交后执行当 opType 为 0 即提高额度情况下，opStep 直接传 1 即可',
	`op_date`                     STRING comment '操作日期',
	`op_time`                    bigint comment '操作时间',
    `reqSn`                       STRINg comment '额度变更流水号',
    `ecif_id`                     STRING comment 'ecif_id',
    `product_code`                STRING comment '产品编号',
    `oldAmount`                   decimal(15,2) comment '变更前授信额度',
    `newAmount`                   decimal(15,2) comment '变更后授信额度',
    `oldInterestRate`             decimal(15,6) comment '变更前利率',
    `newInterestRate`             decimal(15,6) comment '变更后利率',
    `odsInterestPenaltyRate`      decimal(15,6) comment '变更前罚息率',
    `newInterestPenaltyRate`      decimal(15,6) comment '变更后罚息率',
    `oldEndDate`                  STRING comment '变更前有效期截止日期',
    `newEndDate`                  STRING comment '变更后有效期截止日期',
    `remark`                      STRING comment '变更原因',
    `error_code`                  STRING comment '授信变更结果',
    `error_msg`                   STRING comment '授信变更结果描述',
    `create_time`                 bigint comment '创建时间'
)COMMENT 'DWB额度流水变更表'
partitioned by (d_date STRING,p_type STRING)
STORED AS PARQUET;

--风控结果表
drop table dwb.dwb_risk_control_result;
CREATE TABLE if not exists dwb.dwb_risk_control_result(
  `channel_id` 			string COMMENT '渠道ID',
  `capital_id` 			string COMMENT '资金方ID',
  `application_no` 		string COMMENT '进件号',
  `contr_nbr` 			string COMMENT '合同号',
  `assess_date` 			string COMMENT '评估日期',
  `process_result` 		string COMMENT '评估结果',
  `amt` 					decimal(15,2) COMMENT '金额',
  `outside_reject_reason` string COMMENT '对外拒绝原因',
  `code`				string comment '拒绝码',
  `reason` 				string comment '拒绝原因',
  `process_date`  string comment '处理时间',
  `ecif_id` 			string comment 'ecif',
  `org` 			string comment 'org',
  `product_code` 			string comment '产品编码'
 )COMMENT 'DWB风控结果表'
 partitioned by (p_type STRING)

STORED AS parquet;



--dwb催收案件信息表

DROP TABLE  dwb.`dwb_case_main`;
CREATE TABLE if not exists dwb.`dwb_case_main` (
  `org`                             STRING COMMENT '机构号',
  `ecif_id`                         STRING COMMENT 'ecif号',
  `case_main_id`                    STRING COMMENT '案件编号',
  `cust_no`                         STRING COMMENT '客户编号',
  `id_type`                         STRING COMMENT '证件类型',
  `id_no`                           STRING COMMENT '证件号码',
  `cust_name`                       STRING COMMENT '客户姓名',
  `mobile_no`                       STRING COMMENT '移动电话',
  `pre_function_code`               STRING COMMENT '上一逾期阶段',
  `function_code`                   STRING COMMENT '逾期阶段',
  `risk_rank`                       INT    COMMENT '风险等级',
  `status_code`                     STRING COMMENT '案件状态代码',
  `coll_state`                      STRING COMMENT '催收状态',
  `is_close`                        STRING COMMENT '是否结案',
  `is_wo`                           STRING COMMENT '是否结清',
  `deal_stat`                       STRING COMMENT '处理状态',
  `over_due_date`                   STRING COMMENT '逾期起始日',
  `over_due_days`                   INT    COMMENT '逾期天数',
  `shd_amt`                         DECIMAL(17,2)  COMMENT '应还款额',
  `shd_principal`                   DECIMAL(17,2)  COMMENT '应还本金',
  `shd_interest`                    DECIMAL(17,2)  COMMENT '应还利息',
  `shd_fee`                         DECIMAL(17,2)  COMMENT '应还手续费',
  `overdue_principal`               DECIMAL(17,2)  COMMENT '逾期未还本金',
  `overdue_comp_inst`               DECIMAL(17,2)  COMMENT '逾期未还复利',
  `overdue_penalty`                 DECIMAL(17,2)  COMMENT '逾期未还罚息',
  `overdue_fee`                     DECIMAL(17,2)  COMMENT '逾期未还手续费',
  `overdue_insterent`               DECIMAL(17,2)  COMMENT '逾期未还利息',
  `sex`                             STRING  COMMENT '性别',
  `is_stop_coll`                    STRING  COMMENT '是否停催',
  `task_pool`                       STRING  COMMENT '催收任务池',
  `create_user`                     STRING  COMMENT '创建人',
  `create_time`                     STRING  COMMENT '创建时间',
  `lst_upd_user`                    STRING  COMMENT '最后修改人',
  `lst_upd_time`                    STRING  COMMENT '最后修改时间',
  `channel_id`                      STRING  COMMENT '渠道Id',
  `channel_name`                    STRING  COMMENT '渠道名称',
  `product_name`                    STRING  COMMENT '产品名称',
  `org_code`                        STRING COMMENT '所属机构',
  `loan_fee`                        DECIMAL(15,2)  COMMENT '放款金额',
  `shd_penalty`                     DECIMAL(17,2)  COMMENT '应还罚息',
  `shd_management_fee`              DECIMAL(17,2)  COMMENT '应还账户管理费',
  `overdue_amt`                     DECIMAL(17,2)  COMMENT '剩余应还总额',
  `overdue_management_fee`          DECIMAL(17,2)  COMMENT '逾期应还账户管理费'
) COMMENT 'dwb催收案件信息表' 
PARTITIONED BY (d_date STRING,p_type STRING) 
STORED AS PARQUET;

--dwb催收子案件信息表

DROP TABLE  dwb.`dwb_sub_case_info`;
CREATE TABLE if not exists dwb.`dwb_sub_case_info` (
  org                             STRING COMMENT '机构号',
  case_sub_id                     STRING COMMENT '子案件编号',
  ecif_id                         STRING COMMENT 'ecif号',
  org_code                        STRING COMMENT '所属机构',
  case_main_id                    STRING COMMENT '案件编号',
  cust_no                         STRING COMMENT '客户号',
  id_type                         STRING COMMENT '证件类型',
  id_no                           STRING COMMENT '证件号码',
  cust_name                       STRING COMMENT '客户姓名',
  mobile_no                       STRING COMMENT '移动电话',
  function_code                   STRING COMMENT '逾期阶段',
  risk_rank                       STRING COMMENT '风险等级',
  status_code                     STRING COMMENT '案件状态代码',
  is_close                        STRING COMMENT '是否结案',
  is_wo                           STRING COMMENT '是否结清',
  collector                       STRING COMMENT '催收员',
  deal_stat                       STRING COMMENT '处理状态',
  over_due_days                   INT COMMENT '逾期天数',
  overdue_principal               DECIMAL(15,2) COMMENT '逾期未还本金',
  overdue_penalty                 DECIMAL(15,2) COMMENT '逾期未还罚息',
  overdue_fee                     DECIMAL(15,2) COMMENT '逾期未还费用',
  overdue_insterent               DECIMAL(15,2) COMMENT '逾期未还利息',
  sex                             STRING COMMENT '性别',
  collect_out_date                STRING COMMENT '出催日期',
  collect_assign_status           STRING COMMENT '分配状态',
  create_time                     BIGINT COMMENT '创建时间',
  overdue_amt                     DECIMAL(15,2) COMMENT '逾期未还金额',
  over_due_nper                   INT COMMENT '逾期期数',
  coll_collection                 STRING COMMENT '催收组',
  loan_id                         STRING COMMENT '借据号',
  channel_id                      STRING COMMENT '渠道Id',
  channel_name                    STRING COMMENT '渠道名称',
  product_name                    STRING COMMENT '产品名称',
  product_id                      STRING COMMENT '产品ID',
  oa_state                        STRING COMMENT '委外状态',
  contr_nbr                       STRING COMMENT '合同编号',
  last_deal_date                  STRING COMMENT '上次处理日期',
  repay_date                      STRING COMMENT '还款日期',
  input_coll_date                 STRING COMMENT '入催时间',
  oa_allot_state                  STRING COMMENT '委外分案状态',
  allot_case_batch_no             STRING COMMENT '分案批次号',
  pre_allot_case_batch_no         STRING COMMENT '上一次分案批次号',
  input_coll_amount               DECIMAL(15,2) COMMENT '入催金额',
  loan_period                     STRING COMMENT '分期总期数',
  is_grace                        STRING COMMENT '是否在宽限期内',
  scene_id                        STRING COMMENT '场景方ID',
  capital_id                      STRING COMMENT '资金方ID',
  input_coll_overdue_principal    DECIMAL(15,2) COMMENT '入催时逾期本金',
  input_coll_overdue_interest     DECIMAL(15,2) COMMENT '入催时逾期利息',
  input_coll_overdue_penalty      DECIMAL(15,2) COMMENT '入催时逾期罚息',
  input_coll_overdue_fee          DECIMAL(15,2) COMMENT '入催时逾期费用',
  input_coll_term                 INT COMMENT '入催时所在期次',
  input_coll_overdue_days         INT COMMENT '入催时逾期天数',
  curr_remainder_principal        DECIMAL(15,2) COMMENT '当前剩余本金',
  curr_term                       INT COMMENT '当前所在期次',
  curr_input_coll_days            INT COMMENT '当前入催天数'
) COMMENT '子案件信息表' 
PARTITIONED BY (d_date STRING,p_type STRING) 
STORED AS PARQUET;

--子案件出催信息表

DROP TABLE  dwb.`dwb_sub_case_out_info`;
CREATE TABLE if not exists dwb. `dwb_sub_case_out_info` (
  id                              STRING COMMENT '出催ID',
  ecif_id                         STRING COMMENT 'ecif号',
  org                             STRING COMMENT '机构号',
  case_sub_id                     STRING COMMENT '子案件编号',
  case_main_id                    STRING COMMENT '案件编号',
  org_code                        STRING COMMENT '所属机构',
  cust_no                         STRING COMMENT '客户号',
  id_type                         STRING COMMENT '证件类型',
  id_no                           STRING COMMENT '证件号码',
  cust_name                       STRING COMMENT '客户姓名',
  mobile_no                       STRING COMMENT '移动电话',
  function_code                   STRING COMMENT '逾期阶段',
  risk_rank                       STRING COMMENT '风险等级',
  status_code                     STRING COMMENT '案件状态代码',
  coll_state                       STRING COMMENT '催收状态',
  is_close                        STRING COMMENT '是否结案',
  is_wo                           STRING COMMENT '是否结清',
  collector                       STRING COMMENT '催收员',
  deal_stat                       STRING COMMENT '处理状态',
  over_due_days                   INT COMMENT '逾期天数',
  overdue_principal               DECIMAL(15,2) COMMENT '逾期未还本金',
  overdue_penalty                 DECIMAL(15,2) COMMENT '逾期未还罚息',
  overdue_fee                     DECIMAL(15,2) COMMENT '逾期未还费用',
  overdue_insterent               DECIMAL(15,2) COMMENT '逾期未还利息',
  sex                             STRING COMMENT '性别',
  is_stop_coll                    STRING COMMENT '是否停催',
  collect_out_date                STRING COMMENT '出催日期',
  collect_assign_status           STRING COMMENT '分配状态',
  create_time                     BIGINT COMMENT '创建时间',
  overdue_amt                     DECIMAL(15,2) COMMENT '逾期未还金额',
  over_due_nper                   INT COMMENT '逾期期数',
  coll_collection                 STRING COMMENT '催收组',
  loan_id                         STRING COMMENT '借据号',
  channel_id                      STRING COMMENT '渠道Id',
  channel_name                    STRING COMMENT '渠道名称',
  product_name                    STRING COMMENT '产品名称',
  product_id                      STRING COMMENT '产品ID',
  oa_state                        STRING COMMENT '委外状态',
  contr_nbr                       STRING COMMENT '合同编号',
  last_deal_date                  STRING COMMENT '上次处理日期',
  repay_date                      STRING COMMENT '还款日期',
  input_coll_date                 STRING COMMENT '入催时间',
  oa_allot_state                  STRING COMMENT '委外分案状态',
  allot_case_batch_no             STRING COMMENT '分案批次号',
  pre_allot_case_batch_no         STRING COMMENT '上一次分案批次号',
  input_coll_amount               DECIMAL(15,2) COMMENT '入催金额',
  loan_period                     STRING COMMENT '分期总期数',
  is_grace                        STRING COMMENT '是否在宽限期内',
  scene_id                        STRING COMMENT '场景方ID',
  capital_id                      STRING COMMENT '资金方ID',
  input_coll_overdue_principal    DECIMAL(15,2) COMMENT '入催时逾期本金',
  input_coll_overdue_interest     DECIMAL(15,2) COMMENT '入催时逾期利息',
  input_coll_overdue_penalty      DECIMAL(15,2) COMMENT '入催时逾期罚息',
  input_coll_overdue_fee          DECIMAL(15,2) COMMENT '入催时逾期费用',
  input_coll_term                 INT COMMENT '入催时所在期次',
  input_coll_overdue_days         INT COMMENT '入催时逾期天数',
  curr_remainder_principal        DECIMAL(15,2) COMMENT '当前剩余本金',
  curr_term                       INT COMMENT '当前所在期次',
  curr_input_coll_days            INT COMMENT '当前入催天数'
) COMMENT '子案件出催信息表' 
PARTITIONED BY (d_date STRING,p_type STRING) 
STORED AS PARQUET;

--催收记录表

DROP TABLE  dwb.`dwb_coll_rec`;
CREATE TABLE if not exists dwb. `dwb_coll_rec` (
  coll_rec_no           STRING COMMENT '催记流水号',
  case_sub_id           STRING COMMENT '子案件编号',
  coll_time             BIGINT COMMENT '催收时间',
  coll_date             STRING COMMENT '催收日期',
  action_code           STRING COMMENT '催收动作',
  coll_result           STRING COMMENT '催收结果',
  contact_name          STRING COMMENT '催收对象',
  tel_no                STRING COMMENT '联系电话',
  collector             STRING COMMENT '催收员',
  promise_amt           DECIMAL(15,2) COMMENT '承诺金额',
  promise_date          STRING COMMENT '承诺日期',
  create_time           BIGINT COMMENT '创建时间',
  loan_id               STRING COMMENT '借据号',
  called_ref_type       STRING COMMENT '引用类型',
  call_status           STRING COMMENT '通话状态',
  call_result           STRING COMMENT '通话结果',
  cust_name             STRING COMMENT '客户姓名',
  coll_collection       STRING COMMENT '催收组',
  oa_org_no             STRING COMMENT '委外机构编号'
) COMMENT '催收记录表' 
PARTITIONED BY (d_date STRING,p_type STRING) 
STORED AS PARQUET;

--外呼消息作业表

DROP TABLE  dwb.`dwb_call_message_work`;
CREATE TABLE if not exists dwb. `dwb_call_message_work` (
  call_message_id       STRING COMMENT '流水号',
  case_sub_id           STRING COMMENT '子案件编号',
  send_state            STRING COMMENT '发送状态',
  send_type             STRING COMMENT '发送类型',
  call_create_time      BIGINT COMMENT '通话创建时间',
  call_start_time       BIGINT COMMENT '通话开始时间',
  call_end_time         BIGINT COMMENT '通话结束时间',
  call_duration         INT COMMENT '总通话时长，单位s',
  handle_date           BIGINT COMMENT '下游平台开始处理时间',
  call_cost             DECIMAL(15,2) COMMENT '总费用',
  call_status           INT COMMENT '是否应答 0：未应答 1.应答',
  msg                   STRING COMMENT '应答信息',
  tel_no                STRING COMMENT '发送号码',
  send_date             BIGINT COMMENT '发送时间',
  collector             STRING COMMENT '操作员',
  create_time           BIGINT COMMENT '发送时间',
  cust_name             STRING COMMENT '客户姓名',
  contact_name          STRING COMMENT '联系人姓名',
  relationship          STRING COMMENT '联系人关系',
  contact_id            STRING COMMENT '联系人ID',
  record_name           STRING COMMENT '录音',
  oa_org_no             STRING COMMENT '委外机构编号',
  coll_collection       STRING COMMENT '催收组',
  loan_id               STRING COMMENT '借据号'
) COMMENT '外呼消息作业表' 
PARTITIONED BY (d_date STRING,p_type STRING) 
STORED AS PARQUET;

--消息作业表

DROP TABLE  dwb.`dwb_message_work`;
CREATE TABLE if not exists dwb. `dwb_message_work` (
  message_id        STRING COMMENT '流水号',
  case_sub_id         STRING COMMENT '子案件编号',
  message_type        STRING COMMENT '信息类型',
  send_stat         STRING COMMENT '发送状态',
  send_type         STRING COMMENT '发送类型',
  content         STRING COMMENT '信息内容',
  task_pool         STRING COMMENT '任务池',
  tel_no        STRING COMMENT '发送号码',
  send_date         BIGINT COMMENT '发送时间',
  collector         STRING COMMENT '操作员',
  create_user         STRING COMMENT '创建人',
  lp_corpno         STRING COMMENT '法人代码',
  tranus        STRING COMMENT '录入人',
  brchno        STRING COMMENT '录入机构',
  upbrchno        STRING COMMENT '最后修改机构',
  create_time         BIGINT COMMENT '创建时间',
  lst_upd_user        STRING COMMENT '最后修改人',
  lst_upd_time        BIGINT COMMENT '最后修改时间',
  oa_org_no             STRING COMMENT '委外机构编号',
  coll_collection       STRING COMMENT '催收组'
) COMMENT '消息作业表' 
PARTITIONED BY (d_date STRING) 
STORED AS PARQUET;


--付款承诺表

DROP TABLE  dwb.`dwb_coll_promise`;
CREATE TABLE if not exists dwb. `dwb_coll_promise` (
  promise_id          STRING COMMENT '承诺付款流水号',
  promise_amt         DECIMAL(15,2) COMMENT '承诺金额',
  promise_date        STRING COMMENT '承诺日期',
  shd_pmt_amt         DECIMAL(15,2) COMMENT '还款金额',
  collector           STRING COMMENT '催收员',
  promise_status      STRING COMMENT '承诺状态',
  case_sub_id         STRING COMMENT '子案件编号',
  create_time         BIGINT COMMENT '创建时间',
  loan_id             STRING COMMENT '借据号'
) COMMENT '付款承诺表' 
PARTITIONED BY (d_date STRING,p_type STRING) 
STORED AS PARQUET;


--代扣申请表

DROP TABLE  dwb.`dwb_early_repay_check`;
CREATE TABLE if not exists dwb. `dwb_early_repay_check` (
  legal_apply_num               STRING COMMENT '还款代扣申请流水',
  org                           STRING COMMENT '机构号',
  sub_case_main_id              STRING COMMENT '子案件编号',
  loan_id                       STRING COMMENT '借据编号',
  cust_name                     STRING COMMENT '客户姓名',
  id_type                       STRING COMMENT '证件类型',
  id_no                         STRING COMMENT '证件号码',
  total_amount_money            DECIMAL(18,2) COMMENT '代扣金额',
  real_debit_result             STRING COMMENT '代扣结果',
  payment_type                  STRING COMMENT '代扣类型',
  repay_channel                 STRING COMMENT '代扣申请渠道',
  payment_time                  STRING COMMENT '代扣时间',
  operator                      STRING COMMENT '操作人',
  operat_date                   BIGINT COMMENT '操作时间',
  manual_repay_term             DECIMAL(18,0) COMMENT '手工还款期次',
  manual_capital                DECIMAL(18,2) COMMENT '手工本金',
  manual_int                    DECIMAL(18,2) COMMENT '手工利息',
  manual_oint                   DECIMAL(18,2) COMMENT '手工罚息',
  manual_paycompound            DECIMAL(18,2) COMMENT '手工复利',
  manual_fee                    DECIMAL(18,2) COMMENT '手工费用',
  manual_fee_details            STRING COMMENT '还款明细',
  repayment_account_type        STRING COMMENT '还款账户类型',
  order_id                      STRING COMMENT '还款订单编号',
  active_repay_date             STRING COMMENT '还款时间',
  coll_rec_no                   INT COMMENT '催记流水号',
  trans_code                    STRING COMMENT '交易类型',
  trans_serial_no               STRING COMMENT '交易流水号',
  due_bill_id                   STRING COMMENT '借据号',
  total_amt                     DECIMAL(18,2) COMMENT '需冻结金额',
  repay_principal               DECIMAL(17,2) COMMENT '归还本金',
  repay_interest                DECIMAL(17,2) COMMENT '归还利息',
  repay_penalty_amount          DECIMAL(17,2) COMMENT '归还逾期罚息',
  repay_fee                     DECIMAL(17,2) COMMENT '归还费用',
  failure_message               STRING COMMENT '失败原因',
  actual_repay_amount           DECIMAL(18,2) COMMENT '还款金额',
  apply_datetime                BIGINT COMMENT '代扣申请时间'
) COMMENT '代扣申请表' 
PARTITIONED BY (d_date STRING,p_type STRING) 
STORED AS PARQUET;

--减免申请表

DROP TABLE  dwb.`dwb_derate_apply`;
CREATE TABLE if not exists dwb. `dwb_derate_apply` (
  id                      STRING COMMENT 'ID',
  der_apply_num           STRING COMMENT '减免申请流水号',
  org                     STRING COMMENT '机构号',
  cust_name               STRING COMMENT '客户姓名',
  loan_id                 STRING COMMENT '借据号',
  apply_date              STRING COMMENT '减免申请日期',
  der_reason              STRING COMMENT '减免原因',
  reduction_amt           DECIMAL(15,2) COMMENT '减免总额',
  reduction_fee           DECIMAL(15,2) COMMENT '减免费用',
  reduction_penalty       DECIMAL(15,2) COMMENT '减免罚息',
  reduction_int           DECIMAL(15,2) COMMENT '减免利息',
  reduction_principal     DECIMAL(15,2) COMMENT '减免本金',
  derate_trans_date       STRING COMMENT '减免交易时间',
  derate_result           STRING COMMENT '减免结果',
  reduction_type          STRING COMMENT '减免方式',
  case_sub_id             STRING COMMENT '子案件编号',
  check_status            STRING COMMENT '审批状态',
  txn_status              STRING COMMENT '扣款结果',
  if_payment              STRING COMMENT '是否扣款',
  actual_repay_amount     DECIMAL(15,2) COMMENT '还款金额',
  repay_principal         DECIMAL(15,2) COMMENT '归还本金',
  repay_interest          DECIMAL(15,2) COMMENT '归还利息',
  repay_penalty_amount    DECIMAL(15,2) COMMENT '归还逾期罚息',
  repay_fee               DECIMAL(15,2) COMMENT '还款费用',
  failure_message         STRING COMMENT '失败原因',
  cust_no                 STRING COMMENT '客户号',
  txn_seq                 STRING COMMENT '交易流水号',
  apply_datetime          BIGINT COMMENT '减免申请时间'
) COMMENT '减免申请表' 
PARTITIONED BY (d_date STRING,p_type STRING) 
STORED AS PARQUET;



drop table `dwb.dwb_customer_info`;
create table if not exists `dwb.dwb_customer_info`(
    `ecif`                     string comment '客户ECIF ID',
	`cutomer_type`              string  comment '客戶类型',
    `name`                     string comment '姓名',
    `idCard`                    string comment '身份证号',
    `age`                       int comment '年龄',
    `sex`                       string comment '性别',
    `credential_due_date`         string comment '证件到期日期',
    `face_detect_url`             string comment '人脸识别素材地址',
    `cell_phone`                     string comment '手机号',
    `init_phone`                 string comment '初始手机号',
    `marry_status`                  string comment '婚姻状态',
    `education`     string comment '学历',
    `init_education` string comment '初始学历',
    `private_owners`             string comment '是否私营业主',
    `position`                 string comment '社会身份（职位）',
    `industry_type`              string comment '行业类别',
    `department`                string comment '任职部门',
    `duties`                    string comment '职务',
    `employ_year`              int comment '工作年限',
    `month_income`               decimal(15,2) comment '月收入',
    `year_income`                decimal(15,2) comment '年收入',
    `family_month_income`         decimal(15,2) comment '家庭月收入',
    `family_nums`                int comment '家庭人数',
    `idcard_location`              string comment '身份证所在地',
	`idcard_provice`   		string comment '身份证对应省',
	`idcard_city`   		string comment '身份证对应市',
	`idcard_area`  		string comment '身份证对应区',
    `apply_loaction`              string comment 'apply_loaction',
    `mobile_home`             string comment '手机所在地',
    `live_addr`                  string comment '居住地址',
    `init_live_addr`              string comment '初始居住地址',
    `live_status`                string comment '居住状态',
    `live_prov`             string comment '居住地址-省',
    `init_live_prov`          string comment '初始居住地址-省',
    `live_city`                  string comment '居住地址-市',
    `init_live_city`              string comment '初始居住地址-市',
    `live_area`                  string comment '居住地址-区',
    `live_road`                  string comment '居住地址-街道',
    `live_longitude`             string comment '居住地址-经度',
    `live_latitude`              string comment '居住地址-纬度',
    `company_name`               string comment '工作单位名称',
    `company_phone`              string comment '工作单位电话',
    `company_addr`               string comment '工作单位地址',
    `init_company_addr`           string comment '初始工作单位地址',
    `company_prov`           string comment '工作单位地址-省',
    `init_company_prov`        string comment '初始工作单位地址-省',
    `company_Ctiy`               string comment '工作单位地址-市',
    `init_company_city`           string comment '初始工作单位地址-市',
    `company_area`               string comment '工作单位地址-区',
    `company_road`               string comment '工作单位地址-街道',
    `company_longitude`          string comment '工作单位地址-经度',
     `marks`          string comment '标志滴滴(DD)汇通(HT)盒子(HZ)',
     `org`   string comment '租户号',
 `id_type` string comment '证件类型',
   `product_code` string comment '产品编号',
 `channel_id` string comment '渠道号'
        )comment 'ods客户信息表'
partitioned by (d_date string,p_type string)
stored as parquet;



--DWB滴滴日志详情表

drop table dwb.dwb_dd_log_detail;
create table if not exists  dwb.dwb_dd_log_detail(
      ecif    string    comment  'ECIF',
      name	    string    comment  '用户姓名',
      cardNo    string    comment  '身份证ID',
      idType    string    comment  '证件类型',
      phone    string    comment  '银行卡绑定手机号',
      telephone    string    comment  '滴滴登陆手机号',
      bankCardNo	    string    comment  '银行卡号',
      userRole    string    comment  '职业',
      idCardValidDate		    string    comment  '有效期结束日期',
      address	    string    comment  '地址',
      ocrInfo	    string    comment  'ocr解析结果',
      idCardBackInfo	    string    comment  '',
      imageType    int    comment  '图片类型',
      imageStatus    string    comment  '',
      imageUrl     string    comment  'gift 地址',
      livingImageInfo     string    comment  '',
      sftpDir	    string    comment  '',
      sftp    string    comment  '',
      amount    decimal(15,6)    comment  '授信额度',
      interestRate    decimal(15,6)    comment  '利率',
      interestPenaltyRate    decimal(15,6)    comment  '罚息利率',
      startDate    string    comment  '授信有效起始日期',
      endDate    string    comment  '授信有效截止日期',
      lockDownEndTime    string    comment  '禁闭期截止日期',
      didiRcFeatureJson    string    comment  'didi 数据',
      flowNo    string    comment  '',
      signType    string    comment  '',
      applicationId    string    comment  '申请唯一标示',
      creditResultStatus    string    comment  '授信状态',
      applySource    string    comment  '',
      deal_date    string    comment  '处理时间',
      create_time    string    comment  '创建时间',
      update_time    string    comment  '更新时间',
      linkman_info    string    comment  '联系人信息',
      org    string    comment  '租户org' 
)COMMENT 'DWB滴滴日志详情表'
PARTITIONED BY (d_date string)
STORED AS PARQUET;

--DWB凤金日志详情表

drop table dwb.dwb_fj_log_detail;
create table if not exists dwb.dwb_fj_log_detail(
     org     string    comment  '',
     id    string    comment  '自增主键',
     project_id    string    comment  '项目编号',
     agency_id    string    comment  '机构号',
     partition_key    string    comment  '数据库里的分区时间',
     create_time    string    comment  '创建时间',
     update_time     string    comment  '更新时间',
     ecif    string    comment  'ecif 编号',
     product_no    string    comment  '产品编号',
     request_no    string    comment  '进件流水号',
     loan_apply_use    string    comment  '申请贷款用途',
     loan_rate_type    string    comment  '利率类型',
     currency_type    string    comment  '申请币种',
     contract_no    string    comment  '合同编号',
     contract_amount    decimal(15,2)    comment  '贷款总金额',
     company_loan_bool    string    comment  '是否是企业贷款',
     repay_type    string    comment  '还款方式',
     repay_frequency    string    comment  '还款频率',
     terms    int    comment  '还款期数',
     deduction_date    int    comment  '扣款日',
     loan_rate    decimal(15,6)    comment  '贷款年利率',
     year_rate_base    decimal(15,6)    comment  '年利率基础',
     loan_date    string    comment  '贷款开始日',
     loan_end_date    string    comment  '贷款结束日',
     open_id            string    comment  'Borrower用户唯一标识',
     name       string    comment  '姓名',
     id_type    string    comment  '证件类型',
     id_no    string    comment  '证件号码',
     mobile_phone       string    comment  '手机号码',
     sex        string    comment  '性别',
     age        int    comment  '年龄',
     province       string    comment  '居住地址（省）',
     city       string    comment  '居住地址(市)',
     area       string    comment  '居住地址（区/县）',
     address        string    comment  '详细地址',
     marital_status    string    comment  '婚姻状态',
     education      string    comment  '学历',
     industry    string    comment  '借款人行业',
     annual_income      decimal(15,2)    comment  '年收入',
     have_house    string    comment  '是否有房',
     housing_area       string    comment  '住房面积',
     housing_value      string    comment  '住房价值',
     family_worth       string    comment  '家庭净资产',
     front_url      string    comment  '身份证正面照片地址',
     back_url       string    comment  '身份证反面照片地址',
     private_owners     string    comment  '是否私营业主',
     income_m1       decimal(15,2)    comment  '盒伙人1个月累计佣金',
     income_m3       decimal(15,2)    comment  '盒伙人3个月累计佣金',
     income_m6       decimal(15,2)    comment  '盒伙人6个月累计佣金',
     income_m12      decimal(15,2)    comment  '盒伙人12个月累计佣金',
     social_credit_code     string    comment  '统一社会信用代码',
     company_name    string    comment  '企业名称',
     company_industry       string    comment  '企业行业',
     company_province       string    comment  '注册省份',
     company_city       string    comment  '注册城市',
     company_address        string    comment  '注册地址',
     company_legal_person_name      string    comment  '法人代表名字',
     legal_person_id_type       string    comment  '法人证件类型',
     legal_person_id_no     string    comment  '法人证件号码',
     company_legal_person_phone     string    comment  '法人手机',
     company_phone      string    comment  '企业联系电话',
     company_operate_years      int    comment  '经营年限',
     relational_humans    string    comment  '关联人信息json',
     guaranties    string    comment  '抵押物列表json',
     carJson    string    comment  '',
     houseJson    string    comment  '房子信息json',
     humanjson    string    comment  '',
     apply_request_no     string    comment  '外部交易流水号',
     acct_setup_ind     string    comment  '是否开户成功',
     loan_accountJson    string    comment  '放款账户信息',
     repayment_accountJson    string    comment  '还款账户信息'
)COMMENT 'DWB凤金日志详情表'
PARTITIONED BY (d_date string)
STORED AS PARQUET;

--DWB汇通报文解析详情表

drop table dwb.dwb_ht_log_detail;
create table if not exists  dwb.dwb_ht_log_detail(
     id    string comment  '接口响应日志ID',
     req_log_id    string comment  '请求日志ID',
     org    string comment  '机构号',
     deal_date    string comment  '请求处理时间',
     create_time    bigint comment  '创建时间',
     update_time    bigint comment  '修改时间',
     ecif    string comment  'ecif',
     pre_apply_no    string comment  '预申请编号',
     apply_no    string comment  '申请件编号',
     company_loan_bool    string comment  '是否是企业贷款',
     product_no    string comment  '产品编号',
     currency_type    string comment  '申请币种',
     currency_amt    decimal(15,2) comment  '申请金额',
     loan_amt     decimal(15,2) comment  '贷款总金额',
     loan_terms      int comment  '贷款期数',
     repay_type    string comment  '还款方式',
     loan_rate_type    string comment  '利率类型',
     agreement_rate_ind    string comment  '是否使用协议费/利率',
     loan_rate        decimal(15,6) comment  '贷款年利率',
     loan_fee_rate    decimal(15,6) comment  '贷款手续费比例',
     loan_svc_fee_rate    decimal(15,6) comment  '贷款服务费比例',
     loan_penalty_rate    decimal(15,6) comment  '贷款罚息年利率',
     guarantee_type    string comment  '担保方式',
     loan_apply_use       string comment  '申请贷款用途',
     open_id    string comment  'Borrower用户唯一标识',
     name     string comment  'Borrower借款人姓名',
     id_type      string comment  '证件类型',
     id_no        string comment  'Borrower借款人证件号码',
     mobile_phone    string comment  'Borrower',
     sex      string comment  'Borrower性别',
     age     int comment  'Borrower年龄',
     province    string comment  '居住地址(省）',
     city     string comment  'Borrower居住地址(市)',
     area     string comment  'Borrower居住地址（区/县',
     address      string comment  'Borrower详细地址',
     marital_status       string comment  'Borrower婚姻状态',
     education        string comment  'Borrower学历',
     industry     string comment  'Borrower借款人行业',
     annual_income_min        string comment  'Borrower年收入区间下限',
     annual_income_max    string comment  'Borrower年收入区间上限',
     have_house       string comment  'Borrower是否有房',
     housing_area     string comment  'Borrower  住房面积',
     housing_value        string comment  'Borrower 住房价值',
     drivr_licen_no       string comment  'Borrower 驾驶证号',
     driving_expr    string comment  'Borrower驾龄',
     company_social_credit_code       string comment  '统一社会信用代码',
     company_name     string comment  'Borrower 企业名称',
     company_industry     string comment  ' Borrower 企业行业',
     company_province     string comment  'Borrower 注册省份',
     company_city     string comment  'Borrower 注册城市',
     company_address      string comment  'Borrower 注册地址',
     legal_person_name    string comment  '法人代表名字',
     legal_person_id_type     string comment  '法人证件类型',
     legal_person_id_no       string comment  '法人证件号码',
     legal_person_phone    string comment  '法人手机',
     company_phone    string comment  '企业联系电话',
     company_operate_years        string comment  '经营年限',
     relational_humans    string comment  '关联人关系',
     guaranties    string comment  '抵押物列表json',
     car    string comment  'car',
     account_type    string comment  '账户类型',
     account_num    string comment  '账户号码（银行卡号）',
     account_name        string comment  '账户户名',
     bank_name       string comment  '开户银行',
     branch_name     string comment  '开户支行名称',
     branch_mobile_phone     string comment  '银行卡在银行预留的手机号码',
     acct_setup_ind    string comment  '是否开户成功',
     reject_msg    string comment  '结果信息',
     cust_no    string comment  '内部客户号',
     code    string comment  '请求失败的状态码',
     channel_id    string comment  '',
     status    string comment  '请求成功或者失败'
)COMMENT 'DWB汇通报文解析详情表'
PARTITIONED BY (d_date string)
STORED AS PARQUET;

--DWB乐信报文解析详情表

drop table dwb.dwb_lx_log_detail;
create table if not exists  dwb.dwb_lx_log_detail(
    authNo    string comment  '授权协议编号',
    applyNo    string comment  '申请编号',
    proCode     string comment  '产品编号',
    proName     string comment  '产品名称', 
    dbBankCode     string comment  '银行代码',
    dbBankName    string comment  '银行总行名',
    dbOpenBankName     string comment  '开户行银行名',
    dbBankAccount    string comment  '放款账户卡号',
    dbAccountName     string comment  '放款账户名',
    custName    string comment  '客户名称',
    customerId    string comment  '客户号',
    creditLimit     decimal(15,2) comment  '客户授信额度',
    availableCreditLimit     decimal(15,2) comment  '客户当前可用额度',
    idType    string comment  '证件类型',  
    idNo    string comment  '证件号码',
    idPreDate     string comment  '证件起始日期',
    idEndDate     string comment  '证件截止日期',
    sex    string comment  '性别',
    birth     string comment  '',
    age     int comment  '年龄',
    country    string comment  '国籍',
    edu     string comment  '最高学历',
    degree     string comment  '最高学位',
    phoneNo    string comment  '手机号码',
    telNo    string comment  '联系电话',
    postCode     string comment  '通讯邮编',
    postAddr      string comment  '通讯地址',
    homeArea     string comment  '户籍归属地',
    homeCode    string comment  '住宅邮编',
    homeAddr     string comment  '住宅地址',
    homeTel    string comment  '住宅电话',
    homeSts     string comment  '居住状况',
    marriage    string comment  '婚姻状况',
    mateName     string comment  'mateName',
    mateIdtype     string comment  '配偶证件类型',
    mateIdno     string comment  '配偶证件号码',
    mateWork    string comment  '配偶工作单位',
    mateTel    string comment  '配偶联系电话',
    children     string comment  '是否有子女',
    mincome    string comment  '个人月收入（元）',
    income     string comment  '个人年收入', 
    homeIncome     string comment  '家庭年收入（反洗钱）',
    zxhomeIncome     string comment  '家庭年收入（征信）',
    custType    string comment  '客户类型', 
    workSts    string comment  '工作状态', 
    workName    string comment  '工作单位名称',
    workWay    string comment  '工作单位所属行业',
    workCode    string comment  '工作单位邮编',
    workAddr    string comment  '工作单位地址',
    workType    string comment  '职业',
    workDuty    string comment  '职务',
    workTitle    string comment  '职称',
    workYear    string comment  '四位年份',
    pactAmt    decimal(15,2) comment  '贷款金额', 
    lnRate     decimal(15,6) comment  '利率（月）',
    loanTerm     int comment  '贷款期数', 
    appArea    string comment  '《行政区划》默认',
    appUse    string comment  '申请用途',
    vouType    string comment  '担保方式',
    loanDate    string comment  '申请日期', 
    endDate     string comment  '到期日期',
    rpyMethod     string comment  '还款方式',
    payType    string comment  '',
    payDay     string comment  '扣款日期',
    riskLevel    string comment  '风险等级',
    loanTime    string comment  '放款订单时间',
    trade    string comment  '投向行业（反洗钱）',
    ifCar    string comment  '',
    ifCarCred    string comment  '是否有按揭车贷',
    ifRoom    string comment  '是否有房',
    ifMort    string comment  '是否有按揭房贷',
    ifCard    string comment  '是否有贷记卡',
    cardAmt    decimal(15,2) comment  '贷记卡最低额度',
    ifApp    string comment  '是否填写申请表', 
    ifId    string comment  '是否有身份证信息', 
    ifPact     string comment  '是否已签订借款合同',
    czPactNo     string comment  '查证流水号', 
    ifLaunder    string comment  '是否具有洗钱风险',
    launder    string comment  '反洗钱风险关联度', 
    ifAgent    string comment  '是否有代理人',
    profession    string comment  '职业（反洗钱）',
    isBelowRisk     string comment  '借款人风险等级是否在贷款服务机构指定等级及以下',
    hasOverdueLoan    string comment  '借款人是否存在其他未结清的逾期贷款',
    sales     string comment  '销售渠道',
    resultCode    string comment  '请求状态码',  
    resultMsg     string comment  '请求结果信息',
    pass     string comment  '',
    deal_date     string comment  '处理时间', 
    partner    string comment  '',
    ecif_id    string comment  'ecif_id',  
    org    string comment  '',
    update_time     bigint comment  '修改时间',
    create_time    bigint comment  '',
	req_timestamp  bigint comment  '请求报文里的时间'
)COMMENT 'DWB乐信日志详情表'
PARTITIONED BY (d_date string)
STORED AS PARQUET;




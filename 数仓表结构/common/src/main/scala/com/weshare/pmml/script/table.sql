
create database if not exists eagle comment '风控';

drop table eagle.one_million_random_risk_data;
CREATE  TABLE eagle.one_million_random_risk_data(
  `due_bill_no`                                                       string,
  `product_id`                                                        string,
  `risk_level`                                                        string,
  `loan_init_principal`                                               decimal(15,4),
  `loan_init_term`                                                    decimal(15,4),
  `loan_init_interest_rate`                                           decimal(15,4),
  `credit_coef`                                                       decimal(15,4),
  `loan_init_penalty_rate`                                            decimal(15,4),
  `loan_active_date`                                                  string,
  `settled`                                                           string,
  `paid_out_date`                                                     string,
  `loan_out_reason`                                                   string,
  `paid_out_type`                                                     string,
  `schedule`                                                          string,
  `loan_status`                                                       string,
  `paid_amount`                                                       decimal(15,4),
  `paid_principal`                                                    decimal(15,4),
  `paid_interest`                                                     decimal(15,4),
  `paid_penalty`                                                      decimal(15,4),
  `paid_svc_fee`                                                      decimal(15,4),
  `paid_term_fee`                                                     decimal(15,4),
  `paid_mult`                                                         decimal(15,4),
  `remain_amount`                                                     decimal(15,4),
  `remain_principal`                                                  decimal(15,4),
  `remain_interest`                                                   decimal(15,4),
  `remain_svc_fee`                                                    decimal(15,4),
  `remain_term_fee`                                                   decimal(15,4),
  `overdue_principal`                                                 decimal(15,4),
  `overdue_interest`                                                  decimal(15,4),
  `overdue_svc_fee`                                                   decimal(15,4),
  `overdue_term_fee`                                                  decimal(15,4),
  `overdue_penalty`                                                   decimal(15,4),
  `overdue_mult_amt`                                                  decimal(15,4),
  `overdue_date_first`                                                string,
  `overdue_date_start`                                                string,
  `overdue_days`                                                      decimal(5,0),
  `overdue_date`                                                      string,
  `dpd_begin_date`                                                    string,
  `dpd_days`                                                          decimal(4,0),
  `dpd_days_count`                                                    decimal(4,0),
  `dpd_days_max`                                                      decimal(4,0),
  `collect_out_date`                                                  string,
  `overdue_term`                                                      decimal(3,0),
  `overdue_terms_count`                                               decimal(3,0),
  `overdue_terms_max`                                                 decimal(3,0),
  `overdue_principal_accumulate`                                      decimal(15,4),
  `overdue_principal_max`                                             decimal(15,4),
  `creditlimit`                                                       decimal(10,2),
  `edu`                                                               string,
  `degree`                                                            string,
  `homests`                                                           string,
  `marriage`                                                          string,
  `mincome`                                                           decimal(10,2),
  `homeincome`                                                        string,
  `zxhomeincome`                                                      decimal(10,2),
  `custtype`                                                          string,
  `worktype`                                                          string,
  `workduty`                                                          string,
  `worktitle`                                                         string,
  `idcard_area`                                                       string,
  `risklevel`                                                         string,
  `scorerange`                                                        string,
  `rn`                                                                int,
    biz_date STRING comment '数据抽取时间'
  )comment '风控模型输入数据表'
  partitioned by (project_id STRING comment  '项目Id',cycle_key string comment '循环次数')
STORED AS PARQUET;

CREATE  TABLE eagle.one_million_random_risk_data_txt(
  `due_bill_no`                                                       string,
  `product_id`                                                        string,
  `risk_level`                                                        string,
  `loan_init_principal`                                               decimal(15,4),
  `loan_init_term`                                                    decimal(15,4),
  `loan_init_interest_rate`                                           decimal(15,4),
  `credit_coef`                                                       decimal(15,4),
  `loan_init_penalty_rate`                                            decimal(15,4),
  `loan_active_date`                                                  string,
  `settled`                                                           string,
  `paid_out_date`                                                     string,
  `loan_out_reason`                                                   string,
  `paid_out_type`                                                     string,
  `schedule`                                                          string,
  `loan_status`                                                       string,
  `paid_amount`                                                       decimal(15,4),
  `paid_principal`                                                    decimal(15,4),
  `paid_interest`                                                     decimal(15,4),
  `paid_penalty`                                                      decimal(15,4),
  `paid_svc_fee`                                                      decimal(15,4),
  `paid_term_fee`                                                     decimal(15,4),
  `paid_mult`                                                         decimal(15,4),
  `remain_amount`                                                     decimal(15,4),
  `remain_principal`                                                  decimal(15,4),
  `remain_interest`                                                   decimal(15,4),
  `remain_svc_fee`                                                    decimal(15,4),
  `remain_term_fee`                                                   decimal(15,4),
  `overdue_principal`                                                 decimal(15,4),
  `overdue_interest`                                                  decimal(15,4),
  `overdue_svc_fee`                                                   decimal(15,4),
  `overdue_term_fee`                                                  decimal(15,4),
  `overdue_penalty`                                                   decimal(15,4),
  `overdue_mult_amt`                                                  decimal(15,4),
  `overdue_date_first`                                                string,
  `overdue_date_start`                                                string,
  `overdue_days`                                                      decimal(5,0),
  `overdue_date`                                                      string,
  `dpd_begin_date`                                                    string,
  `dpd_days`                                                          decimal(4,0),
  `dpd_days_count`                                                    decimal(4,0),
  `dpd_days_max`                                                      decimal(4,0),
  `collect_out_date`                                                  string,
  `overdue_term`                                                      decimal(3,0),
  `overdue_terms_count`                                               decimal(3,0),
  `overdue_terms_max`                                                 decimal(3,0),
  `overdue_principal_accumulate`                                      decimal(15,4),
  `overdue_principal_max`                                             decimal(15,4),
  `creditlimit`                                                       decimal(10,2),
  `edu`                                                               string,
  `degree`                                                            string,
  `homests`                                                           string,
  `marriage`                                                          string,
  `mincome`                                                           decimal(10,2),
  `homeincome`                                                        string,
  `zxhomeincome`                                                      decimal(10,2),
  `custtype`                                                          string,
  `worktype`                                                          string,
  `workduty`                                                          string,
  `worktitle`                                                         string,
  `idcard_area`                                                       string,
  `risklevel`                                                         string,
  `scorerange`                                                        string,
  `rn`                                                                int,
  biz_date string,
  project_id string,
  cycle_key string
  )comment '风控模型输入数据表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS textfile ;





load data local inpath '/root/guochao/test/new20000'  OVERWRITE INTO  TABLE eagle.one_million_random_risk_data PARTITION (cycle_key="0");;



drop table eagle.predict_schedule;
create   table eagle.predict_schedule(
due_bill_no                                                               string,
schedule_id                                                               string,
loan_active_date                                                          string,
loan_init_principal                                                       decimal(15,4),
loan_init_term                                                            decimal(15,4),
loan_term                                                                 decimal(15,4),
start_interest_date                                                       string,
curr_bal                                                                  string,
should_repay_date                                                         string,
should_repay_date_history                                                 string,
grace_date                                                                string,
should_repay_amount                                                       decimal(15,4),
should_repay_principal                                                    decimal(15,4),
should_repay_interest                                                     decimal(15,4),
should_repay_term_fee                                                     decimal(15,4),
should_repay_svc_fee                                                      decimal(15,4),
should_repay_penalty                                                      decimal(15,4),
should_repay_mult_amt                                                     decimal(15,4),
should_repay_penalty_acru                                                 decimal(15,4),
schedule_status                                                           string,
schedule_status_cn                                                        string,
paid_out_date                                                             string,
paid_out_type                                                             string,
paid_out_type_cn                                                          string,
paid_amount                                                               decimal(15,4),
paid_principal                                                            decimal(15,4),
paid_interest                                                             decimal(15,4),
paid_term_fee                                                             decimal(15,4),
paid_svc_fee                                                              decimal(15,4),
paid_penalty                                                              decimal(15,4),
paid_mult                                                                 decimal(15,4),
reduce_amount                                                             decimal(15,4),
reduce_principal                                                          decimal(15,4),
reduce_interest                                                           decimal(15,4),
reduce_term_fee                                                           decimal(15,4),
reduce_svc_fee                                                            decimal(15,4),
reduce_penalty                                                            decimal(15,4),
reduce_mult_amt                                                           decimal(15,4),
range_rate                                                                string,
product_id                                                                string,
biz_date string comment '数据抽取时间'
)comment '模型预测完还款计划'
partitioned by (project_id string comment '项目id',cycle_key string comment '循环次数')
STORED AS PARQUET;

drop table eagle.predict_repay_day;
create  table eagle.predict_repay_day(
should_repay_date                                                          string        comment '应还日',
paid_out_date                                                              string        comment '实还日',
should_repay_principal                                                     decimal(15,4) comment '应还本金',
should_repay_interest                                                      decimal(15,4) comment '应还利息',
should_repay_amount                                                        decimal(15,4) comment '应还金额',
paid_principal                                                             decimal(15,4) comment '已还本金',
paid_interest                                                              decimal(15,4) comment '已还利息',
paid_amount                                                                decimal(15,4) comment '已还金额',
remain_principal                                                           decimal(15,4) comment '剩余本金'
)comment '统计结果信息表'
partitioned by (biz_date string comment '数据抽取时间' ,project_id string comment '项目id',cycle_key string comment '循环次数')
STORED AS PARQUET;





drop table eagle.predict_repay_day;
create  table eagle.predict_repay_day_text(
should_repay_date                                                          string        comment '应还日',
paid_out_date                                                              string        comment '实还日',
should_repay_principal                                                     decimal(15,4) comment '应还本金',
should_repay_interest                                                      decimal(15,4) comment '应还利息',
should_repay_amount                                                        decimal(15,4) comment '应还金额',
paid_principal                                                             decimal(15,4) comment '已还本金',
paid_interest                                                              decimal(15,4) comment '已还利息',
paid_amount                                                                decimal(15,4) comment '已还金额',
remain_principal                                                           decimal(15,4) comment '剩余本金',
biz_date                                      string ,
project_id string ,
cycle_key string
)comment '统计结果信息表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS textfile

;











 CREATE  TABLE `eagle.one_million_random_loan_data_day`(
   `due_bill_no` string COMMENT '借据编号',
   `risk_level` string COMMENT '风险等级',              
   `loan_init_principal` decimal(15,4) COMMENT '放款本金',  
   `loan_init_term` decimal(15,4) COMMENT '贷款期数',   
   `loan_init_interest_rate` decimal(15,4) COMMENT '利息利率（8d/%）',  
   `credit_coef` decimal(15,4) COMMENT '综合融资成本（8d/%）',  
   `loan_init_penalty_rate` decimal(15,4) COMMENT '罚息利率（8d/%）',  
   `loan_active_date` string COMMENT '放款日期',        
   `settled` string COMMENT '是否结清',                 
   `paid_out_date` string COMMENT '结清日期',           
   `loan_out_reason` string COMMENT '借据终止原因',       
   `paid_out_type` string COMMENT '结清类型',           
   `schedule` string COMMENT '还款计划-包含该条借据观察日所有还款计划',  
   `loan_status` string COMMENT '借据状态',             
   `paid_amount` decimal(15,4) COMMENT '已还金额',      
   `paid_principal` decimal(15,4) COMMENT '已还本金',   
   `paid_interest` decimal(15,4) COMMENT '已还利息',    
   `paid_penalty` decimal(15,4) COMMENT '已还罚息',     
   `paid_svc_fee` decimal(15,4) COMMENT '已还服务费',    
   `paid_term_fee` decimal(15,4) COMMENT '已还手续费',   
   `paid_mult` decimal(15,4) COMMENT '已还滞纳金',       
   `remain_amount` decimal(15,4) COMMENT '剩余金额：本息费',  
   `remain_principal` decimal(15,4) COMMENT '剩余本金',  
   `remain_interest` decimal(15,4) COMMENT '剩余利息',  
   `remain_svc_fee` decimal(15,4) COMMENT '剩余服务费',  
   `remain_term_fee` decimal(15,4) COMMENT '剩余手续费',  
   `overdue_principal` decimal(15,4) COMMENT '逾期本金',  
   `overdue_interest` decimal(15,4) COMMENT '逾期利息',  
   `overdue_svc_fee` decimal(15,4) COMMENT '逾期服务费',  
   `overdue_term_fee` decimal(15,4) COMMENT '逾期手续费',  
   `overdue_penalty` decimal(15,4) COMMENT '逾期罚息',  
   `overdue_mult_amt` decimal(15,4) COMMENT '逾期滞纳金',  
   `overdue_date_first` string COMMENT '首次逾期日期',    
   `overdue_date_start` string COMMENT '逾期起始日期',    
   `overdue_days` decimal(5,0) COMMENT '逾期天数',      
   `overdue_date` string COMMENT '逾期日期',            
   `dpd_begin_date` string COMMENT 'DPD起始日期',       
   `dpd_days` decimal(4,0) COMMENT 'DPD天数',         
   `dpd_days_count` decimal(4,0) COMMENT '累计DPD天数',  
   `dpd_days_max` decimal(4,0) COMMENT '历史最大DPD天数',  
   `collect_out_date` string COMMENT '出催日期',        
   `overdue_term` decimal(3,0) COMMENT '当前逾期期数',    
   `overdue_terms_count` decimal(3,0) COMMENT '累计逾期期数',  
   `overdue_terms_max` decimal(3,0) COMMENT '历史单次最长逾期期数',  
   `overdue_principal_accumulate` decimal(15,4) COMMENT '累计逾期本金',  
   `overdue_principal_max` decimal(15,4) COMMENT '历史最大逾期本金',
    `biz_date` string COMMENT '观察日')

 PARTITIONED BY (                                   
`product_id` string comment '产品Id')
  STORED AS PARQUET;





 CREATE  TABLE `eagle.one_million_random_customer_reqdata`(
   `due_bill_no` string COMMENT '借据编号',
   `creditlimit` decimal(10,2) COMMENT '客户授信额度',
   `creditcoef` decimal(10,2) COMMENT '额度调整系数',
   `availablecreditlimit` decimal(10,2) COMMENT '客户当前可用额度',
   `edu` string COMMENT '最高学历',
   `degree` string COMMENT '最高学位',
   `homests` string COMMENT '居住状况',
   `marriage` string COMMENT '婚姻状况',
   `mincome` decimal(10,2) COMMENT '个人月收入（元）',
   `income` string COMMENT '个人年收入',
   `homeincome` string COMMENT '家庭年收入（反洗钱）',
   `zxhomeincome` decimal(10,2) COMMENT '家庭年收入（征信）',
   `custtype` string COMMENT '客户类型',
   `workway` string COMMENT '工作单位所属行业',
   `worktype` string COMMENT '职业',
   `workduty` string COMMENT '职务',
   `worktitle` string COMMENT '职称',
   `appuse` string COMMENT '申请用途',
   `ifcar` string COMMENT '是否有车',
   `ifcarcred` string COMMENT '是否有按揭车贷',
   `ifroom` string COMMENT '是否有房',
   `ifmort` string COMMENT '是否有按揭房贷',
   `ifcard` string COMMENT '是否有贷记卡',
   `cardamt` decimal(10,2) COMMENT '贷记卡最低额度',
   `ifapp` string COMMENT '是否填写申请表',
   `ifid` string COMMENT '是否有身份证信息',
   `ifpact` string COMMENT '是否已签订借款合同',
   `iflaunder` string COMMENT '是否具有洗钱风险',
   `launder` string COMMENT '反洗钱风险关联度',
   `ifagent` string COMMENT '是否有代理人',
   `isbelowrisk` string COMMENT '借款人风险等级是否在贷款服务机构指定等级及以下',
   `hasoverdueloan` string COMMENT '借款人是否存在其他未结清的逾期贷款',
   `idcard_area` string COMMENT '身份证大区',
   `resident_area` string COMMENT '常住地大区',
   `risklevel` string COMMENT '风险等级')
 COMMENT '乐信-中铁一百万条随机借据对应客户申请信息'
  PARTITIONED BY ( `product_id` string comment '产品Id')
 STORED AS PARQUET;


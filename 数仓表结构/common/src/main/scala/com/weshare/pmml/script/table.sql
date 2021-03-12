
drop table eagle.one_million_random_risk_data;
CREATE TABLE eagle.one_million_random_risk_data(
  `due_bill_no` string,
  `product_id` string,
  `risk_level` string,
  `loan_init_principal` decimal(15,4),
  `loan_init_term` decimal(15,4),
  `loan_init_interest_rate` decimal(15,4),
  `credit_coef` decimal(15,4),
  `loan_init_penalty_rate` decimal(15,4),
  `loan_active_date` string,
  `settled` string,
  `paid_out_date` string,
  `loan_out_reason` string,
  `paid_out_type` string,
  `schedule` string,
  `loan_status` string,
  `paid_amount` decimal(15,4),
  `paid_principal` decimal(15,4),
  `paid_interest` decimal(15,4),
  `paid_penalty` decimal(15,4),
  `paid_svc_fee` decimal(15,4),
  `paid_term_fee` decimal(15,4),
  `paid_mult` decimal(15,4),
  `remain_amount` decimal(15,4),
  `remain_principal` decimal(15,4),
  `remain_interest` decimal(15,4),
  `remain_svc_fee` decimal(15,4),
  `remain_term_fee` decimal(15,4),
  `overdue_principal` decimal(15,4),
  `overdue_interest` decimal(15,4),
  `overdue_svc_fee` decimal(15,4),
  `overdue_term_fee` decimal(15,4),
  `overdue_penalty` decimal(15,4),
  `overdue_mult_amt` decimal(15,4),
  `overdue_date_first` string,
  `overdue_date_start` string,
  `overdue_days` decimal(5,0),
  `overdue_date` string,
  `dpd_begin_date` string,
  `dpd_days` decimal(4,0),
  `dpd_days_count` decimal(4,0),
  `dpd_days_max` decimal(4,0),
  `collect_out_date` string,
  `overdue_term` decimal(3,0),
  `overdue_terms_count` decimal(3,0),
  `overdue_terms_max` decimal(3,0),
  `overdue_principal_accumulate` decimal(15,4),
  `overdue_principal_max` decimal(15,4),
  `biz_date` string,
  `creditlimit` decimal(10,2),
  `edu` string,
  `degree` string,
  `homests` string,
  `marriage` string,
  `mincome` decimal(10,2),
  `homeincome` string,
  `zxhomeincome` decimal(10,2),
  `custtype` string,
  `worktype` string,
  `workduty` string,
  `worktitle` string,
  `idcard_area` string,
  `risklevel` string,
  `scorerange` string)
  partitioned by (cycle_key string)
STORED AS PARQUET;







load data local inpath '/root/guochao/test/new20000'  OVERWRITE INTO  TABLE eagle.one_million_random_risk_data PARTITION (cycle_key="0");;




drop table eagle.predict_schedule;
create table eagle.predict_schedule(
due_bill_no  string,
schedule_id  string,
loan_active_date  string,
loan_init_principal  decimal(15,4),
loan_init_term  decimal(15,4),
loan_term  decimal(15,4),
start_interest_date  string,
curr_bal  string,
should_repay_date  string,
should_repay_date_history  string,
grace_date  string,
should_repay_amount  decimal(15,4),
should_repay_principal  decimal(15,4),
should_repay_interest  decimal(15,4),
should_repay_term_fee  decimal(15,4),
should_repay_svc_fee  decimal(15,4),
should_repay_penalty  decimal(15,4),
should_repay_mult_amt  decimal(15,4),
should_repay_penalty_acru  decimal(15,4),
schedule_status  string,
schedule_status_cn  string,
paid_out_date  string,
paid_out_type  string,
paid_out_type_cn  string,
paid_amount  decimal(15,4),
paid_principal  decimal(15,4),
paid_interest  decimal(15,4),
paid_term_fee  decimal(15,4),
paid_svc_fee  decimal(15,4),
paid_penalty  decimal(15,4),
paid_mult  decimal(15,4),
reduce_amount  decimal(15,4),
reduce_principal  decimal(15,4),
reduce_interest  decimal(15,4),
reduce_term_fee  decimal(15,4),
reduce_svc_fee  decimal(15,4),
reduce_penalty  decimal(15,4),
reduce_mult_amt  decimal(15,4),
s_d_date  string,
e_d_date  string,
effective_time  string,
expire_time  string
)partitioned by (biz_date string ,product_id string,cycle_key string)
STORED AS PARQUET;

drop table eagle.predict_repay_day;
create table eagle.predict_repay_day(
should_repay_date string,
paid_out_date string ,
should_repay_principal decimal(15,4),
should_repay_interest decimal(15,4),
should_repay_amount decimal(15,4),
paid_principal decimal(15,4),
paid_interest decimal(15,4),
paid_amount decimal(15,4)
)partitioned by (biz_date string ,project_id string,cycle_key string)
STORED AS PARQUET;

insert overwrite table eagle.predict_repay_day partition(biz_Date,project_id,cycle_key)
select
should_repay_date,
paid_out_date ,
sum(should_repay_principal) as should_repay_principal,
sum(should_repay_interest) as should_repay_interest,
sum(should_repay_amount) as should_repay_amount,
sum(paid_principal) as paid_principal,
sum(paid_interest) as paid_interest,
sum(paid_amount) as paid_amount,
"2020-11-30" as biz_Date,
biz_conf.project_id as project_id,
"0" as cycle_key
from
(
select *
from
eagle.predict_schedule
where cycle_key="0" and biz_Date="2020-11-30"
)predict
inner join dim_new.biz_conf on predict.product_id=biz_conf.product_id
group by biz_conf.project_id ,should_repay_date,paid_out_date


select

sum(should_repay_principal) as should_repay_principal,
sum(should_repay_interest) as should_repay_interest,
sum(should_repay_amount) as should_repay_amount,
sum(paid_principal) as paid_principal,
sum(paid_interest) as paid_interest,
sum(paid_amount) as paid_amount

from
eagle.predict_schedule
where cycle_key="1" and biz_Date="2020-11-30"








insert overwrite table eagle.one_million_random_risk_data partition(cycle_key="0")
select
due_bill_no                    ,
product_id                     ,
risk_level                     ,
loan_init_principal            ,
loan_init_term                 ,
loan_init_interest_rate        ,
credit_coef                    ,
loan_init_penalty_rate         ,
loan_active_date               ,
settled                        ,
paid_out_date                  ,
loan_out_reason                ,
paid_out_type                  ,
schedule                       ,
loan_status                    ,
paid_amount                    ,
paid_principal                 ,
paid_interest                  ,
paid_penalty                   ,
paid_svc_fee                   ,
paid_term_fee                  ,
paid_mult                      ,
remain_amount                  ,
remain_principal               ,
remain_interest                ,
remain_svc_fee                 ,
remain_term_fee                ,
overdue_principal              ,
overdue_interest               ,
overdue_svc_fee                ,
overdue_term_fee               ,
overdue_penalty                ,
overdue_mult_amt               ,
overdue_date_first             ,
overdue_date_start             ,
overdue_days                   ,
overdue_date                   ,
dpd_begin_date                 ,
dpd_days                       ,
dpd_days_count                 ,
dpd_days_max                   ,
collect_out_date               ,
overdue_term                   ,
overdue_terms_count            ,
overdue_terms_max              ,
overdue_principal_accumulate   ,
overdue_principal_max          ,
biz_date                       ,
creditlimit                    ,
edu                            ,
degree                         ,
homests                        ,
marriage                       ,
mincome                        ,
homeincome                     ,
zxhomeincome                   ,
custtype                       ,
worktype                       ,
workduty                       ,
worktitle                      ,
idcard_area                    ,
risklevel                      ,
scorerange
from
eagle.one_million_random_risk_data where cycle_key='0' and biz_Date='2020-11-30'
limit 10000;












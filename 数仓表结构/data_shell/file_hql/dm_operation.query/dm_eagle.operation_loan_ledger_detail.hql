set mapred.job.name=dm_eagle.operation_loan_ledger_detail;
set hive.execution.engine=mr;
set mapreduce.map.memory.mb=2048;
set mapreduce.reduce.memory.mb=2048;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=12;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.mapjoin.smalltable.filesize=50000000;
set hive.map.aggr=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=1024000000;
set hive.merge.smallfiles.avgsize=1024000000;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
--create table if not exists dm_eagle.operation_loan_ledger_detail(
--    channel_id                 string               COMMENT '渠道编号'
--   ,project_id                 string               COMMENT '项目编号'
--   ,contract_no                string               COMMENT '合同编号'
--   ,due_bill_no                string               COMMENT '借据编号'
--   ,customer_name              string               COMMENT '客户姓名'  --取值逻辑待定
--   ,loan_init_principal        decimal(15,4)        COMMENT '合同金额'   
--   ,loan_active_date           string               COMMENT '借款生效日'
--   ,debt_conversion_date       string               COMMENT '债转日期'  --取值逻辑待定
--   ,cycle_day                  decimal(2,0)         COMMENT '还款日'
--   ,loan_type                  string               COMMENT '还款方式'
--   ,loan_init_interest_rate    decimal(15,8)        COMMENT '利率'    
--   ,loan_init_penalty_rate     decimal(15,8)        COMMENT '罚息率'
--   ,loan_expire_date           string               COMMENT '借款到期日'
--   ,loan_status                string               COMMENT '借据状态'
--   ,paid_out_date              string               COMMENT '借款结清日'
--   ,loan_init_term             decimal(3,0)         COMMENT '借款期数'
--   ,loan_term_remain           decimal(3,0)         COMMENT '剩余期数'
--   ,overdue_days               decimal(5,0)         COMMENT '逾期天数'
--   ,remain_amount              decimal(15,4)        COMMENT '剩余未还金额'
--   ,remain_principal           decimal(15,4)        COMMENT '剩余未还本金'
--   ,remain_interest            decimal(15,4)        COMMENT '剩余未还利息'
--   ,remain_fee                 decimal(15,4)        COMMENT '剩余未还费用'  --取值逻辑待定
--   ,remain_penalty_interest    decimal(15,4)        COMMENT '剩余未还罚息'  --取值逻辑待定
--   ,overdue_amount             decimal(15,4)        COMMENT '逾期金额'
--   ,overdue_principal          decimal(15,4)        COMMENT '逾期本金'
--   ,overdue_interest           decimal(15,4)        COMMENT '逾期利息'
--   ,overdue_fee                decimal(15,4)        COMMENT '逾期费用'
--   ,overdue_penalty            decimal(15,4)        COMMENT '逾期罚息'
--   ,paid_amount                decimal(15,4)        COMMENT '已还金额'
--   ,paid_principal             decimal(15,4)        COMMENT '已还本金'
--   ,paid_interest              decimal(15,4)        COMMENT '已还利息'
--   ,paid_fee                   decimal(15,4)        COMMENT '已还费用'
--   ,paid_penalty               decimal(15,4)        COMMENT '已还罚息'
--   ,execution_date             string               COMMENT '跑批日期'
--) COMMENT '借据台账-明细' 
--PARTITIONED BY (product_id string COMMENT '产品编号')
--STORED AS PARQUET;
--due_bill_no和name对应的临时表
with due_bill_no_name as 
(
select
a.due_bill_no
,b.dim_decrypt
from(
select
distinct 
due_bill_no
,cust_id 
from 
ods_new_s.loan_apply) a
left join
(
select 
 cust_id,
 name,
 dim_decrypt
from (
select
distinct
cust_id,
name
from
ods_new_s.customer_info) c
left join
(select  
   dim_encrypt
  ,dim_decrypt  
  from dim_new.dim_encrypt_info 
  where dim_type = 'userName'
group by  
dim_encrypt
,dim_decrypt) d
on c.name = d.dim_encrypt
) b
on a.cust_id = b.cust_id
)
--set ST9='2020-10-01';
--借据台账-明细 （取最新的数据）
insert overwrite table dm_eagle.operation_loan_ledger_detail partition(product_id)
select
 t2.channel_id                                                                  --'渠道编号'
,t2.project_id                                                                  --'项目编号'
,t3.contract_no                                                                 --'合同编号'
,t1.due_bill_no                                                                 --'借据编号'
,t4.dim_decrypt as customer_name                                                --'客户姓名'  
,t1.loan_init_principal                                                         --'合同金额'
,t3.loan_active_date                                                            --'借款生效日'
,null as debt_conversion_date                                                   --'债转日期'  --取值逻辑待定
,t3.cycle_day                                                                   --'还款日'
,t3.loan_type                                                                   --'还款方式'
,t3.loan_init_interest_rate                                                     --'利率'    
,t3.loan_init_penalty_rate                                                      --'罚息率'
,cast(0 as decimal(15,8))  as loan_init_fee_rate                                --'费率'
,t3.loan_expire_date                                                            --'借款到期日'
,t1.loan_status                                                                 --'借据状态'
,t1.paid_out_date                                                               --'借款结清日'
,t1.loan_init_term                                                              --'借款期数'
,t1.loan_term_remain                                                            --'剩余期数'
,t1.overdue_days                                                                --'逾期天数'
,t1.remain_amount                                                               --'剩余未还金额'
,t1.remain_principal                                                            --'剩余未还本金'
,t1.remain_interest                                                             --'剩余未还利息'
,(nvl(t1.remain_svc_fee,0) + nvl(t1.remain_term_fee,0)) as remain_fee           --'剩余未还费用'  --取值逻辑待定      
,(nvl(t1.remain_amount,0) - nvl(t1.remain_principal,0)-nvl(t1.remain_interest,0) - nvl(t1.remain_svc_fee,0) - nvl(t1.remain_term_fee,0)) as remain_penalty_interest --'剩余未还罚息'  --取值逻辑待定
,(nvl(t1.overdue_principal,0) + nvl(t1.overdue_interest,0) + nvl(t1.overdue_svc_fee,0) + nvl(t1.overdue_term_fee,0) + nvl(t1.overdue_penalty,0)) as overdue_amount  --'逾期金额'
,t1.overdue_principal                                                           --'逾期本金'
,t1.overdue_interest                                                            --'逾期利息'
,(nvl(t1.overdue_svc_fee,0)+nvl(t1.overdue_term_fee,0)) as overdue_fee          --'逾期费用'
,t1.overdue_penalty                                                             --'逾期罚息'
,t1.paid_amount                                                                 --'已还金额'
,t1.paid_principal                                                              --'已还本金'
,t1.paid_interest                                                               --'已还利息'
,(nvl(t1.paid_svc_fee,0)+nvl(t1.paid_term_fee,0)) as paid_fee                   --'已还费用'
,t1.paid_penalty                                                                --'已还罚息'
,current_date() as execution_date                                               --'跑批日期'
,t1.product_id                                                                  --'产品编号'
from
(select 
* 
from 
ods_new_s_cps.loan_info
where 
'${ST9}' between s_d_date and date_sub(e_d_date,1)
) t1
 join 
(select 
 distinct 
 product_id
, channel_id
, project_id 
from dim_new.biz_conf
where project_id in ('WS0009200001','WS0006200001','WS0006200002','WS0006200003')
) t2
on t1.product_id = t2.product_id
left join
(select 
 due_bill_no
,contract_no
,loan_active_date
,cycle_day
,loan_type
,loan_init_interest_rate 
,loan_init_penalty_rate  
,loan_expire_date        
from 
ods_new_s_cps.loan_lending
) t3
on t1.due_bill_no = t3.due_bill_no
left join
due_bill_no_name t4
on t1.due_bill_no = t4.due_bill_no
union all
select
 t2.channel_id                                                                  --'渠道编号'
,t2.project_id                                                                  --'项目编号'
,t3.contract_no                                                                 --'合同编号'
,t1.due_bill_no                                                                 --'借据编号'
,t4.dim_decrypt as customer_name                                                --'客户姓名'
,t1.loan_init_principal                                                         --'合同金额'
,t3.loan_active_date                                                            --'借款生效日'
,null as debt_conversion_date                                                   --'债转日期'  --取值逻辑待定
,t3.cycle_day                                                                   --'还款日'
,t3.loan_type                                                                   --'还款方式'
,cast(0 as decimal(15,8))                                                       --'利率'
,t3.loan_init_penalty_rate                                                      --'罚息率'
,t3.loan_init_interest_rate   as loan_init_fee_rate                             --'费率'
,t3.loan_expire_date                                                            --'借款到期日'
,t1.loan_status                                                                 --'借据状态'
,t1.paid_out_date                                                               --'借款结清日'
,t1.loan_init_term                                                              --'借款期数'
,t1.loan_term_remain                                                            --'剩余期数'
,t1.overdue_days                                                                --'逾期天数'
,t1.remain_amount                                                               --'剩余未还金额'
,t1.remain_principal                                                            --'剩余未还本金'
,t1.remain_interest                                                             --'剩余未还利息'
,(nvl(t1.remain_svc_fee,0) + nvl(t1.remain_term_fee,0)) as remain_fee           --'剩余未还费用'  --取值逻辑待定
,(nvl(t1.remain_amount,0) - nvl(t1.remain_principal,0)-nvl(t1.remain_interest,0) - nvl(t1.remain_svc_fee,0) - nvl(t1.remain_term_fee,0)) as remain_penalty_interest --'剩余未还罚息'  --取值逻辑待定
,(nvl(t1.overdue_principal,0) + nvl(t1.overdue_interest,0) + nvl(t1.overdue_svc_fee,0) + nvl(t1.overdue_term_fee,0) + nvl(t1.overdue_penalty,0)) as overdue_amount  --'逾期金额'
,t1.overdue_principal                                                           --'逾期本金'
,t1.overdue_interest                                                            --'逾期利息'
,(nvl(t1.overdue_svc_fee,0)+nvl(t1.overdue_term_fee,0)) as overdue_fee          --'逾期费用'
,t1.overdue_penalty                                                             --'逾期罚息'
,t1.paid_amount                                                                 --'已还金额'
,t1.paid_principal                                                              --'已还本金'
,t1.paid_interest                                                               --'已还利息'
,(nvl(t1.paid_svc_fee,0)+nvl(t1.paid_term_fee,0)) as paid_fee                   --'已还费用'
,t1.paid_penalty                                                                --'已还罚息'
,current_date() as execution_date                                               --'跑批日期'
,t1.product_id                                                                  --'产品编号'
from
(select
*
from
ods_new_s.loan_info
where
'${ST9}' between s_d_date and date_sub(e_d_date,1)
) t1
 join
(select
 distinct
 product_id
, channel_id
, project_id
from dim_new.biz_conf
where project_id in ('bd')
) t2
on t1.product_id = t2.product_id
left join
(select
 due_bill_no
,contract_no
,loan_active_date
,cycle_day
,loan_type
,loan_init_interest_rate
,loan_init_penalty_rate
,loan_expire_date
from
ods_new_s.loan_lending
) t3
on t1.due_bill_no = t3.due_bill_no
left join
due_bill_no_name t4
on t1.due_bill_no = t4.due_bill_no
;

set hive.execution.engine=spark;
set spark.driver.memory=2G;
set spark.executor.cores=2;
set spark.memory.fraction=0.9;
set spark.executor.memory=4G;
set hive.mapjoin.followby.gby.localtask.max.memory.usage=0.99;
set hive.mapjoin.optimized.hashtable=true;
set hive.mapjoin.followby.gby.localtask.max.memory.usage=0.99;
set spark.sql.autoBroadcastJoinThreshold=200217728;--设置广播join的为200M
set hive.mapjoin.smalltable.filesize=200217728; --设置mapjoin 的小表阀值
set hive.heartbeat.interval=6000;
set hive.mapjoin.localtask.max.memory.usage=0.99;
set mapreduce.input.fileinputformat.split.minsize=256217728;
set spark.executor.heartbeatInterval=60s;
set hive.spark.client.future.timeout=360;
SET hive.auto.convert.join.noconditionaltask.size=200217728;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;

--set hivevar:ST9=2021-04-21;
--set hivevar:product_id='001801','001802';
insert overwrite table dim.dim_static_overdue_bill partition (biz_date,prj_type)
select
today_loan.product_id,
today_loan.due_bill_no,
today_loan.loan_term ,
today_loan.loan_active_date ,
today_loan.loan_init_principal as loan_init_principal,
today_loan.loan_init_term as loan_init_term,
today_loan.remain_term as remain_term,
nvl(today_loan.remain_principal,0) as remain_principal,
nvl(today_loan.remain_interest,0) as remain_interest,
today_loan.paid_term as paid_term,
today_loan.paid_principal as paid_principal,
today_loan.paid_interest as paid_interest,
today_loan.overdue_date_start ,
today_loan.overdue_days as overdue_days,
today_loan.loan_status as loan_status,
today_loan.biz_date,
today_loan.prj_type
from (
select
concat_ws("::",due_bill_no,product_id,cast(overdue_days as string)) as join_key,
product_id,
due_bill_no,
loan_term as loan_term,
loan_active_date,
loan_init_principal,
loan_init_term,
loan_term_remain as remain_term,
remain_principal,
remain_interest,
loan_term_repaid as paid_term,
paid_principal,
paid_interest,
overdue_date_start,
overdue_days,
loan_status,
'${ST9}' as biz_date,
case
    when product_id in ('001601','001602','001603') then 'HT'
    when product_id in ('001801','001802') then 'LXGM'
    when product_id in ('001901','001902','001903','001904','001905','001906','001907') then 'LXZT'
    when product_id in ('002001','002002','002003','002004','002005','002006','002007') then 'LX2'
    else 'ERROR'end as prj_type
from ods_new_s.loan_info
where product_id in (${product_id})
and '${ST9}' between s_d_date and date_sub(e_d_date,1)  and overdue_days in (31,61,91)
) today_loan
left join  (
select concat_ws("::",due_bill_no,product_id,cast(overdue_days as string)) as join_key from  dim_new.dim_static_overdue_bill where biz_date <'${ST9}'
)history_loan
on today_loan.join_key=history_loan.join_key
where  history_loan.join_key is null;


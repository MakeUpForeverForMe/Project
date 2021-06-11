set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=64000000;      -- 64M
set hive.merge.smallfiles.avgsize=64000000; -- 64M
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;
-- 关闭自动 MapJoin （ Hive3 的 bug，引发 No work found for tablescan ）
set hive.auto.convert.join=false;
set hive.auto.convert.join.noconditionaltask=false;

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
from ods.loan_info
where product_id in (${product_id})
and '${ST9}' between s_d_date and date_sub(e_d_date,1)  and overdue_days in (31,61,91)
) today_loan
left join  (
select concat_ws("::",due_bill_no,product_id,cast(overdue_days as string)) as join_key from  dim.dim_static_overdue_bill where biz_date <'${ST9}' and  product_id in (${product_id})
)history_loan
on today_loan.join_key=history_loan.join_key
where  history_loan.join_key is null;


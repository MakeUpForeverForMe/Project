
--SET hivevar:ST9=2021-04-30;
set hivevar:product_id_list='001801','001802';
--set hivevar:product_id_list='001801','001802';
set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=256000000;      -- 64M
set hive.merge.smallfiles.avgsize=256000000; -- 64M
set mapreduce.input.fileinputformat.split.minsize=268435456;

-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;
insert overwrite table eagle.predict_repay_day partition(biz_date,project_id,cycle_key)
select
should_repay_date,
paid_out_date,
sum(if(date_format(should_repay_date,'yyyy-MM')>=date_format( add_months('${ST9}',1) ,'yyyy-MM')and paid_out_date<='${ST9}',0,should_repay_principal ))          as  should_repay_principal,
sum(if(date_format(should_repay_date,'yyyy-MM')>=date_format( add_months('${ST9}',1),'yyyy-MM') and paid_out_date<='${ST9}',0,should_repay_interest ))           as should_repay_interest,
sum(if(date_format(should_repay_date,'yyyy-MM')>=date_format( add_months('${ST9}',1),'yyyy-MM') and paid_out_date<='${ST9}',0,should_repay_amount ))             as should_repay_amount,
sum(paid_principal)                   as paid_principal,
sum(paid_interest)                    as paid_interest,
sum(paid_amount)                      as paid_amount ,
sum(if(paid_out_date is null ,should_repay_principal,nvl(should_repay_principal,0)-nvl(paid_principal,0))) as remain_principal,
'${ST9}' as biz_date,
biz_conf.project_id as  project_id,
"settle" as cycle_key
from
(
    select
        due_bill_no
        ,product_id
        from
        ods.loan_info
        where
        '${ST9}' between s_d_date
        and date_sub(e_d_date,1) and   product_id in (${product_id_list}) and loan_status='F'

)loan
join (
select
due_bill_no,
product_id,
paid_out_date                        as paid_out_date,
should_repay_date                   as should_repay_date,
sum(should_repay_principal)                                 as should_repay_principal,
sum(should_repay_interest)                                  as should_repay_interest,
sum(should_repay_amount)                                    as should_repay_amount,
sum(paid_principal)                                         as paid_principal,
sum(paid_interest)                                          as paid_interest,
sum(paid_amount)                                            as paid_amount
from ods.repay_schedule
        where
        '${ST9}' between s_d_date
        and date_sub(e_d_date,1) and   product_id in (${product_id_list})
group by due_bill_no,paid_out_date,should_repay_date,product_id
)schedule
on loan.due_bill_no =schedule.due_bill_no and  loan.product_id =schedule.product_id
join(
select
max(if(col_name='project_id',col_val,null)) as project_id,
max(if(col_name='product_id',col_val,null)) as product_id
from dim.data_conf
where col_type='ac'
group by col_id
)biz_conf on loan.product_id =biz_conf.product_id
group by biz_conf.project_id,schedule.paid_out_date,should_repay_date


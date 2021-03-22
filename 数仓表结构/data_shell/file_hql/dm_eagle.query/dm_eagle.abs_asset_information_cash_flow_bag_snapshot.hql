-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=128000000; -- 128M
set hive.merge.smallfiles.avgsize=128000000; -- 128M
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;


-- set hivevar:bag_id=;

-- select ${bag_id};
-- !exit



insert overwrite table dm_eagle.abs_asset_information_cash_flow_bag_snapshot partition(bag_id)
-- explain
select
  project_id                                              as project_id,
  should_repay_date                                       as should_repay_date,
  cast(sum(remain_principal_term_begin) as decimal(20,8)) as remain_principal_term_begin,
  cast(sum(remain_principal_term_end)   as decimal(20,8)) as remain_principal_term_end,
  cast(sum(should_repay_amount)         as decimal(20,8)) as should_repay_amount,
  cast(sum(should_repay_principal)      as decimal(20,8)) as should_repay_principal,
  cast(sum(should_repay_interest)       as decimal(20,8)) as should_repay_interest,
  cast(sum(should_repay_cost)           as decimal(20,8)) as should_repay_cost,
  bag_id                                                  as bag_id
from (
  select
    project_id,
    should_repay_date,
    sum(should_repay_principal)      over(partition by project_id,bag_id,due_bill_no order by should_repay_date desc) as remain_principal_term_begin,
    sum(should_repay_principal_next) over(partition by project_id,bag_id,due_bill_no order by should_repay_date desc) as remain_principal_term_end,
    should_repay_amount,
    should_repay_principal,
    should_repay_interest,
    should_repay_cost,
    due_bill_no,
    bag_id
  from (
    select
      bag_info.project_id        as project_id,
      should_repay_date          as should_repay_date,
      is_empty(lead(should_repay_principal) over(partition by bag_info.project_id,bag_info.bag_id,repay_schedule.due_bill_no order by should_repay_date),0) as should_repay_principal_next,
      should_repay_amount        as should_repay_amount,
      should_repay_principal     as should_repay_principal,
      should_repay_interest      as should_repay_interest,
      should_repay_cost          as should_repay_cost,
      repay_schedule.due_bill_no as due_bill_no,
      bag_info.bag_id            as bag_id
    from (select project_id,bag_date,bag_id from dim.bag_info ${bag_id}) as bag_info
    join (select due_bill_no,bag_id from dim.bag_due_bill_no ${bag_id}) as bag_due
    on bag_info.bag_id = bag_due.bag_id
    join (
      select
        sum(should_repay_amount) as should_repay_amount,
        sum(should_repay_principal) as should_repay_principal,
        sum(should_repay_interest) as should_repay_interest,
        sum(should_repay_term_fee + should_repay_svc_fee + should_repay_penalty + should_repay_mult_amt) as should_repay_cost,
        should_repay_date,
        due_bill_no,
        s_d_date,
        e_d_date,
        project_id
      from ods.repay_schedule_abs
      where 1 > 0
        -- and due_bill_no = '1000682129'
      group by should_repay_date,due_bill_no,s_d_date,e_d_date,project_id
      -- order by project_id,due_bill_no,should_repay_date,s_d_date
    ) as repay_schedule
    on  repay_schedule.project_id  = bag_info.project_id
    and repay_schedule.due_bill_no = bag_due.due_bill_no
    where 1 > 0
      and if(bag_info.bag_date < s_d_date,s_d_date,bag_info.bag_date) between s_d_date and date_sub(e_d_date,1)
      and bag_info.bag_date <= should_repay_date
  ) as tmp
) as tmp
group by project_id,should_repay_date,bag_id
-- order by project_id,bag_id,should_repay_date
-- limit 10
;

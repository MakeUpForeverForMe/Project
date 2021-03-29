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

set hive.auto.convert.join=false;                    -- 关闭自动 MapJoin
set hive.auto.convert.join.noconditionaltask=false;  -- 关闭自动 MapJoin



-- set hivevar:bag_id=;
-- set hivevar:ST9=2021-01-11;


with loan_base as ( -- 取封包日期至当前时间为止的数据
  select
    bag_info.project_id                                                             as project_id,
    bag_info.bag_id                                                                 as bag_id,
    bag_due.due_bill_no                                                             as due_bill_no,
    loan_lending.contract_no                                                        as contract_no,
    loan_info.overdue_days                                                          as overdue_days,
    if(loan_info.s_d_date = loan_info.overdue_days_min_start_date,'y','n')          as is_first_overdue_day,
    bag_info.bag_date                                                               as bag_date,
    if(bag_info.bag_date < loan_info.min_date,loan_info.min_date,bag_info.bag_date) as min_date,
    '${ST9}'                                                                        as biz_date,
    loan_info.s_d_date                                                              as s_d_date,
    loan_info.e_d_date                                                              as e_d_date
  from (
    select
      project_id,bag_date,bag_id
    from dim.bag_info
    where 1 > 0
      and bag_date <= '${ST9}'
      ${bag_id}
  ) as bag_info
  join (
    select
      due_bill_no,bag_id
    from dim.bag_due_bill_no
    where 1 > 0
      ${bag_id}
  ) as bag_due
  on bag_info.bag_id = bag_due.bag_id
  join (
    select
      project_id,
      due_bill_no,
      overdue_days,
      min(overdue_date_start) over(partition by due_bill_no,overdue_days) as overdue_days_min_start_date,
      min(s_d_date)           over(partition by due_bill_no) as min_date,
      s_d_date,
      e_d_date
    from ods.loan_info_abs
    where 1 > 0
      and s_d_date <= '${ST9}'
      -- and overdue_days = 0
      -- and overdue_days > 60
      -- and overdue_days > 180
      -- and due_bill_no = '004102b282c14d8590c442d78089edd1'
  ) as loan_info
  on  loan_info.project_id  = bag_info.project_id
  and loan_info.due_bill_no = bag_due.due_bill_no
  left join (
    select distinct
      project_id,
      due_bill_no,
      contract_no
    from ods.loan_lending_abs
  ) as loan_lending
  on  loan_info.project_id  = loan_lending.project_id
  and loan_info.due_bill_no = loan_lending.due_bill_no
  join (
    select
      project_id,
      due_bill_no,
      user_hash_no
    from ods.customer_info_abs
    where 1 > 0
      -- and user_hash_no = 'a_7836af23d8bc3d641933b6ed459945d964bbbf975cecc7a9ae09293c28fd53a3'
  ) as customer_info
  on  customer_info.project_id  = bag_info.project_id
  and customer_info.due_bill_no = bag_due.due_bill_no
  where 1 > 0
    and loan_info.e_d_date > bag_info.bag_date
    -- and overdue_days > 30 and overdue_days < 60
  -- order by due_bill_no,s_d_date
  -- limit 10
)


insert overwrite table dm_eagle.abs_overdue_rate_details_day partition(biz_date,project_id,bag_id)
select
  molecular_once.is_allbag          as is_allbag,
  molecular_once.due_bill_no        as due_bill_no,
  molecular_once.contract_no        as contract_no,
  loan_info.overdue_days            as overdue_days,
  nvl(molecular_curr.type_curr,'n') as type_curr,
  nvl(molecular_once.type_once,'n') as type_once,
  nvl(molecular_new.type_new,'n')   as type_new,
  loan_info.loan_init_term          as loan_terms,
  loan_info.loan_init_principal     as loan_principal,
  loan_info.loan_term_remain        as remain_term,
  loan_info.dpd_days_max            as overdue_days_max,
  loan_info.remain_principal        as remain_principal,
  loan_info.overdue_principal       as overdue_principal,
  loan_info.overdue_interest        as overdue_interest,
  (loan_info.overdue_svc_fee + loan_info.overdue_term_fee + loan_info.overdue_penalty + loan_info.overdue_mult_amt) as overdue_cost,
  molecular_once.biz_date           as biz_date,
  molecular_once.project_id         as project_id,
  molecular_once.bag_id             as bag_id
from ( -- 累计
  select distinct -- 封包时累计所有包
    biz_date,
    project_id,
    'default_all_bag' as bag_id,
    'y' as is_allbag,
    'y' as type_once,
    due_bill_no,
    contract_no
  from loan_base
  where 1 > 0
    and is_first_overdue_day = 'y'
    and overdue_days in (1,8,15,31,61,91,121,151,181)
    -- and project_id = 'CL201911130070'
    -- and bag_id = 'CL201911130070_12'
  union all
  select distinct -- 封包时累计单个包
    biz_date,
    project_id,
    bag_id,
    'n' as is_allbag,
    'y' as type_once,
    due_bill_no,
    contract_no
  from loan_base
  where 1 > 0
    and is_first_overdue_day = 'y'
    and overdue_days in (1,8,15,31,61,91,121,151,181)
    -- and project_id = 'CL201911130070'
    -- and bag_id = 'CL201911130070_12'
) as molecular_once
left join ( -- 当前
  select distinct -- 封包时当前所有包
    project_id,
    'default_all_bag' as bag_id,
    'y' as is_allbag,
    'y' as type_curr,
    due_bill_no
  from loan_base
  where 1 > 0
    and biz_date between s_d_date and date_sub(e_d_date,1)
    -- and if(bag_date < min_date,min_date,bag_date) between s_d_date and date_sub(e_d_date,1)
  union all
  select distinct -- 封包时当前单个包
    project_id,
    bag_id,
    'n' as is_allbag,
    'y' as type_curr,
    due_bill_no
  from loan_base
  where 1 > 0
    and biz_date between s_d_date and date_sub(e_d_date,1)
    -- and if(bag_date < min_date,min_date,bag_date) between s_d_date and date_sub(e_d_date,1)
) as molecular_curr
on  molecular_once.project_id  = molecular_curr.project_id
and molecular_once.bag_id      = molecular_curr.bag_id
and molecular_once.is_allbag   = molecular_curr.is_allbag
and molecular_once.due_bill_no = molecular_curr.due_bill_no
left join ( -- 新增
  select distinct -- 封包时新增所有包
    project_id,
    'default_all_bag' as bag_id,
    'y' as is_allbag,
    'y' as type_new,
    due_bill_no
  from loan_base
  where 1 > 0
    and biz_date between s_d_date and date_sub(e_d_date,1)
    and overdue_days in (1,8,15,31,61,91,121,151,181)
    and is_first_overdue_day = 'y'
  union all
  select distinct -- 封包时新增单个包
    project_id,
    bag_id,
    'n' as is_allbag,
    'y' as type_new,
    due_bill_no
  from loan_base
  where 1 > 0
    and biz_date between s_d_date and date_sub(e_d_date,1)
    and overdue_days in (1,8,15,31,61,91,121,151,181)
    and is_first_overdue_day = 'y'
) as molecular_new
on  molecular_once.project_id  = molecular_new.project_id
and molecular_once.bag_id      = molecular_new.bag_id
and molecular_once.is_allbag   = molecular_new.is_allbag
and molecular_once.due_bill_no = molecular_new.due_bill_no
left join (
  select * from ods.loan_info_abs
  where 1 > 0
    and '${ST9}' between s_d_date and date_sub(e_d_date,1)
) as loan_info
on  molecular_once.project_id  = loan_info.project_id
and molecular_once.due_bill_no = loan_info.due_bill_no
-- order by due_bill_no,project_id,bag_id
-- limit 10
;

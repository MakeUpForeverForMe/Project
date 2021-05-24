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


-- set hivevar:ST9=2021-05-10;

-- set hivevar:bag_id=
--   select distinct bag_id
--   from dim.bag_info
--   where 1 > 0
-- ;



with loan_base as ( -- 取封包日期至当前时间为止的数据
  select
    dw_abs.project_id          as project_id,
    dw_abs.due_bill_no         as due_bill_no,
    dw_abs.contract_no         as contract_no,
    dw_abs.loan_init_term      as loan_init_term,
    dw_abs.loan_init_principal as loan_init_principal,
    dw_abs.loan_term_remain    as loan_term_remain,
    dw_abs.remain_principal    as remain_principal,
    dw_abs.overdue_principal   as overdue_principal,
    dw_abs.overdue_interest    as overdue_interest,
    dw_abs.overdue_svc_fee     as overdue_svc_fee,
    dw_abs.overdue_term_fee    as overdue_term_fee,
    dw_abs.overdue_penalty     as overdue_penalty,
    dw_abs.overdue_mult_amt    as overdue_mult_amt,
    dw_abs.dpd_days_max        as dpd_days_max,
    dw_abs.overdue_days        as overdue_days,
    dw_abs.dpd_map             as dpd_map,
    dw_abs.biz_date            as biz_date,
    bag_due.bag_date           as bag_date,
    bag_due.bag_id             as bag_id
  from (
    select
      *
    from dw.abs_due_info_day_abs
    where 1 > 0
      and biz_date = '${ST9}'
  ) as dw_abs
  inner join (
    select
      bag_info.project_id,
      bag_info.bag_id,
      bag_info.bag_date,
      bag_due.due_bill_no
    from (
      select
        project_id,
        bag_id,
        bag_date
      from dim.bag_info
      where 1 > 0
        and bag_id in (${bag_id})
        and bag_date <= '${ST9}'
    ) as bag_info
    inner join (
      select
        project_id,
        bag_id,
        due_bill_no
      from dim.bag_due_bill_no
    ) as bag_due
    on  bag_info.project_id = bag_due.project_id
    and bag_info.bag_id     = bag_due.bag_id
  ) as bag_due
  on  dw_abs.project_id  = bag_due.project_id
  and dw_abs.due_bill_no = bag_due.due_bill_no
)


insert overwrite table dm_eagle.abs_overdue_rate_details_day partition(biz_date,project_id,bag_id)
select
  molecular_all.due_bill_no         as due_bill_no,
  molecular_all.contract_no         as contract_no,
  molecular_all.overdue_days        as overdue_days,
  nvl(molecular_curr.type_curr,'n') as type_curr,
  nvl(molecular_once.type_once,'n') as type_once,
  nvl(molecular_new.type_new,'n')   as type_new,
  molecular_all.loan_init_term      as loan_terms,
  molecular_all.loan_init_principal as loan_principal,
  molecular_all.loan_term_remain    as remain_term,
  molecular_all.dpd_days_max        as overdue_days_max,
  molecular_all.remain_principal    as remain_principal,
  molecular_all.overdue_principal   as overdue_principal,
  molecular_all.overdue_interest    as overdue_interest,
  (molecular_all.overdue_svc_fee + molecular_all.overdue_term_fee + molecular_all.overdue_penalty + molecular_all.overdue_mult_amt) as overdue_cost,
  molecular_all.biz_date            as biz_date,
  molecular_all.project_id          as project_id,
  molecular_all.bag_id              as bag_id
from (
  select
    biz_date,
    project_id,
    bag_id,
    due_bill_no,
    overdue_days,
    loan_init_term,
    loan_init_principal,
    loan_term_remain,
    remain_principal,
    overdue_principal,
    overdue_interest,
    overdue_svc_fee,
    overdue_term_fee,
    overdue_penalty,
    overdue_mult_amt,
    dpd_days_max,
    contract_no
  from loan_base
) as molecular_all
left join ( -- 累计
  select distinct
    project_id,
    bag_id,
    'y' as type_once,
    due_bill_no,
    contract_no
  from (
    select
      project_id,
      bag_id,
      due_bill_no,
      contract_no,
      overdue_date_start,
      min(overdue_date_start) over(partition by project_id,bag_id,due_bill_no,overdue_days_once) as min_overdue_date_start
    from loan_base
    lateral view explode(dpd_map) dpd_maps as overdue_days_once,dpd_val
    lateral view explode(map_from_str(dpd_val)) dpd_vals as overdue_date_start,overdue_remain_principal_once
    where overdue_date_start >= bag_date
  ) as tmp
  where overdue_date_start = min_overdue_date_start
) as molecular_once
on  molecular_all.project_id  = molecular_once.project_id
and molecular_all.bag_id      = molecular_once.bag_id
and molecular_all.due_bill_no = molecular_once.due_bill_no
left join ( -- 当前
  select distinct
    project_id,
    bag_id,
    'y' as type_curr,
    due_bill_no
  from loan_base
  where 1 > 0
    and overdue_days > 0
) as molecular_curr
on  molecular_all.project_id  = molecular_curr.project_id
and molecular_all.bag_id      = molecular_curr.bag_id
and molecular_all.due_bill_no = molecular_curr.due_bill_no
left join ( -- 新增
  select distinct
    project_id,
    bag_id,
    'y' as type_new,
    due_bill_no
  from loan_base
  lateral view explode(dpd_map) dpd_maps as overdue_days_once,dpd_val
  where 1 > 0
    and overdue_days in (1,8,15,31,61,91,121,151,181)
    and overdue_days = overdue_days_once
    and size(map_keys(map_from_str(dpd_val))) = 1
    and map_keys(map_from_str(dpd_val))[0] = date_sub(biz_date,cast(overdue_days as int) - 1)
) as molecular_new
on  molecular_all.project_id  = molecular_new.project_id
and molecular_all.bag_id      = molecular_new.bag_id
and molecular_all.due_bill_no = molecular_new.due_bill_no
-- order by due_bill_no,project_id,bag_id
-- limit 10
;

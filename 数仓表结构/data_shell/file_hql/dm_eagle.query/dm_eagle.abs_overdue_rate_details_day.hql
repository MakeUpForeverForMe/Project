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
    loan_info.project_id           as project_id,
    loan_info.due_bill_no          as due_bill_no,
    loan_info.contract_no          as contract_no,
    loan_info.overdue_days         as overdue_days,
    loan_info.is_first_overdue_day as is_first_overdue_day,
    loan_info.s_d_date             as s_d_date,
    loan_info.e_d_date             as e_d_date,
    loan_info.biz_date             as biz_date,
    bag_due.due_bill_no            as bag_due_bill_no,
    bag_due.bag_date               as bag_date,
    bag_due.bag_id                 as bag_id
  from (
    select
      loan.*,
      '${ST9}' as biz_date,
      lending.contract_no
    from (
      select
        project_id                                                                                                      as project_id,
        due_bill_no                                                                                                     as due_bill_no,
        overdue_days                                                                                                    as overdue_days,
        if(overdue_date_start = min(overdue_date_start) over(partition by project_id,due_bill_no,overdue_days),'y','n') as is_first_overdue_day,
        s_d_date                                                                                                        as s_d_date,
        e_d_date                                                                                                        as e_d_date
      from ods.loan_info_abs
      where 1 > 0
        and s_d_date <= '${ST9}'
        -- and project_id  = 'CL202011090089'
        -- and due_bill_no = '1000002321'
    ) as loan
    join (
      select
        project_id,
        due_bill_no,
        contract_no
      from ods.loan_lending_abs
    ) as lending
    on  loan.project_id  = lending.project_id
    and loan.due_bill_no = lending.due_bill_no
  ) as loan_info
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
  on  loan_info.project_id  = bag_due.project_id
  and loan_info.due_bill_no = bag_due.due_bill_no
  and loan_info.e_d_date    > bag_due.bag_date
)


insert overwrite table dm_eagle.abs_overdue_rate_details_day partition(biz_date,project_id,bag_id)
select
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
  select distinct
    biz_date,
    project_id,
    bag_id,
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
  select distinct
    project_id,
    bag_id,
    'y' as type_curr,
    due_bill_no
  from loan_base
  where 1 > 0
    and biz_date between s_d_date and date_sub(e_d_date,1)
    -- and if(bag_date < min_date,min_date,bag_date) between s_d_date and date_sub(e_d_date,1)
) as molecular_curr
on  molecular_once.project_id  = molecular_curr.project_id
and molecular_once.bag_id      = molecular_curr.bag_id
and molecular_once.due_bill_no = molecular_curr.due_bill_no
left join ( -- 新增
  select distinct
    project_id,
    bag_id,
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

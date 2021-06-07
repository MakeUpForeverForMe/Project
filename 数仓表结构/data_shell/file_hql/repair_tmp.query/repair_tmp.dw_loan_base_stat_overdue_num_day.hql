set hivevar:db_suffix=;
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
-- maojoin 调优
set hive.mapjoin.smalltable.filesize=25000000;
set hive.auto.convert.join.noconditionaltask.size =10000000;

set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10;
set hivevar:hive_param_str= and product_id ='002001';



insert overwrite table repair_tmp.dw_loan_base_stat_overdue_num_day partition(biz_date,product_id)
select
  loan_init_term                                                                                 as loan_terms,
  loan_active_date                                                                               as loan_active_date,
  overdue_mob                                                                                    as overdue_mob,
  if(is_first_term_overdue = 1,1,0)                                                              as is_first_term_overdue,
  if(paid_out_type = 'BUY_BACK',1,0)                                                             as is_buyback,
  nvl(datediff('${ST9}',coalesce(should_repay_date_repay_schedule,loan_active_date,null)) + 1,0) as overdue_dob,

  should_repay_date                                                                              as should_repay_date_curr,
  should_repay_date_history_curr                                                                 as should_repay_date_history_curr,
  sum(nvl(should_repay_principal_curr,0))                                                        as should_repay_principal_curr,
  count(due_bill_no_curr)                                                                        as should_repay_loan_num_curr,

  sum(paid_principal_curr)                                                                       as should_repay_paid_principal_curr,

  sum(nvl(should_repay_principal_curr_actual,0))                                                 as should_repay_principal_curr_actual,
  count(if(nvl(should_repay_principal_curr_actual,0) = 0,null,due_bill_no_curr))                 as should_repay_loan_num_curr_actual,

  should_repay_date_repay_schedule                                                               as should_repay_date,
  should_repay_date_history_repay_schedule                                                       as should_repay_date_history,
  sum(nvl(should_repay_principal,0))                                                             as should_repay_principal,
  count(due_bill_no_repay_schedule)                                                              as should_repay_loan_num,

  overdue_date_first                                                                             as overdue_date_first,
  overdue_date_start                                                                             as overdue_date_start,
  is_first_overdue_day                                                                           as is_first_overdue_day,
  overdue_days                                                                                   as overdue_days,
  nvl(overdue_stage_previous,0)                                                                  as overdue_stage_previous,
  nvl(overdue_stage_curr,0)                                                                      as overdue_stage_curr,
  overdue_stage                                                                                  as overdue_stage,
  sum(nvl(unposted_principal,0))                                                                 as unposted_principal,
  sum(nvl(remain_principal,0))                                                                   as remain_principal,
  sum(nvl(paid_principal,0))                                                                     as paid_principal,
  sum(nvl(overdue_principal,0))                                                                  as overdue_principal,
  count(overdue_due_bill_no)                                                                     as overdue_loan_num,
  sum(nvl(overdue_remain_principal,0))                                                           as overdue_remain_principal,
  biz_date                                                                                       as biz_date,
  product_id                                                                                     as product_id
from ( -- 借据主表
  select
    '${ST9}'                                                                                   as biz_date,
    product_id                                                                                 as product_id,
    due_bill_no                                                                                as due_bill_no,
    loan_init_term                                                                             as loan_init_term,
    loan_active_date                                                                           as loan_active_date,
    (year('${ST9}') - year(loan_active_date)) * 12 + month('${ST9}') - month(loan_active_date) as overdue_mob,
    should_repay_date                                                                          as should_repay_date,
    overdue_date_first                                                                         as overdue_date_first,
    overdue_date_start                                                                         as overdue_date_start,
    if(loan_term = loan_term_min,1,0)                                                          as is_first_overdue_day,
    overdue_days                                                                               as overdue_days,
    ceil(overdue_days / 30)                                                                    as overdue_stage,
    nvl(remain_principal,0)                                                                    as remain_principal,
    nvl(paid_principal,0)                                                                      as paid_principal,
    nvl(overdue_principal,0)                                                                   as overdue_principal,
    if(overdue_days > 0,due_bill_no,null)                                                      as overdue_due_bill_no,
    if(overdue_days > 0,nvl(remain_principal,0),0)                                             as overdue_remain_principal,
    due_bill_no_curr                                                                           as due_bill_no_curr,
    should_repay_date_history_curr                                                             as should_repay_date_history_curr,
    should_repay_principal_curr                                                                as should_repay_principal_curr,
    paid_principal_curr                                                                        as paid_principal_curr,
    should_repay_principal_curr_actual                                                         as should_repay_principal_curr_actual,
    paid_out_type                                                                              as paid_out_type
  from ( -- 主表，正常取数据
    select
      *,
      concat(due_bill_no,'_',overdue_days)      as due_bill_no_overdue_days,
      concat(due_bill_no,'_',should_repay_date) as due_bill_no_should_repay_date
    from ods${db_suffix}.loan_info
    where 1 > 0
      and '${ST9}' between s_d_date and date_sub(e_d_date,1)
      and loan_active_date <= '${ST9}' -- 目前针对滴滴，过滤掉提前获取到数据的情况（如：2020-08-28 给了放款日为 2020-08-29 的数据，这样的需要过滤掉。否则涉及到分母为放款金额的，会出现计算不准确）
      ${hive_param_str}
  ) as loan_info
  left join ( -- 判断当前逾期天数是否是第一期出现
    select
      product_id                           as product_id_first,
      concat(due_bill_no,'_',overdue_days) as due_bill_no_overdue_days_first,
      min(loan_term)                       as loan_term_min
    from ods${db_suffix}.loan_info
    where 1 > 0
      ${hive_param_str}
    group by product_id,concat(due_bill_no,'_',overdue_days)
  ) as is_first_overdue_day
  on  product_id               = product_id_first
  and due_bill_no_overdue_days = due_bill_no_overdue_days_first
  left join ( -- 取出当前 计划应还金额、已还金额、实际应还金额
    select
      product_id                                   as product_id_curr,
      due_bill_no                                  as due_bill_no_curr,
      concat(due_bill_no,'_',should_repay_date)    as due_bill_no_should_repay_date_curr,
      min(should_repay_date_history)               as should_repay_date_history_curr,
      sum(should_repay_principal)                  as should_repay_principal_curr,
      sum(paid_principal)                          as paid_principal_curr,
      sum(should_repay_principal - paid_principal) as should_repay_principal_curr_actual
    from ods${db_suffix}.repay_schedule
    where 1 > 0
      and '${ST9}' between s_d_date and date_sub(e_d_date,1)
      and loan_term != 0
      and loan_active_date <= '${ST9}' -- 目前针对滴滴，过滤掉提前获取到数据的情况（如：2020-08-28 给了放款日为 2020-08-29 的数据，这样的需要过滤掉。否则涉及到分母为放款金额的，会出现计算不准确）
      ${hive_param_str}
    group by product_id,due_bill_no,concat(due_bill_no,'_',should_repay_date)
  ) as repay_schedule_curr
  on  product_id                    = product_id_curr
  and due_bill_no_should_repay_date = due_bill_no_should_repay_date_curr
) as loan_info
left join ( -- 取上期应还相关信息
  select
    product_id                     as product_id_repay_schedule,
    due_bill_no                    as due_bill_no_repay_schedule,
    should_repay_date              as should_repay_date_repay_schedule,
    is_first_term_overdue          as is_first_term_overdue,
    min(should_repay_date_history) as should_repay_date_history_repay_schedule,
    sum(should_repay_principal)    as should_repay_principal
  from ( -- 还款计划当前正常数据
    select
      product_id,
      due_bill_no,
      should_repay_date,
      concat(due_bill_no,'_',should_repay_date) as due_bill_no_should_repay_date,
      if(schedule_status = 'O' and loan_term = 1,1,0) as is_first_term_overdue,
      should_repay_date_history,
      should_repay_principal
    from ods${db_suffix}.repay_schedule
    where 1 > 0
      and '${ST9}' between s_d_date and date_sub(e_d_date,1)
      and loan_term != 0
      and loan_active_date <= '${ST9}' -- 目前针对滴滴，过滤掉提前获取到数据的情况（如：2020-08-28 给了放款日为 2020-08-29 的数据，这样的需要过滤掉。否则涉及到分母为放款金额的，会出现计算不准确）
      ${hive_param_str}
  ) as repay_schedule_a
  join ( -- 还款计划 应还日 在 当前快照日 之前的数据，且只取最大应还日 只有其中一期
    select
      concat(due_bill_no,'_',max(should_repay_date)) as due_bill_no_should_repay_date_b,
      product_id                                     as product_id_b
    from ods${db_suffix}.repay_schedule
    where 1 > 0
      and '${ST9}' between s_d_date and date_sub(e_d_date,1)
      and should_repay_date <= '${ST9}' -- 取应还日小于等于快照日的数据中的最大应还日
      and loan_term != 0
      and loan_active_date <= '${ST9}' -- 目前针对滴滴，过滤掉提前获取到数据的情况（如：2020-08-28 给了放款日为 2020-08-29 的数据，这样的需要过滤掉。否则涉及到分母为放款金额的，会出现计算不准确）
      ${hive_param_str}
    group by due_bill_no,product_id
  ) as repay_schedule_b
  on  product_id        = product_id_b
  and due_bill_no_should_repay_date = due_bill_no_should_repay_date_b
  group by
    product_id,
    due_bill_no,
    should_repay_date,
    should_repay_date_history,
    is_first_term_overdue
) as repay_schedule
on  product_id  = product_id_repay_schedule
and due_bill_no = due_bill_no_repay_schedule
-- and nvl(should_repay_date_history_curr,'a') = nvl(should_repay_date_history_repay_schedule,'a')
left join ( -- 取未出账本金
  select
    product_id                                   as product_id_unposted,
    due_bill_no                                  as due_bill_no_unposted,
    sum(should_repay_principal - paid_principal) as unposted_principal
  from ods${db_suffix}.repay_schedule
  where 1 > 0
    -- and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
    and '${ST9}' between s_d_date and date_sub(e_d_date,1)
    and schedule_status = 'N'
    and loan_active_date <= '${ST9}' -- 目前针对滴滴，过滤掉提前获取到数据的情况（如：2020-08-28 给了放款日为 2020-08-29 的数据，这样的需要过滤掉。否则涉及到分母为放款金额的，会出现计算不准确）
    ${hive_param_str}
  group by product_id,due_bill_no
  -- having count(due_bill_no) > 1
  -- order by due_bill_no_unposted,product_id_unposted
  -- limit 10
) as unposted
on  product_id  = product_id_unposted
and due_bill_no = due_bill_no_unposted
left join (  select
    nvl(product_id_curr,product_id_previous)   as product_id_overdue_stage,
    nvl(due_bill_no_curr,due_bill_no_previous) as due_bill_no_overdue_stage,
    nvl(overdue_stage_previous,0)              as overdue_stage_previous,
    nvl(overdue_stage_curr,0)                  as overdue_stage_curr
  from ( -- 按照期数取当前的逾期阶段
    select
      product_id       as product_id_curr,
      due_bill_no      as due_bill_no_curr,
      count(loan_term) as overdue_stage_curr
    from ods${db_suffix}.repay_schedule
    where 1 > 0
      and '${ST9}' between s_d_date and date_sub(e_d_date,1)
      and loan_active_date <= '${ST9}' -- 目前针对滴滴，过滤掉提前获取到数据的情况（如：2020-08-28 给了放款日为 2020-08-29 的数据，这样的需要过滤掉。否则涉及到分母为放款金额的，会出现计算不准确）
      and loan_term != 0
      and schedule_status = 'O'
      ${hive_param_str}
    group by product_id,due_bill_no
  ) as repay_schedule_overdue_stage_curr
  full join ( -- 按照期数取上期的逾期阶段
    select
      product_id_previous,
      due_bill_no_previous,
      count(loan_term_previous) as overdue_stage_previous
    from (
      select
        product_id  as product_id_previous,
        due_bill_no as due_bill_no_previous,
        loan_term   as loan_term_previous,
        s_d_date,
        e_d_date
      from ods${db_suffix}.repay_schedule
      where 1 > 0
        and loan_active_date <= '${ST9}' -- 目前针对滴滴，过滤掉提前获取到数据的情况（如：2020-08-28 给了放款日为 2020-08-29 的数据，这样的需要过滤掉。否则涉及到分母为放款金额的，会出现计算不准确）
        and loan_term != 0
        and schedule_status = 'O'
        ${hive_param_str}
    ) as repay_schedule_overdue_stage_previous
    join ( -- 还款计划 应还日 在 当前快照日 之前的数据，且只取最大应还日 只有其中一期
      select
        product_id             as product_id_should,
        due_bill_no            as due_bill_no_should,
        max(should_repay_date) as should_repay_date_max
      from ods${db_suffix}.repay_schedule
      where 1 > 0
        and '${ST9}' between s_d_date and date_sub(e_d_date,1)
        and should_repay_date <= '${ST9}' -- 取应还日小于等于快照日的数据中的最大应还日
        and loan_term != 0
        and loan_active_date <= '${ST9}' -- 目前针对滴滴，过滤掉提前获取到数据的情况（如：2020-08-28 给了放款日为 2020-08-29 的数据，这样的需要过滤掉。否则涉及到分母为放款金额的，会出现计算不准确）
        ${hive_param_str}
      group by due_bill_no,product_id
    ) as repay_schedule_b
    on  product_id_previous  = product_id_should
    and due_bill_no_previous = due_bill_no_should
    where 1 > 0 and date_sub(should_repay_date_max,1) between s_d_date and date_sub(e_d_date,1)
    group by product_id_previous,due_bill_no_previous
  ) as tmp
  on  product_id_curr  = product_id_previous
  and due_bill_no_curr = due_bill_no_previous
) as repay_schedule_overdue_stage
on  product_id  = product_id_overdue_stage
and due_bill_no = due_bill_no_overdue_stage
-- where 1 > 0
  -- and overdue_days > 0
  -- and product_id = 'DIDI201908161538'
  -- and due_bill_no = '1120070719352198875035'
group by
  loan_init_term,
  loan_active_date,
  overdue_mob,
  if(is_first_term_overdue = 1,1,0),
  if(paid_out_type = 'BUY_BACK',1,0),
  nvl(datediff('${ST9}',coalesce(should_repay_date_repay_schedule,loan_active_date,null)) + 1,0),
  should_repay_date,
  should_repay_date_history_curr,
  should_repay_date_repay_schedule,
  should_repay_date_history_repay_schedule,
  overdue_date_first,
  overdue_date_start,
  is_first_overdue_day,
  overdue_days,
  nvl(overdue_stage_previous,0),
  nvl(overdue_stage_curr,0),
  overdue_stage,
  biz_date,
  product_id
-- limit 10
;


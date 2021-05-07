 set var:ST9=2021-02-01;

 set var:db_suffix=;

 set var:db_suffix=_cps;

invalidate metadata ods${var:db_suffix}.repay_schedule;
invalidate metadata ods${var:db_suffix}.repay_detail;
invalidate metadata dm_eagle${var:db_suffix}.eagle_should_repay_repaid_amount_day;
-- ods 应还、实还 与 dm 应还实还 对比 应还金额、实还金额
--还款计划观察日应还 - 已还
--实还明细观察日实还
--dm观察日应还，已还
--还款计划观察日应还 - 已还 vs dm应还日应还 && 实还明细观察日实还 vs dm观察日已还
select
  'ods 应还、实还 与 dm 应还实还 对比 应还金额、实还金额'             as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                       as cps,
  nvl(schedule.biz_date,repay_detail.biz_date)                        as biz_date,
  nvl(schedule.project_id,repay_detail.project_id)                    as project_id,
  nvl(schedule.should_repay_amount,0)                                 as should_dw,
  nvl(dm.should_repay_amount,0)                                       as should_dm,
  nvl(schedule.should_repay_amount,0) - nvl(dm.should_repay_amount,0) as diff_should,
  nvl(repay_detail.repaid_amount,0)                                   as repaid_dw,
  nvl(dm.repaid_amount,0)                                             as repaid_dm,
  nvl(repay_detail.repaid_amount,0) - nvl(dm.repaid_amount,0)         as diff_repaid
from (
  select
    project_id               as project_id,
    should_repay_date        as biz_date,
    sum(should_repay_amount) as should_repay_amount
  from (
  select 
      distinct
      project_id,
      product_id
      from (
      select
        max(if(col_name = 'project_id',   col_val,null)) as project_id,
        max(if(col_name = 'product_id',   col_val,null)) as product_id
      from dim.data_conf
      where col_type = 'ac'
      group by col_id
      )tmp
      where project_id in ('WS0006200001','WS0006200002','WS0006200003','WS0009200001')
  )  biz_conf
  join (
    select
      product_id                             as product_id,
      should_repay_date                      as should_repay_date,
      sum(should_repay_amount - paid_amount) as should_repay_amount
    from ods${var:db_suffix}.repay_schedule
    where 1 > 0
      and product_id in (
        '001801','001802','001803','001804',
        '001901','001902','001903','001904','001905','001906','001907',
        '002001','002002','002003','002004','002005','002006','002007',
        '002401','002402',
        ''
      )
      and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
      --and should_repay_date <= '${var:ST9}'
      and should_repay_date = '${var:ST9}'
    group by product_id,should_repay_date
  ) as repay_schedule
  on biz_conf.product_id = repay_schedule.product_id
  group by project_id,should_repay_date
) as schedule
full join (
  select
    project_id         as project_id,
    biz_date           as biz_date,
    sum(repaid_amount) as repaid_amount
  from (
  select 
      distinct
      project_id,
      product_id
      from (
      select
        max(if(col_name = 'project_id',   col_val,null)) as project_id,
        max(if(col_name = 'product_id',   col_val,null)) as product_id
      from dim.data_conf
      where col_type = 'ac'
      group by col_id
      )tmp
      where project_id in ('WS0006200001','WS0006200002','WS0006200003','WS0009200001')
  )  biz_conf
  join (
    select
      product_id        as product_id,
      biz_date          as biz_date,
      sum(repay_amount) as repaid_amount
    from ods${var:db_suffix}.repay_detail
    where 1 > 0
      and product_id in (
        '001801','001802','001803','001804',
        '001901','001902','001903','001904','001905','001906','001907',
        '002001','002002','002003','002004','002005','002006','002007',
        '002401','002402',
        ''
      )
      --and biz_date <= '${var:ST9}'
      and biz_date = '${var:ST9}'
    group by product_id,biz_date
  ) as repay_detail
  on biz_conf.product_id = repay_detail.product_id
  group by project_id,biz_date
) as repay_detail
on  schedule.project_id = repay_detail.project_id
and schedule.biz_date   = repay_detail.biz_date
left join 
(
   select
     project_id                      as project_id,
     biz_date                        as biz_date,
     sum(should_repay_amount_actual) as should_repay_amount,   --实际应还
     sum(repaid_amount)              as repaid_amount          --实际已还
   from dm_eagle${var:db_suffix}.eagle_should_repay_repaid_amount_day
   where 1 > 0
     and project_id in ('WS0006200001','WS0006200002','WS0006200003','WS0009200001')
     --and biz_date <= '${var:ST9}'
     and biz_date = '${var:ST9}'
   group by biz_date,project_id
) as dm
on  nvl(schedule.project_id,repay_detail.project_id) = dm.project_id
and nvl(schedule.biz_date,repay_detail.biz_date)     = dm.biz_date
where nvl(schedule.should_repay_amount,0) - nvl(dm.should_repay_amount,0) != 0 or nvl(repay_detail.repaid_amount,0) - nvl(dm.repaid_amount,0) != 0
order by biz_date,project_id
;
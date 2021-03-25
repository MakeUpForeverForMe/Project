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



-- set hivevar:ST9=2019-10-01;

insert overwrite table dm_eagle.abs_asset_information_cash_flow_bag_day partition(biz_date,project_id,bag_id)
select
  biz_dates.bag_date                           as bag_date,
  max(repay_date_max.biz_date) over(partition by biz_dates.project_id,biz_dates.bag_date,biz_dates.bag_id order by biz_dates.biz_date) as data_extraction_day,

  nvl(repay_schedule.should_repay_amount   ,0) as should_repay_amount,
  nvl(repay_schedule.should_repay_principal,0) as should_repay_principa,
  nvl(repay_schedule.should_repay_interest ,0) as should_repay_interest,
  nvl(repay_schedule.should_repay_cost     ,0) as should_repay_cost,

  nvl(repay_detail.paid_amount             ,0) as paid_amount,
  nvl(repay_detail.paid_principal          ,0) as paid_principal,
  nvl(repay_detail.paid_interest           ,0) as paid_interest,
  nvl(repay_detail.paid_cost               ,0) as paid_cost,

  nvl(repay_detail.overdue_paid_amount     ,0) as overdue_paid_amount,
  nvl(repay_detail.overdue_paid_principal  ,0) as overdue_paid_principa,
  nvl(repay_detail.overdue_paid_interest   ,0) as overdue_paid_interest,
  nvl(repay_detail.overdue_paid_cost       ,0) as overdue_paid_cost,

  nvl(repay_detail.prepayment_amount       ,0) as prepayment_amount,
  nvl(repay_detail.prepayment_principal    ,0) as prepayment_principal,
  nvl(repay_detail.prepayment_interest     ,0) as prepayment_interest,
  nvl(repay_detail.prepayment_cost         ,0) as prepayment_cost,

  nvl(repay_detail.normal_paid_amount      ,0) as normal_paid_amount,
  nvl(repay_detail.normal_paid_principal   ,0) as normal_paid_principal,
  nvl(repay_detail.normal_paid_interest    ,0) as normal_paid_interest,
  nvl(repay_detail.normal_paid_cost        ,0) as normal_paid_cost,

  0                                            as pmml_should_repayamount,
  0                                            as pmml_should_repayprincipal,
  0                                            as pmml_should_repayinterest,

  0                                            as pmml_paid_amount,
  0                                            as pmml_paid_principal,
  0                                            as pmml_paid_interest,

  biz_dates.collect_date                       as collect_date,

  biz_dates.biz_date                           as biz_date,
  biz_dates.project_id                         as project_id,
  biz_dates.bag_id                             as bag_id
from (
  select
    project_id,
    bag_id,
    bag_date,
    date_add(bag_date,pos) as collect_date,
    '${ST9}'               as biz_date
  from (
    select
      bag_info.bag_id,
      bag_info.project_id,
      bag_info.bag_date,
      repay_schedule.should_repay_date_max
    from (
      select
        project_id,
        due_bill_no,
        max(should_repay_date) as should_repay_date_max
      from ods.repay_schedule_abs
      where '${ST9}' between s_d_date and date_sub(e_d_date,1)
      group by project_id,due_bill_no
    ) as repay_schedule
    join dim.bag_due_bill_no as bag_due
    on  repay_schedule.project_id  = bag_due.project_id
    and repay_schedule.due_bill_no = bag_due.due_bill_no
    join dim.bag_info as bag_info
    on  bag_due.project_id = bag_info.project_id
    and bag_due.bag_id     = bag_info.bag_id
  ) as tmp
  lateral view posexplode(split(space(datediff(should_repay_date_max,bag_date)),' ')) tf as pos,val
) as biz_dates
left join (
  select
    schedule.project_id,
    bag_due.bag_id,
    schedule.should_repay_date,
    sum(nvl(schedule.should_repay_amount,0))    as should_repay_amount,
    sum(nvl(schedule.should_repay_principal,0)) as should_repay_principal,
    sum(nvl(schedule.should_repay_interest,0))  as should_repay_interest,
    sum(nvl(schedule.should_repay_term_fee,0) + nvl(schedule.should_repay_svc_fee,0) + nvl(schedule.should_repay_penalty,0) + nvl(schedule.should_repay_mult_amt,0)) as should_repay_cost
  from ods.repay_schedule_abs as schedule
  join dim.bag_due_bill_no as bag_due
  on  schedule.project_id  = bag_due.project_id
  and schedule.due_bill_no = bag_due.due_bill_no
  where '${ST9}' between s_d_date and date_sub(e_d_date,1)
    and if(paid_out_type_cn is NULL,'A',paid_out_type_cn) != '提前结清'
  group by schedule.project_id,bag_due.bag_id,schedule.should_repay_date
) as repay_schedule
on  biz_dates.project_id   = repay_schedule.project_id
and biz_dates.bag_id       = repay_schedule.bag_id
and biz_dates.collect_date = repay_schedule.should_repay_date
left join (
  select
    repay_detail.project_id,
    bag_due.bag_id,
    repay_detail.biz_date,

    sum(repay_detail.paid_amount)            as paid_amount,
    sum(repay_detail.paid_principal)         as paid_principal,
    sum(repay_detail.paid_interest)          as paid_interest,
    sum(repay_detail.paid_cost)              as paid_cost,

    sum(repay_detail.normal_paid_amount)     as normal_paid_amount,
    sum(repay_detail.normal_paid_principal)  as normal_paid_principal,
    sum(repay_detail.normal_paid_interest)   as normal_paid_interest,
    sum(repay_detail.normal_paid_cost)       as normal_paid_cost,

    sum(repay_detail.prepayment_amount)      as prepayment_amount,
    sum(repay_detail.prepayment_principal)   as prepayment_principal,
    sum(repay_detail.prepayment_interest)    as prepayment_interest,
    sum(repay_detail.prepayment_cost)        as prepayment_cost,

    sum(repay_detail.overdue_paid_amount)    as overdue_paid_amount,
    sum(repay_detail.overdue_paid_principal) as overdue_paid_principal,
    sum(repay_detail.overdue_paid_interest)  as overdue_paid_interest,
    sum(repay_detail.overdue_paid_cost)      as overdue_paid_cost
  from dim.bag_due_bill_no as bag_due
  join (
    select
      project_id                                                                                                        as project_id,
      due_bill_no                                                                                                       as due_bill_no,
      max(biz_date)                                                                                                     as biz_date,

      sum(nvl(repay_amount,0))                                                                                          as paid_amount,
      sum(nvl(if(bnp_type = 'Pricinpal',repay_amount,0),0))                                                             as paid_principal,
      sum(nvl(if(bnp_type = 'Interest',repay_amount,0),0))                                                              as paid_interest,
      sum(nvl(if(bnp_type != 'Pricinpal' and bnp_type != 'Interest',repay_amount,0),0))                                 as paid_cost,

      sum(nvl(if(repay_type_cn = '正常还款',repay_amount,0),0))                                                         as normal_paid_amount,
      sum(nvl(if(repay_type_cn = '正常还款' and bnp_type = 'Pricinpal',repay_amount,0),0))                              as normal_paid_principal,
      sum(nvl(if(repay_type_cn = '正常还款' and bnp_type = 'Interest',repay_amount,0),0))                               as normal_paid_interest,
      sum(nvl(if(repay_type_cn = '正常还款' and bnp_type != 'Pricinpal' and bnp_type != 'Interest',repay_amount,0),0))  as normal_paid_cost,

      sum(nvl(if(repay_type_cn = '提前还款',repay_amount,0),0))                                                         as prepayment_amount,
      sum(nvl(if(repay_type_cn = '提前还款' and bnp_type = 'Pricinpal',repay_amount,0),0))                              as prepayment_principal,
      sum(nvl(if(repay_type_cn = '提前还款' and bnp_type = 'Interest',repay_amount,0),0))                               as prepayment_interest,
      sum(nvl(if(repay_type_cn = '提前还款' and bnp_type != 'Pricinpal' and bnp_type != 'Interest',repay_amount,0),0))  as prepayment_cost,

      sum(nvl(if(repay_type_cn = '逾期还款',repay_amount,0),0))                                                         as overdue_paid_amount,
      sum(nvl(if(repay_type_cn = '逾期还款' and bnp_type = 'Pricinpal',repay_amount,0),0))                              as overdue_paid_principal,
      sum(nvl(if(repay_type_cn = '逾期还款' and bnp_type = 'Interest',repay_amount,0),0))                               as overdue_paid_interest,
      sum(nvl(if(repay_type_cn = '逾期还款' and bnp_type != 'Pricinpal' and bnp_type != 'Interest',repay_amount,0),0))  as overdue_paid_cost
    from ods.repay_detail_abs
    where biz_date <= '${ST9}'
    group by project_id,due_bill_no
  ) as repay_detail
  on  bag_due.project_id  = repay_detail.project_id
  and bag_due.due_bill_no = repay_detail.due_bill_no
  group by repay_detail.project_id,bag_due.bag_id,repay_detail.biz_date
) as repay_detail
on  biz_dates.project_id   = repay_detail.project_id
and biz_dates.bag_id       = repay_detail.bag_id
and biz_dates.collect_date = repay_detail.biz_date
left join (
  select distinct
    bag_due.project_id,
    bag_due.bag_id,
    repay_detail.biz_date
  from ods.repay_detail_abs as repay_detail
  join dim.bag_due_bill_no as bag_due
  on  repay_detail.project_id  = bag_due.project_id
  and repay_detail.due_bill_no = bag_due.due_bill_no
) as repay_date_max
on  biz_dates.project_id   = repay_date_max.project_id
and biz_dates.bag_id       = repay_date_max.bag_id
and biz_dates.collect_date = repay_date_max.biz_date
-- limit 10
;

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
-- 关闭自动 MapJoin
set hive.auto.convert.join=false;
set hive.auto.convert.join.noconditionaltask=false;



-- set hivevar:ST9=2021-05-18;
-- set hivevar:project_id='CL202104080105';
-- set hivevar:bag_id='CL202104080105_1';

-- set hivevar:date_extr=date_sub(next_day(current_date,'Sun'),8);

-- set hivevar:project_id=
--   select distinct project_id
--   from dim.project_info
--   where 1 > 0
--     and project_id != 'PL202105120104'
-- ;
-- set hivevar:bag_id=
--   select distinct bag_id
--   from dim.bag_info
--   where 1 > 0
--     and project_id != 'PL202105120104'
-- ;



insert overwrite table dm_eagle.abs_asset_information_cash_flow_bag_day partition(biz_date,project_id,bag_id)
-- 单个包维度
select
  repay_schedule.bag_date                           as bag_date,
  repay_detail.max_biz_date                         as data_extraction_day,

  nvl(repay_schedule.remain_principal_term_begin,0) as remain_principal_term_begin,
  nvl(repay_schedule.remain_principal_term_end  ,0) as remain_principal_term_end,

  nvl(repay_schedule.should_repay_amount   ,0)      as should_repay_amount,
  nvl(repay_schedule.should_repay_principal,0)      as should_repay_principa,
  nvl(repay_schedule.should_repay_interest ,0)      as should_repay_interest,
  nvl(repay_schedule.should_repay_cost     ,0)      as should_repay_cost,

  nvl(repay_detail.paid_amount             ,0)      as paid_amount,
  nvl(repay_detail.paid_principal          ,0)      as paid_principal,
  nvl(repay_detail.paid_interest           ,0)      as paid_interest,
  nvl(repay_detail.paid_cost               ,0)      as paid_cost,

  nvl(repay_detail.overdue_paid_amount     ,0)      as overdue_paid_amount,
  nvl(repay_detail.overdue_paid_principal  ,0)      as overdue_paid_principa,
  nvl(repay_detail.overdue_paid_interest   ,0)      as overdue_paid_interest,
  nvl(repay_detail.overdue_paid_cost       ,0)      as overdue_paid_cost,

  nvl(repay_detail.prepayment_amount       ,0)      as prepayment_amount,
  nvl(repay_detail.prepayment_principal    ,0)      as prepayment_principal,
  nvl(repay_detail.prepayment_interest     ,0)      as prepayment_interest,
  nvl(repay_detail.prepayment_cost         ,0)      as prepayment_cost,

  nvl(repay_detail.normal_paid_amount      ,0)      as normal_paid_amount,
  nvl(repay_detail.normal_paid_principal   ,0)      as normal_paid_principal,
  nvl(repay_detail.normal_paid_interest    ,0)      as normal_paid_interest,
  nvl(repay_detail.normal_paid_cost        ,0)      as normal_paid_cost,

  0                                                 as pmml_should_repayamount,
  0                                                 as pmml_should_repayprincipal,
  0                                                 as pmml_should_repayinterest,

  0                                                 as pmml_paid_amount,
  0                                                 as pmml_paid_principal,
  0                                                 as pmml_paid_interest,

  repay_schedule.collect_date                       as collect_date,

  repay_schedule.biz_date                           as biz_date,
  repay_schedule.project_id                         as project_id,
  repay_schedule.bag_id                             as bag_id
from (
  select
    bag_date,

    sum(should_repay_principal)      over(partition by project_id,bag_id order by collect_date desc) as remain_principal_term_begin,
    sum(should_repay_principal_next) over(partition by project_id,bag_id order by collect_date desc) as remain_principal_term_end,

    should_repay_principal_next,
    should_repay_date,
    should_repay_amount,
    should_repay_principal,
    should_repay_interest,
    should_repay_cost,

    collect_date,
    biz_date,
    project_id,
    bag_id
  from (
    select
      biz_dates.bag_date,

      nvl(lead(repay_schedule.should_repay_principal) over(partition by biz_dates.project_id,biz_dates.bag_id order by biz_dates.collect_date),0) as should_repay_principal_next,
      repay_schedule.should_repay_date,
      repay_schedule.should_repay_amount,
      repay_schedule.should_repay_principal,
      repay_schedule.should_repay_interest,
      repay_schedule.should_repay_cost,

      biz_dates.collect_date,
      biz_dates.biz_date,
      biz_dates.project_id,
      biz_dates.bag_id
    from (
      select
        project_id,
        bag_id,
        bag_date,
        date_add(bag_date,pos) as collect_date,
        '${ST9}'               as biz_date
      from (
        select
          bag_info.project_id,
          bag_info.bag_id,
          bag_info.bag_date,
          max(repay_schedule.should_repay_date) as should_repay_date_max
        from (
          select
            project_id,
            bag_date,
            bag_id
          from dim.bag_info
          where 1 > 0
            and project_id in (${project_id})
        ) as bag_info
        join (
          select
            project_id,
            due_bill_no,
            bag_id
          from dim.bag_due_bill_no
          where 1 > 0
            and bag_id in (${bag_id})
        ) as bag_due
        on  bag_info.project_id = bag_due.project_id
        and bag_info.bag_id     = bag_due.bag_id
        join (
          select
            project_id,
            due_bill_no,
            should_repay_date
          from ods.repay_schedule_abs
          where 1 > 0
            and '${ST9}' between s_d_date and date_sub(e_d_date,1)
            and project_id in (${project_id})
        ) as repay_schedule
        on  bag_due.project_id  = repay_schedule.project_id
        and bag_due.due_bill_no = repay_schedule.due_bill_no
        group by bag_info.project_id,bag_info.bag_id,bag_info.bag_date
      ) as tmp
      lateral view posexplode(split(space(datediff(should_repay_date_max,bag_date)),' ')) tf as pos,val
    ) as biz_dates
    left join (
      select
        schedule.project_id,
        schedule.should_repay_date,
        bag_due.bag_id,
        sum(nvl(schedule.should_repay_amount,0))    as should_repay_amount,
        sum(nvl(schedule.should_repay_principal,0)) as should_repay_principal,
        sum(nvl(schedule.should_repay_interest,0))  as should_repay_interest,
        sum(nvl(schedule.should_repay_term_fee,0) + nvl(schedule.should_repay_svc_fee,0) + nvl(schedule.should_repay_penalty,0) + nvl(schedule.should_repay_mult_amt,0)) as should_repay_cost
      from (
        select *
        from ods.repay_schedule_abs
        where 1 > 0
          and project_id in (${project_id})
          and '${ST9}' between s_d_date and date_sub(e_d_date,1)
          and if(paid_out_type_cn is NULL,'A',paid_out_type_cn) != '提前结清'
      ) as schedule
      join (
        select
          project_id,
          due_bill_no,
          bag_id
        from dim.bag_due_bill_no
        where 1 > 0
          and bag_id in (${bag_id})
      ) as bag_due
      on  schedule.project_id  = bag_due.project_id
      and schedule.due_bill_no = bag_due.due_bill_no
      group by schedule.project_id,schedule.should_repay_date,bag_due.bag_id
    ) as repay_schedule
    on  biz_dates.project_id   = repay_schedule.project_id
    and biz_dates.bag_id       = repay_schedule.bag_id
    and biz_dates.collect_date = repay_schedule.should_repay_date
  ) as tmp
) as repay_schedule
left join (
  select
    repay_detail.project_id,
    bag_due.bag_id,
    repay_detail.biz_date,

    -- max(repay_detail.max_biz_date)           as max_biz_date,
    max(repay_detail.biz_date) over(partition by repay_detail.project_id,bag_due.bag_id) as max_biz_date,

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
  from (
    select
      project_id,
      due_bill_no,
      bag_id
    from dim.bag_due_bill_no
    where 1 > 0
      and bag_id in (${bag_id})
  ) as bag_due
  join (
    select
      project_id                                                                                                        as project_id,
      due_bill_no                                                                                                       as due_bill_no,
      biz_date                                                                                                          as biz_date,
      -- max(biz_date) over(partition by project_id,due_bill_no)                                                           as max_biz_date,
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
    where 1 > 0
      and biz_date <= '${ST9}'
      and project_id in (${project_id})
    group by project_id,due_bill_no,biz_date -- group by 中要有 repay_term 取每一期中的实还日最大值
  ) as repay_detail
  on  bag_due.project_id  = repay_detail.project_id
  and bag_due.due_bill_no = repay_detail.due_bill_no
  group by repay_detail.project_id,bag_due.bag_id,repay_detail.biz_date
) as repay_detail
on  repay_schedule.project_id   = repay_detail.project_id
and repay_schedule.bag_id       = repay_detail.bag_id
and repay_schedule.collect_date = repay_detail.biz_date
union all
-- 所有包维度
select
  repay_schedule.bag_date                           as bag_date,
  repay_detail.max_biz_date                         as data_extraction_day,

  nvl(repay_schedule.remain_principal_term_begin,0) as remain_principal_term_begin,
  nvl(repay_schedule.remain_principal_term_end  ,0) as remain_principal_term_end,

  nvl(repay_schedule.should_repay_amount   ,0)      as should_repay_amount,
  nvl(repay_schedule.should_repay_principal,0)      as should_repay_principa,
  nvl(repay_schedule.should_repay_interest ,0)      as should_repay_interest,
  nvl(repay_schedule.should_repay_cost     ,0)      as should_repay_cost,

  nvl(repay_detail.paid_amount             ,0)      as paid_amount,
  nvl(repay_detail.paid_principal          ,0)      as paid_principal,
  nvl(repay_detail.paid_interest           ,0)      as paid_interest,
  nvl(repay_detail.paid_cost               ,0)      as paid_cost,

  nvl(repay_detail.overdue_paid_amount     ,0)      as overdue_paid_amount,
  nvl(repay_detail.overdue_paid_principal  ,0)      as overdue_paid_principa,
  nvl(repay_detail.overdue_paid_interest   ,0)      as overdue_paid_interest,
  nvl(repay_detail.overdue_paid_cost       ,0)      as overdue_paid_cost,

  nvl(repay_detail.prepayment_amount       ,0)      as prepayment_amount,
  nvl(repay_detail.prepayment_principal    ,0)      as prepayment_principal,
  nvl(repay_detail.prepayment_interest     ,0)      as prepayment_interest,
  nvl(repay_detail.prepayment_cost         ,0)      as prepayment_cost,

  nvl(repay_detail.normal_paid_amount      ,0)      as normal_paid_amount,
  nvl(repay_detail.normal_paid_principal   ,0)      as normal_paid_principal,
  nvl(repay_detail.normal_paid_interest    ,0)      as normal_paid_interest,
  nvl(repay_detail.normal_paid_cost        ,0)      as normal_paid_cost,

  0                                                 as pmml_should_repayamount,
  0                                                 as pmml_should_repayprincipal,
  0                                                 as pmml_should_repayinterest,

  0                                                 as pmml_paid_amount,
  0                                                 as pmml_paid_principal,
  0                                                 as pmml_paid_interest,

  repay_schedule.collect_date                       as collect_date,

  repay_schedule.biz_date                           as biz_date,
  repay_schedule.project_id                         as project_id,
  'default_all_bag'                                 as bag_id
from (
  select
    bag_date,

    sum(should_repay_principal)      over(partition by project_id order by collect_date desc) as remain_principal_term_begin,
    sum(should_repay_principal_next) over(partition by project_id order by collect_date desc) as remain_principal_term_end,

    should_repay_principal_next,
    should_repay_date,
    should_repay_amount,
    should_repay_principal,
    should_repay_interest,
    should_repay_cost,

    collect_date,
    biz_date,
    project_id
  from (
    select
      biz_dates.bag_date,

      nvl(lead(repay_schedule.should_repay_principal) over(partition by biz_dates.project_id order by biz_dates.collect_date),0) as should_repay_principal_next,
      repay_schedule.should_repay_date,
      repay_schedule.should_repay_amount,
      repay_schedule.should_repay_principal,
      repay_schedule.should_repay_interest,
      repay_schedule.should_repay_cost,

      biz_dates.collect_date,
      biz_dates.biz_date,
      biz_dates.project_id
    from (
      select
        project_id,
        bag_date,
        date_add(bag_date,pos) as collect_date,
        '${ST9}'               as biz_date
      from (
        select
          bag_info.project_id,
          min(bag_info.bag_date)                as bag_date,
          max(repay_schedule.should_repay_date) as should_repay_date_max
        from (
          select
            project_id,
            bag_date,
            bag_id
          from dim.bag_info
          where 1 > 0
            and project_id in (${project_id})
        ) as bag_info
        join (
          select
            project_id,
            due_bill_no,
            bag_id
          from dim.bag_due_bill_no
          where 1 > 0
            and bag_id in (${bag_id})
        ) as bag_due
        on  bag_info.project_id = bag_due.project_id
        and bag_info.bag_id     = bag_due.bag_id
        join (
          select
            project_id,
            due_bill_no,
            should_repay_date
          from ods.repay_schedule_abs
          where 1 > 0
            and '${ST9}' between s_d_date and date_sub(e_d_date,1)
            and project_id in (${project_id})
        ) as repay_schedule
        on  bag_due.project_id  = repay_schedule.project_id
        and bag_due.due_bill_no = repay_schedule.due_bill_no
        group by bag_info.project_id
      ) as tmp
      lateral view posexplode(split(space(datediff(should_repay_date_max,bag_date)),' ')) tf as pos,val
    ) as biz_dates
    left join (
      select
        schedule.project_id,
        schedule.should_repay_date,
        sum(nvl(schedule.should_repay_amount,0))    as should_repay_amount,
        sum(nvl(schedule.should_repay_principal,0)) as should_repay_principal,
        sum(nvl(schedule.should_repay_interest,0))  as should_repay_interest,
        sum(nvl(schedule.should_repay_term_fee,0) + nvl(schedule.should_repay_svc_fee,0) + nvl(schedule.should_repay_penalty,0) + nvl(schedule.should_repay_mult_amt,0)) as should_repay_cost
      from (
        select *
        from ods.repay_schedule_abs
        where 1 > 0
          and project_id in (${project_id})
          and if(paid_out_type_cn is NULL,'A',paid_out_type_cn) != '提前结清'
      ) as schedule
      join (
        select
          project_id,
          due_bill_no
        from dim.bag_due_bill_no
        where 1 > 0
          and bag_id in (${bag_id})
      ) as bag_due
      on  schedule.project_id  = bag_due.project_id
      and schedule.due_bill_no = bag_due.due_bill_no
      where '${ST9}' between s_d_date and date_sub(e_d_date,1)
      group by schedule.project_id,schedule.should_repay_date
    ) as repay_schedule
    on  biz_dates.project_id   = repay_schedule.project_id
    and biz_dates.collect_date = repay_schedule.should_repay_date
  ) as tmp
) as repay_schedule
left join (
  select
    repay_detail.project_id,
    repay_detail.biz_date,

    -- max(max_biz_date)                        as max_biz_date,
    max(repay_detail.biz_date) over(partition by repay_detail.project_id) as max_biz_date,

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
  from (
    select
      project_id,
      due_bill_no
    from dim.bag_due_bill_no
    where 1 > 0
      and bag_id in (${bag_id})
  ) as bag_due
  join (
    select
      project_id                                                                                                        as project_id,
      due_bill_no                                                                                                       as due_bill_no,
      biz_date                                                                                                          as biz_date,

      -- max(biz_date) over (partition by project_id,due_bill_no)                                                          as max_biz_date,

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
    where 1 > 0
      and biz_date <= '${ST9}'
      and project_id in (${project_id})
    group by project_id,due_bill_no,biz_date -- group by 中要有 repay_term 取每一期中的实还日最大值
  ) as repay_detail
  on  bag_due.project_id  = repay_detail.project_id
  and bag_due.due_bill_no = repay_detail.due_bill_no
  group by repay_detail.project_id,repay_detail.biz_date
) as repay_detail
on  repay_schedule.project_id   = repay_detail.project_id
and repay_schedule.collect_date = repay_detail.biz_date
union all
-- 项目维度
select
  repay_schedule.bag_date                                     as bag_date,
  repay_detail.max_biz_date                                   as data_extraction_day,

  nvl(repay_schedule.remain_principal_term_begin,0)           as remain_principal_term_begin,
  nvl(repay_schedule.remain_principal_term_end  ,0)           as remain_principal_term_end,

  nvl(repay_schedule.should_repay_amount   ,0)                as should_repay_amount,
  nvl(repay_schedule.should_repay_principal,0)                as should_repay_principa,
  nvl(repay_schedule.should_repay_interest ,0)                as should_repay_interest,
  nvl(repay_schedule.should_repay_cost     ,0)                as should_repay_cost,

  nvl(repay_detail.paid_amount             ,0)                as paid_amount,
  nvl(repay_detail.paid_principal          ,0)                as paid_principal,
  nvl(repay_detail.paid_interest           ,0)                as paid_interest,
  nvl(repay_detail.paid_cost               ,0)                as paid_cost,

  nvl(repay_detail.overdue_paid_amount     ,0)                as overdue_paid_amount,
  nvl(repay_detail.overdue_paid_principal  ,0)                as overdue_paid_principa,
  nvl(repay_detail.overdue_paid_interest   ,0)                as overdue_paid_interest,
  nvl(repay_detail.overdue_paid_cost       ,0)                as overdue_paid_cost,

  nvl(repay_detail.prepayment_amount       ,0)                as prepayment_amount,
  nvl(repay_detail.prepayment_principal    ,0)                as prepayment_principal,
  nvl(repay_detail.prepayment_interest     ,0)                as prepayment_interest,
  nvl(repay_detail.prepayment_cost         ,0)                as prepayment_cost,

  nvl(repay_detail.normal_paid_amount      ,0)                as normal_paid_amount,
  nvl(repay_detail.normal_paid_principal   ,0)                as normal_paid_principal,
  nvl(repay_detail.normal_paid_interest    ,0)                as normal_paid_interest,
  nvl(repay_detail.normal_paid_cost        ,0)                as normal_paid_cost,

  nvl(pmml_should_paid.pmml_should_repay_amount,0)            as pmml_should_repayamount,
  nvl(pmml_should_paid.pmml_should_repay_principal,0)         as pmml_should_repayprincipal,
  nvl(pmml_should_paid.pmml_should_repay_interest,0)          as pmml_should_repayinterest,

  nvl(pmml_should_paid.pmml_paid_amount,0)                    as pmml_paid_amount,
  nvl(pmml_should_paid.pmml_paid_principal,0)                 as pmml_paid_principal,
  nvl(pmml_should_paid.pmml_paid_interest,0)                  as pmml_paid_interest,

  nvl(repay_schedule.collect_date,to_date(pmml_collect_date)) as collect_date,

  nvl(repay_schedule.biz_date,pmml_biz_date)                  as biz_date,
  nvl(repay_schedule.project_id,pmml_project_id)              as project_id,
  'default_project'                                           as bag_id
from (
  select
    bag_date,

    sum(should_repay_principal)      over(partition by project_id order by collect_date desc) as remain_principal_term_begin,
    sum(should_repay_principal_next) over(partition by project_id order by collect_date desc) as remain_principal_term_end,

    should_repay_principal_next,
    should_repay_date,
    should_repay_amount,
    should_repay_principal,
    should_repay_interest,
    should_repay_cost,

    collect_date,
    biz_date,
    project_id
  from (
    select
      biz_dates.bag_date,

      nvl(lead(repay_schedule.should_repay_principal) over(partition by biz_dates.project_id order by biz_dates.collect_date),0) as should_repay_principal_next,
      repay_schedule.should_repay_date,
      repay_schedule.should_repay_amount,
      repay_schedule.should_repay_principal,
      repay_schedule.should_repay_interest,
      repay_schedule.should_repay_cost,

      biz_dates.collect_date,
      biz_dates.biz_date,
      biz_dates.project_id
    from (
      select
        project_id                     as project_id,
        loan_active_date               as bag_date,
        date_add(loan_active_date,pos) as collect_date,
        '${ST9}'                       as biz_date
      from (
        select
          project_id,
          min(loan_active_date)  as loan_active_date,
          max(should_repay_date) as should_repay_date
        from ods.repay_schedule_abs
        where 1 > 0
          and '${ST9}' between s_d_date and date_sub(e_d_date,1)
          and project_id in (${project_id})
        group by project_id
      ) as tmp
      lateral view posexplode(split(space(datediff(should_repay_date,loan_active_date)),' ')) tf as pos,val
    ) as biz_dates
    left join (
      select
        schedule.project_id,
        schedule.should_repay_date,
        sum(nvl(schedule.should_repay_amount,0))    as should_repay_amount,
        sum(nvl(schedule.should_repay_principal,0)) as should_repay_principal,
        sum(nvl(schedule.should_repay_interest,0))  as should_repay_interest,
        sum(nvl(schedule.should_repay_term_fee,0) + nvl(schedule.should_repay_svc_fee,0) + nvl(schedule.should_repay_penalty,0) + nvl(schedule.should_repay_mult_amt,0)) as should_repay_cost
      from ods.repay_schedule_abs as schedule
      where 1 > 0
        and '${ST9}' between s_d_date and date_sub(e_d_date,1)
        and project_id in (${project_id})
        and if(paid_out_type_cn is NULL,'A',paid_out_type_cn) != '提前结清'
      group by schedule.project_id,schedule.should_repay_date
    ) as repay_schedule
    on  biz_dates.project_id   = repay_schedule.project_id
    and biz_dates.collect_date = repay_schedule.should_repay_date
  ) as tmp
) as repay_schedule
left join (
  select
    project_id                                                                                                        as project_id,
    biz_date                                                                                                          as biz_date,

    max(biz_date) over (partition by project_id)                                                                      as max_biz_date,

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
  where 1 > 0
    and biz_date <= '${ST9}'
    and project_id in (${project_id})
  group by project_id ,biz_date-- group by 中要有 repay_term 取每一期中的实还日最大值
) as repay_detail
on  repay_schedule.project_id   = repay_detail.project_id
and repay_schedule.collect_date = repay_detail.biz_date
full join (
  select
    '${ST9}'                                                         as pmml_biz_date,
    nvl(pmml_should_repay.project_id,pmml_paid.project_id)           as pmml_project_id,
    nvl(pmml_should_repay.should_repay_amount,0)                     as pmml_should_repay_amount,
    nvl(pmml_should_repay.should_repay_principal,0)                  as pmml_should_repay_principal,
    nvl(pmml_should_repay.should_repay_interest,0)                   as pmml_should_repay_interest,
    nvl(pmml_paid.paid_amount,0)                                     as pmml_paid_amount,
    nvl(pmml_paid.paid_principal,0)                                  as pmml_paid_principal,
    nvl(pmml_paid.paid_interest,0)                                   as pmml_paid_interest,
    nvl(pmml_should_repay.should_repay_date,pmml_paid.paid_out_date) as pmml_collect_date
  from (
    select
      project_id,should_repay_date,
      sum(nvl(should_repay_amount,0))    as should_repay_amount,
      sum(nvl(should_repay_principal,0)) as should_repay_principal,
      sum(nvl(should_repay_interest,0))  as should_repay_interest
    from eagle.predict_repay_day
    where 1 > 0
      and biz_date = ${date_extr}
      and cycle_key = '0'
      and project_id in (${project_id})
      and project_id = 'CL202011090089'
    group by project_id,should_repay_date
  ) as pmml_should_repay
  full join (
    select
      project_id,paid_out_date,
      sum(nvl(paid_amount,0))    as paid_amount,
      sum(nvl(paid_principal,0)) as paid_principal,
      sum(nvl(paid_interest,0))  as paid_interest
    from eagle.predict_repay_day
    where 1 > 0
      and biz_date = ${date_extr}
      and cycle_key = '0'
      and project_id in (${project_id})
      and project_id = 'CL202011090089'
      and is_empty(paid_out_date) is not null
    group by project_id,paid_out_date
  ) as pmml_paid
  on  pmml_should_repay.project_id        = pmml_paid.project_id
  and pmml_should_repay.should_repay_date = pmml_paid.paid_out_date
) as pmml_should_paid
on  repay_schedule.project_id   = pmml_should_paid.pmml_project_id
and repay_schedule.collect_date = pmml_should_paid.pmml_collect_date
-- limit 10
;

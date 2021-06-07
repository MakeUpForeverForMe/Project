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


set hivevar:ST9=2021-05-10;
set hivevar:date_extr=2021-04-14;
set hivevar:project_id=WS0006200001;

insert overwrite table dm_eagle.abs_asset_information_cash_flow_bag_day partition(biz_date,project_id,bag_id)
select
  nvl(bag_date,pmml_bag_date)         as bag_date,
  data_extraction_day                 as data_extraction_day,

  nvl(should_repay_amount,0)          as should_repay_amount,
  nvl(should_repay_principal,0)       as should_repay_principal,
  nvl(should_repay_interest,0)        as should_repay_interest,
  nvl(should_repay_cost,0)            as should_repay_cost,

  nvl(paid_amount,0)                  as paid_amount,
  nvl(paid_principal,0)               as paid_principal,
  nvl(paid_interest,0)                as paid_interest,
  nvl(paid_cost,0)                    as paid_cost,

  0                                   as overdue_paid_amount,
  0                                   as overdue_paid_principal,
  0                                   as overdue_paid_interest,
  0                                   as overdue_paid_cost,

  0                                   as prepayment_amount,
  0                                   as prepayment_principal,
  0                                   as prepayment_interest,
  0                                   as prepayment_cost,

  0                                   as normal_paid_amount,
  0                                   as normal_paid_principal,
  0                                   as normal_paid_interest,
  0                                   as normal_paid_cost,

  nvl(pmml_should_repay_amount,0)     as pmml_should_repayamount,
  nvl(pmml_should_repay_principal,0)  as pmml_should_repayprincipal,
  nvl(pmml_should_repay_interest,0)   as pmml_should_repayinterest,

  nvl(pmml_paid_amount,0)             as pmml_paid_amount,
  nvl(pmml_paid_principal,0)          as pmml_paid_principal,
  nvl(pmml_paid_interest,0)           as pmml_paid_interest,

  nvl(collect_date,pmml_collect_date) as collect_date,
  '${ST9}'                            as biz_date,
  nvl(project_id,pmml_project_id)     as project_id,
  "default_project"                   as bag_id
from (
  select
    '2020-06-02'                                           as bag_date,
    nvl(should_repay.project_id,repay_hst.project_id)      as project_id,
    nvl(should_repay.should_repay_amount,0)                as should_repay_amount,
    nvl(should_repay.should_repay_principal,0)             as should_repay_principal,
    nvl(should_repay.should_repay_interest,0)              as should_repay_interest,
    nvl(should_repay.should_repay_cost,0)                  as should_repay_cost,
    nvl(repay_hst.max_biz_date,null)                       as data_extraction_day,
    nvl(repay_hst.paid_amount,0)                           as paid_amount,
    nvl(repay_hst.paid_principal,0)                        as paid_principal,
    nvl(repay_hst.paid_interest,0)                         as paid_interest,
    nvl(repay_hst.paid_cost,0)                             as paid_cost,
    nvl(should_repay.should_repay_date,repay_hst.biz_date) as collect_date
  from (
    -- 应收
    select
      project_id                         as project_id,
      should_repay_date                  as should_repay_date,
      sum(nvl(should_repay_amount,0))    as should_repay_amount,
      sum(nvl(should_repay_principal,0)) as should_repay_principal,
      sum(nvl(should_repay_interest,0))  as should_repay_interest,
      sum(nvl(should_repay_cost,0))      as should_repay_cost
    from (
      select
        product_id                                                               as product_id,
        should_repay_date                                                        as should_repay_date,
        sum(should_repay_amount)                                                 as should_repay_amount,
        sum(should_repay_principal)                                              as should_repay_principal,
        sum(should_repay_interest)                                               as should_repay_interest,
        sum(should_repay_term_fee + should_repay_svc_fee + should_repay_penalty) as should_repay_cost
      from ods.repay_schedule
      where '${ST9}' between s_d_date and date_sub(e_d_date,1)
        and if(paid_out_type_cn is NULL,'A',paid_out_type_cn) != '提前结清'
      group by product_id,should_repay_date
    ) as schedule
    join (
      select
        max(if(col_name = 'channel_id',col_val,null)) as channel_id,
        max(if(col_name = 'project_id',col_val,null)) as project_id,
        max(if(col_name = 'product_id',col_val,null)) as product_id
      from dim.data_conf
      where col_type = 'ac'
      group by col_id
    ) as bizconf
    on schedule.product_id = bizconf.product_id
    where 1 > 0
      -- and bizconf.channel_id = '0006'
      and bizconf.project_id = '${project_id}'
    group by should_repay_date,project_id
  ) as should_repay
  full join  (
    -- 实收（及每个项目的最大实还时间）
    select
      biz_date                                    as biz_date,
      project_id                                  as project_id,
      max(biz_date) over(partition by project_id) as max_biz_date,
      sum(paid_amount)                            as paid_amount,
      sum(paid_principal)                         as paid_principal,
      sum(paid_interest)                          as paid_interest,
      sum(paid_cost)                              as paid_cost
    from (
      select
        biz_date                                                         as biz_date,
        product_id                                                       as product_id,
        sum(nvl(repay_amount,0))                                         as paid_amount,
        sum(if(bnp_type = 'Pricinpal',repay_amount,0))                   as paid_principal,
        sum(if(bnp_type = 'Interest',repay_amount,0))                    as paid_interest,
        sum(if(bnp_type not in ('Interest','Pricinpal'),repay_amount,0)) as paid_cost
      from ods.repay_detail
      where biz_date <= '${ST9}'
      group by biz_date,product_id
    ) as repay_hst
    join (
      select
        max(if(col_name = 'channel_id',col_val,null)) as channel_id,
        max(if(col_name = 'project_id',col_val,null)) as project_id,
        max(if(col_name = 'product_id',col_val,null)) as product_id
      from dim.data_conf
      where col_type = 'ac'
      group by col_id
    ) as bizconf
    on repay_hst.product_id = bizconf.product_id
    where 1 > 0
      -- and bizconf.channel_id = '0006'
      and bizconf.project_id = '${project_id}'
    group by biz_date,project_id
  ) as repay_hst
  on  should_repay.project_id        = repay_hst.project_id
  and should_repay.should_repay_date = repay_hst.biz_date
) as should_paid
full join (
  select
    '2020-06-02'                                                as pmml_bag_date,
    nvl(pmml_should_repay.project_id,pmml_paid.project_id)      as pmml_project_id,
    nvl(pmml_should_repay.should_repay_amount,0)                as pmml_should_repay_amount,
    nvl(pmml_should_repay.should_repay_principal,0)             as pmml_should_repay_principal,
    nvl(pmml_should_repay.should_repay_interest,0)              as pmml_should_repay_interest,
    nvl(pmml_paid.paid_amount,0)                                as pmml_paid_amount,
    nvl(pmml_paid.paid_principal,0)                             as pmml_paid_principal,
    nvl(pmml_paid.paid_interest,0)                              as pmml_paid_interest,
    nvl(pmml_should_repay.should_repay_date,pmml_paid.biz_date) as pmml_collect_date
  from (
    select
      project_id                         as project_id,
      should_repay_date                  as should_repay_date,
      sum(nvl(should_repay_amount,0))    as should_repay_amount,
      sum(nvl(should_repay_principal,0)) as should_repay_principal,
      sum(nvl(should_repay_interest,0))  as should_repay_interest
    from eagle.predict_repay_day
    where 1 > 0
      and biz_date = '${date_extr}'
      and project_id = '${project_id}'
      -- and biz_date = date_sub(next_day(current_date,'Sun'),8)
      and cycle_key = '0'
    group by project_id,should_repay_date
  ) as pmml_should_repay
  full join (
    select
      paid_out_date              as biz_date,
      project_id                 as project_id,
      sum(nvl(paid_amount,0))    as paid_amount,
      sum(nvl(paid_principal,0)) as paid_principal,
      sum(nvl(paid_interest,0))  as paid_interest
    from eagle.predict_repay_day
    where 1 > 0
      and biz_date = '${date_extr}'
      and project_id = '${project_id}'
      -- and biz_date = date_sub(next_day(current_date,'Sun'),8)
      and cycle_key = '0'
    group by project_id,paid_out_date
  ) as pmml_paid
  on  pmml_should_repay.project_id        = pmml_paid.project_id
  and pmml_should_repay.should_repay_date = pmml_paid.biz_date
) as pmml_should_paid
on  should_paid.project_id   = pmml_should_paid.pmml_project_id
and should_paid.collect_date = pmml_should_paid.pmml_collect_date
-- limit 10
;

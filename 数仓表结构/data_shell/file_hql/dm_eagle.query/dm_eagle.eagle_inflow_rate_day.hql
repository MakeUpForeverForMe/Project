set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.shuffle.memoryFraction=0.6;         -- shuffle操作的内存占比
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.auto.convert.join=false;             -- 关闭自动 MapJoin
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;


insert overwrite table dm_eagle${db_suffix}.eagle_inflow_rate_day partition(biz_date,product_id)
select
  capital_id                                               as capital_id,
  channel_id                                               as channel_id,
  project_id                                               as project_id,
  loan_terms                                               as loan_terms,
  overdue_dob                                              as overdue_dob,
  is_first                                                 as is_first,
  nvl(should_repay_loan_num,0)                             as should_repay_loan_num,
  overdue_loan_num                                         as overdue_loan_num,
  nvl(overdue_loan_num / should_repay_loan_num * 100,0)    as overdue_loan_inflow_rate,
  loan_active_month                                        as loan_active_month,
  should_repay_date                                        as should_repay_date,
  nvl(remain_principal,0)                                  as remain_principal,
  nvl(should_repay_principal,0)                            as should_repay_principal,
  overdue_principal                                        as overdue_principal,
  overdue_remain_principal                                 as overdue_remain_principal,
  nvl(overdue_principal / should_repay_principal * 100,0)  as overdue_principal_inflow_rate,
  nvl(overdue_remain_principal / remain_principal * 100,0) as remain_principal_inflow_rate,
  biz_date                                                 as biz_date,
  product_id                                               as product_id
from (
  select
    biz_conf.capital_id                                      as capital_id,
    biz_conf.channel_id                                      as channel_id,
    biz_conf.project_id                                      as project_id,
    biz_conf.product_id${vt}                                 as product_id,
    overdue_num_main.biz_date                                as biz_date,
    overdue_num_main.loan_terms                              as loan_terms,
    overdue_num_main.overdue_dob                             as overdue_dob,
    date_format(overdue_num_main.loan_active_date,'yyyy-MM') as loan_active_month,
    overdue_num_main.should_repay_date                       as should_repay_date,
    overdue_num_main.is_first                                as is_first,
    sum(nvl(overdue_loan_num,0))                             as overdue_loan_num,
    sum(nvl(overdue_principal,0))                            as overdue_principal,
    sum(nvl(overdue_remain_principal,0))                     as overdue_remain_principal
  from (
    select
      capital_id,
      channel_id,
      project_id,
      product_id,
      product_id_vt
    from dim_new.biz_conf
    where 1 > 0
      and product_id_vt is not null
      and (case when product_id = 'pl00282' and '${ST9}' > '2020-02-22' then 0 else 1 end) = 1
  ) as biz_conf
  join (
    select distinct
      biz_date,
      product_id,
      overdue_dob,
      loan_active_date,
      loan_terms,
      should_repay_date,
      is_first
    from dw_new${db_suffix}.dw_loan_base_stat_overdue_num_day
    lateral view explode(split(concat_ws(',',if(overdue_date_first = overdue_date_start,'y',null),'n'),',')) first as is_first -- 必须加的字段，维度上必须有的字段
    where 1 > 0
      and biz_date = '${ST9}'
      and overdue_mob <= 12
      and overdue_dob is not null
      and should_repay_date is not null
      and overdue_stage_previous = 0
      and overdue_stage_curr = 1
  ) as overdue_num_main
  on biz_conf.product_id = overdue_num_main.product_id
  left join ( -- 取的是正常的分子中的逾期金额、逾期借据剩余金额等，但是关联分母时会过滤掉部分分母的数据，所以要有 overdue_num_main 这个子查询
    select
      biz_date,
      product_id,
      overdue_dob,
      loan_active_date,
      loan_terms,
      should_repay_date,
      overdue_loan_num,
      overdue_principal,
      overdue_remain_principal,
      is_first
    from dw_new${db_suffix}.dw_loan_base_stat_overdue_num_day
    lateral view explode(split(concat_ws(',',if(overdue_date_first = overdue_date_start,'y',null),'n'),',')) first as is_first
    where 1 > 0
      and biz_date = '${ST9}'
      and overdue_dob <= 30
      and overdue_mob <= 12
      and overdue_days > 0
      and overdue_days <= 30
      and overdue_stage_previous = 0
      and overdue_stage_curr = 1
  ) as overdue_num_sum
  on  overdue_num_main.biz_date          = overdue_num_sum.biz_date
  and overdue_num_main.product_id        = overdue_num_sum.product_id
  and overdue_num_main.overdue_dob       = overdue_num_sum.overdue_dob
  and overdue_num_main.loan_active_date  = overdue_num_sum.loan_active_date
  and overdue_num_main.loan_terms        = overdue_num_sum.loan_terms
  and overdue_num_main.should_repay_date = overdue_num_sum.should_repay_date
  and overdue_num_main.is_first          = overdue_num_sum.is_first
  group by
    biz_conf.capital_id,
    biz_conf.channel_id,
    biz_conf.project_id,
    biz_conf.product_id${vt},
    overdue_num_main.biz_date,
    overdue_num_main.loan_terms,
    overdue_num_main.overdue_dob,
    date_format(overdue_num_main.loan_active_date,'yyyy-MM'),
    overdue_num_main.should_repay_date,
    overdue_num_main.is_first
  -- order by product_id,loan_active_month,loan_terms,overdue_dob,should_repay_date,is_first
) as molecular
left join (
  select
    biz_date                                as biz_date_remain,
    biz_conf.product_id${vt}                as product_id_remain,
    date_format(loan_active_date,'yyyy-MM') as loan_active_month_remain,
    loan_terms                              as loan_terms_remain,
    should_repay_date                       as should_repay_date_remain,
    sum(should_repay_loan_num_curr_actual)  as should_repay_loan_num,
    sum(remain_principal)                   as remain_principal,
    sum(should_repay_principal_curr_actual) as should_repay_principal
  from (
    select
      product_id,
      product_id_vt
    from dim_new.biz_conf
    where 1 > 0
      and product_id_vt is not null
      and (case when product_id = 'pl00282' and '${ST9}' > '2020-02-22' then 0 else 1 end) = 1
  ) as biz_conf
  join (
    select
      biz_date,
      product_id,
      loan_active_date,
      loan_terms,
      should_repay_date_curr as should_repay_date,
      should_repay_loan_num_curr_actual,
      remain_principal,
      should_repay_principal_curr_actual
    from dw_new${db_suffix}.dw_loan_base_stat_overdue_num_day
    where 1 > 0
      and biz_date = date_sub(should_repay_date_curr,1)
      and overdue_mob <= 12
      and overdue_days = 0
  ) overdue_num
  on biz_conf.product_id = overdue_num.product_id
  group by
    biz_date,
    biz_conf.product_id${vt},
    date_format(loan_active_date,'yyyy-MM'),
    loan_terms,
    should_repay_date
  -- order by product_id_remain,loan_active_month_remain,loan_terms_remain,should_repay_date_remain
) as denominator
on  date_sub(biz_date,cast(overdue_dob as int)) = biz_date_remain
and product_id        = product_id_remain
and loan_active_month = loan_active_month_remain
and loan_terms        = loan_terms_remain
and should_repay_date = should_repay_date_remain
-- order by should_repay_date,loan_active_month,product_id,loan_terms,is_first
-- limit 10
;

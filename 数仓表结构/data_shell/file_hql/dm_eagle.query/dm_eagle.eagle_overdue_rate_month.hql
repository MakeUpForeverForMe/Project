set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.maxRemoteBlockSizeFetchToMem=4G;
set spark.shuffle.memoryFraction=0.6;
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;


insert overwrite table dm_eagle${db_suffix}.eagle_overdue_rate_month partition (biz_date,product_id)
select
  nvl(capital_id,capital_id_once)                                as capital_id,
  nvl(channel_id,channel_id_once)                                as channel_id,
  nvl(project_id,project_id_once)                                as project_id,
  nvl(loan_terms,loan_terms_once)                                as loan_terms,
  nvl(dpd_once,0)                                                as dpd,
  nvl(mob,mob_once)                                              as mob,
  nvl(loan_month,loan_month_once)                                as loan_month,
  nvl(loan_principal,0)                                          as loan_principal,
  nvl(remain_principal_current,0)                                as remain_principal,
  nvl(overdue_principal_current,0)                               as overdue_principal,
  nvl(overdue_remain_principal_current,0)                        as overdue_remain_principal,
  nvl(overdue_remain_principal_current / loan_principal,0) * 100 as overdue_rate,
  nvl(overdue_principal_once,0)                                  as overdue_principal_once,
  nvl(overdue_remain_principal_once,0)                           as overdue_remain_principal_once,
  nvl(overdue_remain_principal_once / loan_principal,0) * 100    as overdue_rate_once,
  nvl(biz_date,biz_date_once)                                    as biz_date,
  product_id                                                     as product_id
from (
  select
    capital_id                      as capital_id,
    channel_id                      as channel_id,
    project_id                      as project_id,
    loan_terms                      as loan_terms,
    date_format(biz_date,'yyyy-MM') as loan_month,
    month('${ST9}') + (year('${ST9}') - year(biz_date)) * 12 - month(biz_date) as mob,
    sum(loan_principal)             as loan_principal,
    '${ST9}'                        as biz_date,
    biz_conf.product_id${vt}        as product_id
  from dw_new${db_suffix}.dw_loan_base_stat_loan_num_day as loan_num
  join dim_new.biz_conf
  on  loan_num.product_id = biz_conf.product_id
  and loan_num.biz_date <= '${ST9}'
  and biz_conf.product_id${vt} is not null
  group by
    capital_id,
    channel_id,
    project_id,
    loan_terms,
    date_format(biz_date,'yyyy-MM'),
    month('${ST9}') + (year('${ST9}') - year(biz_date)) * 12 - month(biz_date),
    biz_conf.product_id${vt}
  -- order by loan_month,product_id,loan_terms
  -- limit 10;
) as loan_principal
full join ( -- 曾有
  select
    capital_id                              as capital_id_once,
    channel_id                              as channel_id_once,
    project_id                              as project_id_once,
    loan_terms                              as loan_terms_once,
    dpd                                     as dpd_once,
    overdue_mob                             as mob_once,
    date_format(loan_active_date,'yyyy-MM') as loan_month_once,
    sum(overdue_principal)                  as overdue_principal_once,
    sum(overdue_remain_principal)           as overdue_remain_principal_once,
    '${ST9}'                                as biz_date_once,
    biz_conf.product_id${vt}                as product_id_once
  from (
    select
      loan_terms,
      (year('${ST9}') - year(loan_active_date)) * 12 + month('${ST9}') - month(loan_active_date) as overdue_mob,
      loan_active_date,
      should_repay_date,
      overdue_date_first,
      overdue_date_start,
      overdue_principal,
      overdue_remain_principal,
      is_first_overdue_day as ifo,
      overdue_days,
      biz_date,
      product_id,
      case overdue_days
      when 0   then '0'
      when 1   then '1+'
      when 4   then '3+'
      when 8   then '7+'
      when 15  then '14+'
      when 31  then '30+'
      when 61  then '60+'
      when 91  then '90+'
      when 121 then '120+'
      when 151 then '150+'
      when 181 then '180+'
      else null end dpd
    from dw_new${db_suffix}.dw_loan_base_stat_overdue_num_day
    where 1 > 0
      and biz_date <= '${ST9}'
      and is_first_overdue_day = 1
      and overdue_days in (0,1,4,8,15,31,61,91,121,151,181)
    -- order by product_id,biz_date,loan_active_date
  ) as overdue_num
  join dim_new.biz_conf
  on  overdue_num.product_id = biz_conf.product_id
  and biz_conf.product_id${vt} is not null
  group by
    capital_id,
    channel_id,
    project_id,
    loan_terms,
    dpd,
    overdue_mob,
    date_format(loan_active_date,'yyyy-MM'),
    biz_conf.product_id${vt}
  -- order by loan_month_once,product_id_once,loan_terms_once,mob_once,dpd_once
) as once
on  product_id = product_id_once
and loan_month = loan_month_once
and loan_terms = loan_terms_once
left join ( -- 当前
  select
    overdue_num.loan_terms                              as loan_terms_current,
    overdue_num.dpd                                     as dpd_current,
    overdue_num.overdue_mob                             as mob_current,
    date_format(overdue_num.loan_active_date,'yyyy-MM') as loan_month_current,
    sum(nvl(overdue_principal,0))                       as overdue_principal_current,
    sum(nvl(overdue_remain_principal,0))                as overdue_remain_principal_current,
    biz_conf.product_id${vt}                            as product_id_current
  from dim_new.biz_conf
  join (
    select
      loan_terms,
      overdue_mob,
      loan_active_date,
      should_repay_date,
      sum(overdue_principal)        as overdue_principal,
      sum(overdue_remain_principal) as overdue_remain_principal,
      biz_date,
      product_id,
      dpd
    from dw_new${db_suffix}.dw_loan_base_stat_overdue_num_day
    lateral view explode(
      split(
        concat_ws(',',
          if(overdue_days = 0,  '0',   null),
          if(overdue_days >= 1, '1+',  null),
          if(overdue_days > 3,  '3+',  null),
          if(overdue_days > 7,  '7+',  null),
          if(overdue_days > 14, '14+', null),
          if(overdue_days > 30, '30+', null),
          if(overdue_days > 60, '60+', null),
          if(overdue_days > 90, '90+', null),
          if(overdue_days > 120,'120+',null),
          if(overdue_days > 150,'150+',null),
          if(overdue_days > 180,'180+',null)
        ),','
      )
    ) dpd_x as dpd
    where 1 > 0
      and biz_date = '${ST9}'
    group by product_id,biz_date,loan_active_date,should_repay_date,loan_terms,dpd,overdue_mob
    -- order by product_id,biz_date,loan_active_date,should_repay_date
  ) as overdue_num
  on  overdue_num.product_id = biz_conf.product_id
  and biz_conf.product_id${vt} is not null
  group by
    overdue_num.loan_terms,
    overdue_num.dpd,
    overdue_num.overdue_mob,
    date_format(overdue_num.loan_active_date,'yyyy-MM'),
    biz_conf.product_id${vt}
) as curr
on  product_id_once = product_id_current
and loan_month_once = loan_month_current
and loan_terms_once = loan_terms_current
and mob_once        = mob_current
and dpd_once        = dpd_current
left join (
  select
    sum(remain_principal)                   as remain_principal_current,
    loan_terms                              as loan_terms_remain,
    date_format(loan_active_date,'yyyy-MM') as loan_month_remain,
    biz_conf.product_id${vt}                as product_id_remain
  from dw_new${db_suffix}.dw_loan_base_stat_overdue_num_day as overdue_num
  join dim_new.biz_conf
  on  overdue_num.product_id = biz_conf.product_id
  and overdue_num.biz_date = '${ST9}'
  and biz_conf.product_id${vt} is not null
  group by biz_conf.product_id${vt},loan_terms,date_format(loan_active_date,'yyyy-MM')
) as remain_principal_sum
on  once.loan_terms_once = remain_principal_sum.loan_terms_remain
and once.loan_month_once = remain_principal_sum.loan_month_remain
and once.product_id_once = remain_principal_sum.product_id_remain
where 1 > 0
  and (case when product_id = 'vt_pl00282' and nvl(loan_month,loan_month_once) > '2019-02' then 0 else 1 end) = 1
  -- and product_id = 'vt_001801'
  -- and loan_month = '2020-06'
  -- and loan_terms = 6
  -- and dpd_once = '1+'
-- order by loan_month,product_id,loan_terms,dpd
-- limit 10
;

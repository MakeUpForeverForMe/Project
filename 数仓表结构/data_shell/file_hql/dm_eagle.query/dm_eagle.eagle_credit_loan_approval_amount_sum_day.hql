set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.shuffle.memoryFraction=0.6;        -- shuffle操作的内存占比
set spark.maxRemoteBlockSizeFetchToMem=4G;
set hive.auto.convert.join=false;            -- 关闭自动 MapJoin
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;


insert overwrite table dm_eagle.eagle_credit_loan_approval_amount_sum_day partition (biz_date,product_id)
select
  capital_id                                   as capital_id,
  channel_id                                   as channel_id,
  project_id                                   as project_id,
  nvl(loan_loan_terms,0)                       as loan_terms,
  nvl(credit_approval_amount,0)                as credit_approval_amount,
  nvl(credit_approval_amount_accumulate,0)     as credit_approval_amount_accumulate,
  nvl(credit_approval_amount_dod_ratio,0)      as credit_approval_amount_dod_ratio,
  nvl(credit_approval_num,0)                   as credit_approval_num,
  nvl(credit_approval_num_accumulate,0)        as credit_approval_num_accumulate,
  nvl(credit_approval_num_dod_ratio,0)         as credit_approval_num_dod_ratio,
  nvl(credit_approval_num_num_avg,0)           as credit_approval_num_num_avg,
  nvl(credit_approval_num_num_avg_dod_ratio,0) as credit_approval_num_num_avg_dod_ratio,
  nvl(loan_approval_amount,0)                  as loan_approval_amount,
  nvl(loan_approval_amount_accumulate,0)       as loan_approval_amount_accumulate,
  nvl(loan_approval_amount_dod_ratio,0)        as loan_approval_amount_dod_ratio,
  nvl(loan_approval_num,0)                     as loan_approval_num,
  nvl(loan_approval_num_accumulate,0)          as loan_approval_num_accumulate,
  nvl(loan_approval_num_dod_ratio,0)           as loan_approval_num_dod_ratio,
  nvl(loan_approval_num_num_avg,0)             as loan_approval_num_num_avg,
  nvl(loan_approval_num_num_avg_dod_ratio,0)   as loan_approval_num_num_avg_dod_ratio,
  nvl(credit_approval_num_person,0)            as credit_approval_num_person,
  nvl(credit_approval_num_person_accumulate,0) as credit_approval_num_person_accumulate,
  nvl(credit_approval_num_person_dod_ratio,0)  as credit_approval_num_person_dod_ratio,
  nvl(credit_approval_num_person_num_avg,0)    as credit_approval_num_person_num_avg,
  nvl(loan_approval_num_person,0)              as loan_approval_num_person,
  nvl(loan_approval_num_person_accumulate,0)   as loan_approval_num_person_accumulate,
  nvl(loan_approval_num_person_dod_ratio,0)    as loan_approval_num_person_dod_ratio,
  nvl(loan_approval_num_person_num_avg,0)      as loan_approval_num_person_num_avg,
  nvl(credit_biz_date,loan_biz_date)           as biz_date,
  nvl(credit_product_id,loan_product_id)       as product_id
from (
  select
    product_id                                                                                                                     as credit_product_id,
    biz_date                                                                                                                       as credit_biz_date,

    nvl(credit_approval_amount,cast(0 as decimal(15,4)))                                                                           as credit_approval_amount,
    nvl(credit_approval_amount_count,cast(0 as decimal(15,4)))                                                                     as credit_approval_amount_accumulate,
    nvl((credit_approval_amount - credit_approval_amount_yestday) / credit_approval_amount_yestday * 100,cast(0 as decimal(15,8))) as credit_approval_amount_dod_ratio,

    nvl(credit_approval_num,0)                                                                                                     as credit_approval_num,
    nvl(credit_approval_num_count,0)                                                                                               as credit_approval_num_accumulate,
    nvl((credit_approval_num - credit_approval_num_yestday) / credit_approval_num_yestday * 100,0)                                 as credit_approval_num_dod_ratio,
    nvl(credit_approval_num_num_avg,0)                                                                                             as credit_approval_num_num_avg,
    nvl((credit_approval_num_num_avg - credit_approval_num_num_avg_yestday) / credit_approval_num_num_avg_yestday * 100,0)         as credit_approval_num_num_avg_dod_ratio,
    nvl(credit_approval_num_person,0)                                                                                              as credit_approval_num_person,
    nvl(credit_approval_num_person_count,0)                                                                                        as credit_approval_num_person_accumulate,
    nvl((credit_approval_num_person - credit_approval_num_person_yestday) / credit_approval_num_person_yestday * 100,0)            as credit_approval_num_person_dod_ratio,
    nvl(credit_approval_num_person_num_avg,0)                                                                                      as credit_approval_num_person_num_avg
  from (
    select
      biz_date                                                      as biz_date,
      biz_conf.product_id${vt}                                      as product_id,
      max(credit_approval_amount)                                   as credit_approval_amount,
      max(credit_approval_amount_count)                             as credit_approval_amount_count,
      max(credit_approval_num)                                      as credit_approval_num,
      max(credit_approval_num_count)                                as credit_approval_num_count,
      max(credit_approval_amount) / max(credit_approval_num)        as credit_approval_num_num_avg,
      max(credit_approval_num_person)                               as credit_approval_num_person,
      max(credit_approval_num_person_count)                         as credit_approval_num_person_count,
      max(credit_approval_amount) / max(credit_approval_num_person) as credit_approval_num_person_num_avg
    from dw_new.dw_credit_approval_stat_day as t
    join dim_new.biz_conf
    on  t.product_id = biz_conf.product_id
    and t.biz_date = '${ST9}'
    -- and t.biz_date like '2019%'
    -- and t.biz_date between '2020-01-01' and '2020-08-31'
    -- and t.biz_date between '2020-12-18' and '2021-01-02'
    and biz_conf.product_id${vt} is not null
    group by biz_date,biz_conf.product_id${vt}
  ) as today
  left join (
    select
      biz_date                                                      as biz_date_yestday,
      biz_conf.product_id${vt}                                      as product_id_yestday,
      max(credit_approval_amount)                                   as credit_approval_amount_yestday,
      max(credit_approval_num)                                      as credit_approval_num_yestday,
      max(credit_approval_amount) / max(credit_approval_num)        as credit_approval_num_num_avg_yestday,
      max(credit_approval_num_person)                               as credit_approval_num_person_yestday,
      max(credit_approval_amount) / max(credit_approval_num_person) as credit_approval_num_person_num_avg_yestday
    from dw_new.dw_credit_approval_stat_day as t
    join dim_new.biz_conf
    on  t.product_id = biz_conf.product_id
    and biz_conf.product_id${vt} is not null
    group by biz_date,biz_conf.product_id${vt}
  ) as yestday
  on  product_id = product_id_yestday
  and biz_date   = date_add(biz_date_yestday,1)
) as credit_day
full join (
  select
    biz_date                                                                                                         as loan_biz_date,
    product_id                                                                                                       as loan_product_id,
    loan_terms                                                                                                       as loan_loan_terms,

    nvl(loan_approval_amount,0)                                                                                      as loan_approval_amount,
    nvl(loan_approval_amount_count,0)                                                                                as loan_approval_amount_accumulate,
    nvl((loan_approval_amount - loan_approval_amount_yestday) / loan_approval_amount_yestday * 100,0)                as loan_approval_amount_dod_ratio,
    nvl(loan_approval_num,0)                                                                                         as loan_approval_num,
    nvl(loan_approval_num_count,0)                                                                                   as loan_approval_num_accumulate,
    nvl((loan_approval_num - loan_approval_num_yestday) / loan_approval_num_yestday * 100,0)                         as loan_approval_num_dod_ratio,
    nvl(loan_approval_num_num_avg,0)                                                                                 as loan_approval_num_num_avg,
    nvl((loan_approval_num_num_avg - loan_approval_num_num_avg_yestday) / loan_approval_num_num_avg_yestday * 100,0) as loan_approval_num_num_avg_dod_ratio,
    nvl(loan_approval_num_person,0)                                                                                  as loan_approval_num_person,
    nvl(loan_approval_num_person_count,0)                                                                            as loan_approval_num_person_accumulate,
    nvl((loan_approval_num_person - loan_approval_num_person_yestday) / loan_approval_num_person_yestday * 100,0)    as loan_approval_num_person_dod_ratio,
    nvl(loan_approval_num_person_num_avg,0)                                                                          as loan_approval_num_person_num_avg
  from (
    select
      biz_date                                                    as biz_date,
      biz_conf.product_id${vt}                                    as product_id,
      loan_terms                                                  as loan_terms,
      sum(loan_approval_amount)                                   as loan_approval_amount,
      sum(loan_approval_amount_count)                             as loan_approval_amount_count,
      sum(loan_approval_num)                                      as loan_approval_num,
      sum(loan_approval_num_count)                                as loan_approval_num_count,
      sum(loan_approval_amount) / sum(loan_approval_num)          as loan_approval_num_num_avg,
      sum(loan_approval_num_person)                               as loan_approval_num_person,
      sum(loan_approval_num_person_count)                         as loan_approval_num_person_count,
      sum(loan_approval_amount) / sum(loan_approval_num_person)   as loan_approval_num_person_num_avg
    from dw_new.dw_loan_approval_stat_day
    join dim_new.biz_conf
    on  dw_loan_approval_stat_day.product_id = biz_conf.product_id
    and dw_loan_approval_stat_day.biz_date = '${ST9}'
    -- and dw_loan_approval_stat_day.biz_date like '2019%'
    -- and dw_loan_approval_stat_day.biz_date between '2020-01-01' and '2020-08-31'
    -- and dw_loan_approval_stat_day.biz_date between '2020-12-18' and '2021-01-02'
    and biz_conf.product_id${vt} is not null
    group by biz_date,biz_conf.product_id${vt},loan_terms
  ) as today
  left join (
    select
      biz_date                                                   as biz_date_yestday,
      biz_conf.product_id${vt}                                   as product_id_yestday,
      loan_terms                                                 as loan_terms_yestday,
      sum(loan_approval_amount)                                  as loan_approval_amount_yestday,
      sum(loan_approval_num)                                     as loan_approval_num_yestday,
      sum(loan_approval_amount) / sum(loan_approval_num)         as loan_approval_num_num_avg_yestday,
      sum(loan_approval_num_person)                              as loan_approval_num_person_yestday,
      sum(loan_approval_amount) / sum(loan_approval_num_person)  as loan_approval_num_person_num_avg_yestday
    from dw_new.dw_loan_approval_stat_day
    join dim_new.biz_conf
    on  dw_loan_approval_stat_day.product_id = biz_conf.product_id
    and biz_conf.product_id${vt} is not null
    group by biz_date,biz_conf.product_id${vt},loan_terms
  ) as yestday
  on  product_id = product_id_yestday
  and loan_terms = loan_terms_yestday
  and biz_date   = date_add(biz_date_yestday,1)
) as loan_apply_day
on  credit_product_id = loan_product_id
and credit_biz_date   = loan_biz_date
join (
  select distinct
    capital_id,channel_id,project_id,
    product_id${vt} as dim_product_id
  from dim_new.biz_conf
  where (case when product_id = 'pl00282' and '${ST9}' > '2019-02-22' then 0 else 1 end) = 1
    and product_id${vt} is not null
) as biz_conf
on nvl(credit_product_id,loan_product_id) = dim_product_id
-- limit 10
;

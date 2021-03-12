set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.maxRemoteBlockSizeFetchToMem=200M;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.groupby.orderby.position.alias=true;


insert overwrite table dm_eagle.eagle_credit_loan_approval_rate_day partition (biz_date ='${ST9}',product_id)
select
  capital_id                             as capital_id,
  channel_id                             as channel_id,
  project_id                             as project_id,
  coalesce(loan_loan_terms,loan_accu_loan_terms,0)                 as loan_terms,
  nvl(credit_apply_num,0)                as credit_apply_num,
  nvl(accu_credit_apply_num,0)           as credit_apply_num_accumulate,
  nvl(credit_approval_num,0)             as credit_approval_num,
  nvl(credit_approval_rate,0)            as credit_approval_rate,
  nvl(credit_approval_rate_dod_ratio,0)  as credit_approval_rate_dod_ratio,
  nvl(loan_apply_num,0)                  as loan_apply_num,
  nvl(accu_loan_apply_num,0)             as loan_apply_num_accumulate,
  nvl(loan_approval_num,0)               as loan_approval_num,
  nvl(loan_approval_rate,0)              as loan_approval_rate,
  nvl(loan_approval_rate_dod_ratio,0)    as loan_approval_rate_dod_ratio,
  nvl(credit_apply_amount,0)             as credit_apply_amount,
  nvl(accu_credit_apply_amount,0)        as credit_apply_amount_accumulate,
  nvl(credit_approval_amount,0)          as credit_approval_amount,
  nvl(credit_approval_rate_amount,0)     as credit_approval_rate_amount,
  nvl(loan_apply_amount,0)               as loan_apply_amount,
  nvl(accu_loan_apply_amount,0)          as loan_apply_amount_accumulate,
  nvl(loan_approval_amount,0)            as loan_approval_amount,
  nvl(loan_approval_rate_amount,0)       as loan_approval_rate_amount,
  nvl(credit_apply_num_person,0)         as credit_apply_num_person,
  nvl(accu_credit_apply_num_person,0)    as credit_apply_num_person_accumulate,
  nvl(credit_approval_num_person,0)      as credit_approval_num_person,
  nvl(credit_approval_rate_person,0)     as credit_approval_rate_person,
  nvl(loan_apply_num_person,0)           as loan_apply_num_person,
  nvl(accu_loan_apply_num_person,0)      as loan_apply_num_person_accumulate,
  nvl(loan_approval_num_person,0)        as loan_approval_num_person,
  nvl(loan_approval_rate_person,0)       as loan_approval_rate_person,
  coalesce(loan_product_id,credit_product_id,credit_accu_product_id,loan_accu_product_id) as product_id
from (
  select
    loan_terms                                                                                  as loan_loan_terms,
    biz_date                                                                                    as loan_biz_date,
    product_id                                                                                  as loan_product_id,
    nvl(loan_apply_num,0)                                                                       as loan_apply_num,
    nvl(loan_approval_num,0)                                                                    as loan_approval_num,
    nvl((loan_approval_num / loan_apply_num) * 100,0)                                           as loan_approval_rate,
    nvl(((loan_approval_num - loan_approval_num_yestday) / loan_approval_num_yestday ) * 100,0) as loan_approval_rate_dod_ratio,
    nvl(loan_apply_amount,0)                                                                    as loan_apply_amount,
    nvl(loan_approval_amount,0)                                                                 as loan_approval_amount,
    nvl((loan_approval_amount / loan_apply_amount) * 100,0)                                     as loan_approval_rate_amount,
    nvl(loan_apply_num_person,0)                                                                as loan_apply_num_person,
    nvl(loan_approval_num_person,0)                                                             as loan_approval_num_person,
    nvl((loan_approval_num_person / loan_apply_num_person) * 100,0)                             as loan_approval_rate_person
  from (
    select
      loan_terms,
      biz_date,
      biz_conf.product_id${vt} as product_id,
      sum(loan_apply_num)                                                      as loan_apply_num,
      sum(if(loan_apply_date = loan_approval_date,loan_approval_num,0))        as loan_approval_num,
      sum(loan_apply_amount)                                                   as loan_apply_amount,
      sum(if(loan_apply_date = loan_approval_date,loan_approval_amount,0))     as loan_approval_amount,
      sum(loan_apply_num_person)                                               as loan_apply_num_person,
      sum(if(loan_apply_date = loan_approval_date,loan_approval_num_person,0)) as loan_approval_num_person
    from dw_new.dw_loan_apply_stat_day
    join dim_new.biz_conf
    on  dw_loan_apply_stat_day.product_id = biz_conf.product_id
    and dw_loan_apply_stat_day.biz_date = '${ST9}'
    and biz_conf.product_id${vt} is not null
    group by 1,2,3
  ) as today
  left join (
    select
      loan_terms                                                               as loan_terms_yestday,
      biz_date                                                                 as biz_date_yestday,
      biz_conf.product_id${vt}                                                 as product_id_yestday,
      sum(loan_apply_num)                                                      as loan_apply_num_yestday,
      sum(if(loan_apply_date = loan_approval_date,loan_approval_num,0))        as loan_approval_num_yestday,
      sum(loan_apply_amount)                                                   as loan_apply_amount_yestday,
      sum(if(loan_apply_date = loan_approval_date,loan_approval_amount,0))     as loan_approval_amount_yestday,
      sum(loan_apply_num_person)                                               as loan_apply_num_person_yestday,
      sum(if(loan_apply_date = loan_approval_date,loan_approval_num_person,0)) as loan_approval_num_person_yestday
    from dw_new.dw_loan_apply_stat_day
    join dim_new.biz_conf
    on  dw_loan_apply_stat_day.product_id = biz_conf.product_id
    and biz_conf.product_id${vt} is not null
    group by 1,2,3
  ) as yestday
  on  product_id = product_id_yestday
  and loan_terms = loan_terms_yestday
  and biz_date   = date_add(biz_date_yestday,1)
) as loanapply_cal
full join (
  select
    biz_date                                                                                     as credit_biz_date,
    product_id                                                                                   as credit_product_id,
    nvl(credit_apply_num,0)                                                                      as credit_apply_num,
    nvl(credit_approval_num,0)                                                                   as credit_approval_num,
    nvl(((credit_approval_num - loan_approval_num_yestday) / loan_approval_num_yestday) * 100,0) as credit_approval_rate_dod_ratio,
    nvl((credit_approval_num / credit_apply_num) * 100,0)                                        as credit_approval_rate,
    nvl(credit_apply_amount,0)                                                                   as credit_apply_amount,
    nvl(credit_approval_amount,0)                                                                as credit_approval_amount,
    nvl((credit_approval_amount / credit_apply_amount) * 100,0)                                  as credit_approval_rate_amount,
    nvl(credit_apply_num_person,0)                                                               as credit_apply_num_person,
    nvl(credit_approval_num_person,0)                                                            as credit_approval_num_person,
    nvl((credit_approval_num_person / credit_apply_num_person) * 100,0)                          as credit_approval_rate_person
  from (
    select
      biz_date,
      biz_conf.product_id${vt} as product_id,
      sum(credit_apply_num)                                                          as credit_apply_num,
      sum(if(credit_apply_date = credit_approval_date,credit_approval_num,0))        as credit_approval_num,
      sum(credit_apply_amount)                                                       as credit_apply_amount,
      sum(if(credit_apply_date = credit_approval_date,credit_approval_amount,0))     as credit_approval_amount,
      sum(credit_apply_num_person)                                                   as credit_apply_num_person,
      sum(if(credit_apply_date = credit_approval_date,credit_approval_num_person,0)) as credit_approval_num_person
    from  dw_new.dw_credit_apply_stat_day
    join dim_new.biz_conf
    on  dw_credit_apply_stat_day.product_id = biz_conf.product_id
    and dw_credit_apply_stat_day.biz_date = '${ST9}'
    and biz_conf.product_id${vt} is not null
    group by 1,2
  ) as today
  left join (
    select
      biz_date                                                                       as biz_date_yestday,
      biz_conf.product_id${vt}                                                       as product_id_yestday,
      sum(credit_apply_num)                                                          as credit_apply_num_yestday,
      sum(if(credit_apply_date = credit_approval_date,credit_approval_num,0))        as loan_approval_num_yestday,
      sum(credit_apply_amount)                                                       as credit_apply_amount_yestday,
      sum(if(credit_apply_date = credit_approval_date,credit_approval_amount,0))     as credit_approval_amount_yestday,
      sum(credit_apply_num_person)                                                   as credit_apply_num_person_yestday,
      sum(if(credit_apply_date = credit_approval_date,credit_approval_num_person,0)) as credit_approval_num_person_yestday
    from  dw_new.dw_credit_apply_stat_day
    join dim_new.biz_conf
    on  dw_credit_apply_stat_day.product_id = biz_conf.product_id
    and biz_conf.product_id${vt} is not null
    group by 1,2
  ) as yestday
  on  product_id = product_id_yestday
  and biz_date   = date_add(biz_date_yestday,1)
) as credit_cal
on  loan_product_id = credit_product_id
and credit_biz_date = loan_biz_date
full join
(
    select
        '${ST9}'                                                                       as credit_accu_biz_date,
        biz_conf.product_id${vt}                                                       as credit_accu_product_id,
        sum(credit_apply_num)                                                          as accu_credit_apply_num,
        sum(credit_apply_amount)                                                       as accu_credit_apply_amount,
        sum(credit_apply_num_person)                                                   as accu_credit_apply_num_person
    from  dw_new.dw_credit_apply_stat_day
    join dim_new.biz_conf
    on  dw_credit_apply_stat_day.product_id = biz_conf.product_id
    and dw_credit_apply_stat_day.biz_date <= '${ST9}'
    and biz_conf.product_id${vt} is not null
    group by 1,2
) accu_credit_apply
on credit_product_id = credit_accu_product_id
and credit_biz_date = credit_accu_biz_date
full join
(
    select
        loan_terms                                                               as loan_accu_loan_terms,
        '${ST9}'                                                                 as loan_accu_biz_date,
        biz_conf.product_id${vt}                                                 as loan_accu_product_id,
        sum(loan_apply_num)                                                      as accu_loan_apply_num,
        sum(loan_apply_amount)                                                   as accu_loan_apply_amount,
        sum(loan_apply_num_person)                                               as accu_loan_apply_num_person
    from dw_new.dw_loan_apply_stat_day
    join dim_new.biz_conf
    on  dw_loan_apply_stat_day.product_id = biz_conf.product_id
    and dw_loan_apply_stat_day.biz_date <= '${ST9}'
    and biz_conf.product_id${vt} is not null
    group by 1,2,3
) accu_loan_apply
on loan_loan_terms = loan_accu_loan_terms
and loan_biz_date = loan_accu_biz_date
and loan_product_id = loan_accu_product_id
 join (
  select distinct
    capital_id,channel_id,project_id,
    product_id${vt} as dim_product_id
  from dim_new.biz_conf
  where (case when product_id = 'pl00282' and '${ST9}' > '2019-02-22' then 0 else 1 end) = 1
) as biz_conf
on coalesce(credit_product_id,loan_product_id,credit_accu_product_id,loan_accu_product_id) = dim_product_id
--where coalesce(loan_product_id,credit_product_id,credit_accu_product_id,loan_accu_product_id) != 'vt_pl00282'
;

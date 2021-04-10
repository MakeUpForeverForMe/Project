set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;
set hive.groupby.orderby.position.alias=true;
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
  nvl(accu_loan_approve_num,0)           as loan_approval_num_accumulate,
  nvl(loan_approval_rate,0)              as loan_approval_rate,
  nvl(loan_approval_rate_dod_ratio,0)    as loan_approval_rate_dod_ratio,
  nvl(credit_apply_amount,0)             as credit_apply_amount,
  nvl(accu_credit_apply_amount,0)        as credit_apply_amount_accumulate,
  nvl(credit_approval_amount,0)          as credit_approval_amount,
  nvl(credit_approval_rate_amount,0)     as credit_approval_rate_amount,
  nvl(loan_apply_amount,0)               as loan_apply_amount,
  nvl(accu_loan_apply_amount,0)          as loan_apply_amount_accumulate,
  nvl(loan_approval_amount,0)            as loan_approval_amount,
  nvl(accu_loan_approve_amount,0)        as loan_approval_amount_accumulate,
  nvl(loan_approval_rate_amount,0)       as loan_approval_rate_amount,
  nvl(credit_apply_num_person,0)         as credit_apply_num_person,
  nvl(accu_credit_apply_num_person,0)    as credit_apply_num_person_accumulate,
  nvl(credit_approval_num_person,0)      as credit_approval_num_person,
  nvl(credit_approval_rate_person,0)     as credit_approval_rate_person,
  nvl(loan_apply_num_person,0)           as loan_apply_num_person,
  nvl(accu_loan_apply_num_person,0)      as loan_apply_num_person_accumulate,
  nvl(loan_approval_num_person,0)        as loan_approval_num_person_accumulate,
  nvl(accu_loan_approve_num_person,0)    as loan_approval_num_person,
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
    from dw.dw_loan_apply_stat_day
    join (
    select distinct
       capital_id,
       channel_id,
       project_id,
       product_id_vt,
       product_id
       from (
         select
           max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
           max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
           max(if(col_name = 'project_id',   col_val,null)) as project_id,
           max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
           max(if(col_name = 'product_id',   col_val,null)) as product_id
         from dim.data_conf
         where col_type = 'ac'
         group by col_id
         )tmp
    ) as biz_conf
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
    from dw.dw_loan_apply_stat_day
    join (
    select distinct
       capital_id,
       channel_id,
       project_id,
       product_id_vt,
       product_id
       from (
         select
           max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
           max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
           max(if(col_name = 'project_id',   col_val,null)) as project_id,
           max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
           max(if(col_name = 'product_id',   col_val,null)) as product_id
         from dim.data_conf
         where col_type = 'ac'
         group by col_id
         )tmp
    )as biz_conf
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
    from  dw.dw_credit_apply_stat_day
    join (
    select distinct
       capital_id,
       channel_id,
       project_id,
       product_id_vt,
       product_id
       from (
         select
           max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
           max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
           max(if(col_name = 'project_id',   col_val,null)) as project_id,
           max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
           max(if(col_name = 'product_id',   col_val,null)) as product_id
         from dim.data_conf
         where col_type = 'ac'
         group by col_id
         )tmp
    )biz_conf
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
    from  dw.dw_credit_apply_stat_day
    join (
        select distinct
       capital_id,
       channel_id,
       project_id,
       product_id_vt,
       product_id
       from (
         select
           max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
           max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
           max(if(col_name = 'project_id',   col_val,null)) as project_id,
           max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
           max(if(col_name = 'product_id',   col_val,null)) as product_id
         from dim.data_conf
         where col_type = 'ac'
         group by col_id
         )tmp
    )biz_conf
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
    from  dw.dw_credit_apply_stat_day
    join (
        select distinct
       capital_id,
       channel_id,
       project_id,
       product_id_vt,
       product_id
       from (
         select
           max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
           max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
           max(if(col_name = 'project_id',   col_val,null)) as project_id,
           max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
           max(if(col_name = 'product_id',   col_val,null)) as product_id
         from dim.data_conf
         where col_type = 'ac'
         group by col_id
         )tmp
    )biz_conf
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
        sum(loan_apply_num_count)                                                as accu_loan_apply_num,
        sum(loan_apply_amount_count)                                             as accu_loan_apply_amount,
        sum(loan_apply_num_person_count)                                         as accu_loan_apply_num_person
    from dw.dw_loan_apply_stat_day
    join (
        select distinct
       capital_id,
       channel_id,
       project_id,
       product_id_vt,
       product_id
       from (
         select
           max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
           max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
           max(if(col_name = 'project_id',   col_val,null)) as project_id,
           max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
           max(if(col_name = 'product_id',   col_val,null)) as product_id
         from dim.data_conf
         where col_type = 'ac'
         group by col_id
         )tmp
    )biz_conf
    on  dw_loan_apply_stat_day.product_id = biz_conf.product_id
    and dw_loan_apply_stat_day.biz_date = '${ST9}'
    and biz_conf.product_id${vt} is not null
    group by 1,2,3
) accu_loan_apply
on loan_loan_terms = loan_accu_loan_terms
and loan_biz_date = loan_accu_biz_date
and loan_product_id = loan_accu_product_id
full join (
    select
        loan_terms                                                               as loan_appov_accu_loan_terms,
        '${ST9}'                                                                 as loan_appov_accu_biz_date,
        biz_conf.product_id${vt}                                                 as loan_appov_accu_product_id,
        sum(loan_approval_num_count)                                             as accu_loan_approve_num,
        sum(loan_approval_amount_count)                                          as accu_loan_approve_amount,
        sum(loan_approval_num_person_count)                                      as accu_loan_approve_num_person
        from
    dw.dw_loan_approval_stat_day
  join (
        select distinct
       capital_id,
       channel_id,
       project_id,
       product_id_vt,
       product_id
       from (
         select
           max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
           max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
           max(if(col_name = 'project_id',   col_val,null)) as project_id,
           max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
           max(if(col_name = 'product_id',   col_val,null)) as product_id
         from dim.data_conf
         where col_type = 'ac'
         group by col_id
         )tmp
    )biz_conf
    on  dw_loan_approval_stat_day.product_id = biz_conf.product_id
    and dw_loan_approval_stat_day.biz_date = '${ST9}'
    and biz_conf.product_id${vt} is not null
    group by 1,2,3
)loan_appro_acc
on loan_loan_terms = loan_appov_accu_loan_terms
and loan_biz_date = loan_appov_accu_biz_date
and loan_product_id = loan_appov_accu_product_id
 join (
        select distinct
       capital_id,
       channel_id,
       project_id,
       product_id${vt} as  dim_product_id,
       product_id
       from (
         select
           max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
           max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
           max(if(col_name = 'project_id',   col_val,null)) as project_id,
           max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
           max(if(col_name = 'product_id',   col_val,null)) as product_id
         from dim.data_conf
         where col_type = 'ac'
         group by col_id
         )tmp
        where (case when product_id = 'pl00282' and '${ST9}' > '2019-02-22' then 0 else 1 end) = 1
) as biz_conf
on coalesce(credit_product_id,loan_product_id,credit_accu_product_id,loan_accu_product_id) = dim_product_id
--where coalesce(loan_product_id,credit_product_id,credit_accu_product_id,loan_accu_product_id) != 'vt_pl00282'
;

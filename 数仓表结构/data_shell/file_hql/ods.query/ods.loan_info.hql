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

-- 设置可以使用正则匹配 `(a|b)?+.+`
set hive.support.quoted.identifiers=None;




-- set hivevar:ST9=2021-04-20;

-- 代偿前
-- set hivevar:db_suffix=;
-- 代称后
-- set hivevar:db_suffix=_cps;

-- 跑所有项目的产品编号设置
-- set hivevar:product_id=;

-- 跑非乐信项目的产品编号设置
-- set hivevar:product_id=and product_id not in (
--   '001801','001802','001803','001804',
--   '001901','001902','001903','001904','001905','001906','001907',
--   '002001','002002','002003','002004','002005','002006','002007',
--   '002401','002402',
--   ''
-- );

-- 跑非乐信看管项目的产品编号设置
-- set hivevar:product_id=and product_id in (
--   'DIDI201908161538',
--   '001601','001602','001603',
--   '001701','001702',
--   '002201','002202','002203',
--   ''
-- );


-- -- 跑全量
-- insert overwrite table ods${db_suffix}.loan_info partition(is_settled = 'no',product_id)
-- select
--   due_bill_no,
--   apply_no,
--   loan_active_date,
--   loan_init_term,
--   loan_init_principal,
--   loan_init_interest,
--   loan_init_term_fee,
--   loan_init_svc_fee,
--   loan_term,
--   account_age,
--   should_repay_date,
--   loan_term_repaid,
--   loan_term_remain,
--   loan_status,
--   loan_status_cn,
--   loan_out_reason,
--   paid_out_type,
--   paid_out_type_cn,
--   paid_out_date,
--   terminal_date,
--   paid_amount,
--   paid_principal,
--   paid_interest,
--   paid_penalty,
--   paid_svc_fee,
--   paid_term_fee,
--   paid_mult,
--   remain_amount,
--   remain_principal,
--   remain_interest,
--   remain_svc_fee,
--   remain_term_fee,
--   remain_othAmounts,
--   overdue_principal,
--   overdue_interest,
--   overdue_svc_fee,
--   overdue_term_fee,
--   overdue_penalty,
--   overdue_mult_amt,
--   min(overdue_date_start)                                over(partition by product_id,due_bill_no order by biz_date)    as overdue_date_first,
--   overdue_date_start,
--   overdue_days,
--   overdue_date,
--   overdue_date_start as dpd_begin_date,
--   overdue_days as dpd_days,
--   max(nvl(dpd_dpd_days_count,0) + overdue_days)          over(partition by product_id,due_bill_no order by biz_date)    as dpd_days_count,
--   max(overdue_days)                                      over(partition by product_id,due_bill_no order by biz_date)    as dpd_days_max,
--   collect_out_date as collect_out_date,
--   overdue_term,
--   count(distinct if(overdue_days > 0,overdue_term,null)) over(partition by product_id,due_bill_no order by biz_date)    as overdue_terms_count,
--   max(overdue_terms_max)                                 over(partition by product_id,due_bill_no order by biz_date)    as overdue_terms_max,
--   nvl(sum(distinct overdue_principal)                    over(partition by product_id,due_bill_no order by biz_date),0) as overdue_principal_accumulate,
--   nvl(max(overdue_principal)                             over(partition by product_id,due_bill_no order by biz_date),0) as overdue_principal_max,
--   create_time,
--   update_time,
--   biz_date as s_d_date,
--   nvl(lead(biz_date)                                     over(partition by product_id,due_bill_no order by biz_date),'3000-12-31') as e_d_date,
--   product_id
-- from (
--   select * from ods${db_suffix}.loan_info_inter
--   where 1 > 0
--     ${product_id}
-- ) as loan_info
-- left join (
--   select
--     dpd_product_id,
--     dpd_due_bill_no,
--     dpd_overdue_date_start,
--     dpd_overdue_date_next,
--     nvl(lag(dpd_dpd_days_count) over(partition by dpd_product_id,dpd_due_bill_no order by dpd_overdue_date_start),0) as dpd_dpd_days_count
--   from (
--     select
--       product_id                                                                                                       as dpd_product_id,
--       due_bill_no                                                                                                      as dpd_due_bill_no,
--       nvl(overdue_date_start,'1970-01-01')                                                                             as dpd_overdue_date_start,
--       nvl(lead(overdue_date_start) over(partition by product_id,due_bill_no order by overdue_date_start),'3000-12-31') as dpd_overdue_date_next,
--       sum(max(overdue_days))       over(partition by product_id,due_bill_no order by overdue_date_start)               as dpd_dpd_days_count
--     from ods${db_suffix}.loan_info_inter
--     where 1 > 0
--       and overdue_days > 0
--       ${product_id}
--     group by product_id,due_bill_no,overdue_date_start
--   ) as tmp
-- ) as dpd_days_count
-- on  product_id  = dpd_product_id
-- and due_bill_no = dpd_due_bill_no
-- where biz_date between dpd_overdue_date_start and date_sub(dpd_overdue_date_next,1)
-- ;







-- 跑增量
insert overwrite table ods${db_suffix}.loan_info partition(is_settled = 'no',product_id)
select
  due_bill_no,
  apply_no,
  loan_active_date,
  loan_init_term,
  loan_init_principal,
  loan_init_interest,
  loan_init_term_fee,
  loan_init_svc_fee,
  loan_term,
  account_age,
  should_repay_date,
  loan_term_repaid,
  loan_term_remain,
  loan_status,
  loan_status_cn,
  loan_out_reason,
  paid_out_type,
  paid_out_type_cn,
  paid_out_date,
  terminal_date,
  paid_amount,
  paid_principal,
  paid_interest,
  paid_penalty,
  paid_svc_fee,
  paid_term_fee,
  paid_mult,
  remain_amount,
  remain_principal,
  remain_interest,
  remain_svc_fee,
  remain_term_fee,
  remain_othAmounts,
  overdue_principal,
  overdue_interest,
  overdue_svc_fee,
  overdue_term_fee,
  overdue_penalty,
  overdue_mult_amt,
  min(overdue_date_start)                                over(partition by product_id,due_bill_no order by s_d_date)    as overdue_date_first,
  overdue_date_start,
  overdue_days,
  overdue_date,
  overdue_date_start as dpd_begin_date,
  overdue_days as dpd_days,
  max(nvl(dpd_dpd_days_count,0) + overdue_days)          over(partition by product_id,due_bill_no order by s_d_date)    as dpd_days_count,
  max(overdue_days)                                      over(partition by product_id,due_bill_no order by s_d_date)    as dpd_days_max,
  collect_out_date as collect_out_date,
  overdue_term,
  count(distinct if(overdue_days > 0,overdue_term,null)) over(partition by product_id,due_bill_no order by s_d_date)    as overdue_terms_count,
  max(overdue_terms_max)                                 over(partition by product_id,due_bill_no order by s_d_date)    as overdue_terms_max,
  nvl(sum(distinct overdue_principal)                    over(partition by product_id,due_bill_no order by s_d_date),0) as overdue_principal_accumulate,
  nvl(max(overdue_principal)                             over(partition by product_id,due_bill_no order by s_d_date),0) as overdue_principal_max,
  create_time,
  update_time,
  s_d_date,
  e_d_date,
  product_id
from (
  select
    old_data.due_bill_no,
    old_data.apply_no,
    old_data.loan_active_date,
    old_data.loan_init_term,
    old_data.loan_init_principal,
    old_data.loan_init_interest,
    old_data.loan_init_term_fee,
    old_data.loan_init_svc_fee,
    old_data.loan_term,
    old_data.account_age,
    old_data.should_repay_date,
    old_data.loan_term_repaid,
    old_data.loan_term_remain,
    old_data.loan_status,
    old_data.loan_status_cn,
    old_data.loan_out_reason,
    old_data.paid_out_type,
    old_data.paid_out_type_cn,
    old_data.paid_out_date,
    old_data.terminal_date,
    old_data.paid_amount,
    old_data.paid_principal,
    old_data.paid_interest,
    old_data.paid_penalty,
    old_data.paid_svc_fee,
    old_data.paid_term_fee,
    old_data.paid_mult,
    old_data.remain_amount,
    old_data.remain_principal,
    old_data.remain_interest,
    old_data.remain_svc_fee,
    old_data.remain_term_fee,
    old_data.remain_othAmounts,
    old_data.overdue_principal,
    old_data.overdue_interest,
    old_data.overdue_svc_fee,
    old_data.overdue_term_fee,
    old_data.overdue_penalty,
    old_data.overdue_mult_amt,
    old_data.overdue_date_start,
    old_data.overdue_days,
    old_data.overdue_date,
    old_data.collect_out_date,
    old_data.dpd_days_max,
    old_data.overdue_term,
    old_data.overdue_terms_count,
    old_data.overdue_terms_max,
    old_data.overdue_principal_accumulate,
    old_data.overdue_principal_max,
    old_data.create_time,
    old_data.update_time,
    old_data.s_d_date,
    case
      when new_data.due_bill_no is not null and old_data.s_d_date < new_data.biz_date and new_data.biz_date < old_data.e_d_date then new_data.biz_date
      when new_data.due_bill_no is not null and old_data.s_d_date = new_data.biz_date                                           then '3000-12-31'
      when new_data.due_bill_no is     null and old_data.s_d_date < '${ST9}' and '${ST9}' < old_data.e_d_date                   then '3000-12-31'
      when new_data.due_bill_no is     null and old_data.e_d_date = '${ST9}'                                                    then '3000-12-31'
      else old_data.e_d_date
    end as e_d_date,
    old_data.product_id
  from (
    select
      *
    from ods${db_suffix}.loan_info
    where 1 > 0
      and is_settled = 'no'
      and s_d_date < '${ST9}'
      ${product_id}
  ) as old_data
  left join (
    select
      due_bill_no,
      biz_date,
      product_id
    from ods${db_suffix}.loan_info_inter
    where 1 > 0
      and biz_date = '${ST9}'
      ${product_id}
  ) as new_data
  on  old_data.product_id  = new_data.product_id
  and old_data.due_bill_no = new_data.due_bill_no
  union all
  select
    `(biz_date|product_id)?+.+`,
    biz_date     as s_d_date,
    '3000-12-31' as e_d_date,
    product_id
  from ods${db_suffix}.loan_info_inter
  where 1 > 0
    and biz_date = '${ST9}'
    ${product_id}
) as loan_info
left join (
  select
    dpd_product_id,
    dpd_due_bill_no,
    dpd_overdue_date_start,
    dpd_overdue_date_next,
    nvl(lag(dpd_dpd_days_count) over(partition by dpd_product_id,dpd_due_bill_no order by dpd_overdue_date_start),0) as dpd_dpd_days_count
  from (
    select
      product_id                                                                                                       as dpd_product_id,
      due_bill_no                                                                                                      as dpd_due_bill_no,
      nvl(overdue_date_start,'1970-01-01')                                                                             as dpd_overdue_date_start,
      nvl(lead(overdue_date_start) over(partition by product_id,due_bill_no order by overdue_date_start),'3000-12-31') as dpd_overdue_date_next,
      sum(max(overdue_days))       over(partition by product_id,due_bill_no order by overdue_date_start)               as dpd_dpd_days_count
    from ods${db_suffix}.loan_info
    where 1 > 0
      and overdue_days > 0
      ${product_id}
    group by product_id,due_bill_no,overdue_date_start
  ) as tmp
) as dpd_days_count
on  product_id  = dpd_product_id
and due_bill_no = dpd_due_bill_no
where s_d_date between dpd_overdue_date_start and date_sub(dpd_overdue_date_next,1)
-- limit 10
;

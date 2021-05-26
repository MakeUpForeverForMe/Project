-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=128000000; -- 128M
set hive.merge.smallfiles.avgsize=128000000; -- 128M
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;



insert overwrite table dw.dw_loan_approval_stat_day partition(biz_date,product_id)
select
  nvl(loan_terms,loan_terms_count)                                           as loan_terms,
  '${ST9}'                                                                   as loan_approval_date,
  nvl(loan_approval_num,0)                                                   as loan_approval_num,
  nvl(loan_approval_num_count,0)                                             as loan_approval_num_count,
  nvl(loan_approval_num_person,0)                                            as loan_approval_num_person,
  nvl(loan_approval_num_person_count,0)                                      as loan_approval_num_person_count,
  nvl(loan_approval_amount,0)                                                as loan_approval_amount,
  nvl(loan_approval_amount_count,0)                                          as loan_approval_amount_count,
  '${ST9}'                                                                   as biz_date,
  nvl(product_id,product_id_count)                                           as product_id
from (
  select
    product_id                   as product_id,
    loan_terms                   as loan_terms,
    count(distinct apply_id)   as loan_approval_num,
    count(distinct user_hash_no) as loan_approval_num_person,
    sum(loan_amount_approval)    as loan_approval_amount
  from (
    select
      user_hash_no,
      apply_id,
      loan_terms,
      nvl(loan_amount_approval,0) as loan_amount_approval,
      product_id
    from ods.loan_apply
    where 1 > 0
      and apply_status in (1,4)
      and to_date(issue_time) = '${ST9}'
      and (
        case
          when product_id = 'pl00282' and to_date(issue_time) > '2019-02-22' then false
          else true
        end
      )
      ${hive_param_str}
  ) as tmp
  group by product_id,loan_terms
) as loan_apply
full join (
  select
    product_id                   as product_id_count,
    loan_terms                   as loan_terms_count,
    count(distinct apply_id)    as loan_approval_num_count,
    count(distinct user_hash_no) as loan_approval_num_person_count,
    sum(loan_amount_approval)    as loan_approval_amount_count
  from (
    select
      user_hash_no,
      apply_id,
      loan_terms,
      nvl(loan_amount_approval,0) as loan_amount_approval,
      product_id
    from ods.loan_apply
    where 1 > 0
      and apply_status in (1,4)
      and to_date(issue_time) <= '${ST9}'
      and (
        case
          when product_id = 'pl00282' and to_date(issue_time) > '2019-02-22' then false
          else true
        end
      )
      ${hive_param_str}
  ) as tmp
  group by product_id,loan_terms
) as ods_count
on  product_id = product_id_count
and loan_terms = loan_terms_count
-- limit 1
;

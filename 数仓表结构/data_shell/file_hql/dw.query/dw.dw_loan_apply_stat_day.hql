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



insert overwrite table dw.dw_loan_apply_stat_day partition(biz_date,product_id)
select
  loan_terms,
  loan_apply_date                                             as loan_apply_date,
  count(due_bill_no)                                          as loan_apply_num,
  count(distinct user_hash_no)                                as loan_apply_num_person,
  sum(loan_amount_apply)                                      as loan_apply_amount,
  loan_approval_date                                          as loan_approval_date,
  count(if(apply_status in (1,4),due_bill_no,null))           as loan_approval_num,
  count(distinct if(apply_status in (1,4),user_hash_no,null)) as loan_approval_num_person,
  sum(if(apply_status in (1,4),loan_amount_approval,0))       as loan_approval_amount,
  loan_apply_date                                          as biz_date,
  product_id
from (
  select
    user_hash_no,
    apply_id,
    due_bill_no,
    loan_terms,
    biz_date as loan_apply_date,
    loan_amount_apply,
    apply_status,
    to_date(issue_time) as loan_approval_date,
    loan_amount_approval,
    product_id
  from ods.loan_apply
  where 1 > 0
    and biz_date = '${ST9}'
    and (
      case
        when product_id = 'pl00282' and biz_date > '2019-02-22' then false
        else true
      end
    )
    ${hive_param_str}
) as loan_apply
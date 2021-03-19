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



insert overwrite table dw.dw_credit_apply_stat_day partition(biz_date,product_id)
select
  biz_date                                     as credit_apply_date,
  count(apply_id)                              as credit_apply_num,
  count(distinct user_hash_no)                 as credit_apply_num_person,
  sum(apply_amount)                            as credit_apply_amount,
  credit_approval_date                         as credit_approval_date,
  count(if(resp_code = '1',apply_id,null))     as credit_approval_num,
  count(if(resp_code = '1',user_hash_no,null)) as credit_approval_num_person,
  sum(if(resp_code = '1',credit_amount,0))     as credit_approval_amount,
  biz_date                                     as biz_date,
  product_id                                   as product_id
from (
  select distinct
    user_hash_no,
    apply_id,
    apply_amount,
    resp_code,
    if(resp_code = '1',to_date(credit_approval_time),null) as credit_approval_date,
    credit_amount,
    biz_date as biz_date,
    product_id
  from ods.credit_apply
  where 1 > 0
    and biz_date = '${ST9}'
    and (
      case
        when product_id = 'pl00282' and biz_date > '2019-02-22' then false
        else true
      end
    )
    ${hive_param_str}
) as credit_apply
group by
  credit_approval_date,
  biz_date,
  product_id
-- limit 10
;

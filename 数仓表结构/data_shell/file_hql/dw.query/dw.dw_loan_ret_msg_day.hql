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



insert overwrite table dw.dw_loan_ret_msg_day partition(biz_date,product_id)
select distinct
  loan_terms,
  resp_msg,
  sum(if(resp_msg is not null,1,0)) over(partition by biz_date,product_id,loan_terms,resp_msg) as ret_msg_num,
  nvl(
    sum(if(resp_msg is not null,1,0)) over(partition by biz_date,product_id,loan_terms,resp_msg) / sum(if(resp_msg is not null,1,0)) over(partition by biz_date,product_id,loan_terms) * 100,
    0
  ) as ret_msg_rate,
  biz_date,
  product_id
from (
  select
    apply_id   as apply_id,
    product_id as product_id,
    loan_terms,
    biz_date
  from ods.loan_apply
  where 1 > 0
    and biz_date = '${ST9}'
    ${hive_param_str}
  group by biz_date,product_id,loan_terms,apply_id
) as loan_apply
left join (
  select distinct
    order_id as apply_id_t,
    pro_code as product_id_t,
    ret_msg  as resp_msg
  from stage.t_personas
  where 1 > 0
    and lower(pass) = 'no'
) as t_personas
on  product_id = product_id_t
and apply_id   = apply_id_t
where resp_msg is not null
-- order by biz_date,product_id,loan_terms,resp_msg
-- limit 10
;

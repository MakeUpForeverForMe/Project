set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.maxRemoteBlockSizeFetchToMem=4G;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
 

insert overwrite table dw.dw_loan_ret_msg_day partition(biz_date = '${ST9}',product_id)
-- insert overwrite table dw.dw_loan_ret_msg_day partition(biz_date,product_id)
select distinct
  loan_terms,
  resp_msg,
  sum(if(resp_msg is not null,1,0)) over(partition by biz_date,product_id,loan_terms,resp_msg) as ret_msg_num,
  nvl(sum(if(resp_msg is not null,1,0)) over(partition by biz_date,product_id,loan_terms,resp_msg) / sum(if(resp_msg is not null,1,0)) over(partition by biz_date,product_id,loan_terms) * 100,0) as ret_msg_rate,
  -- biz_date,
  product_id
from (
  select
    apply_id   as apply_id,
    product_id as product_id,
    loan_terms,
    biz_date
  from ods.loan_apply
  where 1 > 0
    and biz_date = '${ST9}'  ${hive_param_str}
  group by biz_date,product_id,loan_terms,apply_id
) as loan_apply
left join (
  select distinct
    order_id as apply_id_t,
    pro_code as product_id_t,
    ret_msg  as resp_msg
  from stg.t_personas
  where 1 > 0
    and lower(pass) = 'no'
) as t_personas
on  product_id = product_id_t
and apply_id   = apply_id_t
where resp_msg is not null
-- order by biz_date,product_id,loan_terms,resp_msg
-- limit 10
;


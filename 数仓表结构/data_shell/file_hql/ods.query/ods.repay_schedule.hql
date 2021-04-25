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
-- insert overwrite table ods${db_suffix}.repay_schedule partition(is_settled = 'no',product_id)
-- select
--   `(biz_date|product_id)?+.+`,
--   biz_date as s_d_date,
--   nvl(lead(biz_date) over(partition by product_id,due_bill_no,loan_term order by biz_date),'3000-12-31') as e_d_date,
--   product_id
-- from ods${db_suffix}.repay_schedule_inter
-- where 1 > 0
--   ${product_id}
-- -- limit 10
-- ;



-- 跑增量
insert overwrite table ods${db_suffix}.repay_schedule partition(is_settled = 'no',product_id)
select
  old_data.due_bill_no,
  old_data.loan_active_date,
  old_data.loan_init_principal,
  old_data.loan_init_term,
  old_data.loan_term,
  old_data.start_interest_date,
  old_data.curr_bal,
  old_data.should_repay_date,
  old_data.should_repay_date_history,
  old_data.grace_date,
  old_data.should_repay_amount,
  old_data.should_repay_principal,
  old_data.should_repay_interest,
  old_data.should_repay_term_fee,
  old_data.should_repay_svc_fee,
  old_data.should_repay_penalty,
  old_data.should_repay_mult_amt,
  old_data.should_repay_penalty_acru,
  old_data.schedule_status,
  old_data.schedule_status_cn,
  old_data.repay_status,
  old_data.paid_out_date,
  old_data.paid_out_type,
  old_data.paid_out_type_cn,
  old_data.paid_amount,
  old_data.paid_principal,
  old_data.paid_interest,
  old_data.paid_term_fee,
  old_data.paid_svc_fee,
  old_data.paid_penalty,
  old_data.paid_mult,
  old_data.reduce_amount,
  old_data.reduce_principal,
  old_data.reduce_interest,
  old_data.reduce_term_fee,
  old_data.reduce_svc_fee,
  old_data.reduce_penalty,
  old_data.reduce_mult_amt,
  old_data.effective_date,
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
  from ods${db_suffix}.repay_schedule
  where 1 > 0
    and is_settled = 'no'
    and s_d_date < '${ST9}'
    and product_id in (${product_id})
) as old_data
left join (
  select
    due_bill_no,
    loan_term,
    biz_date,
    product_id
  from ods${db_suffix}.repay_schedule_inter
  where 1 > 0
    and biz_date = '${ST9}'
    and product_id in (${product_id})
) as new_data
on  old_data.product_id  = new_data.product_id
and old_data.due_bill_no = new_data.due_bill_no
and old_data.loan_term   = new_data.loan_term
union all
select
  `(biz_date|product_id)?+.+`,
  biz_date     as s_d_date,
  '3000-12-31' as e_d_date,
  product_id
from ods${db_suffix}.repay_schedule_inter
where 1 > 0
  and biz_date = '${ST9}'
  and product_id in (${product_id})
-- limit 10
;

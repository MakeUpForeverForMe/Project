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
-- set hivevar:db_suffix=;set hivevar:product_id_param=and product_id in (${product_id});
-- 代称后
-- set hivevar:db_suffix=_cps;set hivevar:product_id_param=and product_id in (${product_id});
-- 其他
-- set hivevar:db_suffix=;set hivevar:product_id_param=and product_id not in (${product_id});


-- 跑非乐信项目的产品编号设置
-- set hivevar:product_id=
--   '001801','001802','001803','001804',
--   '001901','001902','001903','001904','001905','001906','001907',
--   '002001','002002','002003','002004','002005','002006','002007',
--   '002401','002402',
--   ''
-- ;

-- 跑非乐信看管项目的产品编号设置
-- set hivevar:product_id=
--   'DIDI201908161538',
--   '001601','001602','001603',
--   '001701','001702',
--   '002201','002202','002203',
--   ''
-- ;


-- -- set hivevar:product_id_param=and product_id in (${product_id});
-- -- 跑全量
-- -- 关闭自动 MapJoin （ Hive3 的 bug，引发 No work found for tablescan ），e_d_date 有不准确的时候
-- set hive.auto.convert.join=false;
-- set hive.auto.convert.join.noconditionaltask=false;

-- insert overwrite table ods${db_suffix}.repay_schedule partition(is_settled = 'no',product_id)
-- select
--   t1.due_bill_no,
--   t1.loan_active_date,
--   t1.loan_init_principal,
--   t1.loan_init_term,
--   t1.loan_term,
--   t1.start_interest_date,
--   t1.curr_bal,
--   t1.should_repay_date,
--   t1.should_repay_date_history,
--   t1.grace_date,
--   t1.should_repay_amount,
--   t1.should_repay_principal,
--   t1.should_repay_interest,
--   t1.should_repay_term_fee,
--   t1.should_repay_svc_fee,
--   t1.should_repay_penalty,
--   t1.should_repay_mult_amt,
--   t1.should_repay_penalty_acru,
--   t1.schedule_status,
--   t1.schedule_status_cn,
--   t1.repay_status,
--   t1.paid_out_date,
--   t1.paid_out_type,
--   t1.paid_out_type_cn,
--   t1.paid_amount,
--   t1.paid_principal,
--   t1.paid_interest,
--   t1.paid_term_fee,
--   t1.paid_svc_fee,
--   t1.paid_penalty,
--   t1.paid_mult,
--   t1.reduce_amount,
--   t1.reduce_principal,
--   t1.reduce_interest,
--   t1.reduce_term_fee,
--   t1.reduce_svc_fee,
--   t1.reduce_penalty,
--   t1.reduce_mult_amt,
--   t1.effective_date,
--   t1.create_time,
--   t1.update_time,
--   t1.s_d_date,
--   -- t2.after_biz_date,
--   if(t1.loan_init_term > t2.after_init_term,t2.after_biz_date,t1.e_d_date) as e_d_date,
--   t1.product_id
-- from (
--   select
--     due_bill_no,
--     loan_active_date,
--     loan_init_principal,
--     loan_init_term,
--     loan_term,
--     start_interest_date,
--     curr_bal,
--     should_repay_date,
--     should_repay_date_history,
--     grace_date,
--     should_repay_amount,
--     should_repay_principal,
--     should_repay_interest,
--     should_repay_term_fee,
--     should_repay_svc_fee,
--     should_repay_penalty,
--     should_repay_mult_amt,
--     should_repay_penalty_acru,
--     schedule_status,
--     schedule_status_cn,
--     repay_status,
--     paid_out_date,
--     paid_out_type,
--     paid_out_type_cn,
--     paid_amount,
--     paid_principal,
--     paid_interest,
--     paid_term_fee,
--     paid_svc_fee,
--     paid_penalty,
--     paid_mult,
--     reduce_amount,
--     reduce_principal,
--     reduce_interest,
--     reduce_term_fee,
--     reduce_svc_fee,
--     reduce_penalty,
--     reduce_mult_amt,
--     effective_date,
--     create_time,
--     update_time,
--     biz_date as s_d_date,
--     lead(biz_date,1,'3000-12-31') over(partition by product_id,due_bill_no,loan_term order by biz_date) as e_d_date,
--     product_id
--   from ods${db_suffix}.repay_schedule_inter
--   where 1 > 0
--     ${product_id_param}
--     -- and product_id = 'CL202101260095'
--     -- and due_bill_no = 'GALC-HL-1609070134'
-- ) as t1
-- left join (
--   select
--     product_id,
--     due_bill_no,
--     biz_date,
--     lead(loan_init_term,1,loan_init_term) over(partition by product_id,due_bill_no order by loan_init_term desc) as after_init_term,
--     lead(biz_date,1,biz_date) over(partition by product_id,due_bill_no order by biz_date) as after_biz_date
--   from (
--     select distinct
--       due_bill_no,
--       loan_init_term,
--       biz_date,
--       product_id
--   from ods${db_suffix}.repay_schedule_inter
--   where 1 > 0
--     ${product_id_param}
--     -- and product_id = 'CL202101260095'
--     -- and due_bill_no = 'GALC-HL-1609070134'
--   ) as tmp
-- ) as t2
-- on  t1.product_id  = t2.product_id
-- and t1.due_bill_no = t2.due_bill_no
-- and t1.s_d_date    = t2.biz_date
-- -- order by product_id,due_bill_no,cast(loan_term as int),s_d_date
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

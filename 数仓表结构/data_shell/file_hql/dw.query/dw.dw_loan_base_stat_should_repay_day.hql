set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.shuffle.memoryFraction=0.6;         -- shuffle操作的内存占比
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.mapjoin.followby.gby.localtask.max.memory.usage=0.9;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
set hive.auto.convert.join=false;            -- 关闭自动 MapJoin
set hive.exec.parallel=true;
-- 执行sql前，加上如下参数，禁用hive矢量执行：
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;
 


insert overwrite table dw${db_suffix}.dw_loan_base_stat_should_repay_day partition(biz_date='${ST9}',product_id)
select
  should_principal.loan_init_term                                                                                   as loan_terms,
  should_principal.loan_active_date                                                                                 as loan_active_date,
  should_principal.should_repay_date                                                                                as should_repay_date,
  sum(should_principal.loan_init_principal)                                                                         as loan_principal,
  --T 日应还
  sum(nvl(should_penalty_zero.should_repay_interest_fee_zero,0)+ nvl(should_principal.should_repay_amount,0))       as should_repay_amount,
  sum(nvl(should_principal.should_repay_principal,0))                                                               as should_repay_principal,
  sum(nvl(should_penalty_zero.should_repay_interest_fee_zero,0)+ nvl(should_principal.should_repay_interest_fee,0)) as should_repay_interest_fee,
  sum(nvl(should_principal.should_repay_interest,0))                                                                as should_repay_interest,
  sum(nvl(should_penalty_zero.should_repay_svc_term_zero,0)+ nvl(should_principal.should_repay_svc_term,0))         as should_repay_svc_term,
  sum(nvl(should_penalty_zero.should_repay_term_fee_zero,0)+ nvl(should_principal.should_repay_term_fee,0))         as should_repay_term_fee,
  sum(nvl(should_penalty_zero.should_repay_svc_fee_zero,0) + nvl(should_principal.should_repay_svc_fee,0))          as should_repay_svc_fee,
  sum(nvl(should_penalty_zero.should_repay_penalty_zero,0)+ nvl(should_principal.should_repay_penalty,0))           as should_repay_penalty,

  --T-1 已还 + T-1 已还罚息
  sum(nvl(history_repaid.paid_amount,0))                                                                            as should_repay_paid_amount,
  sum(nvl(history_repaid.paid_principal,0))                                                                         as should_repay_paid_principal,
  sum(nvl(history_repaid.paid_interest_fee,0)+nvl(paid_penalty_zero.paid_interest_fee,0))                           as should_repay_paid_interest_fee,
  sum(nvl(history_repaid.paid_interest,0))                                                                          as should_repay_paid_interest,
  sum(nvl(history_repaid.paid_svc_term,0)+nvl(paid_penalty_zero.paid_svc_term,0))                                   as should_repay_paid_svc_term,
  sum(nvl(history_repaid.paid_term_fee,0)+nvl(paid_penalty_zero.paid_term_fee,0))                                   as should_repay_paid_term_fee,
  sum(nvl(history_repaid.paid_svc_fee,0)+nvl(paid_penalty_zero.paid_svc_fee,0))                                     as should_repay_paid_svc_fee,
  sum(nvl(history_repaid.paid_penalty,0)+nvl(paid_penalty_zero.paid_penalty,0))                                     as should_repay_paid_penalty,
  sum(nvl(history_repaid.paid_mult,0)+nvl(paid_penalty_zero.paid_mult,0))                                           as should_repay_paid_mult,

  --T-1 逾期
  sum(nvl(overdue_should_repay_amount,0))                                                                           as overdue_should_repay_amount,
  sum(nvl(overdue_should_repay_interest,0))                                                                         as overdue_should_repay_interest,
  sum(nvl(overdue_should_repay_principal,0))                                                                        as overdue_should_repay_principal,
  sum(nvl(overdue_should_repay_interest_fee,0))                                                                     as overdue_should_repay_interest_fee,
  sum(nvl(overdue_should_repay_svc_term,0))                                                                         as overdue_should_repay_svc_term,
  sum(nvl(overdue_should_repay_term_fee,0))                                                                         as overdue_should_repay_term_fee,
  sum(nvl(overdue_should_repay_svc_fee,0))                                                                          as overdue_should_repay_svc_fee,
  sum(nvl(overdue_should_repay_penalty,0))                                                                          as overdue_should_repay_penalty,

  --T 日实际应还=T日计划应还+T应还罚息+T-1逾期 减去 T-1当期已还 減去T-1 已还罚息-T-1 减免-T-1 费用减免
  sum(nvl(should_penalty_zero.should_repay_interest_fee_zero,0)+nvl(should_principal.should_repay_amount,0)+nvl(overdue_should_repay_amount,0)-nvl(history_repaid.paid_amount,0)-nvl(history_repaid.reduce_amount,0)-nvl(paid_penalty_zero.paid_interest_fee,0)-nvl(paid_penalty_zero.reduce_amount,0) )                                     as should_repay_amount_actual,
  sum(nvl(should_principal.should_repay_principal,0)+nvl(overdue_should_repay_principal,0)-nvl(history_repaid.paid_principal,0)-nvl(history_repaid.reduce_principal,0))                                                                                                                                                                      as should_repay_principal_actual,
  sum(nvl(should_penalty_zero.should_repay_interest_fee_zero,0)+ nvl(should_principal.should_repay_interest_fee,0)+nvl(overdue_should_repay_interest_fee,0)-nvl(history_repaid.paid_interest_fee,0)-nvl(paid_penalty_zero.paid_interest_fee,0)-nvl(history_repaid.reduce_interest_fee,0)-nvl(paid_penalty_zero.reduce_interest_fee,0))       as should_repay_interest_fee_actual,
  sum(nvl(should_principal.should_repay_interest,0)+nvl(overdue_should_repay_interest,0)-nvl(history_repaid.paid_interest,0)-nvl(history_repaid.reduce_interest,0) )                                                                                                                                                                           as should_repay_interest_actual,
  sum(nvl(should_penalty_zero.should_repay_svc_term_zero,0)+ nvl(should_principal.should_repay_svc_term,0)+nvl(overdue_should_repay_svc_term,0)-nvl(history_repaid.paid_svc_term,0)-nvl(paid_penalty_zero.paid_svc_term,0)-nvl(history_repaid.reduce_svc_term,0)-nvl(paid_penalty_zero.reduce_svc_term,0))                                   as should_repay_svc_term_actual,
  sum(nvl(should_penalty_zero.should_repay_term_fee_zero,0)+ nvl(should_principal.should_repay_term_fee,0)+nvl(overdue_should_repay_term_fee,0)-nvl(history_repaid.paid_term_fee,0)-nvl(paid_penalty_zero.paid_term_fee,0)-nvl(history_repaid.reduce_term_fee,0)-nvl(paid_penalty_zero.reduce_term_fee,0))                                   as should_repay_term_fee_actual,
  sum(nvl(should_penalty_zero.should_repay_svc_fee_zero,0)+ nvl(should_principal.should_repay_svc_fee,0)+nvl(overdue_should_repay_svc_fee,0)-nvl(history_repaid.paid_svc_fee,0)-nvl(paid_penalty_zero.paid_svc_fee,0)-nvl(history_repaid.reduce_svc_fee,0) -nvl(paid_penalty_zero.reduce_svc_fee,0))                                         as should_repay_svc_fee_actual,
  sum(nvl(should_penalty_zero.should_repay_penalty_zero,0)+ nvl(should_principal.should_repay_penalty,0)+nvl(overdue_should_repay_penalty,0)-nvl(history_repaid.paid_penalty,0)-nvl(paid_penalty_zero.paid_penalty,0)-nvl(history_repaid.reduce_penalty,0) -nvl(paid_penalty_zero.reduce_penalty,0))                                         as should_repay_penalty_actual,


  should_principal.product_id                                                                                                                                                                                                                                                                                                                as product_id
from
( --当期应还
 select
    concat(product_id,"_",loan_active_date,"_",loan_init_term,"_",due_bill_no)                                                                              as should_repay_due_bill_no,
    product_id,
    loan_active_date,
    loan_init_term,
    due_bill_no,
    should_repay_date,
    sum(loan_init_principal)                                                                                                                                     as loan_init_principal,
    sum(nvl(should_repay_principal,0) + nvl(should_repay_interest,0) + nvl(should_repay_term_fee,0) + nvl(should_repay_svc_fee,0) + nvl(should_repay_penalty,0)) as should_repay_amount,
    sum(nvl(should_repay_principal,0))                                                                                                                           as should_repay_principal,
    sum(nvl(should_repay_interest,0))                                                                                                                            as should_repay_interest,
    sum(nvl(should_repay_interest,0) + nvl(should_repay_term_fee,0) + nvl(should_repay_svc_fee,0) + nvl(should_repay_penalty,0))                                 as should_repay_interest_fee,
    sum(nvl(should_repay_term_fee,0) + nvl(should_repay_svc_fee,0))                                                                                              as should_repay_svc_term,
    sum(nvl(should_repay_term_fee,0))                                                                                                                            as should_repay_term_fee,
    sum(nvl(should_repay_svc_fee,0))                                                                                                                             as should_repay_svc_fee,
    sum(nvl(should_repay_penalty,0))                                                                                                                             as should_repay_penalty
from
  (
     select
       product_id,
       due_bill_no,
       loan_active_date,
       loan_init_term,
       loan_term,
       should_repay_date,
       loan_init_principal,
       should_repay_principal,
       should_repay_interest,
       should_repay_term_fee,
       should_repay_svc_fee,
       should_repay_penalty,
       LAG(should_repay_date,1,'1970-01-01') OVER(PARTITION BY due_bill_no ORDER BY loan_term) AS lag_should_repay_date
     from ods${db_suffix}.repay_schedule
     where 1 > 0
       and ('${ST9}' between s_d_date and date_sub(e_d_date,1))
       and loan_term>0
       ${hive_param_str}
  )tmp  where 1>0
   and  lag_should_repay_date<'${ST9}' and '${ST9}'<=should_repay_date and loan_active_date<='${ST9}'
   group by product_id,loan_active_date,loan_init_term,should_repay_date,due_bill_no
)should_principal
left  join
( -- t-1 逾期
   select
    concat(product_id,"_",loan_active_date,"_",loan_init_term,"_",due_bill_no)                                                                                as overdue_should_due_bill_no,
    product_id,
    loan_active_date,
    loan_init_term,
    due_bill_no,
    sum(nvl(should_repay_principal,0) + nvl(should_repay_interest,0) + nvl(should_repay_term_fee,0) + nvl(should_repay_svc_fee,0) + nvl(should_repay_penalty,0)) as overdue_should_repay_amount,
    sum(nvl(should_repay_principal,0))                                                                                                                           as overdue_should_repay_principal,
    sum(nvl(should_repay_interest,0) + nvl(should_repay_term_fee,0) + nvl(should_repay_svc_fee,0) + nvl(should_repay_penalty,0))                                 as overdue_should_repay_interest_fee,
    sum(nvl(should_repay_interest,0))                                                                                                                            as overdue_should_repay_interest,
    sum(nvl(should_repay_term_fee,0) + nvl(should_repay_svc_fee,0))                                                                                              as overdue_should_repay_svc_term,
    sum(nvl(should_repay_term_fee,0))                                                                                                                            as overdue_should_repay_term_fee,
    sum(nvl(should_repay_svc_fee,0))                                                                                                                             as overdue_should_repay_svc_fee,
    sum(nvl(should_repay_penalty,0))                                                                                                                             as overdue_should_repay_penalty
     from ods${db_suffix}.repay_schedule
     where 1 > 0
       and date_sub('${ST9}',1) between s_d_date and date_sub(e_d_date,1)
       and loan_term>0  and  schedule_status='O'  and loan_active_date<='${ST9}'
       ${hive_param_str}
   group by product_id,loan_active_date,loan_init_term,due_bill_no
)history_overdue_should_repay
on should_principal.should_repay_due_bill_no=history_overdue_should_repay.overdue_should_due_bill_no

left join
( --当期已还 T-1
  select
    concat(product_id,"_",loan_active_date,"_",loan_init_term,"_",due_bill_no)                                                                                as paid_due_bill_no,
    product_id,
    loan_active_date,
    loan_init_term,
    due_bill_no,
    sum(nvl(paid_principal,0) + nvl(paid_interest,0) + nvl(paid_term_fee,0) + nvl(paid_svc_fee,0) + nvl(paid_penalty,0))                                         as paid_amount,
    sum(nvl(paid_principal,0))                                                                                                                                   as paid_principal,
    sum(nvl(paid_interest,0))                                                                                                                                    as paid_interest,
    sum(nvl(paid_interest,0) + nvl(paid_term_fee,0) + nvl(paid_svc_fee,0) + nvl(paid_penalty,0))                                                                 as paid_interest_fee,
    sum(nvl(paid_term_fee,0) + nvl(paid_svc_fee,0))                                                                                                              as paid_svc_term,
    sum(nvl(paid_term_fee,0))                                                                                                                                    as paid_term_fee,
    sum(nvl(paid_svc_fee,0))                                                                                                                                     as paid_svc_fee,
    sum(nvl(paid_penalty,0))                                                                                                                                     as paid_penalty,
    sum(nvl(paid_mult,0))                                                                                                                                        as paid_mult,
    sum(nvl(reduce_principal,0))                                                                                                                                 as reduce_principal,
    sum(nvl(reduce_interest,0))                                                                                                                                  as reduce_interest,
    sum(nvl(reduce_term_fee,0))                                                                                                                                  as reduce_term_fee,
    sum(nvl(reduce_interest,0) + nvl(reduce_term_fee,0) + nvl(reduce_svc_fee,0) + nvl(reduce_penalty,0))                                                         as reduce_interest_fee,
    sum(nvl(reduce_term_fee,0) + nvl(reduce_svc_fee,0))                                                                                                          as reduce_svc_term,
    sum(nvl(reduce_svc_fee,0))                                                                                                                                   as reduce_svc_fee,
    sum(nvl(reduce_mult_amt,0))                                                                                                                                  as reduce_mult_amt,
    sum(nvl(reduce_penalty,0))                                                                                                                                   as reduce_penalty,
    sum(nvl(reduce_amount,0))                                                                                                                                    as reduce_amount
    from
  (
     select
       product_id,
       due_bill_no,
       loan_active_date,
       loan_init_term,
       should_repay_date,
       loan_init_principal,
       paid_principal,
       paid_interest,
       paid_term_fee,
       paid_svc_fee,
       paid_penalty,
       paid_mult,
       reduce_principal,
       reduce_interest,
       reduce_term_fee,
       reduce_svc_fee,
       reduce_penalty,
       reduce_mult_amt,
       reduce_amount,
       LAG(should_repay_date,1,'1970-01-01') OVER(PARTITION BY due_bill_no ORDER BY loan_term) AS lag_should_repay_date
     from ods${db_suffix}.repay_schedule
     where 1 > 0
       and (date_sub('${ST9}',1) between s_d_date and date_sub(e_d_date,1))
       and loan_term>0
       ${hive_param_str}
  )tmp  where 1>0
   and (tmp.lag_should_repay_date<'${ST9}' and  '${ST9}' <=should_repay_date and loan_active_date<='${ST9}' )
   group by product_id,loan_active_date,loan_init_term,due_bill_no
)history_repaid
on should_principal.should_repay_due_bill_no=history_repaid.paid_due_bill_no

left join
( -- 当前应还罚息T
select
    concat(product_id,"_",loan_active_date,"_",loan_init_term,"_",due_bill_no)                                                                                as should_repay_due_bill_no,
    product_id,
    loan_active_date,
    loan_init_term,
    due_bill_no,
    sum(nvl(should_repay_svc_fee,0))+sum(nvl(should_repay_penalty,0))+sum(nvl(should_repay_term_fee,0))                            as should_repay_interest_fee_zero,
    sum(nvl(should_repay_svc_fee,0))                                                                                               as should_repay_svc_fee_zero,
    sum(nvl(should_repay_term_fee,0))                                                                                              as should_repay_term_fee_zero,
    sum(nvl(should_repay_penalty,0))                                                                                               as should_repay_penalty_zero,
    sum(nvl(should_repay_term_fee,0) + nvl(should_repay_svc_fee,0))                                                                as should_repay_svc_term_zero
   from ods${db_suffix}.repay_schedule
     where 1 > 0
     and ('${ST9}' between s_d_date and date_sub(e_d_date,1)) and loan_active_date<='${ST9}'
     ${hive_param_str}
     and loan_term=0
 group by product_id,loan_active_date,loan_init_term,due_bill_no
)should_penalty_zero
on should_principal.should_repay_due_bill_no=should_penalty_zero.should_repay_due_bill_no
left join
( -- 当前T-1 已还罚息 只能取实还表 否则没有时间限制
select
    concat(product_id,"_",loan_active_date,"_",loan_init_term,"_",due_bill_no)                                                                                  as paid_due_bill_no,
    product_id,
    loan_active_date,
    loan_init_term,
    due_bill_no,
    sum(nvl(paid_interest,0) + nvl(paid_term_fee,0) + nvl(paid_svc_fee,0) + nvl(paid_penalty,0))                                                                 as paid_interest_fee,
    sum(nvl(paid_term_fee,0) + nvl(paid_svc_fee,0))                                                                                                              as paid_svc_term,
    sum(nvl(paid_term_fee,0))                                                                                                                                    as paid_term_fee,
    sum(nvl(paid_svc_fee,0))                                                                                                                                     as paid_svc_fee,
    sum(nvl(paid_penalty,0))                                                                                                                                     as paid_penalty,
    sum(nvl(paid_mult,0))                                                                                                                                        as paid_mult,
    sum(nvl(reduce_term_fee,0))                                                                                                                                  as reduce_term_fee,
    sum(nvl(reduce_interest,0) + nvl(reduce_term_fee,0) + nvl(reduce_svc_fee,0) + nvl(reduce_penalty,0))                                                         as reduce_interest_fee,
    sum(nvl(reduce_term_fee,0) + nvl(reduce_svc_fee,0))                                                                                                          as reduce_svc_term,
    sum(nvl(reduce_svc_fee,0))                                                                                                                                   as reduce_svc_fee,
    sum(nvl(reduce_mult_amt,0))                                                                                                                                  as reduce_mult_amt,
    sum(nvl(reduce_penalty,0))                                                                                                                                   as reduce_penalty,
    sum(nvl(reduce_amount,0))                                                                                                                                    as reduce_amount
from
  (
     select
       product_id,
       due_bill_no,
       loan_active_date,
       loan_init_term,
       should_repay_date,
       loan_init_principal,
       paid_interest,
       paid_term_fee,
       paid_svc_fee,
       paid_penalty,
       paid_mult,
       reduce_principal,
       reduce_interest,
       reduce_term_fee,
       reduce_svc_fee,
       reduce_penalty,
       reduce_mult_amt,
       reduce_amount
     from ods${db_suffix}.repay_schedule
     where 1 > 0
       and (date_sub('${ST9}',1) between s_d_date and date_sub(e_d_date,1))
       and loan_term=0 and loan_active_date<='${ST9}'
       ${hive_param_str}
  )tmp  where 1>0
group by product_id,loan_active_date,loan_init_term,due_bill_no
)paid_penalty_zero
on should_principal.should_repay_due_bill_no=paid_penalty_zero.paid_due_bill_no
group by should_principal.product_id,should_principal.loan_active_date,should_principal.loan_init_term,should_principal.should_repay_date
-- limit 10

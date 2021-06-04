--CREATE TABLE IF NOT EXISTS `dw${db_suffix}.dw_loan_base_stat_repay_detail_day`(
--  `loan_terms`                            decimal(3,0)  COMMENT '贷款期数',
--  `loan_active_date`                      string        COMMENT '放款日',
--
--  `repaid_num`                            decimal(11,0) COMMENT '实还借据数',
--  `repaid_num_count`                      decimal(11,0) COMMENT '累计实还借据数',
--
--  `repaid_amount`                         decimal(15,4) COMMENT '实还金额（实还本金+实还息费）',
--  `repaid_principal`                      decimal(15,4) COMMENT '实还本金',
--  `repaid_interest_penalty_svc_fee`       decimal(15,4) COMMENT '实还息费（实还利息+实还费用+实还罚金）',
--  `repaid_interest`                       decimal(15,4) COMMENT '实还利息',
--  `repaid_repay_svc_term`                 decimal(15,4) COMMENT '实还费用（实还手续费+实还服务费）',
--  `repaid_repay_term_fee`                 decimal(15,4) COMMENT '实还手续费',
--  `repaid_repay_svc_fee`                  decimal(15,4) COMMENT '实还服务费',
--  `repaid_penalty`                        decimal(15,4) COMMENT '实还罚金',
--  `repaid_amount_count`                   decimal(15,4) COMMENT '累计回款金额（累计回款本金+累计回款息费）',
--  `repaid_principal_count`                decimal(15,4) COMMENT '累计回款本金',
--  `repaid_interest_penalty_svc_fee_count` decimal(15,4) COMMENT '累计回款息费（累计回款利息+累计回款费用+累计回款罚金）',
--  `repaid_interest_count`                 decimal(15,4) COMMENT '累计回款利息',
--  `repaid_repay_svc_term_count`           decimal(15,4) COMMENT '累计回款费用（累计回款手续费+累计回款服务费）',
--  `repaid_repay_term_fee_count`           decimal(15,4) COMMENT '累计回款手续费',
--  `repaid_repay_svc_fee_count`            decimal(15,4) COMMENT '累计回款服务费',
--  `repaid_penalty_count`                  decimal(15,4) COMMENT '累计回款罚金',
--
--  `settle_num`                           decimal(15,0) COMMENT '当日结清借据数',
--  `settle_count`                         decimal(15,0) COMMENT '累计结清借据数',
--  `settle_loan_days`                     decimal(15,0) COMMENT '当天结清借据总用信天数(从放款到结清天数)',
--  `settle_loan_days_count`               decimal(15,0) COMMENT '累计用信天数(从放款到结清天数)',
--  `prepay_principal`                     decimal(15,4) COMMENT '当日提前还款金额'
--) COMMENT '实还统计 - 日级'
--PARTITIONED BY (`biz_date` string COMMENT '实还日期',`product_id` string COMMENT '产品编号')  
--STORED AS PARQUET;  

--set hivevar:db_suffix=;
set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=128000000;      -- 128M
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

set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10;
set hive.auto.convert.join=false;
--set hivevar:db_suffix=;
--set hivevar:ST9=2021-03-07;
--SET hivevar:hive_param_str=;

insert overwrite table dw${db_suffix}.dw_loan_base_stat_repay_detail_day partition(biz_date = '${ST9}',product_id)

select
  coalesce(loan_init_term,loan_init_term_settle,loan_init_term_schedule)                                      as loan_terms,
  coalesce(loan_active_date,loan_active_date_settle,loan_active_date_schedule )                               as loan_active_date,
  
  nvl(repaid_num,0)                                                                                           as repaid_num,
  nvl(repaid_num_count,0)                                                                                     as repaid_num_count,
  
  nvl(repaid_amount,0)                                                                                        as repaid_amount,
  nvl(repaid_principal,0)                                                                                     as repaid_principal,
  nvl(repaid_interest_penalty_svc_fee,0)                                                                      as repaid_interest_penalty_svc_fee,
  nvl(repaid_interest,0)                                                                                      as repaid_interest,
  nvl(repaid_repay_svc_term,0)                                                                                as repaid_repay_svc_term,
  nvl(repaid_repay_term_fee,0)                                                                                as repaid_repay_term_fee,
  nvl(repaid_repay_svc_fee,0)                                                                                 as repaid_repay_svc_fee,
  nvl(repaid_penalty,0)                                                                                       as repaid_penalty,

  nvl(repaid_amount_count,0)                                                                                  as repaid_amount_count,
  nvl(repaid_principal_count,0)                                                                               as repaid_principal_count,
  nvl(repaid_interest_penalty_svc_fee_count,0)                                                                as repaid_interest_penalty_svc_fee_count,
  nvl(repaid_interest_count,0)                                                                                as repaid_interest_count,
  nvl(repaid_repay_svc_term_count,0)                                                                          as repaid_repay_svc_term_count,
  nvl(repaid_repay_term_fee_count,0)                                                                          as repaid_repay_term_fee_count,
  nvl(repaid_repay_svc_fee_count,0)                                                                           as repaid_repay_svc_fee_count,
  nvl(repaid_penalty_count,0)                                                                                 as repaid_penalty_count,
  
  nvl(settle_num      , 0)                                                                                    as settle_num,
  nvl(settle_count    , 0)                                                                                    as settle_count,
  nvl(settle_loan_days, 0)                                                                                    as settle_loan_days,
  nvl(settle_loan_days_count, 0)                                                                              as settle_loan_days_count,
  nvl(prepay_principal,0)                                                                                     as prepay_principal,

  coalesce(product_id,product_id_settle,product_id_schedule)                                                  as product_id
from 
(
  select
    loan_init_term                                                                    as loan_init_term,
    loan_active_date                                                                  as loan_active_date,
    
    
    repaid_num_count                                                                  as repaid_num_count,
    pricinpal_count + interest_count + term_fee_count + svc_fee_count + penalty_count as repaid_amount_count,
    pricinpal_count                                                                   as repaid_principal_count,
    interest_count + term_fee_count + svc_fee_count + penalty_count                   as repaid_interest_penalty_svc_fee_count,
    interest_count                                                                    as repaid_interest_count,
    term_fee_count + svc_fee_count                                                    as repaid_repay_svc_term_count,
    term_fee_count                                                                    as repaid_repay_term_fee_count,
    svc_fee_count                                                                     as repaid_repay_svc_fee_count,
    penalty_count                                                                     as repaid_penalty_count,
    
    repaid_num                                                                        as repaid_num,
    pricinpal + interest + term_fee + svc_fee + penalty                               as repaid_amount,
    pricinpal                                                                         as repaid_principal,
    interest + term_fee + svc_fee + penalty                                           as repaid_interest_penalty_svc_fee,
    interest                                                                          as repaid_interest,
    term_fee + svc_fee                                                                as repaid_repay_svc_term,
    term_fee                                                                          as repaid_repay_term_fee,
    svc_fee                                                                           as repaid_repay_svc_fee,
    penalty                                                                           as repaid_penalty,
    product_id                                                                        as product_id
  from (
    select
      loan.loan_init_term,
      loan.loan_active_date,
      
      count(distinct detail.due_bill_no)                                                             as repaid_num_count,
      sum(case detail.bnp_type when 'Pricinpal' then detail.repay_amount else 0 end)                 as pricinpal_count,
      sum(case detail.bnp_type when 'Interest'  then detail.repay_amount else 0 end)                 as interest_count,
      sum(case detail.bnp_type when 'TERMFee'   then detail.repay_amount else 0 end)                 as term_fee_count,
      sum(case detail.bnp_type when 'SVCFee'    then detail.repay_amount else 0 end)                 as svc_fee_count,
      sum(case detail.bnp_type when 'Penalty'   then detail.repay_amount else 0 end)                 as penalty_count,
                                                                                                     
      count(distinct( if(biz_date = '${ST9}', detail.due_bill_no,null)))                             as repaid_num,
      sum(case when bnp_type = 'Pricinpal' and biz_date = '${ST9}' then repay_amount else 0 end)     as pricinpal,
      sum(case when bnp_type = 'Interest'  and biz_date = '${ST9}' then repay_amount else 0 end)     as interest,
      sum(case when bnp_type = 'TERMFee'   and biz_date = '${ST9}' then repay_amount else 0 end)     as term_fee,
      sum(case when bnp_type = 'SVCFee'    and biz_date = '${ST9}' then repay_amount else 0 end)     as svc_fee,
      sum(case when bnp_type = 'Penalty'   and biz_date = '${ST9}' then repay_amount else 0 end)     as penalty,
      
      loan.product_id
    from ods${db_suffix}.repay_detail detail
    join (select due_bill_no,product_id,loan_init_term,loan_active_date from ods${db_suffix}.loan_info where '${ST9}' between s_d_date and date_sub(e_d_date, 1) ${hive_param_str})    loan
    on detail.due_bill_no = loan.due_bill_no
    and detail.product_id = loan.product_id
    where 1 > 0
      and detail.biz_date <= '${ST9}'
    group by loan.loan_init_term,loan.product_id,loan.loan_active_date
  ) as tmp
) as loan_active_current_past
full join
(
select
    product_id                                                               as product_id_settle
    ,loan_init_term                                                          as loan_init_term_settle
    ,loan_active_date                                                        as loan_active_date_settle
    ,sum(if(paid_out_date = '${ST9}',1,0))                                   as settle_num                  --'当日结清借据数'
    ,count(due_bill_no)                                                      as settle_count                --'累计结清借据数'
    ,sum(if(paid_out_date = '${ST9}',datediff('${ST9}',loan_active_date),0)) as settle_loan_days            --'用信天数(从放款到结清天数)'
    ,sum(datediff('${ST9}',loan_active_date))                                as settle_loan_days_count      --'累计用信天数(从放款到结清天数)'
from
    ods${db_suffix}.loan_info
where '${ST9}' BETWEEN  s_d_date and date_sub(e_d_date, 1)
    and paid_out_date <= '${ST9}' ${hive_param_str}
group by
    product_id
    ,loan_active_date
    ,loan_init_term
)  settle_info
on  product_id     = product_id_settle
and loan_init_term = loan_init_term_settle
and loan_active_date = loan_active_date_settle
full join
(
select
    product_id             as product_id_schedule
    ,loan_init_term        as loan_init_term_schedule
    ,loan_active_date      as loan_active_date_schedule
    ,sum(paid_principal)   as prepay_principal                     --当日提前还款金额
from
    ods${db_suffix}.repay_schedule
where '${ST9}' BETWEEN  s_d_date and date_sub(e_d_date, 1)
    and paid_out_date = '${ST9}'
    and loan_term != 0
    and loan_active_date <= '${ST9}'
    and paid_out_date < start_interest_date
group by 
    product_id
    ,loan_init_term
    ,loan_active_date
) prepay_schedule
on  product_id     = product_id_schedule
and loan_init_term = loan_init_term_schedule 
and loan_active_date = loan_active_date_schedule
;


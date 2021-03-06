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
-- 关闭自动 MapJoin
set hive.auto.convert.join=false;

--set hivevar:ST9=2020-10-13;
--set hivevar:suffix=;


insert overwrite table dm_eagle${suffix}.operation_overdue_base_detail partition (product_id,snapshot_day)
select
    biz.channel_id                                                          as channel_id,
    biz.project_id                                                          as project_id,
    biz.product_name                                                        as product_name,
    biz.project_name                                                        as project_name,
    loan.due_bill_no                                                        as due_bill_no,
    lending.contract_no                                                     as contract_no,
    lending.loan_active_date                                                as loan_active_date,
    lending.loan_expire_date                                                as loan_expire_date,
    loan.loan_init_principal                                                as loan_init_principal,
    lending.cycle_day                                                       as cycle_day,
    case
    when overdue_days =0   then 'C'
    when overdue_days >0   and overdue_days <=30  then 'M1'--m1
    when overdue_days >30  and overdue_days <=60  then 'M2'--m2
    when overdue_days >60  and overdue_days <=90  then 'M3'--m3
    when overdue_days >90  and overdue_days <=120 then 'M4'--m4
    when overdue_days >120 and overdue_days <=150 then 'M5'--m5
    when overdue_days >150 and overdue_days <=180 then 'M6'--m6
    when overdue_days >180 then 'M7+'--m7
    else 'M7+'
    end                                                                     as curr_overdue_stage,
    overdue_date_first                                                      as overdue_date_first,
    overdue_date_start                                                      as overdue_date_start,
    overdue_terms_count                                                     as overdue_terms_count,
    overdue_term                                                            as overdue_term,
    overdue_days                                                            as overdue_days,
    (nvl(overdue_principal, 0) +
    nvl(overdue_interest , 0) +
    nvl(overdue_svc_fee  , 0) +
    nvl(overdue_term_fee , 0) +
    nvl(overdue_penalty  , 0) +
    nvl(overdue_mult_amt , 0))                                              as overdue_amount,
    overdue_principal                                                       as overdue_principal,
    overdue_interest                                                        as overdue_interest,
    (nvl(overdue_svc_fee,0)+nvl(overdue_term_fee,0))                        as overdue_fee,
    overdue_penalty                                                         as overdue_penalty,
    remain_amount                                                           as remain_amount,
    remain_principal                                                        as remain_principal,
    remain_interest                                                         as remain_interest,
    (nvl(remain_svc_fee, 0) + nvl(remain_term_fee,0))                       as remain_fee,
    remain_othamounts                                                       as remain_penalty_interest,
    remain_amount_cps                                                       as remain_amount_cps,
    remain_principal_cps                                                    as remain_principal_cps,
    remain_interest_cps                                                     as remain_interest_cps,
    (nvl(remain_svc_fee_cps, 0) + nvl(remain_term_fee_cps,0))               as remain_fee_cps,
    remain_othamounts_cps                                                   as remain_penalty_interest_cps,
    if(buy_back_count > 0,'Y','N')                                          as is_buy_back,
    loan.product_id                                                         as product_id,
    '${ST9}'                                                                as snapshot_day
from
(
    select
        due_bill_no,product_id,paid_out_type,loan_init_principal,overdue_days,
        overdue_date_first,overdue_date_start,overdue_terms_count,
        overdue_term,overdue_principal,overdue_interest,
        overdue_svc_fee,overdue_term_fee,overdue_penalty,overdue_mult_amt,
        remain_amount,remain_principal,remain_interest,remain_svc_fee,remain_term_fee,remain_othAmounts
    from ods${suffix}.loan_info where '${ST9}' between s_d_date and date_sub(e_d_date,1)
    and overdue_days > 0
) loan
inner join
(
    select
      distinct
      channel_id,
      project_id,
      product_id,
      product_name,
      project_name
      from
      (
        select
           max(if(col_name = 'channel_id',     col_val,null)) as channel_id,
           max(if(col_name = 'project_id',     col_val,null)) as project_id,
           max(if(col_name = 'product_id',     col_val,null)) as product_id,
           max(if(col_name = 'product_name',   col_val,null)) as product_name,
           max(if(col_name = 'project_name',   col_val,null)) as project_name
         from dim.data_conf
         where col_type = 'ac'
         group by col_id
      ) tmp
      where  project_id in ('WS0012200001','WS0009200001','WS0006200001','WS0006200002','WS0006200003','WS0013200001')
) biz
on loan.product_id = biz.product_id
left join ods${suffix}.loan_lending lending
on loan.due_bill_no = lending.due_bill_no
left join
(
    select
        due_bill_no as due_bill_no_cps,remain_amount as remain_amount_cps,remain_principal as remain_principal_cps
        ,remain_interest as remain_interest_cps,remain_svc_fee as remain_svc_fee_cps,remain_term_fee as
        remain_term_fee_cps,remain_othamounts as remain_othamounts_cps
    from ods_cps${suffix}.loan_info where '${ST9}' between s_d_date and date_sub(e_d_date,1)
) loan_cps
on loan.due_bill_no = loan_cps.due_bill_no_cps
left join
(
    select due_bill_no as due_bill_no_schedule,count(if(paid_out_type='BUY_BACK',1,null)) as buy_back_count
    from ods_cps${suffix}.repay_schedule where '${ST9}' between s_d_date and date_sub(e_d_date,1)
    group by due_bill_no
) repay_schedule
on loan.due_bill_no = repay_schedule.due_bill_no_schedule
;
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


insert overwrite table ods${db_suffix}.repay_schedule_inter partition(biz_date,product_id)
select
  repay_schedule.due_bill_no,
  ecas_loan.loan_active_date,
  repay_schedule.loan_init_principal,
  repay_schedule.loan_init_term,
  repay_schedule.loan_term,
  repay_schedule.start_interest_date,
  repay_schedule.curr_bal,
  repay_schedule.should_repay_date,
  repay_schedule.should_repay_date_history,
  repay_schedule.grace_date,
  nvl(repay_schedule.should_repay_amount,0),
  nvl(repay_schedule.should_repay_principal,0),
  nvl(repay_schedule.should_repay_interest,0),
  nvl(repay_schedule.should_repay_term_fee,0),
  nvl(repay_schedule.should_repay_svc_fee,0),
  nvl(repay_schedule.should_repay_penalty,0),
  nvl(repay_schedule.should_repay_mult_amt,0),
  nvl(repay_schedule.should_repay_penalty_acru,0),
  repay_schedule.schedule_status,
  repay_schedule.schedule_status_cn,
  null as repay_status,
  repay_schedule.paid_out_date,
  repay_schedule.paid_out_type,
  repay_schedule.paid_out_type_cn,
  nvl(repay_schedule.paid_amount,0),
  nvl(repay_schedule.paid_principal,0),
  nvl(repay_schedule.paid_interest,0),
  nvl(repay_schedule.paid_term_fee,0),
  nvl(repay_schedule.paid_svc_fee,0),
  nvl(repay_schedule.paid_penalty,0),
  nvl(repay_schedule.paid_mult,0),
  nvl(repay_schedule.reduce_amount,0),
  nvl(repay_schedule.reduce_principal,0),
  nvl(repay_schedule.reduce_interest,0),
  nvl(repay_schedule.reduce_term_fee,0),
  nvl(repay_schedule.reduce_svc_fee,0),
  nvl(repay_schedule.reduce_penalty,0),
  nvl(repay_schedule.reduce_mult_amt,0),
  repay_schedule.d_date as effective_date,
  repay_schedule.create_time,
  repay_schedule.update_time,
  repay_schedule.d_date,
  repay_schedule.product_id
    from (
             select
                 product_code                                                                             as product_id,
                 schedule_id                                                                              as schedule_id,
                 out_side_schedule_no                                                                     as out_side_schedule_no,
                 schedule.due_bill_no                                                                              as due_bill_no,
                 loan_init_prin                                                                           as loan_init_principal,
                 loan_init_term                                                                           as loan_init_term,
                 schedule.curr_term                                                                                as loan_term,
                 start_interest_date                                                                      as start_interest_date,
                 curr_bal                                                                                 as curr_bal,
                 nvl(origin_pmt_due_date,pmt_due_date)                                                    as should_repay_date,
                 origin_pmt_due_date                                                                      as should_repay_date_history,
                 grace_date                                                                               as grace_date,
                 (due_term_prin +
                 if(product_code='DIDI201908161538' and due_bill_no='DD0002303620191024080400c3a57e' and curr_term=3 and d_date between '2019-10-24' and '2019-11-29',0,due_term_int)
                 + due_term_fee + due_svc_fee + due_penalty + due_mult_amt) as should_repay_amount,
                 due_term_prin                                                                            as should_repay_principal,
                 case
                     when product_code='DIDI201908161538' and due_bill_no='DD0002303620191024080400c3a57e' and curr_term=3 and d_date between '2019-10-24' and '2019-11-29' then 0
                 else due_term_int end                                                                    as should_repay_interest,
                 nvl(due_term_fee,0)                                                                             as should_repay_term_fee,
                 nvl(due_svc_fee ,0)                                                                             as should_repay_svc_fee,
                 nvl(due_penalty ,0)                                                                             as should_repay_penalty,
                 nvl(due_mult_amt,0)                                                                             as should_repay_mult_amt,
                 nvl(penalty_acru,0)                                                                             as should_repay_penalty_acru,
                 case
                     when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and '${ST9}' then 'F'
                     when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and '${ST9}' then 'F'
                     when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date between '2020-06-07' and '${ST9}' then 'F'
                     when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date between '2020-06-07' and '${ST9}' then 'F'
                     when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date between '2020-06-07' and '${ST9}' then 'F'
                     when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date between '2020-06-07' and '${ST9}' then 'F'
                     when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and '${ST9}' then 'F'
                     when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and '${ST9}' then 'F'
                     when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date between '2020-06-07' and '${ST9}' then 'F'
                     when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and '${ST9}' then 'F'
                     when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and '${ST9}' then 'F'
                     when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-07' and '${ST9}' then 'F'
                     when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-07' and '${ST9}' then 'F'
                     when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-07' and '${ST9}' then 'F'
                     when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-07' and '${ST9}' then 'F'
                     when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-07' and '${ST9}' then 'F'
                     when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620191024080400c3a57e' and d_date='2019-11-29' then 'F'
                     when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620191206211600a2e46a' and d_date='2019-12-06' and curr_term=1 then 'F'
                     when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620200106174600d0fffd' and d_date='2020-01-12' and curr_term=1 then 'F'
                     when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620191231062500b66bbe' and d_date='2020-01-15' and curr_term=1 then 'F'
                     when nvl(repayhst.repayhst_paid_principal,0) >= due_term_prin then 'F'
                     when '${ST9}' < pmt_due_date then 'N'
                     when '${ST9}' >= pmt_due_date then 'O'
                     else schedule_status end                                                                 as schedule_status,
                 case
                     case
                         when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and '${ST9}' then 'F'
                         when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and '${ST9}' then 'F'
                         when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date between '2020-06-07' and '${ST9}' then 'F'
                         when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date between '2020-06-07' and '${ST9}' then 'F'
                         when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date between '2020-06-07' and '${ST9}' then 'F'
                         when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date between '2020-06-07' and '${ST9}' then 'F'
                         when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and '${ST9}' then 'F'
                         when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and '${ST9}' then 'F'
                         when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date between '2020-06-07' and '${ST9}' then 'F'
                         when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and '${ST9}' then 'F'
                         when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and '${ST9}' then 'F'
                         when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-07' and '${ST9}' then 'F'
                         when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-07' and '${ST9}' then 'F'
                         when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-07' and '${ST9}' then 'F'
                         when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-07' and '${ST9}' then 'F'
                         when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-07' and '${ST9}' then 'F'
                         when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620191024080400c3a57e' and d_date='2019-11-29' then 'F'
                         when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620191206211600a2e46a' and d_date='2019-12-06' and curr_term=1 then 'F'
                         when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620200106174600d0fffd' and d_date='2020-01-12' and curr_term=1 then 'F'
                         when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620191231062500b66bbe' and d_date='2020-01-15' and curr_term=1 then 'F'
                         else schedule_status end
                     when 'N' then '正常'
                     when 'O' then '逾期'
                     when 'F' then '已还清'
                     else schedule.schedule_status end                                                                 as schedule_status_cn,
                 case
                     when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and '${ST9}' then '2020-06-06'
                     when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and '${ST9}' then '2020-06-06'
                     when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date between '2020-06-07' and '${ST9}' then '2020-06-05'
                     when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date between '2020-06-07' and '${ST9}' then '2020-06-05'
                     when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date between '2020-06-07' and '${ST9}' then '2020-06-05'
                     when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date between '2020-06-07' and '${ST9}' then '2020-06-05'
                     when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and '${ST9}' then '2020-06-06'
                     when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and '${ST9}' then '2020-06-06'
                     when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date between '2020-06-07' and '${ST9}' then '2020-06-05'
                     when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and '${ST9}' then '2020-06-06'
                     when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and '${ST9}' then '2020-06-06'
                     when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-07' and '${ST9}' then '2020-06-06'
                     when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-07' and '${ST9}' then '2020-06-06'
                     when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-07' and '${ST9}' then '2020-06-06'
                     when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-07' and '${ST9}' then '2020-06-06'
                     when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-07' and '${ST9}' then '2020-06-06'
                     when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620191024080400c3a57e' and d_date='2019-11-29' and curr_term=2 then '2019-11-29'
                     when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620191024080400c3a57e' and d_date='2019-11-29' and curr_term=3 then '2019-11-29'
                     when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620191206211600a2e46a' and d_date='2019-12-06' and curr_term=1 then '2019-12-06'
                     when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620200106174600d0fffd' and d_date='2020-01-12' and curr_term=1 then '2020-01-12'
                     when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620191231062500b66bbe' and d_date='2020-01-15' and curr_term=1 then '2020-01-15'
                     --拿最新的还款计划 修复快照日内的结清日期
                     when schedule.d_date>=new_schedule.paid_out_date then new_schedule.paid_out_date
                     when schedule.paid_out_date!=new_schedule.paid_out_date and schedule.paid_out_date is not null then new_schedule.paid_out_date
                     else schedule.paid_out_date end                                                                                                        as paid_out_date,
                 case
                     when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and '${ST9}' then 'PRE_SETTLE'
                     when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and '${ST9}' then 'PRE_SETTLE'
                     when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                     when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                     when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                     when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                     when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and '${ST9}' then 'PRE_SETTLE'
                     when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and '${ST9}' then 'PRE_SETTLE'
                     when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                     when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and '${ST9}' then 'PRE_SETTLE'
                     when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and '${ST9}' then 'PRE_SETTLE'
                     when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                     when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                     when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                     when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                     when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                     else if(paid_out_type = 'BUY_BACKAV','BUY_BACK',paid_out_type) end                       as paid_out_type,
                 case
                     case
                         when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and '${ST9}' then 'PRE_SETTLE'
                         when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and '${ST9}' then 'PRE_SETTLE'
                         when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                         when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                         when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                         when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                         when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and '${ST9}' then 'PRE_SETTLE'
                         when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and '${ST9}' then 'PRE_SETTLE'
                         when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                         when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and '${ST9}' then 'PRE_SETTLE'
                         when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and '${ST9}' then 'PRE_SETTLE'
                         when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                         when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                         when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                         when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                         when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                         else if(paid_out_type = 'BUY_BACKAV','BUY_BACK',paid_out_type) end
                     when 'BANK_REF'          then '退票结清'
                     when 'BUY_BACK'          then '资产回购'
                     when 'CAPITAL_VERI'      then '资产核销'
                     when 'DISPOSAL'          then '处置结束'
                     when 'NORMAL_SETTLE'     then '正常结清'
                     when 'OVER_COMP'         then '逾期代偿'
                     when 'OVERDUE_SETTLE'    then '逾期结清'
                     when 'PRE_SETTLE'        then '提前结清'
                     when 'REDEMPTION'        then '赎回'
                     when 'REFUND'            then '退车'
                     when 'REFUND_SETTLEMENT' then '退票结清'
                     else if(paid_out_type = 'BUY_BACKAV','BUY_BACK',paid_out_type) end                       as paid_out_type_cn,
                 nvl(repayhst_paid_principal,0) +
                 nvl(repayhst_paid_interest, 0) +
                 coalesce(repayhst_paid_term_fee, paid_term_fee, 0) +
                 coalesce(repayhst_paid_svc_fee,  paid_svc_fee,  0) +
                 coalesce(repayhst_paid_penalty,  paid_penalty,  0) +
                 coalesce(repayhst_paid_mult,     paid_mult_amt, 0)                                       as paid_amount,
                 nvl(repayhst.repayhst_paid_principal,0)                                                  as paid_principal,
                 nvl(repayhst.repayhst_paid_interest,0)                                                   as paid_interest,
                 coalesce(repayhst_paid_term_fee, paid_term_fee, 0)                                       as paid_term_fee,
                 coalesce(repayhst_paid_svc_fee,  paid_svc_fee,  0)                                       as paid_svc_fee,
                 coalesce(repayhst_paid_penalty,  paid_penalty,  0)                                       as paid_penalty,
                 coalesce(repayhst_paid_mult,     paid_mult_amt, 0)                                       as paid_mult,
                 nvl(reduce_term_prin,0) + nvl(reduce_term_int,0) + nvl(reduce_term_fee,0) + nvl
                 (reduce_svc_fee,0) + nvl(reduce_penalty,0) + nvl(reduce_mult_amt,0)                      as reduce_amount,
                 nvl(reduce_term_prin,0)                                                                         as reduce_principal,
                 nvl(reduce_term_int ,0)                                                                         as reduce_interest,
                 nvl(reduce_term_fee ,0)                                                                         as reduce_term_fee,
                 nvl(reduce_svc_fee  ,0)                                                                         as reduce_svc_fee,
                 nvl(reduce_penalty  ,0)                                                                         as reduce_penalty,
                 nvl(reduce_mult_amt ,0)                                                                         as reduce_mult_amt,
                 is_empty(create_time,create_user)                                                        as create_time,
                 is_empty(lst_upd_time,lst_upd_user)                                                      as update_time,
                 d_date                                                                                   as d_date
             from (
             select
                    *,
                    concat(due_bill_no,'::',curr_term) as due_bill_no_curr_term
             from stage.ecas_repay_schedule${tb_suffix}
             where d_date = '${ST9}' and p_type in ('ddht') and  product_code in (${product_id})
             and due_bill_no not in ('DD000230362019111701130061a514','DD000230362019121511470083cc26','DD0002303620200111210700a1ceb3',
             'DD0002303620200206232600de511b','DD0002303620200208195000ae8354','DD0002303620200307225700e545de',
             'DD0002303620200308222600ebbea9','DD00023036202003191421004c1251','DD0002303620200404090400770962',
             'DD0002303620200409104700bfb018')
             ) as schedule
             left join (
             select
                 repayhst_due_bill_no,
                 repayhst_due_bill_no_term,
                 repayhst_paid_principal,
                 repayhst_paid_interest,
                 repayhst_paid_term_fee,
                 repayhst_paid_svc_fee,
                 repayhst_paid_penalty,
                 repayhst_paid_mult
             from (
                 select
                     due_bill_no                                                     as repayhst_due_bill_no,
                     concat(due_bill_no,'::',term)                                   as repayhst_due_bill_no_term,
                     sum(if(bnp_type = 'Pricinpal',        repay_amt,0)) - case
                       when due_bill_no = '11200709120939931 72613' and d_date in ('2020-07-10','2020-07-11','2020-07-12','2020-07-13') then 2500
                       when due_bill_no = '1120061910384241252747' then 200 else 0 end as repayhst_paid_principal,
                     sum(if(bnp_type = 'Interest',         repay_amt,0))             as repayhst_paid_interest,
                     sum(if(bnp_type = 'TERMFee',          repay_amt,0))             as repayhst_paid_term_fee,
                     sum(if(bnp_type = 'SVCFee',           repay_amt,0))             as repayhst_paid_svc_fee,
                     sum(if(bnp_type = 'Penalty',          repay_amt,0))             as repayhst_paid_penalty,
                     sum(if(bnp_type = 'LatePaymentCharge',repay_amt,0))             as repayhst_paid_mult
                     from stage.ecas_repay_hst${tb_suffix}
                     where 1 > 0
                       and d_date='${d_date}'
                       and p_type in ('ddht')
                       and txn_date <= '${ST9}'
                     group by d_date,concat(due_bill_no,'::',term),due_bill_no
                 ) temp
             ) as repayhst
             on  due_bill_no_curr_term = repayhst_due_bill_no_term
            left join
            (
                select paid_out_date,concat(due_bill_no,'::',curr_term) as new_due_bill_no_curr_term
                from stage.ecas_repay_schedule where d_date='${d_date}'  and p_type='ddht' and  product_code in (${product_id})
                and paid_out_date is not null and paid_out_date<=date_add('${ST9}',10)
            )new_schedule on due_bill_no_curr_term = new_due_bill_no_curr_term
         ) as repay_schedule
        left join (
        select
            product_code                                              as product_id,
            schedule_id                                               as schedule_id,
            schedule_yest.due_bill_no                                               as due_bill_no,
            loan_init_prin                                            as loan_init_principal,
            loan_init_term                                            as loan_init_term,
            schedule_yest.curr_term                                                 as loan_term,
            start_interest_date                                       as start_interest_date,
            curr_bal                                                  as curr_bal,
            nvl(origin_pmt_due_date,pmt_due_date)                     as should_repay_date,
            origin_pmt_due_date                                       as should_repay_date_history,
            grace_date                                                as grace_date,
            nvl(due_term_prin,0)                                            as should_repay_principal,
            nvl(due_term_int ,0)                                            as should_repay_interest,
            nvl(due_term_fee ,0)                                            as should_repay_term_fee,
            nvl(due_svc_fee  ,0)                                            as should_repay_svc_fee,
            nvl(due_penalty  ,0)                                            as should_repay_penalty,
            nvl(due_mult_amt ,0)                                            as should_repay_mult_amt,
            nvl(penalty_acru ,0)                                            as should_repay_penalty_acru,
            case
                when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and '${ST9}' then 'F'
                when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and '${ST9}' then 'F'
                when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date between '2020-06-07' and '${ST9}' then 'F'
                when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date between '2020-06-07' and '${ST9}' then 'F'
                when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date between '2020-06-07' and '${ST9}' then 'F'
                when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date between '2020-06-07' and '${ST9}' then 'F'
                when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and '${ST9}' then 'F'
                when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and '${ST9}' then 'F'
                when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date between '2020-06-07' and '${ST9}' then 'F'
                when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and '${ST9}' then 'F'
                when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and '${ST9}' then 'F'
                when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-07' and '${ST9}' then 'F'
                when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-07' and '${ST9}' then 'F'
                when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-07' and '${ST9}' then 'F'
                when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-07' and '${ST9}' then 'F'
                when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-07' and '${ST9}' then 'F'
                when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620191024080400c3a57e' and d_date='2019-11-29' then 'F'
                when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620191206211600a2e46a' and d_date='2019-12-06' and curr_term=1 then 'F'
                when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620200106174600d0fffd' and d_date='2020-01-12' and curr_term=1 then 'F'
                when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620191231062500b66bbe' and d_date='2020-01-15' and curr_term=1 then 'F'
                when nvl(repayhst_yest.repayhst_paid_principal,0) >= due_term_prin then 'F'
                when date_sub('${ST9}',1) < pmt_due_date then 'N'
                when date_sub('${ST9}',1) >= pmt_due_date then 'O'
                else schedule_yest.schedule_status end                                                                 as schedule_status,
            case
                when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and '${ST9}' then '2020-06-06'
                when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and '${ST9}' then '2020-06-06'
                when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date between '2020-06-07' and '${ST9}' then '2020-06-05'
                when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date between '2020-06-07' and '${ST9}' then '2020-06-05'
                when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date between '2020-06-07' and '${ST9}' then '2020-06-05'
                when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date between '2020-06-07' and '${ST9}' then '2020-06-05'
                when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and '${ST9}' then '2020-06-06'
                when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and '${ST9}' then '2020-06-06'
                when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date between '2020-06-07' and '${ST9}' then '2020-06-05'
                when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and '${ST9}' then '2020-06-06'
                when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and '${ST9}' then '2020-06-06'
                when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-07' and '${ST9}' then '2020-06-06'
                when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-07' and '${ST9}' then '2020-06-06'
                when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-07' and '${ST9}' then '2020-06-06'
                when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-07' and '${ST9}' then '2020-06-06'
                when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-07' and '${ST9}' then '2020-06-06'
                when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620191024080400c3a57e' and d_date='2019-11-29' and curr_term=2 then '2019-11-29'
                when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620191024080400c3a57e' and d_date='2019-11-29' and curr_term=3 then '2019-11-29'
                when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620191206211600a2e46a' and d_date='2019-12-06' and curr_term=1 then '2019-12-06'
                when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620200106174600d0fffd' and d_date='2020-01-12' and curr_term=1 then '2020-01-12'
                when product_code = 'DIDI201908161538' and due_bill_no='DD0002303620191231062500b66bbe' and d_date='2020-01-15' and curr_term=1 then '2020-01-15'
                 --拿最新的还款计划 修复快照日内的结清日期
                when schedule_yest.d_date>=new_schedule.paid_out_date then new_schedule.paid_out_date
                when schedule_yest.paid_out_date!=new_schedule.paid_out_date and schedule_yest.paid_out_date is not null then new_schedule.paid_out_date
                else schedule_yest.paid_out_date end                                                                                                        as paid_out_date,
            case
                when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and '${ST9}' then 'PRE_SETTLE'
                when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and '${ST9}' then 'PRE_SETTLE'
                when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and '${ST9}' then 'PRE_SETTLE'
                when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and '${ST9}' then 'PRE_SETTLE'
                when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and '${ST9}' then 'PRE_SETTLE'
                when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and '${ST9}' then 'PRE_SETTLE'
                when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                else if(paid_out_type = 'BUY_BACKAV','BUY_BACK',paid_out_type) end                       as paid_out_type,
            nvl(repayhst_yest.repayhst_paid_principal,0)                                                 as paid_principal,
            nvl(repayhst_yest.repayhst_paid_interest,0)                                                  as paid_interest,
            coalesce(repayhst_paid_term_fee, paid_term_fee, 0)        as paid_term_fee,
            coalesce(repayhst_paid_svc_fee,  paid_svc_fee,  0)        as paid_svc_fee,
            coalesce(repayhst_paid_penalty,  paid_penalty,  0)        as paid_penalty,
            coalesce(repayhst_paid_mult,     paid_mult_amt, 0)        as paid_mult,
            nvl(reduce_term_prin,0) + nvl(reduce_term_int,0) + nvl(reduce_term_fee,0) + nvl
            (reduce_svc_fee,0) + nvl(reduce_penalty,0) + nvl(reduce_mult_amt,0)                     as reduce_amount,
           nvl(reduce_term_prin,0)                                         as reduce_principal,
           nvl(reduce_term_int,0)                                         as reduce_interest,
           nvl(reduce_term_fee,0)                                         as reduce_term_fee,
           nvl(reduce_svc_fee ,0)                                         as reduce_svc_fee,
           nvl(reduce_penalty ,0)                                         as reduce_penalty,
           nvl(reduce_mult_amt,0)                                         as reduce_mult_amt
        from
        (select
            *,
            concat(due_bill_no,'::',curr_term) as due_bill_no_curr_term
        from stage.ecas_repay_schedule${tb_suffix}
        where d_date = date_sub('${ST9}',1) and p_type in ('ddht') and  product_code in (${product_id})
        and due_bill_no not in ('DD000230362019111701130061a514','DD000230362019121511470083cc26','DD0002303620200111210700a1ceb3',
        'DD0002303620200206232600de511b','DD0002303620200208195000ae8354','DD0002303620200307225700e545de',
        'DD0002303620200308222600ebbea9','DD00023036202003191421004c1251','DD0002303620200404090400770962',
        'DD0002303620200409104700bfb018')
         ) as schedule_yest
         left join
         (
         select
            repayhst_due_bill_no,
            repayhst_due_bill_no_term,
            repayhst_paid_principal,
            repayhst_paid_interest,
            repayhst_paid_term_fee,
            repayhst_paid_svc_fee,
            repayhst_paid_penalty,
            repayhst_paid_mult
         from (
            select
                due_bill_no                                                     as repayhst_due_bill_no,
                concat(due_bill_no,'::',term)                                   as repayhst_due_bill_no_term,
                sum(if(bnp_type = 'Pricinpal',        repay_amt,0)) - case
                      when due_bill_no = '1120070912093993172613' and d_date in ('2020-07-10','2020-07-11','2020-07-12','2020-07-13') then 2500
                      when due_bill_no = '1120061910384241252747' then 200 else 0 end as repayhst_paid_principal,
                sum(if(bnp_type = 'Interest',         repay_amt,0))             as repayhst_paid_interest,
                sum(if(bnp_type = 'TERMFee',          repay_amt,0))             as repayhst_paid_term_fee,
                sum(if(bnp_type = 'SVCFee',           repay_amt,0))             as repayhst_paid_svc_fee,
                sum(if(bnp_type = 'Penalty',          repay_amt,0))             as repayhst_paid_penalty,
                sum(if(bnp_type = 'LatePaymentCharge',repay_amt,0))             as repayhst_paid_mult
            from stage.ecas_repay_hst${tb_suffix}
            where 1 > 0
              -- and d_date = date_sub('${ST9}',1)
              and d_date = '${d_date}'
              and p_type in ('ddht')
              and txn_date <= date_sub('${ST9}',1)
            group by due_bill_no,term,d_date
            ) temp
        ) as repayhst_yest
        on due_bill_no_curr_term = repayhst_due_bill_no_term
        left join
        (
            select paid_out_date,concat(due_bill_no,'::',curr_term) as new_due_bill_no_curr_term
            from stage.ecas_repay_schedule where d_date='${d_date}'  and p_type='ddht' and  product_code in (${product_id})
            and paid_out_date is not null and paid_out_date<=date_add('${ST9}',10)
        )new_schedule on due_bill_no_curr_term = new_due_bill_no_curr_term
    ) as repay_schedule_tmp
    on concat_ws('::',
                   is_empty(repay_schedule.product_id,                   'a'),
                   is_empty(repay_schedule.due_bill_no,                  'a'),
                   is_empty(repay_schedule.loan_init_term,               'a'),
                   is_empty(repay_schedule.loan_term,                    'a'),
                   is_empty(repay_schedule.start_interest_date,          'a'),
                   is_empty(repay_schedule.should_repay_date,            'a'),
                   is_empty(repay_schedule.should_repay_date_history,    'a'),
                   is_empty(repay_schedule.grace_date,                   'a'),
                   is_empty(repay_schedule.schedule_status,              'a'),
                   is_empty(repay_schedule.paid_out_date,                'a'),
                   is_empty(repay_schedule.paid_out_type,                'a')
         ) =
         concat_ws('::',
                   is_empty(repay_schedule_tmp.product_id,               'a'),
                   is_empty(repay_schedule_tmp.due_bill_no,              'a'),
                   is_empty(repay_schedule_tmp.loan_init_term,           'a'),
                   is_empty(repay_schedule_tmp.loan_term,                'a'),
                   is_empty(repay_schedule_tmp.start_interest_date,      'a'),
                   is_empty(repay_schedule_tmp.should_repay_date,        'a'),
                   is_empty(repay_schedule_tmp.should_repay_date_history,'a'),
                   is_empty(repay_schedule_tmp.grace_date,               'a'),
                   is_empty(repay_schedule_tmp.schedule_status,          'a'),
                   is_empty(repay_schedule_tmp.paid_out_date,            'a'),
                   is_empty(repay_schedule_tmp.paid_out_type,            'a')
         )
          and is_empty(repay_schedule.loan_init_principal,      'a') = is_empty(repay_schedule_tmp.loan_init_principal,      'a')
          and is_empty(repay_schedule.curr_bal,                 'a') = is_empty(repay_schedule_tmp.curr_bal,                 'a')
          and is_empty(repay_schedule.should_repay_principal,   'a') = is_empty(repay_schedule_tmp.should_repay_principal,   'a')
          and is_empty(repay_schedule.should_repay_interest,    'a') = is_empty(repay_schedule_tmp.should_repay_interest,    'a')
          and is_empty(repay_schedule.should_repay_term_fee,    'a') = is_empty(repay_schedule_tmp.should_repay_term_fee,    'a')
          and is_empty(repay_schedule.should_repay_svc_fee,     'a') = is_empty(repay_schedule_tmp.should_repay_svc_fee,     'a')
          and is_empty(repay_schedule.should_repay_penalty,     'a') = is_empty(repay_schedule_tmp.should_repay_penalty,     'a')
          and is_empty(repay_schedule.should_repay_mult_amt,    'a') = is_empty(repay_schedule_tmp.should_repay_mult_amt,    'a')
          and is_empty(repay_schedule.should_repay_penalty_acru,'a') = is_empty(repay_schedule_tmp.should_repay_penalty_acru,'a')
          and is_empty(repay_schedule.paid_principal,           'a') = is_empty(repay_schedule_tmp.paid_principal,           'a')
          and is_empty(repay_schedule.paid_interest,            'a') = is_empty(repay_schedule_tmp.paid_interest,            'a')
          and is_empty(repay_schedule.paid_term_fee,            'a') = is_empty(repay_schedule_tmp.paid_term_fee,            'a')
          and is_empty(repay_schedule.paid_svc_fee,             'a') = is_empty(repay_schedule_tmp.paid_svc_fee,             'a')
          and is_empty(repay_schedule.paid_penalty,             'a') = is_empty(repay_schedule_tmp.paid_penalty,             'a')
          and is_empty(repay_schedule.paid_mult,                'a') = is_empty(repay_schedule_tmp.paid_mult,                'a')
          and is_empty(repay_schedule.reduce_amount,            'a') = is_empty(repay_schedule_tmp.reduce_amount,            'a')
          and is_empty(repay_schedule.reduce_principal,         'a') = is_empty(repay_schedule_tmp.reduce_principal,         'a')
          and is_empty(repay_schedule.reduce_interest,          'a') = is_empty(repay_schedule_tmp.reduce_interest,          'a')
          and is_empty(repay_schedule.reduce_term_fee,          'a') = is_empty(repay_schedule_tmp.reduce_term_fee,          'a')
          and is_empty(repay_schedule.reduce_svc_fee,           'a') = is_empty(repay_schedule_tmp.reduce_svc_fee,           'a')
          and is_empty(repay_schedule.reduce_penalty,           'a') = is_empty(repay_schedule_tmp.reduce_penalty,           'a')
          and is_empty(repay_schedule.reduce_mult_amt,          'a') = is_empty(repay_schedule_tmp.reduce_mult_amt,          'a')
  left join (
  select distinct
    due_bill_no  as ecas_loan_due_bill_no,
    active_date  as loan_active_date,
    product_code as product_id
  from stage.ecas_loan${tb_suffix}
  where 1 > 0
    and d_date = '${ST9}'
    and product_code in (${product_id})
) as ecas_loan
on  repay_schedule.product_id  = ecas_loan.product_id
and repay_schedule.due_bill_no = ecas_loan.ecas_loan_due_bill_no
    where repay_schedule_tmp.due_bill_no is null;








set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.shuffle.memoryFraction=0.6;        -- shuffle操作的内存占比
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.auto.convert.join=false;            -- 关闭自动 MapJoin
set hive.groupby.skewindata=true;
set hive.support.quoted.identifiers=None;
set hive.mapjoin.optimized.hashtable=false;
set hive.mapjoin.followby.gby.localtask.max.memory.usage=0.9;


-- DROP TABLE IF EXISTS `ods_new_s${db_suffix}.his_repay_schedule_repaired`;
CREATE TABLE IF NOT EXISTS `ods_new_s${db_suffix}.his_repay_schedule_repaired` like `ods_new_s${db_suffix}.repay_schedule`;

DROP TABLE IF EXISTS `ods_new_s${db_suffix}.his_repay_schedule_tmp_repaired`;
CREATE TABLE IF NOT EXISTS `ods_new_s${db_suffix}.his_repay_schedule_tmp_repaired` like `ods_new_s${db_suffix}.repay_schedule`;


insert overwrite table ods_new_s${db_suffix}.his_repay_schedule_tmp_repaired partition(is_settled = 'no',product_id)
select `(is_settled)?+.+`
from ods_new_s${db_suffix}.his_repay_schedule_repaired
where 1 > 0
  and is_settled = 'no'
  and product_id in (${product_id})
  and s_d_date < '${ST9}'
  ${due_bill_no}
  -- and to_date(effective_time) <= date_add('${ST9}',1)
-- distribute by rand(),loan_term,loan_active_date,should_repay_date
-- limit 1
;



with ods_new_s_repay_schedule as (
  select
    repay_schedule.product_id,
    repay_schedule.schedule_id,
    repay_schedule.due_bill_no,
    repay_schedule.loan_init_principal,
    repay_schedule.loan_init_term,
    repay_schedule.loan_term,
    repay_schedule.start_interest_date,
    repay_schedule.curr_bal,
    repay_schedule.should_repay_date,
    repay_schedule.should_repay_date_history,
    repay_schedule.grace_date,
    repay_schedule.should_repay_amount,
    repay_schedule.should_repay_principal,
    repay_schedule.should_repay_interest,
    repay_schedule.should_repay_term_fee,
    repay_schedule.should_repay_svc_fee,
    repay_schedule.should_repay_penalty,
    repay_schedule.should_repay_mult_amt,
    repay_schedule.should_repay_penalty_acru,
    repay_schedule.schedule_status,
    repay_schedule.schedule_status_cn,
    repay_schedule.paid_out_date,
    repay_schedule.paid_out_type,
    repay_schedule.paid_out_type_cn,
    repay_schedule.paid_amount,
    repay_schedule.paid_principal,
    repay_schedule.paid_interest,
    repay_schedule.paid_term_fee,
    repay_schedule.paid_svc_fee,
    repay_schedule.paid_penalty,
    repay_schedule.paid_mult,
    repay_schedule.reduce_amount,
    repay_schedule.reduce_principal,
    repay_schedule.reduce_interest,
    repay_schedule.reduce_term_fee,
    repay_schedule.reduce_svc_fee,
    repay_schedule.reduce_penalty,
    repay_schedule.reduce_mult_amt,
    repay_schedule.d_date,
    '3000-12-31' as e_d_date,
    cast(datefmt(repay_schedule.create_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as create_time,
    cast(datefmt(repay_schedule.update_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as update_time,
    cast('3000-12-31 00:00:00' as timestamp) as expire_time
  from (
    select
      product_code                                                                             as product_id,
      schedule_id                                                                              as schedule_id,
      due_bill_no                                                                              as due_bill_no,
      loan_init_prin                                                                           as loan_init_principal,
      loan_init_term                                                                           as loan_init_term,
      curr_term                                                                                as loan_term,
      start_interest_date                                                                      as start_interest_date,
      curr_bal                                                                                 as curr_bal,
      pmt_due_date                                                                             as should_repay_date,
      origin_pmt_due_date                                                                      as should_repay_date_history,
      grace_date                                                                               as grace_date,
      (due_term_prin + due_term_int + due_term_fee + due_svc_fee + due_penalty + due_mult_amt) as should_repay_amount,
      due_term_prin                                                                            as should_repay_principal,
      due_term_int                                                                             as should_repay_interest,
      due_term_fee                                                                             as should_repay_term_fee,
      due_svc_fee                                                                              as should_repay_svc_fee,
      due_penalty                                                                              as should_repay_penalty,
      due_mult_amt                                                                             as should_repay_mult_amt,
      penalty_acru                                                                             as should_repay_penalty_acru,
      case
      when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and '${ST9}' then 'F'
      when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and '${ST9}' then 'F'
      when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and '${ST9}' then 'F'
      when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and '${ST9}' then 'F'
      when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and '${ST9}' then 'F'
      when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and '${ST9}' then 'F'
      when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date between '2020-06-07' and '${ST9}' then 'F'
      when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date between '2020-06-07' and '${ST9}' then 'F'
      when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date between '2020-06-07' and '${ST9}' then 'F'
      when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date between '2020-06-07' and '${ST9}' then 'F'
      when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date between '2020-06-07' and '${ST9}' then 'F'
      when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-07' and '${ST9}' then 'F'
      when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-07' and '${ST9}' then 'F'
      when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-07' and '${ST9}' then 'F'
      when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-07' and '${ST9}' then 'F'
      when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-07' and '${ST9}' then 'F'
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
        else schedule_status end
      when 'N' then '正常'
      when 'O' then '逾期'
      when 'F' then '已还清'
      else schedule_status end                                                                 as schedule_status_cn,
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
      else paid_out_date end                                                                   as paid_out_date,
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
      if(due_bill_no_order is not null,due_term_prin,coalesce(repayhst_paid_principal,paid_term_pric,0)) +
      coalesce(repayhst_paid_interest, paid_term_int, 0) +
      coalesce(repayhst_paid_term_fee, paid_term_fee, 0) +
      coalesce(repayhst_paid_svc_fee,  paid_svc_fee,  0) +
      coalesce(repayhst_paid_penalty,  paid_penalty,  0) +
      coalesce(repayhst_paid_mult,     paid_mult_amt, 0)                                       as paid_amount,
      if(due_bill_no_order is not null,due_term_prin,coalesce(repayhst_paid_principal,paid_term_pric,0)) as paid_principal,
      coalesce(repayhst_paid_interest, paid_term_int, 0)                                       as paid_interest,
      coalesce(repayhst_paid_term_fee, paid_term_fee, 0)                                       as paid_term_fee,
      coalesce(repayhst_paid_svc_fee,  paid_svc_fee,  0)                                       as paid_svc_fee,
      coalesce(repayhst_paid_penalty,  paid_penalty,  0)                                       as paid_penalty,
      coalesce(repayhst_paid_mult,     paid_mult_amt, 0)                                       as paid_mult,
      reduced_amt                                                                              as reduce_amount,
      reduce_term_prin                                                                         as reduce_principal,
      reduce_term_int                                                                          as reduce_interest,
      reduce_term_fee                                                                          as reduce_term_fee,
      reduce_svc_fee                                                                           as reduce_svc_fee,
      reduce_penalty                                                                           as reduce_penalty,
      reduce_mult_amt                                                                          as reduce_mult_amt,
      is_empty(create_time,create_user)                                                        as create_time,
      is_empty(lst_upd_time,lst_upd_user)                                                      as update_time,
      d_date                                                                                   as d_date
    from (
      select
        *,
        concat(due_bill_no,'::',curr_term) as due_bill_no_curr_term
      from ods.ecas_repay_schedule${tb_suffix}
      where 1 > 0
        and d_date = '${ST9}'
        and product_code in (${product_id})
        ${due_bill_no}
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
        repayhst_paid_mult,
        due_bill_no_order
      from (
        select
          due_bill_no                                         as repayhst_due_bill_no,
          concat(due_bill_no,'::',term)                       as repayhst_due_bill_no_term,
          sum(if(bnp_type = 'Pricinpal',        repay_amt,0))
          - case when due_bill_no = '1120070912093993172613' and d_date between '2020-07-10' and '2020-07-13' then 2500 else 0 end
                                                              as repayhst_paid_principal,
          sum(if(bnp_type = 'Interest',         repay_amt,0)) as repayhst_paid_interest,
          sum(if(bnp_type = 'TERMFee',          repay_amt,0)) as repayhst_paid_term_fee,
          sum(if(bnp_type = 'SVCFee',           repay_amt,0)) as repayhst_paid_svc_fee,
          sum(if(bnp_type = 'Penalty',          repay_amt,0)) as repayhst_paid_penalty,
          sum(if(bnp_type = 'LatePaymentCharge',repay_amt,0)) as repayhst_paid_mult
        from ods.ecas_repay_hst${tb_suffix}
        where 1 > 0
          and d_date = '${ST9}'
          and if('${tb_suffix}' = '_asset',true,
            payment_id not in ( -- 代偿后  在 2020-07-10 到 2020-07-13 这四天有六笔，最新分区只有一笔。
              '000015945007161admin000083000002',
              '000015945007161admin000083000003',
              '000015945007161admin000083000004',
              '000015945007161admin000083000005',
              '000015945007161admin000083000006'
            )
          )
          ${due_bill_no}
        group by d_date,concat(due_bill_no,'::',term),due_bill_no
      ) as repay_detail
      left join (
        select distinct due_bill_no as due_bill_no_order
        from ods.ecas_order_asset
        where 1 > 0
          and d_date <= to_date(current_timestamp())
          and d_date = '${ST9}'
          and loan_usage = 'T'
          ${due_bill_no}
      ) as order_info
      on repayhst_due_bill_no = due_bill_no_order
    ) as repayhst
    on due_bill_no_curr_term = repayhst_due_bill_no_term
  ) as repay_schedule
  left join (
    select
      product_code                                              as product_id,
      schedule_id                                               as schedule_id,
      due_bill_no                                               as due_bill_no,
      loan_init_prin                                            as loan_init_principal,
      loan_init_term                                            as loan_init_term,
      curr_term                                                 as loan_term,
      start_interest_date                                       as start_interest_date,
      curr_bal                                                  as curr_bal,
      pmt_due_date                                              as should_repay_date,
      origin_pmt_due_date                                       as should_repay_date_history,
      grace_date                                                as grace_date,
      due_term_prin                                             as should_repay_principal,
      due_term_int                                              as should_repay_interest,
      due_term_fee                                              as should_repay_term_fee,
      due_svc_fee                                               as should_repay_svc_fee,
      due_penalty                                               as should_repay_penalty,
      due_mult_amt                                              as should_repay_mult_amt,
      penalty_acru                                              as should_repay_penalty_acru,
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
      else schedule_status end                                                                 as schedule_status,
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
      else paid_out_date end                                                                   as paid_out_date,
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
      if(due_bill_no_order is not null,due_term_prin,coalesce(repayhst_paid_principal,paid_term_pric,0)) as paid_principal,
      coalesce(repayhst_paid_interest, paid_term_int, 0)        as paid_interest,
      coalesce(repayhst_paid_term_fee, paid_term_fee, 0)        as paid_term_fee,
      coalesce(repayhst_paid_svc_fee,  paid_svc_fee,  0)        as paid_svc_fee,
      coalesce(repayhst_paid_penalty,  paid_penalty,  0)        as paid_penalty,
      coalesce(repayhst_paid_mult,     paid_mult_amt, 0)        as paid_mult,
      reduced_amt                                               as reduce_amount,
      reduce_term_prin                                          as reduce_principal,
      reduce_term_int                                           as reduce_interest,
      reduce_term_fee                                           as reduce_term_fee,
      reduce_svc_fee                                            as reduce_svc_fee,
      reduce_penalty                                            as reduce_penalty,
      reduce_mult_amt                                           as reduce_mult_amt
    from (
      select
        *,
        concat(due_bill_no,'::',curr_term) as due_bill_no_curr_term
      from ods.ecas_repay_schedule${tb_suffix}
      where 1 > 0
        and d_date = date_sub('${ST9}',1)
        and product_code in (${product_id})
        ${due_bill_no}
      ) as schedule_yest
    left join (
      select
        repayhst_due_bill_no,
        repayhst_due_bill_no_term,
        repayhst_paid_principal,
        repayhst_paid_interest,
        repayhst_paid_term_fee,
        repayhst_paid_svc_fee,
        repayhst_paid_penalty,
        repayhst_paid_mult,
        due_bill_no_order
      from (
        select
          due_bill_no                                         as repayhst_due_bill_no,
          concat(due_bill_no,'::',term)                       as repayhst_due_bill_no_term,
          sum(if(bnp_type = 'Pricinpal',        repay_amt,0))
          - case when due_bill_no = '1120070912093993172613' and d_date between '2020-07-10' and '2020-07-13' then 2500 else 0 end
                                                              as repayhst_paid_principal,
          sum(if(bnp_type = 'Interest',         repay_amt,0)) as repayhst_paid_interest,
          sum(if(bnp_type = 'TERMFee',          repay_amt,0)) as repayhst_paid_term_fee,
          sum(if(bnp_type = 'SVCFee',           repay_amt,0)) as repayhst_paid_svc_fee,
          sum(if(bnp_type = 'Penalty',          repay_amt,0)) as repayhst_paid_penalty,
          sum(if(bnp_type = 'LatePaymentCharge',repay_amt,0)) as repayhst_paid_mult
        from ods.ecas_repay_hst${tb_suffix}
        where 1 > 0
          and d_date = date_sub('${ST9}',1)
          and if('${tb_suffix}' = '_asset',true,
            payment_id not in ( -- 代偿后  在 2020-07-10 到 2020-07-13 这四天有六笔，最新分区只有一笔。
              '000015945007161admin000083000002',
              '000015945007161admin000083000003',
              '000015945007161admin000083000004',
              '000015945007161admin000083000005',
              '000015945007161admin000083000006'
            )
          )
          ${due_bill_no}
        group by d_date,concat(due_bill_no,'::',term),due_bill_no
      ) as repay_detail
      left join (
        select distinct due_bill_no as due_bill_no_order
        from ods.ecas_order_asset
        where 1 > 0
          and d_date <= to_date(current_timestamp())
          and d_date = date_sub('${ST9}',1)
          and loan_usage = 'T'
          ${due_bill_no}
      ) as order_info
      on repayhst_due_bill_no = due_bill_no_order
    ) as repayhst_yest
    on due_bill_no_curr_term = repayhst_due_bill_no_term
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
  ) = concat_ws('::',
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
  where repay_schedule_tmp.due_bill_no is null
)
insert overwrite table ods_new_s${db_suffix}.his_repay_schedule_repaired partition(is_settled = 'no',product_id)
select
  repay_schedule.schedule_id,
  repay_schedule.due_bill_no,
  repay_schedule.loan_active_date,
  repay_schedule.loan_init_principal,
  repay_schedule.loan_init_term,
  repay_schedule.loan_term,
  repay_schedule.start_interest_date,
  repay_schedule.curr_bal,
  repay_schedule.should_repay_date,
  repay_schedule.should_repay_date_history,
  repay_schedule.grace_date,
  repay_schedule.should_repay_amount,
  repay_schedule.should_repay_principal,
  repay_schedule.should_repay_interest,
  repay_schedule.should_repay_term_fee,
  repay_schedule.should_repay_svc_fee,
  repay_schedule.should_repay_penalty,
  repay_schedule.should_repay_mult_amt,
  repay_schedule.should_repay_penalty_acru,
  repay_schedule.schedule_status,
  repay_schedule.schedule_status_cn,
  repay_schedule.paid_out_date,
  repay_schedule.paid_out_type,
  repay_schedule.paid_out_type_cn,
  repay_schedule.paid_amount,
  repay_schedule.paid_principal,
  repay_schedule.paid_interest,
  repay_schedule.paid_term_fee,
  repay_schedule.paid_svc_fee,
  repay_schedule.paid_penalty,
  repay_schedule.paid_mult,
  repay_schedule.reduce_amount,
  repay_schedule.reduce_principal,
  repay_schedule.reduce_interest,
  repay_schedule.reduce_term_fee,
  repay_schedule.reduce_svc_fee,
  repay_schedule.reduce_penalty,
  repay_schedule.reduce_mult_amt,
  repay_schedule.s_d_date,
  if(repay_schedule.e_d_date > '${ST9}' and ods_new_s_repay_schedule.schedule_id is not null,ods_new_s_repay_schedule.d_date,if(repay_schedule.e_d_date = '${ST9}' and ods_new_s_repay_schedule.loan_term is null,'3000-12-31',repay_schedule.e_d_date)) as e_d_date,
  repay_schedule.effective_time,
  cast(if(to_date(repay_schedule.expire_time) > '${ST9}' and ods_new_s_repay_schedule.schedule_id is not null,ods_new_s_repay_schedule.update_time,repay_schedule.expire_time) as timestamp) as expire_time,
  repay_schedule.product_id
from (select * from ods_new_s${db_suffix}.his_repay_schedule_tmp_repaired where 1 > 0 and product_id in (${product_id})) as repay_schedule
left join ods_new_s_repay_schedule
on concat(repay_schedule.due_bill_no,repay_schedule.loan_term) = concat(ods_new_s_repay_schedule.due_bill_no,ods_new_s_repay_schedule.loan_term)
union all
select
  repay_schedule.schedule_id,
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
  repay_schedule.should_repay_amount,
  repay_schedule.should_repay_principal,
  repay_schedule.should_repay_interest,
  repay_schedule.should_repay_term_fee,
  repay_schedule.should_repay_svc_fee,
  repay_schedule.should_repay_penalty,
  repay_schedule.should_repay_mult_amt,
  repay_schedule.should_repay_penalty_acru,
  repay_schedule.schedule_status,
  repay_schedule.schedule_status_cn,
  repay_schedule.paid_out_date,
  repay_schedule.paid_out_type,
  repay_schedule.paid_out_type_cn,
  repay_schedule.paid_amount,
  repay_schedule.paid_principal,
  repay_schedule.paid_interest,
  repay_schedule.paid_term_fee,
  repay_schedule.paid_svc_fee,
  repay_schedule.paid_penalty,
  repay_schedule.paid_mult,
  repay_schedule.reduce_amount,
  repay_schedule.reduce_principal,
  repay_schedule.reduce_interest,
  repay_schedule.reduce_term_fee,
  repay_schedule.reduce_svc_fee,
  repay_schedule.reduce_penalty,
  repay_schedule.reduce_mult_amt,
  repay_schedule.d_date as s_d_date,
  repay_schedule.e_d_date,
  repay_schedule.update_time as effective_time,
  repay_schedule.expire_time,
  repay_schedule.product_id
from ods_new_s_repay_schedule as repay_schedule
left join (
  select distinct
    due_bill_no  as ecas_loan_due_bill_no,
    active_date  as loan_active_date,
    product_code as product_id
  from ods.ecas_loan${tb_suffix}
  where 1 > 0
    and d_date = '${ST9}'
    and product_code in (${product_id})
) as ecas_loan
on  repay_schedule.product_id  = ecas_loan.product_id
and repay_schedule.due_bill_no = ecas_loan.ecas_loan_due_bill_no
-- limit 1
;

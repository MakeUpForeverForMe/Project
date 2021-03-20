
set hive.support.quoted.identifiers=None;
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

insert overwrite table ods${db_suffix}.loan_info_inter partition(biz_date,product_id)
  select
  today.due_bill_no                                           as due_bill_no,
  today.apply_no                                              as apply_no,
  today.loan_active_date                                      as loan_active_date,
  today.loan_init_term                                        as loan_init_term,
  today.loan_init_principal                                   as loan_init_principal,
  today.loan_init_interest                                    as loan_init_interest,
  today.loan_init_term_fee                                    as loan_init_term_fee,
  today.loan_init_svc_fee                                     as loan_init_svc_fee,
  today.loan_term                                             as loan_term,
  today.loan_term                                             as account_age,
  today.should_repay_date                                     as should_repay_date,
  today.loan_term_repaid                                      as loan_term_repaid,
  today.loan_term_remain                                      as loan_term_remain,
  today.loan_status                                           as loan_status,
  today.loan_status_cn                                        as loan_status_cn,
  today.loan_out_reason                                       as loan_out_reason,
  today.paid_out_type                                         as paid_out_type,
  today.paid_out_type_cn                                      as paid_out_type_cn,
  today.paid_out_date                                         as paid_out_date,
  today.terminal_date                                         as terminal_date,
  nvl(today.paid_amount,0)                                    as paid_amount,
  nvl(today.paid_principal,0)                                 as paid_principal,
  nvl(today.paid_interest,0)                                  as paid_interest,
  nvl(today.paid_penalty,0)                                   as paid_penalty,
  nvl( today.paid_svc_fee,0)                                  as paid_svc_fee,
  nvl( today.paid_term_fee,0)                                 as paid_term_fee,
  nvl( today.paid_mult,0)                                     as paid_mult,
  nvl(today.remain_amount,0)                                  as remain_amount,
  nvl( today.remain_principal,0)                              as remain_principal,
  nvl( today.remain_interest,0)                               as remain_interest,
  nvl(today.remain_svc_fee,0)                                 as remain_svc_fee,
  nvl( today.remain_term_fee,0)                               as remain_term_fee,
  0                                                           as remain_othAmounts,
  nvl( today.overdue_principal,0)                             as overdue_principal,
  nvl( today.overdue_interest,0)                              as overdue_interest,
  nvl( today.overdue_svc_fee,0)                               as overdue_svc_fee,
  nvl( today.overdue_term_fee,0)                              as overdue_term_fee,
  nvl( today.overdue_penalty,0)                               as overdue_penalty,
  nvl(  today.overdue_mult_amt,0)                             as overdue_mult_amt,
 today.overdue_date                                           as overdue_date_start,
  today.overdue_days                                          as overdue_days,
  date_add(today.overdue_date,today.overdue_days - 1)         as overdue_date,
  today.collect_out_date                                      as collect_out_date,
  0                                                           as dpd_days_max,
  today.overdue_term                                          as overdue_term,
  0                                                           as overdue_terms_count,
  today.overdue_terms_max                                     as overdue_terms_max,
  0                                                           as overdue_principal_accumulate,
  0                                                           as overdue_principal_max,
  today.create_time                                           as create_time,
  today.update_time                                           as update_time,
  today.d_date                                                as biz_date,
  today.product_id                                            as product_id
  from (
    select
      ecas_loan.product_id,
      ecas_loan.due_bill_no,
      ecas_loan.apply_no,
      ecas_loan.loan_active_date,
      ecas_loan.loan_init_term,
      case
      when ecas_loan.paid_out_date = ecas_loan.loan_active_date then 1
      when ecas_loan.paid_out_date is null                      then repay_schedule.loan_term2
      when '${ST9}' <= ecas_loan.paid_out_date                  then repay_schedule.loan_term2
      else null end as loan_term,
      if(
        (ecas_loan.paid_out_date = ecas_loan.loan_active_date and ecas_loan.loan_term = 1) or ecas_loan.paid_out_date is null or '${ST9}' <= ecas_loan.paid_out_date,
        repay_schedule.should_repay_date,
        null
      ) as should_repay_date,
      --取用实还上的已还期数更新借据上的已还期数和剩余期数
      nvl(repay_detail.paid_term,0)                              as loan_term_repaid,
      ecas_loan.loan_init_term-nvl(repay_detail.paid_term,0)     as loan_term_remain,
      case
            when repay_detail.paid_term=ecas_loan.loan_init_term then 'F'
            when overdue_day.overdue_date!='9999-12-31' then 'O'
            when ecas_loan.loan_status='O' and overdue_day.overdue_date='9999-12-31' then "N"
      else  'N'  end as loan_status,
       case
            when repay_detail.paid_term=ecas_loan.loan_init_term then '已结清'
            when overdue_day.overdue_date!='9999-12-31' then '逾期'
            when ecas_loan.loan_status='O' and overdue_day.overdue_date='9999-12-31' then "正常"
      else  '正常'  end as loan_status_cn,
      ecas_loan.loan_out_reason,
      ecas_loan.paid_out_type,
      ecas_loan.paid_out_type_cn,
      ecas_loan.paid_out_date,
      ecas_loan.terminal_date,
      ecas_loan.loan_init_principal,
      ecas_loan.loan_init_interest_rate,
      ecas_loan.loan_init_interest,
      ecas_loan.loan_init_term_fee_rate,
      ecas_loan.loan_init_term_fee,
      ecas_loan.loan_init_svc_fee_rate,
      ecas_loan.loan_init_svc_fee,
      ecas_loan.loan_init_penalty_rate,
      coalesce(repay_detail.paid_principal,ecas_loan.paid_principal,0) +
      coalesce(repay_detail.paid_interest, ecas_loan.paid_interest, 0) +
      coalesce(repay_detail.paid_penalty,  ecas_loan.paid_penalty,  0) +
      coalesce(repay_detail.paid_svc_fee,  ecas_loan.paid_svc_fee,  0) +
      coalesce(repay_detail.paid_term_fee, ecas_loan.paid_term_fee, 0) +
      coalesce(repay_detail.paid_mult,     ecas_loan.paid_mult,     0) as paid_amount,
      nvl(repay_detail.paid_principal,0)                               as paid_principal,
      coalesce(repay_detail.paid_interest, ecas_loan.paid_interest, 0) as paid_interest,
      coalesce(repay_detail.paid_penalty,  ecas_loan.paid_penalty,  0) as paid_penalty,
      coalesce(repay_detail.paid_svc_fee,  ecas_loan.paid_svc_fee,  0) as paid_svc_fee,
      coalesce(repay_detail.paid_term_fee, ecas_loan.paid_term_fee, 0) as paid_term_fee,
      coalesce(repay_detail.paid_mult,     ecas_loan.paid_mult,     0) as paid_mult,
      (
        nvl(ecas_loan.loan_init_principal,0) +
        nvl(ecas_loan.loan_init_interest, 0) +
        nvl(ecas_loan.loan_init_term_fee, 0) +
        nvl(ecas_loan.loan_init_svc_fee,  0)
      ) - (
        coalesce(repay_detail.paid_principal,ecas_loan.paid_principal,0) +
        coalesce(repay_detail.paid_interest, ecas_loan.paid_interest, 0) +
        coalesce(repay_detail.paid_svc_fee,  ecas_loan.paid_svc_fee,  0) +
        coalesce(repay_detail.paid_term_fee, ecas_loan.paid_term_fee, 0)
      ) - (
        nvl(ecas_loan.reduce_prin,0) +
        nvl(ecas_loan.reduce_interest, 0) +
        nvl(ecas_loan.reduce_svc_fee, 0) +
        nvl(ecas_loan.reduce_term_fee, 0)
      ) as remain_amount,
      ecas_loan.loan_init_principal - nvl(repay_detail.paid_principal,0) as remain_principal,
      ecas_loan.loan_init_interest  - nvl(repay_detail.paid_interest,0) as remain_interest,
      ecas_loan.loan_init_svc_fee   - coalesce(repay_detail.paid_svc_fee,  ecas_loan.paid_svc_fee,  0) as remain_svc_fee,
      ecas_loan.loan_init_term_fee  - coalesce(repay_detail.paid_term_fee, ecas_loan.paid_term_fee, 0) as remain_term_fee,
      overdue_principal_reload.overdue_principal,
      overdue_principal_reload.overdue_interest,
      ecas_loan.overdue_svc_fee,
      ecas_loan.overdue_term_fee,
      ecas_loan.overdue_penalty,
      ecas_loan.overdue_mult_amt,
      case
            when repay_detail.paid_term=ecas_loan.loan_init_term then null
            when overdue_day.overdue_date!='9999-12-31' then overdue_day.overdue_date
            else   null  end as overdue_date,
      case  when repay_detail.paid_term=ecas_loan.loan_init_term then 0
            when overdue_day.overdue_date!='9999-12-31' then abs(datediff(overdue_day.overdue_date,'${ST9}'))+1
      else   0  end as overdue_days,
      ecas_loan.collect_out_date,
      overdue_term.overdue_term,
      nvl(overdue_term.overdue_terms_max,0) as overdue_terms_max,
      ecas_loan.sync_date,
      ecas_loan.create_time,
      ecas_loan.update_time,
      ecas_loan.d_date
    from (
      select
        product_code                      as product_id,
        due_bill_no                       as due_bill_no,
        apply_no                          as apply_no,
        active_date                       as loan_active_date,
        loan_init_term                    as loan_init_term,
        curr_term                         as loan_term,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 6
        else repay_term end               as loan_term_repaid,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
        else remain_term end              as loan_term_remain,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        else loan_status end              as loan_status,
        case
          case
          when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
          else loan_status end
        when 'N' then '正常'
        when 'O' then '逾期'
        when 'F' then '已还清'
        else
          case
          when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
          else loan_status end
        end                               as loan_status_cn,
        terminal_reason_cd                as loan_out_reason,
        loan_settle_reason                as paid_out_type,
        case loan_settle_reason
        when 'NORMAL_SETTLE'  then '正常结清'
        when 'OVERDUE_SETTLE' then '逾期结清'
        when 'PRE_SETTLE'     then '提前结清'
        when 'REFUND'         then '退车'
        when 'REDEMPTION'     then '赎回'
        when 'BANK_REF'       then '退票结清'
        when 'BUY_BACK'       then '资产回购'
        when 'CAPITAL_VERI'   then '资产核销'
        when 'DISPOSAL'       then '处置结束'
        when 'OVER_COMP'      then '逾期代偿'
        else loan_settle_reason
        end                               as paid_out_type_cn,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-03'
        else paid_out_date end            as paid_out_date,
        terminal_date                     as terminal_date,
        loan_init_prin                    as loan_init_principal,
        interest_rate                     as loan_init_interest_rate,
        totle_int                         as loan_init_interest,
        term_fee_rate                     as loan_init_term_fee_rate,
        totle_term_fee                    as loan_init_term_fee,
        svc_fee_rate                      as loan_init_svc_fee_rate,
        totle_svc_fee                     as loan_init_svc_fee,
        penalty_rate                      as loan_init_penalty_rate,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then cast('1000.00' as decimal(15,2))
        else paid_principal end           as paid_principal,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then cast('1.00' as decimal(15,2))
        else paid_interest end            as paid_interest,
        paid_term_fee                     as paid_term_fee,
        paid_svc_fee                      as paid_svc_fee,
        paid_penalty                      as paid_penalty,
        paid_mult                         as paid_mult,
        case
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        else overdue_prin end             as overdue_principal,
        case
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        else overdue_interest end         as overdue_interest,
        overdue_svc_fee                   as overdue_svc_fee,
        overdue_term_fee                  as overdue_term_fee,
        overdue_penalty                   as overdue_penalty,
        overdue_mult_amt                  as overdue_mult_amt,
        reduce_prin                       as reduce_prin,
        reduce_interest                   as reduce_interest,
        reduce_svc_fee                    as reduce_svc_fee,
        reduce_term_fee                   as reduce_term_fee,
        case
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then null
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then null
        else overdue_date end             as overdue_date,
        case
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        else overdue_days end             as overdue_days,
        collect_out_date                  as collect_out_date,
        case
        when d_date = '2020-02-21' and sync_date = 'ZhongHang' then capital_plan_no
        when d_date = '2020-02-27' and capital_type = '2020-02-28' then capital_type
        when d_date = '2020-02-28' and capital_type = '2020-02-29' then capital_type
        when d_date = '2020-02-29' and capital_type = '2020-03-01' then capital_type
        else sync_date end                as sync_date,
        cast(datefmt(create_time, 'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as create_time,
        cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as update_time,
        d_date                            as d_date
      from stage.ecas_loan${tb_suffix}
      where 1 > 0
        and d_date = '${ST9}'
        and p_type in ('ddht')
        and product_code in(${product_id})
        and due_bill_no not in ('DD000230362019111701130061a514','DD000230362019121511470083cc26','DD0002303620200111210700a1ceb3',
        'DD0002303620200206232600de511b','DD0002303620200208195000ae8354','DD0002303620200307225700e545de',
        'DD0002303620200308222600ebbea9','DD00023036202003191421004c1251','DD0002303620200404090400770962',
        'DD0002303620200409104700bfb018','DD0002303620200627045600f1cc28','DD0002303620200501162300e1b938')
    ) as ecas_loan
    left join (
      select
        due_bill_no,
        sum(if(bnp_type = 'Pricinpal',        paid_principal,0)) as  paid_principal,
        sum(if(bnp_type = 'Interest',         paid_interest,0)) as paid_interest,
        sum(if(bnp_type = 'TERMFee',          paid_term_fee,0)) as paid_term_fee,
        sum(if(bnp_type = 'SVCFee',           paid_svc_fee,0)) as paid_svc_fee,
        sum(if(bnp_type = 'Penalty',          paid_penalty,0)) as paid_penalty,
        sum(if(bnp_type = 'LatePaymentCharge',paid_mult,0)) as paid_mult,
        sum( if(bnp_type = 'Pricinpal',1,0))  as paid_term
      from
      (
      select
      due_bill_no,
      term,
      bnp_type,
      sum(if(bnp_type = 'Pricinpal',        repay_amt,0)) as  paid_principal,
      sum(if(bnp_type = 'Interest',         repay_amt,0)) as paid_interest,
      sum(if(bnp_type = 'TERMFee',          repay_amt,0)) as paid_term_fee,
      sum(if(bnp_type = 'SVCFee',           repay_amt,0)) as paid_svc_fee,
      sum(if(bnp_type = 'Penalty',          repay_amt,0)) as paid_penalty,
      sum(if(bnp_type = 'LatePaymentCharge',repay_amt,0)) as paid_mult
      from
      stage.ecas_repay_hst${tb_suffix}
      where 1 > 0
        and d_date = '${d_date}'
        and p_type in ('ddht')
        and txn_date <= '${ST9}'
        group by due_bill_no,term,bnp_type
      )repay_info
      group by due_bill_no
    ) as repay_detail
    on ecas_loan.due_bill_no = repay_detail.due_bill_no
    left join (
      select
        due_bill_no,
        min(curr_term)    as loan_term2,
        min(nvl(origin_pmt_due_date,pmt_due_date)) as should_repay_date,
        product_code      as product_id
      from stage.ecas_repay_schedule${tb_suffix}
      where 1 > 0
        and d_date = '${ST9}'      -- 取快照日当天的所有还款计划数据
        and p_type in ('ddht')
        and product_code in(${product_id})
        and d_date <= nvl(origin_pmt_due_date,pmt_due_date) -- 取快照日当天之后的所有还款计划数据（取最小值时，即为当前期数、应还日）
        and curr_term > 0          -- 过滤掉汇通的第 0 期的情况
      group by due_bill_no,product_code
    ) as repay_schedule
    on ecas_loan.due_bill_no = repay_schedule.due_bill_no
    left join (
      select
        due_bill_no,
        max(curr_term)   as overdue_term,
        count(curr_term) as overdue_terms_max,
        d_date
      from stage.ecas_repay_schedule${tb_suffix}
      where 1 > 0
        and d_date='${ST9}'
        and p_type='ddht'
        and nvl(origin_pmt_due_date,pmt_due_date) <= d_date
        and case
          when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
          when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
          when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
          when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
          when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
          when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
          when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
          when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
          when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
          when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
          when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
          when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
          when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
          when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
          when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
          when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
          else schedule_status end = 'O'
      group by due_bill_no,d_date
    ) as overdue_term
    on  ecas_loan.d_date      = overdue_term.d_date
    and ecas_loan.due_bill_no = overdue_term.due_bill_no
    left join
    (
        select
            repay_schedule.due_bill_no                                                  as due_bill_no,
            sum(case
                when nvl(repay_hst.repayhst_paid_principal,0) >= due_term_prin then 0
                when '${ST9}' < pmt_due_date then 0
                when '${ST9}' >= pmt_due_date then due_term_prin
                end )                                                                   as overdue_principal,
            sum(case
                when nvl(repay_hst.repayhst_paid_principal,0) >= due_term_prin then 0
                when '${ST9}' < pmt_due_date then 0
                when '${ST9}' >= pmt_due_date then due_term_int
                end )                                                                   as overdue_interest
        from
        (
            select
                *,
                concat(due_bill_no,'::',curr_term) as due_bill_no_curr_term
            from stage.ecas_repay_schedule${tb_suffix}
            where d_date = '${ST9}' and p_type in ('ddht') and product_code in (${product_id})
            and due_bill_no not in ('DD000230362019111701130061a514','DD000230362019121511470083cc26','DD0002303620200111210700a1ceb3',
            'DD0002303620200206232600de511b','DD0002303620200208195000ae8354','DD0002303620200307225700e545de',
            'DD0002303620200308222600ebbea9','DD00023036202003191421004c1251','DD0002303620200404090400770962',
            'DD0002303620200409104700bfb018')
        ) repay_schedule
        left join
        (
             select
                 due_bill_no                                                     as repayhst_due_bill_no,
                 concat(due_bill_no,'::',term)                                   as repayhst_due_bill_no_term,
                 sum(if(bnp_type = 'Pricinpal',        repay_amt,0)) - case
                   when due_bill_no = '11200709120939931 72613' and d_date in ('2020-07-10','2020-07-11','2020-07-12','2020-07-13') then 2500
                   when due_bill_no = '1120061910384241252747' then 200 else 0 end as repayhst_paid_principal,
                 sum(if(bnp_type = 'Interest',         repay_amt,0))             as repayhst_paid_interest
             from stage.ecas_repay_hst${tb_suffix}
             where 1 > 0
               and d_date='${d_date}'
               and p_type in ('ddht')
               and txn_date <= '${ST9}'
             group by d_date,concat(due_bill_no,'::',term),due_bill_no
        ) repay_hst
        on repay_schedule.due_bill_no_curr_term = repay_hst.repayhst_due_bill_no_term
        group by repay_schedule.due_bill_no
    ) as overdue_principal_reload
    on ecas_loan.due_bill_no = overdue_principal_reload.due_bill_no
    --修复借据上逾期起始日期和 逾期天数
    left join(
     select
        due_bill_no ,
        min(overdue_date) as overdue_date
        from
        (
        select
        schedule.due_bill_no,
        case when '${ST9}'>=pmt_due_date and nvl(repayhst_paid_principal,0)<schedule.due_term_prin  then pmt_due_date
         when schedule.paid_out_date is null and '${ST9}'>=pmt_due_date then pmt_due_date
        else '9999-12-31' end as overdue_date
        from
            (
                select tmp.due_bill_no,concat(tmp.due_bill_no,"::",tmp.curr_term) as schedule_curr_term_due_bill_no,
                case
                     when tmp.d_date>=new_schedule.paid_out_date then new_schedule.paid_out_date
                     when tmp.paid_out_date!=new_schedule.paid_out_date and tmp.paid_out_date is not null then new_schedule.paid_out_date
                else tmp.paid_out_date end as paid_out_date,
                tmp.schedule_status,tmp.pmt_due_date,tmp.due_term_prin
                from
                (
                    select * from stage.ecas_repay_schedule where d_date='${ST9}' and  p_type='ddht'  and  product_code in(${product_id})
                )tmp
                left join
                (
                    select due_bill_no,curr_term,paid_out_date,schedule_status
                    from stage.ecas_repay_schedule where d_date='${d_date}'  and p_type='ddht' and product_code in (${product_id})
                    and paid_out_date is not null and paid_out_date<=date_add('${ST9}',10)
                )new_schedule on tmp.due_bill_no=new_schedule.due_bill_no and tmp.curr_term=new_schedule.curr_term
             )schedule
             left join
             (
                 select
                     due_bill_no                                                     as repayhst_due_bill_no,
                     concat(due_bill_no,'::',term)                                   as repayhst_due_bill_no_term,
                     sum(if(bnp_type = 'Pricinpal',        repay_amt,0)) - case
                       when due_bill_no = '11200709120939931 72613' and d_date in ('2020-07-10','2020-07-11','2020-07-12','2020-07-13') then 2500
                       when due_bill_no = '1120061910384241252747' then 200 else 0 end as repayhst_paid_principal,
                     sum(if(bnp_type = 'Interest',         repay_amt,0))             as repayhst_paid_interest
                 from stage.ecas_repay_hst
                 where 1 > 0
                   and d_date='${d_date}'
                   and p_type in ('ddht')
                   and txn_date <='${ST9}'
                 group by d_date,concat(due_bill_no,'::',term),due_bill_no
            ) repay_hst on  schedule.schedule_curr_term_due_bill_no=repay_hst.repayhst_due_bill_no_term
        )tmp
        group by due_bill_no
    )overdue_day  on ecas_loan.due_bill_no = overdue_day.due_bill_no
  ) as today
  left join (
    select
      ecas_loan.product_id,
      ecas_loan.due_bill_no,
      ecas_loan.apply_no,
      ecas_loan.loan_active_date,
      ecas_loan.loan_init_term,
      case
      when ecas_loan.paid_out_date = ecas_loan.loan_active_date then 1
      when ecas_loan.paid_out_date is null                      then repay_schedule.loan_term2
      when '${ST9}' <= ecas_loan.paid_out_date                  then repay_schedule.loan_term2
      else null end as loan_term,
      if(
        (ecas_loan.paid_out_date = ecas_loan.loan_active_date and ecas_loan.loan_term = 1) or ecas_loan.paid_out_date is null or '${ST9}' <= ecas_loan.paid_out_date,
        repay_schedule.should_repay_date,
        null
      ) as should_repay_date,
      nvl(repay_detail.paid_term,0)                              as loan_term_repaid,
      ecas_loan.loan_init_term-nvl(repay_detail.paid_term,0)     as loan_term_remain,
       case
            when repay_detail.paid_term=ecas_loan.loan_init_term then 'F'
            when overdue_day.overdue_date!='9999-12-31' then 'O'
            when ecas_loan.loan_status='O' and overdue_day.overdue_date='9999-12-31' then "N"
      else  'N'  end as loan_status,
      ecas_loan.loan_out_reason,
      ecas_loan.paid_out_type,
      ecas_loan.paid_out_date,
      ecas_loan.terminal_date,
      ecas_loan.loan_init_principal,
      ecas_loan.loan_init_interest_rate,
      ecas_loan.loan_init_interest,
      ecas_loan.loan_init_term_fee_rate,
      ecas_loan.loan_init_term_fee,
      ecas_loan.loan_init_svc_fee_rate,
      ecas_loan.loan_init_svc_fee,
      ecas_loan.loan_init_penalty_rate,
      nvl(repay_detail.paid_principal,0)  as paid_principal,
      overdue_principal_reload.overdue_principal,
      overdue_principal_reload.overdue_interest,
      ecas_loan.overdue_svc_fee,
      ecas_loan.overdue_term_fee,
      ecas_loan.overdue_penalty,
      ecas_loan.overdue_mult_amt,
        case
        when repay_detail.paid_term=ecas_loan.loan_init_term then null
        when overdue_day.overdue_date!='9999-12-31' then overdue_day.overdue_date
      else   null  end as overdue_date,
      case
      when repay_detail.paid_term=ecas_loan.loan_init_term then 0
      when overdue_day.overdue_date!='9999-12-31' then abs(datediff(overdue_day.overdue_date,date_sub('${ST9}',1)))+1
      else   0  end as overdue_days
    from (
      select
        product_code                      as product_id,
        due_bill_no                       as due_bill_no,
        contract_no                       as contract_no,
        apply_no                          as apply_no,
        active_date                       as loan_active_date,
        loan_init_term                    as loan_init_term,
        curr_term                         as loan_term,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 6
        else repay_term end               as loan_term_repaid,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
        else remain_term end              as loan_term_remain,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        else loan_status end              as loan_status,
        terminal_reason_cd                as loan_out_reason,
        loan_settle_reason                as paid_out_type,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-03'
        else paid_out_date end            as paid_out_date,
        terminal_date                     as terminal_date,
        loan_init_prin                    as loan_init_principal,
        interest_rate                     as loan_init_interest_rate,
        totle_int                         as loan_init_interest,
        term_fee_rate                     as loan_init_term_fee_rate,
        totle_term_fee                    as loan_init_term_fee,
        svc_fee_rate                      as loan_init_svc_fee_rate,
        totle_svc_fee                     as loan_init_svc_fee,
        penalty_rate                      as loan_init_penalty_rate,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then cast('1000.00' as decimal(15,2))
        else paid_principal end           as paid_principal,
        case
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        else overdue_prin end             as overdue_principal,
        case
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        else overdue_interest end         as overdue_interest,
        overdue_svc_fee                   as overdue_svc_fee,
        overdue_term_fee                  as overdue_term_fee,
        overdue_penalty                   as overdue_penalty,
        overdue_mult_amt                  as overdue_mult_amt,
        case
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then null
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then null
        else overdue_date end             as overdue_date,
        case
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        else overdue_days end             as overdue_days
      from stage.ecas_loan${tb_suffix}
      where 1 > 0
        and d_date = date_sub('${ST9}',1)
        and p_type in ('ddht')
        and product_code in(${product_id})
        and due_bill_no not in ('DD000230362019111701130061a514','DD000230362019121511470083cc26','DD0002303620200111210700a1ceb3',
        'DD0002303620200206232600de511b','DD0002303620200208195000ae8354','DD0002303620200307225700e545de',
        'DD0002303620200308222600ebbea9','DD00023036202003191421004c1251','DD0002303620200404090400770962',
        'DD0002303620200409104700bfb018','DD0002303620200627045600f1cc28','DD0002303620200501162300e1b938')
    ) as ecas_loan
    left join (
         select
        due_bill_no,
        sum(if(bnp_type = 'Pricinpal',        paid_principal,0)) as  paid_principal,
        sum(if(bnp_type = 'Interest',         paid_interest,0)) as paid_interest,
        sum(if(bnp_type = 'TERMFee',          paid_term_fee,0)) as paid_term_fee,
        sum(if(bnp_type = 'SVCFee',           paid_svc_fee,0)) as paid_svc_fee,
        sum(if(bnp_type = 'Penalty',          paid_penalty,0)) as paid_penalty,
        sum(if(bnp_type = 'LatePaymentCharge',paid_mult,0)) as paid_mult,
        sum( if(bnp_type = 'Pricinpal',1,0))  as paid_term
      from
      (
      select
      due_bill_no,
      term,
      bnp_type,
      sum(if(bnp_type = 'Pricinpal',        repay_amt,0)) as  paid_principal,
      sum(if(bnp_type = 'Interest',         repay_amt,0)) as paid_interest,
      sum(if(bnp_type = 'TERMFee',          repay_amt,0)) as paid_term_fee,
      sum(if(bnp_type = 'SVCFee',           repay_amt,0)) as paid_svc_fee,
      sum(if(bnp_type = 'Penalty',          repay_amt,0)) as paid_penalty,
      sum(if(bnp_type = 'LatePaymentCharge',repay_amt,0)) as paid_mult
      from
      stage.ecas_repay_hst${tb_suffix}
      where 1 > 0
        and d_date = '${d_date}'
        and p_type in ('ddht')
        and txn_date <= date_sub('${ST9}',1)
        group by due_bill_no,term,bnp_type
      )repay_info
      group by due_bill_no
    ) as repay_detail
    on ecas_loan.due_bill_no = repay_detail.due_bill_no
    left join (
      select
        due_bill_no,
        min(curr_term)    as loan_term2,
        min(nvl(origin_pmt_due_date,pmt_due_date)) as should_repay_date,
        product_code      as product_id
      from stage.ecas_repay_schedule${tb_suffix}
      where 1 > 0
        and d_date = date_sub('${ST9}',1) -- 取快照日当天的所有还款计划数据
        and p_type = 'ddht'
        and d_date <= nvl(origin_pmt_due_date,pmt_due_date)        -- 取快照日当天之后的所有还款计划数据（取最小值时，即为当前期数、应还日）
        and curr_term > 0                 -- 过滤掉汇通的第 0 期的情况
      group by due_bill_no,product_code
    ) as repay_schedule
    on ecas_loan.due_bill_no = repay_schedule.due_bill_no
    left join
    (
        select
            repay_schedule.due_bill_no                                                  as due_bill_no,
            sum(case
                when nvl(repay_hst.repayhst_paid_principal,0) >= due_term_prin then 0
                when date_sub('${ST9}',1) < pmt_due_date then 0
                when date_sub('${ST9}',1) >= pmt_due_date then due_term_prin
                end )                                                                   as overdue_principal,
            sum(case
                when nvl(repay_hst.repayhst_paid_principal,0) >= due_term_prin then 0
                when date_sub('${ST9}',1) < pmt_due_date then 0
                when date_sub('${ST9}',1) >= pmt_due_date then due_term_int
                end )                                                                   as overdue_interest
        from
        (
            select
                *,
                concat(due_bill_no,'::',curr_term) as due_bill_no_curr_term
            from stage.ecas_repay_schedule${tb_suffix}
            where d_date = date_sub('${ST9}',1) and p_type in ('ddht') and  product_code in (${product_id})
            and due_bill_no not in ('DD000230362019111701130061a514','DD000230362019121511470083cc26','DD0002303620200111210700a1ceb3',
            'DD0002303620200206232600de511b','DD0002303620200208195000ae8354','DD0002303620200307225700e545de',
            'DD0002303620200308222600ebbea9','DD00023036202003191421004c1251','DD0002303620200404090400770962',
            'DD0002303620200409104700bfb018')
        ) repay_schedule
        left join
        (
             select
                 due_bill_no                                                     as repayhst_due_bill_no,
                 concat(due_bill_no,'::',term)                                   as repayhst_due_bill_no_term,
                 sum(if(bnp_type = 'Pricinpal',        repay_amt,0)) - case
                   when due_bill_no = '11200709120939931 72613' and d_date in ('2020-07-10','2020-07-11','2020-07-12','2020-07-13') then 2500
                   when due_bill_no = '1120061910384241252747' then 200 else 0 end as repayhst_paid_principal,
                 sum(if(bnp_type = 'Interest',         repay_amt,0))             as repayhst_paid_interest
             from stage.ecas_repay_hst${tb_suffix}
             where 1 > 0
               and d_date='${d_date}'
               and p_type in ('ddht')
               and txn_date <= date_sub('${ST9}',1)
             group by d_date,concat(due_bill_no,'::',term),due_bill_no
        ) repay_hst
        on repay_schedule.due_bill_no_curr_term = repay_hst.repayhst_due_bill_no_term
        group by repay_schedule.due_bill_no
    ) as overdue_principal_reload
    on ecas_loan.due_bill_no = overdue_principal_reload.due_bill_no
    left join(
    select
        due_bill_no ,
        min(overdue_date) as overdue_date
        from
        (
        select
        schedule.due_bill_no,
        case when date_sub('${ST9}',1)>=pmt_due_date and nvl(repayhst_paid_principal,0)<schedule.due_term_prin  then pmt_due_date
         when schedule.paid_out_date is null and date_sub('${ST9}',1)>=pmt_due_date then pmt_due_date
        else '9999-12-31' end as overdue_date
        from
            (
                select tmp.due_bill_no,concat(tmp.due_bill_no,"::",tmp.curr_term) as schedule_curr_term_due_bill_no,
                case
                     when tmp.d_date>=new_schedule.paid_out_date then new_schedule.paid_out_date
                     when tmp.paid_out_date!=new_schedule.paid_out_date and tmp.paid_out_date is not null then new_schedule.paid_out_date
                else tmp.paid_out_date end as paid_out_date,
                tmp.schedule_status,tmp.pmt_due_date,tmp.due_term_prin
                from
                (
                    select * from stage.ecas_repay_schedule where d_date=date_sub('${ST9}',1) and  p_type='ddht' and  product_code in (${product_id})
                )tmp
                left join
                (
                    select due_bill_no,curr_term,paid_out_date,schedule_status
                    from stage.ecas_repay_schedule where d_date='${d_date}'  and p_type='ddht' and  product_code in (${product_id})
                    and paid_out_date is not null and paid_out_date<=date_add('${ST9}',10)
                )new_schedule on tmp.due_bill_no=new_schedule.due_bill_no and tmp.curr_term=new_schedule.curr_term
             )schedule
             left join
             (
                 select
                     due_bill_no                                                     as repayhst_due_bill_no,
                     concat(due_bill_no,'::',term)                                   as repayhst_due_bill_no_term,
                     sum(if(bnp_type = 'Pricinpal',        repay_amt,0)) - case
                       when due_bill_no = '11200709120939931 72613' and d_date in ('2020-07-10','2020-07-11','2020-07-12','2020-07-13') then 2500
                       when due_bill_no = '1120061910384241252747' then 200 else 0 end as repayhst_paid_principal,
                     sum(if(bnp_type = 'Interest',         repay_amt,0))             as repayhst_paid_interest
                 from stage.ecas_repay_hst
                 where 1 > 0
                   and d_date='${d_date}'
                   and p_type in ('ddht')
                   and txn_date <= date_sub('${ST9}',1)
                 group by d_date,concat(due_bill_no,'::',term),due_bill_no
            ) repay_hst on  schedule.schedule_curr_term_due_bill_no=repay_hst.repayhst_due_bill_no_term
        )tmp
        group by due_bill_no
    )overdue_day  on ecas_loan.due_bill_no = overdue_day.due_bill_no
  ) as yesterday
  on  is_empty(today.product_id             ,'a') = is_empty(yesterday.product_id             ,'a')
  and is_empty(today.due_bill_no            ,'a') = is_empty(yesterday.due_bill_no            ,'a')
  and is_empty(today.apply_no               ,'a') = is_empty(yesterday.apply_no               ,'a')
  and is_empty(today.loan_active_date       ,'a') = is_empty(yesterday.loan_active_date       ,'a')
  and is_empty(today.loan_init_term         ,'a') = is_empty(yesterday.loan_init_term         ,'a')
  and is_empty(today.loan_term              ,'a') = is_empty(yesterday.loan_term              ,'a')
  and is_empty(today.should_repay_date      ,'a') = is_empty(yesterday.should_repay_date      ,'a')
  and is_empty(today.loan_term_repaid       ,'a') = is_empty(yesterday.loan_term_repaid       ,'a')
  and is_empty(today.loan_term_remain       ,'a') = is_empty(yesterday.loan_term_remain       ,'a')
  and is_empty(today.loan_status            ,'a') = is_empty(yesterday.loan_status            ,'a')
  and is_empty(today.loan_out_reason        ,'a') = is_empty(yesterday.loan_out_reason        ,'a')
  and is_empty(today.paid_out_type          ,'a') = is_empty(yesterday.paid_out_type          ,'a')
  and is_empty(today.paid_out_date          ,'a') = is_empty(yesterday.paid_out_date          ,'a')
  and is_empty(today.terminal_date          ,'a') = is_empty(yesterday.terminal_date          ,'a')
  and is_empty(today.loan_init_principal    ,'a') = is_empty(yesterday.loan_init_principal    ,'a')
  and is_empty(today.loan_init_interest_rate,'a') = is_empty(yesterday.loan_init_interest_rate,'a')
  and is_empty(today.loan_init_interest     ,'a') = is_empty(yesterday.loan_init_interest     ,'a')
  and is_empty(today.loan_init_term_fee_rate,'a') = is_empty(yesterday.loan_init_term_fee_rate,'a')
  and is_empty(today.loan_init_term_fee     ,'a') = is_empty(yesterday.loan_init_term_fee     ,'a')
  and is_empty(today.loan_init_svc_fee_rate ,'a') = is_empty(yesterday.loan_init_svc_fee_rate ,'a')
  and is_empty(today.loan_init_svc_fee      ,'a') = is_empty(yesterday.loan_init_svc_fee      ,'a')
  and is_empty(today.loan_init_penalty_rate ,'a') = is_empty(yesterday.loan_init_penalty_rate ,'a')
  and is_empty(today.paid_principal         ,'a') = is_empty(yesterday.paid_principal         ,'a')
  and is_empty(today.overdue_principal      ,'a') = is_empty(yesterday.overdue_principal      ,'a')
  and is_empty(today.overdue_interest       ,'a') = is_empty(yesterday.overdue_interest       ,'a')
  and is_empty(today.overdue_svc_fee        ,'a') = is_empty(yesterday.overdue_svc_fee        ,'a')
  and is_empty(today.overdue_term_fee       ,'a') = is_empty(yesterday.overdue_term_fee       ,'a')
  and is_empty(today.overdue_penalty        ,'a') = is_empty(yesterday.overdue_penalty        ,'a')
  and is_empty(today.overdue_mult_amt       ,'a') = is_empty(yesterday.overdue_mult_amt       ,'a')
  and is_empty(today.overdue_date           ,'a') = is_empty(yesterday.overdue_date           ,'a')
  and is_empty(today.overdue_days           ,'a') = is_empty(yesterday.overdue_days           ,'a')
  where yesterday.due_bill_no is null;


-- DROP TABLE IF EXISTS `ods_new_s${db_suffix}.loan_info`;
-- ALTER TABLE ods_new_s${db_suffix}.loan_info_intsert RENAME TO ods_new_s${db_suffix}.loan_info;











-- 凤金
-- select distinct
--   concat('00',loan_code)                                              as product_id,
--   loan_id                                                             as loan_id,
--   due_bill_no                                                         as due_bill_no,
--   contr_nbr                                                           as contract_no,
--   due_bill_no                                                         as apply_no,
--   purpose                                                             as loan_usage,
--   register_date                                                       as register_date,
--   request_time                                                        as request_time,
--   active_date                                                         as loan_active_date,
--   cast(cycle_day as decimal(2,0))                                     as cycle_day,
--   loan_expire_date                                                    as loan_expire_date,
--   loan_type                                                           as loan_type,
--   case loan_type
--   when 'R'    then '消费转分期'
--   when 'C'    then '现金分期'
--   when 'B'    then '账单分期'
--   when 'P'    then 'POS分期'
--   when 'M'    then '大额分期（专项分期）'
--   when 'MCAT' then '随借随还'
--   when 'MCEP' then '等额本金'
--   when 'MCEI' then '等额本息'
--   else loan_type
--   end                                                                 as loan_type_cn,
--   loan_init_term                                                      as loan_init_term,
--   curr_term                                                           as loan_term,
--   0                                                                   as loan_term_repaid,
--   remain_term                                                         as loan_term_remain,
--   loan_status                                                         as loan_status,
--   case loan_status
--   when 'N' then '正常'
--   when 'O' then '逾期'
--   when 'F' then '已还清'
--   else loan_status
--   end                                                                 as loan_status_cn,
--   terminal_reason_cd                                                  as loan_out_reason,
--   force_flag                                                          as paid_out_type,
--   paid_out_date                                                       as paid_out_date,
--   terminal_date                                                       as terminal_date,
--   loan_init_prin                                                      as loan_init_principal,
--   interest_rate                                                       as loan_init_interest_rate,
--   pay_interest                                                        as loan_init_interest,
--   fee_rate                                                            as loan_init_term_fee_rate,
--   loan_init_fee                                                       as loan_init_term_fee,
--   installment_fee_rate                                                as loan_init_svc_fee_rate,
--   tol_svc_fee                                                         as loan_init_svc_fee,
--   penalty_rate                                                        as loan_init_penalty_rate,
--   paid_fee                                                            as paid_amount,
--   paid_principal                                                      as paid_principal,
--   paid_interest                                                       as paid_interest,
--   paid_penalty                                                        as paid_penalty,
--   paid_svc_fee                                                        as paid_svc_fee,
--   0                                                                   as paid_term_fee,
--   0                                                                   as paid_mult,
--   remain_amount                                                       as remain_amount,
--   remain_principal                                                    as remain_principal,
--   remain_interest                                                     as remain_interest,
--   prin                                                                as overdue_principal,
--   interest                                                            as overdue_interest,
--   0                                                                   as overdue_svc_fee,
--   0                                                                   as overdue_term_fee,
--   penalty                                                             as overdue_penalty,
--   0                                                                   as overdue_mult_amt,
--   overdue_date                                                        as overdue_date,
--   overdue_days                                                        as overdue_days,
--   if(overdue_date is null,null,first_value(overdue_date) over(partition by due_bill_no order by cast(overdue_date as timestamp))) as first_overdue_date,
--   null                                                                as dpd_begin_date,
--   0                                                                   as dpd_days,
--   0                                                                   as dpd_days_count,
--   max_dpd                                                             as dpd_days_max,
--   collect_out_date                                                    as collect_out_date,
--   0                                                                   as overdue_term,
--   0                                                                   as overdue_terms_count,
--   0                                                                   as overdue_terms_max,
--   0                                                                   as overdue_principal_accumulate,
--   0                                                                   as overdue_principal_max,
--   null                                                                as mob,
--   null                                                                as sync_date,
--   create_time                                                         as create_time,
--   update_time                                                         as update_time
-- from (
--   select distinct
--     ref_nbr,
--     d_date,
--     loan_code,
--     loan_id,
--     contr_nbr,
--     due_bill_no,
--     purpose,
--     register_date,
--     cast(datefmt(request_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)      as request_time,
--     active_date,
--     loan_expire_date,
--     loan_type,
--     loan_init_term,
--     curr_term,
--     remain_term,
--     loan_status,
--     terminal_reason_cd,
--     force_flag,
--     paid_out_date,
--     terminal_date,
--     loan_init_prin,
--     interest_rate,
--     pay_interest,
--     fee_rate,
--     loan_init_fee,
--     installment_fee_rate,
--     tol_svc_fee,
--     penalty_rate,
--     paid_fee,
--     paid_principal,
--     paid_interest,
--     paid_penalty,
--     paid_svc_fee,
--     (loan_init_prin + pay_interest + loan_init_fee + tol_svc_fee - paid_fee) as remain_amount,
--     (loan_init_prin - paid_principal)                                        as remain_principal,
--     (pay_interest - paid_interest)                                           as remain_interest,
--     overdue_date,
--     if(overdue_date is null,0,datediff(current_date,overdue_date))           as overdue_days,
--     max_dpd,
--     collect_out_date,
--     cast(datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)       as create_time,
--     cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)      as update_time
--   from ods.ccs_loan
--   where d_date = '${ST9}'
-- ) as ccs_loan
-- left join (
--   select distinct
--     ref_nbr,
--     day(loan_pmt_due_date) as cycle_day
--   from ods.ccs_repay_schedule
-- ) as ccs_repay_schedule
-- on ccs_loan.ref_nbr = ccs_repay_schedule.ref_nbr
-- left join (
--   select
--     sum(past_principal + ctd_principal)            as prin,
--     sum(past_interest + ctd_interest)              as interest,
--     sum(penalty_acru + past_penalty + ctd_penalty) as penalty,
--     ref_nbr,
--     d_date as ccs_plan_d_date
--   from ods.ccs_plan
--   group by ref_nbr,d_date
-- ) as ccs_plan
-- on ccs_loan.ref_nbr = ccs_plan.ref_nbr and ccs_loan.d_date = ccs_plan.ccs_plan_d_date

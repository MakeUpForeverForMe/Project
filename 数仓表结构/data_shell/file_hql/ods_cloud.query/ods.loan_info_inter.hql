set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

-- 设置 Container 大小
set hive.tez.container.size=2048;
set tez.am.resource.memory.mb=2048;
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



insert overwrite table ods.loan_info_inter partition(biz_date,product_id)
select
  today.due_bill_no,
  today.apply_no,
  today.loan_active_date,
  today.loan_init_term,
  today.loan_init_principal,
  today.loan_init_interest,
  today.loan_init_term_fee,
  today.loan_init_svc_fee,
  today.loan_term,
  today.account_age,
  today.should_repay_date,
  today.loan_term_repaid,
  today.loan_term_remain,
  today.loan_status,
  today.loan_status_cn,
  today.loan_out_reason,
  today.paid_out_type,
  today.paid_out_type_cn,
  today.paid_out_date,
  today.terminal_date,
  today.paid_amount,
  today.paid_principal,
  today.paid_interest,
  today.paid_penalty,
  today.paid_svc_fee,
  today.paid_term_fee,
  today.paid_mult,
  today.remain_amount,
  today.remain_principal,
  today.remain_interest,
  today.remain_svc_fee,
  today.remain_term_fee,
  today.remain_othAmounts,
  today.overdue_principal,
  today.overdue_interest,
  today.overdue_svc_fee,
  today.overdue_term_fee,
  today.overdue_penalty,
  today.overdue_mult_amt,
  today.overdue_date_start,
  today.overdue_days,
  today.overdue_date,
  today.collect_out_date,
  today.dpd_days_max,
  today.overdue_term,
  today.overdue_terms_count,
  today.overdue_terms_max,
  today.overdue_principal_accumulate,
  today.overdue_principal_max,
  today.create_time,
  today.update_time,
  today.d_date,
  today.product_id
from (
  select
    asset.asset_id                                                                                as due_bill_no,
    asset.asset_id                                                                                as apply_no,
    loan.loan_begin_date                                                                          as loan_active_date,
    if(asset.curr_period is null,1,asset.curr_period)                                             as account_age,
    asset.loan_total_amount                                                                       as loan_init_principal,
    asset.periods                                                                                 as loan_init_term,
    if(asset.assets_status = '已结清',asset.periods,asset.curr_period)                            as loan_term,
    asset.next_pay_date                                                                           as should_repay_date,
    asset.repayedPeriod                                                                           as loan_term_repaid,
    asset.remain_period                                                                           as loan_term_remain,
    nvl(asset.remain_interest,0) + nvl(asset.total_rel_interest,0)                                as loan_init_interest,
    nvl(loan.loan_total_fee,0)                                                                    as loan_init_term_fee,
    0                                                                                             as loan_init_svc_fee,
    case  when asset.assets_status = '正常'   then 'N'
          when asset.assets_status = '已结清' then 'F'
          when asset.assets_status = '逾期'   then 'O'
          else '未知类型'
    end                                                                                           as loan_status,
    asset.assets_status                                                                           as loan_status_cn,
    asset.settle_reason                                                                           as loan_out_reason,
    case  when asset.settle_reason = '提前结清' then 'PRE_SETTLE'
          when asset.settle_reason = '处置结清' then 'DISPOSAL'
          when asset.settle_reason = '正常结清' then 'NORMAL_SETTLE'
          when asset.settle_reason = '逾期结清' then 'OVERDUE_SETTLE'
          when asset.settle_reason = '回购结清' then 'BUY_BACK'
          else asset.settle_reason
    end                                                                                           as paid_out_type,
    asset.settle_reason                                                                           as paid_out_type_cn,
    asset.loan_settlement_date                                                                    as paid_out_date,
    asset.loan_settlement_date                                                                    as terminal_date,
    nvl(asset.total_rel_amount,0)                                                                 as paid_amount,
    nvl(asset.total_rel_principal,0)                                                              as paid_principal,
    nvl(asset.total_rel_interest,0)                                                               as paid_interest,
    nvl(asset.total_rel_penalty,0)                                                                as paid_penalty,
    0                                                                                             as paid_svc_fee,
    nvl(asset.total_rel_fee,0)                                                                    as paid_term_fee,
    0                                                                                             as paid_mult,
    nvl(asset.remain_principal,0) + nvl(asset.remain_interest,0) + nvl(asset.remain_othAmounts,0) as remain_amount,
    nvl(asset.remain_principal,0)                                                                 as remain_principal,
    nvl(asset.remain_interest,0)                                                                  as remain_interest,
    0                                                                                             as remain_svc_fee,
    0                                                                                             as remain_term_fee,
    nvl(asset.remain_othAmounts,0)                                                                as remain_othAmounts,
    nvl(asset.current_overdue_principal,0)                                                        as overdue_principal,
    nvl(asset.current_overdue_interest,0)                                                         as overdue_interest,
    0                                                                                             as overdue_svc_fee,
    nvl(asset.current_overdue_fee,0)                                                              as overdue_term_fee,
    0                                                                                             as overdue_penalty,
    0                                                                                             as overdue_mult_amt,
    if(asset.current_overdue_days > 0,date_sub('${ST9}',asset.current_overdue_days - 1),null)     as overdue_date_start,
    asset.current_overdue_days                                                                    as overdue_days,
    if(asset.current_overdue_days > 0,'${ST9}',null)                                              as overdue_date,
    asset.his_overdue_mdays                                                                       as dpd_days_max,
    null                                                                                          as collect_out_date,
    asset.current_overdue_period                                                                  as overdue_term,
    asset.accum_overdue_period                                                                    as overdue_terms_count,
    asset.his_overdue_mperiods                                                                    as overdue_terms_max,
    asset.accum_overdue_principal                                                                 as overdue_principal_accumulate,
    asset.his_overdue_mprincipal                                                                  as overdue_principal_max,
    asset.account_date                                                                            as d_date,
    asset.create_time                                                                             as create_time,
    asset.update_time                                                                             as update_time,
    asset.project_id                                                                              as product_id
  from (
    select distinct
      asset_id,
      loan_total_amount,
      periods,
      curr_period,
      next_pay_date,
      repayedPeriod,
      remain_period,
      assets_status,
      settle_reason,
      loan_settlement_date,
      total_rel_amount,
      total_rel_principal,
      total_rel_interest,
      total_rel_penalty,
      total_rel_fee,
      remain_principal,
      remain_interest,
      remain_othAmounts,
      current_overdue_principal,
      current_overdue_interest,
      current_overdue_fee,
      current_overdue_days,
      his_overdue_mdays,
      current_overdue_period,
      accum_overdue_period,
      his_overdue_mperiods,
      accum_overdue_principal,
      his_overdue_mprincipal,
      account_date,
      create_time,
      update_time,
      case project_id when 'Cl00333' then 'cl00333' else project_id end as project_id
    from stage.asset_10_t_asset_check
    where account_date = '${ST9}'
      and project_id not in (
        '001601',
        'DIDI201908161538',
        'WS0005200001',
        'CL202012280092',
        'CL202102010097',
        ''
      )
  ) as asset
  left join (
    select distinct
      case is_empty(map_from_str(extra_info)['项目编号'],project_id)
        when 'Cl00333' then 'cl00333'
        else is_empty(map_from_str(extra_info)['项目编号'],project_id)
      end                                                                 as project_id,
      is_empty(map_from_str(extra_info)['借据号'],asset_id)               as asset_id,
      is_empty(
        case when length(map_from_str(extra_info)['实际放款时间']) = 19 then to_date(map_from_str(extra_info)['实际放款时间'])
        else is_empty(datefmt(map_from_str(extra_info)['实际放款时间'],'','yyyy-MM-dd')) end,
        case when length(map_from_str(extra_info)['合同开始时间']) = 19 then to_date(map_from_str(extra_info)['合同开始时间'])
        else is_empty(datefmt(map_from_str(extra_info)['合同开始时间'],'','yyyy-MM-dd')) end
      )                                                                   as loan_begin_date,
      is_empty(map_from_str(extra_info)['贷款总费用(元)'],loan_total_fee) as loan_total_fee
    from stage.asset_01_t_loan_contract_info
  ) as loan
  on  asset.project_id = loan.project_id
  and asset.asset_id   = loan.asset_id
) as today
left join (
  select
    asset.asset_id                                                                                        as due_bill_no,
    asset.asset_id                                                                                        as apply_no,
    loan.loan_begin_date                                                                                  as loan_active_date,
    if(asset.curr_period is null,1,asset.curr_period)                                                     as account_age,
    asset.loan_total_amount                                                                               as loan_init_principal,
    asset.periods                                                                                         as loan_init_term,
    if(asset.assets_status = '已结清',asset.periods,asset.curr_period)                                    as loan_term,
    asset.next_pay_date                                                                                   as should_repay_date,
    asset.repayedPeriod                                                                                   as loan_term_repaid,
    asset.remain_period                                                                                   as loan_term_remain,
    nvl(asset.remain_interest,0) + nvl(asset.total_rel_interest,0)                                        as loan_init_interest,
    nvl(loan.loan_total_fee,0)                                                                            as loan_init_term_fee,
    0                                                                                                     as loan_init_svc_fee,
    case  when asset.assets_status = '正常'   then 'N'
          when asset.assets_status = '已结清' then 'F'
          when asset.assets_status = '逾期'   then 'O'
          else '未知类型'
    end                                                                                                   as loan_status,
    asset.assets_status                                                                                   as loan_status_cn,
    asset.settle_reason                                                                                   as loan_out_reason,
    case  when asset.settle_reason = '提前结清' then 'PRE_SETTLE'
          when asset.settle_reason = '处置结清' then 'DISPOSAL'
          when asset.settle_reason = '正常结清' then 'NORMAL_SETTLE'
          when asset.settle_reason = '逾期结清' then 'OVERDUE_SETTLE'
          when asset.settle_reason = '回购结清' then 'BUY_BACK'
          else asset.settle_reason
    end                                                                                                   as paid_out_type,
    asset.settle_reason                                                                                   as paid_out_type_cn,
    asset.loan_settlement_date                                                                            as paid_out_date,
    asset.loan_settlement_date                                                                            as terminal_date,
    nvl(asset.total_rel_amount,0)                                                                         as paid_amount,
    nvl(asset.total_rel_principal,0)                                                                      as paid_principal,
    nvl(asset.total_rel_interest,0)                                                                       as paid_interest,
    nvl(asset.total_rel_penalty,0)                                                                        as paid_penalty,
    0                                                                                                     as paid_svc_fee,
    nvl(asset.total_rel_fee,0)                                                                            as paid_term_fee,
    0                                                                                                     as paid_mult,
    nvl(asset.remain_principal,0) + nvl(asset.remain_interest,0) + nvl(asset.remain_othAmounts,0)         as remain_amount,
    nvl(asset.remain_principal,0)                                                                         as remain_principal,
    nvl(asset.remain_interest,0)                                                                          as remain_interest,
    0                                                                                                     as remain_svc_fee,
    0                                                                                                     as remain_term_fee,
    nvl(asset.remain_othAmounts,0)                                                                        as remain_othAmounts,
    nvl(asset.current_overdue_principal,0)                                                                as overdue_principal,
    nvl(asset.current_overdue_interest,0)                                                                 as overdue_interest,
    0                                                                                                     as overdue_svc_fee,
    nvl(asset.current_overdue_fee,0)                                                                      as overdue_term_fee,
    0                                                                                                     as overdue_penalty,
    0                                                                                                     as overdue_mult_amt,
    if(asset.current_overdue_days > 0,date_sub(date_sub('${ST9}',1),asset.current_overdue_days - 1),null) as overdue_date_start,
    asset.current_overdue_days                                                                            as overdue_days,
    if(asset.current_overdue_days > 0,date_sub('${ST9}',1),null)                                          as overdue_date,
    asset.his_overdue_mdays                                                                               as dpd_days_max,
    null                                                                                                  as collect_out_date,
    asset.current_overdue_period                                                                          as overdue_term,
    asset.accum_overdue_period                                                                            as overdue_terms_count,
    asset.his_overdue_mperiods                                                                            as overdue_terms_max,
    asset.accum_overdue_principal                                                                         as overdue_principal_accumulate,
    asset.his_overdue_mprincipal                                                                          as overdue_principal_max,
    asset.account_date                                                                                    as d_date,
    asset.project_id                                                                                      as product_id
  from (
    select distinct
      asset_id,
      loan_total_amount,
      periods,
      curr_period,
      next_pay_date,
      repayedPeriod,
      remain_period,
      assets_status,
      settle_reason,
      loan_settlement_date,
      total_rel_amount,
      total_rel_principal,
      total_rel_interest,
      total_rel_penalty,
      total_rel_fee,
      remain_principal,
      remain_interest,
      remain_othAmounts,
      current_overdue_principal,
      current_overdue_interest,
      current_overdue_fee,
      current_overdue_days,
      his_overdue_mdays,
      current_overdue_period,
      accum_overdue_period,
      his_overdue_mperiods,
      accum_overdue_principal,
      his_overdue_mprincipal,
      account_date,
      create_time,
      update_time,
      case project_id when 'Cl00333' then 'cl00333' else project_id end as project_id
    from stage.asset_10_t_asset_check
    where account_date = date_sub('${ST9}',1)
      and project_id not in (
        '001601',
        'DIDI201908161538',
        'WS0005200001',
        'CL202012280092',
        'CL202102010097',
        ''
      )
  ) as asset
  left join (
    select distinct
      case is_empty(map_from_str(extra_info)['项目编号'],project_id)
        when 'Cl00333' then 'cl00333'
        else is_empty(map_from_str(extra_info)['项目编号'],project_id)
      end                                                                 as project_id,
      is_empty(map_from_str(extra_info)['借据号'],asset_id)               as asset_id,
      is_empty(
        case when length(map_from_str(extra_info)['实际放款时间']) = 19 then to_date(map_from_str(extra_info)['实际放款时间'])
        else is_empty(datefmt(map_from_str(extra_info)['实际放款时间'],'','yyyy-MM-dd')) end,
        case when length(map_from_str(extra_info)['合同开始时间']) = 19 then to_date(map_from_str(extra_info)['合同开始时间'])
        else is_empty(datefmt(map_from_str(extra_info)['合同开始时间'],'','yyyy-MM-dd')) end
      )                                                                   as loan_begin_date,
      is_empty(map_from_str(extra_info)['贷款总费用(元)'],loan_total_fee) as loan_total_fee
    from stage.asset_01_t_loan_contract_info
  ) as loan
  on  asset.project_id = loan.project_id
  and asset.asset_id   = loan.asset_id
) as yesterday
on  is_empty(today.product_id                  ,'a') = is_empty(yesterday.product_id                  ,'a')
and is_empty(today.due_bill_no                 ,'a') = is_empty(yesterday.due_bill_no                 ,'a')
and is_empty(today.apply_no                    ,'a') = is_empty(yesterday.apply_no                    ,'a')
and is_empty(today.loan_active_date            ,'a') = is_empty(yesterday.loan_active_date            ,'a')
and is_empty(today.loan_init_term              ,'a') = is_empty(yesterday.loan_init_term              ,'a')
and is_empty(today.loan_term                   ,'a') = is_empty(yesterday.loan_term                   ,'a')
and is_empty(today.should_repay_date           ,'a') = is_empty(yesterday.should_repay_date           ,'a')
and is_empty(today.loan_term_repaid            ,'a') = is_empty(yesterday.loan_term_repaid            ,'a')
and is_empty(today.loan_term_remain            ,'a') = is_empty(yesterday.loan_term_remain            ,'a')
and is_empty(today.loan_status                 ,'a') = is_empty(yesterday.loan_status                 ,'a')
and is_empty(today.loan_out_reason             ,'a') = is_empty(yesterday.loan_out_reason             ,'a')
and is_empty(today.paid_out_type               ,'a') = is_empty(yesterday.paid_out_type               ,'a')
and is_empty(today.paid_out_date               ,'a') = is_empty(yesterday.paid_out_date               ,'a')
and is_empty(today.terminal_date               ,'a') = is_empty(yesterday.terminal_date               ,'a')
and is_empty(today.loan_init_principal         ,  0) = is_empty(yesterday.loan_init_principal         ,  0)
and is_empty(today.loan_init_interest          ,  0) = is_empty(yesterday.loan_init_interest          ,  0)
and is_empty(today.loan_init_term_fee          ,  0) = is_empty(yesterday.loan_init_term_fee          ,  0)
and is_empty(today.paid_principal              ,  0) = is_empty(yesterday.paid_principal              ,  0)
and is_empty(today.overdue_principal           ,  0) = is_empty(yesterday.overdue_principal           ,  0)
and is_empty(today.overdue_interest            ,  0) = is_empty(yesterday.overdue_interest            ,  0)
and is_empty(today.overdue_term_fee            ,  0) = is_empty(yesterday.overdue_term_fee            ,  0)
and is_empty(today.overdue_date                ,'a') = is_empty(yesterday.overdue_date                ,'a')
and is_empty(today.overdue_days                ,'a') = is_empty(yesterday.overdue_days                ,'a')
and is_empty(today.overdue_term                ,'a') = is_empty(yesterday.overdue_term                ,'a')
and is_empty(today.overdue_terms_count         ,'a') = is_empty(yesterday.overdue_terms_count         ,'a')
and is_empty(today.overdue_terms_max           ,'a') = is_empty(yesterday.overdue_terms_max           ,'a')
and is_empty(today.overdue_principal_accumulate,  0) = is_empty(yesterday.overdue_principal_accumulate,  0)
and is_empty(today.overdue_principal_max       ,  0) = is_empty(yesterday.overdue_principal_max       ,  0)
and is_empty(today.dpd_days_max                ,'a') = is_empty(yesterday.dpd_days_max                ,'a')
where yesterday.due_bill_no is null
-- limit 1
;

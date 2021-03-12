-- set var:ST9=2020-12-22;

-- set var:db_suffix=;

-- set var:db_suffix=_cps;

invalidate metadata ods_new_s${var:db_suffix}.loan_info;
invalidate metadata ods_new_s${var:db_suffix}.repay_schedule;
invalidate metadata dm_eagle${var:db_suffix}.eagle_asset_scale_principal_day;
-- ods 借据 与 dm 资产规模 对比 本金余额 = 逾期金额 + 未出账本金
select
  'ods 放款 与 dm 放款规模 对比 本金余额 = 逾期金额 + 未出账本金'                                           as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                                                             as cps,
  '${var:ST9}'                                                                                              as biz_date,
  nvl(loan_info.product_id,repay_schedule.product_id)                                                       as product_id,
  nvl(loan_info.remain_principal,0)                                                                         as remain_ods,
  nvl(dm.remain_principal,0)                                                                                as remain_dm,
  nvl(loan_info.remain_principal,0)        - nvl(dm.remain_principal,0)                                     as diff_remain,
  nvl(dm.remain_principal,0) - (nvl(dm.unposted_principal,0) + nvl(dm.overdue_principal,0)) - (
    if(('${var:ST9}' between '2020-06-23' and '9999-99-99') and nvl(loan_info.product_id,repay_schedule.product_id) = 'vt_001802',-200,0) +
    if(('${var:ST9}' between '2020-11-06' and '2021-01-03') and nvl(loan_info.product_id,repay_schedule.product_id) = 'vt_001802' and '${var:db_suffix}' = '',68.0500,0) +
    if(('${var:ST9}' between '2021-01-04' and '2021-01-17') and nvl(loan_info.product_id,repay_schedule.product_id) = 'vt_001802' and '${var:db_suffix}' = '',-11.2000,0) +
    if(('${var:ST9}' between '2021-01-18' and '9999-99-99') and nvl(loan_info.product_id,repay_schedule.product_id) = 'vt_001802' and '${var:db_suffix}' = '',-83.9200,0) +

    if(('${var:ST9}' between '2020-11-06' and '9999-99-99') and nvl(loan_info.product_id,repay_schedule.product_id) = 'vt_001902' and '${var:db_suffix}' = '',3175.6100,0) +

    if(('${var:ST9}' between '2021-01-04' and '9999-99-99') and nvl(loan_info.product_id,repay_schedule.product_id) = 'vt_001802' and '${var:db_suffix}' = '_cps',-79.2500,0)
  )                                                                                                         as diff_principal,
  nvl(repay_schedule.unposted_principal,0)                                                                  as unposted_ods,
  nvl(dm.unposted_principal,0)                                                                              as unposted_dm,
  nvl(repay_schedule.unposted_principal,0) - nvl(dm.unposted_principal,0)                                   as diff_unposted,
  nvl(loan_info.overdue_principal,0)                                                                        as overdue_ods,
  nvl(dm.overdue_principal,0)                                                                               as overdue_dm,
  nvl(loan_info.overdue_principal,0)       - nvl(dm.overdue_principal,0)                                    as diff_overdue
from (
  select
    product_id_vt          as product_id,
    sum(remain_principal)  as remain_principal,
    sum(overdue_principal) as overdue_principal
  from (
    select product_id,product_id_vt
    from dim_new.biz_conf
    where 1 > 0
      and channel_id = '0006'
      and product_id_vt is not null
  ) as biz_conf
  join (
    select
      product_id,
      remain_principal,
      overdue_principal
    from ods_new_s${var:db_suffix}.loan_info
    where 1 > 0
      and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
      and product_id in (
        '001801','001802','001803','001804',
        '001901','001902','001903','001904','001905','001906','001907',
        '002001','002002','002003','002004','002005','002006','002007',
        '002401','002402'
      )
      and overdue_days < 181
  ) as loan_info
  on biz_conf.product_id = loan_info.product_id
  group by product_id_vt
) as loan_info
full join (
  select
    product_id_vt               as product_id,
    sum(should_repay_principal) as unposted_principal
  from (
    select product_id,product_id_vt
    from dim_new.biz_conf
    where 1 > 0
      and channel_id = '0006'
  ) as biz_conf
  join (
    select
      product_id,
      due_bill_no,
      should_repay_principal
    from ods_new_s${var:db_suffix}.repay_schedule
    where 1 > 0
      and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
      and product_id in (
        '001801','001802','001803','001804',
        '001901','001902','001903','001904','001905','001906','001907',
        '002001','002002','002003','002004','002005','002006','002007',
        '002401','002402',
        ''
      )
      and schedule_status = 'N'
  ) as repay_schedule
  on biz_conf.product_id = repay_schedule.product_id
  left join (
    select
      product_id,
      due_bill_no
    from ods_new_s${var:db_suffix}.loan_info
    where 1 > 0
      and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
      and product_id in (
        '001801','001802','001803','001804',
        '001901','001902','001903','001904','001905','001906','001907',
        '002001','002002','002003','002004','002005','002006','002007',
        '002401','002402',
        ''
      )
      and overdue_days > 180
  ) as loan_info
  on  repay_schedule.product_id  = loan_info.product_id
  and repay_schedule.due_bill_no = loan_info.due_bill_no
  where 1 > 0 and loan_info.due_bill_no is null
  group by product_id_vt
) as repay_schedule
on loan_info.product_id = repay_schedule.product_id
left join (
  select
    product_id              as product_id,
    sum(remain_principal)   as remain_principal,
    sum(overdue_principal)  as overdue_principal,
    sum(unposted_principal) as unposted_principal
  from dm_eagle${var:db_suffix}.eagle_asset_scale_principal_day
  where 1 > 0
    and biz_date = '${var:ST9}'
      and channel_id = '0006'
  group by product_id
) as dm
on nvl(loan_info.product_id,repay_schedule.product_id) = dm.product_id
where 1 > 0
  and (
    nvl(loan_info.remain_principal,0)        - nvl(dm.remain_principal,0)   != 0 or
    nvl(repay_schedule.unposted_principal,0) - nvl(dm.unposted_principal,0) != 0 or
    nvl(loan_info.overdue_principal,0)       - nvl(dm.overdue_principal,0)  != 0 or
    nvl(dm.remain_principal,0) - (nvl(dm.unposted_principal,0) + nvl(dm.overdue_principal,0)) - (
      if(('${var:ST9}' between '2020-06-23' and '9999-99-99') and nvl(loan_info.product_id,repay_schedule.product_id) = 'vt_001802',-200,0) +
      if(('${var:ST9}' between '2020-11-06' and '2021-01-03') and nvl(loan_info.product_id,repay_schedule.product_id) = 'vt_001802' and '${var:db_suffix}' = '',68.0500,0) +
      if(('${var:ST9}' between '2021-01-04' and '2021-01-17') and nvl(loan_info.product_id,repay_schedule.product_id) = 'vt_001802' and '${var:db_suffix}' = '',-11.2000,0) +
      if(('${var:ST9}' between '2021-01-18' and '9999-99-99') and nvl(loan_info.product_id,repay_schedule.product_id) = 'vt_001802' and '${var:db_suffix}' = '',-83.9200,0) +

      if(('${var:ST9}' between '2020-11-06' and '9999-99-99') and nvl(loan_info.product_id,repay_schedule.product_id) = 'vt_001902' and '${var:db_suffix}' = '',3175.6100,0) +

      if(('${var:ST9}' between '2021-01-04' and '9999-99-99') and nvl(loan_info.product_id,repay_schedule.product_id) = 'vt_001802' and '${var:db_suffix}' = '_cps',-79.2500,0)
    ) != 0
  )
order by biz_date,product_id;

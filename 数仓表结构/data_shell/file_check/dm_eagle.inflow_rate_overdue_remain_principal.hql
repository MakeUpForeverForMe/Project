-- set var:db_suffix=;

-- set var:db_suffix=_cps;

-- set var:ST9=2020-08-07;

invalidate metadata ods_new_s${var:db_suffix}.loan_info;
invalidate metadata ods_new_s${var:db_suffix}.repay_schedule;
invalidate metadata dm_eagle${var:db_suffix}.eagle_inflow_rate_day;
-- ods_new_s 借据、应还 与 dm 逾期流入率 对比 应还日本金余额（应还日为T，T-1为C的T-1日实际应还本金）、应还日应还本金（应还日为T，T-1为C的T-1日实际本金余额）、逾期借据逾期本金、逾期借据本金余额
select
  'ods_new_s 借据、应还 与 dm 逾期流入率 对比 分子指标校验'                            as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                                        as cps,
  nvl(ods_new_s.biz_date,dm_eagle.biz_date)                                            as biz_date,
  nvl(ods_new_s.product_id,dm_eagle.product_id)                                        as product_id,
  nvl(ods_new_s.loan_active_month,dm_eagle.loan_active_month)                          as active_month,
  nvl(ods_new_s.dob,dm_eagle.dob)                                                      as dob,
  nvl(ods_new_s.should_repay_date,dm_eagle.should_repay_date)                          as should_date,
  nvl(ods_new_s.overdue_loan_num,0)                                                    as num_ods,
  nvl(dm_eagle.overdue_loan_num,0)                                                     as num_dm,
  nvl(ods_new_s.overdue_loan_num,0)         - nvl(dm_eagle.overdue_loan_num,0)         as num,
  nvl(ods_new_s.overdue_principal,0)                                                   as o_prin_ods,
  nvl(dm_eagle.overdue_principal,0)                                                    as o_prin_dm,
  nvl(ods_new_s.overdue_principal,0)        - nvl(dm_eagle.overdue_principal,0)        as o_prin,
  nvl(ods_new_s.overdue_remain_principal,0)                                            as o_r_prin_ods,
  nvl(dm_eagle.overdue_remain_principal,0)                                             as o_r_prin_dm,
  nvl(ods_new_s.overdue_remain_principal,0) - nvl(dm_eagle.overdue_remain_principal,0) as o_r_prin
from (
  select
    '${var:ST9}'                as biz_date,
    product_id_vt               as product_id,
    loan_active_month           as loan_active_month,
    dob                         as dob,
    should_repay_date           as should_repay_date,
    count(distinct due_bill_no) as overdue_loan_num,
    sum(overdue_principal)      as overdue_principal,
    sum(remain_principal)       as overdue_remain_principal
  from (select product_id_vt,product_id as product_id_biz from dim_new.biz_conf) as biz_conf
  join (
    select
      product_id,
      due_bill_no,
      substr(loan_active_date,1,7) as loan_active_month,
      overdue_days as dob,
      overdue_principal,
      remain_principal
    from ods_new_s${var:db_suffix}.loan_info
    where 1 > 0
      and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
      and overdue_days between 1 and 30
  ) as loan_info
  on product_id_biz = product_id
  join (
    select
      product_id as product_id_c,
      due_bill_no as due_bill_no_c,
      substr(loan_active_date,1,7) as active_month_c,
      s_d_date,
      e_d_date
    from ods_new_s${var:db_suffix}.loan_info
    where 1 > 0
      and overdue_days = 0
  ) as loan_c
  on  product_id        = product_id_c
  and due_bill_no       = due_bill_no_c
  and loan_active_month = active_month_c
  join (
    select
      product_id as product_id_s,
      due_bill_no as due_bill_no_s,
      substr(loan_active_date,1,7) as active_month_s,
      should_repay_date
    from ods_new_s${var:db_suffix}.repay_schedule
    where 1 > 0
      and e_d_date = '3000-12-31'
      and should_repay_date < '${var:ST9}'
  ) as repay_schedule
  on  product_id        = product_id_s
  and due_bill_no       = due_bill_no_s
  and loan_active_month = active_month_s
  and should_repay_date = date_sub('${var:ST9}',dob - 1) -- 取应还日
  where 1 > 0
    and date_sub('${var:ST9}',dob) between s_d_date and date_sub(e_d_date,1) -- 应还日前一天
  group by product_id_vt,loan_active_month,dob,should_repay_date
) as ods_new_s
full join (
  select
    '${var:ST9}'                  as biz_date,
    product_id                    as product_id,
    loan_active_month             as loan_active_month,
    cast(dob as int)              as dob,
    should_repay_date             as should_repay_date,
    sum(overdue_loan_num)         as overdue_loan_num,
    sum(overdue_principal)        as overdue_principal,
    sum(overdue_remain_principal) as overdue_remain_principal
  from dm_eagle.eagle_inflow_rate_day
  where 1 > 0
    and biz_date = '${var:ST9}'
    and is_first = 'n'
  group by product_id,loan_active_month,dob,should_repay_date
) as dm_eagle
on  ods_new_s.biz_date          = dm_eagle.biz_date
and ods_new_s.product_id        = dm_eagle.product_id
and ods_new_s.loan_active_month = dm_eagle.loan_active_month
and ods_new_s.dob               = dm_eagle.dob
and ods_new_s.should_repay_date = dm_eagle.should_repay_date
where 1 > 0
  and ods_new_s.product_id in (
    'vt_001801','vt_001802','vt_001803','vt_001804',
    'vt_001901','vt_001902','vt_001903','vt_001904','vt_001905','vt_001906','vt_001907',
    'vt_002001','vt_002002','vt_002003','vt_002004','vt_002005','vt_002006','vt_002007',
    'vt_002401','vt_002402',
    ''
  )
  and (
    nvl(ods_new_s.overdue_loan_num,0)         - nvl(dm_eagle.overdue_loan_num,0)         != 0 or
    nvl(ods_new_s.overdue_principal,0)        - nvl(dm_eagle.overdue_principal,0)        != 0 or
    nvl(ods_new_s.overdue_remain_principal,0) - nvl(dm_eagle.overdue_remain_principal,0) != 0
  )
order by product_id,active_month,dob;

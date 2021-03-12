select
  c.project_no,
  a.iou_no,
  b.interest_rate_type,
  b.loan_annual_interest_rate,
  b.total_yield,
  b.subsidy_fee,
  b.discount_amount,
  a.remaining_principal,
  a.current_overdue_days,
  b.dealer_name,
  c.car_type,
  c.car_series,
  c.car_models,
  c.car_age
from (
  select * from (
    select distinct iou_no,remaining_principal,current_overdue_days,from_unixtime(cast(message_timestamp/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as message_timestamp
    ,row_number() over(partition by iou_no order by from_unixtime(cast(message_timestamp/1000 as bigint),'yyyy-MM-dd HH:mm:ss') desc) as od
    from ods_starconnect.10_distinct_starconnect_assets_reconciliation_info
    where data_fetch_date = '2020-03-12'
  ) as t0
  where t0.od = 1
) as a
left join (
  select * from (
    select distinct iou_no,interest_rate_type,loan_annual_interest_rate,total_yield,subsidy_fee,discount_amount,from_unixtime(cast(message_timestamp/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as message_timestamp,dealer_name
    ,row_number() over(partition by iou_no order by from_unixtime(cast(message_timestamp/1000 as bigint),'yyyy-MM-dd HH:mm:ss') desc) as od
    from ods_starconnect.01_starconnect_loancontract
    where project_no in ('cl00326','CL201911130070','cl00297','cl00306')
  ) as t1
  where t1.od = 1
) b on a.iou_no = b.iou_no
left join (
  select * from (
    select distinct iou_no,project_no,car_type,car_series,car_models,car_age,from_unixtime(cast(message_timestamp/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as message_timestamp
    ,row_number() over(partition by iou_no order by from_unixtime(cast(message_timestamp/1000 as bigint),'yyyy-MM-dd HH:mm:ss') desc) as od
    from ods_starconnect.04_starconnect_mortgage_car_info
    where from_unixtime(cast(message_timestamp/1000 as bigint),'yyyy-MM-dd HH:mm:ss') =
  ) t2
  where t2.od = 1
) c on a.iou_no = c.iou_no;




select distinct
  case when total_nper <= '12' then '1~12 '
  when 12 < total_nper and total_nper <= 24 then '13~24'
  when 24 < total_nper and total_nper <= 36 then '25~36'
  else '36+' end total_nper,
  case when surplus_nper <= '12' then '1~12 '
  when 12 < surplus_nper and surplus_nper <= 24 then '13~24'
  when 24 < surplus_nper and surplus_nper <= 36 then '25~36'
  else '36+' end surplus_nper
from ods_starconnect.`10_distinct_starconnect_assets_reconciliation_info` limit 10;














select
data_fetch_date,
project_no,
-- total_nper,
-- surplus_nper,
sum(loan_total_amount),
sum(remaining_principal)
from
(select distinct
  data_fetch_date as data_fetch_date,
  project_no,
  cast(loan_total_amount as decimal(15,4)) as loan_total_amount,
  cast(remaining_principal as decimal(15,4)) as remaining_principal
  -- case when cast(total_nper as int) <= 12 then '1~12 '
  -- when 12 < cast(total_nper as int) and cast(total_nper as int) <= 24 then '13~24'
  -- when 24 < cast(total_nper as int) and cast(total_nper as int) <= 36 then '25~36'
  -- else '36+' end total_nper,
  -- case when cast(surplus_nper as int) <= 12 then '1~12 '
  -- when 12 < cast(surplus_nper as int) and cast(surplus_nper as int) <= 24 then '13~24'
  -- when 24 < cast(surplus_nper as int) and cast(surplus_nper as int) <= 36 then '25~36'
  -- else '36+' end surplus_nper
from ods_starconnect.`10_distinct_starconnect_assets_reconciliation_info`
where data_fetch_date = last_day(data_fetch_date)
-- and data_fetch_date = '2019-01-31'
-- and current_overdue_days > 30
-- and iou_no = '1000000060'
and project_no = 'pl00282'
-- order by cast(data_fetch_date as date),current_overdue_days;
) as tmp
group by data_fetch_date,project_no
-- ,total_nper,surplus_nper
order by data_fetch_date;








select
count(1) as cnt
from
(select
  data_fetch_date,
  project_no,
  total_nper,
  surplus_nper,
  sum(cast(loan_total_amount as decimal(15,4))) as loan_total_amount,
  sum(cast(remaining_principal as decimal(15,4))) as remaining_principal
from (
  select
    data_fetch_date,
    project_no,
    case when cast(total_nper as int) <= 12 then '1~12 '
    when 12 < cast(total_nper as int) and cast(total_nper as int) <= 24 then '13~24'
    when 24 < cast(total_nper as int) and cast(total_nper as int) <= 36 then '25~36'
    else '36+' end total_nper,
    case when cast(surplus_nper as int) <= 12 then '1~12 '
    when 12 < cast(surplus_nper as int) and cast(surplus_nper as int) <= 24 then '13~24'
    when 24 < cast(surplus_nper as int) and cast(surplus_nper as int) <= 36 then '25~36'
    else '36+' end surplus_nper,
    cast(loan_total_amount as decimal(15,4)) as loan_total_amount,
    cast(remaining_principal as decimal(15,4)) as remaining_principal
  from ods_starconnect.`10_distinct_starconnect_assets_reconciliation_info`
  where data_fetch_date >= '2019-01-01'
  and data_fetch_date = last_day(data_fetch_date)
  -- and project_no = 'pl00282'
) as tmp
group by data_fetch_date,project_no,total_nper,surplus_nper
order by data_fetch_date) as tmp
;




-- refresh ods_starconnect.`10_starconnect_assets_reconciliation_info`;
select distinct
  data_fetch_date,
  iou_no,
  project_no,
  total_nper,
  surplus_nper,
  current_overdue_days,
  loan_total_amount,
  remaining_principal
from
-- ods_starconnect.`10_distinct_starconnect_assets_reconciliation_info`
(select * from (
    select * ,row_number() over(partition by iou_no,data_fetch_date order by message_timestamp desc) as od
    from ods_starconnect.`10_starconnect_assets_reconciliation_info`
  ) t where t.od=1) as tmp
where project_no = 'pl00282' and data_fetch_date = last_day(data_fetch_date)
and iou_no = 'c1a33202414e48cc8da3c159e88e8237'
-- and 12 < cast(surplus_nper as int) and cast(surplus_nper as int) <= 24
order by data_fetch_date
;




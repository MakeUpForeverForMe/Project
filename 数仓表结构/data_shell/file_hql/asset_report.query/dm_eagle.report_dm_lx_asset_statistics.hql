
insert overwrite table dm_eagle.report_dm_lx_asset_statistics partition(snapshot_date,project_id)
select

  cast(m2plus_recover_amount_accum as decimal(15,4)) as m2plus_recover_amount_accum,
  cast(m2plus_recover_prin_accum as decimal(15,4)) as m2plus_recover_prin_accum,
  cast(m2plus_recover_inter_accum as decimal(15,4))as m2plus_recover_inter_accum,
  cast(m4plus_recover_amount_accum as decimal(15,4))as m4plus_recover_amount_accum,
  cast(m4plus_recover_prin_accum as decimal(15,4))as m4plus_recover_prin_accum,
  cast(m4plus_recover_inter_accum as decimal(15,4))as m4plus_recover_inter_accum,
  cast(static_30_amount as decimal(15,4)) as static_30_amount,
  cast(static_30_prin as decimal(15,4)) as static_30_prin,
  cast(static_30_iner as decimal(15,4))as static_30_iner,
  cast(static_90_amount as decimal(15,4))as static_90_amount,
  cast(static_90_prin as decimal(15,4))as static_90_prin,
  cast(static_90_iner as decimal(15,4))as static_90_iner,
  cast(overdue_remain_principal as decimal(15,4))as overdue_remain_principal,
  cast(acc_buy_back_amount as decimal(15,4))as acc_buy_back_amount,
  cast(from_unixtime(UNIX_TIMESTAMP(date_add('${var:ST9}',1)),'yyyy-MM-dd') as string) as snapshot_date,
  project_id
from
(
  select
  project_id ,
  sum(m2plus_recover_amount_acc) as m2plus_recover_amount_accum,
  sum(m2plus_recover_prin_acc)as m2plus_recover_prin_accum,
  sum(m2plus_recover_inter_acc) as m2plus_recover_inter_accum,
  sum(m4plus_recover_amount_acc) as m4plus_recover_amount_accum,
  sum(m4plus_recover_prin_acc) as m4plus_recover_prin_accum,
  sum(m4plus_recover_inter_acc) as m4plus_recover_inter_accum,
  sum(static_30_acc_amount) as static_30_amount,
  sum(static_acc_30_prin) as static_30_prin,
  sum(static_acc_30_iner) as static_30_iner,
  sum(static_acc_90_amount) as static_90_amount,
  sum(static_acc_90_prin) as static_90_prin,
  sum(static_acc_90_iner) as static_90_iner,
  sum(overdue_remain_principal) as overdue_remain_principal,
  sum(acc_buy_back_amount) as acc_buy_back_amount
  from
  dw_new.dw_report_cal_day where biz_date='${var:ST9}'
  group by  project_id
)temp;



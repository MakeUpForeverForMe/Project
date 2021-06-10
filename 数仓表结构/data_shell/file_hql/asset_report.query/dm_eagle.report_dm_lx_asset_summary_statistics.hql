
insert overwrite table dm_eagle.report_dm_lx_asset_summary_statistics partition(snapshot_date,project_id)
  select

  cast(m2plus_recover_amount as DECIMAL(15,4)) as m2plus_recover_amount ,
  cast(m2plus_recover_prin as DECIMAL(15,4)) as m2plus_recover_prin ,
  cast(m2plus_recover_inter as DECIMAL(15,4))as m2plus_recover_inter ,
  cast(m4plus_recover_amount as DECIMAL(15,4))as m4plus_recover_amount ,
  cast(m4plus_recover_prin as DECIMAL(15,4)) as m4plus_recover_prin,
  cast(m4plus_recover_inter as DECIMAL(15,4))as  m4plus_recover_inter,
  cast(m2plus_num as Int) as m2plus_num,
  cast( m2plus_amount as DECIMAL(15,4)) as m2plus_amount,
  cast( m2plus_prin as DECIMAL(15,4))as  m2plus_prin,
  cast(m2plus_inter as DECIMAL(15,4)) as m2plus_inter ,
  cast(m4plus_num as Int) as m4plus_num,
  cast( m4plus_amount as DECIMAL(15,4))  as m4plus_amount,
  cast( m4plus_prin as DECIMAL(15,4)) as m4plus_prin,
  cast( m4plus_inter as DECIMAL(15,4)) as  m4plus_inter,
  cast( static_30_new_amount as DECIMAL(15,4))  as static_30_new_amount,
  cast(static_30_new_prin as DECIMAL(15,4)) as  static_30_new_prin,
  cast(static_30_new_inter as DECIMAL(15,4))as static_30_new_inter ,
  cast( static_90_new_amount as DECIMAL(15,4))as  static_90_new_amount,
  cast(static_90_new_prin as DECIMAL(15,4)) as static_90_new_prin,
  cast( static_90_new_inter as DECIMAL(15,4))as static_90_new_inter ,
  cast(from_unixtime(UNIX_TIMESTAMP(date_add('${var:ST9}',1)),'yyyy-MM-dd') as string) as snapshot_date,
  project_id
  from
  (
  select
   project_id,
  sum(m2plus_recover_amount) as m2plus_recover_amount,
  sum(m2plus_recover_prin) as m2plus_recover_prin,
  sum(m2plus_recover_inter) as m2plus_recover_inter,
  sum(m4plus_recover_amount) as m4plus_recover_amount,
  sum(m4plus_recover_prin) as m4plus_recover_prin,
  sum(m4plus_recover_inter) as m4plus_recover_inter,
  sum(m2plus_num) as m2plus_num,
  sum(m2plus_amount) as m2plus_amount,
  sum(m2plus_prin) as m2plus_prin,
  sum(m2plus_inter) as m2plus_inter,
  sum(m4plus_num) as m4plus_num,
  sum(m4plus_amount) as m4plus_amount,
  sum(m4plus_prin) as m4plus_prin,
  sum(m4plus_inter) as m4plus_inter,
  sum(static_30_new_amount) as static_30_new_amount,
  sum(static_30_new_prin) as static_30_new_prin,
  sum(static_30_new_inter) as static_30_new_inter,
  sum(static_90_new_amount) as static_90_new_amount,
  sum(static_90_new_prin) as static_90_new_prin,
  sum(static_90_new_inter) as static_90_new_inter
  from  dw_new.dw_report_cal_day where biz_date='${var:ST9}'
  group by project_id
  )temp ;

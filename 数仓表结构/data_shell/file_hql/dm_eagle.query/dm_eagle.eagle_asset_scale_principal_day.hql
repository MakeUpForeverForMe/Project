set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.maxRemoteBlockSizeFetchToMem=4G;
set spark.shuffle.memoryFraction=0.6;
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;


insert overwrite table dm_eagle${db_suffix}.eagle_asset_scale_principal_day partition(biz_date,product_id)
select
  capital_id,channel_id,project_id,
  loan_terms               as loan_terms,
  sum(remain_principal)    as remain_principal,
  sum(overdue_principal)   as overdue_principal,
  sum(unposted_principal)  as unposted_principal,
  if(sum(remain_principal) = 0,0,sum(overdue_principal) / sum(remain_principal) * 100) as overdue_principal_rate,
  if(sum(remain_principal) = 0,0,sum(unposted_principal) / sum(remain_principal) * 100) as unposted_principal_rate,
  biz_date                 as biz_date,
  biz_conf.product_id${vt} as product_id
from (
  select capital_id,channel_id,project_id,product_id,product_id_vt
  from dim_new.biz_conf
  where 1 > 0
    and product_id_vt is not null
) as biz_conf
join (
  select *
  from dw_new${db_suffix}.dw_loan_base_stat_overdue_num_day
  where 1 > 0
    and biz_date = '${ST9}'
    -- and biz_date like '2020%'
    and if(
      product_id in (
        '001801','001802','001803','001804',
        '001901','001902','001903','001904','001905','001906','001907',
        '002001','002002','002003','002004','002005','002006','002007',
        '002401','002402'
      ),
      overdue_days < 180,
      true
    )
) as overdue_num
on overdue_num.product_id = biz_conf.product_id
group by capital_id,channel_id,project_id,loan_terms,biz_date,biz_conf.product_id${vt}
-- limit 10
;

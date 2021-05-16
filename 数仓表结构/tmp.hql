set hivevar:ST9=2020-12-02;


set hivevar:ST9=2020-12-22;

set hivevar:ST9=2020-08-07;

set hivevar:ST9=2020-04-19;

set hivevar:ST9=2020-04-20;

set hivevar:ST9=2021-01-25;

set hivevar:ST9=2021-05-13;

set hive.execution.engine=mr;

set hive.execution.engine=spark;

set mapreduce.map.memory.mb=8192;
set mapreduce.reduce.memory.mb=8192;




set hive.map.aggr=true;                             -- 是否在 Map 端进行聚合，默认为 True
set hive.groupby.skewindata=true;                   -- 有数据倾斜的时候进行负载均衡（默认是 false）
set hive.groupby.mapaggr.checkinterval=100000;      -- 在 Map 端进行聚合操作的条目数目


set spark.shuffle.manager=hash;                      -- 设置ShuffleManager的类型 hash、sort和tungsten-sort（慎用）  -- 报错
set spark.default.parallelism=2000;                  -- spark shuffle的并发度
set spark.sql.shuffle.partitions=2000;               -- spark shuffle的并发度
set spark.shuffle.sort.bypassMergeThreshold=2000;    -- bypass 模式，超过阈值才sort
set spark.sql.autoBroadcastJoinThreshold=1073741824; -- 设置广播变量的大小（b）（默认10M）设置1G


set parquet.memory.min.chunk.size=32768;             -- 设置为32K

set parquet.memory.min.chunk.size=32768;             -- 设置为32K

set hive.optimize.skewjoin=true;                             -- 有数据倾斜时开启负载均衡，默认false
set hive.auto.convert.join=true;                             -- 设置自动选择MapJoin，默认是true
set hive.auto.convert.join.noconditionaltask=true;           -- map-side join
set hive.auto.convert.join.noconditionaltask.size=100000000; -- 多大的表可以自动触发放到内层LocalTask中，默认大小10M
set hive.mapjoin.smalltable.filesize=200000000;              -- 设置mapjoin小表的文件大小为20M，小表阈值

set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.shuffle.memoryFraction=0.6;         -- shuffle操作的内存占比
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.support.quoted.identifiers=None;     -- 设置可以使用正则表达式查找字段
-- set hive.groupby.orderby.position.alias=true; -- 设置 Hive 可以使用 group by 1,2,3
set hive.optimize.index.filter=true;
set hive.stats.fetch.column.stats=true;
-- 关闭自动 MapJoin
-- set hive.auto.convert.join=false;
-- set hive.auto.convert.join.noconditionaltask=false;  -- Hive在基于输入文件大小的前提下将普通JOIN转换成MapJoin，并是否将多个MJ合并成一个
-- set hive.auto.convert.join.noconditionaltask.size=1073741824; -- 多个MJ合并成一个MJ时，其表的总的大小须小于该值，同时hive.auto.convert.join.noconditionaltask必须为true
-- 不忽略 MAPJOIN 标记 /* +mapjoin(date_list) */
set hive.ignore.mapjoin.hint=false;
-- 设置小表不超过多大时开启 mapjoin 优化
set hive.mapjoin.smalltable.filesize=256000000;
set hive.mapjoin.followby.gby.localtask.max.memory.usage=0.9;
-- set hive.mapjoin.optimized.hashtable=false;
-- set hive.mapred.mode=nonstrict; -- 非严格模式
set parquet.memory.min.chunk.size=32768;             -- 设置为10K
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 执行sql前，加上如下参数，禁用hive矢量执行：
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;

set hivevar:product_id='001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007';
set hivevar:product_id=select product_id from dim_new.biz_conf where channel_id = '0006';
set hivevar:vt=_vt;
set hivevar:hive_param_str=;

set hivevar:db_suffix=;set hivevar:tb_suffix=_asset;

set spark.app.name=hive_table${db_suffix};set mapred.job.name=hive_table${db_suffix};





set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.shuffle.memoryFraction=0.6;         -- shuffle操作的内存占比
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.support.quoted.identifiers=None;     -- 设置可以使用正则表达式查找字段
-- set hive.groupby.orderby.position.alias=true; -- 设置 Hive 可以使用 group by 1,2,3
set hive.optimize.index.filter=true;
set hive.stats.fetch.column.stats=true;
-- set hive.auto.convert.join=false;             -- 关闭自动 MapJoin
set hive.auto.convert.join.noconditionaltask.size=1073741824; -- 基于统计信息将基础join转化为map join的阈值
set hive.mapjoin.followby.gby.localtask.max.memory.usage=0.9;
-- set hive.mapjoin.optimized.hashtable=false;
-- set hive.mapred.mode=nonstrict; -- 非严格模式
set parquet.memory.min.chunk.size=32768;             -- 设置为10K
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 执行sql前，加上如下参数，禁用hive矢量执行：
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;

set hivevar:product_id='001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007';
set hivevar:vt=_vt;
set hivevar:hive_param_str=;

set hivevar:db_suffix=_cps;set hivevar:tb_suffix=;

set spark.app.name=hive_table${db_suffix};set mapred.job.name=hive_table${db_suffix};





('WS0006200001','WS0006200002','WS0006200003','WS0009200001')

and product_id in (
  '001801','001802','001803','001804',
  '001901','001902','001903','001904','001905','001906','001907',
  '002001','002002','002003','002004','002005','002006','002007',
  '002401','002402',
  ''
)

and ods_new_s.product_id in (
  'vt_001801','vt_001802','vt_001803','vt_001804',
  'vt_001901','vt_001902','vt_001903','vt_001904','vt_001905','vt_001906','vt_001907',
  'vt_002001','vt_002002','vt_002003','vt_002004','vt_002005','vt_002006','vt_002007',
  'vt_002401','vt_002402',
  ''
)






insert overwrite table ods_new_s${db_suffix}.loan_info_tmp partition(is_settled = 'no',product_id)
select `(is_settled)?+.+` from ods_new_s${db_suffix}.loan_info where due_bill_no != '1120061910384241252747';


insert overwrite table ods_new_s${db_suffix}.loan_info partition(is_settled = 'no',product_id)
select `(is_settled)?+.+` from ods_new_s${db_suffix}.loan_info_tmp;





insert overwrite table ods_new_s${db_suffix}.repay_schedule_tmp partition(is_settled = 'no',product_id)
select `(is_settled)?+.+` from ods_new_s${db_suffix}.repay_schedule where due_bill_no != '1120061910384241252747';



insert overwrite table ods_new_s${db_suffix}.repay_schedule partition(is_settled = 'no',product_id)
select `(is_settled)?+.+` from ods_new_s${db_suffix}.repay_schedule_tmp;




ALTER TABLE dw_new${db_suffix}.dw_loan_base_stat_overdue_num_day RENAME TO dw_new${db_suffix}.dw_loan_base_stat_overdue_num_day_bak_20201028;

insert overwrite table dw_new${db_suffix}.dw_loan_base_stat_overdue_num_day partition(biz_date,product_id)
select
  loan_terms,
  loan_active_date,
  overdue_mob,
  is_first_term_overdue,
  overdue_dob,
  should_repay_date_curr,
  null as should_repay_date_history_curr,
  should_repay_principal_curr,
  should_repay_loan_num_curr,
  0 as should_repay_paid_principal_curr_actual,
  should_repay_principal_curr_actual,
  should_repay_loan_num_curr_actual,
  should_repay_date,
  null as should_repay_date_history,
  should_repay_principal,
  should_repay_loan_num,
  overdue_date_first,
  overdue_date_start,
  is_first_overdue_day,
  overdue_days,
  overdue_stage,
  unposted_principal,
  remain_principal,
  paid_principal,
  overdue_principal,
  overdue_loan_num,
  overdue_remain_principal,
  biz_date,
  product_id
from dw_new${db_suffix}.dw_loan_base_stat_overdue_num_day_bak_20201028;









DROP TABLE IF EXISTS `dm_eagle${db_suffix}.eagle_loan_info`;


ALTER TABLE dm_eagle${db_suffix}.eagle_loan_info_bak_cust_id RENAME TO dm_eagle${db_suffix}.eagle_loan_info;




ALTER TABLE dim_new.dim_encrypt_info RENAME TO dim_new.del_dim_encrypt_info;



insert overwrite table dim_new.dim_encrypt_info partition(product_id)
select distinct
  dim_type,
  dim_encrypt,
  dim_decrypt,
  product_id
from dim_new.del_dim_encrypt_info
-- limit 10
;


select *
from dim_new.dim_encrypt_info
where 1 > 0
  and dim_encrypt = 'a_c6eadf36dc237cc723f29293ba8a7f9b86f0b2d6d57d6895e1cc93cc6c557005'
;












select distinct
  is_empty(map_from_str(extra_info)['交易类型'],trade_type) as txn_type
from ods.t_asset_pay_flow
;

select distinct keys
from stage.asset_01_t_loan_contract_info
lateral view explode(map_keys(map_from_str(extra_info))) key as keys
where 1 > 0
order by keys
-- limit 10
;

select distinct
  asset_id,
  first_repay_date
from stage.asset_01_t_loan_contract_info
order by first_repay_date
limit 10;

select
  project_id,
  asset_id,
  period,
  repay_date
from stage.asset_05_t_repayment_schedule
where asset_id = 'GALC-HL-2006282251'
;
















ALTER TABLE dw_new${db_suffix}.dw_loan_base_stat_should_repay_day RENAME TO dw_new${db_suffix}.del_dw_loan_base_stat_should_repay_day;


ALTER TABLE dw_new${db_suffix}.dw_loan_base_stat_overdue_num_day RENAME TO dw_new${db_suffix}.del_dw_loan_base_stat_overdue_num_day;


insert overwrite table dw_new${db_suffix}.dw_loan_base_stat_overdue_num_day partition(biz_date,product_id)
select * from dw_new${db_suffix}.del_dw_loan_base_stat_overdue_num_day;



ALTER TABLE dw_new.dw_loan_base_stat_overdue_num_day_bak RENAME TO dw_new.del_dw_loan_base_stat_overdue_num_day_bak;
ALTER TABLE dw_new.dw_loan_base_stat_overdue_num_day_tmp RENAME TO dw_new.del_dw_loan_base_stat_overdue_num_day_tmp;
ALTER TABLE dw_new.dw_loan_base_stat_overdue_num_day_bak_20201028 RENAME TO dw_new.del_dw_loan_base_stat_overdue_num_day_bak_20201028;


ALTER TABLE dw_new_cps.dw_loan_base_stat_overdue_num_day_bak RENAME TO dw_new_cps.del_dw_loan_base_stat_overdue_num_day_bak;
ALTER TABLE dw_new_cps.dw_loan_base_stat_overdue_num_day_tmp RENAME TO dw_new_cps.del_dw_loan_base_stat_overdue_num_day_tmp;
ALTER TABLE dw_new_cps.dw_loan_base_stat_overdue_num_day_bak_20201028 RENAME TO dw_new_cps.del_dw_loan_base_stat_overdue_num_day_bak_20201028;

ALTER TABLE dw_new.dw_loan_base_stat_should_repay_day_hst1 RENAME TO dw_new.del_dw_loan_base_stat_should_repay_day_hst1;
ALTER TABLE dw_new_cps.dw_loan_base_stat_should_repay_day_hst1 RENAME TO dw_new_cps.del_dw_loan_base_stat_should_repay_day_hst1;




ALTER TABLE dw_new.dw_loan_base_stat_overdue_num_day RENAME TO dw_new.del_dw_loan_base_stat_overdue_num_day;
ALTER TABLE dw_new.dw_loan_base_stat_overdue_num_day_hst RENAME TO dw_new.dw_loan_base_stat_overdue_num_day;

ALTER TABLE dw_new_cps.dw_loan_base_stat_overdue_num_day RENAME TO dw_new_cps.del_dw_loan_base_stat_overdue_num_day;
ALTER TABLE dw_new_cps.dw_loan_base_stat_overdue_num_day_hst RENAME TO dw_new_cps.dw_loan_base_stat_overdue_num_day;



-- drop table dw_new.del_dw_loan_base_stat_overdue_num_day;
-- drop table dw_new.del_dw_loan_base_stat_overdue_num_day_bak;
-- drop table dw_new.del_dw_loan_base_stat_overdue_num_day_tmp;
-- drop table dw_new.del_dw_loan_base_stat_overdue_num_day_bak_20201028;
-- drop table dw_new.del_dw_loan_base_stat_should_repay_day;

-- drop table dw_new_cps.del_dw_loan_base_stat_should_repay_day;
-- drop table dw_new_cps.del_dw_loan_base_stat_overdue_num_day;
-- drop table dw_new_cps.del_dw_loan_base_stat_overdue_num_day_bak;
-- drop table dw_new_cps.del_dw_loan_base_stat_overdue_num_day_tmp;
-- drop table dw_new_cps.del_dw_loan_base_stat_overdue_num_day_bak_20201028;



invalidate metadata dim.dim_encrypt_info;
invalidate metadata dim.data_conf;

invalidate metadata dim.dim_idno;

invalidate metadata dim.project_info;
invalidate metadata dim.project_due_bill_no;

invalidate metadata dim.bag_info;
invalidate metadata dim.bag_due_bill_no;


invalidate metadata stage.abs_t_related_assets;
invalidate metadata stage.ecas_loan;




invalidate metadata ods.enterprise_info;
invalidate metadata ods.enterprise_info_abs;

invalidate metadata ods.risk_control;
invalidate metadata ods.risk_control_abs;

invalidate metadata ods.linkman_info;
invalidate metadata ods.linkman_info_abs;

invalidate metadata ods.guaranty_info;
invalidate metadata ods.guaranty_info_abs;

invalidate metadata ods.customer_info;
invalidate metadata ods.customer_info_abs;

invalidate metadata ods.loan_lending;
invalidate metadata ods.loan_lending_abs;

invalidate metadata ods.loan_info;
invalidate metadata ods.loan_info_abs;

invalidate metadata ods.repay_detail;
invalidate metadata ods.repay_detail_abs;
invalidate metadata ods.t_07_actualrepayinfo;

invalidate metadata ods.order_info;
invalidate metadata ods.order_info_abs;

invalidate metadata ods.repay_schedule;
invalidate metadata ods.repay_schedule_abs;
invalidate metadata ods.t_05_repaymentplan;

invalidate metadata ods.t_10_basic_asset_stage;
invalidate metadata ods.t_10_basic_asset;


invalidate metadata ods_cps.order_info;




refresh dm_eagle.abs_asset_information_bag_snapshot;
refresh dm_eagle.abs_asset_distribution_bag_snapshot_day;

refresh dm_eagle.abs_early_payment_asset_statistic;
refresh dm_eagle.abs_early_payment_asset_details;


refresh dim.bag_due_bill_no;

refresh ods.customer_info;
refresh ods.guaranty_info;
refresh ods.enterprise_info;
refresh ods.risk_control;
refresh ods.loan_lending;
refresh ods.loan_info;
refresh ods.repay_schedule;
refresh ods.repay_detail;
refresh ods.order_info;
refresh ods.t_10_basic_asset_stage;



refresh dm_eagle.abs_asset_information_bag_snapshot;
refresh dm_eagle.abs_asset_distribution_bag_snapshot_day;
refresh dm_eagle.abs_asset_information_cash_flow_bag_snapshot;

refresh dm_eagle.abs_asset_information_project;
refresh dm_eagle.abs_asset_information_bag;
refresh dm_eagle.abs_asset_information_cash_flow_bag_day;
refresh dm_eagle.abs_asset_distribution_day;
refresh dm_eagle.abs_asset_distribution_bag_day;
refresh dm_eagle.abs_overdue_rate_day;
refresh dm_eagle.abs_overdue_rate_details_day;
refresh dm_eagle.abs_early_payment_asset_statistic;
refresh dm_eagle.abs_early_payment_asset_details;





invalidate metadata dm_eagle.abs_asset_information_bag;
invalidate metadata dm_eagle.abs_asset_information_project;
invalidate metadata dm_eagle.abs_asset_information_bag_snapshot;


invalidate metadata dm_eagle.abs_overdue_rate_details_day;
invalidate metadata dm_eagle.abs_early_payment_asset_details;

invalidate metadata dm_eagle.abs_asset_information_cash_flow_bag_day;
invalidate metadata dm_eagle.abs_asset_information_cash_flow_bag_snapshot;

invalidate metadata dm_eagle.abs_early_payment_asset_statistic;
invalidate metadata dm_eagle.abs_early_payment_asset_details;

invalidate metadata dm_eagle.abs_early_payment_asset_details;



invalidate metadata hivemetastore.dbs;



analyze table dim.bag_info compute statistics;
analyze table dim.bag_due_bill_no compute statistics;
analyze table dw.abs_due_info_day_abs compute statistics;



ALTER TABLE ods_new_s.loan_lending_cloud DROP IF EXISTS PARTITION (product_id = 'Cl00333');



show tables from ods_new_s;

-- ods_new_s层
set hivevar:ods_new_s_db_tb=
loan_info_bak
-- loan_apply_bak_20201014
-- loan_info_bak_20201014
-- loan_info_bak_20201015
-- loan_info_bak_20201020
-- loan_info_bak_20201024
-- loan_info_pf_result_history
-- loan_info_rerun
-- loan_info_tmp_rerun
-- loan_lending_bak_20201018
-- order_info_bak_20201015
-- order_info_bak_20201024
-- repay_detail_bak_20201015
-- repay_detail_bak_20201024
-- repay_schedule_bak_20201014
-- repay_schedule_bak_20201020
-- repay_schedule_bak_20201024
repay_schedule_tmp_rerun
;


ALTER TABLE ods_new_s.${ods_new_s_db_tb} RENAME TO ods_new_s.del_${ods_new_s_db_tb};




ALTER TABLE dm_eagle.abs_overdue_rate_day RENAME TO dm_eagle.del_abs_overdue_rate_day;




show partitions ${dw_db_tb} partition(product_id = 'DIDI201908161538');

alter table ${dw_db_tb} drop if exists partition(product_id = '001601');
alter table ${dw_db_tb} drop if exists partition(product_id = '001602');
alter table ${dw_db_tb} drop if exists partition(product_id = '001603');
alter table ${dw_db_tb} drop if exists partition(product_id = '001702');
alter table ${dw_db_tb} drop if exists partition(product_id = 'DIDI201908161538');


-- dw层
set hivevar:dw_db_tb=
dw_new_cps.dw_loan_base_stat_overdue_num_day
;

show partitions ${dw_db_tb}
-- partition(product_id = 'DIDI201908161538')
;

alter table ${dw_db_tb} drop if exists partition(product_id = '001601');
alter table ${dw_db_tb} drop if exists partition(product_id = '001602');
alter table ${dw_db_tb} drop if exists partition(product_id = '001603');
alter table ${dw_db_tb} drop if exists partition(product_id = '001702');
alter table ${dw_db_tb} drop if exists partition(product_id = 'DIDI201908161538');

-- dm产品级
set hivevar:dm_db_tb=
dm_eagle_cps.eagle_overdue_rate_month
;

show partitions ${dm_db_tb}
-- partition(biz_date = '2020-12-20')
-- partition(product_id = 'DIDI201908161538')
;

alter table ${dm_db_tb} drop if exists partition(product_id = 'vt_001601');
alter table ${dm_db_tb} drop if exists partition(product_id = 'vt_001602');
alter table ${dm_db_tb} drop if exists partition(product_id = 'vt_001603');
alter table ${dm_db_tb} drop if exists partition(product_id = 'vt_001702');
alter table ${dm_db_tb} drop if exists partition(product_id = 'vt_DIDI201908161538');



-- dm项目级
set hivevar:dm_db_tb=
dm_eagle_cps.eagle_should_repay_repaid_amount_day
;

show partitions ${dm_db_tb}
-- partition(biz_date = '2020-12-20')
-- partition(project_id = 'DIDI201908161538')
;

alter table ${dm_db_tb} drop if exists partition(project_id = 'DIDI201908161538');
alter table ${dm_db_tb} drop if exists partition(project_id = 'WS0005200001');
alter table ${dm_db_tb} drop if exists partition(project_id = 'WS10043190001');





















insert overwrite table dim_new.project_due_bill_no partition(project_id)
select
  due_bill_no,
  case project_id
  when '001601' then 'CL202012140024'
  else project_id end project_id
from dim_new.project_due_bill_no;


insert overwrite table ods_new_s.loan_info_cloud partition(is_settled = 'no',product_id)
select
  due_bill_no,
  apply_no,
  loan_active_date,
  loan_init_principal,
  account_age,
  loan_init_term,
  loan_term,
  should_repay_date,
  loan_term_repaid,
  loan_term_remain,
  loan_init_interest,
  loan_init_term_fee,
  loan_init_svc_fee,
  loan_status,
  loan_status_cn,
  loan_out_reason,
  paid_out_type,
  paid_out_type_cn,
  paid_out_date,
  terminal_date,
  paid_amount,
  paid_principal,
  paid_interest,
  paid_penalty,
  paid_svc_fee,
  paid_term_fee,
  paid_mult,
  remain_amount,
  remain_principal,
  remain_interest,
  remain_svc_fee,
  remain_term_fee,
  remain_othAmounts,
  overdue_principal,
  overdue_interest,
  overdue_svc_fee,
  overdue_term_fee,
  overdue_penalty,
  overdue_mult_amt,
  overdue_date_first,
  overdue_date_start,
  overdue_days,
  overdue_date,
  dpd_begin_date,
  dpd_days,
  dpd_days_count,
  dpd_days_max,
  collect_out_date,
  overdue_term,
  overdue_terms_count,
  overdue_terms_max,
  overdue_principal_accumulate,
  overdue_principal_max,
  s_d_date,
  e_d_date,
  effective_time,
  expire_time,
  case product_id
  when '001601' then 'CL202012140024'
  else product_id end product_id
from ods_new_s.loan_info_cloud;


insert overwrite table ods_new_s.cust_loan_mapping partition(biz_date,product_id)
select
  cust_id,
  user_hash_no,
  birthday,
  age,
  due_bill_no,
  biz_date,
  case product_id
  when '001601' then 'CL202012140024'
  else product_id end product_id
from ods_new_s.cust_loan_mapping;



insert overwrite table ods_new_s.customer_info_cloud partition(product_id)
select
  cust_id,
  user_hash_no,
  outer_cust_id,
  idcard_type,
  idcard_no,
  name,
  mobie,
  card_phone,
  sex,
  birthday,
  marriage_status,
  education,
  education_ws,
  idcard_address,
  idcard_area,
  idcard_province,
  idcard_city,
  idcard_county,
  idcard_township,
  resident_address,
  resident_area,
  resident_province,
  resident_city,
  resident_county,
  resident_township,
  job_type,
  job_year,
  income_month,
  income_year,
  cutomer_type,
  case product_id
  when '001601' then 'CL202012140024'
  else product_id end product_id
from ods_new_s.customer_info_cloud;



insert overwrite table ods_new_s.guaranty_info partition(product_id)
select
  due_bill_no,
  guaranty_code,
  guaranty_handling_status,
  guaranty_alignment,
  car_property,
  financing_type,
  guarantee_type,
  pawn_value,
  car_sales_price,
  car_new_price,
  total_investment,
  purchase_tax_amouts,
  insurance_type,
  car_insurance_premium,
  total_poundage,
  cumulative_car_transfer_number,
  one_year_car_transfer_number,
  liability_insurance_cost1,
  liability_insurance_cost2,
  car_type,
  frame_num,
  engine_num,
  gps_code,
  gps_cost,
  license_num,
  car_brand,
  car_system,
  car_model,
  car_age,
  car_energy_type,
  production_date,
  mileage,
  register_date,
  buy_car_address,
  car_colour,
  create_time,
  update_time,
  case product_id
  when '001601' then 'CL202012140024'
  else product_id end product_id
from ods_new_s.guaranty_info;


insert overwrite table ods_new_s.loan_lending_cloud partition(biz_date,product_id)
select
  apply_no,
  contract_no,
  contract_term,
  due_bill_no,
  guarantee_type,
  loan_usage,
  loan_issue_date,
  loan_expiry_date,
  loan_active_date,
  loan_expire_date,
  cycle_day,
  loan_type,
  loan_type_cn,
  contract_daily_interest_rate_basis,
  loan_init_term,
  loan_init_principal,
  interest_rate_type,
  loan_init_interest_rate,
  loan_init_term_fee_rate,
  loan_init_svc_fee_rate,
  loan_init_penalty_rate,
  biz_date,
  case product_id
  when '001601' then 'CL202012140024'
  else product_id end product_id
from ods_new_s.loan_lending_cloud
where 1 > 0
  and product_id != 'CL202012140024'
;


insert overwrite table ods_new_s.repay_schedule_inter partition(biz_date,product_id)
select
  due_bill_no,
  loan_active_date,
  loan_init_principal,
  loan_init_term,
  loan_term,
  start_interest_date,
  curr_bal,
  should_repay_date,
  should_repay_date_history,
  grace_date,
  should_repay_amount,
  should_repay_principal,
  should_repay_interest,
  should_repay_term_fee,
  should_repay_svc_fee,
  should_repay_penalty,
  should_repay_mult_amt,
  should_repay_penalty_acru,
  schedule_status,
  schedule_status_cn,
  repay_status,
  paid_out_date,
  paid_out_type,
  paid_out_type_cn,
  paid_amount,
  paid_principal,
  paid_interest,
  paid_term_fee,
  paid_svc_fee,
  paid_penalty,
  paid_mult,
  reduce_amount,
  reduce_principal,
  reduce_interest,
  reduce_term_fee,
  reduce_svc_fee,
  reduce_penalty,
  reduce_mult_amt,
  effective_date,
  biz_date,
  case product_id
  when '001601' then 'CL202012140024'
  else product_id end product_id
from ods_new_s.repay_schedule_inter
where 1 > 0
  and product_id != 'CL202012140024'
;


select * from ods_new_s.repay_schedule_inter;


insert overwrite table ods_new_s.repay_detail_cloud partition(biz_date,product_id)
select
  due_bill_no,
  loan_active_date,
  loan_init_term,
  repay_term,
  order_id,
  loan_status,
  loan_status_cn,
  overdue_days,
  payment_id,
  txn_time,
  post_time,
  bnp_type,
  bnp_type_cn,
  repay_amount,
  batch_date,
  create_time,
  update_time,
  biz_date,
  case product_id
  when '001601' then 'CL202012140024'
  else product_id end product_id
from ods_new_s.repay_detail_cloud;



insert overwrite table ods_new_s.order_info_cloud partition(biz_date,product_id)
select
  order_id,
  ori_order_id,
  apply_no,
  due_bill_no,
  term,
  channel_id,
  pay_channel,
  command_type,
  order_status,
  order_time,
  repay_serial_no,
  service_id,
  assign_repay_ind,
  repay_way,
  txn_type,
  txn_amt,
  original_txn_amt,
  success_amt,
  currency,
  code,
  message,
  response_code,
  response_message,
  business_date,
  send_time,
  opt_datetime,
  setup_date,
  loan_usage,
  purpose,
  online_flag,
  online_allow,
  order_pay_no,
  bank_trade_no,
  bank_trade_time,
  bank_trade_act_no,
  bank_trade_act_name,
  bank_trade_act_phone,
  service_sn,
  outer_no,
  confirm_flag,
  txn_time,
  txn_date,
  capital_plan_no,
  memo,
  create_time,
  update_time,
  biz_date,
  case product_id
  when '001601' then 'CL202012140024'
  else product_id end product_id
from ods_new_s.order_info_cloud;






















-- 客户表校验
invalidate metadata ods_new_s.cust_loan_mapping;

invalidate metadata ods_new_s.customer_info_cloud;

-- select *
-- from (


select
  '放款表 starCloud' as tbl,
  age                as age,
  annual_income / 12 as income_month,
  annual_income      as income_year,
  marital_status     as marriage_status,
  house_city         as resident_city,
  house_province     as resident_province,
  sex                as sex,
  serial_number      as due_bill_no
from stage.t_02_borrowerinfo
where 1 > 0
  and serial_number = '5100767323'
;

-- ) as a
-- join (

select distinct
  '放款表 ods_new_s' as tbl,
  age,
  income_month,
  income_year,
  marriage_status,
  resident_city,
  resident_province,
  sex,
  birthday,
  product_id,
  due_bill_no
from ods_new_s.customer_info_cloud
where 1 > 0
  and due_bill_no = '5100767323'
;



-- ) as b on a.due_bill_no = b.due_bill_no
-- where a.marriage_status != b.marriage_status
-- and product_id = '001601'
-- limit 10
-- ;



select distinct
  '放款表 calibrati' as tbl,
  is_empty(map_from_str(extra_info)['年龄'],age) as age,
  datefmt(substring(decrypt_aes(is_empty(map_from_str(extra_info)['身份证号'],document_num),'tencentabs123456'),7,8),'yyyyMMdd','yyyy-MM-dd') as birthday,
  is_empty(map_from_str(extra_info)['年收入(元)'],annual_income) / 12 as income_month,
  is_empty(map_from_str(extra_info)['年收入(元)'],annual_income) as income_year,
  is_empty(map_from_str(extra_info)['婚姻状况'],marital_status) as marriage_status,
  is_empty(map_from_str(extra_info)['客户户籍所在市'],is_empty(map_from_str(extra_info)['客户居住所在市'],city))     as resident_city,
  is_empty(map_from_str(extra_info)['客户户籍所在省'],is_empty(map_from_str(extra_info)['客户居住所在省'],province)) as resident_province,
  is_empty(is_empty(map_from_str(extra_info)['性别'],sex),sex_idno(decrypt_aes(is_empty(map_from_str(extra_info)['身份证号'],document_num),'tencentabs123456'))) as sex,
  sex as sex1,
  sex_idno(decrypt_aes(is_empty(map_from_str(extra_info)['身份证号'],document_num),'tencentabs123456')) as sex2,
  is_empty(map_from_str(extra_info)['项目编号'],project_id) as project_id,
  is_empty(map_from_str(extra_info)['借据号'],asset_id) as serial_number
from ods.t_principal_borrower_info
where 1 > 0
  and is_empty(map_from_str(extra_info)['借据号'],asset_id) = '5100767323'
;






-- 放款表校验
invalidate metadata ods_new_s.loan_lending_cloud;

-- select count(1) as cnt from (
select distinct
  starCloud.loan_init_interest_rate as loan_init_interest_rate_star,
  ods_new_s.loan_init_interest_rate as loan_init_interest_rate_ods,
  calibrati.loan_init_interest_rate as loan_init_interest_rate_calibrati,
  calibrati.loan_init_interest_rate1 as loan_init_interest_rate1_calibrati,
  starCloud.due_bill_no as due_bill_no,
  starCloud.project_id as project_id
from (


select
  '放款表 starCloud' as tbl,
  contract_term           as contract_term,
  loan_repay_date         as cycle_day,
  loan_use                as loan_usage,
  guarantee_type          as guarantee_type,
  poundage_rate           as loan_init_term_fee_rate,
  contract_interest_rate  as loan_init_interest_rate,
  daily_calculation_basis as contract_daily_interest_rate_basis,
  serial_number           as due_bill_no,
  create_time,
  update_time,
  project_id
from stage.t_01_loancontractinfo
where 1 > 0
  -- and serial_number = '5100767323'
  and project_id != 'cl00306'
-- ;

) as starCloud
join (

select distinct
  '放款表 ods_new_s' as tbl,
  contract_term,
  cycle_day,
  loan_usage,
  guarantee_type,
  loan_init_term_fee_rate,
  loan_init_interest_rate,
  contract_daily_interest_rate_basis,
  due_bill_no,
  product_id as project_id
from ods_new_s.loan_lending_cloud
where 1 > 0
  -- and due_bill_no = '5100767323'
-- ;

) as ods_new_s
on  starCloud.due_bill_no = ods_new_s.due_bill_no
and starCloud.project_id  = ods_new_s.project_id
join (


select distinct
  '放款表 calibrati' as tbl,
  is_empty(map_from_str(extra_info)['合同期限(月)']) as contract_term,
  case
  when length(map_from_str(extra_info)['贷款还款日']) = 1 then datefmt(map_from_str(extra_info)['贷款还款日'],'d','dd')
  when length(map_from_str(extra_info)['贷款还款日']) = 2 then map_from_str(extra_info)['贷款还款日']
  else is_empty(map_from_str(extra_info)['贷款还款日']) end as cycle_day,
  is_empty(map_from_str(extra_info)['贷款用途'],loan_use) as loan_usage,
  loan_use as loan_usage1,
  is_empty(map_from_str(extra_info)['担保方式'],guarantee_type) as guarantee_type,
  is_empty(guarantee_type) as guarantee_type1,
  is_empty(map_from_str(extra_info)['手续费利率'],0) as loan_init_term_fee_rate,
  is_empty(map_from_str(extra_info)['贷款年利率(%)'],is_empty(loan_interest_rate,0)) as loan_init_interest_rate,
  is_empty(loan_interest_rate,0) as loan_init_interest_rate1,
  is_empty(map_from_str(extra_info)['日利率计算基础']) as contract_daily_interest_rate_basis,
  is_empty(map_from_str(extra_info)['借据号'],asset_id) as due_bill_no,
  create_time,
  update_time,
  is_empty(map_from_str(extra_info)['项目编号'],project_id) as project_id
from ods.t_loan_contract_info
where 1 > 0
  -- and is_empty(map_from_str(extra_info)['借据号'],asset_id) = '5100767323'
-- ;


) as calibrati
on  starCloud.due_bill_no = calibrati.due_bill_no
and starCloud.project_id  = calibrati.project_id
where cast(starCloud.loan_init_interest_rate as double) != cast(ods_new_s.loan_init_interest_rate as double)
order by due_bill_no
-- ) as tmp
limit 20;



invalidate metadata dim_new.bag_info;


insert overwrite table dim_new.bag_info partition(bag_id)
select
  project_id,
  bag_name,
  bag_status,
  bag_remain_principal,
  datefmt(bag_date,'','yyyy-MM-dd') as bag_date,
  insert_date,
  bag_id
from dim_new.bag_info
;







































show partitions dm_eagle.abs_asset_information_bag_snapshot partition(project_id = 'CL202011090089');


show partitions dw.dw_credit_apply_stat_day partition(product_id = 'pl00282');
-- alter table dw.dw_credit_apply_stat_day drop if exists partition (product_id = 'pl00282');


show partitions dw.dw_loan_apply_stat_day partition(product_id = 'pl00282');
-- alter table dw.dw_loan_apply_stat_day drop if exists partition (product_id = 'pl00282');





invalidate metadata dim.biz_conf;



-- 乐信代偿前
set hivevar:db_suffix=;set hivevar:tb_suffix=_asset;
-- 乐信代称后
set hivevar:db_suffix=_cps;set hivevar:tb_suffix=;
-- 其他数据
set hivevar:db_suffix=;set hivevar:tb_suffix=;set hivevar:vt=_vt;

set hivevar:hive_param_str=;


set hivevar:product_id=
'001801','001802','001803','001804',
'001901','001902','001903','001904','001905','001906','001907',
'002001','002002','002003','002004','002005','002006','002007',
'002401','002402',
''
;



set hivevar:product_id='001601';


set hivevar:ST9=2019-10-27;


set hivevar:ST9=2019-11-28;
set hivevar:ST9=2019-11-29;
set hivevar:ST9=2019-11-30;


set hivevar:ST9=2020-10-15;

set hivevar:ST9=2019-10-23;
set hivevar:ST9=2019-10-25;


set hivevar:ST9=2020-10-01;


set hivevar:ST9=2020-11-30;
set hivevar:ST9=2020-12-01;


set hivevar:ST9=2021-02-01;


set hivevar:ST9=2021-04-14;




set tez.am.container.reuse.enabled=false;



-- HBase 插入数据前加这个配置（Hive官网）
set hive.hbase.wal.enabled=false;

set hive.exec.input.listing.max.threads=50; -- 最大1024
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

set hive.execution.engine=mr;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;

set mapreduce.map.java.opts=-Xmx4096m;
set mapreduce.reduce.java.opts=-Xmx4096m;
set yarn.app.mapreduce.am.resource.mb=5192;
set yarn.app.mapreduce.am.command-opts=-Xmx4096m;



set tez.task.resource.cpu.vcores=1;
set tez.runtime.pipelined-shuffle.enabled=true;
set tez.runtime.io.sort.mb=1536;
set tez.runtime.unordered.output.buffer.size-mb=1024;


-- 关闭yarn虚拟内存检查
set yarn.nodemanager.vmem-check-enabled=false;
-- 使 Hive 写入时的线程数为 1
set hive.load.dynamic.partitions.thread=1;

set hive.cbo.enable=false;

set fs.cosn.read.ahead.block.size=424288;
set fs.cosn.read.ahead.queue.size=2;
set fs.cosn.read.ahead.block.size=524288;
set fs.cosn.upload.buffer=mapped_disk;
set fs.cosn.upload.buffer.size=-1;
set fs.cosn.read.ahead.block.size=524288;
set hive.exec.input.listing.max.threads=36;



-- 设置map处理的文件大小 可减少Map task 的个数
set mapreduce.input.fileinputformat.split.maxsize=1024000000;



-- 关闭自动 MapJoin （ Hive3 的 bug，引发 No work found for tablescan ）
set hive.auto.convert.join=false;
set hive.auto.convert.join.noconditionaltask=false;

-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=64000000;      -- 64M
set hive.merge.smallfiles.avgsize=64000000; -- 64M
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=200000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;

-- 设置可以使用正则匹配 `(a|b)?+.+`
set hive.support.quoted.identifiers=None;






case project_id when 'Cl00333' then 'cl00333' else project_id end as project_id

select distinct product_id
from (
  select
    max(if(col_name = 'channel_id',col_val,null)) as channel_id,
    max(if(col_name = 'product_id',col_val,null)) as product_id
  from dim.data_conf
  where col_type = 'ac'
  group by col_id
) as tmp
where channel_id = '0006'


select
  product_id,
  due_bill_no,
  max(if(map_key = 'wind_control_status',map_val,null)) as wind_control_status,
  max(if(map_key = 'cheat_level',map_val,null))         as cheat_level,
  max(if(map_key = 'score_range',map_val,null))         as score_range
from ods.risk_control
where source_table in ('t_asset_wind_control_history')
  and map_key in ('wind_control_status','cheat_level','score_range')
group by product_id,due_bill_no
limit 10
;



(
  select distinct
    capital_id,
    channel_id,
    project_id,
    product_id_vt,
    product_id
  from (
    select
      max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
      max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
      max(if(col_name = 'project_id',   col_val,null)) as project_id,
      max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
      max(if(col_name = 'product_id',   col_val,null)) as product_id
    from dim.data_conf
    where col_type = 'ac'
    group by col_id
  ) as tmp
) as biz_conf





  select distinct
    abs_project_id,
    channel_id
  from (
    select
      max(if(col_name = 'project_id',  col_val,null)) as abs_project_id,
      max(if(col_name = 'channel_id',  col_val,null)) as channel_id
    from dim.data_conf
    where col_type = 'pp'
    group by col_id
  ) as tmp

  and is_empty(map_from_str(extra_info)['项目编号'],project_id) not in (
    '001601',           -- 汇通
    'DIDI201908161538', -- 滴滴
    'WS0005200001',     -- 瓜子
    'CL202012280092',   -- 汇通国银
    'CL202102010097',   -- 汇通国银
    ''
  )



(
      select distinct
        product_id,
        channel_id
      from (
        select
          max(if(col_name = 'product_id',  col_val,null)) as product_id,
          max(if(col_name = 'channel_id',  col_val,null)) as channel_id
        from dim.data_conf
        where col_type = 'ac'
        group by col_id
      ) as tmp
    ) as biz_conf
    join
    on linkman.product_id = biz_conf.product_id


  select distinct
    product_id as dim_product_id,
    channel_id
  from (
    select
      max(if(col_name = 'product_id',  col_val,null)) as product_id,
      max(if(col_name = 'channel_id',  col_val,null)) as channel_id
    from dim.data_conf
    where col_type = 'ac'
    group by col_id
  ) as tmp


      select distinct
        project_id,
        bag_id,
        bag_date
      from (
        select
          max(if(col_name = 'project_id',col_val,null)) as project_id,
          max(if(col_name = 'bag_id',    col_val,null)) as bag_id,
          max(if(col_name = 'bag_date',  col_val,null)) as bag_date
        from dim.data_conf
        where 1 > 0
          and col_type = 'ab'
        group by col_id
      ) as tmp

    select *
    from (
      select distinct
        project_id,
        bag_id,
        bag_date
      from (
        select
          max(if(col_name = 'project_id',col_val,null)) as project_id,
          max(if(col_name = 'bag_id',    col_val,null)) as bag_id,
          max(if(col_name = 'bag_date',  col_val,null)) as bag_date
        from dim.data_conf
        where 1 > 0
          and col_type = 'ab'
        group by col_id
      ) as tmp
    ) as biz_conf
    where 1 > 0
      and bag_date <= '2019-01-01';







select
  ods.project_id    as project_id_ods,   stage.project_id    as project_id_stage,   if(nvl(ods.project_id,   'a') != nvl(stage.project_id,   'a'),1,0) as product_id,
  ods.serial_number as serial_number_ods,stage.serial_number as serial_number_stage,if(nvl(ods.serial_number,'a') != nvl(stage.serial_number,'a'),1,0) as serial_number,
  '123'             as aa
from (
  select distinct
    (case is_empty(map_from_str(a.extra_info)['项目编号'],a.project_id) when 'Cl00333' then 'cl00333' else is_empty(map_from_str(a.extra_info)['项目编号'],a.project_id) end) as project_id,
    is_empty(map_from_str(a.extra_info)['借据号'],a.asset_id) as serial_number,
    '123' as aa
  from ods.t_loan_contract_info      as a
  join ods.t_principal_borrower_info as b
  on  (case is_empty(map_from_str(a.extra_info)['项目编号'],a.project_id) when 'Cl00333' then 'cl00333' else is_empty(map_from_str(a.extra_info)['项目编号'],a.project_id) end) = is_empty(map_from_str(b.extra_info)['项目编号'],b.project_id)
  and is_empty(map_from_str(a.extra_info)['借据号'],a.asset_id)     = is_empty(map_from_str(b.extra_info)['借据号'],b.asset_id)
  where 1 > 0
    and (case is_empty(map_from_str(a.extra_info)['项目编号'],a.project_id) when 'Cl00333' then 'cl00333' else is_empty(map_from_str(a.extra_info)['项目编号'],a.project_id) end) in (select distinct project_id from stage.t_project)
    and is_empty(map_from_str(a.extra_info)['借据号'],a.asset_id) not in (
      '5100835880',
      '5100836522',
      '5100839019',
      '5100842477',
      '5100844953',
      '5100847081',
      '5100848663',
      '5100851402',
      '5100851697',
      '5100854650',
      '5100855935',
      '5100856230',
      '5100857239',
      '5100871716',
      '5100872146',
      '5100874704',
      ''
    )
) as ods
full join (
  select distinct
    case a.project_id when 'Cl00333' then 'cl00333' else a.project_id end as project_id,
    a.serial_number,
    '123' as aa
  from stage.t_01_loancontractinfo as a
  join stage.t_02_borrowerinfo     as b
  on  (case a.project_id when 'Cl00333' then 'cl00333' else a.project_id end) = b.project_id
  and a.serial_number = b.serial_number
  where 1 > 0
    and (case a.project_id when 'Cl00333' then 'cl00333' else a.project_id end) in (
      select distinct a.project_id
      from (
        select distinct (case is_empty(map_from_str(extra_info)['项目编号'],project_id) when 'Cl00333' then 'cl00333' else is_empty(map_from_str(extra_info)['项目编号'],project_id) end) as project_id
        from ods.t_loan_contract_info
        where (case is_empty(map_from_str(extra_info)['项目编号'],project_id) when 'Cl00333' then 'cl00333' else is_empty(map_from_str(extra_info)['项目编号'],project_id) end) in (select distinct project_id from stage.t_project)
      ) as a
      join (select distinct is_empty(map_from_str(extra_info)['项目编号'],project_id) as project_id from ods.t_principal_borrower_info) as b
      on a.project_id = b.project_id
    )
) as stage
on  ods.project_id    = stage.project_id
and ods.serial_number = stage.serial_number
where 1 > 0
  and (
    nvl(ods.project_id,   'a') != nvl(stage.project_id,   'a') or
    nvl(ods.serial_number,'a') != nvl(stage.serial_number,'a')
  )
order by project_id_ods,project_id_stage,serial_number_ods,serial_number_stage
limit 100
;


select distinct keys
from stage.asset_07_t_repayment_info
lateral view explode(map_keys(map_from_str(extra_info))) key as keys
order by keys;





insert overwrite table stage.ecas_msg_log partition(is_his,msg_type)
select
  msg_log_id,
  foreign_id,
  original_msg,
  target_msg,
  deal_date,
  org,
  create_time,
  update_time,
  jpa_version,
  is_his,
  msg_type
from stage.ecas_msg_log
where 1 > 0
  and is_his = 'Y'
order by deal_date,update_time
;



select due_bill_no,age,idcard_province,user_hash_no from ods.customer_info
where idcard_province is null
  and product_id in (
    'DIDI201908161538',

    '001601',
    '001602',
    '001603',
    '002201',
    '002202',
    '002203',

    '001801',
    '001802',
    '001901',
    '001902',
    '001906',
    '002001',
    '002002',
    '002006',
    '002401',
    '002402',

    '001701',
    '001702',
    ''
  )
limit 10;



select due_bill_no,age,idcard_province,user_hash_no from ods.customer_info
where age = -1
  and product_id in (
    'DIDI201908161538',

    '001601',
    '001602',
    '001603',
    '002201',
    '002202',
    '002203',

    -- '001801',
    '001802',
    -- '001901',
    '001902',
    '001906',
    -- '002001',
    '002002',
    '002006',
    '002401',
    '002402',

    '001701',
    '001702',
    ''
  )
limit 10;

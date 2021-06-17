#!/bin/bash
ST9=$1
beeline_commad='beeline -n hadoop -u "jdbc:hive2://10.80.0.46:2181,10.80.0.255:2181,10.80.1.113:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" --color=true --hiveconf hive.resultset.use.unique.column.names=false'
[[ -z "$ST9" ]]&&{
  ST9=$(date -d '-1 day' +\%F)
}
table_list=(
stage.ecas_loan_asset
stage.ecas_repay_schedule_asset
stage.ecas_repay_schedule
stage.ecas_loan
stage.ecas_repay_hst
stage.ecas_repay_hst_asset
stage.ecas_order
stage.ecas_order_asset
stage.ecas_order_hst
stage.ecas_order_hst_asset
)
partition_keys=(lx lx2 lx3 lxzt)

for table_name in "${table_list[@]}"
do
  echo "${partition_keys}"
  for partition_key in "${partition_keys[@]}"
  do
     echo "执行表统计信息刷新mysql的元数据:${table_name},${partition_key},sql:ANALYZE TABLE ${table_name} partition(d_date = '${ST9}',p_type='${partition_key}') COMPUTE STATISTICS noscan"
    $beeline_commad -e "ANALYZE TABLE ${table_name} partition(d_date = '${ST9}',p_type='${partition_key}') COMPUTE STATISTICS noscan;" 2>>/dev/null
  done;
done;



#
###analyze TABLE stage.ecas_loan_asset partition(d_date = '2021-06-09',p_type=='lx') COMPUTE STATISTICS noscan;
###ANALYZE TABLE stage.ecas_repay_schedule_asset partition(d_date = '2021-06-09',p_type=='lx') COMPUTE STATISTICS noscan;
###ANALYZE TABLE stage.ecas_repay_schedule partition(d_date = '2021-06-09',p_type=='lx') COMPUTE STATISTICS noscan;
###ANALYZE TABLE stage.ecas_loan partition(d_date = '2021-06-09',p_type=='lx') COMPUTE STATISTICS noscan;
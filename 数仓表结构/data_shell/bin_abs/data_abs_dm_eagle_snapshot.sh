#!/bin/bash -e

. ${data_ods_new_s_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh

s_date=$1
e_date=$2
bag_id=$3
base_file_name=$(basename "${BASH_SOURCE[0]}")

[[ -z $s_date || -z $e_date ]] && {
  echo "$(date +'%F %T') ${base_file_name} 请输入 开始和结束日期 参数！"
  exit 1
}

log=$log/${base_file_name}.${e_date}.log

echo -e "${date_s_aa:=$(date +'%F %T')} abs资产 dm_eagle封包快照  开始\n" &>> $log
# dm 层

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_project.hql -i $param_dir/dm_eagle.abs-param.hql &
sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_bag.hql -i $param_dir/dm_eagle.abs-param.hql &
sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_bag_snapshot.hql -i $param_dir/dm_eagle.abs-param.hql & # 只在封包时跑一次就好
sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_cash_flow_bag_snapshot.hql -i $param_dir/dm_eagle.abs-param.hql & # 只在封包时跑一次就好

wait_jobs

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_overdue_rate_day.hql -k bag_id='' &
sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_early_payment_asset_details.hql   &
sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_early_payment_asset_statistic.hql &

wait_jobs

table_list=(
  dm_eagle.abs_asset_information_bag-$dm_eagle_uabs_core
  dm_eagle.abs_asset_information_project-$dm_eagle_uabs_core
  dm_eagle.abs_asset_information_bag_snapshot-$dm_eagle_uabs_core # 只在创建包的时候导出一次

  dm_eagle.abs_asset_information_cash_flow_bag_snapshot-$dm_eagle_uabs_core # 只在创建包的时候导出一次

  dm_eagle.abs_overdue_rate_day-$dm_eagle_uabs_core
)

sh $bin/data_export.sh $s_date $e_date "${table_list[*]}" &>> $log

wait_jobs

echo -e "${date_e_aa:=$(date +'%F %T')} abs资产 dm_eagle封包快照 结束    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log

$mail $pm_rd '数据 4.0 abs资产 dm_eagle封包快照  执行结束' "
  执行开始日期： $s_date
  执行结束日期： $e_date
  执行开始时间： $date_s_aa
  执行结束时间： $date_e_aa
  执行执行时长： $during_time
" &>> $log

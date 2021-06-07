#!/usr/bin/env bash

. ${data_ods_new_s_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh


s_date=$1
e_date=$2
base_file_name=$(basename "${BASH_SOURCE[0]}")

[[ -z $s_date || -z $e_date ]] && {
  echo "$(date +'%F %T') ${base_file_name} 请输入 开始和结束日期 参数！"
  exit 1
}

log=$log/${base_file_name}.${e_date}.log

echo -e "${date_s_aa:=$(date +'%F %T')} 资产 ods_cloud dw_dm-day 开始 当前脚本进程ID为：$(pid)\n" &>> $log

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dw_new_hql/dw.abs_due_info_day.hql -i $param_dir/dw.abs-param.hql &

wait_jobs
echo -e "${date_a_aa:=$(date +'%F %T')} 资产 ods_cloud dw_dm-day dw 结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_a_aa" "$date_s_aa")\n\n" &>> $log

echo -e "${date_b_aa:=$(date +'%F %T')} 资产 ods_cloud dw_dm-day dm 开始 当前脚本进程ID为：$(pid)\n" &>> $log

# 只在封包时跑一次就好
# sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_bag_snapshot.hql -i $param_dir/dm_eagle.abs-param.hql &
# sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_bag_snapshot_day.hql -i $param_dir/dm_eagle.abs-param.hql &
# sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_cash_flow_bag_snapshot.hql -i $param_dir/dm_eagle.abs-param.hql &

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_project.hql -i $param_dir/dm_eagle.abs-param.hql &
sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_bag.hql -i $param_dir/dm_eagle.abs-param.hql &

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_day.hql -i $param_dir/dm_eagle.abs-param.hql &
sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_bag_day.hql -i $param_dir/dm_eagle.abs-param.hql &

sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_cash_flow_bag_day.hql -i $param_dir/dm_eagle.abs-param.hql &

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_overdue_rate_day.hql -i $param_dir/dm_eagle.abs-param.hql &
sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_overdue_rate_details_day.hql -i $param_dir/dm_eagle.abs-param.hql &

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_early_payment_asset_statistic.hql -i $param_dir/dm_eagle.abs-param.hql &
sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_early_payment_asset_details.hql -i $param_dir/dm_eagle.abs-param.hql &

wait_jobs

echo -e "${date_c_aa:=$(date +'%F %T')} 资产 ods_cloud dw_dm-day dm 结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_c_aa" "$date_b_aa")\n\n" &>> $log

echo -e "${date_d_aa:=$(date +'%F %T')} 资产 ods_cloud dw_dm-day 数据导出上传  开始 当前脚本进程ID为：$(pid)" &>> $log

table_list=(
  # 只在创建包的时候导出一次
  # dm_eagle.abs_asset_information_bag_snapshot-$dm_eagle_uabs_core
  # dm_eagle.abs_asset_distribution_bag_snapshot_day-$dm_eagle_uabs_core
  # dm_eagle.abs_asset_information_cash_flow_bag_snapshot-$dm_eagle_uabs_core

  dm_eagle.abs_asset_information_bag-$dm_eagle_uabs_core
  dm_eagle.abs_asset_information_project-$dm_eagle_uabs_core

  dm_eagle.abs_asset_distribution_day-$dm_eagle_uabs_core
  dm_eagle.abs_asset_distribution_bag_day-$dm_eagle_uabs_core

  dm_eagle.abs_asset_information_cash_flow_bag_day-$dm_eagle_uabs_core

  dm_eagle.abs_overdue_rate_day-$dm_eagle_uabs_core

  # 不需要导出到 MySQL
  # dm_eagle.abs_overdue_rate_details_day-$dm_eagle_uabs_core
  # dm_eagle.abs_early_payment_asset_details-$dm_eagle_uabs_core
  # dm_eagle.abs_early_payment_asset_statistic-$dm_eagle_uabs_core
)

sh $bin/data_export.sh $s_date $e_date "${table_list[*]}" &>> $log

echo -e "${date_f_aa:=$(date +'%F %T')} 资产 ods_cloud dw_dm-day 数据导出上传  结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_f_aa" "$date_d_aa")\n\n" &>> $log

echo -e "${date_e_aa:=$(date +'%F %T')} 资产 ods_cloud dw_dm-day 结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log


$mail $pm_rd "ods_cloud dw_dm-day $(pid)" "
  执行开始日期： $s_date
  执行结束日期： $e_date
  执行开始时间： $date_s_aa
  执行结束时间： $date_e_aa
  执行执行时长： $during_time
" &>> $log

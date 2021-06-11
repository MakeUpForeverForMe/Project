#!/usr/bin/env bash

. ${data_ods_new_s_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh



s_date=$1
e_date=$2
project_id=${3}

[[ -z $s_date || -z $e_date || -z $project_id ]] && error "请输入 开始（$s_date）和结束（$e_date）日期 参数！或项目编号（$project_id）为空"

log_file=$log/$(basename "${BASH_SOURCE[0]}").${e_date}.log

job_name='星云 重新计算所有包'

info "${job_name} 跑批 开始！"
date_s_time=$curr_time

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_project.hql -k project_id="'${project_id}'" -k bag_id="select distinct bag_id from dim.bag_info" &

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_day.hql -k project_id="'${project_id}'" -k bag_id="select distinct bag_id from dim.bag_info" &

sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_cash_flow_bag_day.hql -k project_id="'${project_id}'" -k bag_id="select distinct bag_id from dim.bag_info" &

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_overdue_rate_day.hql -k project_id="'${project_id}'" -k bag_id="select distinct bag_id from dim.bag_info" &
sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_overdue_rate_details_day.hql -k project_id="'${project_id}'" -k bag_id="select distinct bag_id from dim.bag_info" &

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_early_payment_asset_statistic.hql -k project_id="'${project_id}'" -k bag_id="select distinct bag_id from dim.bag_info" &
sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_early_payment_asset_details.hql -k project_id="'${project_id}'" -k bag_id="select distinct bag_id from dim.bag_info" &

wait_jobs

info "${job_name} 跑批 结束！用时：$(during "$date_s_time")"

info "${job_name} 数据导出上传 开始 "
date_a_time=$curr_time

table_list=(
  # 直接在 MySQL 中删除
  # dm_eagle.abs_asset_information_bag_snapshot-$dm_eagle_uabs_core
  # dm_eagle.abs_asset_distribution_bag_snapshot_day-$dm_eagle_uabs_core
  # dm_eagle.abs_asset_information_cash_flow_bag_snapshot-$dm_eagle_uabs_core
  # dm_eagle.abs_asset_information_bag-$dm_eagle_uabs_core
  # dm_eagle.abs_asset_distribution_bag_day-$dm_eagle_uabs_core

  dm_eagle.abs_asset_information_project-$dm_eagle_uabs_core
  dm_eagle.abs_asset_distribution_day-$dm_eagle_uabs_core
  dm_eagle.abs_asset_information_cash_flow_bag_day-$dm_eagle_uabs_core
  dm_eagle.abs_overdue_rate_day-$dm_eagle_uabs_core

  # 不需要导出到 MySQL
  # dm_eagle.abs_overdue_rate_details_day-$dm_eagle_uabs_core
  # dm_eagle.abs_early_payment_asset_details-$dm_eagle_uabs_core
  # dm_eagle.abs_early_payment_asset_statistic-$dm_eagle_uabs_core
)

sh $bin/data_export.sh $s_date $e_date "${table_list[*]}" 2>&1 | tee -a $log_file

info "${job_name} 数据导出上传 结束！用时：$(during "$date_a_time")"

info "${job_name} 跑批 结束！用时：${during_time:=$(during "$date_s_time")}"


$mail $pm_rd "${job_name} 跑批 $(pid)" "
  执行开始日期： $s_date
  执行结束日期： $e_date
  执行开始时间： $date_s_time
  执行结束时间： $curr_time
  执行执行时长： $during_time
" 2>&1 | tee -a $log_file

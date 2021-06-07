#!/usr/bin/env bash

. ${data_ods_new_s_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh

s_date=$1
e_date=$2
bag_id=$3

[[ -z $s_date || -z $e_date || -z $bag_id ]] && error "请输入 开始（$s_date）和结束（$e_date）日期 参数！或包编号为空"

log_file=$log/$(basename "${BASH_SOURCE[0]}").${e_date}.log


job_name='星云 封包时计算任务'

info "${job_name} 跑批 开始！"
date_s_time=$curr_time

# 只在封包时跑一次就好
sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_bag_snapshot.hql -k bag_id="'${bag_id}'" &
sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_bag_snapshot_day.hql -k bag_id="'${bag_id}'" &
sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_cash_flow_bag_snapshot.hql -k bag_id="'${bag_id}'" &

wait_jobs

info "${job_name} 跑批 结束！用时：$(during "$date_s_time")"

info "${job_name} 数据导出上传 开始 "
date_a_time=$curr_time

table_list=(
  # 只在创建包的时候导出一次
  dm_eagle.abs_asset_information_bag_snapshot-$dm_eagle_uabs_core
  dm_eagle.abs_asset_distribution_bag_snapshot_day-$dm_eagle_uabs_core
  dm_eagle.abs_asset_information_cash_flow_bag_snapshot-$dm_eagle_uabs_core
)

sh $bin/data_export.sh $s_date $e_date "${table_list[*]}" 2>&1 | tee -a $log_file

info "${job_name} 数据导出上传 结束！用时：$(during "$date_a_time")"

info "${job_name} 跑批 结束！用时：${during_time:=$(during "$date_s_time")}"


$mail $ximing "${job_name} 跑批 $(pid)" "
  执行开始日期： $s_date
  执行结束日期： $e_date
  执行开始时间： $date_s_time
  执行结束时间： $curr_time
  执行执行时长： $during_time
" 2>&1 | tee -a $log_file

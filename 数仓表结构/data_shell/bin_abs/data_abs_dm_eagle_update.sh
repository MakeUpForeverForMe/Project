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

echo -e "${date_s_aa:=$(date +'%F %T')} abs资产 dim_new封包更新状态  开始\n" &>> $log
# dm 层

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dim_new_hql/dim.abs_asset_information_update.hql -k bag_id="${bag_id}" -b "AssetFileToHive-${bag_id}" &

wait_jobs

echo -e "${date_e_aa:=$(date +'%F %T')} abs资产 dim_new封包更新状态 结束    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log

$mail $pm_rd '数据 4.0 abs资产 dim_new封包更新状态  执行结束' "
  执行开始日期： $s_date
  执行结束日期： $e_date
  执行开始时间： $date_s_aa
  执行结束时间： $date_e_aa
  执行执行时长： $during_time
" &>> $log

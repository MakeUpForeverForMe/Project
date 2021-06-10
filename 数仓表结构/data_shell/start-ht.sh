#!/bin/bash -e

. ${start_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/conf_env/env.sh

s_date=$1
e_date=$2
repair_date=$3
base_file_name=$(basename "${BASH_SOURCE[0]}")
log=$log/${base_file_name}.${e_date}.log

[[ -z $s_date || -z $e_date ]] && {
  echo "$(date +'%F %T') ${base_file_name} 请输入 开始和结束日期 参数！"
  exit 1
}

echo -e "${date_s_aa:=$(date +'%F %T')}EMR 汇通任务 开始 当前脚本进程ID为：$(pid)\n" &>> $log


sh $bin_abn/ht_ods_new_s.sh  $s_date $e_date $repair_date &>> $log

sh $bin_abn/ht_asset_report.sh  $s_date $e_date  &>> $log


echo -e "${date_e_aa:=$(date +'%F %T')} EMR 汇通任务 结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log

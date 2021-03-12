#!/bin/bash -e

. ${start_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/conf_env/env.sh
. $lib/function.sh

s_date=$1
e_date=$2

base_file_name=$(basename "${BASH_SOURCE[0]}")
log=$log/${base_file_name}.${e_date}.log

[[ -z $s_date || -z $e_date ]] && {
  echo "$(date +'%F %T') ${base_file_name} 请输入 开始和结束日期 参数！"
  exit 1
}

echo -e "${date_s_aa:=$(date +'%F %T')} 整体任务  开始 当前脚本进程ID为：$(pid)\n" &>> $log

sh $bin_abn/data_income.sh     $e_date $e_date &>> $log

sh $bin_abn/data_ods_new_s.sh  $s_date $e_date &>> $log

####################### 以上为 ods_new_s 层跑数，以下为依赖 ods_new_s 的跑数 #######################


sh $bin_abn/data_asset.sh      $s_date $e_date &>> $log

sh $bin_abn/data_dw_dm.sh      $s_date $e_date &>> $log

# 导出dm层数据文件，要在进件和dm层之后进行
sh $bin/data_export.sh                         &>> $log

sh $bin_abn/dm_operation.sh    $s_date $e_date &>> $log

sh $bin_abn/lx_asset_report.sh $s_date $e_date &>> $log

sh $bin_abn/lx_over_report.sh  $s_date $e_date &>> $log

# 删除日志文件
sh $bin/data_delete_file.sh                    &>> $log

echo -e "${date_e_aa:=$(date +'%F %T')} 整体任务  结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log

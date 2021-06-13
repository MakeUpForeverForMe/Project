#!/usr/bin/env bash

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

echo -e "${date_s_aa:=$(date +'%F %T')} 星云 整体任务  开始 当前脚本进程ID为：$(pid)\n" &>> $log

# sh $bin_abs/sqoop-abs.sh &>> $log &

wait_jobs

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dim_new_hql/dim.dim_encrypt_info.hql &>> $log &

sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $dim_new_hql/dim.project_info-his.hql &>> $log &
sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $dim_new_hql/dim.project_due_bill_no-his.hql &>> $log &
sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $dim_new_hql/dim.bag_info-his.hql &>> $log &
sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $dim_new_hql/dim.bag_due_bill_no-his.hql &>> $log &

sh $bin_abs/data_cloud-ods.sh $s_date $e_date &>> $log

sh $bin_abs/data_cloud-dw_dm-day.sh $s_date $e_date &>> $log

# 删除日志文件
sh $bin/data_delete_file.sh                      &>> $log

wait_jobs

echo -e "${date_e_aa:=$(date +'%F %T')} 星云 整体任务  结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log


$mail $pm_rd "星云 整体任务 $(pid)" "
  执行开始日期： $s_date
  执行结束日期： $e_date
  执行开始时间： $date_s_aa
  执行结束时间： $date_e_aa
  执行执行时长： $during_time
" &>> $log

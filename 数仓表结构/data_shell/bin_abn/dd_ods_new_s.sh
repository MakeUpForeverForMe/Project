#!/bin/bash -e

. ${data_ods_new_s_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh


s_date=$1
e_date=$2
repair_date=$3
base_file_name=$(basename "${BASH_SOURCE[0]}")

[[ -z $s_date || -z $e_date ]] && {
  echo "$(date +'%F %T') ${base_file_name} 请输入 开始和结束日期 参数！"
  exit 1
}

log=$log/${base_file_name}.${e_date}.log

echo -e "${date_s_aa:=$(date +'%F %T')}EMR 滴滴资产 ods_new_s  开始 当前脚本进程ID为：$(pid)\n" &>> $log


sh $data_manage -s ${repair_date} -e ${repair_date} -f $ods_new_s_hql/ods.repay_detail_ddhtgz.hql -i $param_dir/ods.param_ddgz.hql -a $rd &


sh $data_manage -s ${e_date} -e ${e_date} -f $ods_new_s_hql/ods.loan_lending.hql -i $param_dir/ods.param_ddgz.hql -a $rd &


sh $data_manage -s ${repair_date} -e ${repair_date} -f $ods_new_s_hql/ods.order_info.hql -i $param_dir/ods.param_ddgz.hql -a $rd &

wait_jobs


sh $data_manage -s ${s_date} -e ${e_date} -f $reload_hql/ods.reload_loan_info_iter.hql -i $param_dir/ods.param_ddgz.hql -k d_date="${repair_date}"  -a $rd &

sh $data_manage -s ${s_date} -e ${e_date} -f $reload_hql/ods.reload_repay_schedule_iter.hql -i $param_dir/ods.param_ddgz.hql -k d_date="${repair_date}"  -a $rd &

wait_jobs
sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods.loan_info.hql -i $param_dir/ods.param_ddgz.hql   -a $rd &

sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods.repay_schedule.hql -i $param_dir/ods.param_ddgz.hql -a $rd &


echo -e "${date_e_aa:=$(date +'%F %T')} EMR  滴滴资产 ods_new_s ods_new_s层 结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log

#{
#  sh $data_check_all ${s_date} ${e_date} ods_new_s &>> $log &
#} &

# dm 层


$mail $pm_rd '数据 4.0 EMR  滴滴资产 ods_new_s 执行结束' "
  执行开始日期： $s_date
  执行结束日期： $e_date
  执行开始时间： $date_s_aa
  执行结束时间： $date_e_aa
  执行执行时长： $during_time
" &>> $log

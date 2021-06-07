#!/bin/bash -e

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



echo -e "${date_s_aa:=$(date +'%F %T')} 百度资产 ods  开始 当前脚本进程ID为：$(pid)\n" &>> $log
# wait_jobs

sh $data_manage -s ${s_date} -e ${e_date} -f $ods_newcore_baidu_hql/ods.repay_detail.hql -i $param_dir/ods.param_baidu.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $ods_newcore_baidu_hql/ods.repay_detail.hql -i $param_dir/ods_cps.param_baidu.hql -a $rd &

wait_jobs

sh $data_manage -s ${e_date} -e ${e_date} -f $ods_newcore_baidu_hql/ods.loan_lending.hql -i $param_dir/ods.param_baidu.hql -a $rd &
sh $data_manage -s ${e_date} -e ${e_date} -f $ods_newcore_baidu_hql/ods.loan_lending.hql -i $param_dir/ods_cps.param_baidu.hql -a $rd &

wait_jobs

sh $data_manage -s ${e_date} -e ${e_date} -f $ods_newcore_baidu_hql/ods.order_info.hql -i $param_dir/ods.param_baidu.hql -a $rd &
sh $data_manage -s ${e_date} -e ${e_date} -f $ods_newcore_baidu_hql/ods.order_info.hql -i $param_dir/ods_cps.param_baidu.hql -a $rd &

wait_jobs


sh $data_manage -s ${s_date} -e ${e_date} -f $ods_newcore_baidu_hql/ods.loan_info.hql -i $param_dir/ods.param_baidu.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $ods_newcore_baidu_hql/ods.loan_info.hql -i $param_dir/ods_cps.param_baidu.hql -a $rd &


wait_jobs


sh $data_manage -s ${s_date} -e ${e_date} -f $ods_newcore_baidu_hql/ods.repay_schedule.hql -i $param_dir/ods.param_baidu.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $ods_newcore_baidu_hql/ods.repay_schedule.hql -i $param_dir/ods_cps.param_baidu.hql -a $rd &

wait_jobs

#nohup sh /root/data_shell/bin_abn/dm_operation_bd.sh  ${s_date}  ${e_date}  &>> /root/data_shell/log/dm_operation.sh.${e_date}.log  &

#wait_jobs

echo -e "${date_a_aa:=$(date +'%F %T')} 资产 ods ods层 结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_a_aa" "$date_s_aa")\n\n" &>> $log

#{
#  sh $data_check_all ${s_date} ${e_date} ods_new_s &>> $log &
#} &


wait_jobs



echo -e "${date_e_aa:=$(date +'%F %T')} 资产 ods  结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log

$mail $pm_rd '新核心百度医美emr ods 执行结束' "
  执行开始日期： $s_date
  执行结束日期： $e_date
  执行开始时间： $date_s_aa
  执行结束时间： $date_e_aa
  执行执行时长： $during_time
" &>> $log

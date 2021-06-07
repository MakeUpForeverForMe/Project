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


echo -e "${date_s_aa:=$(date +'%F %T')} 资产 ods_cloud ods 开始 当前脚本进程ID为：$(pid)\n" &>> $log

sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $ods_cloud_hql/ods.risk_control.hql &

sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $ods_cloud_hql/ods.customer_info.hql &

sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $ods_cloud_hql/ods.enterprise_info.hql &

sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $ods_cloud_hql/ods.linkman_info.hql &

sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $ods_cloud_hql/ods.guaranty_info.hql &

sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $ods_cloud_hql/ods.loan_lending.hql &


sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $ods_cloud_hql/ods.loan_info_inter.hql &

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $ods_cloud_hql/ods.repay_schedule_inter.hql &

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $ods_cloud_hql/ods.repay_detail.hql &

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $ods_cloud_hql/ods.order_info.hql &


wait_jobs

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods.loan_info.hql      -i $param_dir/ods.abs-param.hql &

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods.repay_schedule.hql -i $param_dir/ods.abs-param.hql &

wait_jobs

sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $ods_cloud_hql/ods.t_10_basic_asset_stage.hql &

sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $ods_cloud_hql/ods.t_07_actualrepayinfo.hql &

wait_jobs


echo -e "${date_e_aa:=$(date +'%F %T')} 资产 ods_cloud ods 结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log



$mail $pm_rd "ods_cloud ods $(pid)" "
  执行开始日期： $s_date
  执行结束日期： $e_date
  执行开始时间： $date_s_aa
  执行结束时间： $date_e_aa
  执行执行时长： $during_time
" &>> $log

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

parallel='-n 10'

echo -e "${date_s_aa:=$(date +'%F %T')} 资产 ods_cloud 开始 当前脚本进程ID为：$(pid)\n" &>> $log


sh $data_manage -s ${e_date} -e ${e_date} -f $ods_cloud_hql/ods.risk_control.hql -a $rd &

sh $data_manage -s ${e_date} -e ${e_date} -f $ods_cloud_hql/ods.customer_info.hql -a $rd &

sh $data_manage -s ${e_date} -e ${e_date} -f $ods_cloud_hql/ods.enterprise_info.hql -a $rd &

sh $data_manage -s ${e_date} -e ${e_date} -f $ods_cloud_hql/ods.linkman_info.hql -a $rd &

sh $data_manage -s ${e_date} -e ${e_date} -f $ods_cloud_hql/ods.guaranty_info.hql -a $rd &

sh $data_manage -s ${s_date} -e ${e_date} -f $ods_cloud_hql/ods.loan_lending.hql -a $rd ${parallel} &

sh $data_manage -s ${s_date} -e ${e_date} -f $ods_cloud_hql/ods.loan_info_inter.hql -a $rd ${parallel} &

sh $data_manage -s ${s_date} -e ${e_date} -f $ods_cloud_hql/ods.repay_schedule_inter.hql -a $rd ${parallel} &

sh $data_manage -s ${s_date} -e ${e_date} -f $ods_cloud_hql/ods.repay_detail.hql -a $rd ${parallel} &

sh $data_manage -s ${s_date} -e ${e_date} -f $ods_cloud_hql/ods.order_info.hql -a $rd ${parallel} &


wait_jobs

echo -e "${date_a_aa:=$(date +'%F %T')} 资产 ods_cloud ods层 结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_a_aa" "$date_s_aa")\n\n" &>> $log


# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_day.hql -a $rd &


# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_loan_info.hql -a $rd &


# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_repayment_record_day.hql -a $rd &


wait_jobs


echo -e "${date_b_aa:=$(date +'%F %T')} 资产 ods_cloud dm_eagle层 结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_b_aa" "$date_a_aa")\n\n" &>> $log

echo -e "${date_e_aa:=$(date +'%F %T')} 资产 ods_cloud 结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log


$mail $pm_rd '数据 4.0 资产 ods_cloud 执行结束' "
  执行开始日期： $s_date
  执行结束日期： $e_date
  执行开始时间： $date_s_aa
  执行结束时间： $date_e_aa
  执行执行时长： $during_time
" &>> $log

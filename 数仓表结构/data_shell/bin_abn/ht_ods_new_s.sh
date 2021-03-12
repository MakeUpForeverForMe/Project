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


echo -e "${date_s_aa:=$(date +'%F %T')} 汇通 ods_new_s  开始 当前脚本进程ID为：$(pid)\n" &>> $log
##修数的表
sh $data_manage -s ${repair_date} -e ${repair_date} -f $file_repaired/abn_repaired/ods.schedule_repay_order_info_ddht.hql -i $param_dir/ods_new_s.param_ht.hql -a $guochao
wait_jobs

sh $data_manage -s ${repair_date} -e ${repair_date} -f $ods_new_s_hql/ods_new_s.repay_detail_ddhtgz.hql -i $param_dir/ods_new_s.param_ht.hql -a $guochao &


sh $data_manage -s ${repair_date} -e ${repair_date} -f $ods_new_s_hql/ods_new_s.loan_lending.hql -i $param_dir/ods_new_s.param_ht.hql -a $guochao &


sh $data_manage -s ${repair_date} -e ${repair_date} -f $ods_new_s_hql/ods_new_s.order_info.hql -i $param_dir/ods_new_s.param_ht.hql -a $guochao &




sh $data_manage -s ${s_date} -e ${e_date} -f $reload_hql/ods_new_s.loan_info.hql_ht -i $param_dir/ods_new_s.param_ht.hql  -k d_date=$repair_date -a $guochao &

sh $data_manage -s ${s_date} -e ${e_date} -f $reload_hql/ods_new_s.repay_schedule.hql_ht -i $param_dir/ods_new_s.param_ht.hql -k d_date=$repair_date  -a $guochao &

wait_jobs

echo -e "${date_e_aa:=$(date +'%F %T')} 汇通 ods_new_s ods_new_s层 结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log



$mail $pm_rd '数据 4.0 汇通 ods_new_s 执行结束' "
  执行开始日期： $s_date
  执行结束日期： $e_date
  执行开始时间： $date_s_aa
  执行结束时间： $date_e_aa
  执行执行时长： $during_time
" &>> $log

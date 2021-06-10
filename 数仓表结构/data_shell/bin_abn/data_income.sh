#!/bin/bash -e

. ${data_income_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh


s_date=$1
e_date=$2
base_file_name=$(basename "${BASH_SOURCE[0]}")

[[ -z $s_date || -z $e_date ]] && {
  echo "$(date +'%F %T') ${base_file_name} 请输入 开始和结束日期 参数！"
  exit 1
}

log=$log/${base_file_name}.${e_date}.log


echo -e "${date_s_aa:=$(date +'%F %T')} 进件任务  开始 当前脚本进程ID为：$(pid)\n" &>> $log


# ods 层 及 dw 层

sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods.customer_info.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods.credit_apply.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dim_new_hql/dim.dim_encrypt_info.hql -a $rd &


wait_jobs

sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw.dw_credit_apply_stat_day.hql -i $param_dir/dw.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw.dw_credit_ret_msg_day.hql -i $param_dir/dw.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods.loan_apply.hql -a $rd &

# sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods_new_s.user_info.hql -a $rd &

wait_jobs


sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw.dw_credit_approval_stat_day.hql -i $param_dir/dw.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw.dw_loan_apply_stat_day.hql -i $param_dir/dw.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw.dw_loan_approval_stat_day.hql -i $param_dir/dw.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw.dw_loan_ret_msg_day.hql -i $param_dir/dw.param.hql -a $rd &

sh $data_manage -s ${e_date} -e ${e_date} -f $ods_new_s_hql/ods.user_label.hql -a $rd &

wait_jobs

# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_credit_loan_approval_amount_rate_day.hql -i $param_dir/dm_eagle.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_credit_loan_approval_amount_rate_day.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_credit_loan_approval_amount_sum_day.hql -i $param_dir/dm_eagle.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_credit_loan_approval_amount_sum_day.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_credit_loan_approval_rate_day.hql -i $param_dir/dm_eagle.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_credit_loan_approval_rate_day.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_ret_msg_day.hql -i $param_dir/dm_eagle.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_ret_msg_day.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &

sh $data_manage -s ${e_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.assets_distribution.hql -i $param_dir/dm_eagle.param.hql -a $rd &

wait_jobs

table_list=(
 # 进件
 dm_eagle.eagle_ret_msg_day-$dm_eagle_dm_eagle
 dm_eagle.eagle_credit_loan_approval_rate_day-$dm_eagle_dm_eagle
 dm_eagle.eagle_credit_loan_approval_amount_sum_day-$dm_eagle_dm_eagle
 dm_eagle.eagle_credit_loan_approval_amount_rate_day-$dm_eagle_dm_eagle

)

# 进件模块 dm_eagle 数据导出mysql
sh $bin/data_export.sh $s_date $e_date "${table_list[*]}" &>> $log

wait_jobs

echo -e "${date_e_aa:=$(date +'%F %T')} EMR 进件任务  结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log


$mail $pm_rd 'EMR 数据 4.0 进件任务 结束' "
  执行开始日期： $s_date
  执行结束日期： $e_date
  操作开始时间： $date_s_aa
  操作结束时间： $date_e_aa
  操作执行时长： $during_time
" &>> $log

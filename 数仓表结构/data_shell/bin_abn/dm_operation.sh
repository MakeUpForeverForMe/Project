#!/bin/bash -e

. ${dm_operation_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh


s_date=$1
e_date=$2
base_file_name=$(basename "${BASH_SOURCE[0]}")

[[ -z $s_date || -z $e_date ]] && {
  echo "$(date +'%F %T') ${base_file_name} 请输入 开始和结束日期 参数！"
  exit 1
}

log=$log/${base_file_name}.${e_date}.log


echo -e "${date_s:=$(date +'%F %T')} 任务执行  开始 当前脚本进程ID为：$(pid)\n" &>> $log

#运营平台快照日任务



sh $data_manage -s ${s_date} -e ${e_date} -f $dm_operation_hql/dm_eagle.operation_overdue_datail.hql -i $param_dir/dm_eagle.param_operation.hql                -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_operation_hql/dm_eagle.operation_product_management_report_agg.hql -i $param_dir/dm_eagle.param_operation.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_operation_hql/dm_eagle.operation_should_repay_day_agg.hql  -i $param_dir/dm_eagle.param_operation.hql         -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_operation_hql/dm_eagle.operation_overdue_base_detail.hql  -i $param_dir/dm_eagle.param_operation.hql         -a $rd &

wait_jobs

#运营平台取最新数据任务
sh $data_manage -s ${e_date} -e ${e_date} -f $dm_operation_hql/dm_eagle.operation_daily_repay_agg.hql -i $param_dir/dm_eagle.param_operation.hql               -a $rd &
sh $data_manage -s ${e_date} -e ${e_date} -f $dm_operation_hql/dm_eagle.operation_daily_repay_detail.hql -i $param_dir/dm_eagle.param_operation.hql            -a $rd &

wait_jobs

sh $data_manage -s ${e_date} -e ${e_date} -f $dm_operation_hql/dm_eagle.operation_lending_daily_agg.hql  -i $param_dir/dm_eagle.param_operation.hql            -a $rd &
sh $data_manage -s ${e_date} -e ${e_date} -f $dm_operation_hql/dm_eagle.operation_loan_ledger_detail.hql -i $param_dir/dm_eagle.param_operation.hql            -a $rd &

wait_jobs


sh $data_manage -s ${e_date} -e ${e_date} -f $dm_operation_hql/dm_eagle.operation_return_ticket_agg.hql  -i $param_dir/dm_eagle.param_operation.hql            -a $rd &

wait_jobs

sh $data_manage -s ${e_date} -e ${e_date} -f $dm_operation_hql/dm_eagle.operation_return_ticket_detail.hql -i $param_dir/dm_eagle.param_operation.hql          -a $rd &
sh $data_manage -s ${e_date} -e ${e_date} -f $dm_operation_hql/dm_eagle.operation_should_repay_detail.hql -i $param_dir/dm_eagle.param_operation.hql           -a $rd &

echo -e "${date_e:=$(date +'%F %T')} 任务执行  结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e" "$date_s")}\n\n" &>> $log

$mail $pm_rd 'EMR 数据 4.0 运营平台任务 结束' "
  执行开始日期： $s_date
  执行结束日期： $e_date
  操作开始时间： $date_s
  操作结束时间： $date_e
  操作执行时长： $during_time
" &>> $log

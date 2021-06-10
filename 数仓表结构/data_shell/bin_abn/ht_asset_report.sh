#!/bin/bash

#dim_static_overdue_bill表执行

. ${lx_over_report_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh


s_date=$1
e_date=$2
base_file_name=$(basename "${BASH_SOURCE[0]}")

[[ -z $s_date || -z $e_date ]] && {
  echo "$(date +'%F %T') ${base_file_name} 请输入 开始和结束日期 参数！"
  exit 1
}

log=$log/${base_file_name}.${e_date}.log




echo -e "${date_s:=$(date +'%F %T')} 任务执行 $base_file_name 开始 当前脚本进程ID为：$(pid)\n" &>> $log

sh $data_manage -s ${s_date} -e ${e_date} -f $asset_report_hql/dim.dim_static_overdue_bill.hql -i $param_dir/dim.param_ht.hql   -a $rd

sh $data_manage  -s ${s_date} -e ${e_date} -f $asset_report_hql/dm_eagle.report_ht_asset_project.hql -a $rd

sh $data_manage  -s ${s_date} -e ${e_date} -f $asset_report_hql/dm_eagle.report_ht_overdue_more9_assets_detail.hql -a $rd

sh $data_manage  -s ${s_date} -e ${e_date} -f $asset_report_hql/dm_eagle.report_ht_prepayment_assets_detail.hql  -a $rd

echo -e "${date_e:=$(date +'%F %T')} 任务执行 $base_file_name 结束 当前脚本进程ID为：$(pid)    用时：用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log


$mail $pm_rd 'EMR 数据 4.0 汇通服务报告执行结束' "
  执行开始日期： $s_date
  执行结束日期： $e_date
  执行开始时间： $date_s
  执行结束时间： $date_e
  执行执行时长： $during_time
" &>> $log
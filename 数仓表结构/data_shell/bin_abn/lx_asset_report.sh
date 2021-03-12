#!/bin/bash

# start_date=$1
# end_date=$2
# end_date=`date -d "+1 day $end_date" +%Y-%m-%d`

# while [[ $start_date != $end_date  ]]
# do
# 	echo "########$start_date#########"
#         sh /root/data_shell/yuheng.wang/report_lx_asset_report_accu_comp.sh $start_date
#         sh /root/data_shell/yuheng.wang/report_lx_asset_report_snapshot_comp.sh $start_date
#         sh /root/data_shell/yuheng.wang/report_lx_preSettle.sh $start_date
# 	start_date=`date -d "+1 day $start_date" +%Y-%m-%d`
# done


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


# sh /root/data_shell/yuheng.wang/report_lx_asset_report_accu_comp.sh $start_date
sh $data_manage -t -s ${s_date} -e ${e_date} -f $asset_report_hql/dm_eagle.report_dm_lx_asset_report_accu_comp.hql -a $rd

# sh /root/data_shell/yuheng.wang/report_lx_asset_report_snapshot_comp.sh $start_date
sh $data_manage -t -s ${s_date} -e ${e_date} -f $asset_report_hql/dm_eagle.report_dm_lx_asset_report_snapshot_comp.hql -a $rd

# sh /root/data_shell/yuheng.wang/report_lx_preSettle.sh $start_date
sh $data_manage -t -s ${s_date} -e ${e_date} -f $asset_report_hql/dm_eagle.report_lx_preSettle.hql -a $rd


echo -e "${date_e:=$(date +'%F %T')} 任务执行 $base_file_name 结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_e" "$date_s")\n\n" &>> $log

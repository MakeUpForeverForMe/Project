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



# ST9=$1
# if [ ! $ST9 ]
# then
#         ST9=`date -d "1 day ago" +%Y-%m-%d`
# fi;
# echo "当前批次时间:$date"


echo -e "${date_s:=$(date +'%F %T')} 任务执行 $base_file_name 开始 当前脚本进程ID为：$(pid)\n" &>> $log

# beeline -u jdbc:hive2://node47:10000 -n hive -f /root/data_shell/data_report/dim_new.dim_static_overdue_bill.hql --hivevar ST9=${ST9}  >> /root/data_shell/data_report/log/dw_report_cal_day$(date +%F).log
sh $data_manage -s ${s_date} -e ${e_date} -f $asset_report_hql/dim_new.dim_static_overdue_bill.hql -i  $param_dir/dim_new.param_lx.hql -a $guochao

# beeline -u jdbc:hive2://node47:10000 -n hive -f /root/data_shell/data_report/dw_new.dw_report_cal_day.hql --hivevar ST9=${ST9} >> /root/data_shell/data_report/log/$(date +%F).log
sh $data_manage -s ${s_date} -e ${e_date} -f $asset_report_hql/dw_new.dw_report_cal_day.hql -a $guochao

# impala-shell -q "refresh dw_new.dw_report_cal_day;!exit"

# sh /root/data_shell/data_report/dm_eagle/dm_eagle.dm_lx_asset_statistics.hql ${ST9}  >> /root/data_shell/data_report/log/dm_lx_asset_statistics$(date +%F).log
sh $data_manage -t -s ${s_date} -e ${e_date} -f $asset_report_hql/dm_eagle.report_dm_lx_asset_statistics.hql -a $guochao

# sh /root/data_shell/data_report/dm_eagle/dm_eagle.report_dm_lx_asset_summary_statistics.hql ${ST9} >> /root/data_shell/data_report/log/dm_lx_asset_statistics$(date +%F).log
sh $data_manage -t -s ${s_date} -e ${e_date} -f $asset_report_hql/dm_eagle.report_dm_lx_asset_summary_statistics.hql -a $guochao

echo -e "${date_e:=$(date +'%F %T')} 任务执行 $base_file_name 结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_e" "$date_s")\n\n" &>> $log

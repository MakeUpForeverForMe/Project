#!/bin/bash -e

. ${data_ods_new_s_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh

base_file_name=$(basename "${BASH_SOURCE[0]}")
log=$log/${base_file_name}.${e_date}.log

echo -e "${date_s_aa:=$(date +'%F %T')} 数据导出  开始 当前脚本进程ID为：$(pid)" &>> $log


#tables['dm_eagle.abs_asset_information_bag']=$dm_eagle_uabs_core
#tables['dm_eagle.abs_asset_information_project']=$dm_eagle_uabs_core
#tables['dm_eagle.abs_asset_information_bag_snapshot']=$dm_eagle_uabs_core
#tables['dm_eagle.abs_asset_information_cash_flow_bag_snapshot']=$dm_eagle_uabs_core
tables['dm_eagle.abs_asset_information_cash_flow_bag_day']=$dm_eagle_uabs_core

for db_tb in ${!tables[@]};do
  {
    export_file="$file_export/${db_tb}.tsv"
    mysql="$($mysql_cmd ${tables[$db_tb]})"

    export_file_from_hive "select * from ${db_tb};" &>> $log
    import_file_to_mysql &>> $log
  } &
  p_opera 5 &>> $log
  # break
done

wait_jobs

echo -e "${date_e_aa:=$(date +'%F %T')} 数据导出  结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log

# $mail $pm_rd '数据 4.0 abs 数据导出 结束' "
#   执行导出日期： $e_date
#   操作开始时间： $date_s_aa
#   操作结束时间： $date_e_aa
#   操作执行时长： $during_time
# " &>> $log

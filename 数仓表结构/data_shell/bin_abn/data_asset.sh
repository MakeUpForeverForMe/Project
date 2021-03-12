#!/bin/bash -e

. ${data_asset_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh


s_date=$1
e_date=$2
base_file_name=$(basename "${BASH_SOURCE[0]}")

[[ -z $s_date || -z $e_date ]] && {
  echo "$(date +'%F %T') ${base_file_name} 请输入 开始和结束日期 参数！"
  exit 1
}

log=$log/${base_file_name}.${e_date}.log


echo -e "${date_s_aa:=$(date +'%F %T')} 资金页面资产任务  开始 当前脚本进程ID为：$(pid)\n" &>> $log

# dw 层

sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw_new_cps.dw_asset_info_day.hql -i $param_dir/dw_new_cps.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw_new.dw_asset_info_day.hql -i $param_dir/dw_new.param.hql -a $rd &


wait_jobs

sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_asset_change_t1.hql  -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_asset_change_comp_t1.hql -i $param_dir/dm_eagle_cps.param_virtual.hql -a $rd &

wait_jobs

echo -e "${date_e_aa:=$(date +'%F %T')} 资金页面资产任务  结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_e_aa" "$date_s_aa")\n" &>> $log

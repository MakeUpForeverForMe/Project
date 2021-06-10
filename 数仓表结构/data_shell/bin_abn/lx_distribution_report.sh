#!/bin/bash

. ${lx_over_report_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh

s_date=$1
e_date=$2
p_type=$3
base_file_name=$(basename "${BASH_SOURCE[0]}")

[[ -z $s_date || -z $e_date ]] && {
  echo "$(date +'%F %T') ${base_file_name} 请输入 开始和结束日期 参数！"
  exit 1
}
[[ -z p_type ]] && {
  echo "$(date +'%F %T') ${base_file_name} 请输入 项目类型 参数！"
  exit 1
}

#if [ ${p_type} = 'lx' ];then
#  project_id='WS0006200001'
#  product_id="'001801','001802'"
#elif [ ${p_type} = 'lx2' ];then
#  project_id='WS0006200002'
#  product_id="'002001','002002','002003','002004','002005','002006','002007'"
#elif [ ${p_type} = 'lxzt' ];then
#  project_id='WS0009200001'
#  product_id="'001901','001902','001903','001904','001905','001906','001907'"
#else
#  echo "类型错误"
#  exit 1
#fi



log=$log/${base_file_name}.${e_date}.log

echo -e "${date_s:=$(date +'%F %T')} 任务执行 $base_file_name 开始 当前脚本进程ID为：$(pid)\n" &>> $log



sh $data_manage -t -s ${s_date} -e ${e_date} -f $asset_report_hql/dm_eagle.report_dm_lx_asset_distribution.hql -i $param_dir/dm_eagle.param_report_${p_type}.hql -a $rd

sh $data_manage -t -s ${s_date} -e ${e_date} -f $asset_report_hql/dm_eagle.report_dm_lx_asset_refund_distribution.hql -i $param_dir/dm_eagle.param_report_${p_type}.hql -a $rd

sh $data_manage -t -s ${s_date} -e ${e_date} -f $asset_report_hql/dm_eagle.report_dm_lx_comp_overdue_distribution.hql -i $param_dir/dm_eagle.param_report_${p_type}.hql -a $rd

sh $data_manage -t -s ${s_date} -e ${e_date} -f $asset_report_hql/dm_eagle.report_dm_lx_refund_bill_detail.hql -i $param_dir/dm_eagle.param_report_${p_type}.hql -a $rd


echo -e "${date_e:=$(date +'%F %T')} 任务执行 $base_file_name 结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_e" "$date_s")\n\n" &>> $log

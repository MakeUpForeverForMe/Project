#!/usr/bin/env bash

. ${data_ods_new_s_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/conf_env/env.sh
. $lib/function.sh


# s_date=2017-06-01
# e_date=2021-05-10

s_date=2021-05-09
e_date=2020-01-01

p_num=20


echo -e "${date_s_aa:=$(date +'%F %T')} 星云 手动跑数 开始 当前脚本进程ID为：$(pid)\n"


sh_list=(
  'sh $data_manage -a $ximing -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_cash_flow_bag_day.hql -i $param_dir/dm_eagle.abs-param.hql'

  'sh $data_manage -a $ximing -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_bag.hql -i $param_dir/dm_eagle.abs-param.hql'
  'sh $data_manage -a $ximing -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_project.hql -i $param_dir/dm_eagle.abs-param.hql'

  'sh $data_manage -a $ximing -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_day.hql -i $param_dir/dm_eagle.abs-param.hql'
  'sh $data_manage -a $ximing -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_bag_day.hql -i $param_dir/dm_eagle.abs-param.hql'

  'sh $data_manage -a $ximing -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_overdue_rate_day.hql -i $param_dir/dm_eagle.abs-param.hql'
  'sh $data_manage -a $ximing -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_overdue_rate_details_day.hql -i $param_dir/dm_eagle.abs-param.hql'

  'sh $data_manage -a $ximing -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_early_payment_asset_statistic.hql -i $param_dir/dm_eagle.abs-param.hql'
  'sh $data_manage -a $ximing -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_early_payment_asset_details.hql -i $param_dir/dm_eagle.abs-param.hql'
)


for cmd in "${sh_list[@]}"; do
  # for (( s_date = $(date -d "$s_date" +%Y%m%d); s_date <= $(date -d "$e_date" +%Y%m%d); s_date = $(date -d "+1 day $s_date" +%Y%m%d) )); do
  for (( s_date = $(date -d "$s_date" +%Y%m%d); s_date >= $(date -d "$e_date" +%Y%m%d); s_date = $(date -d "-1 day $s_date" +%Y%m%d) )); do
    s_date=$(date -d "$s_date" +%F)
    eval $cmd &
    p_opera $p_num
  done
done

wait_jobs

echo -e "${date_b_aa:=$(date +'%F %T')} 星云 手动跑数 结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_b_aa" "$date_a_aa")\n\n"

echo -e "${date_c_aa:=$(date +'%F %T')} 星云 手动跑数 数据导出上传  开始 当前脚本进程ID为：$(pid)"

table_list=(
  dm_eagle.abs_asset_information_cash_flow_bag_day-$dm_eagle_uabs_core

  dm_eagle.abs_asset_information_bag-$dm_eagle_uabs_core
  dm_eagle.abs_asset_information_project-$dm_eagle_uabs_core

  dm_eagle.abs_asset_distribution_day-$dm_eagle_uabs_core
  dm_eagle.abs_asset_distribution_bag_day-$dm_eagle_uabs_core

  dm_eagle.abs_overdue_rate_day-$dm_eagle_uabs_core
)

sh $bin/data_export.sh $e_date $s_date "${table_list[*]}"

echo -e "${date_d_aa:=$(date +'%F %T')} 星云 手动跑数 数据导出上传  结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_d_aa" "$date_c_aa")\n\n"

echo -e "${date_e_aa:=$(date +'%F %T')} 星云 手动跑数 结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n"


$mail $pm_rd '星云 手动跑数' "
  执行开始日期： $s_date
  执行结束日期： $e_date
  执行开始时间： $date_s_aa
  执行结束时间： $date_e_aa
  执行执行时长： $during_time
"

#!/usr/bin/env bash

. ${data_ods_new_s_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/conf_env/env.sh
. $lib/function.sh


s_date=2021-01-01
e_date=$([[ $(date +%Y%m%d%H) -le $(date +%Y%m%d19) ]] && echo $(date -d '-2 day' +%F) || echo $(date -d '-1 day' +%F))

s_date="$(date -d "${1:-2021-05-12}" +%F)"
e_date="$(date -d "${2:-2020-01-01}" +%F)"

p_num=15


echo -e "${date_s_aa:=$(date +'%F %T')} 星云 手动跑数 开始 当前脚本进程ID为：$(pid)\n"


sh_list=(
  'sh $data_manage -a $ximing -s ${execute_date} -e ${execute_date} -f $dw_new_hql/dw.abs_due_info_day.hql -i $param_dir/dw.abs-param.hql'

  'sh $data_manage -a $ximing -s ${execute_date} -e ${execute_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_bag.hql -i $param_dir/dm_eagle.abs-param.hql'
  'sh $data_manage -a $ximing -s ${execute_date} -e ${execute_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_project.hql -i $param_dir/dm_eagle.abs-param.hql'

  'sh $data_manage -a $ximing -s ${execute_date} -e ${execute_date} -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_day.hql -i $param_dir/dm_eagle.abs-param.hql'
  'sh $data_manage -a $ximing -s ${execute_date} -e ${execute_date} -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_bag_day.hql -i $param_dir/dm_eagle.abs-param.hql'

  # 'sh $data_manage -a $ximing -s ${execute_date} -e ${execute_date} -f $dm_eagle_hql/dm_eagle.abs_overdue_rate_day.hql -i $param_dir/dm_eagle.abs-param.hql'
  # 'sh $data_manage -a $ximing -s ${execute_date} -e ${execute_date} -f $dm_eagle_hql/dm_eagle.abs_overdue_rate_details_day.hql -i $param_dir/dm_eagle.abs-param.hql'

  # 'sh $data_manage -a $ximing -s ${execute_date} -e ${execute_date} -f $dm_eagle_hql/dm_eagle.abs_early_payment_asset_statistic.hql -i $param_dir/dm_eagle.abs-param.hql'
  # 'sh $data_manage -a $ximing -s ${execute_date} -e ${execute_date} -f $dm_eagle_hql/dm_eagle.abs_early_payment_asset_details.hql -i $param_dir/dm_eagle.abs-param.hql'
)


# for (( e_month = $(date -d "$s_date" +%Y%m); e_month >= $(date -d "$e_date" +%Y%m); e_month = $(date -d "-1 month ${e_month}01" +%Y%m) )); do
#   s_day=$([[ $(date -d "$s_date" +%Y%m) = $e_month ]] && echo $(date -d "$s_date" +%Y%m%d) || echo $(date -d "$(date -d "${e_month}01 +1 month" +%Y%m%d) -1 day" +%Y%m%d))
#   e_day=$([[ $(date -d "$e_date" +%Y%m) = $e_month ]] && echo $(date -d "$e_date" +%Y%m%d) || echo $(date -d "${e_month}01" +%Y%m%d))

#   for cmd in "${sh_list[@]}"; do
#     for (( q_date = $(date -d "$s_day" +%Y%m%d); q_date >= $(date -d "$e_day" +%Y%m%d); q_date = $(date -d "-1 day ${q_date}" +%Y%m%d) )); do
#       execute_date=$(date -d "$q_date" +%F)
#       eval $cmd &
#       p_opera $p_num
#     done
#   done
# done


for cmd in "${sh_list[@]}"; do
  # for (( s_date = $(date -d "$s_date" +%Y%m%d); s_date <= $(date -d "$e_date" +%Y%m%d); s_date = $(date -d "+1 day $s_date" +%Y%m%d) )); do
  for (( execute_date = $(date -d "$s_date" +%Y%m%d); execute_date >= $(date -d "$e_date" +%Y%m%d); execute_date = $(date -d "-1 day $execute_date" +%Y%m%d) )); do
    execute_date=$(date -d "$execute_date" +%F)
    eval $cmd &
    p_opera $p_num
  done
done

wait_jobs

echo -e "${date_b_aa:=$(date +'%F %T')} 星云 手动跑数 结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_b_aa" "$date_s_aa")\n\n"

echo -e "${date_c_aa:=$(date +'%F %T')} 星云 手动跑数 数据导出上传  开始 当前脚本进程ID为：$(pid)"

table_list=(
  dm_eagle.abs_asset_information_bag-$dm_eagle_uabs_core
  dm_eagle.abs_asset_information_project-$dm_eagle_uabs_core

  dm_eagle.abs_asset_distribution_day-$dm_eagle_uabs_core
  dm_eagle.abs_asset_distribution_bag_day-$dm_eagle_uabs_core

  dm_eagle.abs_overdue_rate_day-$dm_eagle_uabs_core
)

sh $bin/data_export.sh $e_date $s_date "${table_list[*]}"

echo -e "${date_d_aa:=$(date +'%F %T')} 星云 手动跑数 数据导出上传  结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_d_aa" "$date_c_aa")\n\n"

echo -e "${date_e_aa:=$(date +'%F %T')} 星云 手动跑数 结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n"


$mail $pm_rd "星云 手动跑数 $(pid)" "
  执行开始日期： $s_date
  执行结束日期： $e_date
  执行开始时间： $date_s_aa
  执行结束时间： $date_e_aa
  执行执行时长： $during_time
"

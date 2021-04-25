#!/bin/bash -e

. ${data_ods_new_s_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh


s_date=$1
e_date=$2
base_file_name=$(basename "${BASH_SOURCE[0]}")

[[ -z $s_date || -z $e_date ]] && {
  echo "$(date +'%F %T') ${base_file_name} 请输入 开始和结束日期 参数！"
  exit 1
}

log=$log/${base_file_name}.${e_date}.log


echo -e "${date_s_aa:=$(date +'%F %T')} 资产 ods_cloud 开始 当前脚本进程ID为：$(pid)\n" &>> $log

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $dim_new_hql/dim.dim_encrypt_info.hql &

sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $ods_cloud_hql/ods.risk_control.hql &

sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $ods_cloud_hql/ods.customer_info.hql &

sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $ods_cloud_hql/ods.enterprise_info.hql &

sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $ods_cloud_hql/ods.linkman_info.hql &

sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $ods_cloud_hql/ods.guaranty_info.hql &


sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $ods_cloud_hql/ods.loan_lending.hql &

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $ods_cloud_hql/ods.loan_info_inter.hql &

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $ods_cloud_hql/ods.repay_schedule_inter.hql &

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $ods_cloud_hql/ods.repay_detail.hql &

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $ods_cloud_hql/ods.order_info.hql &


wait_jobs

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods.loan_info.hql      -i $param_dir/ods.param-abs.hql &

sh $data_manage -a $rd -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods.repay_schedule.hql -i $param_dir/ods.param-abs.hql &

wait_jobs

sh $data_manage -a $rd -s ${e_date} -e ${e_date} -f $ods_cloud_hql/ods.t_10_basic_asset_stage.hql &

wait_jobs

echo -e "${date_a_aa:=$(date +'%F %T')} 资产 ods_cloud ods层 结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_a_aa" "$date_s_aa")\n\n" &>> $log


sh $data_manage  -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_project.hql &
sh $data_manage  -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_bag.hql -k bag_id='' &
sh $data_manage  -a $rd -s ${e_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_bag_snapshot.hql -k bag_id='' &

sh $data_manage  -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_day.hql &
sh $data_manage  -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_bag_day.hql &
sh $data_manage  -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_bag_snapshot_day.hql &

sh $data_manage  -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_cash_flow_bag_day.hql &
sh $data_manage  -a $rd -s ${e_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_cash_flow_bag_snapshot.hql -k bag_id='' &

sh $data_manage  -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_overdue_rate_day.hql -k bag_id='' &
sh $data_manage  -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_overdue_rate_details_day.hql -k bag_id='' &

# sh $data_manage  -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_early_payment_asset_details.hql   &
# sh $data_manage  -a $rd -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_early_payment_asset_statistic.hql &

wait_jobs


echo -e "${date_b_aa:=$(date +'%F %T')} 资产 ods_cloud dm_eagle层 结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_b_aa" "$date_a_aa")\n\n" &>> $log

echo -e "${date_c_aa:=$(date +'%F %T')} 资产 ods_cloud 数据导出  开始 当前脚本进程ID为：$(pid)" &>> $log



tables['dm_eagle.abs_asset_distribution_day']=$dm_eagle_uabs_core
tables['dm_eagle.abs_asset_distribution_bag_day']=$dm_eagle_uabs_core
tables['dm_eagle.abs_asset_distribution_bag_snapshot_day']=$dm_eagle_uabs_core

tables['dm_eagle.abs_asset_information_bag']=$dm_eagle_uabs_core
tables['dm_eagle.abs_asset_information_project']=$dm_eagle_uabs_core
tables['dm_eagle.abs_asset_information_bag_snapshot']=$dm_eagle_uabs_core

tables['dm_eagle.abs_asset_information_cash_flow_bag_snapshot']=$dm_eagle_uabs_core


tables['dm_eagle.abs_overdue_rate_day']=$dm_eagle_uabs_core
tables['dm_eagle.abs_asset_information_cash_flow_bag_day']=$dm_eagle_uabs_core


s_date=2017-06-01
e_date=$(date +%F)

# s_date=2021-04-14
# e_date=2021-04-14

p_num=30

p_operas=(
  dm_eagle.abs_overdue_rate_day
  dm_eagle.abs_asset_distribution_day
  dm_eagle.abs_asset_distribution_bag_day

  dm_eagle.abs_asset_information_bag
  dm_eagle.abs_asset_information_project

  dm_eagle.abs_asset_information_cash_flow_bag_day
)

for db_tb in ${!tables[@]};do
    hql=()
    current_date=$(date +%Y%m%d)

    if [[ "${db_tb}" = 'dm_eagle.abs_asset_information_cash_flow_bag_day' ]]; then
      hql[$current_date]="
        select * from dm_eagle.abs_asset_information_cash_flow_bag_day
        where biz_date = (
          select max(biz_date) from dm_eagle.abs_asset_information_cash_flow_bag_day
          where biz_date between to_date(date_sub(current_timestamp(),3)) and to_date(date_sub(current_timestamp(),1))
        );
      "
    elif [[ "${p_operas[@]}" =~ "${db_tb}" ]]; then
      for (( excute_date = $(date -d "${s_date}" +%Y%m%d); excute_date <= $(date -d "${e_date}" +%Y%m%d); excute_date = $(date -d "1 day ${excute_date}" +%Y%m%d) )); do
        hql[$excute_date]="select * from ${db_tb} where biz_date = '$(date -d "${excute_date}" +%F)';"
      done
    else
      hql[$current_date]="select * from ${db_tb};"
    fi

    for hql_key in "${!hql[@]}"; do
      {
        export_file="$file_export/${db_tb}.$hql_key.tsv"

        echo "$hql_key" "$export_file" "${hql[$hql_key]}" &>> $log

        export_file_from_hive "${hql[$hql_key]}" &>> $log
        # sleep 2
      } &
      p_opera $p_num &>> $log
    done
done

wait_jobs

echo -e "${date_d_aa:=$(date +'%F %T')} 资产 ods_cloud 数据导出 结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_d_aa" "$date_c_aa")\n\n" &>> $log

for db_tb in ${!tables[@]};do

  all_file="$file_export/${db_tb}.*.tsv"
  export_file="$file_export/${db_tb}.tsv"
  mysql="$($mysql_cmd ${tables[$db_tb]})"

  # echo $all_file

  cat $all_file > $export_file 2>> $log
  rm -f $all_file &>> $log

  import_file_to_mysql &>> $log
  p_opera $p_num &>> $log
done

wait_jobs

echo -e "${date_f_aa:=$(date +'%F %T')} 资产 ods_cloud 数据上传到 MySQL 结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_f_aa" "$date_d_aa")\n\n" &>> $log


echo -e "${date_e_aa:=$(date +'%F %T')} 资产 ods_cloud 结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log



$mail $pm_rd '数据 4.0 资产 ods_cloud 执行结束' "
  执行开始日期： $s_date
  执行结束日期： $e_date
  执行开始时间： $date_s_aa
  执行结束时间： $date_e_aa
  执行执行时长： $during_time
" &>> $log

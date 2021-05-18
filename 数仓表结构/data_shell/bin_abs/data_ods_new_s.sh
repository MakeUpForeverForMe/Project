#!/bin/bash -e

. ${data_ods_new_s_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh


s_date=$1
e_date=$2

base_file_name=$(basename "${BASH_SOURCE[0]}")
batch_date=`date -d '-1 day' +%Y-%m-%d`

[[ -z $s_date || -z $e_date ]] && {
  echo "$(date +'%F %T') ${base_file_name} 请输入 开始和结束日期 参数！"
  exit 1
}

log=$log/${base_file_name}.${e_date}.log
yuheng=$wy

echo -e "${date_s_aa:=$(date +'%F %T')} abs资产 ods_new_s  开始\n" &>> $log

# sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_cloud_hql/ods_new_s.repay_detail.hql -i $param_dir/ods_new_s.param_lx.hql -a $yuheng &

# sh $data_manage -s ${e_date} -e ${e_date} -f $ods_new_s_cloud_hql/ods_new_s.loan_lending.hql -i $param_dir/ods_new_s.param_lx.hql -a $yuheng &

# sh $data_manage -s ${e_date} -e ${e_date} -f $ods_new_s_cloud_hql/ods_new_s.order_info.hql -i $param_dir/ods_new_s.param_lx.hql -a $yuheng &
# sh $data_manage -s ${e_date} -e ${e_date} -f $ods_new_s_cloud_hql/ods_new_s.customer_info.hql -i $param_dir/ods_new_s.param_lx.hql -a $yuheng &

wait_jobs

# sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_cloud_hql/ods_new_s.loan_info.hql -i $param_dir/ods_new_s.param_lx.hql -a $yuheng &
# sh $data_manage -s ${batch_date} -e ${batch_date} -f $ods_new_s_cloud_hql/ods_new_s.repay_schedule_inter.hql -i $param_dir/ods_new_s.param_lx.hql -a $yuheng &

wait_jobs
# sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_cloud_hql/ods_new_s.t_10_basic_asset_stage.hql -i $param_dir/ods_new_s.param_lx.hql -a $yuheng &

wait_jobs


sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_bag_day.hql -a $yuheng &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_bag_snapshot_day.hql -a $yuheng &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_day.hql -a $yuheng &

# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_cash_flow_bag_day.hql -a $yuheng &
# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_cash_flow_bag_snapshot.hql -k bag_id='' -a $yuheng &

sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_project.hql -a $yuheng &

wait_jobs

sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_bag.hql -k bag_id='' -k snapshot='' -a $yuheng &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_bag.hql -k bag_id='' -k snapshot='_snapshot' -a $yuheng &

sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_early_payment_asset_details.hql -a $yuheng &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_early_payment_asset_statistic.hql -a $yuheng &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_overdue_rate_day.hql -k bag_id='' -a $yuheng &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_overdue_rate_details_day.hql -k bag_id='' -a $yuheng &

wait_jobs

echo -e "${date_e_aa:=$(date +'%F %T')} abs资产 ods_new_s  结束    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log

$mail $pm_rd '数据 4.0 abs资产 ods_new_s 执行结束' "
  执行开始日期： $s_date
  执行结束日期： $e_date
  执行开始时间： $date_s_aa
  执行结束时间： $date_e_aa
  执行执行时长： $during_time
" &>> $log

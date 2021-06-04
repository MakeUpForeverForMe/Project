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

echo -e "${date_s_aa:=$(date +'%F %T')} Emr 资产 ods_new_s  开始 当前脚本进程ID为：$(pid)\n" &>> $log

# sh $data_manage -s ${s_date} -e ${e_date} -f $file_repaired/abn_repaired/ods.schedule_repay_order_info_ddht.hql -a $rd &

# wait_jobs

sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods.repay_detail_lx.hql -i $param_dir/ods.param_lx.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods.repay_detail_lx.hql -i $param_dir/ods_cps.param_lx.hql -a $rd &


# sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods_new_s.repay_detail_ddhtgz.hql -a $rd &


sh $data_manage -s ${e_date} -e ${e_date} -f $ods_new_s_hql/ods.loan_lending.hql -i $param_dir/ods.param_lx.hql -a $rd &
sh $data_manage -s ${e_date} -e ${e_date} -f $ods_new_s_hql/ods.loan_lending.hql -i $param_dir/ods_cps.param_lx.hql -a $rd &


sh $data_manage -s ${e_date} -e ${e_date} -f $ods_new_s_hql/ods.order_info.hql -i $param_dir/ods.param_lx.hql -a $rd &
sh $data_manage -s ${e_date} -e ${e_date} -f $ods_new_s_hql/ods.order_info.hql -i $param_dir/ods_cps.param_lx.hql -a $rd &

wait_jobs


sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods.loan_info_inter.hql -i $param_dir/ods.param_lx.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods.loan_info_inter.hql -i $param_dir/ods_cps.param_lx.hql -a $rd &


wait_jobs


sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods.repay_schedule_inter.hql -i $param_dir/ods.param_lx.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods.repay_schedule_inter.hql -i $param_dir/ods_cps.param_lx.hql -a $rd &

wait_jobs

sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods.loan_info.hql -i $param_dir/ods.param_lx.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods.loan_info.hql -i $param_dir/ods_cps.param_lx.hql -a $rd &
wait_jobs
sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods.repay_schedule.hql -i $param_dir/ods.param_lx.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $ods_new_s_hql/ods.repay_schedule.hql -i $param_dir/ods_cps.param_lx.hql -a $rd &

wait_jobs

sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_cash_flow_bag_day-lx.hql -i $param_dir/dm_eagle.abs-param.hql -a $rd &
wait_jobs
table_list=(

  dm_eagle.abs_asset_information_cash_flow_bag_day-$dm_eagle_uabs_core
)

sh $bin/data_export.sh $e_date $s_date "${table_list[*]}"



echo -e "${date_a_aa:=$(date +'%F %T')} 资产 EMR ods_new_s ods_new_s层 结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_a_aa" "$date_s_aa")\n\n" &>> $log

{
  sh $data_check_all ${s_date} ${e_date} ods       &>> $log &
  sh $data_check_all ${s_date} ${e_date} ods_stage &>> $log &
} &

# dm 层

#sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_repay_detail.hql -i $param_dir/dm_eagle.param.hql -a $rd &
#sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_repay_detail.hql -i $param_dir/dm_eagle_cps.param.hql -a $rd &


#sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_order_info.hql -i $param_dir/dm_eagle.param.hql -a $rd &
#sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_order_info.hql -i $param_dir/dm_eagle_cps.param.hql -a $rd &



wait_jobs


 ##sh $data_manage -s ${s_date} -e ${e_date} -f $hql/ods.repay_schedule.hql -i $param_dir/ods_new_s.param_ddhtgz.hql -a $rd &
 ##sh $data_manage -s ${s_date} -e ${e_date} -f $hql/ods.loan_info.hql -i $param_dir/ods_new_s.param_ddhtgz.hql -a $rd &


wait_jobs



#sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_repay_schedule.hql -i $param_dir/dm_eagle.param.hql -a $rd &
#sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_repay_schedule.hql -i $param_dir/dm_eagle_cps.param.hql -a $rd &
#
#sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_loan_info.hql -i $param_dir/dm_eagle.param.hql -a $rd &
#sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_loan_info.hql -i $param_dir/dm_eagle_cps.param.hql -a $rd &
#
#sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_repayment_record_day.hql -i $param_dir/dm_eagle.param.hql -a $rd &
#sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_repayment_record_day.hql -i $param_dir/dm_eagle_cps.param.hql -a $rd &



wait_jobs




echo -e "${date_e_aa:=$(date +'%F %T')} EMR 资产 ods_new_s  结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log

$mail $pm_rd 'EMR 数据 4.0 资产 ods_new_s 执行结束' "
  执行开始日期： $s_date
  执行结束日期： $e_date
  执行开始时间： $date_s_aa
  执行结束时间： $date_e_aa
  执行执行时长： $during_time
" &>> $log

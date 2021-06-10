#!/bin/bash -e

. ${dw_dm_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh


s_date=$1
e_date=$2
base_file_name=$(basename "${BASH_SOURCE[0]}")

[[ -z $s_date || -z $e_date ]] && {
  echo "$(date +'%F %T') ${base_file_name} 请输入 开始和结束日期 参数！"
  exit 1
}

log=$log/${base_file_name}.${e_date}.log


echo -e "${date_s_aa:=$(date +'%F %T')} 资产dw_dm  开始 当前脚本进程ID为：$(pid)\n" &>> $log


sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw.dw_loan_base_stat_loan_num_day.hql -i $param_dir/dw.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw.dw_loan_base_stat_loan_num_day.hql -i $param_dir/dw_cps.param.hql -a $rd &

sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw.dw_loan_base_stat_overdue_num_day.hql -i $param_dir/dw.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw.dw_loan_base_stat_overdue_num_day.hql -i $param_dir/dw_cps.param.hql -a $rd &

wait_jobs

sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw.dw_loan_base_stat_repay_detail_day.hql -i $param_dir/dw.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw.dw_loan_base_stat_repay_detail_day.hql -i $param_dir/dw_cps.param.hql -a $rd &

sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw.dw_loan_base_stat_should_repay_day.hql -i $param_dir/dw.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw.dw_loan_base_stat_should_repay_day.hql -i $param_dir/dw_cps.param.hql -a $rd &

wait_jobs 

sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw.dw_should_repay_summary.hql -i $param_dir/dw.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw.dw_should_repay_summary.hql -i $param_dir/dw_cps.param.hql -a $rd &

sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw.dw_user_information_stat_d.hql -i $param_dir/dw.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw.dw_user_information_stat_d.hql -i $param_dir/dw_cps.param.hql -a $rd &


wait_jobs

echo -e "${date_e_aa:=$(date +'%F %T')} 资产dw_dm  dw结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_e_aa" "$date_s_aa")\n\n" &>> $log

{
  sh $data_check_all ${s_date} ${e_date} dw_new &>> $log &
} &

# dm 层

sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_should_repay_repaid_amount_day.hql -i $param_dir/dm_eagle.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_should_repay_repaid_amount_day.hql -i $param_dir/dm_eagle_cps.param.hql -a $rd &


sh $data_manage -s ${e_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_title_info_create.hql -a $rd &

wait_jobs

# sh $data_manage -s ${e_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_title_info.hql -i $param_dir/dm_eagle.param.hql -a $rd &
# sh $data_manage -s ${e_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_title_info.hql -i $param_dir/dm_eagle_cps.param.hql -a $rd &
sh $data_manage -s ${e_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_title_info.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
sh $data_manage -s ${e_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_title_info.hql -i $param_dir/dm_eagle_cps.param_virtual.hql -a $rd &


# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_asset_scale_principal_day.hql -i $param_dir/dm_eagle.param.hql -a $rd &
# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_asset_scale_principal_day.hql -i $param_dir/dm_eagle_cps.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_asset_scale_principal_day.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_asset_scale_principal_day.hql -i $param_dir/dm_eagle_cps.param_virtual.hql -a $rd &


# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_loan_amount_day.hql -i $param_dir/dm_eagle.param.hql -a $rd &
# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_loan_amount_day.hql -i $param_dir/dm_eagle_cps.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_loan_amount_day.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_loan_amount_day.hql -i $param_dir/dm_eagle_cps.param_virtual.hql -a $rd &

wait_jobs

# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_asset_scale_repaid_day.hql -i $param_dir/dm_eagle.param.hql -a $rd &
# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_asset_scale_repaid_day.hql -i $param_dir/dm_eagle_cps.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_asset_scale_repaid_day.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_asset_scale_repaid_day.hql -i $param_dir/dm_eagle_cps.param_virtual.hql -a $rd &


# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_inflow_rate_first_term_day.hql -i $param_dir/dm_eagle.param.hql -a $rd &
# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_inflow_rate_first_term_day.hql -i $param_dir/dm_eagle_cps.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_inflow_rate_first_term_day.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_inflow_rate_first_term_day.hql -i $param_dir/dm_eagle_cps.param_virtual.hql -a $rd &


# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_deferred_overdue_rate_full_month_product_day.hql -i $param_dir/dm_eagle.param.hql -a $rd &
# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_deferred_overdue_rate_full_month_product_day.hql -i $param_dir/dm_eagle_cps.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_deferred_overdue_rate_full_month_product_day.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_deferred_overdue_rate_full_month_product_day.hql -i $param_dir/dm_eagle_cps.param_virtual.hql -a $rd &

wait_jobs



# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_inflow_rate_day.hql -i $param_dir/dm_eagle.param.hql -a $rd &
# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_inflow_rate_day.hql -i $param_dir/dm_eagle_cps.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_inflow_rate_day.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_inflow_rate_day.hql -i $param_dir/dm_eagle_cps.param_virtual.hql -a $rd &

# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_overdue_rate_month.hql -i $param_dir/dm_eagle.param.hql -a $rd &
# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_overdue_rate_month.hql -i $param_dir/dm_eagle_cps.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_overdue_rate_month.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_overdue_rate_month.hql -i $param_dir/dm_eagle_cps.param_virtual.hql -a $rd &


# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_migration_rate_month.hql -i $param_dir/dm_eagle.param.hql -a $rd &
# sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_migration_rate_month.hql -i $param_dir/dm_eagle_cps.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_migration_rate_month.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_migration_rate_month.hql -i $param_dir/dm_eagle_cps.param_virtual.hql -a $rd &


wait_jobs

sh $data_check_all ${s_date} ${e_date} dm_eagle &>> $log &

table_list=(
 # 资产
 dm_eagle.eagle_inflow_rate_first_term_day-$dm_eagle_dm_eagle
 dm_eagle_cps.eagle_inflow_rate_first_term_day-$dm_eagle_dm_eagle_cps

 dm_eagle.eagle_inflow_rate_day-$dm_eagle_dm_eagle
 dm_eagle_cps.eagle_inflow_rate_day-$dm_eagle_dm_eagle_cps

 dm_eagle.eagle_title_info-$dm_eagle_dm_eagle
 dm_eagle_cps.eagle_title_info-$dm_eagle_dm_eagle_cps

 dm_eagle.eagle_loan_amount_day-$dm_eagle_dm_eagle
 dm_eagle_cps.eagle_loan_amount_day-$dm_eagle_dm_eagle_cps

 dm_eagle.eagle_asset_scale_principal_day-$dm_eagle_dm_eagle
 dm_eagle_cps.eagle_asset_scale_principal_day-$dm_eagle_dm_eagle_cps

 dm_eagle.eagle_asset_scale_repaid_day-$dm_eagle_dm_eagle
 dm_eagle_cps.eagle_asset_scale_repaid_day-$dm_eagle_dm_eagle_cps

 dm_eagle.eagle_overdue_rate_month-$dm_eagle_dm_eagle
 dm_eagle_cps.eagle_overdue_rate_month-$dm_eagle_dm_eagle_cps

 dm_eagle.eagle_deferred_overdue_rate_full_month_product_day-$dm_eagle_dm_eagle
 dm_eagle_cps.eagle_deferred_overdue_rate_full_month_product_day-$dm_eagle_dm_eagle_cps

 dm_eagle.eagle_migration_rate_month-$dm_eagle_dm_eagle
 dm_eagle_cps.eagle_migration_rate_month-$dm_eagle_dm_eagle_cps

 dm_eagle.eagle_should_repay_repaid_amount_day-$dm_eagle_dm_eagle
 dm_eagle_cps.eagle_should_repay_repaid_amount_day-$dm_eagle_dm_eagle_cps


 # 资金页面资产
 dm_eagle.eagle_asset_change-$dm_eagle_dm_eagle
 dm_eagle.eagle_asset_comp_info-$dm_eagle_dm_eagle
 dm_eagle.eagle_asset_change_t1-$dm_eagle_dm_eagle
 dm_eagle.eagle_asset_change_comp-$dm_eagle_dm_eagle
 dm_eagle.eagle_asset_change_comp_t1-$dm_eagle_dm_eagle

 # 进件
 # dm_eagle.eagle_ret_msg_day-$dm_eagle_dm_eagle
 # dm_eagle.eagle_credit_loan_approval_rate_day-$dm_eagle_dm_eagle
 # dm_eagle.eagle_credit_loan_approval_amount_sum_day-$dm_eagle_dm_eagle
 # dm_eagle.eagle_credit_loan_approval_amount_rate_day-$dm_eagle_dm_eagle

 # 资金页面资金
 # dm_eagle.eagle_funds-$dm_eagle_dm_eagle
 # dm_eagle.eagle_acct_cost-$dm_eagle_dm_eagle
 # dm_eagle.eagle_unreach_funds-$dm_eagle_dm_eagle
 # dm_eagle.eagle_repayment_detail-$dm_eagle_dm_eagle

 # 应还实还
 dm_eagle.eagle_should_repay_repaid_amount_day-$dm_eagle_dm_eagle
 dm_eagle_cps.eagle_should_repay_repaid_amount_day-$dm_eagle_dm_eagle_cps
 # dm_eagle.eagle_should_repay_repaid_amount_day_hst-$dm_eagle_dm_eagle
 # dm_eagle_cps.eagle_should_repay_repaid_amount_day_hst-$dm_eagle_dm_eagle_cps

)

# dm_eagle 数据导出mysql
sh $bin/data_export.sh $s_date $e_date "${table_list[*]}" &>> $log

wait_jobs

echo -e "${date_e_aa:=$(date +'%F %T')} 资产dw_dm  结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log


$mail $pm_rd ' EMR  数据 4.0 资产dw_dm 结束' "
  执行开始日期： $s_date
  执行结束日期： $e_date
  操作开始时间： $date_s_aa
  操作结束时间： $date_e_aa
  操作执行时长： $during_time
" &>> $log


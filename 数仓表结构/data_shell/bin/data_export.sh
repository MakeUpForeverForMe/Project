#!/bin/bash -e

. ${data_export_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh

e_date=$(date +%F)
log=$log/$(basename "${BASH_SOURCE[0]}").${e_date}.log

echo -e "${date_s_aa:=$(date +'%F %T')} 数据导出  开始 当前脚本进程ID为：$(pid)" #&>> $log


tables['dm_eagle.eagle_inflow_rate_first_term_day']=$dm_eagle_dm_eagle
tables['dm_eagle_cps.eagle_inflow_rate_first_term_day']=$dm_eagle_dm_eagle_cps

tables['dm_eagle.eagle_inflow_rate_day']=$dm_eagle_dm_eagle
tables['dm_eagle_cps.eagle_inflow_rate_day']=$dm_eagle_dm_eagle_cps

tables['dm_eagle.eagle_title_info']=$dm_eagle_dm_eagle
tables['dm_eagle_cps.eagle_title_info']=$dm_eagle_dm_eagle_cps

tables['dm_eagle.eagle_loan_amount_day']=$dm_eagle_dm_eagle
tables['dm_eagle_cps.eagle_loan_amount_day']=$dm_eagle_dm_eagle_cps

tables['dm_eagle.eagle_asset_scale_principal_day']=$dm_eagle_dm_eagle
tables['dm_eagle_cps.eagle_asset_scale_principal_day']=$dm_eagle_dm_eagle_cps

tables['dm_eagle.eagle_asset_scale_repaid_day']=$dm_eagle_dm_eagle
tables['dm_eagle_cps.eagle_asset_scale_repaid_day']=$dm_eagle_dm_eagle_cps

tables['dm_eagle.eagle_overdue_rate_month']=$dm_eagle_dm_eagle
tables['dm_eagle_cps.eagle_overdue_rate_month']=$dm_eagle_dm_eagle_cps

tables['dm_eagle.eagle_deferred_overdue_rate_full_month_product_day']=$dm_eagle_dm_eagle
tables['dm_eagle_cps.eagle_deferred_overdue_rate_full_month_product_day']=$dm_eagle_dm_eagle_cps

tables['dm_eagle.eagle_migration_rate_month']=$dm_eagle_dm_eagle
tables['dm_eagle_cps.eagle_migration_rate_month']=$dm_eagle_dm_eagle_cps

tables['dm_eagle.eagle_should_repay_repaid_amount_day']=$dm_eagle_dm_eagle
tables['dm_eagle_cps.eagle_should_repay_repaid_amount_day']=$dm_eagle_dm_eagle_cps


# 资金页面资产
tables['dm_eagle.eagle_asset_change']=$dm_eagle_dm_eagle
tables['dm_eagle.eagle_asset_comp_info']=$dm_eagle_dm_eagle
tables['dm_eagle.eagle_asset_change_t1']=$dm_eagle_dm_eagle
tables['dm_eagle.eagle_asset_change_comp']=$dm_eagle_dm_eagle
tables['dm_eagle.eagle_asset_change_comp_t1']=$dm_eagle_dm_eagle

# 进件
tables['dm_eagle.eagle_ret_msg_day']=$dm_eagle_dm_eagle
tables['dm_eagle.eagle_credit_loan_approval_rate_day']=$dm_eagle_dm_eagle
tables['dm_eagle.eagle_credit_loan_approval_amount_sum_day']=$dm_eagle_dm_eagle
tables['dm_eagle.eagle_credit_loan_approval_amount_rate_day']=$dm_eagle_dm_eagle

# 资金页面资金
tables['dm_eagle.eagle_funds']=$dm_eagle_dm_eagle
tables['dm_eagle.eagle_acct_cost']=$dm_eagle_dm_eagle
tables['dm_eagle.eagle_unreach_funds']=$dm_eagle_dm_eagle
tables['dm_eagle.eagle_repayment_detail']=$dm_eagle_dm_eagle

# 应还实还
tables['dm_eagle.eagle_should_repay_repaid_amount_day']=$dm_eagle_dm_eagle
tables['dm_eagle_cps.eagle_should_repay_repaid_amount_day']=$dm_eagle_dm_eagle_cps
#tables['dm_eagle.eagle_should_repay_repaid_amount_day_hst']=$dm_eagle_dm_eagle
#tables['dm_eagle_cps.eagle_should_repay_repaid_amount_day_hst']=$dm_eagle_dm_eagle_cps
#tables['dm_eagle_cps.eagle_should_repay_repaid_amount_day_hst']=$dm_eagle_dm_eagle_cps


# 星云 ABS 导出
#tables['dm_eagle.abs_overdue_rate_day']=$dm_eagle_uabs_core
#tables['dm_eagle.abs_asset_information_bag']=$dm_eagle_uabs_core
#tables['dm_eagle.abs_asset_distribution_day']=$dm_eagle_uabs_core
#tables['dm_eagle.abs_asset_information_project']=$dm_eagle_uabs_core
#tables['dm_eagle.abs_asset_distribution_bag_day']=$dm_eagle_uabs_core
#tables['dm_eagle.abs_asset_information_bag_snapshot']=$dm_eagle_uabs_core
#tables['dm_eagle.abs_asset_information_cash_flow_bag_day']=$dm_eagle_uabs_core
#tables['dm_eagle.abs_asset_distribution_bag_snapshot_day']=$dm_eagle_uabs_core
#tables['dm_eagle.abs_asset_information_cash_flow_bag_snapshot']=$dm_eagle_uabs_core







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

echo -e "${date_e_aa:=$(date +'%F %T')} 数据导出  结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" #&>> $log

# $mail $pm_rd '数据 4.0 数据导出 结束' "
#   执行导出日期： $e_date
#   操作开始时间： $date_s_aa
#   操作结束时间： $date_e_aa
#   操作执行时长： $during_time
# " #&>> $log

#!/bin/bash -e

. ${data_export_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh

e_date=$(date +%F)
log=$log/$(basename "${BASH_SOURCE[0]}").${e_date}.log

echo -e "${date_s_aa:=$(date +'%F %T')} 数据导出  开始 当前脚本进程ID为：$(pid)"


# tables['dm_eagle.eagle_inflow_rate_first_term_day']=$dm_eagle_dm_eagle
# tables['dm_eagle_cps.eagle_inflow_rate_first_term_day']=$dm_eagle_dm_eagle_cps

# tables['dm_eagle.eagle_inflow_rate_day']=$dm_eagle_dm_eagle
# tables['dm_eagle_cps.eagle_inflow_rate_day']=$dm_eagle_dm_eagle_cps

# tables['dm_eagle.eagle_title_info']=$dm_eagle_dm_eagle
# tables['dm_eagle_cps.eagle_title_info']=$dm_eagle_dm_eagle_cps

# tables['dm_eagle.eagle_loan_amount_day']=$dm_eagle_dm_eagle
# tables['dm_eagle_cps.eagle_loan_amount_day']=$dm_eagle_dm_eagle_cps

# tables['dm_eagle.eagle_asset_scale_principal_day']=$dm_eagle_dm_eagle
# tables['dm_eagle_cps.eagle_asset_scale_principal_day']=$dm_eagle_dm_eagle_cps

# tables['dm_eagle.eagle_asset_scale_repaid_day']=$dm_eagle_dm_eagle
# tables['dm_eagle_cps.eagle_asset_scale_repaid_day']=$dm_eagle_dm_eagle_cps

# tables['dm_eagle.eagle_overdue_rate_month']=$dm_eagle_dm_eagle
# tables['dm_eagle_cps.eagle_overdue_rate_month']=$dm_eagle_dm_eagle_cps

# tables['dm_eagle.eagle_deferred_overdue_rate_full_month_product_day']=$dm_eagle_dm_eagle
# tables['dm_eagle_cps.eagle_deferred_overdue_rate_full_month_product_day']=$dm_eagle_dm_eagle_cps

# tables['dm_eagle.eagle_migration_rate_month']=$dm_eagle_dm_eagle
# tables['dm_eagle_cps.eagle_migration_rate_month']=$dm_eagle_dm_eagle_cps

# tables['dm_eagle.eagle_should_repay_repaid_amount_day']=$dm_eagle_dm_eagle
# tables['dm_eagle_cps.eagle_should_repay_repaid_amount_day']=$dm_eagle_dm_eagle_cps


# # 资金页面资产
# tables['dm_eagle.eagle_asset_change']=$dm_eagle_dm_eagle
# tables['dm_eagle.eagle_asset_comp_info']=$dm_eagle_dm_eagle
# tables['dm_eagle.eagle_asset_change_t1']=$dm_eagle_dm_eagle
# tables['dm_eagle.eagle_asset_change_comp']=$dm_eagle_dm_eagle
# tables['dm_eagle.eagle_asset_change_comp_t1']=$dm_eagle_dm_eagle

# # 进件
# tables['dm_eagle.eagle_ret_msg_day']=$dm_eagle_dm_eagle
# tables['dm_eagle.eagle_credit_loan_approval_rate_day']=$dm_eagle_dm_eagle
# tables['dm_eagle.eagle_credit_loan_approval_amount_sum_day']=$dm_eagle_dm_eagle
# tables['dm_eagle.eagle_credit_loan_approval_amount_rate_day']=$dm_eagle_dm_eagle

# # 资金页面资金
# tables['dm_eagle.eagle_funds']=$dm_eagle_dm_eagle
# tables['dm_eagle.eagle_acct_cost']=$dm_eagle_dm_eagle
# tables['dm_eagle.eagle_unreach_funds']=$dm_eagle_dm_eagle
# tables['dm_eagle.eagle_repayment_detail']=$dm_eagle_dm_eagle

# # 应还实还
# tables['dm_eagle.eagle_should_repay_repaid_amount_day']=$dm_eagle_dm_eagle
# tables['dm_eagle_cps.eagle_should_repay_repaid_amount_day']=$dm_eagle_dm_eagle_cps
#tables['dm_eagle.eagle_should_repay_repaid_amount_day_hst']=$dm_eagle_dm_eagle
#tables['dm_eagle_cps.eagle_should_repay_repaid_amount_day_hst']=$dm_eagle_dm_eagle_cps
#tables['dm_eagle_cps.eagle_should_repay_repaid_amount_day_hst']=$dm_eagle_dm_eagle_cps

# 星云 ABS 导出
# tables['dm_eagle.abs_overdue_rate_day']=$dm_eagle_uabs_core

# tables['dm_eagle.abs_asset_distribution_day']=$dm_eagle_uabs_core
# tables['dm_eagle.abs_asset_distribution_bag_day']=$dm_eagle_uabs_core
# tables['dm_eagle.abs_asset_distribution_bag_snapshot_day']=$dm_eagle_uabs_core

# tables['dm_eagle.abs_asset_information_bag']=$dm_eagle_uabs_core
tables['dm_eagle.abs_asset_information_project']=$dm_eagle_uabs_core
# tables['dm_eagle.abs_asset_information_bag_snapshot']=$dm_eagle_uabs_core

# tables['dm_eagle.abs_asset_information_cash_flow_bag_day']=$dm_eagle_uabs_core
# tables['dm_eagle.abs_asset_information_cash_flow_bag_snapshot']=$dm_eagle_uabs_core



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

        echo "$hql_key" "$export_file" "${hql[$hql_key]}"

        export_file_from_hive "${hql[$hql_key]}"
        # sleep 2
      } &
      p_opera $p_num &> /dev/null
    done
done

wait_jobs
echo -e "${date_a_aa:=$(date +'%F %T')} 从Hive导出数据结束，开始上传到MySQL 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_a_aa" "$date_s_aa")}\n\n"

for db_tb in ${!tables[@]};do

  all_file="$file_export/${db_tb}.*.tsv"
  export_file="$file_export/${db_tb}.tsv"
  mysql="$($mysql_cmd ${tables[$db_tb]})"

  # echo $all_file

  cat $all_file > $export_file
  rm -f $all_file

  import_file_to_mysql &
  p_opera $p_num &> /dev/null

done

wait_jobs

echo -e "${date_e_aa:=$(date +'%F %T')} 数据导出  结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_a_aa")}\n\n"

# $mail $pm_rd '数据 4.0 数据导出 结束' "
#   执行导出日期： $e_date
#   操作开始时间： $date_s_aa
#   操作结束时间： $date_e_aa
#   操作执行时长： $during_time
# "

#!/usr/bin/env bash

. ${data_export_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh

log_file=$log/$(basename "${BASH_SOURCE[0]}")-$(pid).log

info "导出数据 开始！"
date_s_aa=$curr_time

table_list=(
  # # 资产
  # dm_eagle.eagle_inflow_rate_first_term_day-$dm_eagle_dm_eagle
  # dm_eagle_cps.eagle_inflow_rate_first_term_day-$dm_eagle_dm_eagle_cps

  # dm_eagle.eagle_inflow_rate_day-$dm_eagle_dm_eagle
  # dm_eagle_cps.eagle_inflow_rate_day-$dm_eagle_dm_eagle_cps

  # dm_eagle.eagle_title_info-$dm_eagle_dm_eagle
  # dm_eagle_cps.eagle_title_info-$dm_eagle_dm_eagle_cps

  # dm_eagle.eagle_loan_amount_day-$dm_eagle_dm_eagle
  # dm_eagle_cps.eagle_loan_amount_day-$dm_eagle_dm_eagle_cps

  # dm_eagle.eagle_asset_scale_principal_day-$dm_eagle_dm_eagle
  # dm_eagle_cps.eagle_asset_scale_principal_day-$dm_eagle_dm_eagle_cps

  # dm_eagle.eagle_asset_scale_repaid_day-$dm_eagle_dm_eagle
  # dm_eagle_cps.eagle_asset_scale_repaid_day-$dm_eagle_dm_eagle_cps

  # dm_eagle.eagle_overdue_rate_month-$dm_eagle_dm_eagle
  # dm_eagle_cps.eagle_overdue_rate_month-$dm_eagle_dm_eagle_cps

  # dm_eagle.eagle_deferred_overdue_rate_full_month_product_day-$dm_eagle_dm_eagle
  # dm_eagle_cps.eagle_deferred_overdue_rate_full_month_product_day-$dm_eagle_dm_eagle_cps

  # dm_eagle.eagle_migration_rate_month-$dm_eagle_dm_eagle
  # dm_eagle_cps.eagle_migration_rate_month-$dm_eagle_dm_eagle_cps

  # dm_eagle.eagle_should_repay_repaid_amount_day-$dm_eagle_dm_eagle
  # dm_eagle_cps.eagle_should_repay_repaid_amount_day-$dm_eagle_dm_eagle_cps


  # # 资金页面资产
  # dm_eagle.eagle_asset_change-$dm_eagle_dm_eagle
  # dm_eagle.eagle_asset_comp_info-$dm_eagle_dm_eagle
  # dm_eagle.eagle_asset_change_t1-$dm_eagle_dm_eagle
  # dm_eagle.eagle_asset_change_comp-$dm_eagle_dm_eagle
  # dm_eagle.eagle_asset_change_comp_t1-$dm_eagle_dm_eagle

  # # 进件
  # dm_eagle.eagle_ret_msg_day-$dm_eagle_dm_eagle
  # dm_eagle.eagle_credit_loan_approval_rate_day-$dm_eagle_dm_eagle
  # dm_eagle.eagle_credit_loan_approval_amount_sum_day-$dm_eagle_dm_eagle
  # dm_eagle.eagle_credit_loan_approval_amount_rate_day-$dm_eagle_dm_eagle

  # # 资金页面资金
  # dm_eagle.eagle_funds-$dm_eagle_dm_eagle
  # dm_eagle.eagle_acct_cost-$dm_eagle_dm_eagle
  # dm_eagle.eagle_unreach_funds-$dm_eagle_dm_eagle
  # dm_eagle.eagle_repayment_detail-$dm_eagle_dm_eagle

  # # 应还实还
  # dm_eagle.eagle_should_repay_repaid_amount_day-$dm_eagle_dm_eagle
  # dm_eagle_cps.eagle_should_repay_repaid_amount_day-$dm_eagle_dm_eagle_cps
  # dm_eagle.eagle_should_repay_repaid_amount_day_hst-$dm_eagle_dm_eagle
  # dm_eagle_cps.eagle_should_repay_repaid_amount_day_hst-$dm_eagle_dm_eagle_cps
  # dm_eagle_cps.eagle_should_repay_repaid_amount_day_hst-$dm_eagle_dm_eagle_cps

  # # 星云 ABS 导出
  # dm_eagle.abs_overdue_rate_day-$dm_eagle_uabs_core

  # dm_eagle.abs_asset_distribution_day-$dm_eagle_uabs_core
  # dm_eagle.abs_asset_distribution_bag_day-$dm_eagle_uabs_core
  # dm_eagle.abs_asset_distribution_bag_snapshot_day-$dm_eagle_uabs_core

  # dm_eagle.abs_asset_information_bag-$dm_eagle_uabs_core
  # dm_eagle.abs_asset_information_project-$dm_eagle_uabs_core
  # dm_eagle.abs_asset_information_bag_snapshot-$dm_eagle_uabs_core

  # dm_eagle.abs_asset_information_cash_flow_bag_day-$dm_eagle_uabs_core
  # dm_eagle.abs_asset_information_cash_flow_bag_snapshot-$dm_eagle_uabs_core
)

# s_date=2017-06-01
# e_date=2021-04-30
s_date=$(date -d '-1 day' +%F)
e_date=$(date -d '-1 day' +%F)

s_date="$(date -d "${1:-${s_date}}" +%F)"
e_date="$(date -d "${2:-${e_date}}" +%F)"
table_list=(${3:-${table_list[@]}})


for tbl in ${table_list[@]}; do
  tbl_mysql=($(b_a $tbl))
  tables[${tbl_mysql[0]}]=$(echo ${tbl_mysql[1]})
done



p_num=30

# 全表导出的表
export_all_data_tbl=(
  dm_eagle.eagle_title_info
  dm_eagle_cps.eagle_title_info

  dm_eagle.abs_asset_information_bag_snapshot
  dm_eagle.abs_asset_distribution_bag_snapshot_day
  dm_eagle.abs_asset_information_cash_flow_bag_snapshot
)

info "导出数据 从 Hive 导出数据 开始!"
date_a_aa=$curr_time

for db_tb in ${!tables[@]};do
    hql=()
    current_date=$(date +%Y%m%d)

    if [[ -n $(echo "${export_all_data_tbl[@]}" | grep -ow "${db_tb}") ]]; then
      hql[$current_date]="select * from ${db_tb};"
    elif [[ "${db_tb}" = 'dm_eagle.abs_asset_information_cash_flow_bag_day' ]]; then
      hql[$current_date]="select * from dm_eagle.abs_asset_information_cash_flow_bag_day where biz_date = '2099-12-31';"
    else
      for (( excute_date = $(date -d "${s_date}" +%Y%m%d); excute_date <= $(date -d "${e_date}" +%Y%m%d); excute_date = $(date -d "1 day ${excute_date}" +%Y%m%d) )); do
        hql[$excute_date]="select * from ${db_tb} where biz_date = '$(date -d "${excute_date}" +%F)';"
      done
    fi


    for hql_key in "${!hql[@]}"; do
      {
        export_file="$file_export/${db_tb}.$hql_key.tsv"

        debug "$hql_key"'\t'"$export_file"'\t'"${hql[$hql_key]}"

        export_file_from_hive "${hql[$hql_key]}"
      } &
      p_opera $p_num &> /dev/null
    done
done

wait_jobs

info "导出数据 从 Hive 导出数据 结束！用时：$(during "${date_a_aa}")"
info "导出数据 MySQL 上传 开始！"
date_a_aa=$curr_time

for db_tb in ${!tables[@]};do

  all_file="$file_export/${db_tb}.*.tsv"
  export_file="$file_export/${db_tb}.tsv"
  mysql="$($mysql_cmd ${tables[$db_tb]})"

  debug "整理数据 all_file ——> simple 开始 ${db_tb}！"
  cat $all_file > $export_file
  debug "整理数据 all_file ——> simple 结束 ${db_tb}！"

  debug "删除数据 all_file 开始 ${db_tb}！"
  rm -f $all_file
  debug "删除数据 all_file 结束 ${db_tb}！"

  tb=$(p_r_r ${db_tb})
  if [[ -n $(echo "${export_all_data_tbl[@]}" | grep -ow "${db_tb}") || "${db_tb}" = 'dm_eagle.abs_asset_information_cash_flow_bag_day' ]]; then
    delete_sql="truncate table ${tb};"
  else
    delete_sql="DELETE FROM ${tb} WHERE biz_date BETWEEN '${1}' and '${2}';"
  fi

  import_file_to_mysql "${delete_sql}" &
  p_opera $p_num &> /dev/null
done

wait_jobs

info "导出数据 MySQL 上传 结束！用时：$(during "${date_a_aa}")"
info "导出数据 结束！用时：$(during "${date_s_aa}")"

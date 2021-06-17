#!/usr/bin/env bash

. ${sqoop_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh


ST9=${1:-$(date -d '-1 day' +%F)}

log_file=$log/sqoop/$(basename "${BASH_SOURCE[0]}").${ST9}.log

job_name='星云 抽数任务'

info "${job_name} 开始！"
date_s_time=$curr_time

sqoop_list=(
  'xinglian-t_asset_check'
  'abs-t_assetaccountcheck'

  'xinglian-t_loan_contract_info'
  'xinglian-t_principal_borrower_info'
  'xinglian-t_contact_person_info'
  'xinglian-t_guaranty_info'
  'xinglian-t_repayment_schedule-y'
  'xinglian-t_asset_pay_flow'
  'xinglian-t_repayment_info'
  'xinglian-t_asset_disposal'
  'xinglian-t_asset_supplement'
  'xinglian-t_project_check'
  'xinglian-t_enterprise_info'
  'xinglian-t_ods_credit'
  'xinglian-t_credit_loan'

  'abs-t_loancontractinfo'
  'abs-t_borrowerinfo'
  'abs-t_associatesinfo'
  'abs-t_guarantycarinfo'
  'abs-t_repaymentplan_history'
  'abs-t_assettradeflow'
  'abs-t_actualrepayinfo'
  'abs-t_assetdealprocessinfo'
  'abs-t_assetaddtradeinfo'
  'abs-t_projectaccountcheck'
  'abs-t_enterpriseinfo'
  'abs-t_guarantyhouseinfo'

  'abs-t_basic_asset'
  'abs-t_asset_wind_control_history'
  'abs-t_wind_control_resp_log'
  'abs-t_project'
  'abs-t_related_assets'
  'abs-t_asset_bag'
)

sh $bin/sqoop_sync.sh "${ST9}" "${sqoop_list[*]}" 2>&1 | tee -a $log_file

info "${job_name} 结束！用时：${during_time:=$(during "$date_s_time")}"


$mail $pm_rd "${job_name} 完成！" "
  执行抽数日期： $ST9
  执行开始时间： $date_s_time
  执行结束时间： $curr_time
  执行执行时长： $during_time
" 2>&1 | tee -a $log_file

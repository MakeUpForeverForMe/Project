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


echo -e "${date_s_aa:=$(date +'%F %T')} 汇通资产dw_dm  开始 当前脚本进程ID为：$(pid)\n" &>> $log

sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_repay_detail.hql -i $param_dir/dm_eagle.param_ht.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_order_info.hql -i $param_dir/dm_eagle.param_ht.hql -a $rd &

wait_jobs


sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_repay_schedule.hql -i $param_dir/dm_eagle.param_ht.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_loan_info.hql -i $param_dir/dm_eagle.param_ht.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_repayment_record_day.hql -i $param_dir/dm_eagle.param_ht.hql -a $rd &

wait_jobs

sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw_new.dw_loan_base_stat_loan_num_day.hql -i $param_dir/dw_new.param_ht.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw_new.dw_loan_base_stat_overdue_num_day.hql -i $param_dir/dw_new.param_ht.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw_new.dw_loan_base_stat_repay_detail_day.hql -i $param_dir/dw_new.param_ht.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw_new.dw_loan_base_stat_should_repay_day.hql -i $param_dir/dw_new.param_ht.hql -a $rd &

wait_jobs

#sh /root/data_shell/bin/data_manage.sh -s '2020-12-25' -e '2020-12-28' -f /root/data_shell/file_hql/dw_new.query/dw_new_cps.dw_asset_info_day.hql -i /root/data_shell/param_beeline/dw_new_cps.param.hql -a  /root/data_shell/conf_mail/data_receives_mail_guochao.config &
sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw_new_cps.dw_asset_info_day.hql -i $param_dir/dw_new_cps.param.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dw_new_hql/dw_new.dw_asset_info_day.hql -i $param_dir/dw_new_cps.param.hql -a $rd &
wait_jobs
# dm 层

sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_should_repay_repaid_amount_day.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
sh $data_manage -s ${e_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_title_info.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_asset_scale_principal_day.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_loan_amount_day.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &

wait_jobs

sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_asset_scale_repaid_day.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_inflow_rate_first_term_day.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_deferred_overdue_rate_full_month_product_day.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &

wait_jobs



sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_inflow_rate_day.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_overdue_rate_month.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_migration_rate_month.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &


wait_jobs

sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_asset_change_t1.hql -i $param_dir/dm_eagle.param_virtual.hql -a $rd &
sh $data_manage -s ${s_date} -e ${e_date} -f $dm_eagle_hql/dm_eagle.eagle_asset_change_comp_t1.hql -i $param_dir/dm_eagle_cps.param_virtual.hql -a $rd &
wait_jobs
echo -e "${date_e_aa:=$(date +'%F %T')} 汇通资产dw_dm  结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log

$mail $pm_rd '数据 4.0 汇通资产dw_dm 结束' "
  执行开始日期： $s_date
  执行结束日期： $e_date
  操作开始时间： $date_s_aa
  操作结束时间： $date_e_aa
  操作执行时长： $during_time
" &>> $log


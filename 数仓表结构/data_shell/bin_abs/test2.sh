#!/usr/bin/env bash



# 并行限制（默认可以并行数为5个）
p_opera(){
  p_num=${1:-5}
  pids=(${pids[@]:-} $!)
  echo '并行限制输出显示' '并行任务的数量：'${#pids[@]} '并行任务的数组下标：'${!pids[@]} '并行任务的PID：'${pids[@]}
  while [[ ${#pids[@]} -ge $p_num ]]; do
    pids_old=(${pids[@]})
    pids=()
    for p in "${pids_old[@]}";do
      [[ -d "/proc/$p" ]] && pids=(${pids[@]} $p)
    done
    sleep 0.01
  done
}


# 等待后台任务执行结束，再执行之后的操作
wait_jobs(){
  for pid in $(jobs -p); do
    wait $pid
  done
}

date_eq(){
  min_date="${1}"
  max_date="${2}"
  s_year=$(date -d "$min_date" +%Y) s_month=$(date -d "$min_date" +%m) s_day=$(date -d "$min_date" +%d)
  e_year=$(date -d "$max_date" +%Y) e_month=$(date -d "$max_date" +%m) e_day=$(date -d "$max_date" +%d)

  # printf '%-20s\t%-s\n' "$min_date" "$max_date" "$s_year" "$e_year" "$s_month" "$e_month" "$s_day" "$e_day"

  if [[ $s_year == $e_year ]]; then
    if [[ $s_month == $e_month ]]; then
      if [[ $s_day == $e_day ]]; then
        echo true
      elif [[ $s_day < $e_day ]]; then
        echo true
      else
        echo false
      fi
    elif [[ $s_month < $e_month ]]; then
      echo true
    else
      echo false
    fi
  elif [[ $s_year < $e_year ]]; then
    echo true
  else
    echo false
  fi
}


root_dir=/data/data_shell

data_manage=$root_dir/bin/data_manage.sh

ods_cloud=$root_dir/file_hql/ods_cloud.query
ods=$root_dir/file_hql/ods.query
dm_eagle_hql=$root_dir/file_hql/dm_eagle.query

param=$root_dir/param_beeline

mail=$root_dir/conf_mail/data_receives_mail_ximing.config

log=$root_dir/log



s_date_in=2017-06-01
e_date_in=2021-03-27

s_date=$s_date_in

p_num=30

while [[ $(date_eq "$s_date" "$e_date_in") == true ]]; do
  sh $data_manage -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_day.hql              -a $mail &
  p_opera $p_num

  sh $data_manage -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_bag_day.hql          -a $mail &
  p_opera $p_num

  sh $data_manage -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_bag_snapshot_day.hql -a $mail &
  p_opera $p_num


  sh $data_manage -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_project.hql -a $mail &
  p_opera $p_num


  sh $data_manage -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_bag.hql -a $mail -k bag_id='' -k snapshot='' &
  p_opera $p_num

  sh $data_manage -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_bag.hql -a $mail -k bag_id='' -k snapshot='_snapshot' &
  p_opera $p_num


  # sh $data_manage -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_early_payment_asset_details.hql   -a $mail &
  p_opera $p_num

  # sh $data_manage -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_early_payment_asset_statistic.hql -a $mail &
  p_opera $p_num

  sh $data_manage -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_asset_information_cash_flow_bag_day.hql -a $mail &
  p_opera $p_num

  sh $data_manage -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_overdue_rate_day.hql         -a $mail -k bag_id='' &
  p_opera $p_num

  sh $data_manage -s ${s_date} -e ${s_date} -f $dm_eagle_hql/dm_eagle.abs_overdue_rate_details_day.hql -a $mail -k bag_id='' &
  p_opera $p_num

  [[ $s_date == $e_date_in ]] && wait_jobs

  s_date=$(date -d "+1 day $s_date" +%F)
done





















# #!/usr/bin/env bash

# # 等待后台任务执行结束，再执行之后的操作
# wait_jobs(){
#   for pid in $(jobs -p); do
#     wait $pid
#   done
# }

# root_dir=/data/data_shell

# data_manage=$root_dir/bin/data_manage.sh

# ods_cloud=$root_dir/file_hql/ods_cloud.query
# ods=$root_dir/file_hql/ods.query
# dm_eagle_hql=$root_dir/file_hql/dm_eagle.query

# param=$root_dir/param_beeline

# mail=$root_dir/conf_mail/data_receives_mail_ximing.config

# log=$root_dir/log

# num=30

# sh $data_manage -s 2019-11-27 -e 2021-03-29 -f $dm_eagle_hql/dm_eagle.abs_asset_distribution_bag_snapshot_day.hql -n $num -a $mail &
# sh $data_manage -s 2019-08-01 -e 2021-03-29 -f $dm_eagle_hql/dm_eagle.abs_asset_information_project.hql -n $num -a $mail &
# wait_jobs

# sh $data_manage -s 2017-06-01 -e 2021-03-29 -f $dm_eagle_hql/dm_eagle.abs_asset_information_bag.hql -n $num -a $mail -k bag_id='' -k snapshot='' &
# sh $data_manage -s 2017-06-01 -e 2021-03-29 -f $dm_eagle_hql/dm_eagle.abs_asset_information_bag.hql -n $num -a $mail -k bag_id='' -k snapshot='_snapshot' &
# wait_jobs

# sh $data_manage -s 2017-06-01 -e 2021-03-29 -f $dm_eagle_hql/dm_eagle.abs_overdue_rate_day.hql -n $num         -a $mail -k bag_id='' &
# sh $data_manage -s 2017-06-01 -e 2021-03-29 -f $dm_eagle_hql/dm_eagle.abs_overdue_rate_details_day.hql -n $num -a $mail -k bag_id='' &
# wait_jobs

# sh $data_manage -s 2017-06-01 -e 2021-03-29 -f $dm_eagle_hql/dm_eagle.abs_asset_information_cash_flow_bag_day.hql -n 60 -a $mail &

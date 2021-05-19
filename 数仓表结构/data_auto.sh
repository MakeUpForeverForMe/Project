#!/usr/bin/env bash

. /etc/profile
. ~/.bash_profile
. ${base_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/data_fun.sh

export LANG=zh_CN.UTF-8


# 做超链接（Windows）
link \
"D:\Users\ximing.wei\Desktop\技术中心\数仓表结构\数仓 表设计\星云\校验平台.sql" \
"D:\Users\ximing.wei\Desktop\技术中心\数仓表结构\校验平台.sql"


link \
"D:\Users\ximing.wei\Desktop\code\shell_sql.md" \
"D:\Users\ximing.wei\Desktop\技术中心\数仓表结构\shell_sql.md"



get_file(){
  dir=${1}
  for file in $dir/*; do
    [[ -d $file ]] && get_file $file || {
      [[ -f $file ]] && {
        echo $file /d/Users/ximing.wei/Desktop/技术中心/数仓表结构/HiveUDF/src/${file:45}
        rm /d/Users/ximing.wei/Desktop/技术中心/数仓表结构/HiveUDF/src/${file:45}
        # link $file /d/Users/ximing.wei/Desktop/技术中心/数仓表结构/HiveUDF/src/${file:45}
      }
    }
  done
}
get_file /d/Users/ximing.wei/Desktop/code/HiveUDF/src




get_file(){
  from_root_dir=/d/Users/ximing.wei/Desktop/技术中心/数仓表结构
  copy_root_dir=/d/Users/ximing.wei/Desktop/code
  for file in ${1:-$from_root_dir}/*; do
    tag_file=$copy_root_dir/数仓表结构/$(echo ${file//"${from_root_dir}/"/ })
    echo $tag_file
    [[ -d $file ]] && {
      # [[ ! -d $tag_file ]] && mkdir $tag_file
      get_file $file
    } || {
      [[ -f $file ]] && {
        printf '%-145s\t%s\n' $file $tag_file
        # rm $tag_file
        # link $file $tag_file
      }
    }
  done
}
get_file






for dir in dm_eagle_cps.param.hql dm_eagle_cps.param_virtual.hql dm_eagle.param.hql dm_eagle.param_virtual.hql dw_new_cps.param.hql dw_new.param.hql dw_new.param_ht_dd.hql dw_new.param_ht.hql ods_new_s_cps.param_lx.hql ods_new_s_cps.param_repaired.hql ods_new_s_cps.param_tmp.hql ods_new_s.param_ddhtgz.hql ods_new_s.param_ht.hql ods_new_s.param_lx.hql ods_new_s.param_repaired.hql ods_new_s.param_tmp.hql; do
  touch $dir
done



p_opera(){
  pids=(${pids[@]:-} $!)
  while [[ ${#pids[@]} -ge $p_num ]]; do
    pids_old=(${pids[@]})   pids=()
    for p in "${pids_old[@]}";do
      [[ -d "/proc/$p" ]] && pids=(${pids[@]} $p)
    done
    num=${#pids[@]}
    sleep 0.01
  done
}


while [[ -d /proc/$(ps -ef | grep -v grep | grep -P '\-i data_param/ods_new_s_cps.param_lx.hql \-a' | awk '{print $2}') ]]; do
  date +'%F %T'
  ps -ef | grep -v grep | grep -P '\-i data_param/ods_new_s_cps.param_lx.hql \-a'
  echo
  sleep 1
done

echo 123

# 总的调度
sh bin/data_manage.sh -s 2020-07-30 &




sh bin/data_manage.sh -s 2017-01-01 -e 2017-01-01 -f file_hql/ods_new_s_cloud.query/ods_new_s.repay_schedule_inter.hql -a conf_mail/data_receives_mail_ximing.config &

nohup sh bin/data_manage.sh -s 2017-01-01 -e 2017-12-31 -f file_hql/ods_new_s_cloud.query/ods_new_s.repay_schedule_inter.hql -a conf_mail/data_receives_mail_ximing.config &> /dev/null &
nohup sh bin/data_manage.sh -s 2018-01-01 -e 2018-12-31 -f file_hql/ods_new_s_cloud.query/ods_new_s.repay_schedule_inter.hql -a conf_mail/data_receives_mail_ximing.config &> /dev/null &
nohup sh bin/data_manage.sh -s 2019-01-01 -e 2019-12-31 -f file_hql/ods_new_s_cloud.query/ods_new_s.repay_schedule_inter.hql -a conf_mail/data_receives_mail_ximing.config &> /dev/null &
nohup sh bin/data_manage.sh -s 2020-01-01 -e 2020-11-30 -f file_hql/ods_new_s_cloud.query/ods_new_s.repay_schedule_inter.hql -a conf_mail/data_receives_mail_ximing.config &> /dev/null &

nohup sh bin/data_manage.sh -s 2020-12-01 -e 2020-12-15 -f file_hql/ods_new_s_cloud.query/ods_new_s.repay_schedule_inter.hql -a conf_mail/data_receives_mail_ximing.config &> /dev/null &


sh bin/data_manage.sh -s 2017-01-01 -e 2017-05-31 -f file_hql/ods_new_s_cloud.query/ods_new_s.repay_schedule_inter.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2017-06-01 -e 2017-12-31 -f file_hql/ods_new_s_cloud.query/ods_new_s.repay_schedule_inter.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2018-01-01 -e 2018-05-31 -f file_hql/ods_new_s_cloud.query/ods_new_s.repay_schedule_inter.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2018-06-01 -e 2018-12-31 -f file_hql/ods_new_s_cloud.query/ods_new_s.repay_schedule_inter.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-01-01 -e 2019-05-31 -f file_hql/ods_new_s_cloud.query/ods_new_s.repay_schedule_inter.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-06-01 -e 2019-12-31 -f file_hql/ods_new_s_cloud.query/ods_new_s.repay_schedule_inter.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-01-01 -e 2020-05-31 -f file_hql/ods_new_s_cloud.query/ods_new_s.repay_schedule_inter.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-01 -e 2020-12-01 -f file_hql/ods_new_s_cloud.query/ods_new_s.repay_schedule_inter.hql -a conf_mail/data_receives_mail_ximing.config &


sh bin/data_manage.sh -s 2020-12-09 -e 2020-12-09 -f file_hql/ods_new_s_cloud.query/ods_new_s.repay_schedule_inter.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-12-10 -e 2020-12-10 -f file_hql/ods_new_s_cloud.query/ods_new_s.repay_schedule_inter.hql -a conf_mail/data_receives_mail_ximing.config &



# ods层 进件
sh bin/data_manage.sh -s 2020-09-20  -e 2020-09-20 -f file_hql/ods_new_s.query/ods_new_s.loan_apply.hql -a conf_mail/data_receives_mail_ximing.config &

# ods层 资产
sh bin/data_manage.sh -s 2020-09-13 -e 2020-09-25 -f file_hql/ods_new_s.query/ods_new_s.loan_info.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-02 -e 2020-10-11 -f file_hql/ods_new_s.query/ods_new_s.loan_info.hql -i param_beeline/ods_new_s.param_001801_001901.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-02 -e 2020-09-26 -f file_hql/ods_new_s.query/ods_new_s.loan_info.hql -i param_beeline/ods_new_s_cps.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-09 -f file_hql/ods_new_s.query/ods_new_s.loan_info.hql -i param_beeline/ods_new_s.param_ddhtgz.hql -a conf_mail/data_receives_mail_ximing.config &



sh bin/data_manage.sh -s 2020-09-13 -e 2020-09-26 -f file_hql/ods_new_s.query/ods_new_s.repay_schedule.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-02 -e 2020-09-26 -f file_hql/ods_new_s.query/ods_new_s.repay_schedule.hql -i param_beeline/ods_new_s_cps.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &



sh bin/data_manage.sh -s 2020-10-03 -e 2020-10-03 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-04 -e 2020-10-04 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-05 -e 2020-10-05 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-06 -e 2020-10-06 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-07 -e 2020-10-07 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-08 -e 2020-10-08 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-10-09 -e 2020-10-09 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-10 -e 2020-10-10 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-11 -e 2020-10-11 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-12 -e 2020-10-12 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-13 -e 2020-10-13 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-14 -e 2020-10-14 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &


sh bin/data_manage.sh -s 2020-06-01 -e 2020-06-15 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-16 -e 2020-08-31 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-15 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-16 -e 2020-09-27 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-01 -e 2020-06-15 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s_cps.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s_cps.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s_cps.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s_cps.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s_cps.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-16 -e 2020-08-31 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s_cps.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-15 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s_cps.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-16 -e 2020-09-30 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s_cps.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &



sh bin/data_manage.sh -s 2020-10-17 -e 2020-10-17 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-17 -e 2020-10-17 -f file_hql/ods_new_s.query/ods_new_s.repay_detail_lx.hql -i param_beeline/ods_new_s_cps.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &





sh bin/data_manage.sh -s 2020-06-02 -e 2020-09-12 -f file_hql/ods_new_s.query/ods_new_s.loan_info.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-02 -e 2020-08-19 -f file_hql/ods_new_s.query/ods_new_s.order_info.hql -a conf_mail/data_receives_mail_ximing.config &





sh bin/data_manage.sh -s 2020-06-02 -e 2020-10-19 -f file_hql/ods_new_s.query/ods_new_s.order_info.hql -i param_beeline/ods_new_s.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-02 -e 2020-10-19 -f file_hql/ods_new_s.query/ods_new_s.order_info.hql -i param_beeline/ods_new_s_cps.param_lx.hql -a conf_mail/data_receives_mail_ximing.config &


# dw层 授信
sh bin/data_manage.sh -s 2020-06-02 -e 2020-09-06 -f file_hql/dw_new.query/dw_new.dw_credit_apply_stat_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-02 -e 2020-09-06 -f file_hql/dw_new.query/dw_new.dw_credit_apply_stat_day.hql -a conf_mail/data_receives_mail_ximing.config &


sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dw_new.query/dw_new.dw_credit_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dw_new.query/dw_new.dw_credit_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dw_new.query/dw_new.dw_credit_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dw_new.query/dw_new.dw_credit_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dw_new.query/dw_new.dw_credit_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-16 -e 2020-08-31 -f file_hql/dw_new.query/dw_new.dw_credit_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-15 -f file_hql/dw_new.query/dw_new.dw_credit_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-16 -e 2020-09-30 -f file_hql/dw_new.query/dw_new.dw_credit_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-01 -e 2020-10-15 -f file_hql/dw_new.query/dw_new.dw_credit_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-16 -e 2020-10-31 -f file_hql/dw_new.query/dw_new.dw_credit_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-11-01 -e 2020-11-15 -f file_hql/dw_new.query/dw_new.dw_credit_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-11-16 -e 2020-11-30 -f file_hql/dw_new.query/dw_new.dw_credit_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-12-01 -e 2020-12-15 -f file_hql/dw_new.query/dw_new.dw_credit_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-12-16 -e 2020-12-27 -f file_hql/dw_new.query/dw_new.dw_credit_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &


sh bin/data_manage.sh -s 2020-06-02 -e 2020-09-06 -f file_hql/dw_new.query/dw_new.dw_credit_ret_msg_day.hql -a conf_mail/data_receives_mail_ximing.config &

# dw层 用信
sh bin/data_manage.sh -s 2020-06-02 -e 2020-09-06 -f file_hql/dw_new.query/dw_new.dw_loan_apply_stat_day.hql -a conf_mail/data_receives_mail_ximing.config &


sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dw_new.query/dw_new.dw_loan_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dw_new.query/dw_new.dw_loan_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dw_new.query/dw_new.dw_loan_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dw_new.query/dw_new.dw_loan_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dw_new.query/dw_new.dw_loan_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-16 -e 2020-08-31 -f file_hql/dw_new.query/dw_new.dw_loan_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-15 -f file_hql/dw_new.query/dw_new.dw_loan_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-16 -e 2020-09-30 -f file_hql/dw_new.query/dw_new.dw_loan_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-01 -e 2020-10-15 -f file_hql/dw_new.query/dw_new.dw_loan_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-16 -e 2020-10-31 -f file_hql/dw_new.query/dw_new.dw_loan_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-11-01 -e 2020-11-15 -f file_hql/dw_new.query/dw_new.dw_loan_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-11-16 -e 2020-11-30 -f file_hql/dw_new.query/dw_new.dw_loan_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-12-01 -e 2020-12-15 -f file_hql/dw_new.query/dw_new.dw_loan_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-12-16 -e 2020-12-27 -f file_hql/dw_new.query/dw_new.dw_loan_approval_stat_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &


sh bin/data_manage.sh -s 2020-06-02 -e 2020-09-06 -f file_hql/dw_new.query/dw_new.dw_loan_ret_msg_day.hql -a conf_mail/data_receives_mail_ximing.config &

# dw层 资产
sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_loan_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_loan_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_loan_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_loan_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_loan_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-16 -e 2020-08-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_loan_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-08 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_loan_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-09-20 -e 2020-09-25 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_loan_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_loan_num_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_loan_num_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_loan_num_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_loan_num_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_loan_num_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-16 -e 2020-08-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_loan_num_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-08 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_loan_num_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &




sh bin/data_manage.sh -s 2019-11-01 -e 2019-11-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-12-01 -e 2019-12-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-01-01 -e 2020-01-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-02-01 -e 2020-02-29 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-03-01 -e 2020-03-01 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &


sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-16 -e 2020-08-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-15 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-16 -e 2020-09-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-01 -e 2020-10-10 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-20 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &



sh bin/data_manage.sh -s 2020-06-14 -e 2020-06-14 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-10-15 -e 2020-10-28 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &


sh bin/data_manage.sh -s 2020-06-02 -e 2020-10-28 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-02 -e 2020-10-28 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_overdue_num_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &




sh $bin/manage.p_opera.sh -s 2019-01-01 -e 2020-05-30 -f $dw_new_hql/dw_new.dw_loan_base_stat_overdue_num_day_hst.hql -i $param_dir/dw_new.param.hql -a $ximing &

sh $bin/manage.p_opera.sh -s 2020-06-01 -e 2021-01-11 -f $dw_new_hql/dw_new.dw_loan_base_stat_overdue_num_day_hst.hql -i $param_dir/dw_new.param.hql -a $ximing &

sh $bin/manage.p_opera.sh -s 2020-06-02 -e 2021-01-11 -f $dw_new_hql/dw_new.dw_loan_base_stat_overdue_num_day_hst.hql -i $param_dir/dw_new_cps.param.hql -a $ximing &


sh $bin/manage.p_opera.sh -s 2019-01-01 -e 2021-01-11 -f $dm_eagle_hql/dm_eagle.eagle_inflow_rate_day_hst.hql -i $param_dir/dm_eagle.param_virtual.hql -a $ximing &

sh $bin/manage.p_opera.sh -s 2020-06-02 -e 2021-01-11 -f $dm_eagle_hql/dm_eagle.eagle_inflow_rate_day_hst.hql -i $param_dir/dm_eagle_cps.param_virtual.hql -a $ximing




sh bin/data_manage.sh -s 2020-06-23 -e 2020-06-23 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-30 -e 2020-09-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-02 -e 2020-10-02 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-03 -e 2020-10-03 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-22 -e 2020-10-22 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-23 -e 2020-10-23 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-24 -e 2020-10-24 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &



sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-16 -e 2020-08-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-15 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-16 -e 2020-09-27 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-16 -e 2020-08-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-08 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_repay_detail_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &



sh bin/data_manage.sh -s 2019-01-01 -e 2019-01-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-02-01 -e 2019-02-28 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-03-01 -e 2019-03-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-04-01 -e 2019-04-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-05-01 -e 2019-05-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-06-01 -e 2019-06-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-07-01 -e 2019-07-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-08-01 -e 2019-08-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-09-01 -e 2019-09-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-10-01 -e 2019-10-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-11-01 -e 2019-11-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-12-01 -e 2019-12-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-01-01 -e 2020-01-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-02-01 -e 2020-02-29 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-03-01 -e 2020-03-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-04-01 -e 2020-04-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-05-01 -e 2020-05-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-01 -e 2020-06-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-01 -e 2020-10-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-11-01 -e 2020-11-22 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-01 -e 2020-10-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-11-01 -e 2020-11-22 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &


sh bin/data_manage.sh -s 2019-01-01 -e 2019-06-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-07-01 -e 2019-12-31 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-03-13 -e 2020-06-30 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-03 -e 2020-11-21 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-08-15 -e 2020-11-21 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &


sh bin/data_manage.sh -s 2020-06-15 -e 2020-06-15 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-08-15 -e 2020-11-21 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &


sh bin/data_manage.sh -s 2020-11-22 -e 2020-11-22 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-11-22 -e 2020-11-22 -f file_hql/dw_new.query/dw_new.dw_loan_base_stat_should_repay_day.hql -i param_beeline/dw_new_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &



# dm 层
sh bin/data_manage.sh -s 2020-01-01 -e 2020-01-01 -f file_hql/dm_eagle.query/dm_eagle.eagle_title_info_create.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-01-01 -e 2020-01-01 -f file_hql/dm_eagle.query/dm_eagle.eagle_title_info.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-01-01 -e 2020-01-01 -f file_hql/dm_eagle.query/dm_eagle.eagle_title_info.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &




sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-16 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-16 -e 2020-09-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-01 -e 2020-10-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-16 -e 2020-10-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-11-01 -e 2020-11-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-11-16 -e 2020-11-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-12-01 -e 2020-12-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-12-16 -e 2020-12-27 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &



sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_sum_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_sum_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_sum_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_sum_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_sum_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-16 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_sum_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_sum_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-16 -e 2020-09-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_sum_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-01 -e 2020-10-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_sum_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-16 -e 2020-10-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_sum_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-11-01 -e 2020-11-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_sum_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-11-16 -e 2020-11-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_sum_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-12-01 -e 2020-12-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_sum_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-12-16 -e 2020-12-27 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_sum_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2021-01-03 -e 2021-01-03 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_amount_sum_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &





sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_rate_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_rate_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_rate_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_rate_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_rate_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_rate_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_rate_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_rate_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_rate_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_rate_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-15 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_rate_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-15 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_rate_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e $(date +'%F') -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_rate_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e $(date +'%F') -f file_hql/dm_eagle.query/dm_eagle.eagle_credit_loan_approval_rate_day.hql -a conf_mail/data_receives_mail_ximing.config &





sh bin/data_manage.sh -s 2020-08-01 -e 2020-09-06 -f file_hql/dm_eagle.query/dm_eagle.eagle_ret_msg_day.hql -a conf_mail/data_receives_mail_ximing.config &


sh bin/data_manage.sh -s 2020-01-10 -e 2020-04-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_loan_info.hql -i param_beeline/dm_eagle.param.hql -a conf_mail/data_receives_mail_ximing.config &


sh bin/data_manage.sh -s 2020-06-02 -e 2020-11-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_repay_schedule.hql -i param_beeline/dm_eagle.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-02 -e 2020-11-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_repay_schedule.hql -i param_beeline/dm_eagle_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &




sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_loan_info.hql -i param_beeline/dm_eagle.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_loan_info.hql -i param_beeline/dm_eagle.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_loan_info.hql -i param_beeline/dm_eagle.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_loan_info.hql -i param_beeline/dm_eagle.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-01 -e 2020-10-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_loan_info.hql -i param_beeline/dm_eagle.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-11-01 -e 2020-11-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_loan_info.hql -i param_beeline/dm_eagle.param.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-02 -e 2020-11-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_loan_info.hql -i param_beeline/dm_eagle_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &









sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_repay_detail.hql -i param_beeline/dm_eagle_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_repay_detail.hql -i param_beeline/dm_eagle_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_repay_detail.hql -i param_beeline/dm_eagle_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_repay_detail.hql -i param_beeline/dm_eagle_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_repay_detail.hql -i param_beeline/dm_eagle_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-16 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_repay_detail.hql -i param_beeline/dm_eagle_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_repay_detail.hql -i param_beeline/dm_eagle_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-16 -e 2020-09-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_repay_detail.hql -i param_beeline/dm_eagle_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &



sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-06 -f file_hql/dm_eagle.query/dm_eagle.eagle_repay_detail.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-06 -f file_hql/dm_eagle.query/dm_eagle.eagle_repay_detail.hql -a conf_mail/data_receives_mail_ximing.config &


sh bin/data_manage.sh -s 2020-06-02 -e 2020-09-06 -f file_hql/dm_eagle.query/dm_eagle.eagle_order_info.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-02 -e 2020-09-06 -f file_hql/dm_eagle.query/dm_eagle.eagle_order_info.hql -a conf_mail/data_receives_mail_ximing.config &


sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_repayment_record_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_repayment_record_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_repayment_record_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_repayment_record_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_repayment_record_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_repayment_record_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_repayment_record_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_repayment_record_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_repayment_record_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_repayment_record_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-15 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_repayment_record_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-15 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_repayment_record_day.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-12-27 -e 2020-12-27 -f file_hql/dm_eagle.query/dm_eagle.eagle_repayment_record_day.hql -i param_beeline/dm_eagle_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &






sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_loan_amount_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_loan_amount_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_loan_amount_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_loan_amount_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-01 -e 2020-11-02 -f file_hql/dm_eagle.query/dm_eagle.eagle_loan_amount_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &



sh bin/data_manage.sh -s 2020-06-02 -e 2020-11-02 -f file_hql/dm_eagle.query/dm_eagle.eagle_loan_amount_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &







sh bin/data_manage.sh -s 2020-10-22 -e 2020-10-24 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-10-08 -e 2020-10-08 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &






sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_should_repay_repaid_amount_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_should_repay_repaid_amount_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_should_repay_repaid_amount_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_should_repay_repaid_amount_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_should_repay_repaid_amount_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-15 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_should_repay_repaid_amount_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-16 -e 2020-09-25 -f file_hql/dm_eagle.query/dm_eagle.eagle_should_repay_repaid_amount_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_should_repay_repaid_amount_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_should_repay_repaid_amount_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_should_repay_repaid_amount_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_should_repay_repaid_amount_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_should_repay_repaid_amount_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-15 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_should_repay_repaid_amount_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-16 -e 2020-09-25 -f file_hql/dm_eagle.query/dm_eagle.eagle_should_repay_repaid_amount_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &



sh bin/data_manage.sh -s 2020-12-01 -e 2020-12-01 -f file_hql/dm_eagle.query/dm_eagle.eagle_should_repay_repaid_amount_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-12-01 -e 2020-12-01 -f file_hql/dm_eagle.query/dm_eagle.eagle_should_repay_repaid_amount_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &




sh bin/data_manage.sh -s 2020-08-12 -e 2020-08-14 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_principal_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-12 -e 2020-08-14 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_principal_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-12 -e 2020-08-14 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_principal_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-12 -e 2020-08-14 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_principal_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-02 -e 2020-11-02 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_principal_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-02 -e 2020-11-02 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_principal_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &





sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-01 -e 2020-11-02 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &


sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-01 -e 2020-11-02 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &







sh bin/data_manage.sh -s 2020-09-13 -e 2020-09-25 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-16 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-06 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-16 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-06 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &


sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-16 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-06 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-16 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-06 -f file_hql/dm_eagle.query/dm_eagle.eagle_asset_scale_repaid_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &







sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-02 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-06-16 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-16 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-19 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-19 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-16 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-16 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e $(date -d '-1 day' +'%F') -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e $(date -d '-1 day' +'%F') -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e $(date -d '-1 day' +'%F') -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e $(date -d '-1 day' +'%F') -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_first_term_day.hql -a conf_mail/data_receives_mail_ximing.config &



sh bin/data_manage.sh -s 2019-01-10 -e 2019-02-28 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-03-01 -e 2019-03-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-04-01 -e 2019-04-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-05-01 -e 2019-05-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-06-01 -e 2019-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-07-01 -e 2019-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-08-01 -e 2019-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-09-01 -e 2019-09-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-10-01 -e 2019-10-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-11-01 -e 2019-11-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-12-01 -e 2019-12-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-01-01 -e 2020-01-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-02-01 -e 2020-02-29 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-03-01 -e 2020-03-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-04-01 -e 2020-04-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-05-01 -e 2020-05-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &


sh bin/data_manage.sh -s 2020-07-03 -e 2020-07-03 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-15 -e 2020-09-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-01 -e 2020-10-10 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-16 -e 2020-09-20 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-10-01 -e 2020-10-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-16 -e 2020-10-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-11-01 -e 2020-11-15 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-11-16 -e 2020-11-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_inflow_rate_day.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &













sh bin/data_manage.sh -s 2020-06-30 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-31 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-31 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-30 -e 2020-09-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-31 -e 2020-10-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &


sh bin/data_manage.sh -s 2020-06-30 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-31 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-31 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-30 -e 2020-09-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-31 -e 2020-10-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &



sh bin/data_manage.sh -s 2019-01-31 -e 2019-01-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-02-28 -e 2019-02-28 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-03-31 -e 2019-03-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-04-30 -e 2019-04-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-05-31 -e 2019-05-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-06-30 -e 2019-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-07-31 -e 2019-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-08-31 -e 2019-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-09-30 -e 2019-09-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-10-31 -e 2019-10-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-11-30 -e 2019-11-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-12-31 -e 2019-12-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-01-31 -e 2020-01-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-02-29 -e 2020-02-29 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-03-31 -e 2020-03-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-04-30 -e 2020-04-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-05-31 -e 2020-05-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-30 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-31 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-31 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-30 -e 2020-09-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-11 -e 2020-10-11 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-30 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-31 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-31 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-30 -e 2020-09-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-11 -e 2020-10-11 -f file_hql/dm_eagle.query/dm_eagle.eagle_migration_rate_month.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &




sh bin/data_manage.sh -s 2019-01-10 -e 2019-02-28 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-03-01 -e 2019-03-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-04-01 -e 2019-04-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-05-01 -e 2019-05-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-06-01 -e 2019-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-07-01 -e 2019-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-08-01 -e 2019-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-09-01 -e 2019-09-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-10-01 -e 2019-10-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-11-01 -e 2019-11-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2019-12-01 -e 2019-12-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-01-01 -e 2020-01-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-02-01 -e 2020-02-29 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-03-01 -e 2020-03-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-04-01 -e 2020-04-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-05-01 -e 2020-05-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-01 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-01 -e 2020-10-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-11-01 -e 2020-11-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-12-01 -e 2020-12-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &

sh bin/data_manage.sh -s 2020-06-02 -e 2020-06-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-01 -e 2020-07-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-08-01 -e 2020-08-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-09-01 -e 2020-09-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-10-01 -e 2020-10-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-11-01 -e 2020-11-30 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-12-01 -e 2020-12-31 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &




sh bin/data_manage.sh -s 2020-07-12 -e 2020-07-12 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &
sh bin/data_manage.sh -s 2020-07-13 -e 2020-07-13 -f file_hql/dm_eagle.query/dm_eagle.eagle_overdue_rate_month.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config &













sh bin/data_manage.sh -s 2020-08-11 -e 2020-08-11 -f file_hql/dm_eagle.query/dm_eagle.eagle_title_info_create.hql -a conf_mail/data_receives_mail_ximing.config

sh bin/data_manage.sh -s 2020-08-11 -e 2020-08-11 -f file_hql/dm_eagle.query/dm_eagle.eagle_title_info.hql -i param_beeline/dm_eagle.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config
sh bin/data_manage.sh -s 2020-08-11 -e 2020-08-11 -f file_hql/dm_eagle.query/dm_eagle.eagle_title_info.hql -i param_beeline/dm_eagle_cps.param_virtual.hql -a conf_mail/data_receives_mail_ximing.config













truncate table dm_eagle.eagle_acct_cost;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_acct_cost.tsv'                                        INTO TABLE dm_eagle.eagle_acct_cost                                        FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,acct_total_fee,fee_type,amount,biz_date);
truncate table dm_eagle.eagle_asset_change;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_asset_change.tsv'                                     INTO TABLE dm_eagle.eagle_asset_change                                     FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,yesterday_remain_sum,loan_sum_daily,repay_sum_daily,repay_other_sum_daily,today_remain_sum,biz_date);
truncate table dm_eagle.eagle_asset_change_comp;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_asset_change_comp.tsv'                                INTO TABLE dm_eagle.eagle_asset_change_comp                                FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,yesterday_remain_sum,loan_sum_daily,repay_sum_daily,repay_other_sum_daily,today_remain_sum,biz_date);
truncate table dm_eagle.eagle_asset_change_comp_t1;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_asset_change_comp_t1.tsv'                             INTO TABLE dm_eagle.eagle_asset_change_comp_t1                             FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,yesterday_remain_sum,loan_sum_daily,repay_sum_daily,repay_other_sum_daily,today_remain_sum,biz_date);
truncate table dm_eagle.eagle_asset_change_t1;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_asset_change_t1.tsv'                                  INTO TABLE dm_eagle.eagle_asset_change_t1                                  FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,yesterday_remain_sum,loan_sum_daily,repay_sum_daily,repay_other_sum_daily,today_remain_sum,biz_date);
truncate table dm_eagle.eagle_asset_comp_info;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_asset_comp_info.tsv'                                  INTO TABLE dm_eagle.eagle_asset_comp_info                                  FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,repay_sum_daily,repay_way,amount_type,amount,biz_date);
truncate table dm_eagle.eagle_funds;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_funds.tsv'                                            INTO TABLE dm_eagle.eagle_funds                                            FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,trade_yesterday_bal,loan_today_bal,repay_today_bal,acct_int,acct_total_fee,invest_cash,trade_today_bal,biz_date);
truncate table dm_eagle.eagle_repayment_detail;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_repayment_detail.tsv'                                 INTO TABLE dm_eagle.eagle_repayment_detail                                 FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,repay_today_bal,repay_type,amount,biz_date);
truncate table dm_eagle.eagle_unreach_funds;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_unreach_funds.tsv'                                    INTO TABLE dm_eagle.eagle_unreach_funds                                    FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,trade_yesterday_bal,repay_today_bal,repay_sum_daily,trade_today_bal,biz_date);

truncate table dm_eagle.eagle_credit_loan_approval_rate_day;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_credit_loan_approval_rate_day.tsv'                    INTO TABLE dm_eagle.eagle_credit_loan_approval_rate_day                    FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,loan_terms,credit_apply_num,credit_apply_num_accumulate,credit_approval_num,credit_approval_num_rate,credit_approval_num_rate_dod_ratio,loan_apply_num,loan_apply_num_accumulate,loan_approval_num,loan_approval_rate,loan_approval_num_rate_dod_ratio,credit_apply_amount,credit_apply_amount_accumulate,credit_approval_amount,credit_approval_amount_rate,loan_apply_amount,loan_apply_amount_accumulate,loan_approval_amount,loan_approval_amount_rate,credit_apply_num_person,credit_apply_num_person_accumulate,credit_approval_num_person,credit_approval_person_rate,loan_apply_num_person,loan_apply_num_person_accumulate,loan_approval_num_person,loan_approval_person_rate,biz_date,product_id);
truncate table dm_eagle.eagle_credit_loan_approval_amount_sum_day;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_credit_loan_approval_amount_sum_day.tsv'              INTO TABLE dm_eagle.eagle_credit_loan_approval_amount_sum_day              FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,loan_terms,credit_approval_amount,credit_approval_amount_accumulate,credit_approval_amount_dod_ratio,credit_approval_num,credit_approval_num_num_avg,credit_approval_num_num_avg_dod_ratio,loan_approval_amount,loan_approval_amount_accumulate,loan_approval_amount_dod_ratio,loan_approval_num,loan_approval_num_num_avg,loan_approval_num_num_avg_dod_ratio,credit_approval_num_accumulate,credit_approval_num_dod_ratio,credit_approval_num_person,credit_approval_num_person_accumulate,credit_approval_num_person_dod_ratio,credit_approval_num_person_num_avg,loan_approval_num_accumulate,loan_approval_num_dod_ratio,loan_approval_num_person,loan_approval_num_person_accumulate,loan_approval_num_person_dod_ratio,loan_approval_num_person_num_avg,biz_date,product_id);
truncate table dm_eagle.eagle_credit_loan_approval_amount_rate_day;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_credit_loan_approval_amount_rate_day.tsv'             INTO TABLE dm_eagle.eagle_credit_loan_approval_amount_rate_day             FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,credit_approval_amount,loan_approval_amount,credit_loan_approval_amount_rate,biz_date,product_id);
truncate table dm_eagle.eagle_ret_msg_day;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_ret_msg_day.tsv'                                      INTO TABLE dm_eagle.eagle_ret_msg_day                                      FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,loan_terms,ret_msg_stage,ret_msg,ret_msg_num,ret_msg_rate,biz_date,product_id);
truncate table dm_eagle.eagle_title_info;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_title_info.tsv'                                       INTO TABLE dm_eagle.eagle_title_info                                       FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,product_id,project_init_amount,loan_month_map,biz_month_list,loan_terms_list,create_date);
truncate table dm_eagle.eagle_loan_amount_day;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_loan_amount_day.tsv'                                  INTO TABLE dm_eagle.eagle_loan_amount_day                                  FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,loan_terms,loan_amount,loan_amount_accumulate,biz_date,product_id);
truncate table dm_eagle.eagle_should_repay_repaid_amount_day;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_should_repay_repaid_amount_day.tsv'                   INTO TABLE dm_eagle.eagle_should_repay_repaid_amount_day                   FIELDS TERMINATED BY '\t' (capital_id,channel_id,loan_terms,should_repay_amount,repaid_amount,biz_date,project_id);
truncate table dm_eagle.eagle_asset_scale_principal_day;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_asset_scale_principal_day.tsv'                        INTO TABLE dm_eagle.eagle_asset_scale_principal_day                        FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,loan_terms,remain_principal,overdue_principal,unposted_principal,overdue_principal_rate,unposted_principal_rate,biz_date,product_id);
truncate table dm_eagle.eagle_asset_scale_repaid_day;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_asset_scale_repaid_day.tsv'                           INTO TABLE dm_eagle.eagle_asset_scale_repaid_day                           FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,loan_terms,paid_amount,paid_principal,paid_interest_penalty_svc_fee,paid_interest,paid_penalty,paid_svc_fee,paid_principal_rate,paid_interest_svc_fee_rate,repay_amount,repay_principal,repay_interest_penalty_svc_fee,repay_interest,repay_penalty,repay_svc_fee,repay_principal_rate,repay_interest_svc_fee_rate,biz_date,product_id);
truncate table dm_eagle.eagle_inflow_rate_first_term_day;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_inflow_rate_first_term_day.tsv'                       INTO TABLE dm_eagle.eagle_inflow_rate_first_term_day                       FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,loan_terms,dob,loan_num,overdue_loan_num,overdue_loan_inflow_rate,loan_active_date,loan_amount,overdue_principal,overdue_principal_inflow_rate,overdue_remain_principal,remain_principal_inflow_rate,biz_date,product_id);
truncate table dm_eagle.eagle_inflow_rate_day;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_inflow_rate_day.tsv'                                  INTO TABLE dm_eagle.eagle_inflow_rate_day                                  FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,loan_terms,dob,is_first,should_repay_loan_num,overdue_loan_num,overdue_loan_inflow_rate,loan_active_month,should_repay_date,remain_principal,should_repay_principal,overdue_principal,overdue_remain_principal,overdue_principal_inflow_rate,remain_principal_inflow_rate,biz_date,product_id);
truncate table dm_eagle.eagle_overdue_rate_month;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_overdue_rate_month.tsv'                               INTO TABLE dm_eagle.eagle_overdue_rate_month                               FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,loan_terms,dpd,mob,loan_month,loan_amount,loan_principal_deferred,overdue_principal,overdue_remain_principal,overdue_rate,overdue_principal_once,overdue_remain_principal_once,overdue_rate_once,biz_date,product_id);
truncate table dm_eagle.eagle_deferred_overdue_rate_full_month_product_day;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_deferred_overdue_rate_full_month_product_day.tsv'     INTO TABLE dm_eagle.eagle_deferred_overdue_rate_full_month_product_day     FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,dpd,loan_principal_deferred,overdue_principal,overdue_remain_principal,overdue_rate_overdue_principal,overdue_rate_remain_principal,biz_date,product_id);
truncate table dm_eagle.eagle_migration_rate_month;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.eagle_migration_rate_month.tsv'                             INTO TABLE dm_eagle.eagle_migration_rate_month                             FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,loan_terms,overdue_stage,mob,loan_month,loan_amount,remain_principal,biz_month,product_id);



truncate table dm_eagle_cps.eagle_title_info;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle_cps.eagle_title_info.tsv'                                   INTO TABLE dm_eagle_cps.eagle_title_info                                   FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,product_id,project_init_amount,loan_month_map,biz_month_list,loan_terms_list,create_date);
truncate table dm_eagle_cps.eagle_loan_amount_day;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle_cps.eagle_loan_amount_day.tsv'                              INTO TABLE dm_eagle_cps.eagle_loan_amount_day                              FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,loan_terms,loan_amount,loan_amount_accumulate,biz_date,product_id);
truncate table dm_eagle_cps.eagle_should_repay_repaid_amount_day;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle_cps.eagle_should_repay_repaid_amount_day.tsv'               INTO TABLE dm_eagle_cps.eagle_should_repay_repaid_amount_day               FIELDS TERMINATED BY '\t' (capital_id,channel_id,loan_terms,should_repay_amount,repaid_amount,biz_date,project_id);
truncate table dm_eagle_cps.eagle_asset_scale_principal_day;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle_cps.eagle_asset_scale_principal_day.tsv'                    INTO TABLE dm_eagle_cps.eagle_asset_scale_principal_day                    FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,loan_terms,remain_principal,overdue_principal,unposted_principal,overdue_principal_rate,unposted_principal_rate,biz_date,product_id);
truncate table dm_eagle_cps.eagle_asset_scale_repaid_day;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle_cps.eagle_asset_scale_repaid_day.tsv'                       INTO TABLE dm_eagle_cps.eagle_asset_scale_repaid_day                       FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,loan_terms,paid_amount,paid_principal,paid_interest_penalty_svc_fee,paid_interest,paid_penalty,paid_svc_fee,paid_principal_rate,paid_interest_svc_fee_rate,repay_amount,repay_principal,repay_interest_penalty_svc_fee,repay_interest,repay_penalty,repay_svc_fee,repay_principal_rate,repay_interest_svc_fee_rate,biz_date,product_id);
truncate table dm_eagle_cps.eagle_inflow_rate_first_term_day;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle_cps.eagle_inflow_rate_first_term_day.tsv'                   INTO TABLE dm_eagle_cps.eagle_inflow_rate_first_term_day                   FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,loan_terms,dob,loan_num,overdue_loan_num,overdue_loan_inflow_rate,loan_active_date,loan_amount,overdue_principal,overdue_principal_inflow_rate,overdue_remain_principal,remain_principal_inflow_rate,biz_date,product_id);
truncate table dm_eagle_cps.eagle_inflow_rate_day;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle_cps.eagle_inflow_rate_day.tsv'                              INTO TABLE dm_eagle_cps.eagle_inflow_rate_day                              FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,loan_terms,dob,is_first,should_repay_loan_num,overdue_loan_num,overdue_loan_inflow_rate,loan_active_month,should_repay_date,remain_principal,should_repay_principal,overdue_principal,overdue_remain_principal,overdue_principal_inflow_rate,remain_principal_inflow_rate,biz_date,product_id);
truncate table dm_eagle_cps.eagle_overdue_rate_month;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle_cps.eagle_overdue_rate_month.tsv'                           INTO TABLE dm_eagle_cps.eagle_overdue_rate_month                           FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,loan_terms,dpd,mob,loan_month,loan_amount,loan_principal_deferred,overdue_principal,overdue_remain_principal,overdue_rate,overdue_principal_once,overdue_remain_principal_once,overdue_rate_once,biz_date,product_id);
truncate table dm_eagle_cps.eagle_deferred_overdue_rate_full_month_product_day;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle_cps.eagle_deferred_overdue_rate_full_month_product_day.tsv' INTO TABLE dm_eagle_cps.eagle_deferred_overdue_rate_full_month_product_day FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,dpd,loan_principal_deferred,overdue_principal,overdue_remain_principal,overdue_rate_overdue_principal,overdue_rate_remain_principal,biz_date,product_id);
truncate table dm_eagle_cps.eagle_migration_rate_month;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle_cps.eagle_migration_rate_month.tsv'                         INTO TABLE dm_eagle_cps.eagle_migration_rate_month                         FIELDS TERMINATED BY '\t' (capital_id,channel_id,project_id,loan_terms,overdue_stage,mob,loan_month,loan_amount,remain_principal,biz_month,product_id);






truncate table abs_asset_information_bag;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\tsv\\dm_eagle.abs_asset_information_bag.tsv'                             INTO TABLE abs_asset_information_bag                                        FIELDS TERMINATED BY '\t' (project_id,asset_count,customer_count,remain_principal,remain_interest,customer_remain_principal_avg,remain_principal_max,remain_principal_min,remain_principal_avg,loan_principal_max,loan_principal_min,loan_principal_avg,loan_terms_max,loan_terms_min,loan_terms_avg,loan_terms_avg_weighted,remain_term_max,remain_term_min,remain_term_avg,remain_term_avg_weighted,repaid_term_max,repaid_term_min,repaid_term_avg,repaid_term_avg_weighted,aging_max,aging_min,aging_avg,aging_avg_weighted,remain_period_max,remain_period_min,remain_period_avg,remain_period_avg_weighted,interest_rate_max,interest_rate_min,interest_rate_avg,interest_rate_avg_weighted,age_max,age_min,age_avg,age_avg_weighted,income_year_max,income_year_min,income_year_avg,income_year_avg_weighted,income_debt_ratio_max,income_debt_ratio_min,income_debt_ratio_avg,income_debt_ratio_avg_weighted,pledged_asset_balance,pledged_asset_count,pledged_asset_balance_ratio,pledged_asset_count_ratio,pawn_value,pledged_asset_rate_avg_weighted,biz_date,bag_id);


truncate table t_investor_info;
LOAD DATA LOCAL INFILE 'D:\\Users\\ximing.wei\\Desktop\\技术中心\\数仓表结构\\data_shell\\file_import\\dim_new.dim_investor_info.tsv' INTO TABLE t_investor_info                                                  FIELDS TERMINATED BY '\t' (capital_id,capital_name,project_id,project_name,investor_id,investor_name,investment_funds,investment_start_date,investment_maturity_date,annualized_days,coupon_rate,Share_type,Share_proportion,coupon_formula,coupon_formula_effective_date,coupon_formula_expire_date,int_calc_rules,create_time,update_time);









































sed -n '
# /{"scoreLevel":0"ratingResult":"Yes","status":"1"}/p;
# /{"scoreLevel":1"ratingResult":"Yes","status":"1"}/p;
# /{"scoreLevel":2"ratingResult":"Yes","status":"1"}/p;
# /{"scoreLevel":3"ratingResult":"Yes","status":"1"}/p;
# /{"scoreLevel":4"ratingResult":"Yes","status":"1"}/p;
# /{"scoreLevel":5"ratingResult":"Yes","status":"1"}/p;

# s/{"scoreLevel":0"ratingResult":"Yes","status":"1"}/{"scoreLevel":0,"ratingResult":"Yes","status":"1"}/g;
# s/{"scoreLevel":1"ratingResult":"Yes","status":"1"}/{"scoreLevel":1,"ratingResult":"Yes","status":"1"}/g;
# s/{"scoreLevel":2"ratingResult":"Yes","status":"1"}/{"scoreLevel":2,"ratingResult":"Yes","status":"1"}/g;
# s/{"scoreLevel":3"ratingResult":"Yes","status":"1"}/{"scoreLevel":3,"ratingResult":"Yes","status":"1"}/g;
# s/{"scoreLevel":4"ratingResult":"Yes","status":"1"}/{"scoreLevel":4,"ratingResult":"Yes","status":"1"}/g;
# s/{"scoreLevel":5"ratingResult":"Yes","status":"1"}/{"scoreLevel":5,"ratingResult":"Yes","status":"1"}/g;

/{"scoreLevel":0,"ratingResult":"Yes","status":"1"}/p;
/{"scoreLevel":1,"ratingResult":"Yes","status":"1"}/p;
/{"scoreLevel":2,"ratingResult":"Yes","status":"1"}/p;
/{"scoreLevel":3,"ratingResult":"Yes","status":"1"}/p;
/{"scoreLevel":4,"ratingResult":"Yes","status":"1"}/p;
/{"scoreLevel":5,"ratingResult":"Yes","status":"1"}/p;
' data_file/t_wind_control_resp_log.tsv | wc -l

























db=dm_eagle
tb=abs_asset_information_cash_flow_bag_day
root_dir=/root/data_shell/file_export

p_opera(){
  p_num=${1:-5}
  pids=(${pids[@]:-} $!)
  while [[ ${#pids[@]} -ge $p_num ]]; do
    pids_old=(${pids[@]})
    pids=()
    for p in "${pids_old[@]}";do
      [[ -d "/proc/$p" ]] && pids=(${pids[@]} $p)
    done
    sleep 0.01
  done
}
wait_jobs(){
  for pid in $(jobs -p); do
    wait $pid
  done
}

for i in $(
  beeline -n hive -u jdbc:hive2://node47:10000 \
  --showHeader=false \
  --outputformat=tsv2 \
  -e "show partitions ${db}.${tb};" 2> /dev/null | grep -E '001801|001802' | grep -vE 'biz_date=2020-12.*'
); do
  {
    file_dir=${tb}/$i
    dir=$root_dir/$file_dir
    echo $root_dir $file_dir $dir
    mkdir -p $dir
    hdfs dfs -get -f /user/hive/warehouse/${db}.db/${file_dir}/* $dir
  } &
  p_opera
done
wait_jobs


sudo -u hive hdfs dfs -put -f /opt/${tb} /user/hive/warehouse/${db}.db




source /root/data_shell/lib/function.sh
load_dir=/root/data_shell
load_file=ods_new_s/risk_control
file_dir=$load_dir/$load_file
tree_list=($(tree --noreport -afi -F $file_dir | grep -Ev '/$|t_wind_control_resp_log'))

n=1
unset hqls
for files in ${tree_list[@]}; do
  [[ ! -f $files ]] && continue
  # echo $files ${files//$dir/}
  dir_arr=($(s_a ${files//$dir/}))
  db_tb=${dir_arr[0]}.${dir_arr[1]}
  # echo ${dir_arr[@]}
  case $((${#dir_arr[@]} - 3)) in
    (0)
      partition=''
      ;;
    (1)
      p_1_n=$(e_l_l ${dir_arr[2]}) p_1_v=$(e_l_r ${dir_arr[2]})
      partition="partition(${p_1_n}='${p_1_v}')"
      ;;
    (2)
      p_1_n=$(e_l_l ${dir_arr[2]}) p_1_v=$(e_l_r ${dir_arr[2]})
      p_1_n=$(e_l_l ${dir_arr[3]}) p_1_v=$(e_l_r ${dir_arr[3]})
      partition="partition(${p_1_n}='${p_1_v}',${p_2_n}='${p_2_v}')"
      ;;
    (3)
      p_1_n=$(e_l_l ${dir_arr[2]}) p_1_v=$(e_l_r ${dir_arr[2]})
      p_2_n=$(e_l_l ${dir_arr[3]}) p_2_v=$(e_l_r ${dir_arr[3]})
      p_3_n=$(e_l_l ${dir_arr[4]}) p_3_v=$(e_l_r ${dir_arr[4]})
      partition="partition(${p_1_n}='${p_1_v}',${p_2_n}='${p_2_v}',${p_3_n}='${p_3_v}')"
      ;;
  esac

  hqls+="load data local inpath '$files' into table ${db_tb} ${partition};
  "

  [[ $(($n % 200)) == 0 || $n == $((${#tree_list[@]} - 1)) ]] && {
    # echo "$hqls"
    sudo -u hive hive -e "$hqls"
    hqls='    '
    # break
  }
  # [[ $n == 5 ]] && break
  ((++n))
done

echo "$hqls"






tbls=(
  t_loancontractinfo      # 文件  一     借款合同信息表
  t_borrowerinfo          # 文件  二     主借款人信息表
  t_associatesinfo        # 文件  三     关联人信息表
  t_guarantycarinfo       # 文件  四     抵押物（车）信息表
  t_repaymentplan         # 文件  五     还款计划信息表
  t_repaymentplan_history # 文件  五     还款计划信息表 历史
  t_assettradeflow        # 文件  六     资产交易支付流水信息表
  t_actualrepayinfo       # 文件  七     实际还款信息表
  t_assetdealprocessinfo  # 文件  八     资产处置过程信息表
  t_assetaddtradeinfo     # 文件  九     资产补充交易信息表
  t_assetaccountcheck     # 文件  十     资产对账信息表
  t_projectaccountcheck   # 文件  十一   项目对账信息表
  t_enterpriseinfo        # 文件  十二   企业名称信息表
  t_guarantyhouseinfo     # 文件  十三   房抵押物信息表

  t_asset_wind_control_history
  t_wind_control_resp_log

  t_project
  t_related_assets
)

file_import_dir=/root/data_shell/file_import
hdfs_dir=/user/hive/warehouse/stage.db

for table in ${tbls[@]};do
  case $table in
    ( t_loancontractinfo      ) tbl_file=t_01_loancontractinfo      ;;
    ( t_borrowerinfo          ) tbl_file=t_02_borrowerinfo          ;;
    ( t_associatesinfo        ) tbl_file=t_03_associatesinfo        ;;
    ( t_guarantycarinfo       ) tbl_file=t_04_guarantycarinfo       ;;
    ( t_repaymentplan         ) tbl_file=t_05_repaymentplan         ;;
    ( t_repaymentplan_history ) tbl_file=t_05_repaymentplan_history ;;
    ( t_assettradeflow        ) tbl_file=t_06_assettradeflow        ;;
    ( t_actualrepayinfo       ) tbl_file=t_07_actualrepayinfo       ;;
    ( t_assetdealprocessinfo  ) tbl_file=t_08_assetdealprocessinfo  ;;
    ( t_assetaddtradeinfo     ) tbl_file=t_09_assetaddtradeinfo     ;;
    ( t_assetaccountcheck     ) tbl_file=t_10_assetaccountcheck     ;;
    ( t_projectaccountcheck   ) tbl_file=t_11_projectaccountcheck   ;;
    ( t_enterpriseinfo        ) tbl_file=t_12_enterpriseinfo        ;;
    ( t_guarantyhouseinfo     ) tbl_file=t_13_guarantyhouseinfo     ;;
    ( *                       ) tbl_file=$table                     ;;
  esac
  case $table in
    ( t_related_assets ) sql="select distinct project_id,related_project_id,related_status from t_related_assets;" ;;
    ( *                ) sql="select * from ${table};" ;;
  esac
  {
    file_name=$file_import_dir/${tbl_file}.tsv
    hdfs_name=$hdfs_dir/${tbl_file}
    mysql -P3306 -h'10.80.16.21' -uroot -p'EXYeaGVQZpsr@CR&' -Dabs-core -s -N -e "${sql}" > $file_name
    echo $file_name
    sudo -u hive hdfs dfs -put -f $file_name $hdfs_name
  } &
done






impala-shell -u hadoop -i 10.80.1.43:27001 -B --output_delimiter=',' -q "
  select * from dm_eagle.abs_asset_information_cash_flow_bag_day where biz_date = '2021-05-10';
  select * from dm_eagle.abs_asset_information_cash_flow_bag_day where biz_date = '2021-04-14' and project_id = 'WS0006200001';
" > aa.tsv



# dim_new.project_info
# -N Don't write column names in results.
# -s Be more silent. Print results with a tab as separator,each row on new line.
source lib/function.sh
aa=$(select_data "${aa:-"$(
  mysql -P3306 -h'10.80.16.21' -u'root' -p'EXYeaGVQZpsr@CR&' -D'abs-core' -e "
    select distinct
      project_name,
      project_stage,
      null as asset_side,
      null as fund_side,
      null as year,
      null as term,
      null as remarks,
      project_full_name,
      asset_type,
      project_type,
      mode,
      project_time,
      project_begin_date,
      project_end_date,
      asset_pool_type,
      public_offer,
      data_source,
      create_user,
      create_time,
      update_time,
      project_id
    from t_project
    where project_id not in ('PL202102010096')
  ;"
)"}")

beeline -n hadoop -u "jdbc:hive2://10.80.0.46:2181,10.80.0.255:2181,10.80.1.113:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" \
--nullemptystring=true --outputformat=tsv2 -e "
  select
    due_bill_no,
    overdue_days,
    loan_status,
    loan_status_cn,
    paid_out_type,
    paid_out_type_cn,
    product_id
  from ods_new_s.loan_info
  where 1 > 0
    and product_id in (
      '001801','001802','001803','001804',
      '001901','001902','001903','001904','001905','001906','001907',
      '002001','002002','002003','002004','002005','002006','002007',
      '002401','002402'
    )
    and ('2021-02-01' between s_d_date and date_sub(e_d_date,1))
    and overdue_days > 180
  order by loan_status,overdue_days
;" > aa.tsv



# 校验平台
mysql -P3306 -h'10.80.16.5' -u'root' -p'Xfx2018@)!*' -D'asset_status' -e "show create table t_enterprise_info;"

# 星云
mysql -P3306 -h'10.80.16.21' -u'root' -p'EXYeaGVQZpsr@CR&' -D'abs-core' -s -N -e "select distinct project_id from t_borrowerinfo limit 10;"
mysql -P3306 -h'10.83.16.15' -u'root' -p'Ws2018!07@' -D'abs-core' -s -N -e "select distinct project_id from t_borrowerinfo limit 10;"


# 看管ueagle库
mysql -P3306 -h'10.80.16.87' -u'root' -p'wH7Emvsrg&V5' -D'ueagle' -e "
LOAD DATA LOCAL INFILE '/root/data_shell/file_import/dim_new.dim_investor_info.tsv' INTO TABLE t_investor_info FIELDS TERMINATED BY '\t'
(
capital_id,capital_name,project_id,project_name,investor_id,investor_name,investment_funds,investment_start_date,investment_maturity_date,
annualized_days,coupon_rate,Share_type,Share_proportion,coupon_formula,coupon_formula_effective_date,coupon_formula_expire_date,int_calc_rules,
create_time,update_time
);
"



# 看管dm_eagle库
mysql -P3306 -h'10.80.16.87' -u'root' -p'wH7Emvsrg&V5' -D'dm_eagle' -e "

select
  *
  -- count(1) as cnt,
  -- biz_date,
  -- product_id
from
dm_eagle.
eagle_credit_loan_approval_amount_sum_day
where 1 > 0
  and product_id like 'vt_001801'
  and biz_date = '2021-01-02'
-- group by product_id
-- ,biz_date
order by product_id
-- ,biz_date
-- limit 1
;"








mv file_export/dm_eagle.eagle_should_repay_repaid_amount_day.tsv             file_export/dm_eagle.eagle_should_repay_repaid_amount_day_old.tsv
mv file_export/dm_eagle.eagle_should_repay_repaid_amount_day_hst.tsv         file_export/dm_eagle.eagle_should_repay_repaid_amount_day.tsv
mv file_export/dm_eagle.eagle_should_repay_repaid_amount_day_old.tsv         file_export/dm_eagle.eagle_should_repay_repaid_amount_day_hst.tsv

mv file_export/dm_eagle_cps.eagle_should_repay_repaid_amount_day.tsv         file_export/dm_eagle_cps.eagle_should_repay_repaid_amount_day_old.tsv
mv file_export/dm_eagle_cps.eagle_should_repay_repaid_amount_day_hst.tsv     file_export/dm_eagle_cps.eagle_should_repay_repaid_amount_day.tsv
mv file_export/dm_eagle_cps.eagle_should_repay_repaid_amount_day_old.tsv     file_export/dm_eagle_cps.eagle_should_repay_repaid_amount_day_hst.tsv


while [[ $(date +%s) -lt $(date -d '04:00:00' +%s) ]]; do
  date +'%F %T'
  sleep 0.5
done


sudo -u hadoop impala-shell -u hadoop -i 10.80.240.5:27001 -t 0 -B --output_delimiter=',' -q '
select
  guaranty.project_id          as "项目编号",
  guaranty.due_bill_no         as "借据号",
  guaranty.guaranty_code       as "抵押物编号",
  guaranty.pawn_value          as "评估价格",
  guaranty.car_sales_price     as "车辆销售价格",
  guaranty.car_new_price       as "新车指导价",
  guaranty.total_investment    as "投资总额",
  guaranty.purchase_tax_amouts as "购置税金额",
  dim_frame_num.dim_decrypt    as "车架号",
  dim_engine_num.dim_decrypt   as "发动机号",
  dim_license_num.dim_decrypt  as "车牌号码",
  guaranty.car_brand           as "车辆品牌",
  guaranty.ts                  as "时间戳",
  guaranty.create_time         as "创建时间",
  guaranty.update_time         as "更新时间"
from ods.guaranty_info_abs as guaranty
join (
  select
    project_id,
    due_bill_no
  from ods.guaranty_info_abs
  group by project_id,due_bill_no
  having count(due_bill_no) > 1
) as guaranty_info
on  guaranty.project_id  = guaranty_info.project_id
and guaranty.due_bill_no = guaranty_info.due_bill_no
left join (
  select
    dim_encrypt,
    dim_decrypt
  from dim.dim_encrypt_info
) as dim_frame_num
on guaranty.frame_num   = dim_frame_num.dim_encrypt
left join (
  select
    dim_encrypt,
    dim_decrypt
  from dim.dim_encrypt_info
) as dim_engine_num
on guaranty.engine_num  = dim_engine_num.dim_encrypt
left join (
  select
    dim_encrypt,
    dim_decrypt
  from dim.dim_encrypt_info
) as dim_license_num
on guaranty.license_num = dim_license_num.dim_encrypt
order by guaranty.project_id,guaranty.due_bill_no,guaranty.ts,guaranty.create_time
;' > aa.csv


$impala -q "select * from $app_name limit 1;"                                                                                                                                                 &>> $manage_log








db_tbs=(
  $(
    sudo -u hadoop \
    beeline -n hadoop \
    -u "jdbc:hive2://10.80.0.46:2181,10.80.0.255:2181,10.80.1.113:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" \
    --color=true \
    --hiveconf hive.resultset.use.unique.column.names=false \
    --showHeader=false \
    --outputformat=tsv2 \
    -e '
      show tables in dm_eagle
    ;' 2> /dev/null
  )
)



for tbl in ${db_tbs[@]}; do
  path=cosn://bigdata-center-prod-1253824322/user/hadoop/warehouse/dm_eagle.db/${tbl}

  files=(
    $(
      sudo -u hadoop hdfs dfs -ls $path | awk '{print $8}' | grep -E '\.hive' 2> /dev/null
    )
  )

  delete_files=''
  for file in ${files[@]}; do
    delete_files+="${file##*/}$([[ $file = ${files[$(( ${#files[@]} - 1 ))]} ]] && echo '' || echo ',')"
  done
  [[ -n $delete_files ]] && {
    delete_files=$path/'{'${delete_files}'}'
    echo $delete_files
    sudo -u hadoop hdfs dfs -rm -r $delete_files
  }
done









tbls=(
  dim.out_project_due_bill_no
  ods.out_customer_info
  ods.out_loan_lending
  ods.out_loan_info
  ods.out_repay_schedule
  ods.out_repay_detail
  ods.out_order_info
)


for tbl in ${tbls[@]}; do
  db_tb=(${tbl//./ })
  echo ${db_tb[@]}
  hdfs dfs -get -f hdfs://10.80.1.155:4007/user/hadoop/warehouse/${db_tb[0]}.db/${db_tb[1]} ./
done


for tbl in ${tbls[@]}; do
  db_tb=(${tbl//./ })
  echo "sudo -u hive hdfs dfs -put ./${db_tb[1]}/* /user/hive/warehouse/${db_tb[0]}.db/${db_tb[1]}"
  sudo -u hive hdfs dfs -put ./${db_tb[1]}/* /user/hive/warehouse/${db_tb[0]}.db/${db_tb[1]}
done






host=10.80.1.67
html=$(curl http://$host:27004/queries 2> /dev/null)
close_queries=$(echo $html | xmllint --html --xpath '//h3[2]/text()' - 2> /dev/null | sed '/^\s*$/d' | grep -Po '\K[\d]+')

[[ $close_queries != 0 ]] && {
  href_list=($(echo $html | xmllint --html --xpath '//table[2]/tr/td[a="Close"]/a/@href' - 2> /dev/null | grep -Po 'href[=" /]+\K[\w?=:]*'))

  echo "$close_queries 个程序需要关闭"
  for href in ${href_list[@]}; do
    echo "#---------------------------------------- $host  close_tab  Close  $href ----------------------------------------#"
    curl http://$host:27004/$href 2> /dev/null | xmllint --html --xpath '//pre/text()' - 2> /dev/null
    echo
  done
} || echo "$close_queries 没有需要关闭的程序"





host=10.80.1.67

host_list=(
  # 10.80.240.5
  # 10.80.240.7

  # 10.80.1.13
  # 10.80.1.43
  10.80.1.67
)


for host in ${host_list[@]}; do
  echo "#---------------------------------------- $host ----------------------------------------#"
  html=$(curl http://$host:27004/queries 2> /dev/null)

  # 先关闭
  close_queries=$(echo $html | xmllint --html --xpath '//h3[2]/text()' - 2> /dev/null | sed '/^\s*$/d' | grep -Po '\K[\d]+')

  [[ $close_queries != 0 ]] && {
    href_list=($(echo $html | xmllint --html --xpath '//table[2]/tr/td[a="Close"]/a/@href' - 2> /dev/null | grep -Po 'href[=" /]+\K[\w?=:]*'))

    for href in ${href_list[@]}; do
      echo "#---------------------------------------- $host  close_tab  Close  $href ----------------------------------------#"
      curl http://$host:27004/$href 2> /dev/null | xmllint --html --xpath '//pre/text()' - 2> /dev/null
      echo
    done
  } || echo '没有需要关闭的程序'


  # 再查看是否有程序在运行
  flight_queries=$(echo $html | xmllint --html --xpath '//h3[1]/text()' - 2> /dev/null | sed '/^\s*$/d' | grep -Po '\K[\d]+')

  [[ $flight_queries != 0 ]] && {
    flight_stat=$(echo $html | xmllint --html --xpath '//table[1]/tr/td[samp][3]/samp/text()' - 2> /dev/null)
    echo "#---------------------------------------- $host  flight_stat  stat  $flight_stat ----------------------------------------#"

    href_list=($(echo $html | xmllint --html --xpath '//table[1]/tr/td[a="Cancel"]/a/@href' - 2> /dev/null | grep -Po 'href[=" /]+\K[\w?=:]*'))

    for href in ${href_list[@]}; do
      echo "#---------------------------------------- $host  flight_stat  Cancel  $href ----------------------------------------#"
      # html=$(curl http://$host:27004/$href 2> /dev/null)
      [[ $flight_stat = 'CREATED' ]] && {
        echo $html | xmllint --html --xpath '//div[strong]/text()' - 2> /dev/null
      }
    done
  } || echo '没有在运行的程序'
done














host_list=(
  10.80.1.94

  10.80.1.155
  10.80.0.195

  10.80.1.13
  10.80.1.43
  10.80.1.67

  10.80.240.16
  10.80.240.27
  10.80.240.34
  10.80.240.35
  10.80.240.70
  10.80.240.72
  10.80.240.79
  10.80.240.93
  10.80.240.128
  10.80.240.134
)

for host in ${host_list[@]}; do
  # echo $host
  # echo '#----------------------- scp star -----------------------#'
  # scp root@10.80.1.94:/data/data_shell/hive-exec-3.1.1.jar root@$host:/usr/local/service/hive/lib/hive-exec-3.1.1.jar.tmp
  # echo '#----------------------- scp end -----------------------#'
  echo '#----------------------- ssh star -----------------------#'
  ssh root@$host "
    # echo '#----------------------- '$host' mv 1 star -----------------------#'
    # mv -f /usr/local/service/hive/lib/hive-exec-3.1.1.jar /usr/local/service/hive/lib/hive-exec-3.1.1.jar-bak2
    # echo '#----------------------- '$host' mv 1 end -----------------------#'
    # echo '#----------------------- '$host' mv 2 star -----------------------#'
    # mv -f /usr/local/service/hive/lib/hive-exec-3.1.1.jar.tmp /usr/local/service/hive/lib/hive-exec-3.1.1.jar
    # echo '#----------------------- '$host' mv 2 end -----------------------#'
    # echo '#----------------------- '$host' chown star -----------------------#'
    # chown hadoop:hadoop /usr/local/service/hive/lib/hive-exec-3.1.1.jar
    # echo '#----------------------- '$host' chown end -----------------------#'
    echo $host
    ls -lh /usr/local/service/hive/lib/hive-exec-3.1.1.jar*
  "
  echo '#----------------------- ssh end -----------------------#'
done



#!/bin/bash

. /etc/profile
. ~/.bash_profile

base_dir=$(cd `dirname "${BASH_SOURCE[0]}"`/..;pwd)


ip=$(ifconfig | grep -Po 'inet[ ]\K[^ ]+' | grep -v '127')

case $ip in
  (10.80.* ) is_test=n hive_host=node233:10000 impala_host=node168 m_host='' m_port='' m_user='' m_pass='' m_db='' ;;
  (10.83.* ) is_test=y hive_host=node123:10000 impala_host=node47  m_host='10.83.16.32' m_port='3306' m_user='bgp_admin' m_pass='3Mt%JjE#WJIt' m_db='configuration' ;;
esac

user=hive

# 目录
lib=$base_dir/lib
log=$base_dir/log

bin=$base_dir/bin
bin_abn=$base_dir/bin_abn
bin_abs=$base_dir/bin_abs

conf_env=$base_dir/conf_env
dm_eagle_dm_eagle=$([[ ${is_test} == 'n' ]] && echo $conf_env/prod_dm_eagle_dm_eagle.mysql_conf || echo $conf_env/test_dm_eagle_dm_eagle.mysql_conf)
dm_eagle_dm_eagle_cps=$([[ ${is_test} == 'n' ]] && echo $conf_env/prod_dm_eagle_dm_eagle_cps.mysql_conf || echo $conf_env/test_dm_eagle_dm_eagle_cps.mysql_conf)
dm_eagle_uabs_core=$([[ ${is_test} == 'n' ]] && echo $conf_env/prod_dm_eagle_uabs_core.mysql_conf || echo $conf_env/test_dm_eagle_uabs_core.mysql_conf)


file_hql=$base_dir/file_hql
reload_hql=$base_dir/data_move_ddht_hql
dim_new_hql=$file_hql/dim_new.query
ods_new_s_hql=$file_hql/ods_new_s.query
ods_new_s_cloud_hql=$file_hql/ods_new_s_cloud.query
dw_new_hql=$file_hql/dw_new.query
dm_eagle_hql=$file_hql/dm_eagle.query
asset_report_hql=$file_hql/asset_report.query
dm_operation_hql=$file_hql/dm_operation.query

file_check=$base_dir/file_check
file_export=$base_dir/file_export
file_import=$base_dir/file_import
file_repaired=$base_dir/file_repaired

param_dir=$base_dir/param_beeline


# 执行命令基础
python=/usr/local/bin/python3
mail="$python $bin/send_mail.py"

beeline="beeline -n ${user} -u 'jdbc:hive2://$hive_host'"
impala="impala-shell -u ${user} -i $impala_host:21000"


mysql="mysql -P${m_port} -h${m_host} -u${m_user} -p${m_pass} -D${m_db}"


hdfs="sudo -u ${user} hdfs dfs"


data_manage=$bin/data_manage.sh
data_check=$bin/data_check.sh
data_check_all=$bin/data_check_all.sh



# 邮箱
conf_mail=$base_dir/conf_mail

funds=$conf_mail/data_receives_mail_funds.config
pm_rd=$conf_mail/data_receives_mail_pm_rd.config
rd=$conf_mail/data_receives_mail_rd.config

guochao=$conf_mail/data_receives_mail_guochao.config
liuhuan=$conf_mail/data_receives_mail_liuhuan.config
ximing=$conf_mail/data_receives_mail_ximing.config
yuheng=$conf_mail/data_receives_mail_yuheng.config

wh=$conf_mail/data_receives_mail_weihuang.config
wgt=$conf_mail/data_receives_mail_wgt.config
wg=$conf_mail/data_receives_mail_wg.config
wy=$conf_mail/data_receives_mail_wy.config


# 变量设置
declare -A tables

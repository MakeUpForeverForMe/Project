#!/bin/bash

. /etc/profile
. ~/.bash_profile

base_dir=$(cd `dirname "${BASH_SOURCE[0]}"`/..;pwd)


ip=$(ifconfig | grep -Po 'inet[ ]\K[^ ]+' | grep -v '127')

case $ip in
  (10.80.* )
    is_test=n
    hive_host='10.80.0.46:2181,10.80.0.255:2181,10.80.1.113:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2'
    impala_host=10.80.3.61:21050
    impala_clb='--protoco=hs2'
    ;;
  (10.83.* )
    is_test=y
    hive_host=node123:10000
    impala_host=node47
    ;;
esac


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
reload_hql=$file_hql/ods_reload.query
dim_new_hql=$file_hql/dim.query
ods_new_s_hql=$file_hql/ods.query
ods_cloud_hql=$file_hql/ods_cloud.query
dw_new_hql=$file_hql/dw.query
dm_eagle_hql=$file_hql/dm_eagle.query
asset_report_hql=$file_hql/asset_report.query
dm_operation_hql=$file_hql/dm_operation.query
eagle_hql=$file_hql/eagle.query
file_check=$base_dir/file_check
file_export=$base_dir/file_export
file_import=$base_dir/file_import
file_repaired=$base_dir/file_repaired

param_dir=$base_dir/param_beeline


# 执行命令基础
python=/usr/bin/python3
mail="$python $bin/send_mail.py"

export HADOOP_CLIENT_OPTS="-Djline.terminal=jline.UnsupportedTerminal"

beeline="beeline -n hadoop -u 'jdbc:hive2://$hive_host'"
impala="impala-shell -u hadoop -i $impala_host ${impala_clb}"

mysql_cmd="mysql_fun"

hdfs='sudo -u hadoop hdfs dfs'
hdfs_root_dir=cosn://bigdata-center-prod-1253824322

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

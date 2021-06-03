#!/usr/bin/env bash

. /etc/profile
. ~/.bash_profile
export LANG=zh_CN.UTF-8
export HADOOP_CLIENT_OPTS="-Djline.terminal=jline.UnsupportedTerminal"

trap 'fatal 脚本 $(basename "${BASH_SOURCE[0]}") 出现错误了，在第 $LINENO 行' ERR

case $(ifconfig | grep -Po 'inet[ ]\K[^ ]+' | grep -v '127') in
  (10.80.* )
    is_test=n
    hive_host='10.80.0.46:2181,10.80.0.255:2181,10.80.1.113:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2'
    # impala_host=10.80.2.51:21050 # 两台 Task
    impala_host=10.80.3.61:21050 # 三台 Core
    impala_clb='--protoco=hs2'
    abs_call_back_addr=https://uabs-server.weshareholdings.com # 10.80.1.25
    hdfs_uri=hdfs://
    cos_uri=cosn://bigdata-center-prod-1253824322
    ftp_host=10.90.0.5
    ftp_user=
    ftp_pass=
    ftp_dir=
    ;;
  (10.83.* )
    is_test=y
    hive_host=10.83.1.43:7001
    impala_host=10.83.1.43:27001 # 或 10.83.1.121:27001
    abs_call_back_addr=http://10.83.0.69:8210
    hdfs_uri=hdfs://
    cos_uri=cosn://bigdatacenter-sit-1253824322
    ftp_host=10.83.0.32
    ftp_user=it-dev
    ftp_pass=058417gv
    ftp_dir=/bigcenter
    ;;
esac


# 系统命令
# Python 系统命令
python=/usr/bin/python3
# Hive 客户端 Beeline 命令
beeline="beeline -n hadoop -u 'jdbc:hive2://$hive_host'"
# Impala 命令
impala="impala-shell -u hadoop -i $impala_host ${impala_clb}"
# MySQL 命令
mysql_cmd="mysql_fun"
# HDFS 命令
hdfs='hdfs dfs'
# HDFS URI
hdfs_uri=$hdfs_uri
# COS URI
cos_uri=$cos_uri
# warehouse
warehouse=/user/hadoop/warehouse



# 根目录
base_dir=$(cd `dirname "${BASH_SOURCE[0]}"`/..;pwd)

# 一级目录
# 引用资源目录
lib=$base_dir/lib
# 日志目录
log=$base_dir/log

# 基础命令目录
bin=$base_dir/bin
# 看管命令目录
bin_abn=$base_dir/bin_abn
# 星云命令目录
bin_abs=$base_dir/bin_abs

# 基础配置目录
conf_env=$base_dir/conf_env
# 邮箱配置目录
conf_mail=$base_dir/conf_mail

# 数仓执行脚本目录
file_hql=$base_dir/file_hql
# 校验执行脚本目录
file_check=$base_dir/file_check
# 修数执行脚本目录
file_repaired=$base_dir/file_repaired

# 导入文件目录
file_import=$base_dir/file_import
# 导出文件目录
file_export=$base_dir/file_export

# Hive 配置文件目录
param_dir=$base_dir/param_beeline


# 基础命令
# 邮箱命令
mail="$python $bin/send_mail.py"
# 执行脚本命令
data_manage=$bin/data_manage.sh
# 独立校验脚本命令
data_check=$bin/data_check.sh
# 联合校验脚本命令
data_check_all=$bin/data_check_all.sh


# MySQL 配置文件
# 看管 MySQL 配置文件
dm_eagle_dm_eagle=$(    [[ ${is_test} == 'n' ]] && echo $conf_env/prod_dm_eagle_dm_eagle.mysql_conf     || echo $conf_env/test_dm_eagle_dm_eagle.mysql_conf)
dm_eagle_dm_eagle_cps=$([[ ${is_test} == 'n' ]] && echo $conf_env/prod_dm_eagle_dm_eagle_cps.mysql_conf || echo $conf_env/test_dm_eagle_dm_eagle_cps.mysql_conf)
# 星云 MySQL 配置文件
dm_eagle_uabs_core=$(   [[ ${is_test} == 'n' ]] && echo $conf_env/prod_dm_eagle_uabs_core.mysql_conf    || echo $conf_env/test_dm_eagle_uabs_core.mysql_conf)


# 邮箱 配置文件
# 邮箱 配置文件 整组级别
funds=$conf_mail/data_receives_mail_funds.config
pm_rd=$conf_mail/data_receives_mail_pm_rd.config
rd=$conf_mail/data_receives_mail_rd.config

# 邮箱 配置文件 组合级别
wh=$conf_mail/data_receives_mail_weihuang.config
wgt=$conf_mail/data_receives_mail_wgt.config
wg=$conf_mail/data_receives_mail_wg.config
wy=$conf_mail/data_receives_mail_wy.config

# 邮箱 配置文件 个人级别
guochao=$conf_mail/data_receives_mail_guochao.config
liuhuan=$conf_mail/data_receives_mail_liuhuan.config
ximing=$conf_mail/data_receives_mail_ximing.config
yuheng=$conf_mail/data_receives_mail_yuheng.config




# 二级目录
# dim 目录
dim_new_hql=$file_hql/dim.query

# ods 目录
ods_new_s_hql=$file_hql/ods.query
reload_hql=$file_hql/ods_reload.query
ods_cloud_hql=$file_hql/ods_cloud.query
ods_newcore_baidu_hql=$file_hql/ods_newcore.query/baidu
ods_newcore_yunxin_hql=$file_hql/ods_newcore.query/yunxin

# dw 目录
dw_new_hql=$file_hql/dw.query
# dm 目录
dm_eagle_hql=$file_hql/dm_eagle.query
dm_operation_hql=$file_hql/dm_operation.query

eagle_hql=$file_hql/eagle.query
asset_report_hql=$file_hql/asset_report.query


# 变量设置
declare -A tables

#!/bin/bash -e

. ${data_income_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh

# -2 day 是取备份日期的前两天，即数据的最新日期
bak_date=$(date -d '-2 day' +'%Y%m%d')
# 删除一个月前的本分分区
del_date=$(date -d '-1 month -2 day' +'%Y%m%d')

log=$log/data_bak.$bak_date.log

#echo \
$beeline --hiveconf mapred.job.name=table_bak-$bak_date --hivevar bak_date=$bak_date --hivevar del_date=$del_date -e '
set hive.execution.engine=mr;

set mapreduce.map.memory.mb=6144;
set mapreduce.reduce.memory.mb=6144;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=30000;

-- 代偿前
set hivevar:db_suffix=;

-- 借据
create table if not exists `ods_new_s${db_suffix}.loan_info_bak_${bak_date}` like `ods_new_s${db_suffix}.loan_info`;
insert overwrite table ods_new_s${db_suffix}.loan_info_bak_${bak_date} partition(is_settled,product_id) select * from ods_new_s${db_suffix}.loan_info;
drop table if exists `ods_new_s${db_suffix}.loan_info_bak_${del_date}`;

-- 应还
create table if not exists `ods_new_s${db_suffix}.repay_schedule_bak_${bak_date}` like `ods_new_s${db_suffix}.repay_schedule`;
insert overwrite table ods_new_s${db_suffix}.repay_schedule_bak_${bak_date} partition(is_settled,product_id) select * from ods_new_s${db_suffix}.repay_schedule;
drop table if exists `ods_new_s${db_suffix}.repay_schedule_bak_${del_date}`;

-- 实还
create table if not exists `ods_new_s${db_suffix}.repay_detail_bak_${bak_date}` like `ods_new_s${db_suffix}.repay_detail`;
insert overwrite table ods_new_s${db_suffix}.repay_detail_bak_${bak_date} partition(biz_date,product_id) select * from ods_new_s${db_suffix}.repay_detail;
drop table if exists `ods_new_s${db_suffix}.repay_detail_bak_${del_date}`;

-- 流水
create table if not exists `ods_new_s${db_suffix}.order_info_bak_${bak_date}` like `ods_new_s${db_suffix}.order_info`;
insert overwrite table ods_new_s${db_suffix}.order_info_bak_${bak_date} partition(biz_date,product_id) select * from ods_new_s${db_suffix}.order_info;
drop table if exists `ods_new_s${db_suffix}.order_info_bak_${del_date}`;




-- 代偿后
set hivevar:db_suffix=_cps;

-- 借据
create table if not exists `ods_new_s${db_suffix}.loan_info_bak_${bak_date}` like `ods_new_s${db_suffix}.loan_info`;
insert overwrite table ods_new_s${db_suffix}.loan_info_bak_${bak_date} partition(is_settled,product_id) select * from ods_new_s${db_suffix}.loan_info;
drop table if exists `ods_new_s${db_suffix}.loan_info_bak_${del_date}`;

-- 应还
create table if not exists `ods_new_s${db_suffix}.repay_schedule_bak_${bak_date}` like `ods_new_s${db_suffix}.repay_schedule`;
insert overwrite table ods_new_s${db_suffix}.repay_schedule_bak_${bak_date} partition(is_settled,product_id) select * from ods_new_s${db_suffix}.repay_schedule;
drop table if exists `ods_new_s${db_suffix}.repay_schedule_bak_${del_date}`;

-- 实还
create table if not exists `ods_new_s${db_suffix}.repay_detail_bak_${bak_date}` like `ods_new_s${db_suffix}.repay_detail`;
insert overwrite table ods_new_s${db_suffix}.repay_detail_bak_${bak_date} partition(biz_date,product_id) select * from ods_new_s${db_suffix}.repay_detail;
drop table if exists `ods_new_s${db_suffix}.repay_detail_bak_${del_date}`;

-- 流水
create table if not exists `ods_new_s${db_suffix}.order_info_bak_${bak_date}` like `ods_new_s${db_suffix}.order_info`;
insert overwrite table ods_new_s${db_suffix}.order_info_bak_${bak_date} partition(biz_date,product_id) select * from ods_new_s${db_suffix}.order_info;
drop table if exists `ods_new_s${db_suffix}.order_info_bak_${del_date}`;
' &> $log

[[ $? != 0 ]] && {
$mail $wh '数据 4.0 备份任务失败' "请检查！
$(cat $log)
"
}



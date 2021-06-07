#!/bin/bash

. ${data_ods_new_s_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh
s_date=$1
e_date=$2
base_file_name=$(basename "${BASH_SOURCE[0]}")

[[ -z $s_date || -z $e_date ]] && {
  echo "$(date +'%F %T') ${base_file_name} 请输入 开始和结束日期 参数！"
  exit 1
}

log=$log/${base_file_name}.${e_date}.log

echo -e "${date_s_aa:=$(date +'%F %T')} 资产 cash predict  开始 当前脚本进程ID为：$(pid)\n" &>> $log

##进件部分数据
sh $data_manage -s ${e_date} -e ${e_date} -f $eagle_hql/eagle.one_million_random_customer_reqdata_lx.hql  -a $rd &
##借据数据
sh $data_manage -s ${e_date} -e ${e_date} -f $eagle_hql/eagle.one_million_random_loan_data_day_lx.hql  -a $rd &
## 结清部分统计数据
sh $data_manage -s ${e_date} -e ${e_date} -f $eagle_hql/eagle.settle_month_paid.hql  -a $rd  &


wait_jobs
## 模型入参数据
sh $data_manage -s ${e_date} -e ${e_date} -f $eagle_hql/eagle.one_million_random_risk_data_lx.hql  -a $rd
## 提交saprk 任务脚本

sudo -u hadoop /usr/bin/sh /data/pmml/start.sh 2021-04-30  WS0006200001 & >> /data/pmml/pmml.log &








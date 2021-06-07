#!/bin/bash
source /etc/profile
export LANG=zh_CN.UTF-8
export PYTHON_EGG_CACHE=/tmp/.python-eggs

d_date=`mysql -h10.80.16.75 -P3306 -ubgp_admin -p'U3$AHfp*a8M&' -e "select * from data_check.funds_flow_date_conf;" | grep -v "business_date" | awk -F " " '{print $2}'`
HOSTNAME=10.80.16.87
PORT=3306
USERNAME=root
PASSWORD='wH7Emvsrg&V5'
DATABASE=dm_eagle

##测试
#d_date=`mysql -u bgp_admin -h10.83.16.32 -P3306 -p3Mt%JjE#WJIt -e "select * from data_check.funds_flow_date_conf;" | grep -v "business_date" | awk -F " " '{print $2}'`
#HOSTNAME=10.83.16.43
#PORT=3306
#USERNAME=root
#PASSWORD='zU!ykpx3EG)$$1e6'
#DATABASE=dm_eagle

delete_data() {
    echo "删除数据"
    sqoop eval \
    --connect jdbc:mysql://${HOSTNAME}:${PORT}/${DATABASE}?useSSL=false \
    --query "delete from $1 where biz_date='${d_date}'" \
    --username "'${USERNAME}'" \
    --password ${PASSWORD}
}

export_data() {
   echo "导出数据"
    sqoop export \
    -D mapreduce.map.memory.mb=4096 \
    -D mapreduce.reduce.memory.mb=4096 \
    --connect jdbc:mysql://${HOSTNAME}:${PORT}/${DATABASE}?useSSL=false \
    --username "'${USERNAME}'" \
    --password ${PASSWORD} \
    --table $1 \
    --hcatalog-database $2 \
    --hcatalog-table $3 \
    --hcatalog-partition-keys biz_date \
    --hcatalog-partition-values $4 \
    --num-mappers 1
}

case $1 in
  "eagle_funds")
     delete_data "eagle_funds"
     export_data "eagle_funds"         "dm_eagle" "eagle_funds" "${d_date}"
;;
  "eagle_acct_cost")
     delete_data "eagle_acct_cost"
     export_data "eagle_acct_cost"     "dm_eagle" "eagle_acct_cost" "${d_date}"
;;
  "eagle_unreach_funds")
     delete_data "eagle_unreach_funds"
     export_data "eagle_unreach_funds" "dm_eagle" "eagle_unreach_funds" "${d_date}"
;;
  "eagle_repayment_detail")
     delete_data "eagle_repayment_detail"
     export_data "eagle_repayment_detail" "dm_eagle" "eagle_repayment_detail" "${d_date}"
;;
  "all")
echo "all"
     delete_data "eagle_funds"
     export_data "eagle_funds"         "dm_eagle" "eagle_funds" "${d_date}"

     delete_data "eagle_acct_cost"
     export_data "eagle_acct_cost"     "dm_eagle" "eagle_acct_cost" "${d_date}"

     delete_data "eagle_unreach_funds"
     export_data "eagle_unreach_funds" "dm_eagle" "eagle_unreach_funds" "${d_date}"

     delete_data "eagle_repayment_detail"
     export_data "eagle_repayment_detail" "dm_eagle" "eagle_repayment_detail" "${d_date}"
;;
esac

# 调用看管接口，刷新资金页面日期

#测试
#curl 10.83.0.69:8041/ueagle/console/fix/resetLastTime
curl 10.80.1.92:8041/ueagle/console/fix/resetLastTime

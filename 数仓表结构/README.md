# **WeShare data_warehouse**

此文档为数据仓库4.0的说明。

## **目录结构**

asset_package          -> 星云数据读取（spark） 

data_shell             -> 数据仓库脚本

HiveUDF                -> hive udf工程

monitor                -> flink 任务监控（java）

reload_ods_new_s.query -> 滴滴15天重跑任务

数仓 表设计            -> 数据仓库文档

##  **数据仓库规范说明**

>表操作
>>1、开发新的流程或者优化现有的表时，以sit_开头命名副表。多个时，以操作人名为后缀  
>>2、重新刷数据或者修数重跑时，以his_开头命名副表。多个时，以操作人名为后缀  
>>3、删除某张表或表中字段时，以del_开头命名副表。多个时，以操作人名为后缀  
>>4、表删除，可以挪到tmp_drop_tables库，如下  
>>> `ALTER TABLE user_order_info_lx_impala                    RENAME TO tmp_drop_tables.user_order_info_lx_impala;`
    
##  **生产环境跑批汇总**

###  node233  
root:
``` bash
 #maxwell 服務告警
 0 */1 * * * python /opt/flink/python_warning/max_well_watch.py  >> /opt/flink/python_warning/log/max_well_watch.log 2>&1
 #运营平台跑批
 #0 8 * * * sh /opt/operation_platform/hexin/get_data.sh >> /opt/operation_platform/hexin/log_$(date +\%F).log
 #乐信运营跑批
 #30 23 * * * sh /opt/operation_platform/hexin/lx_get_data.sh >> /opt/operation_platform/hexin/lx_log_$(date +\%F).log
 # 数仓4.0定时任务
 00 21 * * * /usr/bin/sh /root/data_shell/start.sh $(date -d '-1 day' +\%F) $(date -d '-1 day' +\%F)
 # 每月3号、17号八点备份 业务日期为1号、15号的分区数据并删除上个月的分区
 00 08 3,17 * * /usr/bin/sh /root/data_shell/bin/data_bakup.sh &> /root/data_shell/log/bakup.log
 # 星云
 00 00 * * * /usr/bin/sh /root/data_shell/tmp.sh
```  
###  node47  
root:
```bash
#运营平台跑批
0 8 * * * sh /opt/operation_platform/hexin/get_data.sh >> /opt/operation_platform/hexin/log_$(date +\%F).log
#乐信运营跑批
30 23 * * * sh /opt/operation_platform/hexin/lx_get_data.sh >> /opt/operation_platform/hexin/lx_log_$(date +\%F).log
#乐信运营平台解压缩文件删除
0 10 * * * sh /opt/operation_platform/hexin/delete_more_days.sh /opt/operation_platform/hexin/unzip/heZi 2 unzip_
0 10 * * * sh /opt/operation_platform/hexin/delete_more_days.sh /opt/operation_platform/hexin/unzip/huiTong 2 unzip_
0 10 * * * sh /opt/operation_platform/hexin/delete_more_days.sh /opt/operation_platform/hexin/unzip/huiTong 2 lxgmunzip_
# sas滴滴
0 * * * * sh /opt/operation_platform/didi/didi_six_tables_per_30minutes_execute.sh
0 * * * * sh /opt/operation_platform/didi/external_10_tables_per_30minutes_execute.sh
0 * * * * sh /opt/operation_platform/didi/didi_add_tables/start.sh
# 数仓4.0定时任务
#30 20 * * * /usr/bin/sh /root/data_shell/data_start.sh $(date -d '-1 day' +\%F) $(date -d '-1 day' +\%F) &> /root/data_shell/data_log/data_log.log.$(date -d '-1 day' +\%F)
# 每月3号、17号八点备份 业务日期为1号、15号的分区数据并删除上个月的分区
#00 08 3,17 * * /usr/bin/sh /root/data_shell/data_bak.sh &> /root/data_shell/data_log/data_log.bakup.log
# 舆情同步dm_eagel
0 12,17 * * * sh /root/risk_news/cron_batch_run.sh >> /root/risk_news/batch_run_log_$(date +\%Y-\%m).log
30 8 * * * sh /root/risk_news/cron_batch_run.sh >> /root/risk_news/batch_run_log_$(date +\%Y-\%m).log
```  
admin:  
```bash
#####乐信资产服务报告
35 23 * * * sh /home/admin/lx_asset_report/start.sh
####4.0乐信资产服务报告
00 10 * * * sh /home/admin/lx_asset_report_by4/start.sh
####乐信服务报告逾期资产部分
#35 23 * * * sh /home/admin/asset_report/lx_temp_dm/dm_lx_asset_statistics_sql.hql >> /home/admin/asset_report/log/dm_lx_asset_statistics_sql$(date -I).log 2>&1
#35 23 * * * sh /home/admin/asset_report/lx_temp_dm/dm_lx_asset_summary_statistics_sql.hql >> /home/admin/asset_report/log/dm_lx_asset_summary_statistics_sql.log 2>&1
#max-well 服务监控
#0 */1 * * * python /home/admin/python_warning/max_well_watch.py  >> /home/admin/python_warning/log/max_well_watch.log 2>&1
#0 1 * * * sh /home/admin/python_warning/watch_data/operation_dm_01.sh >> /home/admin/python_warning/log/operation_dm_01.log 2>&1
#0 10 * * * sh /home/admin/python_warning/watch_data/operation_dm_10.sh >> /home/admin/python_warning/log/operation_dm_10.log 2>&1
```  

###  node148
admin:
```bash
30 03 * * * /home/admin/hyn-tools/eagleDataDraw-start.sh
#00 23 * * * sh /home/admin/hyn-tools/start.sh  >>/home/admin/logs/$(date +\%F)-add-y.log 2>&1
#00 2 * * * sh /home/admin/hyn-tools/dc-start.sh  >>/home/admin/logs/$(date +\%F)-add-y.dc-log 2>&1
#45 23 * * * sh /home/admin/hyn-tools/dis-start.sh  >>/home/admin/logs/$(date +\%F)-add-y.dis-log 2>&1
```  

###  node172
root:
```bash
# 可可的盒子新增表
00 4 * * * /usr/bin/sh /root/t1/t1_iboxchain/t1_iboxchain_manage.sh >> /root/t1/t1_iboxchain/t1_iboxchain_log_$(date +\%F).log
# 滴水贷和凤金项目
00 7 * * * /usr/bin/sh /root/t1/t1_drip_loan/t1_drip_loan_manage.sh >> /root/t1/t1_drip_loan/t1_drip_loan_log_$(date +\%F).log
00 9 * * * /usr/bin/sh /root/t1/t1_drip_loan/t1_ht_drip_loan_manage.sh >> /root/t1/t1_drip_loan/t1_drip_loan_ht_log_$(date +\%F).log
30 10 * * * nohup  sh  /home/admin/permission/saas_execute.sh > /dev/null 2>&1 &
#递延逾期率
00 10 * * *  hive  -f  /root/overdue_rate/overdue_rate.hql &>> /root/overdue_rate/overdue_rate_log_$(date +\%F).log
#浦发eagle_temp的数据insert进eagle
30 8 * * * beeline -u jdbc:hive2://node47:10000 -n admin -f /root/crontab_task/pf_eagle_temp_to_eagle/pf_eagle_temp_to_eagle.hql  &>> /root/crontab_task/pf_eagle_temp_to_eagle/pf_eagle_temp_to_eagle_log__$(date +\%F).log
*/30 * * * * /usr/local/qcloud/YunJing/YDCrontab.sh > /dev/null 2>&1 &

```  



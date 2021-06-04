[TOC]
# 一、data_shell 说明
## 1、data_shell 目录说明
```md
data_shell                                            根目录
├── bin                                               ├── 基础命令目录
│   ├── create_table.py                               │   ├── 自动建表脚本
│   ├── data_bakup.sh                                 │   ├── 备份脚本
│   ├── data_biz_conf.sh                              │   ├── biz_conf 表同步到 MySQL 脚本
│   ├── data_check_all.sh                             │   ├── 校验脚本
│   ├── data_check.sh                                 │   ├── 基础校验脚本
│   ├── data_delete_file.sh                           │   ├── 删除文件脚本
│   ├── data_export.sh                                │   ├── 导出数据脚本
│   ├── data_manage.sh                                │   ├── 基础执行脚本
│   ├── data_tail.sh                                  │   ├── 过滤查看文件脚本
│   ├── ftp_udf.sh                                    │   ├── 从 ftp 自动拉取 udf 函数 jar 包，并放到 cos 的脚本
│   ├── HiveMetastoreToExcel.py                       │   ├── 根据 Hive 元数据生成 Excel 脚本
│   ├── py_server.py                                  │   ├── Python 服务脚本
│   ├── send_mail.py                                  │   ├── 发送邮件脚本
│   ├── send_robot.py                                 │   ├── 发送机器人脚本
│   └── tmp_tables_drop.sh                            │   └──
├── bin_abn                                           ├── abn 项目命令目录（主要为 etl 流程）
│   ├── asset_funds                                   │   ├── asset_funds
│   │   ├── dm_eagle.eagle_asset_funds.sh             │   │   ├── dm_eagle.eagle_asset_funds.sh
│   │   ├── dm_eagle.eagle_funds_sqoop.sh             │   │   ├── dm_eagle.eagle_funds_sqoop.sh
│   │   ├── dw.dw_transaction_blend_record.sh         │   │   ├── dw.dw_transaction_blend_record.sh
│   │   └── offline_data_syn.sh                       │   │   └── offline_data_syn.sh
│   ├── baidu_ods.sh                                  │   ├── baidu_ods.sh
│   ├── data_asset.sh                                 │   ├── data_asset.sh
│   ├── data_dw_dm.sh                                 │   ├── dw、dm 及抽数到 MySQL 的 etl 脚本
│   ├── data_ealge_funds.sh                           │   ├── 资金页面相关流程脚本
│   ├── data_income.sh                                │   ├── 进件 etl 脚本
│   ├── data_ods_new_s.sh                             │   ├── ods_new_s 脚本
│   ├── data_risk_control_report.sh                   │   ├── data_risk_control_report.sh
│   ├── dd_ods_new_s.sh                               │   ├── dd_ods_new_s.sh
│   ├── dm_operation.sh                               │   ├── dm_operation.sh
│   ├── eagle_submit_ht.sh                            │   ├── eagle_submit_ht.sh
│   ├── eagle_submit.sh                               │   ├── eagle_submit.sh
│   ├── ht_asset_report.sh                            │   ├── ht_asset_report.sh
│   ├── ht_dw_dm.sh                                   │   ├── ht_dw_dm.sh
│   ├── ht_ods_new_s.sh                               │   ├── ht_ods_new_s.sh
│   ├── lx_asset_report.sh                            │   ├── lx_asset_report.sh
│   ├── lx_over_report.sh                             │   ├── lx_over_report.sh
│   ├── tmp_dw_dm.sh                                  │   ├── tmp_dw_dm.sh
│   └── yunxin_ods.sh                                 │   └── yunxin_ods.sh
├── bin_abs                                           ├── abn 项目命令目录（交互及 etl 流程）
│   ├── abs_wiki.md                                   │   ├── 星云交互文档
│   ├── abs_manage.sh                                 │   ├── 交互主脚本
│   ├── data_cloud-dm-all_bag.sh                      │   ├── 交互时，解包时，重跑所有包数据
│   ├── data_cloud-dm-bag.sh                          │   ├── 交互时，封包时，跑非封包时任务
│   ├── data_cloud-dm-bag_snapshot.sh                 │   ├── 交互时，封包时，跑封包时任务
│   ├── data_cloud-dw_dm-day.sh                       │   ├── 每日定时任务 dw、dm 脚本
│   └── data_cloud-ods.sh                             │   └── 每日定时任务 ods 脚本
├── conf_env                                          ├── 基础配置目录（存储目录、命令、邮件配合文件名）
│   ├── env.sh                                        │   ├── 基础配置脚本
│   ├── prod_dm_eagle_dm_eagle_cps.mysql_conf         │   ├── 生产，看管代偿后 MySQL 配置
│   ├── prod_dm_eagle_dm_eagle.mysql_conf             │   ├── 生产，看管代偿前 MySQL 配置
│   ├── prod_dm_eagle_uabs_core.mysql_conf            │   ├── 生产，星云 MySQL 配置
│   ├── test_dm_eagle_dm_eagle_cps.mysql_conf         │   ├── 测试，看管代偿后 MySQL 配置
│   ├── test_dm_eagle_dm_eagle.mysql_conf             │   ├── 测试，看管代偿前 MySQL 配置
│   └── test_dm_eagle_uabs_core.mysql_conf            │   └── 测试，星云 MySQL 配置
├── conf_mail                                         ├── 邮件配置目录（存放需要发送的邮件配置文件）
├── emr_script                                        ├── emr_script
│   ├── copy-emr-sparkJar.sh                          │   ├── copy-emr-sparkJar.sh
│   └── mv_hbase_jar.sh                               │   └── mv_hbase_jar.sh
├── file_check                                        ├── 校验文件目录
├── file_export                                       ├── 导出文件目录
├── file_hql                                          ├── 执行文件目录
│   ├── asset_report.query                            │   ├── asset_report.query
│   ├── create.query                                  │   ├── 各层表创建文件目录
│   ├── davinci                                       │   ├── 达芬奇报表目录
│   ├── dim.query                                     │   ├── dim 层执行文件目录
│   ├── dm_eagle.query                                │   ├── dm_eagle 层执行文件目录
│   ├── dm_operation.query                            │   ├── dm_operation 层执行文件目录
│   ├── dw.query                                      │   ├── dw 层执行文件目录
│   ├── eagle.query                                   │   ├── eagle 层执行文件目录
│   ├── ods_cloud.query                               │   ├── 星云执行文件目录
│   ├── ods_newcore.query                             │   ├── 新核心执行文件目录
│   │   ├── ods.loan_info.hql                         │   │   ├── 执行文件
│   │   ├── baidu                                     │   │   ├── 百度执行文件目录
│   │   └── yunxin                                    │   │   └── 云信执行文件目录
│   ├── ods.query                                     │   ├── ods 层执行文件目录
│   ├── ods_reload.query                              │   ├── 修数执行文件目录
│   └── repair_tmp.query                              │   └── repair_tmp.query
├── file_import                                       ├── 导入文件目录
│   ├── abs_cloud                                     │   ├── 星云交互导入文件目录
│   │   ├── bag_due_bill_no                           │   │   ├── 包借据关联关系导入文件目录
│   │   ├── bag_info                                  │   │   ├── 包信息导入文件目录
│   │   ├── project_due_bill_no                       │   │   ├── 项目借据关联关系导入文件目录
│   │   └── project_info                              │   │   └── 项目信息导入文件目录
│   └── HiveUDF-1.0-shaded.jar                        │   └── Hive UDF 函数文件
├── file_repaired                                     ├── 修数文件目录
│   ├── abn_repaired                                  │   ├── abn 项目修数文件目录
│   └── abs_repaired                                  │   └── abs 项目修数文件目录
├── lib                                               ├── 引用文件仓库（主要为函数）
│   └── function.sh                                   │   └── Shell 脚本函数文件
├── log                                               ├── 日志（下有各层及各脚本的日志文件）
├── param_beeline                                     ├── 各层级的 beeline 客户端配置文件目录
├── README.md                                         ├── data_shell 说明文档
├── start-abs.sh                                      ├── 星云定时任务启动脚本
├── start-baidu.sh                                    ├── 百度定时任务启动脚本
├── start-ht.sh                                       ├── 汇通定时任务启动脚本
├── start-lx.sh                                       ├── 乐信定时任务启动脚本
├── start-yunxin.sh                                   ├── 云信定时任务启动脚本
└── test.sh                                           └── 测试文件
```

## 2、脚本介绍
### 1、bin目录下的
1. data_bakup.sh 备份脚本
    - 介绍：用于备份 ods_new_s(_cps) 的 借据、应还、实还、流水 表的备份功能脚本
    - 操作：sh data_bakup.sh
    - 参数：无
2. data_biz_conf.sh biz_conf表维护脚本
    - 介绍：维护biz_conf表的脚本。需要先 手动复制[【腾讯文档】手动维护表](https://docs.qq.com/sheet/DRVpEWmtVZHdKWm5l) 中的 ** biz_conf 表 ** 中数据（空值也要保存为空），以tsv格式保存到 file_import/dim_new.biz_conf.tsv 文件中。先上传文件到hive的biz_conf表，再拉部分数据到MySQL
    - 操作：sh data_biz_conf.sh
    - 参数：无
3. data_check.sh 基础校验脚本
    - 介绍：用于校验的脚本
    - 操作：sh data_check.sh 【要执行的校验文件】 【校验开始日期】 【校验结束日期】 [【代偿前后】]
    - 参数：3个[4个]
4. data_check_all.sh 校验脚本
    - 介绍：用于备份 ods_new_s(_cps) 的 借据、应还、实还、流水 表的备份功能脚本
    - 操作：sh data_bakup.sh
    - 参数：无
5. data_biz_conf.sh biz_conf表维护脚本
    - 介绍：用于备份 ods_new_s(_cps) 的 借据、应还、实还、流水 表的备份功能脚本
    - 操作：sh data_bakup.sh
    - 参数：无
6. data_biz_conf.sh biz_conf表维护脚本
    - 介绍：用于备份 ods_new_s(_cps) 的 借据、应还、实还、流水 表的备份功能脚本
    - 操作：sh data_bakup.sh
    - 参数：无
7. tmp_tables_drop.sh 临时删除库表清理脚本
    - 介绍：用于清理临时库中已经存在7天以上的表的功能脚本
    - 操作：sh tmp_tables_drop.sh
    - 参数：无

# 2、Hive UDF 函数
## 1、UDF 函数的上传目录
    /user/hive/auxlib


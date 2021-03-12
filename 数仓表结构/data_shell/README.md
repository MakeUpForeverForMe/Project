[TOC]
# 一、data_shell 说明
## 1、data_shell 目录说明
```
data_shell                                           根目录
├── bin                                              ├── 基础命令
│   ├── data_bakup.sh                                │   ├── 备份脚本
│   ├── data_biz_conf.sh                             │   ├── biz_conf表维护脚本
│   ├── data_check_all.sh                            │   ├── 校验脚本
│   ├── data_check.sh                                │   ├── 基础校验脚本
│   ├── data_delete_file.sh                          │   ├── 删除文件脚本
│   ├── data_manage.sh                               │   ├── 基础执行脚本
│   ├── data_tail.sh                                 │   ├── 过滤查看文件脚本
│   └── send_mail.py                                 │   └── 发送邮件脚本
├── bin_abn                                          ├── abn项目命令（主要为etl流程）
│   ├── data_asset.sh                                │   ├──
│   ├── data_dw_dm.sh                                │   ├── dw、dm及抽数到MySQL的etl脚本
│   ├── data_ealge_funds.sh                          │   ├── 资金页面相关流程脚本
│   ├── data_export.sh                               │   ├── dm层数据导出脚本
│   ├── data_income.sh                               │   ├── 进件etl脚本
│   ├── data_ods_new_s.sh                            │   ├── ods_new_s脚本
│   ├── dm_operation.sh                              │   ├──
│   ├── lx_asset_report.sh                           │   ├──
│   └── lx_over_report.sh                            │   └──
├── bin_abs                                          ├── abn项目命令（主要为etl流程）
│   ├── abs_manage.sh                                │   ├── abs（星云）交互脚本
│   ├── abs-submit.sh                                │   ├──
│   ├── asset_package-1.0-SNAPSHOT-shaded.jar        │   ├──
│   └── driver-log4j.properties                      │   └──
├── conf_env                                         ├── 基础配置（存储目录、命令、邮件配合文件名）
│   └── env.sh                                       │   └── 基础配置脚本
├── conf_mail                                        ├── 邮件配置
│   ├── data_receives_mail_pm_rd.config              │   ├── 接收人为开发产品的邮件配置
│   ·                                                │   ·
│   ·                                                │   ·
│   ·                                                │   ·
├── file_check                                       ├── 校验文件
│   ├── 00edit_ods_pk.hql                            │   ├── ods层所有校验SQL脚本
│   ├── ods_new_s.loan_info_active_num_principal.hql │   ├── ods_new_s层 借据表 放款数、金额校验脚本
│   ├── dw_new.loan_num_loan_principal.hql           │   ├── dw_new层 放款表 放款数、金额校验脚本
│   ├── dm_eagle.loan_amount.hql                     │   ├── dm_eagle层 放款规模表 放款金额校验脚本
│   ·                                                │   ·
│   ·                                                │   ·
│   ·                                                │   ·
├── file_export                                      ├── 导出文件
│   └── dm_eagle.eagle_title_info.tsv                │   └── 导出的文件存放在这里
├── file_hql                                         ├── 执行文件
│   ├── create.query                                 │   ├── 各层表创建文件目录
│   ├── ods_new_s.query                              │   ├── ods_new_s层执行文件目录
│   ├── ods_new_s_cloud.query                        │   ├── 星云系统执行文件目录
│   ├── dim_new.query                                │   ├── dim_new层执行文件目录
│   ├── dw_new.query                                 │   ├── dw_new层执行文件目录
│   ├── dm_eagle.query                               │   ├── dm_eagle层执行文件目录
│   ├── dm_operation.query                           │   ├── 运营平台执行文件目录
│   └── asset_report.query                           │   └── 资服报告存储目录
├── file_import                                      ├── 导入文件
│   ├── abs_cloud                                    │   ├── 星云系统导入数据目录
│   │   ├── bag_due_bill_no                          │   │   ├── 包借据映射文件目录
│   │   ├── bag_info                                 │   │   ├── 包属性文件目录
│   │   ├── project_due_bill_no                      │   │   ├── 项目借据映射文件目录
│   │   └── project_info                             │   │   └── 项目属性文件目录
│   ├── dim_new.biz_conf.csv                         │   ├── biz_conf表维护文件
│   └── dim_new.dim_investor_info.tsv                │   └── 投资人表维护文件
├── file_repaired                                    ├── 修数文件目录
│   ├── abn_repaired                                 │   ├── abn项目修数文件目录
│   └── abs_repaired                                 │   └── abs项目修数文件目录
├── lib                                              ├── 引用文件仓库（主要为函数和有需要的jar包）
│   └── function.sh                                  │   └── Shell脚本函数文件
├── log                                              ├── 日志（下有各层及各脚本的日志文件）
├── param_beeline                                    ├── 各层级的beeline客户端配置文件目录
│   └── ods_new_s.param_ht.hql                       │   └── ods_new_s层乐信项目beeline客户端配置文件
├── README.md                                        ├── data_shell说明文档
└── start.sh                                         └── 全局启动文件
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


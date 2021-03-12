-- DROP TABLE IF EXISTS `dm_eagle.report_dm_lx_asset_statistics`;
CREATE TABLE IF NOT EXISTS dm_eagle.report_dm_lx_asset_statistics(
  `id`                                STRING        NOT NULL COMMENT '主键id:当前统计日yyyyMMdd',
  `project_id`                        STRING        NOT NULL COMMENT '项目id',
  `m2plus_recover_amount_accum`       DECIMAL(15,4)          COMMENT '累计违约30天+回收金额',
  `m2plus_recover_prin_accum`         DECIMAL(15,4)          COMMENT '累计违约30天+回收本金',
  `m2plus_recover_inter_accum`        DECIMAL(15,4)          COMMENT '累计违约30天+回收息费',
  `m4plus_recover_amount_accum`       DECIMAL(15,4)          COMMENT '累计违约90天+回收金额',
  `m4plus_recover_prin_accum`         DECIMAL(15,4)          COMMENT '累计违约90天+回收本金',
  `m4plus_recover_inter_accum`        DECIMAL(15,4)          COMMENT '累计违约90天+回收息费',
  `static_30_amount`                  DECIMAL(15,4)          COMMENT '累计30+逾期资产金额',
  `static_30_prin`                    DECIMAL(15,4)          COMMENT '累计30+逾期资产本金',
  `static_30_iner`                    DECIMAL(15,4)          COMMENT '累计30+逾期资产利息',
  `static_90_amount`                  DECIMAL(15,4)          COMMENT '累计90+逾期资产金额',
  `static_90_prin`                    DECIMAL(15,4)          COMMENT '累计90+逾期资产本金',
  `static_90_iner`                    DECIMAL(15,4)          COMMENT '累计90+逾期资产利息',
  `overdue_remain_principal`          DECIMAL(15,4)          COMMENT '逾期1+剩余本金',
  `acc_buy_back_amount`               DECIMAL(15,4)          COMMENT '累计180+回购金额',
  `snapshot_date`                     STRING                 COMMENT '快照日',
  PRIMARY KEY (id)
)
PARTITION BY HASH (id) PARTITIONS 8
COMMENT 'dm 乐信剔代偿快照统计宽表'
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='node172');

-- DROP TABLE IF EXISTS `dm_eagle.report_dm_lx_asset_summary_statistics`;
CREATE TABLE IF NOT EXISTS `dm_eagle.report_dm_lx_asset_summary_statistics`(
  `id`                                STRING        NOT NULL  COMMENT '主键id:当前统计日yyyyMMdd',
  `project_id`                        STRING        NOT NULL  COMMENT '项目id',
  `m2plus_recover_amount`             DECIMAL(15,4)           COMMENT '违约30天+回收金额',
  `m2plus_recover_prin`               DECIMAL(15,4)           COMMENT '违约30天+回收本金',
  `m2plus_recover_inter`              DECIMAL(15,4)           COMMENT '违约30天+回收息费',
  `m4plus_recover_amount`             DECIMAL(15,4)           COMMENT '违约90天+回收金额',
  `m4plus_recover_prin`               DECIMAL(15,4)           COMMENT '违约90天+回收本金',
  `m4plus_recover_inter`              DECIMAL(15,4)           COMMENT '违约90天+回收息费',
  `m2plus_num`                        INT           DEFAULT 0 COMMENT '违约30天+笔数',
  `m2plus_amount`                     DECIMAL(15,4)           COMMENT '违约30天+金额',
  `m2plus_prin`                       DECIMAL(15,4)           COMMENT '违约30天+本金',
  `m2plus_inter`                      DECIMAL(15,4)           COMMENT '违约30天+利息',
  `m4plus_num`                        INT           DEFAULT 0 COMMENT '违约90天+笔数',
  `m4plus_amount`                     DECIMAL(15,4)           COMMENT '违约90天+金额',
  `m4plus_prin`                       DECIMAL(15,4)           COMMENT '违约90天+本金',
  `m4plus_inter`                      DECIMAL(15,4)           COMMENT '违约90天+利息',
  `static_30_new_amount`              DECIMAL(15,4)           COMMENT '新增30+逾期资产金额',
  `static_30_new_prin`                DECIMAL(15,4)           COMMENT '新增30+逾期资产本金',
  `static_30_new_inter`               DECIMAL(15,4)           COMMENT '新增30+逾期资产利息',
  `static_90_new_amount`              DECIMAL(15,4)           COMMENT '新增90+逾期资产金额',
  `static_90_new_prin`                DECIMAL(15,4)           COMMENT '新增90+逾期资产本金',
  `static_90_new_inter`               DECIMAL(15,4)           COMMENT '新增90+逾期资产利息',
  `snapshot_date`                     STRING                  COMMENT '快照日',
  PRIMARY KEY (id)
)
PARTITION BY HASH (id) PARTITIONS 8
COMMENT 'dm 乐信剔代偿汇总大宽表'
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='node172');


-- DROP TABLE IF EXISTS `dm_eagle.report_dm_lx_asset_report_accu_comp`;
CREATE TABLE IF NOT EXISTS `dm_eagle.report_dm_lx_asset_report_accu_comp`(
  `id`                                STRING        NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '主键id',
  `snapshot_date`                     STRING            NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '快照日',
  `project_id`                        STRING            NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '项目id',
  `total_remain_prin`                 DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '总本金余额',
  `total_remain_num`                  INT               NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '未结清笔数',
  `average_remain`                    DECIMAL(15,6)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '在贷件均余额',
  `weighted_average_rate`             DECIMAL(15,6)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '加权平均利率',
  `weighted_average_remain_tentor`    DECIMAL(15,6)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '加权平均剩余期数',
  `total_contract_amount`             DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '累计发放贷款总额',
  `total_contract_num`                INT               NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '累计发放贷款笔数',
  `average_contract_amount`           DECIMAL(15,6)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '贷款件均金额',
  `total_repurchase_contract`         DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '回购贷款金额',
  `total_repurchase_num`              INT               NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '回购贷款笔数',
  `weighted_average_rate_by_contract` DECIMAL(15,6)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '合同加权利率',
  `weighted_average_term_by_contract` DECIMAL(15,6)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '合同加权期限',
  `loan_amount_accum`                 DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '放款金额',
  `total_amount_accum`                DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '累计回收总金额',
  `total_prin_accum`                  DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '累计回收总本金',
  `total_inter_accum`                 DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '累计回收总息费',
  `prepay_amount_accum`               DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '累计提前结清金额',
  `prepay_prin_accouum`               DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '累计提前结清本金',
  `prepay_inter_accum`                DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '累计提前结清利息',
  `conpensation_amount_accum`         DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '累计代偿金额',
  `conpensation_prin_accum`           DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '累计代偿本金',
  `conpensation_inter_accum`          DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '累计代偿息费',
  `repurchase_amount_accum`           DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '累计回购金额',
  `repurchase_prin_accum`             DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '累计回购本金',
  `repurchase_inter_accum`            DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '累计回购息费',
  `refund_contract_amount_accum`      DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '累计退票金额',
  `refund_contract_num_accum`         INT               NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '累计退票笔数',
  PRIMARY KEY (id)
)
PARTITION BY HASH (id) PARTITIONS 4
 COMMENT '乐信无在途版含代偿汇总宽表'
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='node172');


-- DROP TABLE IF EXISTS `dm_eagle.report_dm_lx_asset_report_snapshot_comp`;
CREATE TABLE IF NOT EXISTS `dm_eagle.report_dm_lx_asset_report_snapshot_comp`(
  `id`                                STRING        NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '主键id',
  `snapshot_date`                     STRING            NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '快照日',
  `project_id`                        STRING            NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '项目id',
  `loan_amount`                       DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '放款金额',
  `total_collection_amount`           DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '总回收款-金额',
  `total_collection_prin`             DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '总回收款-本金',
  `total_collection_inter`            DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '总回收款-息费',
  `normal_amount`                     DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '正常还款金额',
  `normal_prin`                       DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '正常还款本金',
  `normal_inter`                      DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '正常还款息费',
  `overdue_amount`                    DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '逾期还款金额',
  `overdue_prin`                      DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '逾期还款本金',
  `overdue_inter`                     DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '逾期还款息费',
  `prepay_amount`                     DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '提前结清金额',
  `prepay_prin`                       DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '提前结清本金',
  `prepay_inter`                      DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '提前结清息费',
  `compensation_amount`               DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '代偿回款金额',
  `compensation_prin`                 DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '代偿回款本金',
  `compensation_inter`                DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '代偿回款息费',
  `repurchase_amount`                 DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '回购回款金额',
  `repurchase_prin`                   DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '回购回款本金',
  `repurchase_inter`                  DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '回购回款息费',
  `refund_amount`                     DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '退票金额',
  PRIMARY KEY (id)
)
PARTITION BY HASH (id) PARTITIONS 4
COMMENT '乐信无在途版含代偿汇总宽表'
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='node172');


-- DROP TABLE IF EXISTS `dm_eagle.report_dm_lx_asset_report_accu_repayment`;
CREATE TABLE IF NOT EXISTS `dm_eagle.report_dm_lx_asset_report_accu_repayment`(
  `id`                                STRING        NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '主键id',
  `snapshot_date`                     STRING            NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '快照日',
  `prepay_amount_accum`               DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '累计提前结清金额',
  `prepay_prin_accouum`               DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '累计提前结清本金',
  `prepay_inter_accum`                DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '累计提前结清利息',
  PRIMARY KEY (id)
)
PARTITION BY HASH (id) PARTITIONS 4
COMMENT '乐信无在途版含代偿汇总宽表'
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='node172');


-- DROP TABLE IF EXISTS `dm_eagle.report_dm_lx_asset_report_snapshot_repayment`;
CREATE TABLE IF NOT EXISTS `dm_eagle.report_dm_lx_asset_report_snapshot_repayment`(
  `id`                                STRING        NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '主键id',
  `snapshot_date`                     STRING            NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '快照日',
  `normal_amount`                     DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '正常还款金额',
  `normal_prin`                       DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '正常还款本金',
  `normal_inter`                      DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '正常还款息费',
  `overdue_amount`                    DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '逾期还款金额',
  `overdue_prin`                      DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '逾期还款本金',
  `overdue_inter`                     DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '逾期还款息费',
  `prepay_amount`                     DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '提前结清金额',
  `prepay_prin`                       DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '提前结清本金',
  `prepay_inter`                      DECIMAL(15,2)     NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '提前结清息费',
  PRIMARY KEY (id)
)
PARTITION BY HASH (id) PARTITIONS 4
 COMMENT '乐信无在途版含代偿汇总宽表'
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='node172');





---汇通资产服务报告
-- DROP TABLE IF EXISTS `dm_eagle.report_ht_asset_project`;
CREATE TABLE IF NOT EXISTS dm_eagle.report_ht_asset_project(
  `id`                                string        not     null comment '主键id:项目id+批次时间',
  `project_id`                        string        not     null comment '项目id',
  `total_remain_principal`            decimal(15,2)  comment '总本金余额',
  `un_settle_num`                     bigint         comment '未结清笔数',
  `weight_avg_inter_rate`             decimal(15,4)  comment '加权平均利率',
  `weight_remain_term`                decimal(15,4)  comment '加权平均剩余期数(账龄)',
  `current_repay_amount`              decimal(15,2)  comment '当前回收款-金额',
  `current_repay_bill_num`            bigint         comment '当前回收款-笔数',
  `current_in_plan_repay_amount`      decimal(15,2)  comment '当前计划内还款-金额',
  `current_in_plan_repay_num`         bigint         comment '当前计划内还款-笔数',
  `current_early_settle_amount`       decimal(15,2)  comment '当前提前结清-金额',
  `currnt_early_settle_num`           bigint         comment '当前提前结清-笔数',
  `current_back_amount`               decimal(15,2)  comment '当前赎回回款-金额',
  `current_back_num`                  bigint         comment '当前赎回回款-笔数',
  `total_bill_num`                    bigint         comment '合计-笔数',
  `total_amount`                      decimal(15,2)  comment '合计-金额',
  `normal_total_num`                  bigint         comment '正常-笔数',
  `normal_num_rate`                   decimal(15,4)  comment '正常-笔数-占比',
  `normal_total_amount`               decimal(15,2)  comment '正常-金额',
  `normal_amount_rate`                decimal(15,4)  comment '正常-金额-占比',
  `overdue1to30_num_count`            bigint         comment '逾期1~30天-笔数',
  `overdue1to30_num_rate`             decimal(15,4)  comment '逾期1~30天-笔数-占比',
  `overdue1to30_amount`               decimal(15,2)  comment '逾期1~30天-金额',
  `overdue1to30_amount_rate`          decimal(15,4)  comment '逾期1~30天-金额-占比',
  `overdue31to60_num_count`           bigint         comment '逾期31~60天-笔数',
  `overdue31to60_num_rate`            decimal(15,4)  comment '逾期31~60天-笔数-占比',
  `overdue31to60_amount`              decimal(15,2)  comment '逾期31~60天-金额',
  `overdue31to60_amount_rate`         decimal(15,4)  comment '逾期31~60天-金额-占比',
  `overdue61to90_num_count`           bigint         comment '逾期61-90天-笔数',
  `overdue61to90_num_rate`            decimal(15,4)  comment '逾期61-90天-笔数-占比',
  `overdue61to90_amount`              decimal(15,2)  comment '逾期61-90天-金额',
  `overdue61to90_amount_rate`         decimal(15,4)  comment '逾期61-90天-金额-占比',
  `overdue90_num_count`               bigint         comment '逾期90+天-笔数',
  `overdue90_num_rate`                decimal(15,4)  comment '逾期90+天-笔数-占比',
  `overdue90_amount`                  decimal(15,2)  comment '逾期90+天-金额',
  `overdue90_amount_rate`             decimal(15,4)  comment '逾期90+天-金额-占比',
  `overdue30_num_count`               bigint         comment '违约30天+笔数',
  `overdue30_amount`                  decimal(15,2)  comment '违约30天+金额',
  `overdue30_amount_rate`             decimal(15,4)  comment '累计30+违约率',
  `overdue180_num_count`              bigint         comment '违约180天+笔数',
  `overdue180_amount`                 decimal(15,2)  comment '违约180天+金额',
  `static_90to_num`                   bigint         comment '静态90+笔数',
  `static_90to_amount`                decimal(15,2)  comment '静态90+金额',
  `static_90to_amount_rate`           decimal(15,4)  comment '累计90+违约率',
  `accum_30to_amount`                 decimal(15,2)  comment '累计30+回收租金',
  `accum_90to_amount`                 decimal(15,2)  comment '累计90+回收本金',
  `snapshot_date`                     string                     comment '快照日期(yyyy-MM-dd)',
  PRIMARY KEY(id)
)
PARTITION BY HASH PARTITIONS 8
COMMENT 'dm 汇通资产包统计信息'
STORED AS KUDU;










-- DROP TABLE IF EXISTS `dm_eagle.report_ht_overdue_more9_assets_detail`;
CREATE TABLE IF NOT EXISTS `dm_eagle.report_ht_overdue_more9_assets_detail`(
  `ID`                                string        not     null comment '主键id:项目id+借据号',
  `project_id`                        string        not     null comment '项目id',
  `bill_no`                           string        default null comment '借据编号',
  `borrower_name`                     string        default null comment '借款人名称',
  `overdue_start_date`                string        default null comment '逾期开始日期',
  `overdue_amount`                    decimal(15,4) default null comment '逾期金额',
  `snapshot_date`                     string                     comment '快照日期(yyyy-MM-dd)',
  PRIMARY KEY(ID)
)
PARTITION BY HASH PARTITIONS 8
COMMENT 'dm 汇通逾期9天的资产'
STORED AS KUDU;


-- DROP TABLE IF EXISTS `dm_eagle.report_ht_prepayment_assets_detail`;
CREATE TABLE IF NOT EXISTS `dm_eagle.report_ht_prepayment_assets_detail`(
  `ID`                                string        not     null comment '主键id:项目id+借据号',
  `project_id`                        string        not     null comment '项目id',
  `bill_no`                           string        default null comment '借据编号',
  `borrower_name`                     string        default null comment '借款人名称',
  `should_pay_date`                   string        default null comment '应还款日',
  `should_pay_amount`                 decimal(15,4) default null comment '应还金额 本金加利息',
  `advance_paid_out_date`             string        default null comment '提前结清日期',
  `repayment_amount`                  decimal(15,4) default null comment '提前结清金额 本金利息加罚息',
  PRIMARY KEY(ID)
)
PARTITION BY HASH PARTITIONS 8
COMMENT 'dm 汇通早偿'
STORED AS KUDU;

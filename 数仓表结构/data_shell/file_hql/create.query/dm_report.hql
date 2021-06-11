-- DROP TABLE IF EXISTS `dm_eagle.report_dm_lx_asset_statistics`;

CREATE TABLE dm_eagle.report_lx_asset_distribution (
  mold STRING  COMMENT '类型',
  content STRING  COMMENT '内容',
  end_assets_prin DECIMAL(12,2)  COMMENT '期末分布资产余额',
  end_assets_num DECIMAL(12,0)  COMMENT '期末分布资产笔数',
  new_assets_prin DECIMAL(12,2)  COMMENT '新增放款资产余额',
  new_assets_num DECIMAL(12,0)  COMMENT '新增放款资产笔数'
)  COMMENT '乐信资产分布表'
 PARTITIONED BY (`snapshot_date` string COMMENT '快照日',`project_id` string COMMENT '项目编号')
STORED AS PARQUET ;


CREATE TABLE dm_eagle.report_dm_lx_asset_refund_distribution (
  mold STRING ,
  content STRING ,
  contact_amount DECIMAL(12,2) ,
  assets_sum INT
)COMMENT '乐信退票资产分布表'
 PARTITIONED BY (`snapshot_date` string COMMENT '快照日',`project_id` string COMMENT '项目编号')
STORED AS PARQUET ;



CREATE TABLE dm_eagle.report_dm_lx_asset_report_accu_comp (
   total_remain_prin                                              DECIMAL(15,2)  COMMENT '总本金余额',
   total_remain_num                                               INT  COMMENT '未结清笔数',
   average_remain                                                 DECIMAL(15,6)  COMMENT '在贷件均余额',
   weighted_average_rate                                          DECIMAL(15,6)  COMMENT '加权平均利率',
   weighted_average_remain_tentor                                 DECIMAL(15,6)  COMMENT '加权平均剩余期数',
   total_contract_amount                                          DECIMAL(15,2)  COMMENT '累计发放贷款总额',
   total_contract_num                                             INT  COMMENT '累计发放贷款笔数',
   average_contract_amount                                        DECIMAL(15,6)  COMMENT '贷款件均金额',
   total_repurchase_contract                                      DECIMAL(15,2)  COMMENT '回购贷款金额',
   total_repurchase_num                                           INT  COMMENT '回购贷款笔数',
   weighted_average_rate_by_contract                              DECIMAL(15,6)  COMMENT '合同加权利率',
   weighted_average_term_by_contract                              DECIMAL(15,6)  COMMENT '合同加权期限',
   loan_amount_accum                                              DECIMAL(15,2)  COMMENT '放款金额',
   total_amount_accum                                             DECIMAL(15,2)  COMMENT '累计回收总金额',
   total_prin_accum                                               DECIMAL(15,2)  COMMENT '累计回收总本金',
   total_inter_accum                                              DECIMAL(15,2)  COMMENT '累计回收总息费',
   prepay_amount_accum                                            DECIMAL(15,2)  COMMENT '累计提前结清金额',
   prepay_prin_accouum                                            DECIMAL(15,2)  COMMENT '累计提前结清本金',
   prepay_inter_accum                                             DECIMAL(15,2)  COMMENT '累计提前结清利息',
   conpensation_amount_accum                                      DECIMAL(15,2)  COMMENT '累计代偿金额',
   conpensation_prin_accum                                        DECIMAL(15,2)  COMMENT '累计代偿本金',
   conpensation_inter_accum                                       DECIMAL(15,2)  COMMENT '累计代偿息费',
   repurchase_amount_accum                                        DECIMAL(15,2)  COMMENT '累计回购金额',
   repurchase_prin_accum                                          DECIMAL(15,2)  COMMENT '累计回购本金',
   repurchase_inter_accum                                         DECIMAL(15,2)  COMMENT '累计回购息费',
   refund_contract_amount_accum                                   DECIMAL(15,2)  COMMENT '累计退票金额',
   refund_contract_num_accum                                      INT  COMMENT '累计退票笔数'
 ) COMMENT '乐信无在途版含代偿汇总宽表'
  PARTITIONED BY (`snapshot_date` string COMMENT '快照日',`project_id` string COMMENT '项目编号' )
STORED AS PARQUET;



CREATE TABLE dm_eagle.report_dm_lx_asset_report_accu_repayment(
  prepay_amount_accum DECIMAL(15,2)  COMMENT '累计提前结清金额',
  prepay_prin_accouum DECIMAL(15,2)  COMMENT '累计提前结清本金',
  prepay_inter_accum DECIMAL(15,2)  COMMENT '累计提前结清利息'
) COMMENT '乐信无在途版含代偿汇总宽表'
 PARTITIONED BY (`snapshot_date` string COMMENT '快照日',`project_id` string COMMENT '项目编号')
STORED AS parquet;


CREATE TABLE dm_eagle.report_dm_lx_asset_report_snapshot_comp (
  loan_amount DECIMAL(15,2)  COMMENT '放款金额',
  total_collection_amount DECIMAL(15,2)  COMMENT '总回收款-金额',
  total_collection_prin DECIMAL(15,2)  COMMENT '总回收款-本金',
  total_collection_inter DECIMAL(15,2)  COMMENT '总回收款-息费',
  normal_amount DECIMAL(15,2)  COMMENT '正常还款金额',
  normal_prin DECIMAL(15,2)  COMMENT '正常还款本金',
  normal_inter DECIMAL(15,2)  COMMENT '正常还款息费',
  overdue_amount DECIMAL(15,2)  COMMENT '逾期还款金额',
  overdue_prin DECIMAL(15,2)  COMMENT '逾期还款本金',
  overdue_inter DECIMAL(15,2)  COMMENT '逾期还款息费',
  prepay_amount DECIMAL(15,2)  COMMENT '提前结清金额',
  prepay_prin DECIMAL(15,2)  COMMENT '提前结清本金',
  prepay_inter DECIMAL(15,2)  COMMENT '提前结清息费',
  compensation_amount DECIMAL(15,2)  COMMENT '代偿回款金额',
  compensation_prin DECIMAL(15,2)  COMMENT '代偿回款本金',
  compensation_inter DECIMAL(15,2)  COMMENT '代偿回款息费',
  repurchase_amount DECIMAL(15,2)  COMMENT '回购回款金额',
  repurchase_prin DECIMAL(15,2)  COMMENT '回购回款本金',
  repurchase_inter DECIMAL(15,2)  COMMENT '回购回款息费',
  refund_amount DECIMAL(15,2)  COMMENT '退票金额'
)COMMENT '乐信无在途版含代偿汇总宽表'
 PARTITIONED BY (`snapshot_date` string COMMENT '快照日',`project_id` string COMMENT '项目编号')
STORED AS PARQUET;


CREATE TABLE dm_eagle.report_dm_lx_asset_report_snapshot_repayment (
  normal_amount DECIMAL(15,2)  COMMENT '正常还款金额',
  normal_prin DECIMAL(15,2)  COMMENT '正常还款本金',
  normal_inter DECIMAL(15,2)  COMMENT '正常还款息费',
  overdue_amount DECIMAL(15,2)  COMMENT '逾期还款金额',
  overdue_prin DECIMAL(15,2)  COMMENT '逾期还款本金',
  overdue_inter DECIMAL(15,2)  COMMENT '逾期还款息费',
  prepay_amount DECIMAL(15,2)  COMMENT '提前结清金额',
  prepay_prin DECIMAL(15,2)  COMMENT '提前结清本金',
  prepay_inter DECIMAL(15,2)  COMMENT '提前结清息费'
) COMMENT '乐信无在途版含代偿汇总宽表'
  PARTITIONED BY (`snapshot_date` string COMMENT '快照日',`project_id` string COMMENT '项目编号')
STORED AS PARQUET;


CREATE TABLE dm_eagle.report_dm_lx_asset_statistics (
  m2plus_recover_amount_accum DECIMAL(15,4)  COMMENT '累计违约30天+回收金额',
  m2plus_recover_prin_accum DECIMAL(15,4)  COMMENT '累计违约30天+回收本金',
  m2plus_recover_inter_accum DECIMAL(15,4)  COMMENT '累计违约30天+回收息费',
  m4plus_recover_amount_accum DECIMAL(15,4)  COMMENT '累计违约90天+回收金额',
  m4plus_recover_prin_accum DECIMAL(15,4)  COMMENT '累计违约90天+回收本金',
  m4plus_recover_inter_accum DECIMAL(15,4)  COMMENT '累计违约90天+回收息费',
  static_30_amount DECIMAL(15,4)  COMMENT '累计30+逾期资产金额',
  static_30_prin DECIMAL(15,4)  COMMENT '累计30+逾期资产本金',
  static_30_iner DECIMAL(15,4)  COMMENT '累计30+逾期资产利息',
  static_90_amount DECIMAL(15,4)  COMMENT '累计90+逾期资产金额',
  static_90_prin DECIMAL(15,4)  COMMENT '累计90+逾期资产本金',
  static_90_iner DECIMAL(15,4)  COMMENT '累计90+逾期资产利息',
  overdue_remain_principal DECIMAL(15,4)  COMMENT '逾期1+剩余本金',
  acc_buy_back_amount DECIMAL(15,4)  COMMENT '累计180+回购金额'
)COMMENT 'dm 乐信剔代偿快照统计宽表'
 PARTITIONED BY (`snapshot_date` string COMMENT '快照日',`project_id` string COMMENT '项目编号')
STORED AS PARQUET;


CREATE TABLE dm_eagle.report_dm_lx_asset_summary_statistics (
  m2plus_recover_amount DECIMAL(15,4)  COMMENT '违约30天+回收金额',
  m2plus_recover_prin DECIMAL(15,4)  COMMENT '违约30天+回收本金',
  m2plus_recover_inter DECIMAL(15,4)  COMMENT '违约30天+回收息费',
  m4plus_recover_amount DECIMAL(15,4)  COMMENT '违约90天+回收金额',
  m4plus_recover_prin DECIMAL(15,4)  COMMENT '违约90天+回收本金',
  m4plus_recover_inter DECIMAL(15,4)  COMMENT '违约90天+回收息费',
  m2plus_num INT  COMMENT '违约30天+笔数',
  m2plus_amount DECIMAL(15,4)  COMMENT '违约30天+金额',
  m2plus_prin DECIMAL(15,4)  COMMENT '违约30天+本金',
  m2plus_inter DECIMAL(15,4)  COMMENT '违约30天+利息',
  m4plus_num INT   COMMENT '违约90天+笔数',
  m4plus_amount DECIMAL(15,4)  COMMENT '违约90天+金额',
  m4plus_prin DECIMAL(15,4)  COMMENT '违约90天+本金',
  m4plus_inter DECIMAL(15,4)  COMMENT '违约90天+利息',
  static_30_new_amount DECIMAL(15,4)  COMMENT '新增30+逾期资产金额',
  static_30_new_prin DECIMAL(15,4)  COMMENT '新增30+逾期资产本金',
  static_30_new_inter DECIMAL(15,4)  COMMENT '新增30+逾期资产利息',
  static_90_new_amount DECIMAL(15,4)  COMMENT '新增90+逾期资产金额',
  static_90_new_prin DECIMAL(15,4)  COMMENT '新增90+逾期资产本金',
  static_90_new_inter DECIMAL(15,4)  COMMENT '新增90+逾期资产利息'
)COMMENT 'dm 乐信剔代偿汇总大宽表'
 PARTITIONED BY (`snapshot_date` string COMMENT '快照日',`project_id` string COMMENT '项目编号')
STORED AS PARQUET;


CREATE TABLE dm_eagle.report_dm_lx_comp_overdue_distribution (
  normal_prin DECIMAL(12,2)  COMMENT '正常状态剩余本金',
  overdue_prin1 DECIMAL(12,2)  COMMENT '逾期1-30剩余本金',
  overdue_prin31 DECIMAL(12,2)  COMMENT '逾期31-60剩余本金',
  overdue_prin61 DECIMAL(12,2)  COMMENT '逾期61-90剩余本金',
  overdue_prin91 DECIMAL(12,2)  COMMENT '逾期91-120剩余本金',
  overdue_prin121 DECIMAL(12,2)  COMMENT '逾期121-150剩余本金',
  overdue_prin151 DECIMAL(12,2)  COMMENT '逾期151-180剩余本金',
  overdue_prin181 DECIMAL(12,2)  COMMENT '逾期181及以上剩余本金'
)COMMENT '乐信逾期阶段分布表'
 PARTITIONED BY (`snapshot_date` string COMMENT '快照日',`project_id` string COMMENT '项目编号')
STORED AS PARQUET;
























---汇通资产服务报告
-- DROP TABLE IF EXISTS `dm_eagle.report_ht_asset_project`;
CREATE TABLE IF NOT EXISTS dm_eagle.report_ht_asset_project(
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
  `accum_90to_inter`                 decimal(15,2)  comment '累计90+回收利息'
)COMMENT 'dm 汇通资产包统计信息'
PARTITIONED BY(`snapshot_date` string COMMENT '快照日期(yyyy-MM-dd)',`project_id` string comment '项目id')
STORED AS PARQUET;






















-- DROP TABLE IF EXISTS `dm_eagle.report_ht_overdue_more9_assets_detail`;
CREATE TABLE IF NOT EXISTS `dm_eagle.report_ht_overdue_more9_assets_detail`(
  `due_bill_no`                           string      comment '借据编号',
  `borrower_name`                     string         comment '借款人名称',
  `overdue_start_date`                string         comment '逾期开始日期',
  `overdue_amount`                    decimal(15,4)  comment '逾期金额'
)COMMENT 'dm 汇通逾期9天的资产'
PARTITIONED BY(`snapshot_date` string COMMENT '快照日期(yyyy-MM-dd)',`project_id` string comment '项目id')
STORED AS PARQUET;


-- DROP TABLE IF EXISTS `dm_eagle.report_ht_prepayment_assets_detail`;
CREATE TABLE IF NOT EXISTS `dm_eagle.report_ht_prepayment_assets_detail`(
  `due_bill_no`                       string        comment '借据编号',
  `borrower_name`                     string         comment '借款人名称',
  `should_pay_date`                   string         comment '应还款日',
  `should_pay_amount`                 decimal(15,4)  comment '应还金额 本金加利息',
  `advance_paid_out_date`             string         comment '提前结清日期',
  `repayment_amount`                  decimal(15,4)  comment '提前结清金额 本金利息加罚息'
)COMMENT 'dm 汇通早偿'
PARTITIONED BY(`project_id` string comment '项目id')
STORED AS PARQUET;

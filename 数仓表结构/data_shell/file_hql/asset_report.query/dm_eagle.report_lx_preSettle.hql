-- 累计指标
insert overwrite table dm_eagle.report_dm_lx_asset_report_accu_repayment partition(snapshot_date,project_id)
select

    cast(sum(if(repay_type='提前还款',repay_prin+repay_int+repay_fee+repay_penalty,0)) as decimal(15,2))            as    prepay_amount_accum,
    cast(sum(if(repay_type='提前还款',repay_prin,0)) as decimal(15,2))                                              as    prepay_prin_accouum,
    cast(sum(if(repay_type='提前还款',repay_int+repay_fee+repay_penalty,0)) as decimal(15,2))                       as    prepay_inter_accum,
    '${var:ST9}'                                                                              as    snapshot_date,
    project_no  as project_id
from ods.ecas_repayment where txn_date<'${var:ST9}' group by project_no;exit;


-- 每日快照指标
insert overwrite table dm_eagle.report_dm_lx_asset_report_snapshot_repayment partition(snapshot_date,project_id)
select

    cast(sum(if(repay_type='正常还',repay_prin+repay_int+repay_fee+repay_penalty,0)) as decimal(15,2))             as    normal_amount,
    cast(sum(if(repay_type='正常还',repay_prin,0)) as decimal(15,2))                                               as    normal_prin,
    cast(sum(if(repay_type='正常还',repay_int+repay_fee+repay_penalty,0))  as decimal(15,2))                       as    normal_inter,
    cast(sum(if(repay_type='逾期还款',repay_prin+repay_int+repay_fee+repay_penalty,0)) as decimal(15,2))               as    overdue_amount,
    cast(sum(if(repay_type='逾期还款',repay_prin,0)) as decimal(15,2))                                                  as    overdue_prin,
    cast(sum(if(repay_type='逾期还款',repay_int+repay_fee+repay_penalty,0)) as decimal(15,2))                          as    overdue_inter,
    cast(sum(if(repay_type='提前还款',repay_prin+repay_int+repay_fee+repay_penalty,0)) as decimal(15,2))           as    prepay_amount,
    cast(sum(if(repay_type='提前还款',repay_prin,0)) as decimal(15,2))                                              as    prepay_prin,
    cast(sum(if(repay_type='提前还款',repay_int+repay_fee+repay_penalty,0)) as decimal(15,2))                       as    prepay_inter,
    '${var:ST9}'                                                                              as    snapshot_date,
    project_no                                                      as    project_id
from ods.ecas_repayment where txn_date=from_unixtime(unix_timestamp(date_add('${var:ST9}',-1)),'yyyy-MM-dd') group by project_no;exit;


-- 刷新相关字段
insert overwrite table dm_eagle.report_dm_lx_asset_report_accu_comp partition(snapshot_date,project_id)
select

    a.total_remain_prin,
    a.total_remain_num,
    a.average_remain,
    a.weighted_average_rate,
    a.weighted_average_remain_tentor,
    a.total_contract_amount,
    a.total_contract_num,
    a.average_contract_amount,
    a.total_repurchase_contract,
    a.total_repurchase_num,
    a.weighted_average_rate_by_contract,
    a.weighted_average_term_by_contract,
    a.loan_amount_accum,
    a.total_amount_accum,
    a.total_prin_accum,
    a.total_inter_accum,
    b.prepay_amount_accum,
    b.prepay_prin_accouum,
    b.prepay_inter_accum,
    a.conpensation_amount_accum,
    a.conpensation_prin_accum,
    a.conpensation_inter_accum,
    a.repurchase_amount_accum,
    a.repurchase_prin_accum,
    a.repurchase_inter_accum,
    a.refund_contract_amount_accum,
    a.refund_contract_num_accum,
    a.snapshot_date,
    a.project_id
from dm_eagle.report_dm_lx_asset_report_accu_comp a
left join dm_eagle.report_dm_lx_asset_report_accu_repayment b
  on a.project_id=b.project_id and a.snapshot_date=b.snapshot_date;

insert overwrite table dm_eagle.report_dm_lx_asset_report_snapshot_comp partition(snapshot_date,project_id)
select

    a.loan_amount,
    a.total_collection_amount,
    a.total_collection_prin,
    a.total_collection_inter,
    b.normal_amount,
    b.normal_prin,
    b.normal_inter,
    b.overdue_amount,
    b.overdue_prin,
    b.overdue_inter,
    b.prepay_amount,
    b.prepay_prin,
    b.prepay_inter,
    a.compensation_amount,
    a.compensation_prin,
    a.compensation_inter,
    a.repurchase_amount,
    a.repurchase_prin,
    a.repurchase_inter,
    a.refund_amount,
    a.snapshot_date,
    a.project_id
from dm_eagle.report_dm_lx_asset_report_snapshot_comp a
left join dm_eagle.report_dm_lx_asset_report_snapshot_repayment b
on a.project_id=b.project_id and a.snapshot_date=b.snapshot_date;



select
biz_conf.project_id,
loan_approval.biz_date                                                                                            as snapshot_date,
nvl(approval_rate,0)                                                                                              as approval_rate,
nvl(total_contract_amount,0)                                                                                      as total_contract_amount,
nvl(total_contract_num,0)                                                                                         as total_contract_num,
nvl(refund_contract_amount_accum,0)                                                                               as refund_contract_amount_accum,
nvl(contract_minus_refund_amount,0)                                                                               as contract_minus_refund_amount,
nvl(weighted_average_rate_by_contract,0)                                                                          as weighted_average_rate,
nvl(average_contract_amount,0)                                                                                    as average_contract_amount,
nvl(prepay_amount_accum,0)                                                                                        as prepay_amount_accum,
nvl(prepay_prin_accouum,0)                                                                                        as prepay_prin_accouum,
nvl(prepay_inter_accum,0)                                                                                         as prepay_inter_accum,
nvl(advance_settlement_rate,0)                                                                                    as advance_settlement_rate,
nvl(conpensation_amount_accum,0)                                                                                  as conpensation_amount_accum,
nvl(conpensation_prin_accum,0)                                                                                    as conpensation_prin_accum,
nvl(conpensation_inter_accum,0)                                                                                   as conpensation_inter_accum,
nvl(acc_buy_back_amount,0)                                                                                        as overdue180_buyback_amount,
nvl(overdue_remain_principal,0)                                                                                   as overdue1plus_remain_principal,
if(nvl(contract_minus_refund_amount,0)=0,0,nvl(overdue_remain_principal,0)/nvl(contract_minus_refund_amount,0))  as overdue1plus_rate
from
(
select distinct project_id from dim_new.biz_conf where channel_id='0006'
)biz_conf
left join
(

	select
	project_id,
	biz_date,
	max(loan_approval_num)/max(loan_apply_num) as approval_rate
	from
	(
	select project_id,
	sum(nvl(loan_approval_num,0))over(partition by project_id order  by biz_date) as loan_approval_num,
	sum(nvl(loan_apply_num,0))over(partition by project_id order  by biz_date) as loan_apply_num,
	from_unixtime(UNIX_TIMESTAMP(date_sub(biz_date,1)),'yyyy-MM-dd') as biz_date
	from
	dm_eagle.eagle_credit_loan_approval_rate_day
	)a
	where biz_date between $start_date$ and $end_date$
	group by project_id,biz_date

)loan_approval on biz_conf.project_id=loan_approval.project_id
left join (
select
	project_id,
	snapshot_date,
	total_contract_amount  ,
	total_contract_num,
	refund_contract_amount_accum  ,
	nvl(total_contract_amount,0)-nvl(refund_contract_amount_accum,0) as contract_minus_refund_amount,
	weighted_average_rate_by_contract,
	average_contract_amount,
	prepay_amount_accum,
	prepay_prin_accouum,
	prepay_inter_accum,
	prepay_amount_accum/(nvl(total_contract_amount,0)-nvl(refund_contract_amount_accum,0)) as advance_settlement_rate,
	conpensation_amount_accum,
	conpensation_prin_accum ,
	conpensation_inter_accum
	from  dm_eagle.report_dm_lx_asset_report_accu_comp
	where snapshot_date between $start_date$ and $end_date$
)report_accu  on biz_conf.project_id=report_accu.project_id and loan_approval.biz_date=report_accu.snapshot_date
left join (
	select
	project_id,
  snapshot_date,
	overdue_remain_principal,
	acc_buy_back_amount
	from
	dm_eagle.report_dm_lx_asset_statistics
	where snapshot_date between $start_date$ and $end_date$
)asset_statistics on biz_conf.project_id=asset_statistics.project_id and loan_approval.biz_date=asset_statistics.snapshot_date
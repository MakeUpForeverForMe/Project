drop table dw.dw_assets;
create table if not exists dw.dw_asset_comp(
	project_id				string 				comment '项目代码',
	remain_principal		decimal(15,2) 		comment '日终资产余额',
	loan_sum_daily			decimal(15,2) 		comment '放款金额',
	repay_sum_daily			decimal(15,2) 		comment '还款总金额',
	repay_principal			decimal(15,2)		comment '还款本金',
	repay_interest			decimal(15,2) 		comment '还款利息',
	repay_penalty			decimal(15,2)		comment '还款罚息',
	repay_fee				decimal(15,2) 		comment '还款费用',
	cust_repay_sum			decimal(15,2)		comment '客户还款总本金',
	cust_repay_principal	decimal(15,2)		comment '客户还款本金',
	cust_repay_interest		decimal(15,2) 		comment '客户还款利息',
	cust_repay_penalty		decimal(15,2)		comment '客户还款罚息',
	cust_repay_fee			decimal(15,2) 		comment '客户还款费用',
	comp_repay_sum			decimal(15,2) 		comment '代偿回款还款总金额',
	comp_repay_principal	decimal(15,2)		comment '代偿回款还款本金',
	comp_repay_interest		decimal(15,2) 		comment '代偿回款还款利息',
	comp_repay_penalty		decimal(15,2)		comment '代偿回款还款罚息',
	comp_repay_fee			decimal(15,2) 		comment '代偿回款还款费用',
	repo_repay_sum			decimal(15,2) 		comment '回购还款总金额',
	repo_repay_principal	decimal(15,2)		comment '回购还款本金',
	repo_repay_interest		decimal(15,2) 		comment '回购还款利息',
	repo_repay_penalty		decimal(15,2)		comment '回购还款罚息',
	repo_repay_fee			decimal(15,2) 		comment '回购还款费用',
	return_repay_sum		decimal(15,2) 		comment '银行退票总金额',
	return_repay_principal	decimal(15,2) 		comment '银行退票本金',
	return_repay_interest	decimal(15,2) 		comment '银行退票利息',
	return_repay_penalty	decimal(15,2) 		comment '银行退票罚息',
	return_repay_fee		decimal(15,2) 		comment '银行退票费用'
)COMMENT 'DW信托资产表(代偿)'
partitioned by (d_date STRING)
STORED AS PARQUET;

drop table dw.dw_assets;
create table if not exists dw.dw_asset(
	project_id				string 				comment '项目代码',
	remain_principal		decimal(15,2) 		comment '日终资产余额',
	loan_sum_daily			decimal(15,2) 		comment '放款金额',
	repay_sum_daily			decimal(15,2) 		comment '还款总金额',
	repay_principal			decimal(15,2)		comment '还款本金',
	repay_interest			decimal(15,2) 		comment '还款利息',
	repay_penalty			decimal(15,2)		comment '还款罚息',
	repay_fee				decimal(15,2) 		comment '还款费用',
	cust_repay_sum			decimal(15,2)		comment '客户还款总金额',
	cust_repay_principal	decimal(15,2)		comment '客户还款本金',
	cust_repay_interest		decimal(15,2) 		comment '客户还款利息',
	cust_repay_penalty		decimal(15,2)		comment '客户还款罚息',
	cust_repay_fee			decimal(15,2) 		comment '客户还款费用',
	back_repay_sum			decimal(15,2) 		comment '催回回款还款总金额',
	back_repay_principal	decimal(15,2)		comment '催回回款还款本金',
	back_repay_interest		decimal(15,2) 		comment '催回回款还款利息',
	back_repay_penalty		decimal(15,2)		comment '催回回款还款罚息',
	back_repay_fee			decimal(15,2) 		comment '催回回款还款费用'
)COMMENT 'DW信托资产表(剔除代偿)'
partitioned by (d_date STRING)
STORED AS PARQUET;


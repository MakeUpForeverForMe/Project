--无在途版-乐信资产变动信息(代偿版)(还款本金取T-2)
-- drop table if exists dm.dm_watch_asset_change_comp_t1;
create table if not exists dm.dm_watch_asset_change_comp_t1(
    project_id             string               comment '项目编号',
    yesterday_remain_sum   decimal(15,2)        comment '当日初资产余额:前一日的资产余额，空则为0',
    loan_sum_daily         decimal(15,2)        comment '当日新增放款金额',
    repay_sum_daily        decimal(15,2)        comment '前一日还款本金',
    repay_other_sum_daily  decimal(15,2)        comment '前一日资产还款息费',
    today_remain_sum       decimal(15,2)        comment '日终资产余额'
) comment '资产变动信息(代偿版)'
partitioned by (d_date string)
stored as parquet;

--无在途版-乐信资产变动信息(剔除代偿版)(还款本金取T-2)
-- drop table if exists dm.dm_watch_asset_change_t1;
create table if not exists dm.dm_watch_asset_change_t1(
    project_id             string               comment '项目编号',
    yesterday_remain_sum   decimal(15,2)        comment '当日初资产余额:前一日的资产余额，空则为0',
    loan_sum_daily         decimal(15,2)        comment '当日新增放款金额',
    repay_sum_daily        decimal(15,2)        comment '前一日还款本金',
    repay_other_sum_daily  decimal(15,2)        comment '前一日资产还款息费',
    today_remain_sum       decimal(15,2)        comment '日终资产余额'
) comment '资产变动信息(剔除代偿版)'
partitioned by (d_date string)
stored as parquet;

--有在途版-非乐信资产变动信息(代偿版)
-- drop table if exists dm.dm_watch_asset_change_comp;
create table if not exists dm.dm_watch_asset_change_comp(
    project_id             string               comment '项目编号',
    yesterday_remain_sum   decimal(15,2)        comment '当日初资产余额:前一日的资产余额，空则为0',
    loan_sum_daily         decimal(15,2)        comment '当日新增放款金额',
    repay_sum_daily        decimal(15,2)        comment '当日还款本金',
    repay_other_sum_daily  decimal(15,2)        comment '当日资产还款息费',
    today_remain_sum       decimal(15,2)        comment '日终资产余额'
) comment '资产变动信息(代偿版)'
partitioned by (d_date string)
stored as parquet;

--有在途版-非乐信资产变动信息(剔除代偿版)
-- drop table if exists dm.dm_watch_asset_change;
create table if not exists dm.dm_watch_asset_change(
    project_id             string               comment '项目编号',
    yesterday_remain_sum   decimal(15,2)        comment '当日初资产余额:前一日的资产余额，空则为0',
    loan_sum_daily         decimal(15,2)        comment '当日新增放款金额',
    repay_sum_daily        decimal(15,2)        comment '当日还款本金',
    repay_other_sum_daily  decimal(15,2)        comment '当日资产还款息费',
    today_remain_sum       decimal(15,2)        comment '日终资产余额'
) comment '资产变动信息(剔除代偿版)'
partitioned by (d_date string)
stored as parquet;

--拓展信息-到账信息(代偿版)
-- drop table if exists dm.dm_watch_asset_comp_info;
create table if not exists dm.dm_watch_asset_comp_info(
    project_id             string               comment '项目编号',
    repay_sum_daily        decimal(15,2)        comment '当日还款本金',
    repay_way              string               comment '到账方式:1-客户还款2-代偿回款3-回购回款4-代偿催回5-退票回款',
    amount_type            string               comment '金额类型:1-本金2-利息3-罚息4-费用',
    amount                 decimal(15,2)        comment '金额'
) comment '拓展信息-到账信息(代偿版)'
partitioned by (d_date string)
stored as parquet;

--资金账户信息
-- drop table if exists dm.dm_watch_funds;
create table if not exists dm.dm_watch_funds(
    project_id              string              comment '项目编号',
    trade_yesterday_bal     decimal(15,2)       comment '当日初账户余额',
    loan_today_bal          decimal(15,2)       comment '当日放款金额',
    repay_today_bal         decimal(15,2)       comment '当日到账回款金额:客户还款+代偿回款+回购回购+退票回款',
    acct_int                decimal(15,2)       comment '当日账户利息',
    acct_total_fee          decimal(15,2)       comment '当日费用支出',
    invest_cash             decimal(15,2)       comment '当日投资兑付金额',
    trade_today_bal         decimal(15,2)       comment 'T日余额'
) comment '资金账户信息'
partitioned by (d_date string)
stored as parquet;

--在途资金-代偿版
-- drop table if exists dm.dm_watch_unreach_funds;
create table if not exists dm.dm_watch_unreach_funds(
    project_id              string              comment '项目编号',
    trade_yesterday_bal     decimal(15,2)       comment '前一日末在途金额',
    repay_today_bal         decimal(15,2)       comment '当日到账在途金额',
    repay_sum_daily         decimal(15,2)       comment '当日新增在途金额',
    trade_today_bal         decimal(15,2)       comment '当日终在途金额'
) comment '在途资金-代偿版'
partitioned by (d_date string)
stored as parquet;


--当日费用支出-代偿版
-- drop table if exists dm.dm_watch_acct_cost;
create table if not exists dm.dm_watch_acct_cost(
    project_id              string              comment '项目代码',
    acct_total_fee          decimal(15,2)       comment '当日费用支出金额',
    fee_type                string              comment '费用类型：1-账户费用2-扣划手续费3-其他4-税费支持',
    amount                  decimal(15,2)       comment '对应项的类型及金额'
) comment '当日费用支出-代偿版'
partitioned by (d_date string)
stored as parquet;

--回款明细
-- drop table if exists dm.dm_watch_repayment_detail;
create table if not exists dm.dm_watch_repayment_detail(
    project_id              string              comment '项目代码',
    repay_today_bal         decimal(15,2)       comment '当日回款总金额:客户还款+代偿回款+回购回款+退票回款',
    repay_type              string              comment '费用类型：1-客户还款2-代偿回款3-回购回款4-退票回款',
    amount                  decimal(15,2)       comment '对应项的金额'
) comment '回款明细'
partitioned by (d_date string)
stored as parquet;

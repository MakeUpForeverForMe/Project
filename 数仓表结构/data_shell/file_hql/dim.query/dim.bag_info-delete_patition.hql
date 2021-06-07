-- 解包操作
-- dim 层
ALTER TABLE dim.bag_info                                          DROP IF EXISTS PARTITION (bag_id = '${exe_id}');
ALTER TABLE dim.bag_due_bill_no                                   DROP IF EXISTS PARTITION (bag_id = '${exe_id}');
-- 资产总体信息
ALTER TABLE dm_eagle.abs_asset_information_bag                    DROP IF EXISTS PARTITION (bag_id = '${exe_id}');
ALTER TABLE dm_eagle.abs_asset_information_bag_snapshot           DROP IF EXISTS PARTITION (bag_id = '${exe_id}');
-- 现金流分析
ALTER TABLE dm_eagle.abs_asset_information_cash_flow_bag_day      DROP IF EXISTS PARTITION (bag_id = '${exe_id}');
ALTER TABLE dm_eagle.abs_asset_information_cash_flow_bag_snapshot DROP IF EXISTS PARTITION (bag_id = '${exe_id}');
-- 分布
ALTER TABLE dm_eagle.abs_asset_distribution_bag_day               DROP IF EXISTS PARTITION (bag_id = '${exe_id}');
ALTER TABLE dm_eagle.abs_asset_distribution_bag_snapshot_day      DROP IF EXISTS PARTITION (bag_id = '${exe_id}');
-- 逾期
ALTER TABLE dm_eagle.abs_overdue_rate_day                         DROP IF EXISTS PARTITION (bag_id = '${exe_id}');
ALTER TABLE dm_eagle.abs_overdue_rate_details_day                 DROP IF EXISTS PARTITION (bag_id = '${exe_id}');
-- 早偿
ALTER TABLE dm_eagle.abs_early_payment_asset_statistic            DROP IF EXISTS PARTITION (bag_id = '${exe_id}');
ALTER TABLE dm_eagle.abs_early_payment_asset_details              DROP IF EXISTS PARTITION (bag_id = '${exe_id}');

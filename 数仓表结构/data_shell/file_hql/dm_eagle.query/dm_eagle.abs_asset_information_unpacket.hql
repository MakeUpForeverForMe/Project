alter table dm_eagle.abs_asset_information_bag drop partition(bag_id='${bag_id}');
alter table dm_eagle.abs_asset_information_bag_snapshot drop partition(bag_id='${bag_id}');
alter table dm_eagle.abs_asset_information_cash_flow_bag_snapshot drop partition(bag_id='${bag_id}');
alter table dm_eagle.abs_early_payment_asset_details drop partition(bag_id='${bag_id}');
alter table dm_eagle.abs_early_payment_asset_statistic drop partition(bag_id='${bag_id}');

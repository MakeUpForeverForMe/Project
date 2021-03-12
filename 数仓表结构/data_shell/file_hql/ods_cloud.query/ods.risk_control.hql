-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=128000000; -- 128M
set hive.merge.smallfiles.avgsize=128000000; -- 128M
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;


insert overwrite table ods.risk_control partition(product_id,source_table)
select
  3                              as risk_control_type,
  serial_number                  as apply_id,
  serial_number                  as due_bill_no,
  map(
    'status',                  '资产状态(-1征信异常 0准入 1在池 2封包 3发行 4历史',
    'wind_control_status',     '当前风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
    'wind_control_status_pool','入池时风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
    'cheat_level',             '反欺诈等级:1~5,-1:null,其它:异常值',
    'score_range',             '评分等级:1~20,-1:null,其它:异常值',
    'score_level',             '评分区间:1~5,-1:null,其它:异常值'
  )                              as map_comment,
  map(
    'status',                  status,
    'wind_control_status',     wind_control_status,
    'wind_control_status_pool',wind_control_status_pool,
    'cheat_level',             cheat_level,
    'score_range',             score_range,
    'score_level',             score_level
  )                              as map_value,
  create_time                    as create_time,
  update_time                    as update_time,
  project_id                     as product_id,
  't_asset_wind_control_history' as source_table
from stage.abs_t_asset_wind_control_history
where 1 > 0
  -- and to_date(update_time) = '${ST9}'
-- limit 10
;




insert overwrite table ods.risk_control partition(product_id,source_table)
select
  3                         as risk_control_type,
  apply_no                  as apply_id,
  apply_no                  as due_bill_no,
  map(
    'id',                 '主键Id',
    'req_id',             '请求Id',
    'creid_no',           '身份证号',
    'name',               '姓名',
    'mobile_phone',       '手机号',
    'credit_line',        '申请额度，以分为单位，取值范围（0，1000000000）',
    'ret_code',           '应答码',
    'ret_msg',            '应答信息',
    'status',             '查询结果状态（0：未查得，1：查得）',
    'rating_result',      '风控状态（Yes：风控通过，No：风控未通过，NA：风控未查得，Error：风控异常，Fail：调用风控失败）',
    'cheat_level',        '反欺诈等级（1到5，—1：null，其它：异常值）',
    'score_range',        '评分等级（1到20，—1：null，其它：异常值）',
    'score_level',        '评分区间（1到5，—1：null，其它：异常值）',
    'ret_content',        '风控结果报文',
    'wind_moment',        '风控时刻（0：入池时，1：资产跟踪）',
    'wind_interface_type','查询的风控接口类型（1：风控审核4号接口，2：风控结果查询新分享本地接口，3：风控结果查询5号接口，4：风控查询6号接口'
  )                         as map_comment,
  map(
    'id',                  id,
    'req_id',              req_id,
    'creid_no',            sha256(decrypt_aes(creid_no,    'tencentabs123456'),'idNumber',1),
    'name',                sha256(decrypt_aes(name,        'tencentabs123456'),'userName',1),
    'mobile_phone',        sha256(decrypt_aes(mobile_phone,'tencentabs123456'),'phone',   1),
    'credit_line',         credit_line,
    'ret_code',            ret_code,
    'ret_msg',             ret_msg,
    'status',              status,
    'rating_result',       rating_result,
    'cheat_level',         cheat_level,
    'score_range',         score_range,
    'score_level',         score_level,
    'ret_content',         ret_content,
    'ret_content',         ret_content,
    'wind_moment',         wind_moment,
    'wind_interface_type', wind_interface_type
  )                         as map_value,
  create_time               as create_time,
  update_time               as update_time,
  project_id                as product_id,
  't_wind_control_resp_log' as source_table
from stage.abs_t_wind_control_resp_log
where 1 > 0
  -- and to_date(update_time) = '${ST9}'
-- limit 10
;




-- 无数据，暂时不添加
-- insert overwrite table ods.risk_control partition(product_id,source_table)
-- select
--   3                                as risk_control_type,
--   serial_number                    as apply_id,
--   serial_number                    as due_bill_no,
--   map(
--     'asset_level',    '资产等级',
--     'credit_level',   '信用等级',
--     'antifraud_level','反欺诈等级',
--     'is_black_list',  '是否命中黑名单'
--   )                                as map_comment,
--   map(
--     'asset_level',    cast(asset_level     as string),
--     'credit_level',   cast(credit_level    as string),
--     'antifraud_level',cast(antifraud_level as string),
--     'is_black_list',  cast(is_black_list   as string)
--   )                                as map_value,
--   create_time                      as create_time,
--   update_time                      as update_time,
--   project_id                       as product_id,
--   't_duration_risk_control_result' as source_table
-- from stage.t_duration_risk_control_result
-- where 1 > 0
--   and to_date(update_time) = '${ST9}'
-- -- limit 10
-- ;

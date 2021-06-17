set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

-- 设置 Container 大小
set hive.tez.container.size=2048;
set tez.am.resource.memory.mb=2048;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=64000000;      -- 64M
set hive.merge.smallfiles.avgsize=64000000; -- 64M
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
  list_struct.map_key            as map_key,
  list_struct.map_com            as map_com,
  list_struct.map_val            as map_val,
  create_time                    as create_time,
  update_time                    as update_time,
  case project_id when 'Cl00333' then 'cl00333' else project_id end as project_id,
  't_asset_wind_control_history' as source_table
from stage.abs_t_asset_wind_control_history
lateral view explode(
  array(
    named_struct('map_key','status',                  'map_val',status,                  'map_com','资产状态（-1：征信异常，0：准入，1：在池，2：封包，3：发行，4：历史）'),
    named_struct('map_key','wind_control_status',     'map_val',wind_control_status,     'map_com','当前风控状态（Yes：风控通过，No：风控未通过，NA：风控未查得，Without：未跑风控）'),
    named_struct('map_key','wind_control_status_pool','map_val',wind_control_status_pool,'map_com','入池时风控状态（Yes：风控通过，No：风控未通过，NA：风控未查得，Without：未跑风控）'),
    named_struct('map_key','cheat_level',             'map_val',cheat_level,             'map_com','反欺诈等级（1~5，-1：null，其它：异常值）'),
    named_struct('map_key','score_range',             'map_val',score_range,             'map_com','评分等级（1~20，-1：null，其它：异常值）'),
    named_struct('map_key','score_level',             'map_val',score_level,             'map_com','评分区间（1~5，-1：null，其它：异常值）')
  )
) list as list_struct
where 1 > 0
  and list_struct.map_key is not null
  and list_struct.map_val is not null
  -- and to_date(update_time) = '${ST9}'
-- limit 10
;




insert overwrite table ods.risk_control partition(product_id,source_table)
select
  3                         as risk_control_type,
  apply_no                  as apply_id,
  apply_no                  as due_bill_no,
  list_struct.map_key       as map_key,
  list_struct.map_com       as map_com,
  list_struct.map_val       as map_val,
  create_time               as create_time,
  update_time               as update_time,
  case project_id when 'Cl00333' then 'cl00333' else project_id end as project_id,
  't_wind_control_resp_log' as source_table
from stage.abs_t_wind_control_resp_log
lateral view explode(
  array(
    named_struct('map_key','id',                 'map_val',id,                                                               'map_com','主键Id'),
    named_struct('map_key','req_id',             'map_val',req_id,                                                           'map_com','请求Id'),
    named_struct('map_key','creid_no',           'map_val',sha256(decrypt_aes(creid_no,    'tencentabs123456'),'idNumber',1),'map_com','身份证号'),
    named_struct('map_key','name',               'map_val',sha256(decrypt_aes(name,        'tencentabs123456'),'userName',1),'map_com','姓名'),
    named_struct('map_key','mobile_phone',       'map_val',sha256(decrypt_aes(mobile_phone,'tencentabs123456'),'phone',   1),'map_com','手机号'),
    named_struct('map_key','credit_line',        'map_val',credit_line,                                                      'map_com','申请额度，以分为单位，取值范围（0，1000000000）'),
    named_struct('map_key','ret_code',           'map_val',ret_code,                                                         'map_com','应答码'),
    named_struct('map_key','ret_msg',            'map_val',ret_msg,                                                          'map_com','应答信息'),
    named_struct('map_key','status',             'map_val',status,                                                           'map_com','查询结果状态（0：未查得，1：查得）'),
    named_struct('map_key','rating_result',      'map_val',rating_result,                                                    'map_com','风控状态（Yes：风控通过，No：风控未通过，NA：风控未查得，Error：风控异常，Fail：调用风控失败）'),
    named_struct('map_key','cheat_level',        'map_val',cheat_level,                                                      'map_com','反欺诈等级（1~5，—1：null，其它：异常值）'),
    named_struct('map_key','score_range',        'map_val',score_range,                                                      'map_com','评分等级（1~20，—1：null，其它：异常值）'),
    named_struct('map_key','score_level',        'map_val',score_level,                                                      'map_com','评分区间（1~5，—1：null，其它：异常值）'),
    named_struct('map_key','ret_content',        'map_val',ret_content,                                                      'map_com','风控结果报文'),
    named_struct('map_key','wind_moment',        'map_val',wind_moment,                                                      'map_com','风控时刻（0：入池时，1：资产跟踪）'),
    named_struct('map_key','wind_interface_type','map_val',wind_interface_type,                                              'map_com','查询的风控接口类型（1：风控审核4号接口，2：风控结果查询新分享本地接口，3：风控结果查询5号接口，4：风控查询6号接口）')
  )
) list as list_struct
where 1 > 0
  and list_struct.map_key is not null
  and list_struct.map_val is not null
  -- and to_date(update_time) = '${ST9}'
-- limit 10
;




insert overwrite table ods.risk_control partition(product_id,source_table)
select
  3                   as risk_control_type,
  apply_no            as apply_id,
  apply_no            as due_bill_no,
  list_struct.map_key as map_key,
  list_struct.map_com as map_com,
  list_struct.map_val as map_val,
  create_time         as create_time,
  update_time         as update_time,
  project_name        as project_id,
  'duration_result'   as source_table
from (
  select
    duration_result.*,
    case
      when loan_info_abs.overdue_days > 30 or (duration_result.black_4 = 0 and duration_result.black_3 = 1 and duration_result.black_2 = 1 and duration_result.black_1 = 1) then 2
      when (duration_result.score_1 between 17 and 20) and (duration_result.score_2 between 17 and 20) and (duration_result.score_3 between 17 and 20)                      then 1
      else 0
    end as monitoring_level
  from (
    select
      *,
      lag(inner_black,0,0) over(partition by project_name,apply_no order by execute_month) as black_1,
      lag(inner_black,1,0) over(partition by project_name,apply_no order by execute_month) as black_2,
      lag(inner_black,2,0) over(partition by project_name,apply_no order by execute_month) as black_3,
      lag(inner_black,3,0) over(partition by project_name,apply_no order by execute_month) as black_4,

      lag(score_range,0,0) over(partition by project_name,apply_no order by execute_month) as score_1,
      lag(score_range,1,0) over(partition by project_name,apply_no order by execute_month) as score_2,
      lag(score_range,2,0) over(partition by project_name,apply_no order by execute_month) as score_3
    from stage.duration_result
  ) as duration_result
  left join ods.loan_info_abs
  on  duration_result.project_id = loan_info_abs.project_id
  and duration_result.apply_no   = loan_info_abs.due_bill_no
  and duration_result.d_date between loan_info_abs.s_d_date and loan_info_abs.e_d_date
) as tmp
lateral view explode(
  array(
    named_struct('map_key','id',            'map_val',id,            'map_com','主键'),
    named_struct('map_key','request_id',    'map_val',request_id,    'map_com','存续期数据跑批申请表主键'),
    named_struct('map_key','swift_no',      'map_val',swift_no,      'map_com','流水号'),
    named_struct('map_key','name',          'map_val',name,          'map_com','姓名'),
    named_struct('map_key','card_no',       'map_val',card_no,       'map_com','身份证号码'),
    named_struct('map_key','mobile',        'map_val',mobile,        'map_com','手机号'),
    named_struct('map_key','is_settle',     'map_val',is_settle,     'map_com','是否已结清'),
    named_struct('map_key','execute_month', 'map_val',execute_month, 'map_com','执行月份（YYYY-MM）'),
    named_struct('map_key','score_range_t1','map_val',score_range_t1,'map_com','T-1月资产等级'),
    named_struct('map_key','score_range_t2','map_val',score_range_t2,'map_com','T-2月资产等级'),
    named_struct('map_key','score_range',   'map_val',score_range,   'map_com','资产等级'),
    named_struct('map_key','inner_black',   'map_val',inner_black,   'map_com','内部黑名单（0：未命中，1：命中）'),
    named_struct('map_key','focus',         'map_val',focus,         'map_com','关注名单（1：关注，0：非关注）'),
    named_struct('map_key','state',         'map_val',state,         'map_com','数据状态（0：无效，1：处理中，2：处理成功，3：处理失败）'),
    named_struct('map_key','error_msg',     'map_val',error_msg,     'map_com','失败原因')
  )
) list as list_struct
where 1 > 0
  and list_struct.map_key is not null
  and list_struct.map_val is not null
  -- and to_date(update_time) = '${ST9}'
-- limit 10
;

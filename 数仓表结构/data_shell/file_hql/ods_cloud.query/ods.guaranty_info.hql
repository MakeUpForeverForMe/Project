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

-- 设置可以使用正则匹配 `(a|b)?+.+`
set hive.support.quoted.identifiers=None;



insert overwrite table ods.guaranty_info partition(product_id)
select
  `(rn)?+.+`
from (
  select distinct
    is_empty(map_from_str(extra_info)['借据号'],asset_id)                                   as due_bill_no,
    is_empty(map_from_str(extra_info)['抵押物编号'],guaranty_umber)                         as guaranty_code,
    is_empty(map_from_str(extra_info)['抵押办理状态'],mortgage_handle_status)               as guaranty_handling_status,
    is_empty(map_from_str(extra_info)['抵押顺位'],mortgage_alignment)                       as guaranty_alignment,
    is_empty(map_from_str(extra_info)['车辆性质'])                                          as car_property,
    is_empty(map_from_str(extra_info)['融资方式'])                                          as financing_type,
    is_empty(
      map_from_str(extra_info)['担保方式'],
      case guaranty_type
        when 'CAR_MORTGAGE' then '抵押担保'
        else guaranty_type
      end
    )                                                                                       as guarantee_type,
    cast(is_empty(map_from_str(extra_info)['评估价格(元)'],0) as decimal(25,5))             as pawn_value,
    cast(is_empty(map_from_str(extra_info)['车辆销售价格(元)'],0) as decimal(25,5))         as car_sales_price,
    cast(is_empty(map_from_str(extra_info)['新车指导价(元)'],0) as decimal(25,5))           as car_new_price,
    cast(is_empty(map_from_str(extra_info)['投资总额(元)'],0) as decimal(25,5))             as total_investment,
    cast(is_empty(map_from_str(extra_info)['购置税金额(元)'],0) as decimal(25,5))           as purchase_tax_amouts,
    is_empty(map_from_str(extra_info)['保险种类'])                                          as insurance_type,
    cast(is_empty(map_from_str(extra_info)['汽车保险总费用'],0) as decimal(25,5))           as car_insurance_premium,
    cast(is_empty(map_from_str(extra_info)['手续总费用(元)'],0) as decimal(25,5))           as total_poundage,
    cast(is_empty(map_from_str(extra_info)['累计车辆过户次数'],0) as decimal(20,0))         as cumulative_car_transfer_number,
    cast(is_empty(map_from_str(extra_info)['一年内车辆过户次数'],0) as decimal(20,0))       as one_year_car_transfer_number,
    cast(is_empty(map_from_str(extra_info)['责信保费用1'],0) as decimal(25,5))              as liability_insurance_cost1,
    cast(is_empty(map_from_str(extra_info)['责信保费用2'],0) as decimal(25,5))              as liability_insurance_cost2,
    is_empty(map_from_str(extra_info)['车类型'],'未知')                                     as car_type,
    sha256(ptrim(map_from_str(extra_info)['车架号']),'frameNumber',1)                       as frame_num,
    sha256(ptrim(map_from_str(extra_info)['发动机号']),'engineNumber',1)                    as engine_num,
    is_empty(map_from_str(extra_info)['GPS编号'])                                           as gps_code,
    cast(is_empty(map_from_str(extra_info)['GPS费用'],0) as decimal(25,5))                  as gps_cost,
    sha256(ptrim(map_from_str(extra_info)['车牌号码']),'plateNumber',1)                     as license_num,
    is_empty(t_04_car_brand,map_from_str(extra_info)['车辆品牌'])                           as car_brand,
    is_empty(map_from_str(extra_info)['车系'])                                              as car_system,
    ptrim(is_empty(map_from_str(extra_info)['车型']))                                       as car_model,
    cast(is_empty(map_from_str(extra_info)['车龄'],0) as decimal(20,0))                     as car_age,
    is_empty(map_from_str(extra_info)['车辆能源类型'])                                      as car_energy_type,
    is_empty(map_from_str(extra_info)['生产日期'])                                          as production_date,
    cast(is_empty(map_from_str(extra_info)['里程数'],0) as decimal(25,5))                   as mileage,
    is_empty(map_from_str(extra_info)['注册日期'])                                          as register_date,
    is_empty(map_from_str(extra_info)['车辆购买地'])                                        as buy_car_address,
    is_empty(map_from_str(extra_info)['车辆颜色'])                                          as car_colour,
    datefmt(is_empty(map_from_str(extra_info)['TIMESTAMP']),'ms','yyyy-MM-dd HH:mm:dd.ddd') as ts,
    create_time                                                                             as create_time,
    update_time                                                                             as update_time,
    project_id_lower                                                                        as project_id,
    dense_rank() over(partition by project_id_lower,is_empty(map_from_str(extra_info)['借据号'],asset_id) order by datefmt(is_empty(map_from_str(extra_info)['TIMESTAMP']),'ms','yyyy-MM-dd HH:mm:dd.ddd') desc) as rn
  from (
    select
      *,
      case is_empty(map_from_str(extra_info)['项目编号'],project_id)
        when 'Cl00333' then 'cl00333'
        else is_empty(map_from_str(extra_info)['项目编号'],project_id)
      end as project_id_lower
    from stage.asset_04_t_guaranty_info
  ) as t_guaranty_info
  left join (
    select distinct
      case project_id
        when 'Cl00333' then 'cl00333'
        else project_id
      end           as t_04_project_id,
      serial_number as t_04_serial_number,
      guaranty_code as t_04_guaranty_code,
      car_brand     as t_04_car_brand
    from stage.abs_04_t_guarantycarinfo
  ) as stage_04
  on  project_id_lower                                                         = t_04_project_id
  and is_empty(map_from_str(extra_info)['借据号'],asset_id)                    = t_04_serial_number
  and nvl(is_empty(map_from_str(extra_info)['抵押物编号'],guaranty_umber),'a') = is_empty(t_04_guaranty_code,'a')
) as tmp
where rn = 1
-- limit 10
;

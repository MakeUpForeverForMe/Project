set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
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
-- 关闭自动 MapJoin （ Hive3 的 bug，引发 No work found for tablescan ）
set hive.auto.convert.join=false;
set hive.auto.convert.join.noconditionaltask=false;


-- set hivevar:ST9=2021-05-20;

-- set hivevar:bag_id=
--   select distinct bag_id
--   from dim.bag_info
--   where 1 > 0
-- ;


-- 封包时分母所有包
with
-- 获取到多种dpd的码值
dpds as (
  -- select '0'       as dpd union all
  select '1+'      as dpd union all
  select '7+'      as dpd union all
  select '14+'     as dpd union all
  select '30+'     as dpd union all
  select '60+'     as dpd union all
  select '90+'     as dpd union all
  select '120+'    as dpd union all
  select '150+'    as dpd union all
  select '1_7'     as dpd union all
  select '8_14'    as dpd union all
  select '15_30'   as dpd union all
  select '31_60'   as dpd union all
  select '61_90'   as dpd union all
  select '91_120'  as dpd union all
  select '121_150' as dpd union all
  select '151_180' as dpd union all
  select '180+'    as dpd
),

loan_due as (
  select
    dw_abs.biz_date                 as biz_date,
    dw_abs.project_id               as project_id,
    dw_abs.due_bill_no              as due_bill_no,
    dw_abs.user_hash_no             as user_hash_no,
    dw_abs.overdue_days             as overdue_days,
    dw_abs.overdue_days_dpd         as overdue_days_dpd,
    dw_abs.dpd_map                  as dpd_map,
    dw_abs.overdue_principal        as overdue_principal,
    dw_abs.overdue_remain_principal as overdue_remain_principal,
    dw_abs.overdue_due_bill_no      as overdue_due_bill_no,
    dw_abs.overdue_user_hash_no     as overdue_user_hash_no,
    bag_due.bag_date                as bag_date,
    bag_due.bag_id                  as bag_id
  from (
    select
      *
    from dw.abs_due_info_day_abs
    where 1 > 0
      and biz_date = '${ST9}'
  ) as dw_abs
  inner join (
    select
      bag_info.project_id,
      bag_info.bag_id,
      bag_info.bag_date,
      bag_due.due_bill_no
    from (
      select
        project_id,
        bag_id,
        bag_date
      from dim.bag_info
      where 1 > 0
        and bag_id in (${bag_id})
        and bag_date <= '${ST9}'
    ) as bag_info
    inner join (
      select
        project_id,
        bag_id,
        due_bill_no
      from dim.bag_due_bill_no
    ) as bag_due
    on  bag_info.project_id = bag_due.project_id
    and bag_info.bag_id     = bag_due.bag_id
  ) as bag_due
  on  dw_abs.project_id  = bag_due.project_id
  and dw_abs.due_bill_no = bag_due.due_bill_no
)

insert overwrite table dm_eagle.abs_overdue_rate_day partition(biz_date,project_id,bag_id)
select
  'y'                                                 as is_allbag,
  denominator.dpd_x                                   as dpd,

  denominator.remain_principal                        as remain_principal,
  nvl(molecular_curr.overdue_remain_principal,0)      as overdue_remain_principal,
  nvl(molecular_new.overdue_remain_principal_new,0)   as overdue_remain_principal_new,
  nvl(molecular_once.overdue_remain_principal_once,0) as overdue_remain_principal_once,

  denominator.bag_due_num                             as bag_due_num,
  nvl(molecular_curr.overdue_num,0)                   as overdue_num,
  nvl(molecular_new.overdue_num_new,0)                as overdue_num_new,
  nvl(molecular_once.overdue_num_once,0)              as overdue_num_once,

  denominator.bag_due_person_num                      as bag_due_person_num,
  nvl(molecular_curr.overdue_person_num,0)            as overdue_person_num,
  nvl(molecular_new.overdue_person_num_new,0)         as overdue_person_num_new,
  nvl(molecular_once.overdue_person_num_once,0)       as overdue_person_num_once,

  '${ST9}'                                            as biz_date,
  denominator.project_id                              as project_id,
  'default_all_bag'                                   as bag_id
from ( -- 分母
  select
    project_id,
    dpd as dpd_x,
    remain_principal,
    bag_due_num,
    bag_due_person_num
  from (select dpd from dpds) as dpds -- 多种dpd的码值
  full join ( -- 封包时分母所有包
    select
      bag_due.project_id                    as project_id,
      sum(bag_due.package_remain_principal) as remain_principal,
      count(bag_due.due_bill_no)            as bag_due_num,
      count(customer_info_abs.user_hash_no) as bag_due_person_num
    from (
      select * from dim.bag_due_bill_no
      where 1 > 0
        and bag_id in (${bag_id})
    ) as bag_due
    inner join ods.customer_info_abs
    on  bag_due.project_id  = customer_info_abs.project_id
    and bag_due.due_bill_no = customer_info_abs.due_bill_no
    group by bag_due.project_id
  ) as tmp
  where project_id is not null
) as denominator
left join ( -- 累计分子
  select -- 封包时累计分子所有包
    project_id,
    dpd_x,
    sum(overdue_remain_principal) as overdue_remain_principal_once,
    count(overdue_due_bill_no)    as overdue_num_once,
    count(overdue_user_hash_no)   as overdue_person_num_once
  from (
    select
      project_id,
      bag_id,
      due_bill_no,
      overdue_date_start,
      overdue_days_once,
      overdue_remain_principal_once as overdue_remain_principal,
      due_bill_no      as overdue_due_bill_no,
      user_hash_no     as overdue_user_hash_no,
      min(overdue_date_start) over(partition by project_id,bag_id,due_bill_no,overdue_days_once) as min_overdue_date_start
    from loan_due
    lateral view explode(dpd_map) dpd_maps as overdue_days_once,dpd_val
    lateral view explode(map_from_str(dpd_val)) dpd_vals as overdue_date_start,overdue_remain_principal_once
    where overdue_date_start >= bag_date
  ) as tmp
  lateral view explode(
    split(concat_ws(',',
      case overdue_days_once
        when 1   then '1+'
        when 8   then '7+'
        when 15  then '14+'
        when 31  then '30+'
        when 61  then '60+'
        when 91  then '90+'
        when 121 then '120+'
        when 151 then '150+'
        else '180+'
      end,
      case overdue_days_once
        when 1   then '1_7'
        when 8   then '8_14'
        when 15  then '15_30'
        when 31  then '31_60'
        when 61  then '61_90'
        when 91  then '91_120'
        when 121 then '121_150'
        when 151 then '151_180'
        else null
      end
    ),',')
  ) dpd as dpd_x
  where overdue_date_start = min_overdue_date_start
  group by project_id,dpd_x
) as molecular_once
on  denominator.project_id = molecular_once.project_id
and denominator.dpd_x      = molecular_once.dpd_x
left join ( -- 当前分子
  select -- 封包时当前分子所有包
    project_id,
    dpd_x,
    sum(overdue_remain_principal) as overdue_remain_principal,
    count(overdue_due_bill_no)    as overdue_num,
    count(overdue_user_hash_no)   as overdue_person_num
  from loan_due
  lateral view explode(
    split(concat_ws(',',
      if(overdue_days >= 1, '1+',  null),
      if(overdue_days > 7,  '7+',  null),
      if(overdue_days > 14, '14+', null),
      if(overdue_days > 30, '30+', null),
      if(overdue_days > 60, '60+', null),
      if(overdue_days > 90, '90+', null),
      if(overdue_days > 120,'120+',null),
      if(overdue_days > 150,'150+',null),
      if(overdue_days > 180,'180+',null),
      case
        when overdue_days > 180 then null
        when overdue_days > 150 then '151_180'
        when overdue_days > 120 then '121_150'
        when overdue_days > 90  then '91_120'
        when overdue_days > 60  then '61_90'
        when overdue_days > 30  then '31_60'
        when overdue_days > 14  then '15_30'
        when overdue_days > 7   then '8_14'
        when overdue_days > 0   then '1_7'
        else null
      end
    ),',')) dpd as dpd_x
  where 1 > 0
    and overdue_days > 0
  group by project_id,dpd_x
) as molecular_curr
on  denominator.project_id = molecular_curr.project_id
and denominator.dpd_x      = molecular_curr.dpd_x
left join ( -- 新增分子
  select -- 封包时新增分子所有包
    project_id,
    dpd_x,
    sum(overdue_remain_principal) as overdue_remain_principal_new,
    count(overdue_due_bill_no)    as overdue_num_new,
    count(overdue_user_hash_no)   as overdue_person_num_new
  from loan_due
  lateral view explode(
    split(concat_ws(',',
      case overdue_days
        when 1   then '1+'
        when 8   then '7+'
        when 15  then '14+'
        when 31  then '30+'
        when 61  then '60+'
        when 91  then '90+'
        when 121 then '120+'
        when 151 then '150+'
        else '180+'
      end,
      case overdue_days
        when 1   then '1_7'
        when 8   then '8_14'
        when 15  then '15_30'
        when 31  then '31_60'
        when 61  then '61_90'
        when 91  then '91_120'
        when 121 then '121_150'
        when 151 then '151_180'
        else null
      end
    ),',')
  ) dpd as dpd_x
  lateral view explode(dpd_map) dpd_maps as overdue_days_once,dpd_val
  where 1 > 0
    and overdue_days in (1,8,15,31,61,91,121,151,181)
    and overdue_days = overdue_days_once
    and size(map_keys(map_from_str(dpd_val))) = 1
    and map_keys(map_from_str(dpd_val))[0] = date_sub(biz_date,cast(overdue_days as int) - 1)
  group by project_id,dpd_x
) as molecular_new
on  denominator.project_id = molecular_new.project_id
and denominator.dpd_x      = molecular_new.dpd_x
union all
select
  'n'                                                 as is_allbag,
  denominator.dpd_x                                   as dpd,

  denominator.remain_principal                        as remain_principal,
  nvl(molecular_curr.overdue_remain_principal,0)      as overdue_remain_principal,
  nvl(molecular_new.overdue_remain_principal_new,0)   as overdue_remain_principal_new,
  nvl(molecular_once.overdue_remain_principal_once,0) as overdue_remain_principal_once,

  denominator.bag_due_num                             as bag_due_num,
  nvl(molecular_curr.overdue_num,0)                   as overdue_num,
  nvl(molecular_new.overdue_num_new,0)                as overdue_num_new,
  nvl(molecular_once.overdue_num_once,0)              as overdue_num_once,

  denominator.bag_due_person_num                      as bag_due_person_num,
  nvl(molecular_curr.overdue_person_num,0)            as overdue_person_num,
  nvl(molecular_new.overdue_person_num_new,0)         as overdue_person_num_new,
  nvl(molecular_once.overdue_person_num_once,0)       as overdue_person_num_once,

  '${ST9}'                                            as biz_date,
  denominator.project_id                              as project_id,
  denominator.bag_id                                  as bag_id
from ( -- 分母
  select
    project_id,
    bag_id,
    dpd as dpd_x,
    remain_principal,
    bag_due_num,
    bag_due_person_num
  from (select dpd from dpds) as dpds -- 多种dpd的码值
  full join ( -- 封包时分母单个包
    select
      bag_due.project_id                    as project_id,
      bag_due.bag_id                        as bag_id,
      sum(bag_due.package_remain_principal) as remain_principal,
      count(bag_due.due_bill_no)            as bag_due_num,
      count(customer_info_abs.user_hash_no) as bag_due_person_num
    from (
      select * from dim.bag_due_bill_no
      where 1 > 0
        and bag_id in (${bag_id})
    ) as bag_due
    inner join ods.customer_info_abs
    on  bag_due.project_id  = customer_info_abs.project_id
    and bag_due.due_bill_no = customer_info_abs.due_bill_no
    group by bag_due.project_id,bag_due.bag_id
  ) as tmp
  where project_id is not null
) as denominator
left join ( -- 累计分子
  select -- 封包时累计分子单个包
    project_id,
    bag_id,
    dpd_x,
    sum(overdue_remain_principal) as overdue_remain_principal_once,
    count(overdue_due_bill_no)    as overdue_num_once,
    count(overdue_user_hash_no)   as overdue_person_num_once
  from  (
    select
      project_id,
      bag_id,
      due_bill_no,
      overdue_date_start,
      overdue_days_once,
      overdue_remain_principal_once as overdue_remain_principal,
      due_bill_no      as overdue_due_bill_no,
      user_hash_no     as overdue_user_hash_no,
      min(overdue_date_start) over(partition by project_id,bag_id,due_bill_no,overdue_days_once) as min_overdue_date_start
    from loan_due
    lateral view explode(dpd_map) dpd_maps as overdue_days_once,dpd_val
    lateral view explode(map_from_str(dpd_val)) dpd_vals as overdue_date_start,overdue_remain_principal_once
    where overdue_date_start >= bag_date
  ) as tmp
  lateral view explode(
    split(concat_ws(',',
      case overdue_days_once
        when 1   then '1+'
        when 8   then '7+'
        when 15  then '14+'
        when 31  then '30+'
        when 61  then '60+'
        when 91  then '90+'
        when 121 then '120+'
        when 151 then '150+'
        else '180+'
      end,
      case overdue_days_once
        when 1   then '1_7'
        when 8   then '8_14'
        when 15  then '15_30'
        when 31  then '31_60'
        when 61  then '61_90'
        when 91  then '91_120'
        when 121 then '121_150'
        when 151 then '151_180'
        else null
      end
    ),',')
  ) dpd as dpd_x
  where overdue_date_start = min_overdue_date_start
  group by project_id,bag_id,dpd_x
) as molecular_once
on  denominator.project_id = molecular_once.project_id
and denominator.bag_id     = molecular_once.bag_id
and denominator.dpd_x      = molecular_once.dpd_x
left join ( -- 当前分子
  select -- 封包时当前分子单个包
    project_id,
    bag_id,
    dpd_x,
    sum(overdue_remain_principal) as overdue_remain_principal,
    count(overdue_due_bill_no)    as overdue_num,
    count(overdue_user_hash_no)   as overdue_person_num
  from loan_due
  lateral view explode(
    split(concat_ws(',',
      if(overdue_days >= 1, '1+',  null),
      if(overdue_days > 7,  '7+',  null),
      if(overdue_days > 14, '14+', null),
      if(overdue_days > 30, '30+', null),
      if(overdue_days > 60, '60+', null),
      if(overdue_days > 90, '90+', null),
      if(overdue_days > 120,'120+',null),
      if(overdue_days > 150,'150+',null),
      if(overdue_days > 180,'180+',null),
      case
        when overdue_days > 180 then null
        when overdue_days > 150 then '151_180'
        when overdue_days > 120 then '121_150'
        when overdue_days > 90  then '91_120'
        when overdue_days > 60  then '61_90'
        when overdue_days > 30  then '31_60'
        when overdue_days > 14  then '15_30'
        when overdue_days > 7   then '8_14'
        when overdue_days > 0   then '1_7'
        else null
      end
    ),',')) dpd as dpd_x
  where 1 > 0
    and overdue_days > 0
  group by project_id,bag_id,dpd_x
) as molecular_curr
on  denominator.project_id = molecular_curr.project_id
and denominator.bag_id     = molecular_curr.bag_id
and denominator.dpd_x      = molecular_curr.dpd_x
left join ( -- 新增分子
  select -- 封包时新增分子单个包
    project_id,
    bag_id,
    dpd_x,
    sum(overdue_remain_principal) as overdue_remain_principal_new,
    count(overdue_due_bill_no)    as overdue_num_new,
    count(overdue_user_hash_no)   as overdue_person_num_new
  from loan_due
  lateral view explode(
    split(concat_ws(',',
      case overdue_days
        when 1   then '1+'
        when 8   then '7+'
        when 15  then '14+'
        when 31  then '30+'
        when 61  then '60+'
        when 91  then '90+'
        when 121 then '120+'
        when 151 then '150+'
        else '180+'
      end,
      case overdue_days
        when 1   then '1_7'
        when 8   then '8_14'
        when 15  then '15_30'
        when 31  then '31_60'
        when 61  then '61_90'
        when 91  then '91_120'
        when 121 then '121_150'
        when 151 then '151_180'
        else null
      end
    ),',')
  ) dpd as dpd_x
  lateral view explode(dpd_map) dpd_maps as overdue_days_once,dpd_val
  where 1 > 0
    and overdue_days in (1,8,15,31,61,91,121,151,181)
    and overdue_days = overdue_days_once
    and size(map_keys(map_from_str(dpd_val))) = 1
    and map_keys(map_from_str(dpd_val))[0] = date_sub(biz_date,cast(overdue_days as int))
  group by project_id,bag_id,dpd_x
) as molecular_new
on  denominator.project_id = molecular_new.project_id
and denominator.bag_id     = molecular_new.bag_id
and denominator.dpd_x      = molecular_new.dpd_x
-- order by project_id,bag_id,cast(substring(dpd_x,0,if(dpd_x = 0,length(dpd_x),instr(dpd_x,'+') - 1)) as int),is_allbag
-- limit 10
;

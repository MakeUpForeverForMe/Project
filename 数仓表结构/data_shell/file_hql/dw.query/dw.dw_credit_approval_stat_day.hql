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
--set hivevar:ST9=2021-05-17;


set hivevar:days=3; -- 按照动支率的查看天数取值

insert overwrite table dw.dw_credit_approval_stat_day partition(biz_date,product_id)
select
  credit_count.credit_approval_date                    as credit_approval_date,             -- 授信通过日期
  nvl(credit_loan.credit_approval_num,0)               as credit_approval_num,              -- 授信通过笔数
  nvl(credit_count.credit_approval_num_count,0)        as credit_approval_num_count,        -- 累计授信通过笔数
  nvl(credit_loan.credit_approval_num_person,0)        as credit_approval_num_person,       -- 授信通过人数
  nvl(credit_count.credit_approval_num_person_count,0) as credit_approval_num_person_count, -- 累计授信通过人数
  nvl(credit_loan.credit_approval_amount,0)            as credit_approval_amount,           -- 授信通过金额
  nvl(credit_count.credit_approval_amount_count,0)     as credit_approval_amount_count,     -- 累计授信通过金额
  credit_count.biz_date                                    as loan_approval_date,               -- 用信通过日期
  nvl(credit_loan.loan_approval_num,0)                 as credit_loan_approval_num,         -- 用信通过笔数
  nvl(credit_loan.loan_approval_person,0)              as credit_loan_approval_person,      -- 用信通过人数
  nvl(credit_loan.loan_approval_num_amount,0)          as credit_loan_approval_num_amount,  -- 用信通过金额
  credit_count.credit_approval_date                    as biz_date,                         -- 授信通过日期
  credit_count.product_id                              as product_id                        -- 产品编号
from (-- 求累计到观察日的授信统计
  select
  product_id,
  credit_approval_date,
  credit_approval_num_count,
  credit_approval_num_person_count,
  credit_approval_amount_count,
  dim_date.biz_date
  from
  (
  select
    product_id          as product_id,
    '${ST9}'            as credit_approval_date,
    count(apply_id)     as credit_approval_num_count,
    count(user_hash_no) as credit_approval_num_person_count,
    sum(credit_amount)  as credit_approval_amount_count
  from (
    select distinct
      user_hash_no,
      apply_id,
      credit_amount,
      product_id
    from ods.credit_apply
    where 1 > 0
      and resp_code = '1'
      and to_date(credit_approval_time) <= '${ST9}'
      and (
        case
          when product_id = 'pl00282' and to_date(credit_approval_time) > '2019-02-22' then false
          else true
        end
      )
      ${hive_param_str}
  ) as tmp
  group by product_id
) as credit_count_tmp
left join (
  select  biz_date, '${ST9}' as observ_day
  from dim.dim_date
  where 1 > 0
    and biz_date between '${ST9}' and date_add('${ST9}',${days})
  group by biz_date
) as dim_date
on credit_count_tmp.credit_approval_date = dim_date.observ_day
) credit_count
left join ( -- 授信当天，用信 t+0，t+1，t+2，t+3 天的统计
  select
    credit_apply.product_id            as credit_product_id,

    credit_apply.credit_approval_date  as credit_approval_date,
    count(credit_apply.apply_id)       as credit_approval_num,
    count(credit_apply.user_hash_no)   as credit_approval_num_person,
    sum(credit_apply.credit_amount)    as credit_approval_amount,

    loan_apply.loan_approval_date      as loan_approval_date,
    count(loan_apply.apply_id)         as loan_approval_num,
    count(loan_apply.user_hash_no)     as loan_approval_person,
    sum(nvl(loan_apply.loan_amount,0)) as loan_approval_num_amount
  from (
    select distinct
      user_hash_no,
      apply_id,
      to_date(credit_approval_time) as credit_approval_date,
      credit_amount,
      product_id
    from ods.credit_apply
    where 1 > 0
      and resp_code = '1'
      and to_date(credit_approval_time) = '${ST9}'
      and (
        case
          when product_id = 'pl00282' and to_date(credit_approval_time) > '2019-02-22' then false
          else true
        end
      )
      ${hive_param_str}
  ) as credit_apply
  left join (
    select distinct
      user_hash_no,
      apply_id,
      to_date(issue_time) as loan_approval_date,
      loan_amount,
      product_id
    from ods.loan_apply
    where 1 > 0
      and product_id in (select distinct product_id from ods.credit_apply)
      and apply_status in (1,4)
      and to_date(issue_time) between '${ST9}' and date_add('${ST9}',${days})
      and (
        case
          when product_id = 'pl00282' and to_date(issue_time) > '2019-02-22' then false
          else true
        end
      )
      ${hive_param_str}
  ) as loan_apply
  on  credit_apply.product_id = loan_apply.product_id
  and credit_apply.apply_id   = loan_apply.apply_id
  group by credit_apply.product_id,credit_apply.credit_approval_date,loan_apply.loan_approval_date
) as credit_loan
on  credit_count.product_id = credit_loan.credit_product_id
and credit_count.biz_date   = credit_loan.loan_approval_date
-- limit 10
;

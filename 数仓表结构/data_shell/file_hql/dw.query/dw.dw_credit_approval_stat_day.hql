set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.auto.convert.join=false;
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
 

set hivevar:days=3; -- 按照动支率的查看天数取值

insert overwrite table dw.dw_credit_approval_stat_day partition(biz_date = '${ST9}',product_id)
select
  t1.credit_approval_date                    as credit_approval_date,             --'授信通过日期'
  nvl(t2.credit_approval_num,0)              as credit_approval_num,              --'授信通过笔数'
  nvl(t1.credit_approval_num_count,0)        as credit_approval_num_count,        --'累计授信通过笔数'
  nvl(t2.credit_approval_num_person,0)       as credit_approval_num_person,       --'授信通过人数'
  nvl(t1.credit_approval_num_person_count,0) as credit_approval_num_person_count, --'累计授信通过人数'
  nvl(t2.credit_approval_amount,0)           as credit_approval_amount,           --'授信通过金额'
  nvl(t1.credit_approval_amount_count,0)     as credit_approval_amount_count,     --'累计授信通过金额'
  t3.biz_date                                as loan_approval_date,               --'用信通过日期'
  nvl(t3.credit_loan_approval_num,0)         as credit_loan_approval_num,         --'用信通过笔数'
  nvl(t3.credit_loan_approval_person,0)      as credit_loan_approval_person,      --'用信通过人数'
  nvl(t3.credit_loan_approval_num_amount,0)  as credit_loan_approval_num_amount,  --'用信通过金额'
  t1.product_id                              as product_id                        --'产品编号'
from (  --求累计到观察日的授信统计
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
  ) as tmp
  group by product_id
) as t1
left join ( --观察日的授信情况
  select
    product_id,
    credit_approval_date,
    count(apply_id)     as credit_approval_num,
    count(user_hash_no) as credit_approval_num_person,
    sum(credit_amount)  as credit_approval_amount
  from (
    select distinct
      user_hash_no,
      apply_id,
      '${ST9}' as credit_approval_date,
      credit_amount,
      product_id
    from ods.credit_apply
    where 1 > 0
      and resp_code = '1'
      and to_date(credit_approval_time) = '${ST9}'
  ) as credit_apply
  group by product_id,credit_approval_date
) as t2
on t1.product_id = t2.product_id
left join ( --授信当天用信t-0，t-1，t-2，t-3 天的统计
  select
    d.observ_day,
    d.biz_date,
    d.product_id,
    c.credit_loan_approval_num,
    c.credit_loan_approval_person,
    c.credit_loan_approval_num_amount
  from (
    select
      '${ST9}' as observ_day,
      biz_date,
      product_id
    from dim.dim_date
    where 1 > 0
      and biz_date between '${ST9}' and date_add('${ST9}',3)
  ) as d
  left join (
    select
      b.product_id,
      b.loan_approval_date,
      count(b.apply_id)     as credit_loan_approval_num,
      count(b.user_hash_no) as credit_loan_approval_person,
      sum(b.loan_amount)    as credit_loan_approval_num_amount
    from (
      select
        distinct
        user_hash_no,
        apply_id,
        '${ST9}' as credit_approval_date,
        credit_amount,
        product_id
      from ods.credit_apply
      where 1 > 0
        and resp_code = '1'
        and credit_approval_time = '${ST9}'
    ) as a
    join (
      select
        distinct
        user_hash_no,
        apply_id,
        to_date(issue_time) as loan_approval_date,
        loan_amount,
        product_id
      from ods.loan_apply
      where 1 > 0
        and apply_status in (1,4)
        and to_date(issue_time) between '${ST9}' and date_add('${ST9}',3)
    ) as b
    on a.apply_id = b.apply_id
    and a.product_id = b.product_id
    group by b.loan_approval_date,b.product_id
  ) as c
  on c.loan_approval_date = d.biz_date
  and c.product_id        = d.product_id
) as t3
on t1.credit_approval_date = t3.observ_day
and t1.product_id          = t3.product_id
;

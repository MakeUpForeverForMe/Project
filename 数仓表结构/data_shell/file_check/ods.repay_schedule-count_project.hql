-- 关闭自动 MapJoin （ Hive3 的 bug，引发 No work found for tablescan ）
-- set hive.auto.convert.join=false;
-- set hive.auto.convert.join.noconditionaltask=false;


select
  'ods stage repay_schedule count'                                              as title,
  nvl(ods_inter.project_id,ods.project_id)                                      as project_id,
  -- nvl(ods_inter.loan_term,ods.loan_term)                                        as loan_term,
  -- nvl(ods_inter.should_repay_date,ods.should_repay_date)                        as should_repay_date,
  nvl(ods_inter.cnt,0)                                                          as cnt_ods_inter,
  nvl(ods.cnt,0)                                                                as cnt_ods,
  nvl(ods_inter.cnt,0) - nvl(ods.cnt,0)                                         as cnt_diff,
  nvl(ods_inter.should_repay_principal,0)                                       as principal_ods_inter,
  nvl(ods.should_repay_principal,0)                                             as principal_ods,
  nvl(ods_inter.should_repay_principal,0) - nvl(ods.should_repay_principal,0)   as principal_diff
from (
  select
    project_id,
    -- loan_term,
    -- should_repay_date,
    count(due_bill_no) as cnt,
    sum(should_repay_principal) as should_repay_principal
  from (
    select
      t3.project_id,
      t1.loan_term,
      t1.should_repay_date,
      t1.biz_date as s_d_date,
      lead(t1.biz_date,1,'3000-12-31') over(partition by t3.project_id,t1.due_bill_no,t1.loan_term,t1.should_repay_date order by t1.biz_date) as e_d_date,
      t1.should_repay_principal,
      t1.due_bill_no
    from ods.repay_schedule_inter as t1
    join dim.project_due_bill_no as t3
    on  t1.product_id  = t3.partition_id
    and t1.due_bill_no = t3.due_bill_no
  ) as tmp
  where 1 > 0
    and current_date() between s_d_date and date_sub(e_d_date,1)
    -- and project_id = 'DIDI201908161538'
    and project_id not in (
      '001601',           -- 汇通
      'WS0005200001',     -- 瓜子
      -- 'CL202012280092',   -- 汇通国银新增
      'DIDI201908161538', -- 滴滴

      'CL201912100072', -- Excel导入
      'PL201905080051', -- Excel导入
      'CL201912260074', -- Excel导入
      'CL201905310055', -- Excel导入
      'CL202003200082', -- Excel导入
      'CL201906040057', -- Excel导入
      'CL201906040058', -- Excel导入
      'CL201905220053', -- Excel导入
      'CL201912170073', -- Excel导入
      'PL201908210066', -- Excel导入
      'PL201904110050', -- Excel导入
      'CL201906050059', -- Excel导入
      'CL201906040056', -- Excel导入

      'CL201905240054', -- 沣邦租赁
      ''
    )
  group by project_id
  -- ,should_repay_date,loan_term
) as ods_inter
full join (
  select
    project_id,
    -- loan_term,
    -- should_repay_date,
    count(due_bill_no) as cnt,
    sum(should_repay_principal) as should_repay_principal
  from ods.repay_schedule_abs
  where 1 > 0
    and current_date() between s_d_date and date_sub(e_d_date,1)
    -- and project_id = 'DIDI201908161538'
    and project_id not in (
      '001601',           -- 汇通
      'WS0005200001',     -- 瓜子
      -- 'CL202012280092',   -- 汇通国银新增
      'DIDI201908161538', -- 滴滴

      'CL201912100072', -- Excel导入
      'PL201905080051', -- Excel导入
      'CL201912260074', -- Excel导入
      'CL201905310055', -- Excel导入
      'CL202003200082', -- Excel导入
      'CL201906040057', -- Excel导入
      'CL201906040058', -- Excel导入
      'CL201905220053', -- Excel导入
      'CL201912170073', -- Excel导入
      'PL201908210066', -- Excel导入
      'PL201904110050', -- Excel导入
      'CL201906050059', -- Excel导入
      'CL201906040056', -- Excel导入

      'CL201905240054', -- 沣邦租赁
      ''
    )
  group by project_id
  -- ,should_repay_date,loan_term
) as ods
on  ods_inter.project_id        = ods.project_id
-- and ods_inter.loan_term         = ods.loan_term
-- and ods_inter.should_repay_date = ods.should_repay_date
where 1 > 0
  -- and nvl(ods_inter.loan_term,ods.loan_term) != 0
  and (
    nvl(ods_inter.cnt,0) - nvl(ods.cnt,0) != 0 or
    nvl(ods_inter.should_repay_principal,0) - nvl(ods.should_repay_principal,0) != 0
  )
order by project_id
-- ,loan_term,should_repay_date
,cnt_diff
limit 100
;

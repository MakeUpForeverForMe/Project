-- 关闭自动 MapJoin （ Hive3 的 bug，引发 No work found for tablescan ）
-- set hive.auto.convert.join=false;
-- set hive.auto.convert.join.noconditionaltask=false;


select
  'ods stage repay_schedule_inter count'                                  as title,
  nvl(ods.project_id,stage.project_id)                                    as project_id,
  nvl(ods.loan_term,stage.loan_term)                                      as loan_term,
  nvl(ods.should_repay_date,stage.should_repay_date)                      as should_repay_date,
  nvl(ods.cnt,0)                                                          as cnt_ods,
  nvl(stage.cnt,0)                                                        as cnt_stage,
  nvl(ods.cnt,0) - nvl(stage.cnt,0)                                       as cnt_diff,
  nvl(ods.should_repay_principal,0)                                       as principal_ods,
  nvl(stage.should_repay_principal,0)                                     as principal_stage,
  nvl(ods.should_repay_principal,0) - nvl(stage.should_repay_principal,0) as principal_diff
from (
  select
    project_id,
    loan_term,
    should_repay_date,
    count(due_bill_no) as cnt,
    sum(should_repay_principal) as should_repay_principal
  from (
    select
      t3.project_id,
      t1.loan_init_principal,
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
    left join (
      select
        product_id,
        due_bill_no,
        biz_date,
        lead(loan_init_term,1,loan_init_term) over(partition by product_id,due_bill_no order by loan_init_term desc) as after_init_term,
        lead(biz_date,1,biz_date) over(partition by product_id,due_bill_no order by biz_date) as after_biz_date
      from (
        select distinct
          due_bill_no,
          loan_init_term,
          biz_date,
          product_id
      from ods.repay_schedule_inter
      ) as tmp
    ) as t2
    on  t1.product_id  = t2.product_id
    and t1.due_bill_no = t2.due_bill_no
    and t1.biz_date    = t2.biz_date
  ) as tmp
  where 1 > 0
    and if('${ST9}' = '\$\{ST9\}',if(current_timestamp > concat(current_date,' ','18:00:00'),date_sub(current_date,1),date_sub(current_date,2)),'${ST9}') between s_d_date and date_sub(e_d_date,1)
    and loan_init_principal > loan_term
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
  group by project_id,should_repay_date,loan_term
) as ods
full join (
  select
    t3.project_id,
    t1.loan_term,
    t1.should_repay_date,
    count(t1.due_bill_no) as cnt,
    cast(nvl(sum(t1.should_repay_principal),0) as decimal(20,5)) as should_repay_principal
  from (
    select distinct
      case is_empty(map_from_str(extra_info)['项目编号'],project_id)
        when 'Cl00333' then 'cl00333'
        else is_empty(map_from_str(extra_info)['项目编号'],project_id)
      end                                                                as project_id,
      is_empty(map_from_str(extra_info)['借据号'],asset_id)              as due_bill_no,
      is_empty(map_from_str(extra_info)['期次'],period)                  as loan_term,
      is_empty(map_from_str(extra_info)['应还款日'],repay_date)          as should_repay_date,
      is_empty(map_from_str(extra_info)['应还本金(元)'],repay_principal) as should_repay_principal
    from stage.asset_05_t_repayment_schedule
    where 1 > 0
      and d_date = if('${ST9}' = '\$\{ST9\}',if(current_timestamp > concat(current_date,' ','18:00:00'),date_sub(current_date,1),date_sub(current_date,2)),'${ST9}')
  ) as t1
  join dim.project_due_bill_no as t3
  on  t1.project_id  = nvl(t3.related_project_id,t3.project_id)
  and t1.due_bill_no = t3.due_bill_no
  where t3.project_id not in (
      '001601',           -- 汇通
      'WS0005200001',     -- 瓜子
      -- 'CL202012280092',   -- 汇通国银
      'DIDI201908161538', -- 滴滴
      ''
    )
  group by t3.project_id,t1.should_repay_date,t1.loan_term
) as stage
on  ods.project_id        = stage.project_id
and ods.loan_term         = stage.loan_term
and ods.should_repay_date = stage.should_repay_date
where 1 > 0
  and nvl(ods.loan_term,stage.loan_term) != 0
  -- and nvl(ods.project_id,stage.project_id) not in (
  --   -- '001503',
  --   -- '001505',
  --   -- '001504',
  --   ''
  -- )
  and nvl(ods.project_id,stage.project_id) in (
    'cl00297',
    'cl00306',
    'cl00309',
    'CL201911130070',
    'CL202002240081',

    'CL202012280092',

    'CL202011090089',
    ''
  )
  and (
    nvl(ods.cnt,0) - nvl(stage.cnt,0) != 0 or
    nvl(ods.should_repay_principal,0) - nvl(stage.should_repay_principal,0) != 0
  )
order by project_id,loan_term,should_repay_date,cnt_diff
limit 100
;

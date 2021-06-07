-- select '${var:ST9}' as biz_date,if('${var:db_suffix}' = '','代偿前','代偿后') as cps;

invalidate metadata stage.ecas_loan${var:tb_suffix};
invalidate metadata stage.ecas_repay_hst${var:tb_suffix};
invalidate metadata ods${var:db_suffix}.repay_detail;
-- stage 实还 与 ods 实还 对比 借据数、实还金额   以 交易日期 分组
select
  'stage 实还 与 ods 实还 对比 借据数、实还金额   以 交易日期 分组' as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                           as cps,
  nvl(stage.due_bill_no,ods.due_bill_no)                                  as due_bill_no,
  nvl(stage.bnp_type,ods.bnp_type)                                        as bnp_type,
  
  nvl(stage.repaid_num,0)                                                 as repy_num_ods,
  nvl(ods.repaid_num,0)                                                   as repy_num_new,
  nvl(stage.repaid_num,0) - nvl(ods.repaid_num,0)                         as repy_num_his,
  
  nvl(stage.repaid_num_distinct,0)                                        as repy_num_ods_dis,
  nvl(ods.repaid_num_distinct,0)                                          as repy_num_new_dis,
  nvl(stage.repaid_num_distinct,0) - nvl(ods.repaid_num_distinct,0)       as repy_num_his_dis,
  
  nvl(stage.repaid_principal,0)                                           as repy_principal_ods,
  nvl(ods.repaid_principal,0)                                             as repy_principal_new,
  nvl(stage.repaid_principal,0) - nvl(ods.repaid_principal,0)             as repy_principal,
  
  nvl(stage.txn_date,ods.txn_date)                                        as txn_date,
  nvl(stage.product_id,ods.product_id)                                    as product_id
from (
  select
    repay.due_bill_no,
    count(distinct repay.due_bill_no) as repaid_num,
    count(repay.due_bill_no) as repaid_num_distinct,
    sum(repay_amt)
    - case when ecas_loan.product_id = '001802' and repay.d_date between '2020-07-10' and '2020-07-13' and repay.due_bill_no = '1120070912093993172613' then if('${var:tb_suffix}' = '_asset',2500,0) else 0 end
    as repaid_principal,
    bnp_type,
    repay.txn_date as txn_date,
    repay.d_date as d_date,
    ecas_loan.product_id
  from (
    select
      due_bill_no,
      term,
      repay_amt,
      bnp_type,
      txn_date,
      d_date
    from stage.ecas_repay_hst${var:tb_suffix}
    where 1 > 0
      and d_date = '${var:ST9}'
      and txn_date <= d_date
      and if('${var:tb_suffix}' = '_asset',1 > 0,
        payment_id not in ( -- 代偿后  在 2020-07-10 到 2020-07-13 这四天有六笔，最新分区只有一笔。
          '000015945007161admin000083000002',
          '000015945007161admin000083000003',
          '000015945007161admin000083000004',
          '000015945007161admin000083000005',
          '000015945007161admin000083000006'
        )
      )
  ) as repay
  join (
    select product_code as product_id,due_bill_no
    from stage.ecas_loan${var:tb_suffix}
    where 1 > 0
      and d_date = '${var:ST9}'
      and product_code in (
        '001801','001802','001803','001804',
        '001901','001902','001903','001904','001905','001906','001907',
        '002001','002002','002003','002004','002005','002006','002007',
        '002401','002402',
        ''
      )
  ) as ecas_loan
  on repay.due_bill_no = ecas_loan.due_bill_no
  group by repay.d_date,repay.txn_date,ecas_loan.product_id,bnp_type
  ,repay.due_bill_no
) as stage
full join (
  select
    due_bill_no,
    count(distinct due_bill_no) as repaid_num,
    count(due_bill_no) as repaid_num_distinct,
    sum(repay_amount) as repaid_principal,
    bnp_type,
    to_date(txn_time) as txn_date,
    product_id
  from ods${var:db_suffix}.repay_detail
  where 1 > 0
    and biz_date <= '${var:ST9}'
    and to_date(txn_time) <= biz_date
    and product_id in (
      '001801','001802','001803','001804',
      '001901','001902','001903','001904','001905','001906','001907',
      '002001','002002','002003','002004','002005','002006','002007',
      '002401','002402',
      ''
    )
  group by product_id,to_date(txn_time),bnp_type
  ,due_bill_no
) as ods
on  stage.product_id  = ods.product_id
and stage.txn_date    = ods.txn_date
and stage.bnp_type    = ods.bnp_type
and stage.due_bill_no = ods.due_bill_no
having 
nvl(stage.repaid_num,0) - nvl(ods.repaid_num,0)             != 0  or 
nvl(stage.repaid_principal,0) - nvl(ods.repaid_principal,0) != 0
order by 
txn_date,
product_id,
due_bill_no
limit 10
;


---- 校验快照日内订单还款金额 和 实还还款金额
select
repay_hst.due_bill_no as repayhst_due_bill_no,
ord.due_bill_no as ord_due_bill_no,
repay_hst.repay_amount as repayhst_repay_amount,
ord.repay_amount as ord_repay_amount
from
(
select
due_bill_no ,
sum(repay_amt) as repay_amount
from ods.ecas_repay_hst@tb_suffix where p_type in ('lx','lx2','lxzt') and d_date=@date
group by due_bill_no
)repay_hst
full join
(
select
due_bill_no,
sum(txn_amt) as repay_amount
from
(
select due_bill_no,term,txn_amt from  ods.ecas_order@tb_suffix where p_type in ('lx','lx2','lxzt')and d_date=@date
and order_status='S' and  loan_usage!='L'
union all
select due_bill_no,term,txn_amt from  ods.ecas_order_hst@tb_suffix where p_type in ('lx','lx2','lxzt')and d_date=@date and order_status='S' and  loan_usage!='L'
) tmp group by due_bill_no

)ord on repay_hst.due_bill_no=ord.due_bill_no
where abs(nvl(repay_hst.repay_amount,0)-nvl(ord.repay_amount,0))>0
limit 20


---校验逾期ods本金+未出账本金是否等于 应还本金
select
loan.due_bill_no as loan_due_bill_no,
schedule.due_bill_no as schedule_due_bill_no,
loan.remain_principal as remain_principal,
schedule.prin as N_O_schedule_principal
from
(
select
a.due_bill_no,
sum(ifnull(a.prin,0)-ifnull(b.repay,0)) as remain_principal
from
(select
sum(t.loan_init_prin) as prin,t.due_bill_no
from ods.ecas_loan@tb_suffix t where d_date=@date
and  p_type in ('lx','lxzt','lx2')
and t.active_date <= @date group by t.due_bill_no
) a
left join
(
select
sum(t1.repay_amt) as repay,t1.due_bill_no
 from
ods.ecas_repay_hst@tb_suffix  t1
where t1.d_date=@date  and p_type in ('lx','lxzt','lx2')   and t1.bnp_type = 'Pricinpal'
group by t1.due_bill_no
) b on a.due_bill_no = b.due_bill_no
group by a.due_bill_no
)loan
left  join
(
select due_bill_no,sum(t.due_term_prin) as prin
from
ods.ecas_repay_schedule@tb_suffix  t where
t.d_date=@date  and p_type in ('lx','lxzt','lx2')
 and t.schedule_status in ('O','N')
group by due_bill_no
)schedule  on loan.due_bill_no =schedule.due_bill_no
where abs(nvl(loan.remain_principal,0)-nvl(schedule.prin,0))>0

## 首期流入率&催收覆盖度&可联率 分析：  
  -- 点点 7月1日放款 的 首期流入率&催收覆盖度&可联率 by 放款时同盾特征（3个月内申请人在多个平台申请借款）
select 
	a.dob
	,b.td_multi_loan_apply_in3mth  --同盾分：3个月内申请人在多个平台申请借款
	,sum(case when a.overdue_days>0 then a.over_principal end)/sum(a.loan_amount)  as 流入率
	,count(distinct case when cs_flag=1 then a.loan_inner_no end)/count(distinct a.loan_inner_no) as 催收覆盖度（笔数）
	,count(distinct case when contact_flag=1 then a.loan_inner_no end)/count(distinct a.loan_inner_no) as 可联率（笔数）
from zhongan.cdm_fin_loan_asset_struct_ds a 
left join zhongan.adm_fin_cash_loan_tz b
on a.loan_inner_no=b.loan_inner_no
where 
	a.pt=max_pt('zhongan.cdm_fin_loan_asset_struct_ds')
	and b.pt=max_pt('zhongan.adm_fin_cash_loan_tz')
	and a.loan_date='20180701'         --借款时间
	and a.product_id='1301162802'   --点点
group by 
	a.dob
	,b.td_multi_loan_apply_in3mth;

## 逾期率分析：
 --点点3月份放款的DPD1+% by 放款时同盾特征（3个月内身份证关联多个申请信息） 
select 
	to_char(a.withdraw_date,'yyyymm') as 放款月份
	,a.mob
	,b.td_cert_multi_apply_in3mth  --同盾分：3个月内身份证关联多个申请信息
	,sum(a.loan_amount) as wihdraw_amount --放款金额
	,sum(balance_m1+balance_m2+balance_m3+balance_m4+balance_m5+balance_m6+balance_m7) as dpd1_amt --DPD1+金额	
	,sum(balance_m1+balance_m2+balance_m3+balance_m4+balance_m5+balance_m6+balance_m7)/sum(a.loan_amount) as dpd1_per --DPD1+%
from zhongan.cdm_fin_loan_asset_struct_ms a 
left join zhongan.adm_fin_cash_loan_tz b
on a.loan_inner_no=b.loan_inner_no
where 
	a.pt=max_pt('zhongan.cdm_fin_loan_asset_struct_ms')
	and b.pt=max_pt('zhongan.adm_fin_cash_loan_tz')
	and a.product_id='1301162802'   --点点
	and to_char(a.withdraw_date,'yyyymm')='201803'
group by 
	to_char(a.withdraw_date,'yyyymm')
	,a.mob
	,b.td_cert_multi_apply_in3mth;

## 迁徙率分析：
--点点所有日历月的C->M1 by 放款时同盾特征（3个月内身份证关联多个申请信息）
select 
	a.rpt_month  --统计月 月历月份
	,b.td_cert_multi_apply_in3mth  --同盾分：3个月内身份证关联多个申请信息
	,case when sum(lst_m_balance_c)=0 then null else sum(balance_m1)/sum(lst_m_balance_c) end as C_to_M1  --C->M1
from zhongan.cdm_fin_loan_asset_struct_ms a 
left join zhongan.adm_fin_cash_loan_tz b
on a.loan_inner_no=b.loan_inner_no
where 
	a.pt=max_pt('zhongan.cdm_fin_loan_asset_struct_ms')
	and b.pt=max_pt('zhongan.adm_fin_cash_loan_tz')
	and a.product_id='1301162802'   --点点
group by 
	a.rpt_month
	,b.td_cert_multi_apply_in3mth;


#马上金M1+,M2+,M3+ mob Vintage
hvs5123_msj_vintage
#马上金首期日账龄迁徙率by放款月
hvs5123_msj_DOB_month
#DOB 日账龄迁徙率by放款日
hvs5123_msj_DOB_day
#近6个月迁徙率_mob=3
hvs5123_mob3_qxl
#各月业务核心指标表
hvs5123_main_KPI
#各渠道授信及支用情况
hvs5123_msj_channel_sx_zy
#准入及支用拒绝原因分布
hvs5123_sxzy_reject_rea_renshu


#马上金M1+,M2+,M3+ mob Vintage
drop table hvs5123_msj_vintage;
create table hvs5123_msj_vintage as 
select  a.loan_year
        ,a.loan_period
        ,a.mob
        ,a.loan_amt
        ,a.m1plus/a.loan_amt as m1plus_vintage
        ,a.m2plus/a.loan_amt as m2plus_vintage
        ,a.m3plus/a.loan_amt as m3plus_vintage
from (
      select  loan_year
              ,loan_period
              ,mob
              ,sum(loan_amount)loan_amt
              ,sum(left_principal-balance_c)m1plus
              ,sum(left_principal-balance_c-balance_m1)m2plus
              ,sum(left_principal-balance_c-balance_m1-balance_m2)m3plus
      from za_jr_risk.CDM_FIN_LOAN_ASSET_STRUCT_MS
      where product_id='1301146839' 
      and loan_year>=to_char(DATEadd(datetrunc(getdate(),'mm'),-12,'mm'),'yyyymm')
      and loan_year<to_char(datetrunc(getdate(),'mm'),'yyyymm')
      and pt=max_pt('za_jr_risk.CDM_FIN_LOAN_ASSET_STRUCT_MS')
      and mob>0
      and mob<=15
      and loan_period>3 
      group by  loan_year
                ,loan_period
                ,mob)a
;

#DOB 日账龄迁徙率by放款日
drop table hvs5123_msj_DOB_day;
create table hvs5123_msj_DOB_day as
select  a.loan_date
        ,a.dob
        ,a.over_amt/a.tot_amt as 迁徙率
from  (
        select  loan_date
                ,dob
                ,sum(tot_cnt)tot_cnt
                ,sum(tot_amt)tot_amt----表逻辑已修正，只留DOB到期的数据，直接求和就是到期的放款总额
                ,sum(over_amt)over_amt
        from za_jr_prd.ADM_CP_MIGRAT_RATE_FSTMON_DS
        where product_id='1301146839' 
        and pt=max_pt('za_jr_prd.ADM_CP_MIGRAT_RATE_FSTMON_DS')
        and to_Date(loan_date,'yyyy-mm-dd')>= DATEadd(datetrunc(getdate(),'mm' ),-3,'mm')
        and to_Date(loan_date,'yyyy-mm-dd')<DATEadd(datetrunc(getdate(),'mm' ),-0,'mm') 
        group by  loan_date
                  ,dob
        )a
;



#马上金首期日账龄迁徙率by放款月
drop table hvs5123_msj_DOB_month;
create table hvs5123_msj_DOB_month as
select  a.loan_month
        ,a.dob
        ,a.over_amt/a.tot_amt as 迁徙率
from  (
        select  loan_month
                ,dob
                ,sum(tot_cnt)tot_cnt
                ,sum(tot_amt)tot_amt
                ,sum(over_amt)over_amt
        from za_jr_prd.ADM_CP_MIGRAT_RATE_FSTMON_DS
        where product_id='1301146839' 
        and pt=max_pt('za_jr_prd.ADM_CP_MIGRAT_RATE_FSTMON_DS')
        and to_Date(loan_date,'yyyy-mm-dd')>= DATEadd(datetrunc(getdate(),'mm' ),-5,'mm')
        and to_Date(loan_date,'yyyy-mm-dd')<=DATEadd(datetrunc(getdate(),'mm' ),-2,'mm') 
        group by  loan_month
                  ,dob
      )a
;


#近6个月迁徙率_mob=3
drop table hvs5123_mob3_qxl;
create table hvs5123_mob3_qxl as 
select  a.rpt_month
        ,a.loan_period
        ,a.balance_m1/a.pre_balance_c as CtoM1
        ,a.balance_m2/a.pre_balance_m1 as M1toM2
        ,a.balance_m3/a.pre_balance_m2 as M3toM2
from (
        select  rpt_month    
                ,loan_period
                ,mob
                ,sum( pre_balance_c   ) pre_balance_c   
                ,sum( pre_balance_m1  ) pre_balance_m1  
                ,sum( pre_balance_m2  ) pre_balance_m2        
                ,sum( balance_m1      ) balance_m1      
                ,sum( balance_m2      ) balance_m2      
                ,sum( balance_m3      ) balance_m3           
        from za_jr_risk.ADM_CP_OVERDUE_ASSET_MOB_DS
        where pt=max_pt('za_jr_risk.ADM_CP_OVERDUE_ASSET_MOB_DS') 
        and rpt_month>=to_char(DATEadd(datetrunc(getdate(),'mm'),-12,'mm'),'yyyymm')
        and product_name='直营场景贷'
        and loan_period>3
        and mob=3
        group by rpt_month
                ,loan_period
                ,mob)a
;



#各月业务核心指标表
drop table hvs5123_main_KPI;
create table hvs5123_main_KPI as
select  log_month
        ,sum(credit_apply_person_cnt) as 授信申请人数
        ,sum(credit_apply_person_suc_cnt) as 授信核准人数
        ,sum(credit_apply_person_suc_cnt)/sum(credit_apply_person_cnt) as 授信核准率
        ,sum(credit_amt) as 授信金额
        ,sum(credit_amt)/sum(credit_apply_person_suc_cnt) as 人均授信金额
        ,sum(credit_loan_apply_cnt) as 当月授信成功当月支用申请人数
        ,sum(credit_loan_apply_cnt)/sum(credit_apply_person_suc_cnt) as 新客月动支率
        ,sum(loan_apply_person_cnt) as 借款申请人数
        ,sum(loan_apply_person_suc_cnt) as 放款人数
        ,sum(loan_apply_person_suc_cnt)/sum(loan_apply_person_cnt) as 支用核准率
        ,sum(sum_insured) as 保额
        ,sum(premium) as 保费
        ,sum(premium)/sum(sum_insured) as 保费费率
from za_jr_prd.adm_fin_credit_loan_summary_month
where product_name='直营场景贷'
and log_month<to_char(datetrunc(getdate(),'mm' ),'yyyymm')
and log_month>=to_char(DATEadd(datetrunc(getdate(),'mm'),-12,'mm'),'yyyymm')
and pt=max_pt('za_jr_prd.adm_fin_credit_loan_summary_month')
group by log_month;


#各渠道授信及支用情况
drop table hvs5123_channel_temp;
create table hvs5123_channel_temp as 
select  credit_channel_name
        ,log_month
        ,sum(credit_apply_person_cnt) as credit_apply_person_cnt
        ,sum(credit_apply_person_suc_cnt) as credit_apply_person_suc_cnt
        ,sum(loan_apply_person_approve_cnt) as loan_apply_person_approve_cnt
        ,sum(loan_apply_person_cnt) as loan_apply_person_cnt
        ,sum(credit_loan_apply_cnt) as credit_loan_apply_cnt
from za_jr_prd.adm_fin_credit_loan_summary_month
where product_name='直营场景贷'
and log_month<to_char(datetrunc(getdate(),'mm' ),'yyyymm')
and log_month>=to_char(DATEadd(datetrunc(getdate(),'mm' ),-3,'mm'),'yyyymm')
and pt=max_pt('za_jr_prd.adm_fin_credit_loan_summary_month')
group by log_month
        ,credit_channel_name;


drop table hvs5123_msj_channel_sx_zy;
create table hvs5123_msj_channel_sx_zy as
select  case when top16.credit_channel_name is null then '其他' else top16.credit_channel_name end as credit_channel_name
        ,base.log_month 
        ,sum(base.credit_apply_person_cnt) as 授信申请人数
        ,sum(base.credit_apply_person_suc_cnt) as 授信核准人数
        ,sum(base.loan_apply_person_approve_cnt) as 支用核准人数
        ,sum(base.loan_apply_person_cnt) as 支用申请人数
        ,sum(base.credit_loan_apply_cnt) as 当月授信成功当月支用申请人数
from hvs5123_channel_temp base
left join (select a.credit_channel_name,a.rn
            from (
                  select b.credit_channel_name
                          ,row_number() over(order by b.credit_apply_person_cnt_sum desc) as rn
                 from (
                        select  credit_channel_name
                                ,sum(credit_apply_person_cnt) as credit_apply_person_cnt_sum
                        from hvs5123_channel_temp
                        group by credit_channel_name
                       )b
                )a
            where a.rn<=16
            )top16
on base.credit_channel_name=top16.credit_channel_name
group by case when top16.credit_channel_name is null then '其他' else top16.credit_channel_name end
        ,base.log_month;



#准入及支用拒绝原因分布
drop table hvs5123_sxzy_reject_rea_renshu;
create table hvs5123_sxzy_reject_rea_renshu as  
select  '人数by授信拒绝原因' as flag
        ,case when top16.inner_message is null then '其他' else top16.inner_message end as 拒绝原因
        ,to_char(base.loan_apply_date ,'yyyymm') as month
        ,count(distinct base.user_id) as cnt
from za_jr_prd.CDM_FIN_CRD_APPLY_DS base
left join (select a.inner_message,a.rn
            from (
                  select  b.inner_message
                          ,row_number() over(order by b.person_cnt_sum desc) as rn
                  from (
                        select  inner_message
                                ,count(distinct user_id) as person_cnt_sum
                        from za_jr_prd.CDM_FIN_CRD_APPLY_DS
                        where to_char(loan_apply_date,'yyyymm')>=to_char(DATEadd(datetrunc(getdate(),'mm'),-3,'mm'),'yyyymm')
                        and to_char(loan_apply_date,'yyyymm')<to_char(getdate(),'yyyymm')
                        and pt=max_pt('za_jr_prd.CDM_FIN_CRD_APPLY_DS ')
                        and product_id='1301146839'
                        group by inner_message
                       )b
                )a
            where a.rn<=16
            )top16
on base.inner_message=top16.inner_message
where to_char(loan_apply_date,'yyyymm')>=to_char(DATEadd(datetrunc(getdate(),'mm'),-3,'mm'),'yyyymm')
and to_char(loan_apply_date,'yyyymm')<to_char(getdate(),'yyyymm')
and pt=max_pt('za_jr_prd.CDM_FIN_CRD_APPLY_DS ')
and product_id='1301146839'
group by  case when top16.inner_message is null then '其他' else top16.inner_message end,
          to_char(loan_apply_date,'yyyymm')

union all

select  '人数by支用拒绝原因' as flag
        ,case when top16.risk_inner_message is null then '其他' else top16.risk_inner_message end as 拒绝原因
        ,to_char(base.loan_date ,'yyyymm') as month
        ,count(distinct base.user_id) as cnt
from za_jr_prd.CDM_FIN_CASH_LOAN_POLICY base
left join (select a.risk_inner_message,a.rn
            from (
                  select  b.risk_inner_message
                          ,row_number() over(order by b.person_cnt_sum desc) as rn
                  from (
                        select  risk_inner_message
                                ,count(distinct user_id) as person_cnt_sum
                        from za_jr_prd.CDM_FIN_CASH_LOAN_POLICY
                        where to_char(loan_date,'yyyymm')>=to_char(DATEadd(datetrunc(getdate(),'mm'),-3,'mm'),'yyyymm')
                        and to_char(loan_date,'yyyymm')<to_char(getdate(),'yyyymm')
                        and pt=max_pt('za_jr_prd.CDM_FIN_CASH_LOAN_POLICY ')
                        and product_id='1301146839'
                        group by risk_inner_message
                       )b
                )a
            where a.rn<=16
            )top16
on base.risk_inner_message=top16.risk_inner_message
where to_char(loan_date,'yyyymm')>=to_char(DATEadd(datetrunc(getdate(),'mm'),-3,'mm'),'yyyymm')
and to_char(loan_date,'yyyymm')<to_char(getdate(),'yyyymm')
and pt=max_pt('za_jr_prd.CDM_FIN_CASH_LOAN_POLICY ')
and product_id='1301146839'
group by  case when top16.risk_inner_message is null then '其他' else top16.risk_inner_message end,
          to_char(loan_date,'yyyymm');

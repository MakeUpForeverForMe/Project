-- set hive.execution.engine=mr;
set hive.groupby.orderby.position.alias=true;

with loan as (
    select  product_id,loan_init_term,loan_active_date from ods.loan_info
    group by product_id,loan_init_term,loan_active_date
),
product_active as (
    select min(loan_active_date) as loan_active_date,product_id from loan
    group by product_id
),
product_snapshot as (
select tmp.product_id,concat('[',concat_ws(',',collect_set(biz_month)),']') as biz_month_list from (
    select distinct p.product_id,datefmt(d.biz_date,'yyyy-MM-dd','yyyy-MM') as biz_month from product_active p
	inner join dim.dim_date d
	on d.biz_date >= p.loan_active_date
	where d.biz_date <= '${ST9}'
) tmp
group by tmp.product_id
),
loan_num as (select loan.product_id,loan_init_term,loan_active_date,biz_month_list
from loan left join  product_snapshot
on product_snapshot.product_id = loan.product_id
)
insert into table dm_eagle${db_suffix}.eagle_title_info
select
  capital_id,
  channel_id,
  project_id,
  product_id,
  project_amount,
  concat('{',
    concat_ws(',',
      collect_set(
        concat(
          '"',if(product_id = 'vt_pl00282' and loan_month > '2019-02','2019-02',loan_month),'":',
          concat('[',concat_ws(',',loan_terms_list),']')
        )
      )
    ),'}'
  ) as loan_month_map,
  biz_month_list,
  concat('[',concat_ws(',',collect_set(loan_terms)),']') as loan_terms_list,
  current_date() as create_date
from (
  select distinct
    capital_id,
    channel_id,
    project_id,
    biz_conf.product_id_vt as product_id,
    project_amount,
	datefmt(loan_active_date,'yyyy-MM-dd','yyyy-MM') as loan_month,
    biz_month_list,
    cast(loan_init_term as string) as loan_terms,
    sort_array(collect_set(cast(loan_init_term as string)) over(partition by biz_conf.product_id_vt,datefmt(loan_active_date,'yyyy-MM-dd','yyyy-MM'))) as loan_terms_list
  from loan_num
  join (
    select distinct
      capital_id,
      channel_id,
      project_id,
      product_id_vt ,
      project_amount,
      product_id
    from (
      select
        max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
        max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
        max(if(col_name = 'project_id',   col_val,null)) as project_id,
        max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
        max(if(col_name = 'project_amount',col_val,null)) as project_amount,
        max(if(col_name = 'product_id',   col_val,null)) as product_id
      from dim.data_conf
      where col_type = 'ac'
      group by col_id
    ) as tmp
  ) as biz_conf
  on loan_num.product_id = biz_conf.product_id
  and product_id_vt is not null
  -- order by product_id,biz_month,cast(loan_terms as int)
) as tmp
group by 1,2,3,4,5,7
;
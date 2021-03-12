set hive.execution.engine=mr;
set hive.groupby.orderby.position.alias=true;

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
          '"',if(product_id = 'vt_pl00282' and biz_month > '2019-02','2019-02',biz_month),'":',
          concat('[',concat_ws(',',loan_terms_list),']')
        )
      )
    ),'}'
  ) as loan_month_map,
  concat('[',concat_ws(',',collect_set(biz_month)),']') as biz_month_list,
  concat('[',concat_ws(',',collect_set(loan_terms)),']') as loan_terms_list,
  current_date() as create_date
from (
  select distinct
    capital_id,
    channel_id,
    project_id,
    biz_conf.product_id${vt} as product_id,
    project_amount,
    datefmt(biz_date,'yyyy-MM-dd','yyyy-MM') as biz_month,
    cast(loan_terms as string) as loan_terms,
    sort_array(collect_set(cast(loan_terms as string)) over(partition by biz_conf.product_id${vt},datefmt(biz_date,'yyyy-MM-dd','yyyy-MM'))) as loan_terms_list
  from dw_new${db_suffix}.dw_loan_base_stat_loan_num_day as loan_num
  join dim_new.biz_conf
  on loan_num.product_id = biz_conf.product_id
  and product_id${vt} is not null
  -- order by product_id,biz_month,cast(loan_terms as int)
) as tmp
group by 1,2,3,4,5
;

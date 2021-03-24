set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

-- 设置 Container 大小
set hive.tez.container.size=2048;
set tez.am.resource.memory.mb=2048;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=64000000;      -- 64M
set hive.merge.smallfiles.avgsize=64000000; -- 64M
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;



insert overwrite table ods.linkman_info partition(product_id)
select distinct
  concat_ws('_',channel_id,sha256(decrypt_aes(document_num,'tencentabs123456'),'idNumber',1),sha256(decrypt_aes(customer_name,'tencentabs123456'),'userName',1)) as cust_id,
  sha256(decrypt_aes(document_num,'tencentabs123456'),'idNumber',1)                            as user_hash_no,
  asset_id                                                                                     as due_bill_no,
  concat_ws('~',
    sha256(decrypt_aes(phone_num,'tencentabs123456'),'phone',1),
    sha256(decrypt_aes(customer_name,'tencentabs123456'),'userName',1),
    asset_id
  )                                                                                            as linkman_id,
  null                                                                                         as relational_type,
  null                                                                                         as relational_type_cn,
  case is_empty(mainborrower_relationship)
  when '父母'     then '1'
  when '配偶'     then '2'
  when '子女'     then '3'
  when '兄弟姐妹' then '4'
  when '亲属'     then '5'
  when '亲戚'     then '5'
  when '同事'     then '6'
  when '朋友'     then '7'
  when '其他'     then '8'
  else is_empty(mainborrower_relationship)
  end                                                                                          as relationship,
  case is_empty(mainborrower_relationship)
  when '父母'     then '父母'
  when '配偶'     then '配偶'
  when '子女'     then '子女'
  when '兄弟姐妹' then '兄弟姐妹'
  when '亲属'     then '亲属'
  when '亲戚'     then '亲属'
  when '同事'     then '同事'
  when '朋友'     then '朋友'
  when '其他'     then '其他'
  else is_empty(mainborrower_relationship)
  end                                                                                          as relationship_cn,
  '身份证'                                                                                     as relation_idcard_type,
  sha256(decrypt_aes(document_num,'tencentabs123456'),'idNumber',1)                            as relation_idcard_no,
  datefmt(substring(decrypt_aes(document_num,'tencentabs123456'),7,8),'yyyyMMdd','yyyy-MM-dd') as relation_birthday,
  sha256(decrypt_aes(customer_name,'tencentabs123456'),'userName',1)                           as relation_name,
  is_empty(sex_idno(decrypt_aes(document_num,'tencentabs123456')),sex)                         as relation_sex,
  sha256(decrypt_aes(phone_num,'tencentabs123456'),'phone',1)                                  as relation_mobile,
  sha256(decrypt_aes(phone_num,'communication_address'),'address',1)                           as relation_address,
  null                                                                                         as relation_province,
  null                                                                                         as relation_city,
  null                                                                                         as relation_county,
  occupation                                                                                   as corp_type,
  null                                                                                         as corp_name,
  sha256(decrypt_aes(unit_phone_number,'tencentabs123456'),'phone',1)                          as corp_teleph_nbr,
  null                                                                                         as corp_fax,
  null                                                                                         as corp_position,
  to_date(update_time)                                                                         as deal_date,
  create_time                                                                                  as create_time,
  update_time                                                                                  as update_time,
  project_id_lower                                                                             as project_id
from (
  select distinct
    abs_project_id,
    channel_id
  from (
    select
      max(if(col_name = 'project_id',  col_val,null)) as abs_project_id,
      max(if(col_name = 'channel_id',  col_val,null)) as channel_id
    from dim.data_conf
    where col_type = 'pp'
    group by col_id
  ) as tmp
) as biz_conf
join (
  select
    *,
    case project_id when 'Cl00333' then 'cl00333' else project_id end as project_id_lower
  from stage.asset_03_t_contact_person_info
  where project_id not in (
    '001601',           -- 汇通
    'DIDI201908161538', -- 滴滴
    'WS0005200001',     -- 瓜子
    'CL202012280092',   -- 汇通国银
    'CL202102010097',   -- 汇通国银
    ''
  )
) as linkman_info
on abs_project_id = project_id_lower
-- limit 10
;

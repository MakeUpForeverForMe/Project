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



insert overwrite table ods.customer_info partition(product_id)
select distinct
  due_bill_no                                                                                       as apply_id,
  due_bill_no                                                                                       as due_bill_no,
  concat_ws('_',channel_id,sha256(decrypt_aes(document_num,'tencentabs123456'),'idNumber',1),sha256(decrypt_aes(borrower_name,'tencentabs123456'),'userName',1)) as cust_id,
  sha256(decrypt_aes(document_num,'tencentabs123456'),'idNumber',1)                                 as user_hash_no,
  null                                                                                              as outer_cust_id,
  is_empty(certificate_type,'身份证')                                                               as idcard_type,
  sha256(decrypt_aes(document_num,'tencentabs123456'),'idNumber',1)                                 as idcard_no,
  sha256(decrypt_aes(borrower_name,'tencentabs123456'),'userName',1)                                as name,
  sha256(decrypt_aes(phone_num,'tencentabs123456'),'phone',1)                                       as mobie,
  sha256(decrypt_aes(phone_num,'tencentabs123456'),'phone',1)                                       as card_phone,
  is_empty(sex,sex_idno(decrypt_aes(document_num,'tencentabs123456')))                              as sex,
  datefmt(substring(decrypt_aes(document_num,'tencentabs123456'),7,8),'yyyyMMdd','yyyy-MM-dd')      as birthday,
  is_empty(if(age = 0,null,age),age_idno(decrypt_aes(document_num,'tencentabs123456'),active_date)) as age,
  marital_status                                                                                    as marriage_status,
  education_level                                                                                   as education,
  case
    when education_level = '硕士' then '硕士及以上'
    when education_level = '本科' then '大学本科'
    when education_level in ('未知',null,'','null','NULL') then '未知'
    else '大专及以下'
  end                                                                                               as education_ws,
  concat(dim_idno.idno_province_cn,dim_idno.idno_city_cn,dim_idno.idno_county_cn)                   as idcard_address,
  dim_idno.idno_area_cn                                                                             as idcard_area,
  dim_idno.idno_province_cn                                                                         as idcard_province,
  dim_idno.idno_city_cn                                                                             as idcard_city,
  dim_idno.idno_county_cn                                                                           as idcard_county,
  null                                                                                              as idcard_township,
  sha256(concat_ws('::',house_address,mailing_address),'address',1)                                 as resident_address,
  dim_idno_province.idno_area_cn                                                                    as resident_area,
  is_empty(house_province)                                                                          as resident_province,
  is_empty(house_city)                                                                              as resident_city,
  null                                                                                              as resident_county,
  null                                                                                              as resident_township,
  case borrower_industry
    when 'A'    then '农、林、牧、渔业'
    when 'B'    then '采掘业'
    when 'C'    then '制造业'
    when 'D'    then '电力、燃气及水的生产和供应业'
    when 'E'    then '建筑业'
    when 'F'    then '交通运输、仓储和邮政业'
    when 'G'    then '信息传输、计算机服务和软件业'
    when 'H'    then '批发和零售业'
    when 'I'    then '住宿和餐饮业'
    when 'J'    then '金融业'
    when 'K'    then '房地产业'
    when 'L'    then '租赁和商务服务业'
    when 'M'    then '科学研究、技术服务业和地质勘察业'
    when 'N'    then '水利、环境和公共设施管理业'
    when 'O'    then '居民服务和其他服务业'
    when 'P'    then '教育'
    when 'Q'    then '卫生、社会保障和社会福利业'
    when 'R'    then '文化、体育和娱乐业'
    when 'S'    then '公共管理和社会组织'
    when 'T'    then '国际组织'
    when 'Z'    then '其他'
    else is_empty(borrower_industry,'空')
  end                                                                                               as job_type,
  work_years                                                                                        as job_year,
  0                                                                                                 as job_year_max,
  0                                                                                                 as job_year_min,
  annual_income / 12                                                                                as income_month,
  annual_income                                                                                     as income_year,
  0                                                                                                 as income_year_max,
  0                                                                                                 as income_year_min,
  if(loan_type = '企业','企业',is_empty(customer_type,'未知'))                                      as customer_type,
  loan_type                                                                                         as loan_type,
  cust_rating                                                                                       as cust_rating,
  project_id                                                                                        as project_id
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
    case is_empty(map_from_str(extra_info)['项目编号'],project_id)
      when 'Cl00333' then 'cl00333'
      else is_empty(map_from_str(extra_info)['项目编号'],project_id)
    end                                                                                                                as project_id,
    is_empty(map_from_str(extra_info)['借据号'],asset_id)                                                              as due_bill_no,
    is_empty(map_from_str(extra_info)['证件类型'])                                                                     as certificate_type,
    is_empty(map_from_str(extra_info)['身份证号'],document_num)                                                        as document_num,
    is_empty(map_from_str(extra_info)['客户姓名'],customer_name)                                                       as borrower_name,
    is_empty(map_from_str(extra_info)['手机号'],phone_num)                                                             as phone_num,
    is_empty(map_from_str(extra_info)['性别'],sex)                                                                     as sex,
    is_empty(map_from_str(extra_info)['年龄'],age)                                                                     as age,
    is_empty(map_from_str(extra_info)['婚姻状况'],marital_status)                                                      as marital_status,
    is_empty(map_from_str(extra_info)['学历'],degree)                                                                  as education_level,
    is_empty(
      map_from_str(extra_info)['客户户籍地址'],
      is_empty(
        concat_ws('',
          is_empty(map_from_str(extra_info)['客户居住所在省']),is_empty(map_from_str(extra_info)['客户居住所在市']),is_empty(map_from_str(extra_info)['客户居住地址'])
        ),
        concat_ws('',
          is_empty(province),is_empty(city),is_empty(address)
        )
      )
    )                                                                                                                  as house_address,
    is_empty(map_from_str(extra_info)['客户通讯地址'],
      concat_ws('',
        province,city,address
      )
    )                                                                                                                  as mailing_address,
    is_empty(map_from_str(extra_info)['客户户籍所在省'],is_empty(map_from_str(extra_info)['客户居住所在省'],province)) as house_province,
    is_empty(map_from_str(extra_info)['客户户籍所在市'],is_empty(map_from_str(extra_info)['客户居住所在市'],city))     as house_city,
    is_empty(map_from_str(extra_info)['借款人行业'])                                                                   as borrower_industry,
    is_empty(map_from_str(extra_info)['工作年限'])                                                                     as work_years,
    is_empty(map_from_str(extra_info)['年收入(元)'],annual_income)                                                     as annual_income,
    is_empty(map_from_str(extra_info)['客户类型'])                                                                     as customer_type,
    is_empty(map_from_str(extra_info)['内部信用等级'])                                                                 as cust_rating
  from stage.asset_02_t_principal_borrower_info
  where 1 > 0
    and is_empty(map_from_str(extra_info)['项目编号'],project_id) not in (
      '001601',           -- 汇通
      'WS0005200001',     -- 瓜子
      -- 'CL202012280092',   -- 汇通国银
      'DIDI201908161538', -- 滴滴
      ''
    )
) as borrowerinfo
on abs_project_id = project_id
join (
  select
    case is_empty(map_from_str(extra_info)['项目编号'],project_id)
      when 'Cl00333' then 'cl00333'
      else is_empty(map_from_str(extra_info)['项目编号'],project_id)
    end                                                       as product_id_contract,
    is_empty(map_from_str(extra_info)['借据号'],asset_id)     as serial_number,
    is_empty(loan_type,'个人')                                as loan_type,
    is_empty(
      case when length(map_from_str(extra_info)['实际放款时间']) = 19 then to_date(map_from_str(extra_info)['实际放款时间'])
      else is_empty(datefmt(map_from_str(extra_info)['实际放款时间'],'','yyyy-MM-dd')) end,
      case when length(map_from_str(extra_info)['合同开始时间']) = 19 then to_date(map_from_str(extra_info)['合同开始时间'])
      else is_empty(datefmt(map_from_str(extra_info)['合同开始时间'],'','yyyy-MM-dd')) end
    ) as active_date
  from stage.asset_01_t_loan_contract_info
) as loan_contract_info
on  project_id  = product_id_contract
and due_bill_no = serial_number
left join (
  select distinct
    idno_addr,
    idno_area_cn,
    idno_province_cn,
    idno_city_cn,
    idno_county_cn
  from dim.dim_idno
) as dim_idno
on substring(decrypt_aes(document_num,'tencentabs123456'),1,6) = dim_idno.idno_addr
left join (
  select distinct
    idno_area_cn,
    idno_province_cn
  from dim.dim_idno
) as dim_idno_province
on house_province = dim_idno_province.idno_province_cn
-- limit 10
;

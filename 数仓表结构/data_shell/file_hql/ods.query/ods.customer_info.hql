set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
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
-- 关闭自动 MapJoin （ Hive3 的 bug，引发 No work found for tablescan ）
set hive.auto.convert.join=false;
set hive.auto.convert.join.noconditionaltask=false;
--set hivevar:ST9=2021-05-29;




-- 滴滴
insert into table ods.customer_info partition(product_id)
select distinct *
from (
  select
    nvl(loan_result.loan_order_id,get_json_object(msg_log.original_msg,'$.applicationId')) as apply_id,    -- 有放款为借据号，无则为授信申请号
    nvl(loan_result.loan_order_id,get_json_object(msg_log.original_msg,'$.applicationId')) as due_bill_no, -- 有放款为借据号，无则为授信申请号
    concat_ws('_',biz_conf.channel_id,sha256(get_json_object(msg_log.original_msg,'$.userInfo.ocrInfo.idNo'),'idNumber',1),sha256(get_json_object(msg_log.original_msg,'$.userInfo.ocrInfo.name'),'userName',1)) as cust_id,
    sha256(get_json_object(msg_log.original_msg,'$.userInfo.ocrInfo.idNo'),'idNumber',1)   as user_hash_no,
    null                                                                                   as outer_cust_id,
    '身份证'                                                                               as idcard_type,
    sha256(get_json_object(msg_log.original_msg,'$.userInfo.ocrInfo.idNo'),'idNumber',1)   as idcard_no,
    sha256(get_json_object(msg_log.original_msg,'$.userInfo.ocrInfo.name'),'userName',1)   as name,
    sha256(get_json_object(msg_log.original_msg,'$.userInfo.telephone'),'phone',1)         as mobie,
    sha256(get_json_object(msg_log.original_msg,'$.userInfo.phone'),'phone',1)             as card_phone,
    is_empty(
      sex_idno(get_json_object(msg_log.original_msg,'$.userInfo.ocrInfo.idNo')),
      get_json_object(msg_log.original_msg,'$.userInfo.ocrInfo.gender')
    )                                                                                      as sex,
    is_empty(
      datefmt(substring(get_json_object(msg_log.original_msg,'$.userInfo.ocrInfo.idNo'),7,8),'yyyyMMdd','yyyy-MM-dd'),
      get_json_object(msg_log.original_msg,'$.userInfo.ocrInfo.birthday')
    )                                                                                      as birthday,
    age_birth(
      is_empty(
        datefmt(substring(get_json_object(msg_log.original_msg,'$.userInfo.ocrInfo.idNo'),7,8),'yyyyMMdd','yyyy-MM-dd'), -- 取生日
        get_json_object(msg_log.original_msg,'$.userInfo.ocrInfo.birthday') -- 取生日
      ), -- 取生日
      is_empty(
        to_date(loan_result.issuetime), -- 取放款日
        to_date(get_json_object(msg_log.original_msg,'$.creditInfo.startDate')) -- 取授信申请日
      )
    )                                                                                      as age,
    null                                                                                   as marriage_status,
    null                                                                                   as education,
    null                                                                                   as education_ws,
    concat(dim_idno.idno_province_cn,dim_idno.idno_city_cn,dim_idno.idno_county_cn)        as idcard_address,
    dim_idno.idno_area_cn                                                                  as idcard_area,
    dim_idno.idno_province_cn                                                              as idcard_province,
    dim_idno.idno_city_cn                                                                  as idcard_city,
    dim_idno.idno_county_cn                                                                as idcard_county,
    null                                                                                   as idcard_township,
    sha256(get_json_object(msg_log.original_msg,'$.userInfo.address'),'address',1)         as resident_address,
    null                                                                                   as resident_area,
    null                                                                                   as resident_province,
    null                                                                                   as resident_city,
    null                                                                                   as resident_county,
    null                                                                                   as resident_township,
    is_empty(null,'空')                                                                    as job_type,
    0                                                                                      as job_year,
    0                                                                                      as job_year_max,
    0                                                                                      as job_year_min,
    0                                                                                      as income_month,
    0                                                                                      as income_year,
    0                                                                                      as income_year_max,
    0                                                                                      as income_year_min,
    '未知'                                                                                 as customer_type,
    '个人'                                                                                 as loan_type,
    null                                                                                   as cust_rating,
    msg_log.product_code                                                                   as product_id
  from (
    select
      'DIDI201908161538' as product_code,
      regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}'),'\\\\\"','\\\"') as original_msg
    from stage.ecas_msg_log
    where msg_type = 'CREDIT_APPLY'
      and original_msg is not null
      and datefmt(update_time,'ms','yyyy-MM-dd') = '${ST9}'
  ) as msg_log
  left join (
    select
      get_json_object(original_msg,'$.creditId')    as creditId,
      get_json_object(original_msg,'$.loanOrderId') as loan_order_id,
      get_json_object(original_msg,'$.issueTime')   as issuetime
    from stage.ecas_msg_log
    where 1 > 0
      and msg_type = 'LOAN_RESULT'
      and original_msg is not null
  ) as loan_result
  on get_json_object(msg_log.original_msg,'$.applicationId') = loan_result.creditId
  left join (
    select distinct
      product_id as dim_product_id,
      channel_id
    from (
      select
        max(if(col_name = 'product_id',  col_val,null)) as product_id,
        max(if(col_name = 'channel_id',  col_val,null)) as channel_id
      from dim.data_conf
      where col_type = 'ac'
      group by col_id
    ) as tmp
  ) as biz_conf
  on msg_log.product_code = biz_conf.dim_product_id
  left join (
    select distinct
      idno_addr,
      idno_area_cn,
      idno_province_cn,
      idno_city_cn,
      idno_county_cn
    from dim.dim_idno
  ) as dim_idno
  on substring(get_json_object(msg_log.original_msg,'$.userInfo.ocrInfo.idNo'),1,6) = dim_idno.idno_addr
  ---union all
  ---select * from ods.customer_info where product_id = 'DIDI201908161538'
) as tmp
-- limit 1
;








-- 汇通
insert into table ods.customer_info partition(product_id)
select distinct *
from (
  select
    resp_log.due_bill_no                                                            as apply_id,
    resp_log.due_bill_no                                                            as due_bill_no,
    concat_ws('_',biz_conf.channel_id,resp_log.id_no,resp_log.name)                 as cust_id,
    resp_log.id_no                                                                  as user_hash_no,
    resp_log.outer_cust_id                                                          as outer_cust_id,
    case resp_log.id_type
      when 'I' then '身份证'
      when 'T' then '台胞证'
      when 'S' then '军官证/士兵证'
      when 'P' then '护照'
      when 'L' then '营业执照'
      when 'O' then '其他有效证件'
      when 'R' then '户口簿'
      when 'H' then '港澳居民来往内地通行证'
      when 'W' then '台湾同胞来往内地通行证'
      when 'F' then '外国人居留证'
      when 'C' then '警官证'
      when 'B' then '外国护照'
      else resp_log.id_type
    end                                                                             as idcard_type,
    resp_log.id_no                                                                  as idcard_no,
    resp_log.name                                                                   as name,
    resp_log.mobie                                                                  as mobie,
    resp_log.card_phone                                                             as card_phone,
    resp_log.sex                                                                    as sex,
    resp_log.birthday                                                               as birthday,
    age_birth(
      resp_log.birthday, -- 取生日
      is_empty(
        to_date(nms_loan.loan_date), -- 取放款时间
        nms_loan.deal_date, -- 取放款的执行时间
        resp_log.deal_date  -- 取进件的执行时间
      )
    )                                                                               as age,
    case resp_log.marital_status
      when 'C' then '已婚'
      when 'S' then '未婚'
      when 'O' then '其他'
      when 'D' then '离异'
      when 'P' then '丧偶'
      else resp_log.marital_status
    end                                                                             as marital_status,
    case resp_log.education
      when 'A' then '博士及以上'
      when 'B' then '硕士'
      when 'C' then '大学本科'
      when 'D' then '大学专科/专科学校'
      when 'E' then '高中/中专/技校'
      when 'F' then '初中'
      when 'G' then '初中以下'
      else resp_log.education
    end                                                                             as education,
    case
      when resp_log.education in ('A','B') then '硕士及以上'
      when resp_log.education = 'C' then '大学本科'
      when resp_log.education in ('D','E','F','G') then '大专及以下'
      else '未知'
    end                                                                             as education_ws,
    concat(dim_idno.idno_province_cn,dim_idno.idno_city_cn,dim_idno.idno_county_cn) as idcard_address,
    dim_idno.idno_area_cn                                                           as idcard_area,
    dim_idno.idno_province_cn                                                       as idcard_province,
    dim_idno.idno_city_cn                                                           as idcard_city,
    dim_idno.idno_county_cn                                                         as idcard_county,
    null                                                                            as idcard_township,
    resp_log.resident_address                                                       as resident_address,
    dim_idno_province.idno_area_cn                                                  as resident_area,
    resp_log.resident_province                                                      as resident_province,
    resp_log.resident_city                                                          as resident_city,
    resp_log.resident_county                                                        as resident_county,
    null                                                                            as resident_township,
    case resp_log.job_type
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
      else is_empty(resp_log.job_type,'空')
    end                                                                             as job_type,
    0                                                                               as job_year,
    0                                                                               as job_year_max,
    0                                                                               as job_year_min,
    0                                                                               as income_month,
    0                                                                               as income_year,
    is_empty(resp_log.income_year_max,0)                                            as income_year_max,
    is_empty(resp_log.income_year_min,0)                                            as income_year_min,
    if(resp_log.company_loan_bool = 'true','企业','未知')                           as customer_type,
    if(resp_log.company_loan_bool = 'true','企业','个人')                           as loan_type,
    null                                                                            as cust_rating,
    resp_log.product_id                                                             as product_id
  from (
    select
      deal_date,
      due_bill_no,
      id_type,
      id_no,
      name,
      outer_cust_id,
      mobie,
      card_phone,
      sex,
      birthday,
      marital_status,
      education,
      resident_address,
      resident_province,
      resident_city,
      resident_county,
      job_type,
      cast(income_year_max as decimal(25,5)) as income_year_max,
      cast(income_year_min as decimal(25,5)) as income_year_min,
      idcard_area,
      company_loan_bool,
      product_id
    from (
      select
        deal_date                                                                                            as deal_date,
        get_json_object(standard_req_msg,'$.apply_no')                                                       as due_bill_no,
        get_json_object(standard_req_msg,'$.borrower.id_type')                                               as id_type,
        sha256(get_json_object(standard_req_msg,'$.borrower.id_no'),'idNumber',1)                            as id_no,
        sha256(get_json_object(standard_req_msg,'$.borrower.name'),'userName',1)                             as name,
        get_json_object(standard_req_msg,'$.borrower.open_id')                                               as outer_cust_id,
        sha256(get_json_object(standard_req_msg,'$.borrower.mobile_phone'),'phone',1)                        as mobie,
        sha256(get_json_object(standard_req_msg,'$.loan_account.mobile_phone'),'phone',1)                    as card_phone,
        sex_idno(get_json_object(standard_req_msg,'$.borrower.id_no'))                                       as sex,
        datefmt(substring(get_json_object(standard_req_msg,'$.borrower.id_no'),7,8),'yyyyMMdd','yyyy-MM-dd') as birthday,
        get_json_object(standard_req_msg,'$.borrower.marital_status')                                        as marital_status,
        get_json_object(standard_req_msg,'$.borrower.education')                                             as education,
        sha256(get_json_object(standard_req_msg,'$.borrower.address'),'address',1)                           as resident_address,
        get_json_object(standard_req_msg,'$.borrower.province')                                              as resident_province,
        get_json_object(standard_req_msg,'$.borrower.city')                                                  as resident_city,
        get_json_object(standard_req_msg,'$.borrower.area')                                                  as resident_county,
        get_json_object(standard_req_msg,'$.borrower.industry')                                              as job_type,
        get_json_object(standard_req_msg,'$.borrower.annual_income_max')                                     as income_year_max,
        get_json_object(standard_req_msg,'$.borrower.annual_income_min')                                     as income_year_min,
        substring(get_json_object(standard_req_msg,'$.borrower.id_no'),1,6)                                  as idcard_area,
        get_json_object(standard_req_msg,'$.company_loan_bool')                                              as company_loan_bool,
        get_json_object(standard_req_msg,'$.product.product_no')                                             as product_id,
        row_number() over(partition by get_json_object(standard_req_msg,'$.product.product_no'),get_json_object(standard_req_msg,'$.apply_no') order by update_time desc) as rn
      from stage.nms_interface_resp_log
      where sta_service_method_name = 'setupCustCredit'
        and standard_req_msg is not null
        and datefmt(update_time,'ms','yyyy-MM-dd') = '${ST9}'
    ) as tmp
    where rn = 1
  ) as resp_log
  left join (
    select
      deal_date                                                                           as deal_date,
      nvl(due_bills['APPLY_NO'],  due_bills['apply_no'])                                  as apply_no,
      datefmt(nvl(due_bills['LOAN_DATE'],due_bills['loan_date']),'yyyyMMdd','yyyy-MM-dd') as loan_date,
      nvl(due_bills['product_no'],due_bills['PRODUCT_NO'])                                as product_id
    from stage.nms_interface_resp_log
    lateral view explode(json_array_to_array(nvl(get_json_object(standard_req_msg,'$.DUE_BILLS'),get_json_object(standard_req_msg,'$.due_bills')))) bills as due_bills
    where 1> 0
      and sta_service_method_name = 'loanApply'
      and standard_req_msg is not null
      and nvl(due_bills['APPLY_NO'],due_bills['apply_no']) != '7634562346454355'
  ) as nms_loan
  on  resp_log.product_id  = nms_loan.product_id
  and resp_log.due_bill_no = nms_loan.apply_no
  left join (
    select distinct
      product_id as dim_product_id,
      channel_id
    from (
      select
        max(if(col_name = 'product_id',  col_val,null)) as product_id,
        max(if(col_name = 'channel_id',  col_val,null)) as channel_id
      from dim.data_conf
      where col_type = 'ac'
      group by col_id
    ) as tmp
  ) as biz_conf
  on resp_log.product_id = biz_conf.dim_product_id
  left join (
    select distinct
      idno_addr,
      idno_area_cn,
      idno_province_cn,
      idno_city_cn,
      idno_county_cn
    from dim.dim_idno
  ) as dim_idno
  on resp_log.idcard_area = dim_idno.idno_addr
  left join (
    select distinct
      idno_area_cn,
      idno_province_cn
    from dim.dim_idno
  ) as dim_idno_province
  on resp_log.resident_province = dim_idno_province.idno_province_cn
  --union all
  --select customer_info.* from ods.customer_info
  --join (
  --  select distinct get_json_object(standard_req_msg,'$.product.product_no') as product_id
  --  from stage.nms_interface_resp_log
  --  where sta_service_method_name = 'setupCustCredit'
  --    and standard_req_msg is not null
  --) as product_id_tbl
  --on customer_info.product_id = product_id_tbl.product_id
) as tmp
-- limit 1
;








-- 乐信
--explain

insert into table ods.customer_info partition(product_id)
select distinct *
from (
  select
    msg_log.due_bill_no                                                                                                                                                  as apply_id,
    msg_log.due_bill_no                                                                                                                                                  as due_bill_no,
    concat_ws('_','0006',sha256(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.idNo'),'idNumber',1),sha256(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.custName'),'userName',1)) as cust_id,
    sha256(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.idNo'),'idNumber',1)                as user_hash_no,
    get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.customerId')                               as outer_cust_id,
    case get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.idType')
      when '0' then '身份证'
      when '1' then '户口簿'
      when '2' then '护照'
      when '3' then '军官证'
      when '4' then '士兵证'
      when '5' then '港澳居民来往内地通行证'
      when '6' then '台湾同胞来往内地通行证'
      when '7' then '临时身份证'
      when '8' then '外国人居留证'
      when '9' then '警官证'
      when 'A' then '香港身份证'
      when 'B' then '澳门身份证'
      when 'C' then '台湾身份证'
      when 'X' then '其他证件'
      else is_empty(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.idType'))
    end                                                                                                                   as idcard_type,
    sha256(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.idNo'),'idNumber',1)                as idcard_no,
    sha256(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.custName'),'userName',1)            as name,
    sha256(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.phoneNo'),'phone',1)                as mobie,
    sha256(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.telNo'),'phone',1)                  as card_phone,
    is_empty(
      sex_idno(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.idNo')),
      case get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.sex') when '1' then '男' when '2' then '女' else null end
    )                                                                                                                     as sex,
    is_empty(
      datefmt(substring(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.idNo'),7,8),'yyyyMMdd','yyyy-MM-dd'),
      datefmt(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.birth'),'yyyyMMdd','yyyy-MM-dd')
    )                                                                                                                     as birthday,
    age_birth(
      is_empty(
        datefmt(substring(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.idNo'),7,8),'yyyyMMdd','yyyy-MM-dd'),
        datefmt(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.birth'),'yyyyMMdd','yyyy-MM-dd')
      ),
      to_date(
        nvl(
          ecas_loan.active_date,
          get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.loanDate')
        )
      )
    )                                                                                                                     as age,
    case get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.marriage')
      when '10' then '未婚'
      when '20' then '已婚'
      when '21' then '初婚'
      when '22' then '再婚'
      when '23' then '复婚'
      when '30' then '丧偶'
      when '40' then '离婚'
      when '90' then '未说明的婚姻状况'
    else is_empty(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.marriage'))
    end                                                                                                                   as marriage_status,
    case get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.edu')
      when '10' then '研究生'
      when '20' then '大学本科（简称“大学”）'
      when '30' then '大学专科和专科学校（简称“大专”）'
      when '40' then '中等专业学校或中等技术学校'
      when '50' then '技术学校'
      when '60' then '高中'
      when '70' then '初中'
      when '80' then '小学'
      when '90' then '文盲或半文盲'
      when '99' then '未知'
      else is_empty(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.edu'))
    end                                                                                                                   as education,
    case
      when get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.edu') in ('30','40','50','60','70','80','90') then '大专及以下'
      when get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.edu') = '10'                                  then '硕士及以上'
      when get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.edu') = '20'                                  then '大学本科'
      when get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.edu') = '99'                                  then '未知'
      else is_empty(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.edu'))
    end                                                                                                                   as education_ws,
    concat(dim_idno.idno_province_cn,dim_idno.idno_city_cn,dim_idno.idno_county_cn)                                       as idcard_address,
    dim_idno.idno_area_cn                                                                                                 as idcard_area,
    dim_idno.idno_province_cn                                                                                             as idcard_province,
    dim_idno.idno_city_cn                                                                                                 as idcard_city,
    dim_idno.idno_county_cn                                                                                               as idcard_county,
    null                                                                                                                  as idcard_township,
    sha256(
      is_empty(
        get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.postAddr'),
        get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.homeAddr')
      ),
      'address',1
    )                                                                                                                     as resident_address,
    null                                                                                                                  as resident_area,
    null                                                                                                                  as resident_province,
    null                                                                                                                  as resident_city,
    null                                                                                                                  as resident_county,
    null                                                                                                                  as resident_township,
    case get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.workWay')
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
      else is_empty(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.workWay'),'空')
    end                                                                                                                   as job_type,
    if(is_empty(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.workYear')) is null,null,
      age_birth(
        is_empty(
          ecas_loan.active_date,
          msg_log.deal_date
        ),
        is_empty(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.workYear'))
      )
    )                                                                                                                     as job_year,
    0                                                                                                                     as job_year_max,
    0                                                                                                                     as job_year_min,
    cast(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.mincome') as decimal(25,5))           as income_month,
    get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.mincome') * 12                             as income_year,
    0                                                                                                                     as income_year_max,
    0                                                                                                                     as income_year_min,
    case get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.custType')
      when '01' then '农户'
      when '02' then '工薪（包括白领、蓝领）'
      when '03' then '个体工商户'
      when '04' then '学生'
      when '99' then '其他'
      else is_empty(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.custType'),'未知')
    end                                                                                                                   as customer_type,
    '个人'                                                                                                                as loan_type,
    null                                                                                                                  as cust_rating,
    msg_log.product_code                                                                                                  as product_id
  from (
    select
      deal_date,
      original_msg,
      get_json_object(original_msg,'$.reqContent.jsonReq.content.reqData.proCode') as product_code,
      get_json_object(original_msg,'$.reqContent.jsonReq.content.reqData.applyNo') as due_bill_no,
      substring(get_json_object(original_msg,'$.reqContent.jsonReq.content.reqData.idNo'),1,6) as idno_addr
    from (
      select
        deal_date,
        replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(
                  regexp_replace(
                    regexp_replace(
                      regexp_replace(
                        original_msg,'\\\\\"\\\{','\\\{'
                      ),'\\\}\\\\\"','\\\}'
                    ),'\\\"\\\{','\\\{'
                  ),'\\\}\\\"','\\\}'
                ),'\\\\\\\\\\\\\"','\\\"'
              ),'\\\\\"','\\\"'
            ),'\\\\\\\\','\\\\'
          ),'\\\\\"',""
        ) as original_msg
      from stage.ecas_msg_log
      where msg_type = 'WIND_CONTROL_CREDIT'
        and original_msg is not null
        --- and is_his = 'N'
        and deal_date = '${ST9}'
    ) as  original_msg
  ) as msg_log
  left join (
    select
      due_bill_no,
      active_date,
      product_code
    from stage.ecas_loan
    where 1 > 0
      and p_type in ('lx','lx2','lxzt','lx3')
      and d_date between date_sub(current_date,2) 
      and date_sub(current_date,1)
      ---and d_date = '2021-06-08'
      group by
      due_bill_no,
      active_date,
      product_code    
  ) as ecas_loan
  on concat(msg_log.product_code,'_',msg_log.due_bill_no) = concat(ecas_loan.product_code,'_',ecas_loan.due_bill_no)
  left join (
    select distinct
      idno_addr,
      idno_area_cn,
      idno_province_cn,
      idno_city_cn,
      idno_county_cn
    from dim.dim_idno
  ) as dim_idno
  on msg_log.idno_addr = dim_idno.idno_addr
  ---union all
  ---select customer_info.* from ods.customer_info
  ---join (
  ---  select distinct get_json_object(replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\\\"\\\{','\\\{'),'\\\}\\\\\"','\\\}'),'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}'),'\\\\\\\\\\\\\"','\\\"'),'\\\\\"','\\\"'),'\\\\\\\\','\\\\'),'\\\\\"',""),'$.reqContent.jsonReq.content.reqData.proCode') as product_code
  ---  from stage.ecas_msg_log
  ---  where msg_type = 'WIND_CONTROL_CREDIT'
  ---    and original_msg is not null
  ---) as product_id_tbl
  ---on customer_info.product_id = product_id_tbl.product_code
) as tmp
-- limit 1
;








-- 瓜子  申请号与借据号一样，所以直接取申请号
insert into table ods.customer_info partition(product_id)
select distinct *
from (
  select
    get_json_object(msg_log.original_msg,'$.data.applyNo')                                                            as apply_id,
    is_empty(
      loan_result.due_bill_no,
      get_json_object(msg_log.original_msg,'$.data.applyNo')
    )                                                                                                                 as due_bill_no,
    concat_ws('_',biz_conf.channel_id,sha256(get_json_object(msg_log.original_msg,'$.data.borrower.idNo'),'idNumber',1),sha256(get_json_object(msg_log.original_msg,'$.data.borrower.name'),'userName',1)) as cust_id,
    sha256(get_json_object(msg_log.original_msg,'$.data.borrower.idNo'),'idNumber',1)                                 as user_hash_no,
    get_json_object(msg_log.original_msg,'$.data.borrower.openId')                                                    as outer_cust_id,
    case get_json_object(msg_log.original_msg,'$.data.borrower.idType')
      when 'I' then '身份证'
      else get_json_object(msg_log.original_msg,'$.data.borrower.idType')
    end                                                                                                               as idcard_type,
    sha256(get_json_object(msg_log.original_msg,'$.data.borrower.idNo'),'idNumber',1)                                 as idcard_no,
    sha256(get_json_object(msg_log.original_msg,'$.data.borrower.name'),'userName',1)                                 as name,
    sha256(get_json_object(msg_log.original_msg,'$.data.borrower.mobilePhone'),'phone',1)                             as mobie,
    sha256(get_json_object(msg_log.original_msg,'$.data.payAccount.mobilePhone'),'phone',1)                           as card_phone,
    sex_idno(get_json_object(msg_log.original_msg,'$.data.borrower.idNo'))                                            as sex,
    datefmt(substring(get_json_object(msg_log.original_msg,'$.data.borrower.idNo'),7,8),'yyyyMMdd','yyyy-MM-dd')      as birthday,
    age_birth(
      datefmt(substring(get_json_object(msg_log.original_msg,'$.data.borrower.idNo'),7,8),'yyyyMMdd','yyyy-MM-dd'),
      is_empty(
        loan_result.issue_time,
        loan_result.deal_date,
        msg_log.deal_date
      )
    )                                                                                                                 as age,
    case get_json_object(msg_log.original_msg,'$.data.borrower.maritalStatus')
      when 'C' then '已婚'
      when 'S' then '未婚'
      when 'D' then '离异'
      when 'P' then '丧偶'
      when 'O' then '其他'
      else get_json_object(msg_log.original_msg,'$.data.borrower.maritalStatus')
    end                                                                                                               as marriage_status,
    case get_json_object(msg_log.original_msg,'$.data.borrower.education')
      when 'A' then '博士及以上'
      when 'B' then '硕士'
      when 'C' then '大学本科'
      when 'D' then '大学专科/专科学校'
      when 'E' then '高中/中专/技校'
      when 'F' then '初中'
      when 'G' then '初中以下'
      when 'Z' then '未知'
      else is_empty(get_json_object(msg_log.original_msg,'$.data.borrower.education'))
    end                                                                                                               as education,
    case
      when get_json_object(msg_log.original_msg,'$.data.borrower.education') in ('A','B')         then '硕士及以上'
      when get_json_object(msg_log.original_msg,'$.data.borrower.education') = 'C'                then '大学本科'
      when get_json_object(msg_log.original_msg,'$.data.borrower.education') in ('D','E','F','G') then '大专及以下'
      when get_json_object(msg_log.original_msg,'$.data.borrower.education') = 'Z'                then '未知'
      else is_empty(get_json_object(msg_log.original_msg,'$.reqContent.jsonReq.content.reqData.edu'))
    end                                                                                                               as education_ws,
    concat(dim_idno.idno_province_cn,dim_idno.idno_city_cn,dim_idno.idno_county_cn)                                   as idcard_address,
    dim_idno.idno_area_cn                                                                                             as idcard_area,
    dim_idno.idno_province_cn                                                                                         as idcard_province,
    dim_idno.idno_city_cn                                                                                             as idcard_city,
    dim_idno.idno_county_cn                                                                                           as idcard_county,
    null                                                                                                              as idcard_township,
    sha256(get_json_object(msg_log.original_msg,'$.data.borrower.homeAddress'),'address',1)                           as resident_address,
    dim_idno_province.idno_area_cn                                                                                    as resident_area,
    get_json_object(msg_log.original_msg,'$.data.borrower.homeProvince')                                              as resident_province,
    get_json_object(msg_log.original_msg,'$.data.borrower.homeCity')                                                  as resident_city,
    get_json_object(msg_log.original_msg,'$.data.borrower.homeArea')                                                  as resident_county,
    null                                                                                                              as resident_township,
    case get_json_object(msg_log.original_msg,'$.data.borrower.industry')
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
      else is_empty(get_json_object(msg_log.original_msg,'$.data.borrower.industry'),'空')
    end                                                                                                               as job_type,
    0                                                                                                                 as job_year,
    0                                                                                                                 as job_year_max,
    0                                                                                                                 as job_year_min,
    is_empty(get_json_object(msg_log.original_msg,'$.data.borrower.annualIncome'),0) / 12                             as income_month,
    cast(is_empty(get_json_object(msg_log.original_msg,'$.data.borrower.annualIncome'),0) as decimal(25,5))           as income_year,
    cast(is_empty(get_json_object(msg_log.original_msg,'$.data.borrower.annualIncomeMin'),0) as decimal(25,5))        as income_year_max,
    cast(is_empty(get_json_object(msg_log.original_msg,'$.data.borrower.annualIncomeMax'),0) as decimal(25,5))        as income_year_min,
    if(is_empty(get_json_object(msg_log.original_msg,'$.companyLoanBool')) = '企业','企业','未知')                    as customer_type,
    is_empty(get_json_object(msg_log.original_msg,'$.companyLoanBool'),'个人')                                        as loan_type,
    null                                                                                                              as cust_rating,
    get_json_object(msg_log.original_msg,'$.data.product.productNo')                                                  as product_id
  from (
    select
      deal_date                                                                                                        as deal_date,
      regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}'),'\\\\\"','\\\"') as original_msg
    from stage.ecas_msg_log
    where 1 > 0
      and msg_type = 'GZ_CREDIT_APPLY'
      and original_msg is not null
      and datefmt(update_time,'ms','yyyy-MM-dd') = '${ST9}'
  ) as msg_log
  left join (
    select
      get_json_object(original_msg,'$.applyNo')  as apply_no,
      get_json_object(original_msg,'$.planDate') as issue_time,
      deal_date                                  as deal_date,
      get_json_object(original_msg,'$.loanNo')   as due_bill_no
    from stage.ecas_msg_log
    where 1 > 0
      and msg_type = 'GZ_LOAN_RESULT'
      and original_msg is not null
  ) as loan_result
  on get_json_object(msg_log.original_msg,'$.data.applyNo') = loan_result.apply_no
  left join (
    select distinct
      product_id as dim_product_id,
      channel_id
    from (
      select
        max(if(col_name = 'product_id',  col_val,null)) as product_id,
        max(if(col_name = 'channel_id',  col_val,null)) as channel_id
      from dim.data_conf
      where col_type = 'ac'
      group by col_id
    ) as tmp
  ) as biz_conf
  on get_json_object(msg_log.original_msg,'$.data.product.productNo') = biz_conf.dim_product_id
  left join (
    select distinct
      idno_addr,
      idno_area_cn,
      idno_province_cn,
      idno_city_cn,
      idno_county_cn
    from dim.dim_idno
  ) as dim_idno
  on substring(get_json_object(msg_log.original_msg,'$.data.borrower.idNo'),1,6) = dim_idno.idno_addr
  left join (
    select distinct
      idno_area_cn,
      idno_province_cn
    from dim.dim_idno
  ) as dim_idno_province
  on get_json_object(msg_log.original_msg,'$.data.borrower.homeProvince') = case substring(dim_idno_province.idno_province_cn,1,2) when '黑龙' then '东北地区' when '内蒙' then '华北地区' else substring(dim_idno_province.idno_province_cn,1,2) end
  ---union all
  ---select customer_info.* from ods.customer_info
  ---join (
  ---  select distinct regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}'),'\\\\\"','\\\"') as original_msg
  ---  from stage.ecas_msg_log
  ---  where msg_type = 'GZ_CREDIT_APPLY'
  ---    and original_msg is not null
  ---) as product_id_tbl
  ---on customer_info.product_id = get_json_object(product_id_tbl.original_msg,'$.data.product.productNo')
) as tmp
-- limit 1
;







msck repair table stage.kafka_credit_msg;

-- 新核心 乐信云信
insert into table ods.customer_info partition(product_id)
select distinct *
from (
  select
    msg_log.reqdata["applyNo"]                                                                                                       as apply_id,
    msg_log.reqdata["applyNo"]                                                                                                       as due_bill_no,
    concat_ws('_',biz_conf.channel_id,sha256(msg_log.reqdata["idNo"],'idNumber',1),sha256(msg_log.reqdata["custName"],'userName',1)) as cust_id,
    sha256(msg_log.reqdata["idNo"],'idNumber',1)                                                                                     as user_hash_no,
    null                                                                                                                             as outer_cust_id,
    case msg_log.reqdata["idType"]
      when '0' then '身份证'
      when '1' then '户口簿'
      when '2' then '护照'
      when '3' then '军官证'
      when '4' then '士兵证'
      when '5' then '港澳居民来往内地通行证'
      when '6' then '台湾同胞来往内地通行证'
      when '7' then '临时身份证'
      when '8' then '外国人居留证'
      when '9' then '警官证'
      when 'A' then '香港身份证'
      when 'B' then '澳门身份证'
      when 'C' then '台湾身份证'
      when 'X' then '其他证件'
      else is_empty(msg_log.reqdata["idType"])
    end                                                                                                                              as idcard_type,
    sha256(msg_log.reqdata["idNo"],'idNumber',1)                                                                                     as idcard_no,
    sha256(msg_log.reqdata["custName"],'userName',1)                                                                                 as name,
    sha256(msg_log.reqdata["phoneNo"],'phone',1)                                                                                     as mobie,
    null                                                                                                                             as card_phone,
    is_empty(
      case msg_log.reqdata["sex"]
        when '1' then '男'
        when '2' then '女'
        else null
      end,
      sex_idno(msg_log.reqdata["idNo"])
    )                                                                                                                                as sex,
    is_empty(
      datefmt(msg_log.reqdata["birth"],'yyyyMMdd','yyyy-MM-dd'),
      datefmt(substring(msg_log.reqdata["idNo"],7,8),'yyyyMMdd','yyyy-MM-dd')
    )                                                                                                                                as birthday,
    age_birth(
      msg_log.reqdata["loanDate"],
      is_empty(
        datefmt(msg_log.reqdata["birth"],'yyyyMMdd','yyyy-MM-dd'),
        datefmt(substring(msg_log.reqdata["idNo"],7,8),'yyyyMMdd','yyyy-MM-dd')
      )
    )                                                                                                                                as age,
    case msg_log.reqdata["marriage"]
      when '10' then '未婚'
      when '20' then '已婚'
      when '21' then '初婚'
      when '22' then '再婚'
      when '23' then '复婚'
      when '30' then '丧偶'
      when '40' then '离婚'
      when '90' then '未说明的婚姻状况'
      else is_empty(msg_log.reqdata["marriage"])
    end                                                                                                                              as marriage_status,
    case msg_log.resdata["edu"]
      when '10' then '研究生'
      when '20' then '大学本科（简称“大学”）'
      when '30' then '大学专科和专科学校（简称“大专”）'
      when '40' then '中等专业学校或中等技术学校'
      when '50' then '技术学校'
      when '60' then '高中'
      when '70' then '初中'
      when '80' then '小学'
      when '90' then '文盲或半文盲'
      when '99' then '未知'
      else is_empty(msg_log.resdata["edu"])
    end                                                                                                                              as education,
    case
      when resdata["edu"] = '10'                                  then '硕士及以上'
      when resdata["edu"] = '20'                                  then '大学本科'
      when resdata["edu"] in ('30','40','50','60','70','80','90') then '大专及以下'
      when resdata["edu"] = '99'                                  then '未知'
      else is_empty(resdata["edu"])
    end                                                                                                                              as education_ws,
    concat(dim_idno.idno_province_cn,dim_idno.idno_city_cn,dim_idno.idno_county_cn)                                                  as idcard_address,
    dim_idno.idno_area_cn                                                                                                            as idcard_area,
    dim_idno.idno_province_cn                                                                                                        as idcard_province,
    dim_idno.idno_city_cn                                                                                                            as idcard_city,
    dim_idno.idno_county_cn                                                                                                          as idcard_county,
    null                                                                                                                             as idcard_township,
    null                                                                                                                             as resident_address,
    null                                                                                                                             as resident_area,
    null                                                                                                                             as resident_province,
    null                                                                                                                             as resident_city,
    null                                                                                                                             as resident_county,
    null                                                                                                                             as resident_township,
    is_empty(null,'空')                                                                                                              as job_type,
    0                                                                                                                                as job_year,
    0                                                                                                                                as job_year_max,
    0                                                                                                                                as job_year_min,
    0                                                                                                                                as income_month,
    0                                                                                                                                as income_year,
    0                                                                                                                                as income_year_max,
    0                                                                                                                                as income_year_min,
    case msg_log.reqdata["custType"]
      when '01' then '农户'
      when '02' then '工薪（包括白领、蓝领）'
      when '03' then '个体工商户'
      when '04' then '学生'
      when '99' then '其他'
      else is_empty(msg_log.reqdata["custType"],'未知')
    end                                                                                                                              as cutomer_type,
    '个人'                                                                                                                           as loan_type,
    null                                                                                                                             as cust_rating,
    msg_log.reqdata["proCode"]                                                                                                       as product_id
  from (
    select
      *
    from stage.kafka_credit_msg
    where 1 > 0
      and p_type = 'WS0013200001'
      and batch_date between date_sub('${ST9}',7) and '${ST9}'
  ) as msg_log
  left join (
    select distinct
      product_id as dim_product_id,
      channel_id
    from (
      select
        max(if(col_name = 'product_id',  col_val,null)) as product_id,
        max(if(col_name = 'channel_id',  col_val,null)) as channel_id
      from dim.data_conf
      where col_type = 'ac'
      group by col_id
    ) as tmp
  ) as biz_conf
  on msg_log.reqdata["proCode"] = biz_conf.dim_product_id
  left join (
    select distinct
      idno_addr,
      idno_area_cn,
      idno_province_cn,
      idno_city_cn,
      idno_county_cn
    from dim.dim_idno
  ) as dim_idno
  on substring(msg_log.reqdata["idNo"],1,6) = dim_idno.idno_addr
  ---union all
  ---select customer_info.* from ods.customer_info
  ---join (
  ---  select distinct reqdata["proCode"] as product_id
  ---  from stage.kafka_credit_msg
  ---  where p_type = 'WS0013200001'
  ---) as product_id_tbl
  ---on customer_info.product_id = product_id_tbl.product_id
) as tmp
-- limit 1
;







-- 新核心 百度医美
insert into table ods.customer_info partition(product_id)
select distinct *
from (
  select
    msg_log.reqdata["applyNo"]                                                      as apply_id,
    msg_log.reqdata["applyNo"]                                                      as due_bill_no,
    concat_ws('_',biz_conf.channel_id,sha256(msg_log.reqdata["idNo"],'idNumber',1),sha256(msg_log.reqdata["custName"],'userName',1)) as cust_id,
    sha256(msg_log.reqdata["idNo"],'idNumber',1)                                    as user_hash_no,
    null                                                                            as outer_cust_id,
    case msg_log.reqdata["idType"]
      when '0' then '身份证'
      when '1' then '户口簿'
      when '2' then '护照'
      when '3' then '军官证'
      when '4' then '士兵证'
      when '5' then '港澳居民来往内地通行证'
      when '6' then '台湾同胞来往内地通行证'
      when '7' then '临时身份证'
      when '8' then '外国人居留证'
      when '9' then '警官证'
      when 'A' then '香港身份证'
      when 'B' then '澳门身份证'
      when 'C' then '台湾身份证'
      when 'X' then '其他证件'
      else is_empty(msg_log.reqdata["idType"])
    end                                                                             as idcard_type,
    sha256(msg_log.reqdata["idNo"],'idNumber',1)                                    as idcard_no,
    sha256(msg_log.reqdata["custName"],'userName',1)                                as name,
    sha256(msg_log.reqdata["phoneNo"],'phone',1)                                    as mobie,
    null                                                                            as card_phone,
    is_empty(
      case msg_log.reqdata["sex"]
        when '1' then '男'
        when '2' then '女'
        else null
      end,
      sex_idno(msg_log.reqdata["idNo"])
    )                                                                               as sex,
    is_empty(
      datefmt(msg_log.reqdata["birth"],'yyyyMMdd','yyyy-MM-dd'),
      datefmt(substring(msg_log.reqdata["idNo"],7,8),'yyyyMMdd','yyyy-MM-dd')
    )                                                                               as birthday,
    age_birth(
      msg_log.reqdata["loanDate"],
      is_empty(
        datefmt(msg_log.reqdata["birth"],'yyyyMMdd','yyyy-MM-dd'),
        datefmt(substring(msg_log.reqdata["idNo"],7,8),'yyyyMMdd','yyyy-MM-dd')
      )
    )                                                                               as age,
    null                                                                            as marriage_status,
    null                                                                            as education,
    null                                                                            as education_ws,
    concat(dim_idno.idno_province_cn,dim_idno.idno_city_cn,dim_idno.idno_county_cn) as idcard_address,
    dim_idno.idno_area_cn                                                           as idcard_area,
    dim_idno.idno_province_cn                                                       as idcard_province,
    dim_idno.idno_city_cn                                                           as idcard_city,
    dim_idno.idno_county_cn                                                         as idcard_county,
    null                                                                            as idcard_township,
    null                                                                            as resident_address,
    null                                                                            as resident_area,
    null                                                                            as resident_province,
    null                                                                            as resident_city,
    null                                                                            as resident_county,
    null                                                                            as resident_township,
    is_empty(null,'空')                                                             as job_type,
    0                                                                               as job_year,
    0                                                                               as job_year_max,
    0                                                                               as job_year_min,
    0                                                                               as income_month,
    0                                                                               as income_year,
    0                                                                               as income_year_max,
    0                                                                               as income_year_min,
    '未知'                                                                          as customer_type,
    '个人'                                                                          as loan_type,
    null                                                                            as cust_rating,
    msg_log.reqdata["proCode"]                                                      as product_id
  from (
    select
      *
    from stage.kafka_credit_msg
    where 1 > 0
      and p_type = 'WS0012200001'
      and batch_date between date_sub('${ST9}',7) and '${ST9}'
  ) as msg_log
  left join (
    select distinct
      product_id as dim_product_id,
      channel_id
    from (
      select
        max(if(col_name = 'product_id',  col_val,null)) as product_id,
        max(if(col_name = 'channel_id',  col_val,null)) as channel_id
      from dim.data_conf
      where col_type = 'ac'
      group by col_id
    ) as tmp
  ) as biz_conf
  on msg_log.reqdata["proCode"] = biz_conf.dim_product_id
  left join (
    select distinct
      idno_addr,
      idno_area_cn,
      idno_province_cn,
      idno_city_cn,
      idno_county_cn
    from dim.dim_idno
  ) as dim_idno
  on substring(msg_log.reqdata["idNo"],1,6) = dim_idno.idno_addr
  ---union all
  ---select customer_info.* from ods.customer_info
  ---join (
  ---  select distinct reqdata["proCode"] as product_id
  ---  from stage.kafka_credit_msg
  ---  where p_type = 'WS0012200001'
  ---) as product_id_tbl
  ---on customer_info.product_id = product_id_tbl.product_id
) as tmp
-- limit 1
;

insert overwrite table ods.customer_info partition(product_id)
select distinct * from ods.customer_info ;









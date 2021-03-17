-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=128000000; -- 128M
set hive.merge.smallfiles.avgsize=128000000; -- 128M
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;


set hivevar:case_encrypt=case substring(dim_encrypt,1,1)
  when '@' then 'default'
  when 'a' then 'idNumber'
  when 'b' then 'passport'
  when 'c' then 'address'
  when 'd' then 'userName'
  when 'e' then 'phone'
  when 'f' then 'bankCard'
  when 'g' then 'imsi'
  when 'h' then 'imei'
  when 'i' then 'plateNumber'
  when 'j' then 'houseNum'
  when 'k' then 'frameNumber'
  when 'l' then 'engineNumber'
  when 'm' then 'businessNumber'
  when 'n' then 'organizateCode'
  when 'o' then 'taxpayerNumber'
  when 'p' then 'unifiedCreditCode'
  else 'unknow' end
;


-- into
-- overwrite
-- 滴滴
insert into table dim.dim_encrypt_info
select
  ${case_encrypt} as dim_type,
  dim_encrypt,
  dim_decrypt
from (
  select
    regexp_replace(
      regexp_replace(
        regexp_replace(
          original_msg,'\\\"\\\{','\\\{'
        ),'\\\}\\\"','\\\}'
      ),'\\\\\"','\\\"'
    ) as original_msg
  from stage.ecas_msg_log
  where msg_type = 'CREDIT_APPLY'
    and original_msg is not null
    and datefmt(update_time,'ms','yyyy-MM-dd') = '${ST9}'
) as log
lateral view explode(map(
  sha256(get_json_object(original_msg,'$.userInfo.ocrInfo.idNo'),'idNumber',1),  get_json_object(original_msg,'$.userInfo.ocrInfo.idNo'),
  sha256(get_json_object(original_msg,'$.userInfo.ocrInfo.name'),'userName',1),  get_json_object(original_msg,'$.userInfo.ocrInfo.name'),
  sha256(get_json_object(original_msg,'$.userInfo.telephone'),'phone',1),        get_json_object(original_msg,'$.userInfo.telephone'),
  sha256(get_json_object(original_msg,'$.userInfo.phone'),'phone',1),            get_json_object(original_msg,'$.userInfo.phone'),
  sha256(get_json_object(original_msg,'$.userInfo.ocrInfo.address'),'address',1),get_json_object(original_msg,'$.userInfo.ocrInfo.address'),
  sha256(get_json_object(original_msg,'$.userInfo.address'),'address',1),        get_json_object(original_msg,'$.userInfo.address')
)) tbl_map as dim_encrypt,dim_decrypt
where 1 > 0
  and dim_encrypt is not null
-- limit 10
;






-- 汇通
insert into table dim.dim_encrypt_info
select
  ${case_encrypt} as dim_type,
  dim_encrypt,
  dim_decrypt
from (
  select
    get_json_object(standard_req_msg,'$.borrower.id_no')            as idcard_no,
    get_json_object(standard_req_msg,'$.borrower.name')             as name,
    get_json_object(standard_req_msg,'$.borrower.mobile_phone')     as mobie,
    get_json_object(standard_req_msg,'$.loan_account.mobile_phone') as card_phone,
    get_json_object(standard_req_msg,'$.borrower.address')          as resident_address
  from stage.nms_interface_resp_log
  where sta_service_method_name = 'setupCustCredit'
    and standard_req_msg is not null
    and datefmt(update_time,'ms','yyyy-MM-dd') = '${ST9}'
) as log
lateral view explode (map(
  sha256(idcard_no,       'idNumber',1),idcard_no,
  sha256(name,            'userName',1),name,
  sha256(mobie,           'phone',1),   mobie,
  sha256(card_phone,      'phone',1),   card_phone,
  sha256(resident_address,'address',1), resident_address
)) tbl_map as dim_encrypt,dim_decrypt
where 1 > 0
  and dim_encrypt is not null
-- limit 10
;






-- 乐信
insert into table dim.dim_encrypt_info
select
  ${case_encrypt} as dim_type,
  dim_encrypt,
  dim_decrypt
from (
  select
    get_json_object(log.original_msg,'$.reqContent.jsonReq.content.reqData.idNo')     as idcard_no,
    get_json_object(log.original_msg,'$.reqContent.jsonReq.content.reqData.custName') as name,
    get_json_object(log.original_msg,'$.reqContent.jsonReq.content.reqData.phoneNo')  as mobie,
    get_json_object(log.original_msg,'$.reqContent.jsonReq.content.reqData.telNo')    as card_phone,
    is_empty(
      get_json_object(log.original_msg,'$.reqContent.jsonReq.content.reqData.postAddr'),
      get_json_object(log.original_msg,'$.reqContent.jsonReq.content.reqData.homeAddr')
    )                                                                                 as resident_address
  from (
    select
      regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(
                original_msg,'\\\\\"\\\{','\\\{'
              ),'\\\}\\\\\"','\\\}'
            ),'\\\"\\\{','\\\{'
          ),'\\\}\\\"','\\\}'
        ),'\\\\',''
      ) as original_msg
    from stage.ecas_msg_log
    where msg_type = 'WIND_CONTROL_CREDIT'
      and original_msg is not null
      and datefmt(update_time,'ms','yyyy-MM-dd') = '${ST9}'
  ) as log
) as log
lateral view explode (map(
  sha256(idcard_no,       'idNumber',1),idcard_no,
  sha256(name,            'userName',1),name,
  sha256(mobie,           'phone',1),   mobie,
  sha256(card_phone,      'phone',1),   card_phone,
  sha256(resident_address,'address',1), resident_address
)) tbl_map as dim_encrypt,dim_decrypt
where 1 > 0
  and dim_encrypt is not null
-- limit 10
;






-- 瓜子
insert into table dim.dim_encrypt_info
select
  ${case_encrypt} as dim_type,
  dim_encrypt,
  dim_decrypt
from (
  select distinct
    get_json_object(msg_log.original_msg,'$.data.borrower.idNo')          as idcard_no,
    get_json_object(msg_log.original_msg,'$.data.borrower.name')          as name,
    get_json_object(msg_log.original_msg,'$.data.borrower.mobilePhone')   as mobie,
    get_json_object(msg_log.original_msg,'$.data.payAccount.mobilePhone') as card_phone,
    get_json_object(msg_log.original_msg,'$.data.borrower.homeAddress')   as resident_address
  from (
    select
      regexp_replace(
        regexp_replace(
          regexp_replace(
            original_msg,'\\\"\\\{','\\\{'
          ),'\\\}\\\"','\\\}'
        ),'\\\\\"','\\\"'
      ) as original_msg
    from stage.ecas_msg_log
    where 1 > 0
      and msg_type = 'GZ_CREDIT_APPLY'
      and original_msg is not null
      and datefmt(update_time,'ms','yyyy-MM-dd') = '${ST9}'
  ) as msg_log
) as log
lateral view explode (map(
  sha256(idcard_no,       'idNumber',1),idcard_no,
  sha256(name,            'userName',1),name,
  sha256(mobie,           'phone',1),   mobie,
  sha256(card_phone,      'phone',1),   card_phone,
  sha256(resident_address,'address',1), resident_address
)) tbl_map as dim_encrypt,dim_decrypt
where 1 > 0
  and dim_encrypt is not null
-- limit 10
;






-- 星云风控表
insert into table dim.dim_encrypt_info
select
  ${case_encrypt} as dim_type,
  dim_encrypt,
  dim_decrypt
from (
  select
    sha256(decrypt_aes(creid_no,    'tencentabs123456'),'idNumber',1) as encrypt_creid_no,
    sha256(decrypt_aes(name,        'tencentabs123456'),'userName',1) as encrypt_name,
    sha256(decrypt_aes(mobile_phone,'tencentabs123456'),'phone',   1) as encrypt_mobile_phone,
    decrypt_aes(creid_no,    'tencentabs123456')                      as decrypt_creid_no,
    decrypt_aes(name,        'tencentabs123456')                      as decrypt_name,
    decrypt_aes(mobile_phone,'tencentabs123456')                      as decrypt_mobile_phone
  from stage.abs_t_wind_control_resp_log
  where 1 > 0
    and to_date(update_time) = '${ST9}'
) as log
lateral view explode (map(
  encrypt_creid_no,    decrypt_creid_no,
  encrypt_name,        decrypt_name,
  encrypt_mobile_phone,decrypt_mobile_phone
)) tbl_map as dim_encrypt,dim_decrypt
where 1 > 0
  and dim_encrypt is not null
-- limit 10
;







-- 校验平台客户表
insert into table dim.dim_encrypt_info
select
  ${case_encrypt} as dim_type,
  dim_encrypt,
  dim_decrypt
from (
  select
    sha256(decrypt_aes(map_from_str(extra_info)['身份证号'],'tencentabs123456'),'idNumber',1) as encrypt_creid_no,
    sha256(decrypt_aes(map_from_str(extra_info)['客户姓名'],'tencentabs123456'),'userName',1) as encrypt_name,
    sha256(decrypt_aes(map_from_str(extra_info)['手机号'],'tencentabs123456'),'phone',   1)   as encrypt_mobile_phone,
    sha256(concat_ws('::',is_empty(map_from_str(extra_info)['客户户籍地址'],concat(map_from_str(extra_info)['客户居住所在省'],map_from_str(extra_info)['客户居住所在市'],map_from_str(extra_info)['客户居住地址'])),is_empty(map_from_str(extra_info)['客户通讯地址'])),'address',1) as encrypt_address,
    decrypt_aes(map_from_str(extra_info)['身份证号'],'tencentabs123456')                      as decrypt_creid_no,
    decrypt_aes(map_from_str(extra_info)['客户姓名'],'tencentabs123456')                      as decrypt_name,
    decrypt_aes(map_from_str(extra_info)['手机号'],'tencentabs123456')                        as decrypt_mobile_phone,
    concat_ws('::',is_empty(map_from_str(extra_info)['客户户籍地址'],concat(map_from_str(extra_info)['客户居住所在省'],map_from_str(extra_info)['客户居住所在市'],map_from_str(extra_info)['客户居住地址'])),is_empty(map_from_str(extra_info)['客户通讯地址']))                      as decrypt_address
  from stage.asset_02_t_principal_borrower_info
  where 1 > 0
    and to_date(create_time) = '${ST9}'
) as log
lateral view explode (map(
  encrypt_creid_no,    decrypt_creid_no,
  encrypt_name,        decrypt_name,
  encrypt_mobile_phone,decrypt_mobile_phone,
  encrypt_address,     decrypt_address
)) tbl_map as dim_encrypt,dim_decrypt
where 1 > 0
  and dim_encrypt is not null
-- limit 10
;







-- 校验平台流水表
insert into table dim.dim_encrypt_info
select
  ${case_encrypt} as dim_type,
  dim_encrypt,
  dim_decrypt
from (
  select
    sha256(decrypt_aes(is_empty(map_from_str(extra_info)['银行帐号']),'tencentabs123456'),'bankCard',1) as encrypt_bank_no,
    sha256(decrypt_aes(is_empty(map_from_str(extra_info)['姓名']),'tencentabs123456'),'userName',1)     as encrypt_bank_name,
    decrypt_aes(is_empty(map_from_str(extra_info)['银行帐号']),'tencentabs123456')                      as decrypt_bank_no,
    decrypt_aes(is_empty(map_from_str(extra_info)['姓名']),'tencentabs123456')                          as decrypt_bank_name
  from stage.asset_06_t_asset_pay_flow
  where 1 > 0
    and datefmt(map_from_str(extra_info)['交易时间'],'','yyyy-MM-dd') = '${ST9}'
) as log
lateral view explode (map(
  encrypt_bank_no,  decrypt_bank_no,
  encrypt_bank_name,decrypt_bank_name
)) tbl_map as dim_encrypt,dim_decrypt
where 1 > 0
  and dim_encrypt is not null
-- limit 10
;







-- 校验平台抵押物信息表
insert into table dim.dim_encrypt_info
select
  ${case_encrypt} as dim_type,
  dim_encrypt,
  dim_decrypt
from (
  select
    sha256(map_from_str(extra_info)['车架号'],'frame_number',1)    as encrypt_frame_num,
    sha256(map_from_str(extra_info)['发动机号'],'engine_number',1) as encrypt_engine_num,
    sha256(map_from_str(extra_info)['车牌号码'],'plateNumber',1)   as encrypt_license_num,
    map_from_str(extra_info)['车架号']                             as decrypt_frame_num,
    map_from_str(extra_info)['发动机号']                           as decrypt_engine_num,
    map_from_str(extra_info)['车牌号码']                           as decrypt_license_num
  from stage.asset_04_t_guaranty_info
  where 1 > 0
    and to_date(create_time) = '${ST9}'
) as log
lateral view explode (map(
  encrypt_frame_num,   decrypt_frame_num,
  encrypt_engine_num,  decrypt_engine_num,
  encrypt_license_num, decrypt_license_num
)) tbl_map as dim_encrypt,dim_decrypt
where 1 > 0
  and dim_encrypt is not null
-- limit 10
;







-- 校验平台联系人
insert into table dim.dim_encrypt_info
select
  ${case_encrypt} as dim_type,
  dim_encrypt,
  dim_decrypt
from (
  select
    sha256(decrypt_aes(customer_name,'tencentabs123456'),'userName',1)  as encrypt_customer_name,
    sha256(decrypt_aes(document_num,'tencentabs123456'),'idNumber',1)   as encrypt_document_num,
    sha256(decrypt_aes(phone_num,'tencentabs123456'),'phone',1)         as encrypt_phone_num,
    sha256(decrypt_aes(unit_phone_number,'tencentabs123456'),'phone',1) as encrypt_unit_phone_number,
    decrypt_aes(customer_name,'tencentabs123456')                       as decrypt_customer_name,
    decrypt_aes(document_num,'tencentabs123456')                        as decrypt_document_num,
    decrypt_aes(phone_num,'tencentabs123456')                           as decrypt_phone_num,
    decrypt_aes(unit_phone_number,'tencentabs123456')                   as decrypt_unit_phone_number
  from stage.asset_03_t_contact_person_info
  where 1 > 0
    and to_date(create_time) = '${ST9}'
) as log
lateral view explode (map(
  encrypt_customer_name,    decrypt_customer_name,
  encrypt_document_num,     decrypt_document_num,
  encrypt_phone_num,        decrypt_phone_num,
  encrypt_unit_phone_number,decrypt_unit_phone_number
)) tbl_map as dim_encrypt,dim_decrypt
where 1 > 0
  and dim_encrypt is not null
-- limit 10
;







-- 企业信息校验平台
insert into table dim.dim_encrypt_info
select
  ${case_encrypt} as dim_type,
  dim_encrypt,
  dim_decrypt
from (
  select
    sha256(is_empty(business_number),'businessNumber',1)                             as encrypt_business_number,     -- 工商注册号
    sha256(is_empty(organizate_code),'organizateCode',1)                             as encrypt_organizate_code,     -- 组织机构代码
    sha256(is_empty(taxpayer_number),'taxpayerNumber',1)                             as encrypt_taxpayer_number,     -- 纳税人识别号
    sha256(is_empty(unified_credit_code),'unifiedCreditCode',1)                      as encrypt_unified_credit_code, -- 统一信用代码
    sha256(is_empty(registered_address),'address',1)                                 as encrypt_registered_address,  -- 注册地址
    sha256(decrypt_aes(is_empty(legal_person_name),'tencentabs123456'),'userName',1) as encrypt_legal_person_name,   -- 法人代表姓名
    sha256(decrypt_aes(is_empty(id_no),'tencentabs123456'),'idNumber',1)             as encrypt_id_no,               -- 法人证件号码
    sha256(decrypt_aes(is_empty(legal_person_phone),'tencentabs123456'),'phone',1)   as encrypt_legal_person_phone,  -- 法人手机号码
    sha256(decrypt_aes(is_empty(phone),'tencentabs123456'),'phone',1)                as encrypt_phone,               -- 企业联系电话

    is_empty(business_number)                                                        as decrypt_business_number,     -- 工商注册号
    is_empty(organizate_code)                                                        as decrypt_organizate_code,     -- 组织机构代码
    is_empty(taxpayer_number)                                                        as decrypt_taxpayer_number,     -- 纳税人识别号
    is_empty(unified_credit_code)                                                    as decrypt_unified_credit_code, -- 统一信用代码
    is_empty(registered_address)                                                     as decrypt_registered_address,  -- 注册地址
    decrypt_aes(is_empty(legal_person_name),'tencentabs123456')                      as decrypt_legal_person_name,   -- 法人代表姓名
    decrypt_aes(is_empty(id_no),'tencentabs123456')                                  as decrypt_id_no,               -- 法人证件号码
    decrypt_aes(is_empty(legal_person_phone),'tencentabs123456')                     as decrypt_legal_person_phone,  -- 法人手机号码
    decrypt_aes(is_empty(phone),'tencentabs123456')                                  as decrypt_phone                -- 企业联系电话
  from stage.asset_12_t_enterprise_info
  where 1 > 0
    and to_date(create_time) = '${ST9}'
) as log
lateral view explode (map(
  encrypt_business_number,    decrypt_business_number,
  encrypt_organizate_code,    decrypt_organizate_code,
  encrypt_taxpayer_number,    decrypt_taxpayer_number,
  encrypt_unified_credit_code,decrypt_unified_credit_code,
  encrypt_registered_address, decrypt_registered_address,
  encrypt_legal_person_name,  decrypt_legal_person_name,
  encrypt_id_no,              decrypt_id_no,
  encrypt_legal_person_phone, decrypt_legal_person_phone,
  encrypt_phone,              decrypt_phone
)) tbl_map as dim_encrypt,dim_decrypt
where 1 > 0
  and dim_encrypt is not null
-- limit 10
;




-- -- 企业信息星云表
-- insert into table dim.dim_encrypt_info
-- select
--   ${case_encrypt} as dim_type,
--   dim_encrypt,
--   dim_decrypt
-- from (
--   select
--     sha256(is_empty(registration_number),'businessNumber',1)                            as encrypt_business_number,     -- 工商注册号
--     sha256(is_empty(organization_code),'organizateCode',1)                              as encrypt_organizate_code,     -- 组织机构代码
--     sha256(is_empty(taxpayer_identification_number),'taxpayerNumber',1)                 as encrypt_taxpayer_number,     -- 纳税人识别号
--     sha256(is_empty(uniform_credit_code),'unifiedCreditCode',1)                         as encrypt_unified_credit_code, -- 统一信用代码
--     sha256(is_empty(registered_address),'address',1)                                    as encrypt_registered_address,  -- 注册地址
--     sha256(decrypt_aes(is_empty(legal_person_name),'tencentabs123456'),'userName',1)    as encrypt_legal_person_name,   -- 法人代表姓名
--     sha256(decrypt_aes(is_empty(legal_person_card_no),'tencentabs123456'),'idNumber',1) as encrypt_id_no,               -- 法人证件号码
--     sha256(decrypt_aes(is_empty(legal_person_phoneno),'tencentabs123456'),'phone',1)    as encrypt_legal_person_phone,  -- 法人手机号码
--     sha256(decrypt_aes(is_empty(enterprise_phoneno),'tencentabs123456'),'phone',1)      as encrypt_phone,               -- 企业联系电话

--     is_empty(registration_number)                                                       as decrypt_business_number,     -- 工商注册号
--     is_empty(organization_code)                                                         as decrypt_organizate_code,     -- 组织机构代码
--     is_empty(taxpayer_identification_number)                                            as decrypt_taxpayer_number,     -- 纳税人识别号
--     is_empty(uniform_credit_code)                                                       as decrypt_unified_credit_code, -- 统一信用代码
--     is_empty(registered_address)                                                        as decrypt_registered_address,  -- 注册地址
--     decrypt_aes(is_empty(legal_person_name),'tencentabs123456')                         as decrypt_legal_person_name,   -- 法人代表姓名
--     decrypt_aes(is_empty(legal_person_card_no),'tencentabs123456')                      as decrypt_id_no,               -- 法人证件号码
--     decrypt_aes(is_empty(legal_person_phoneno),'tencentabs123456')                      as decrypt_legal_person_phone,  -- 法人手机号码
--     decrypt_aes(is_empty(enterprise_phoneno),'tencentabs123456')                        as decrypt_phone                -- 企业联系电话
--   from stage.t_12_enterpriseinfo
--   where 1 > 0
--     and to_date(create_time) = '${ST9}'
--     -- and serial_number = '1000002809'
-- ) as log
-- lateral view explode (map(
--   encrypt_business_number,    decrypt_business_number,
--   encrypt_organizate_code,    decrypt_organizate_code,
--   encrypt_taxpayer_number,    decrypt_taxpayer_number,
--   encrypt_unified_credit_code,decrypt_unified_credit_code,
--   encrypt_registered_address, decrypt_registered_address,
--   encrypt_legal_person_name,  decrypt_legal_person_name,
--   encrypt_id_no,              decrypt_id_no,
--   encrypt_legal_person_phone, decrypt_legal_person_phone,
--   encrypt_phone,              decrypt_phone
-- )) tbl_map as dim_encrypt,dim_decrypt
-- where 1 > 0
--   and dim_encrypt is not null
-- -- limit 10
-- ;




-- 去重
set hivevar:split_str=~~~;
insert overwrite table dim.dim_encrypt_info
select
  split(product_id,'${split_str}')[0] as dim_type,
  split(product_id,'${split_str}')[1] as dim_encrypt,
  split(product_id,'${split_str}')[2] as dim_decrypt
from (
  select distinct
    concat_ws('${split_str}',dim_type,dim_encrypt,dim_decrypt) as product_id
  from dim.dim_encrypt_info
  where 1 > 0
    and dim_encrypt is not null
) as tmp
-- limit 10
;

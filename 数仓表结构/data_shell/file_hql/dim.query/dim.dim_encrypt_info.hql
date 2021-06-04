-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;
-- 关闭自动 MapJoin （ Hive3 的 bug，引发 No work found for tablescan ）
set hive.auto.convert.join=false;
set hive.auto.convert.join.noconditionaltask=false;


-- 滴滴
insert into table dim.dim_encrypt_info
select dim_encrypt,dim_decrypt
from (
  select
    get_json_object(js2str(original_msg),'$.userInfo.ocrInfo.idNo')    as col_1,
    get_json_object(js2str(original_msg),'$.userInfo.ocrInfo.name')    as col_2,
    get_json_object(js2str(original_msg),'$.userInfo.telephone')       as col_3,
    get_json_object(js2str(original_msg),'$.userInfo.phone')           as col_4,
    get_json_object(js2str(original_msg),'$.userInfo.ocrInfo.address') as col_5,
    get_json_object(js2str(original_msg),'$.userInfo.address')         as col_6
  from stage.ecas_msg_log
  where 1 > 0
    and is_his = 'N'
    and msg_type = 'CREDIT_APPLY'
    and original_msg is not null
    and datefmt(update_time,'ms','yyyy-MM-dd') = '${ST9}'
) as log
lateral view explode(map(
  sha256(col_1,'idNumber',1),col_1,
  sha256(col_2,'userName',1),col_2,
  sha256(col_3,'phone',   1),col_3,
  sha256(col_4,'phone',   1),col_4,
  sha256(col_5,'address', 1),col_5,
  sha256(col_6,'address', 1),col_6
)) tbl_map as dim_encrypt,dim_decrypt
where dim_encrypt is not null
-- limit 10
;






-- 汇通
insert into table dim.dim_encrypt_info
select dim_encrypt,dim_decrypt
from (
  select
    get_json_object(standard_req_msg,'$.borrower.id_no')            as col_1,
    get_json_object(standard_req_msg,'$.borrower.name')             as col_2,
    get_json_object(standard_req_msg,'$.borrower.mobile_phone')     as col_3,
    get_json_object(standard_req_msg,'$.loan_account.mobile_phone') as col_4,
    get_json_object(standard_req_msg,'$.borrower.address')          as col_5
  from stage.nms_interface_resp_log
  where sta_service_method_name = 'setupCustCredit'
    and standard_req_msg is not null
    and datefmt(update_time,'ms','yyyy-MM-dd') = '${ST9}'
) as log
lateral view explode (map(
  sha256(col_1,'idNumber',1),col_1,
  sha256(col_2,'userName',1),col_2,
  sha256(col_3,'phone',   1),col_3,
  sha256(col_4,'phone',   1),col_4,
  sha256(col_5,'address', 1),col_5
)) tbl_map as dim_encrypt,dim_decrypt
where dim_encrypt is not null
-- limit 10
;






-- 乐信  使用 js2str 跑不出来，报错
insert into table dim.dim_encrypt_info
select dim_encrypt,dim_decrypt
from (
  select
    get_json_object(original_msg,'$.reqContent.jsonReq.content.reqData.idNo')     as col_1,
    get_json_object(original_msg,'$.reqContent.jsonReq.content.reqData.custName') as col_2,
    get_json_object(original_msg,'$.reqContent.jsonReq.content.reqData.phoneNo')  as col_3,
    get_json_object(original_msg,'$.reqContent.jsonReq.content.reqData.telNo')    as col_4,
    is_empty(
      get_json_object(original_msg,'$.reqContent.jsonReq.content.reqData.postAddr'),
      get_json_object(original_msg,'$.reqContent.jsonReq.content.reqData.homeAddr')
    )                                                                             as col_5
  from (
    select
      replace(
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
        ),'\\\\\"',""
      ) as original_msg
    from stage.ecas_msg_log
    where 1 > 0
      and is_his = 'N'
      -- and is_his = 'Y'
      and msg_type = 'WIND_CONTROL_CREDIT'
      and original_msg is not null
      and datefmt(update_time,'ms','yyyy-MM-dd') = '${ST9}'
  ) as msg_log
) as log
lateral view explode (map(
  sha256(col_1,'idNumber',1),col_1,
  sha256(col_2,'userName',1),col_2,
  sha256(col_3,'phone',   1),col_3,
  sha256(col_4,'phone',   1),col_4,
  sha256(col_5,'address', 1),col_5
)) tbl_map as dim_encrypt,dim_decrypt
where dim_encrypt is not null
-- limit 10
;






-- 瓜子
insert into table dim.dim_encrypt_info
select dim_encrypt,dim_decrypt
from (
  select
    get_json_object(js2str(original_msg),'$.data.borrower.idNo')          as col_1,
    get_json_object(js2str(original_msg),'$.data.borrower.name')          as col_2,
    get_json_object(js2str(original_msg),'$.data.borrower.mobilePhone')   as col_3,
    get_json_object(js2str(original_msg),'$.data.payAccount.mobilePhone') as col_4,
    get_json_object(js2str(original_msg),'$.data.borrower.homeAddress')   as col_5
  from stage.ecas_msg_log
  where 1 > 0
    and is_his = 'N'
    and msg_type = 'GZ_CREDIT_APPLY'
    and original_msg is not null
    and datefmt(update_time,'ms','yyyy-MM-dd') = '${ST9}'
) as log
lateral view explode (map(
  sha256(col_1,'idNumber',1),col_1,
  sha256(col_2,'userName',1),col_2,
  sha256(col_3,'phone',   1),col_3,
  sha256(col_4,'phone',   1),col_4,
  sha256(col_5,'address', 1),col_5
)) tbl_map as dim_encrypt,dim_decrypt
where dim_encrypt is not null
-- limit 10
;






-- 星云风控表
insert into table dim.dim_encrypt_info
select dim_encrypt,dim_decrypt
from (
  select
    decrypt_aes(creid_no,    'tencentabs123456') as col_1,
    decrypt_aes(name,        'tencentabs123456') as col_2,
    decrypt_aes(mobile_phone,'tencentabs123456') as col_3
  from stage.abs_t_wind_control_resp_log
  where 1 > 0
    and to_date(update_time) = '${ST9}'
) as log
lateral view explode (map(
  sha256(col_1,'idNumber',1),col_1,
  sha256(col_2,'userName',1),col_2,
  sha256(col_3,'phone',   1),col_3
)) tbl_map as dim_encrypt,dim_decrypt
where dim_encrypt is not null
-- limit 10
;







-- 校验平台客户表
insert into table dim.dim_encrypt_info
select dim_encrypt,dim_decrypt
from (
  select
    decrypt_aes(col_1,'tencentabs123456')                                     as col_1,
    decrypt_aes(col_2,'tencentabs123456')                                     as col_2,
    decrypt_aes(col_3,'tencentabs123456')                                     as col_3,
    concat_ws('::',is_empty(col_4,concat(col_5,col_6,col_7)),is_empty(col_8)) as col_4
  from stage.asset_02_t_principal_borrower_info
  lateral view json_tuple(extra_info,'身份证号','客户姓名','手机号','客户户籍地址','客户居住所在省','客户居住所在市','客户居住地址','客户通讯地址') state_json as col_1,col_2,col_3,col_4,col_5,col_6,col_7,col_8
  where 1 > 0
    and to_date(create_time) = '${ST9}'
) as log
lateral view explode (map(
  sha256(col_1,'idNumber',1),col_1,
  sha256(col_2,'userName',1),col_2,
  sha256(col_3,'phone',   1),col_3,
  sha256(col_4,'address', 1),col_4
)) tbl_map as dim_encrypt,dim_decrypt
where dim_encrypt is not null
-- limit 10
;





-- 校验平台流水表
insert into table dim.dim_encrypt_info
select dim_encrypt,dim_decrypt
from (
  select
    decrypt_aes(col_1,'tencentabs123456') as col_1,
    decrypt_aes(col_2,'tencentabs123456') as col_2
  from stage.asset_06_t_asset_pay_flow
  lateral view json_tuple(extra_info,'银行帐号','姓名') state_json as col_1,col_2
  where 1 > 0
    and datefmt(map_from_str(extra_info)['交易时间'],'','yyyy-MM-dd') = '${ST9}'
) as log
lateral view explode (map(
  sha256(col_1,'bankCard',1),col_1,
  sha256(col_2,'userName',1),col_2
)) tbl_map as dim_encrypt,dim_decrypt
where dim_encrypt is not null
-- limit 10
;







-- 校验平台抵押物信息表
insert into table dim.dim_encrypt_info
select dim_encrypt,dim_decrypt
from (
  select col_1,col_2,col_3
  from stage.asset_04_t_guaranty_info
  lateral view json_tuple(extra_info,'车架号','发动机号','车牌号码') state_json as col_1,col_2,col_3
  where 1 > 0
    and to_date(create_time) = '${ST9}'
) as log
lateral view explode (map(
  sha256(col_1,'frameNumber', 1),col_1,
  sha256(col_2,'engineNumber',1),col_2,
  sha256(col_3,'plateNumber', 1),col_3
)) tbl_map as dim_encrypt,dim_decrypt
where dim_encrypt is not null
-- limit 10
;







-- 校验平台联系人
insert into table dim.dim_encrypt_info
select dim_encrypt,dim_decrypt
from (
  select
    decrypt_aes(customer_name,    'tencentabs123456') as col_1,
    decrypt_aes(document_num,     'tencentabs123456') as col_2,
    decrypt_aes(phone_num,        'tencentabs123456') as col_3,
    decrypt_aes(unit_phone_number,'tencentabs123456') as col_4
  from stage.asset_03_t_contact_person_info
  where 1 > 0
    and to_date(create_time) = '${ST9}'
) as log
lateral view explode (map(
  sha256(col_1,'userName',1),col_1,
  sha256(col_2,'idNumber',1),col_2,
  sha256(col_3,'phone',   1),col_3,
  sha256(col_4,'phone',   1),col_4
)) tbl_map as dim_encrypt,dim_decrypt
where dim_encrypt is not null
-- limit 10
;







-- 企业信息校验平台
insert into table dim.dim_encrypt_info
select dim_encrypt,dim_decrypt
from (
  select
    is_empty(business_number)                          as col_1, -- 工商注册号
    is_empty(organizate_code)                          as col_2, -- 组织机构代码
    is_empty(taxpayer_number)                          as col_3, -- 纳税人识别号
    is_empty(unified_credit_code)                      as col_4, -- 统一信用代码
    is_empty(registered_address)                       as col_5, -- 注册地址
    decrypt_aes(legal_person_name, 'tencentabs123456') as col_6, -- 法人代表姓名
    decrypt_aes(id_no,             'tencentabs123456') as col_7, -- 法人证件号码
    decrypt_aes(legal_person_phone,'tencentabs123456') as col_8, -- 法人手机号码
    decrypt_aes(phone,             'tencentabs123456') as col_9  -- 企业联系电话
  from stage.asset_12_t_enterprise_info
  where 1 > 0
    and to_date(create_time) = '${ST9}'
) as log
lateral view explode (map(
  sha256(col_1,'businessNumber',   1),col_1,
  sha256(col_2,'organizateCode',   1),col_2,
  sha256(col_3,'taxpayerNumber',   1),col_3,
  sha256(col_4,'unifiedCreditCode',1),col_4,
  sha256(col_5,'address',          1),col_5,
  sha256(col_6,'userName',         1),col_6,
  sha256(col_7,'idNumber',         1),col_7,
  sha256(col_8,'phone',            1),col_8,
  sha256(col_9,'phone',            1),col_9
)) tbl_map as dim_encrypt,dim_decrypt
where dim_encrypt is not null
-- limit 10
;







-- 百度医美
msck repair table stage.kafka_credit_msg;
insert into table dim.dim_encrypt_info
select dim_encrypt,dim_decrypt
from (
  select
    reqdata["idNo"]     as col_1,
    reqdata["custName"] as col_2,
    reqdata["phoneNo"]  as col_3,
    reqdata["phoneNo"]  as col_4,
    reqdata["idAddr"]   as col_5
  from stage.kafka_credit_msg
  where 1 > 0
    and batch_date = '${ST9}'
    and p_type = 'WS0012200001'
) as log
lateral view explode (map(
  sha256(col_1,'idNumber',1),col_1,
  sha256(col_2,'userName',1),col_2,
  sha256(col_3,'phone',   1),col_3,
  sha256(col_4,'phone',   1),col_4,
  sha256(col_5,'address', 1),col_5
)) tbl_map as dim_encrypt,dim_decrypt
where dim_encrypt is not null
-- limit 10
;







-- 乐信云信
insert into table dim.dim_encrypt_info
select dim_encrypt,dim_decrypt
from (
  select
    reqdata["idNo"]     as col_1,
    reqdata["custName"] as col_2,
    reqdata["phoneNo"]  as col_3,
    reqdata["phoneNo"]  as col_4,
    reqdata["idAddr"]   as col_5
  from stage.kafka_credit_msg
  where 1 > 0
    and batch_date = '${ST9}'
    and p_type = 'WS0013200001'
) as log
lateral view explode (map(
  sha256(col_1,'idNumber',1),col_1,
  sha256(col_2,'userName',1),col_2,
  sha256(col_3,'phone',   1),col_3,
  sha256(col_4,'phone',   1),col_4,
  sha256(col_5,'address', 1),col_5
)) tbl_map as dim_encrypt,dim_decrypt
where dim_encrypt is not null
-- limit 10
;

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


-- 企业信息校验平台
insert overwrite table ods.enterprise_info partition(product_id)
select distinct
  asset_id                                                                         as due_bill_no,         -- 资产借据号
  contract_role                                                                    as contract_role,       -- 合同角色（0：主借款企业，1：共同借款企业，2：担保企业，3：无）
  enterprise_name                                                                  as enterprise_name,     -- 企业，名称
  sha256(is_empty(business_number),'businessNumber',1)                             as business_number,     -- 工商注册号
  sha256(is_empty(organizate_code),'organizateCode',1)                             as organizate_code,     -- 组织机构代码
  sha256(is_empty(taxpayer_number),'taxpayerNumber',1)                             as taxpayer_number,     -- 纳税人识别号
  sha256(is_empty(unified_credit_code),'unifiedCreditCode',1)                      as unified_credit_code, -- 统一信用代码
  sha256(is_empty(registered_address),'address',1)                                 as registered_address,  -- 注册地址
  loan_type                                                                        as loan_type,           -- 借款方类型
  industry                                                                         as industry,            -- 企业行业
  sha256(decrypt_aes(is_empty(legal_person_name),'tencentabs123456'),'userName',1) as legal_person_name,   -- 法人代表姓名
  id_type                                                                          as id_type,             -- 法人证件类型
  sha256(decrypt_aes(is_empty(id_no),'tencentabs123456'),'idNumber',1)             as id_no,               -- 法人证件号码
  sha256(decrypt_aes(is_empty(legal_person_phone),'tencentabs123456'),'phone',1)   as legal_person_phone,  -- 法人手机号码
  sha256(decrypt_aes(is_empty(phone),'tencentabs123456'),'phone',1)                as phone,               -- 企业联系电话
  operate_years                                                                    as operate_years,       -- 企业运营年限
  is_linked                                                                        as is_linked,           -- 是否挂靠企业
  province                                                                         as province,            -- 企业省份
  create_time                                                                      as create_time,         -- 创建时间
  update_time                                                                      as update_time,         -- 更新时间
  case project_id when 'Cl00333' then 'cl00333' else project_id end                as project_id           -- 项目编号
from stage.asset_12_t_enterprise_info
-- limit 10
;



-- -- 企业信息星云表
-- insert overwrite table ods.enterprise_info partition(product_id)
-- select distinct
--   serial_number                                                                       as due_bill_no,         -- 借据号
--   contract_role                                                                       as contract_role,       -- 合同角色（预定义字段：主借款企业、共同借款企业、担保企业、无）
--   enterprise_name                                                                     as enterprise_name,     -- 企业姓名
--   sha256(is_empty(registration_number),'businessNumber',1)                            as business_number,     -- 工商注册号
--   sha256(is_empty(organization_code),'organizateCode',1)                              as organizate_code,     -- 组织机构代码
--   sha256(is_empty(taxpayer_identification_number),'taxpayerNumber',1)                 as taxpayer_number,     -- 纳税人识别号
--   sha256(is_empty(uniform_credit_code),'unifiedCreditCode',1)                         as unified_credit_code, -- 统一信用代码
--   sha256(is_empty(registered_address),'address',1)                                    as registered_address,  -- 注册地址
--   borrower_type                                                                       as loan_type,           -- 借款方类型
--   enterprise_industry                                                                 as industry,            -- 企业行业
--   sha256(decrypt_aes(is_empty(legal_person_name),'tencentabs123456'),'userName',1)    as legal_person_name,   -- 法人代表姓名
--   legal_person_card_type                                                              as id_type,             -- 法人证件类型
--   sha256(decrypt_aes(is_empty(legal_person_card_no),'tencentabs123456'),'idNumber',1) as id_no,               -- 法人证件号码
--   sha256(decrypt_aes(is_empty(legal_person_phoneno),'tencentabs123456'),'phone',1)    as legal_person_phone,  -- 法人手机号码
--   sha256(decrypt_aes(is_empty(enterprise_phoneno),'tencentabs123456'),'phone',1)      as phone,               -- 企业联系电话
--   operation_years                                                                     as operate_years,       -- 企业运营年限
--   is_affiliated                                                                       as is_linked,           -- 是否挂靠企业
--   registered_province                                                                 as province,            -- 企业省份
--   create_time                                                                         as create_time,         -- 创建时间
--   update_time                                                                         as update_time,         -- 更新时间
--   project_id                                                                          as product_id           -- 项目编号
-- from stage.abs_12_t_enterpriseinfo
-- where 1 > 0
--   -- and serial_number = '1000002809'
-- -- limit 10
-- ;



-- insert overwrite table ods_new_s.enterprise_info partition(product_id) select distinct * from ods_new_s.enterprise_info;

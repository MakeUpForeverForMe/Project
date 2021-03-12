-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=128000000;
set hive.merge.smallfiles.avgsize=128000000;
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;
-- 支持正则表达式
set hive.support.quoted.identifiers=None;



-- 滴滴
insert overwrite table ods.linkman_info partition (product_id)
select
  `(rn)?+.+`
from (
  select *,row_number() over (partition by linkman_id order by deal_date desc) as rn
  from (
    select distinct
      concat_ws('_',biz_conf.channel_id,linkman.user_hash_no,linkman.name) as cust_id,
      is_empty(linkman.user_hash_no)                                       as user_hash_no,
      is_empty(linkman.due_bill_no)                                        as due_bill_no,
      is_empty(linkman.linkman_id)                                         as linkman_id,
      is_empty(linkman.relational_type)                                    as relational_type,
      is_empty(linkman.relational_type_cn)                                 as relational_type_cn,
      is_empty(linkman.relationship)                                       as relationship,
      is_empty(linkman.relationship_cn)                                    as relationship_cn,
      is_empty(linkman.relation_idcard_type)                               as relation_idcard_type,
      is_empty(linkman.relation_idcard_no)                                 as relation_idcard_no,
      is_empty(linkman.relation_birthday)                                  as relation_birthday,
      is_empty(linkman.relation_name)                                      as relation_name,
      is_empty(linkman.relation_sex)                                       as relation_sex,
      is_empty(linkman.relation_mobile)                                    as relation_mobile,
      is_empty(linkman.relation_address)                                   as relation_address,
      is_empty(linkman.relation_province)                                  as relation_province,
      is_empty(linkman.relation_city)                                      as relation_city,
      is_empty(linkman.relation_county)                                    as relation_county,
      is_empty(linkman.corp_type)                                          as corp_type,
      is_empty(linkman.corp_name)                                          as corp_name,
      is_empty(linkman.corp_teleph_nbr)                                    as corp_teleph_nbr,
      is_empty(linkman.corp_fax)                                           as corp_fax,
      is_empty(linkman.corp_position)                                      as corp_position,
      is_empty(linkman.deal_date)                                          as deal_date,
      is_empty(linkman.create_time)                                        as create_time,
      is_empty(linkman.update_time)                                        as update_time,
      is_empty(linkman.product_id)                                         as product_id
    from (
      select distinct
        product_id,
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
    join (
      select distinct
        sha256(get_json_object(original_msg,'$.idNo'),'idNumber',1)                                     as user_hash_no,
        sha256(get_json_object(original_msg,'$.name'),'userName',1)                                     as name,
        get_json_object(original_msg,'$.loanOrderId')                                                   as due_bill_no,
        concat_ws('~',
          sha256(get_json_object(original_msg,'$.withdrawContactInfo.emergencyContactMobile'),'phone',1),
          sha256(get_json_object(original_msg,'$.withdrawContactInfo.emergencyContactName'),'userName',1),
          get_json_object(original_msg,'$.loanOrderId')
        )                                                                                               as linkman_id,
        null                                                                                            as relational_type,
        null                                                                                            as relational_type_cn,
        get_json_object(original_msg,'$.withdrawContactInfo.relationship')                              as relationship,
        case get_json_object(original_msg,'$.withdrawContactInfo.relationship')
        when '1' then '父母'
        when '2' then '配偶'
        when '3' then '子女'
        when '4' then '兄弟姐妹'
        else get_json_object(original_msg,'$.withdrawContactInfo.relationship')
        end                                                                                             as relationship_cn,
        null                                                                                            as relation_idcard_type,
        null                                                                                            as relation_idcard_no,
        null                                                                                            as relation_birthday,
        sha256(get_json_object(original_msg,'$.withdrawContactInfo.emergencyContactName'),'userName',1) as relation_name,
        null                                                                                            as relation_sex,
        sha256(get_json_object(original_msg,'$.withdrawContactInfo.emergencyContactMobile'),'phone',1)  as relation_mobile,
        null                                                                                            as relation_address,
        null                                                                                            as relation_province,
        null                                                                                            as relation_city,
        null                                                                                            as relation_county,
        null                                                                                            as corp_type,
        null                                                                                            as corp_name,
        null                                                                                            as corp_teleph_nbr,
        null                                                                                            as corp_fax,
        null                                                                                            as corp_position,
        deal_date                                                                                       as deal_date,
        create_time                                                                                     as create_time,
        update_time                                                                                     as update_time,
        product_id                                                                                      as product_id
      from (
        select
          'DIDI201908161538' as product_id,
          deal_date,
          datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as create_time,
          datefmt(update_time,'ms','yyyy-MM-dd HH:mm:ss') as update_time,
          regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\\',''),'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}') as original_msg
        from stage.ecas_msg_log
        where msg_type = 'LOAN_APPLY'
          and original_msg is not null
          and deal_date between date_sub('${ST9}',4) and date_add('${ST9}',2)
      ) as msg_log
    ) as linkman
    on linkman.product_id = biz_conf.product_id
    left join (
      select
        linkman_id
      from ods.linkman_info
      where 1 > 0
        and product_id = 'DIDI201908161538'
        and deal_date <= date_add('${ST9}',2)
        -- and false
    ) as history
    on linkman.linkman_id = history.linkman_id
    where history.linkman_id is null
    union all
    select * from ods.linkman_info
    where 1 > 0
      and product_id = 'DIDI201908161538'
      and deal_date <= date_add('${ST9}',2)
      -- and false
  ) as tmp
) as tmp
where tmp.rn = 1
-- limit 10
;










-- 汇通
insert overwrite table ods.linkman_info partition (product_id)
select
  `(rn)?+.+`
from (
  select *,row_number() over (partition by linkman_id order by deal_date desc) as rn
  from (
    select distinct
      concat(biz_conf.channel_id,'_',linkman.user_hash_no,linkman.name) as cust_id,
      is_empty(linkman.user_hash_no)                                    as user_hash_no,
      is_empty(linkman.due_bill_no)                                     as due_bill_no,
      is_empty(linkman.linkman_id)                                      as linkman_id,
      is_empty(linkman.relational_type)                                 as relational_type,
      is_empty(linkman.relational_type_cn)                              as relational_type_cn,
      is_empty(linkman.relationship)                                    as relationship,
      is_empty(linkman.relationship_cn)                                 as relationship_cn,
      is_empty(linkman.relation_idcard_type)                            as relation_idcard_type,
      is_empty(linkman.relation_idcard_no)                              as relation_idcard_no,
      is_empty(linkman.relation_birthday)                               as relation_birthday,
      is_empty(linkman.relation_name)                                   as relation_name,
      is_empty(linkman.relation_sex)                                    as relation_sex,
      is_empty(linkman.relation_mobile)                                 as relation_mobile,
      is_empty(linkman.relation_address)                                as relation_address,
      is_empty(linkman.relation_province)                               as relation_province,
      is_empty(linkman.relation_city)                                   as relation_city,
      is_empty(linkman.relation_county)                                 as relation_county,
      is_empty(linkman.corp_type)                                       as corp_type,
      is_empty(linkman.corp_name)                                       as corp_name,
      is_empty(linkman.corp_teleph_nbr)                                 as corp_teleph_nbr,
      is_empty(linkman.corp_fax)                                        as corp_fax,
      is_empty(linkman.corp_position)                                   as corp_position,
      is_empty(linkman.deal_date)                                       as deal_date,
      is_empty(linkman.create_time)                                     as create_time,
      is_empty(linkman.update_time)                                     as update_time,
      is_empty(linkman.product_id)                                      as product_id
    from (
      select distinct
        product_id,
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
    join (
      select
        resp_log.id_no                                                             as user_hash_no,
        resp_log.name                                                              as name,
        resp_log.apply_no                                                          as due_bill_no,
        concat_ws('~',
          sha256(relational_humans['mbile_phone'],'phone',1),
          sha256(relational_humans['name'],'userName',1),
          resp_log.apply_no
        )                                                                          as linkman_id,
        relational_humans['relational_human_type']                                 as relational_type,
        case relational_humans['relational_human_type']
        when 'RHT01' then '借款人联系人'
        when 'RHT02' then '共同借款人'
        when 'RHT03' then '抵押人'
        when 'RHT04' then '抵押人家庭成员信息'
        when 'RHT05' then '保证人-个人信用保证'
        else relational_humans['relational_human_type']
        end                                                                        as relational_type_cn,
        relational_humans['relationship']                                          as relationship,
        case relational_humans['relationship']
        when 'C' then '配偶'
        when 'F' then '父亲'
        when 'M' then '母亲'
        when 'B' then '兄弟'
        when 'S' then '姐妹'
        when 'L' then '亲属'
        when 'W' then '同事'
        when 'D' then '父母'
        when 'H' then '子女'
        when 'X' then '兄弟姐妹'
        when 'T' then '同学'
        when 'Y' then '朋友'
        when 'O' then '其他'
        else relational_humans['relationship']
        end                                                                        as relationship_cn,
        case relational_humans['id_type']
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
        else relational_humans['id_type']
        end                                                                        as relation_idcard_type,
        sha256(relational_humans['id_no'],'idNumber',1)                            as relation_idcard_no,
        datefmt(substring(relational_humans['id_no'],7,8),'yyyyMMdd','yyyy-MM-dd') as relation_birthday,
        sha256(relational_humans['name'],'userName',1)                             as relation_name,
        sex_idno(relational_humans['id_no'])                                       as relation_sex,
        sha256(relational_humans['mbile_phone'],'phone',1)                         as relation_mobile,
        sha256(relational_humans['address'],'address',1)                           as relation_address,
        relational_humans['province']                                              as relation_province,
        relational_humans['city']                                                  as relation_city,
        relational_humans['area']                                                  as relation_county,
        null                                                                       as corp_type,
        null                                                                       as corp_name,
        null                                                                       as corp_teleph_nbr,
        null                                                                       as corp_fax,
        null                                                                       as corp_position,
        deal_date                                                                  as deal_date,
        create_time                                                                as create_time,
        update_time                                                                as update_time,
        resp_log.product_id                                                        as product_id
      from (
        select
          deal_date,
          sha256(get_json_object(standard_req_msg,'$.borrower.id_no'),'idNumber',1) as id_no,
          sha256(get_json_object(standard_req_msg,'$.borrower.name'),'userName',1)  as name,
          get_json_object(standard_req_msg,'$.apply_no')                            as apply_no,
          datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss')                           as create_time,
          datefmt(update_time,'ms','yyyy-MM-dd HH:mm:ss')                           as update_time,
          get_json_object(standard_req_msg,'$.product.product_no')                  as product_id,
          relational_humans
        from stage.nms_interface_resp_log
        lateral view explode(json_array_to_array(get_json_object(standard_req_msg,'$.relational_humans'))) humans as relational_humans
        where sta_service_method_name = 'setupCustCredit'
          and standard_req_msg is not null
          and deal_date between date_sub('${ST9}',4) and date_add('${ST9}',2)
      ) as resp_log
    ) as linkman
    on linkman.product_id = biz_conf.product_id
    left join (
      select
        linkman_id
      from ods.linkman_info
      where 1 > 0
        and deal_date <= date_add('${ST9}',2)
        -- and false
    ) as history on linkman.linkman_id = history.linkman_id
    where history.linkman_id is null
    union all
    select
      `(stage_product_id)?+.+`
    from ods.linkman_info
    join (
      select distinct
        get_json_object(standard_req_msg,'$.product.product_no') as stage_product_id
      from stage.nms_interface_resp_log
      where sta_service_method_name = 'setupCustCredit'
        and standard_req_msg is not null
    ) as stage
    on linkman_info.product_id = stage.stage_product_id
    where 1 > 0
      and deal_date <= date_add('${ST9}',2)
      -- and false
  ) as tmp
) as tmp
where tmp.rn = 1
-- limit 10
;










-- 瓜子
insert overwrite table ods.linkman_info partition(product_id)
select
  `(rn)?+.+`
from (
  select *,row_number() over (partition by linkman_id order by deal_date desc) as rn
  from (
    select
      concat_ws('_',biz_conf.channel_id,linkman.idNo,linkman.name) as cust_id,
      is_empty(linkman.idNo)                                       as user_hash_no,
      is_empty(linkman.due_bill_no)                                as due_bill_no,
      is_empty(linkman.linkman_id)                                 as linkman_id,
      is_empty(linkman.relational_type)                            as relational_type,
      is_empty(linkman.relational_type_cn)                         as relational_type_cn,
      is_empty(linkman.relationship)                               as relationship,
      is_empty(linkman.relationship_cn)                            as relationship_cn,
      is_empty(linkman.relation_idcard_type)                       as relation_idcard_type,
      is_empty(linkman.relation_idcard_no)                         as relation_idcard_no,
      is_empty(linkman.relation_birthday)                          as relation_birthday,
      is_empty(linkman.relation_name)                              as relation_name,
      is_empty(linkman.relation_sex)                               as relation_sex,
      is_empty(linkman.relation_mobile)                            as relation_mobile,
      is_empty(linkman.relation_address)                           as relation_address,
      is_empty(linkman.relation_province)                          as relation_province,
      is_empty(linkman.relation_city)                              as relation_city,
      is_empty(linkman.relation_county)                            as relation_county,
      is_empty(linkman.corp_type)                                  as corp_type,
      is_empty(linkman.corp_name)                                  as corp_name,
      is_empty(linkman.corp_teleph_nbr)                            as corp_teleph_nbr,
      is_empty(linkman.corp_fax)                                   as corp_fax,
      is_empty(linkman.corp_position)                              as corp_position,
      is_empty(linkman.deal_date)                                  as deal_date,
      is_empty(linkman.create_time)                                as create_time,
      is_empty(linkman.update_time)                                as update_time,
      is_empty(linkman.product_id)                                 as product_id
    from (
      select distinct
        product_id,
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
    join (
      select
        sha256(get_json_object(get_json_object(original_msg,'$.data'),'$.borrower.idNo'),'idNumber',1) as idNo,
        sha256(get_json_object(get_json_object(original_msg,'$.data'),'$.borrower.name'),'userName',1) as name,
        get_json_object(get_json_object(original_msg,'$.content'),'$.applyNo')                         as due_bill_no,
        concat_ws('~',
          sha256(relationalHuman['mobilePhone'],'phone',1),
          sha256(relationalHuman['name'],'userName',1),
          get_json_object(get_json_object(original_msg,'$.content'),'$.applyNo')
        )                                                                                              as linkman_id,
        relationalHuman['relationalHumanType']                                                         as relational_type,
        case  when relationalHuman['relationalHumanType'] = 'RHT01' then '借款人联系人'
              when relationalHuman['relationalHumanType'] = 'RHT02' then '共同借款人'
              when relationalHuman['relationalHumanType'] = 'RHT03' then '抵押人'
              when relationalHuman['relationalHumanType'] = 'RHT04' then '抵押人家庭成员信息'
              when relationalHuman['relationalHumanType'] = 'RHT05' then '保证人-个人信用保证'
              else relationalHuman['relationalHumanType']
        end                                                                                            as relational_type_cn,
        relationalHuman['relationship']                                                                as relationship,
        case  when relationalHuman['relationship'] = 'C' then '配偶'
              when relationalHuman['relationship'] = 'F' then '父亲'
              when relationalHuman['relationship'] = 'M' then '母亲'
              when relationalHuman['relationship'] = 'B' then '兄弟'
              when relationalHuman['relationship'] = 'S' then '姐妹'
              when relationalHuman['relationship'] = 'L' then '亲属'
              when relationalHuman['relationship'] = 'W' then '同事'
              when relationalHuman['relationship'] = 'D' then '父母'
              when relationalHuman['relationship'] = 'H' then '子女'
              when relationalHuman['relationship'] = 'X' then '兄弟姐妹'
              when relationalHuman['relationship'] = 'T' then '同学'
              when relationalHuman['relationship'] = 'Y' then '朋友'
              when relationalHuman['relationship'] = 'O' then '其他'
              else relationalHuman['relationship']
        end                                                                                            as relationship_cn,
        relationalHuman['idType']                                                                      as relation_idcard_type,
        sha256(relationalHuman['idNo'],'idNumber',1)                                                   as relation_idcard_no,
        null                                                                                           as relation_birthday,
        sha256(relationalHuman['name'],'userName',1)                                                   as relation_name,
        relationalHuman['sex']                                                                         as relation_sex,
        sha256(relationalHuman['mobilePhone'],'phone',1)                                               as relation_mobile,
        sha256(relationalHuman['address'],'address',1)                                                 as relation_address,
        relationalHuman['province']                                                                    as relation_province,
        relationalHuman['city']                                                                        as relation_city,
        relationalHuman['area']                                                                        as relation_county,
        null                                                                                           as corp_fax,
        null                                                                                           as corp_type,
        null                                                                                           as corp_name,
        null                                                                                           as corp_teleph_nbr,
        null                                                                                           as corp_position,
        deal_date                                                                                      as deal_date,
        datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss')                                                as create_time,
        datefmt(update_time,'ms','yyyy-MM-dd HH:mm:ss')                                                as update_time,
        get_json_object(get_json_object(original_msg,'$.data'),'$.product.productNo')                  as product_id
      from stage.ecas_msg_log
      lateral view explode(json_array_to_array(get_json_object(original_msg,'$.data.relationalHumans'))) relationalHumanArray as relationalHuman
      where msg_type = 'GZ_CREDIT_APPLY'
      and deal_date between date_sub('${ST9}',4) and date_add('${ST9}',2)
    ) as linkman
    on linkman.product_id = biz_conf.product_id
    left join (
      select
        linkman_id
      from ods.linkman_info
      where 1 > 0
        and deal_date <= date_add('${ST9}',2)
        -- and false
    ) as history on linkman.linkman_id = history.linkman_id
    where history.linkman_id is null
    union all
    select
      `(stage_product_id)?+.+`
    from ods.linkman_info
    join (
      select distinct
        get_json_object(get_json_object(original_msg,'$.data'),'$.product.productNo') as stage_product_id
      from stage.ecas_msg_log
      where msg_type = 'GZ_CREDIT_APPLY'
    ) as stage
    on linkman_info.product_id = stage.stage_product_id
    where 1 > 0
      and deal_date <= date_add('${ST9}',2)
      -- and false
  ) as tmp
) as tmp
where tmp.rn = 1
-- limit 10
;

ods.t_loan_contract_info
ods.t_principal_borrower_info
ods.t_contact_person_info
ods.t_guaranty_info
ods.t_repayment_schedule
ods.t_asset_pay_flow
ods.t_repayment_info
ods.t_repayment_info_fix
ods.t_asset_disposal
ods.t_asset_supplement
ods.t_asset_check
ods.t_project
ods.t_guaranty_car
ods.t_ods_credit
ods.t_credit_loan
--贷款合同信息表
--asset_id 唯一 没有重复一日游依然有 人
--CREATE TABLE ods.t_loan_contract_info (
--id INT COMMENT '贷款合同信息主键',
--import_id STRING COMMENT '数据批次号-由接入系统生产',
--project_id STRING COMMENT '项目id',
--agency_id STRING COMMENT '机构编号',
--asset_id STRING COMMENT '资产借据号',
--contract_code STRING COMMENT '贷款合同编号',
--loan_total_amount DECIMAL ( 16, 2 ) COMMENT '贷款总金额',
--periods INT COMMENT '总期数',
--repay_type STRING COMMENT '还款方式,0-等额本息,1-等额本金,2-等本等息3-先息后本4-一次性还本付息,5-气球贷,6-自定义还款计划',
--interest_rate_type STRING COMMENT '0-固定利率,1-浮动利率',
--loan_interest_rate DECIMAL ( 8, 6 ) COMMENT '贷款年利率，单位:%',
--contract_data_status INT COMMENT '合同数据与还款计划中的总额是否一致，0-一致，1-不一致',
--contract_status STRING COMMENT '0-生效,1-不生效',
--first_repay_date STRING COMMENT '首次还款日期，yyyy-MM-dd',
--extra_info STRING COMMENT '扩展信息',
--create_time STRING COMMENT '创建时间',
--update_time STRING COMMENT '更新时间',
--verify_status STRING COMMENT '数据校验状态',
--verify_mark STRING COMMENT '校验标志',
--first_loan_end_date STRING COMMENT '合同结束时间',
--per_loan_end_date STRING COMMENT '上一次合同结束时间',
--cur_loan_end_date STRING COMMENT '当前合同结束时间',
--loan_begin_date STRING COMMENT '合同开始时间',
--abs_push_flag STRING COMMENT 'abs推送标志',
--repay_frequency STRING COMMENT '还款频率',
--nominal_rate DECIMAL ( 16, 4 ) COMMENT '名义费率',
--daily_penalty_rate DECIMAL ( 16, 4 ) COMMENT '日罚息率',
--loan_use STRING COMMENT '贷款用途',
--guarantee_type STRING COMMENT '担保方式',
--loan_type STRING COMMENT '借款方类型',
--loan_total_interest DECIMAL ( 16, 2 ) COMMENT '贷款总利息',
--loan_total_fee DECIMAL ( 16, 2 ) COMMENT '贷款总费用',
--loan_penalty_rate DECIMAL ( 8, 6 ) COMMENT '贷款罚息利率'
--) COMMENT '贷款合同信息表' WITH SERDEPROPERTIES ( 'serialization.format' = '1' )
--STORED AS PARQUET;
--主借款人信息表
--浦发用户没有重复数据823条
CREATE TABLE ods.t_principal_borrower_info (
id                         INT            COMMENT '主借款人信息主键',
project_id              STRING            COMMENT '项目id',
agency_id               STRING            COMMENT '机构编号',
asset_id                STRING            COMMENT '资产借据号',
customer_name           STRING            COMMENT '客户姓名',
document_num            STRING            COMMENT '身份证号码',
phone_num               STRING            COMMENT '手机号码',
age                        INT            COMMENT '年龄',
sex                     STRING            COMMENT '性别，0-男，2-女',
marital_status          STRING            COMMENT '婚姻状态,0-已婚，1-未婚，2-离异，3-丧偶',
degree                  STRING            COMMENT '学位，0-小学，1-初中，2-高中/职高/技校，3-大专，4-本科,5-硕士,6-博士，7-文盲和半文盲',
province                STRING            COMMENT '客户所在省',
city                    STRING            COMMENT '客户所在市',
address                 STRING            COMMENT '客户所在地区',
extra_info              STRING            COMMENT '扩展信息',
create_time             STRING            COMMENT '创建时间',
update_time             STRING            COMMENT '更新时间',
ecif_no                 STRING            COMMENT 'ecifNo',
card_no                 STRING            COMMENT '身份证号码(脱敏处理)',
phone_no                STRING            COMMENT '手机号码(脱敏处理)',
imei                    STRING            COMMENT 'imei号',
education               STRING            COMMENT '学位',
annual_income           DECIMAL ( 16, 2 ) COMMENT '年收入(元)',
processed               INT               COMMENT '是否已处理'
) COMMENT '主借款人信息表' WITH SERDEPROPERTIES ( 'serialization.format' = '1' ) STORED AS PARQUET ;
研究生
大学本科（简称“大学”）
大学专科和专科学校（简称“大专”）
---
INSERT overwrite TABLE ods_new_s.customer_info PARTITION ( product_id = 'pl00282' )
SELECT DISTINCT
concat( '10041', '_', sha256 ( decrypt_aes ( document_num, 'tencentabs123456' ), 'idNumber', 1 ) ) AS cust_id,
sha256 ( decrypt_aes ( document_num, 'tencentabs123456' ), 'idNumber', 1 ) AS user_hash_no,
NULL AS outer_cust_id,
'身份证' AS idcard_type,
sha256 ( decrypt_aes ( document_num, 'tencentabs123456' ), 'idNumber', 1 ) AS idcard_no,
sha256 ( decrypt_aes ( customer_name, 'tencentabs123456' ), 'userName', 1 ) AS NAME,
sha256 ( decrypt_aes ( phone_num, 'tencentabs123456' ), 'phone', 1 ) AS mobie,
NULL AS card_phone,
sex_idno ( decrypt_aes ( document_num, 'tencentabs123456' ) ) AS sex,
datefmt ( substr( decrypt_aes ( document_num, 'tencentabs123456' ), 7, 8 ), 'yyyyMMdd', 'yyyy-MM-dd' ) AS birthday,
marital_status AS marriage_status,
CASE
    degree
    WHEN '硕士' THEN
    '硕士'
    WHEN '本科' THEN
    '大学本科（简称“大学”）'
    WHEN '大专' THEN
    '大学专科和专科学校（简称“大专”）'
    WHEN '初中' THEN
    '初中'
    END AS education,
CASE
    degree
    WHEN '硕士' THEN
    '硕士及以上'
    WHEN '本科' THEN
    '大学本科' ELSE '大专及以下'
    END AS education_ws,
    concat( idno_province_cn, idno_city_cn, idno_county_cn ) AS idcard_address,
    idno_area_cn AS idcard_area,
    idno_province_cn AS idcard_province,
    idno_city_cn AS idcard_city,
    idno_county_cn AS idcard_county,
    NULL AS idcard_township,
    sha256(address,'address',1) AS resident_address,
    NULL AS resident_area,
    NULL AS resident_province,
    NULL AS resident_city,
    NULL AS resident_county,
    NULL AS resident_township,
    NULL AS job_type,
    NULL AS job_year,
    NULL AS income_month,
    NULL AS income_year,
    NULL AS cutomer_type,
    datefmt ( create_time, 'ms', 'yyyy-MM-dd HH:mm:ss' ) AS create_time,
    datefmt ( update_time, 'ms', 'yyyy-MM-dd HH:mm:ss' ) AS update_time
FROM
    ( SELECT * FROM ods.t_principal_borrower_info WHERE project_id = 'pl00282' ) t1
    LEFT JOIN ( SELECT DISTINCT idno_addr, idno_area_cn, idno_province_cn, idno_city_cn, idno_county_cn FROM dim_new.dim_idno ) t2
    ON substr( decrypt_aes ( document_num, 'tencentabs123456' ), 1, 6 ) = t2.idno_addr;


-- /**
--  * 客户信息表
--  *
--  * 数据库主键 cust_id
--  */
-- DROP TABLE IF EXISTS `ods_new_s.customer_info`;
CREATE TABLE IF NOT EXISTS `ods_new_s.customer_info`(
  `cust_id`                       string        COMMENT '客户编号',
  `user_hash_no`                  string        COMMENT '用户编号',
  `outer_cust_id`                 string        COMMENT '外部客户号',
  `idcard_type`                   string        COMMENT '证件类型（身份证等）',
  `idcard_no`                     string        COMMENT '证件号码',
  `name`                          string        COMMENT '客户姓名',
  `mobie`                         string        COMMENT '客户电话',
  `card_phone`                    string        COMMENT '客户银行卡绑定手机号',
  `sex`                           string        COMMENT '客户性别（男、女）',
  `birthday`                      string        COMMENT '出生日期',
  `marriage_status`               string        COMMENT '婚姻状态',
  `education`                     string        COMMENT '学历',
  `education_ws`                  string        COMMENT '学历级别',
  `idcard_address`                string        COMMENT '身份证地址',
  `idcard_area`                   string        COMMENT '身份证大区（东北地区、华北地区、西北地区、西南地区、华南地区、华东地区、华中地区、港澳台地区）',
  `idcard_province`               string        COMMENT '身份证省级（省/直辖市/特别行政区）',
  `idcard_city`                   string        COMMENT '身份证地级（城市）',
  `idcard_county`                 string        COMMENT '身份证县级（区县）',
  `idcard_township`               string        COMMENT '身份证乡级（乡/镇/街）（预留）',
  `resident_address`              string        COMMENT '常住地地址',
  `resident_area`                 string        COMMENT '常住地大区（东北地区、华北地区、西北地区、西南地区、华南地区、华东地区、华中地区、港澳台地区）',
  `resident_province`             string        COMMENT '常住地省级（省/直辖市/特别行政区）',
  `resident_city`                 string        COMMENT '常住地地级（城市）',
  `resident_county`               string        COMMENT '常住地县级（区县）',
  `resident_township`             string        COMMENT '常住地乡级（乡/镇/街）（预留）',
  `job_type`                      string        COMMENT '工作类型',
  `job_year`                      decimal(2,0)  COMMENT '工作年限',
  `income_month`                  decimal(15,4) COMMENT '月收入',
  `income_year`                   decimal(15,4) COMMENT '年收入',
  `cutomer_type`                  string        COMMENT '客戶类型（个人或企业）',
  `create_time`                   timestamp     COMMENT '创建时间',
  `update_time`                   timestamp     COMMENT '更新时间'
) COMMENT '客户信息表'
PARTITIONED BY (`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


--主借款人信息表
--浦发用户没有重复数据 0条 忽略
--CREATE TABLE ods.t_contact_person_info (
--id INT COMMENT '主借款人信息主键',
--project_id STRING COMMENT '项目id',
--agency_id STRING COMMENT '机构编号',
--asset_id STRING COMMENT '资产借据号',
--customer_name STRING COMMENT '客户姓名',
--document_num STRING COMMENT '身份证号码',
--phone_num STRING COMMENT '手机号码',
--age INT COMMENT '年龄',
--sex STRING COMMENT '性别，0-男，2-女',
--mainborrower_relationship STRING COMMENT '与主借款人的关系，0-配偶,1-父母,2-子女,3-亲戚,4-朋友,5-同事',
--occupation STRING COMMENT '职业',
--work_status STRING COMMENT '工作状态，0-在职，1-失业',
--annual_income DECIMAL ( 16, 2 ) COMMENT '年收入',
--communication_address STRING COMMENT '通讯地址',
--unit_address STRING COMMENT '单位地址',
--unit_phone_number STRING COMMENT '单位联系方式',
--create_time STRING COMMENT '创建时间',
--update_time STRING COMMENT '更新时间',
--ecif_no STRING COMMENT 'ecifNo',
--card_no STRING COMMENT '身份证号码(脱敏处理)',
--phone_no STRING COMMENT '手机号码(脱敏处理)',
--imei STRING COMMENT 'imei号'
--) COMMENT '主借款人信息表' WITH SERDEPROPERTIES ( 'serialization.format' = '1' ) STORED AS PARQUET;

--抵押物信息表
--浦发用户没有重复数据
--CREATE TABLE ods.t_guaranty_info (
--id INT COMMENT '抵押物信息主键',
--project_id STRING COMMENT '项目id',
--agency_id STRING COMMENT '机构编号',
--asset_id STRING COMMENT '资产借据号',
--guaranty_type STRING COMMENT '抵押物类型',
--guaranty_umber STRING COMMENT '抵押物编号',
--mortgage_handle_status STRING COMMENT '抵押办理状态，0-办理中，1-办理完成，2-尚未办理',
--mortgage_alignment STRING COMMENT '抵押顺位0-第一顺位,2-第二顺位,3-其他',
--extra_info STRING COMMENT '扩展信息',
--create_time STRING COMMENT '创建时间',
--update_time STRING COMMENT '更新时间',
--processed INT COMMENT '是否已处理'
--) COMMENT '抵押物信息表' WITH SERDEPROPERTIES ( 'serialization.format' = '1' ) STORED AS PARQUET;

nohup  sh  /home/admin/permission/saas_execute.sh > /dev/null 2>&1 &
/usr/bin/sh /root/t1/t1_iboxchain/t1_iboxchain_manage.sh >> /root/t1/t1_iboxchain/t1_iboxchain_log_$(date +\%F).log
nohup sh  /root/t1/t1_iboxchain/t1_iboxchain_manage.sh > /dev/null 2>&1 &

/usr/bin/sh /root/t1/t1_drip_loan/t1_drip_loan_manage.sh >> /root/t1/t1_drip_loan/t1_drip_loan_log_$(date +\%F).log
nohup sh /root/t1/t1_drip_loan/t1_drip_loan_manage.sh > /dev/null 2>&1 &
select
*
from
(select *
from ods.t_principal_borrower_info
where  project_id = 'pl00282') t1
left join
ods.t_repayment_schedule t2
on t1.asset_id = t2.asset_id

--还款计划信息表
--没有分区
CREATE TABLE ods.t_repayment_schedule (
id INT COMMENT '实际还款信息表主键',
import_id STRING COMMENT '数据批次号-由接入系统生产',
project_id STRING COMMENT '项目id',
agency_id STRING COMMENT '机构编号',
asset_id STRING COMMENT '资产借据号',
period INT COMMENT '期次',
repay_date STRING COMMENT '应还款日期',
repay_principal DECIMAL ( 16, 2 ) COMMENT '应还本金(元)',
repay_interest DECIMAL ( 16, 2 ) COMMENT '应还利息(元)',
repay_fee DECIMAL ( 16, 2 ) COMMENT '应还费用(元)',
begin_loan_principal DECIMAL ( 16, 2 ) COMMENT '期初剩余本金',
end_loan_principal DECIMAL ( 16, 2 ) COMMENT '期末剩余本金',
execute_date STRING COMMENT '生效日期',
TIMESTAMP STRING COMMENT '时间戳信息，区分还款计划的批次问题',
extra_info STRING COMMENT '扩展信息',
create_time STRING COMMENT '创建时间',
update_time STRING COMMENT '更新时间',
begin_loan_interest DECIMAL ( 16, 2 ) COMMENT '期初剩余利息',
end_loan_interest DECIMAL ( 16, 2 ) COMMENT '期末剩余利息',
begin_loan_fee DECIMAL ( 16, 2 ) COMMENT '期初剩余费用',
end_loan_fee DECIMAL ( 16, 2 ) COMMENT '期末剩余费用',
remainder_periods DECIMAL ( 16, 2 ) COMMENT '剩余期数',
next_repay_date STRING COMMENT '下次应还日期',
repay_penalty DECIMAL ( 16, 2 ) COMMENT '应还罚息'
) COMMENT '还款计划信息表' WITH SERDEPROPERTIES ( 'serialization.format' = '1' ) STORED AS PARQUET;
insert overwrite table ods.repay_schedule_zip
select
t2.cust_id
,t2.user_hash_no
,t1.id as schedule_id
,null as out_side_schedule_no
,t1.asset_id as due_bill_no
,t1.execute_date as loan_active_date
,t1.loan_init_principal
,(t1.period + t1.remainder_periods) as loan_init_term

from
(select *,
first_value(begin_loan_principal) over(partition by asset_id order by period) as loan_init_principal
from ods.t_repayment_schedule
where project_id = 'pl00282') t1
left join
(select distinct
concat( '10041', '_', sha256 ( decrypt_aes ( document_num, 'tencentabs123456' ), 'idNumber', 1 ) ) AS cust_id,
sha256 ( decrypt_aes ( document_num, 'tencentabs123456' ), 'idNumber', 1 ) AS user_hash_no,
asset_id
from ods.t_principal_borrower_info
where project_id = 'pl00282') t2
on t1.asset_id = t2.asset_id
left join

create table ods_new_s.repay_schedule_pf like  ods_new_s.repay_schedule;
set hive.support.quoted.identifiers=None;
insert overwrite table ods_new_s.repay_schedule_pf partition(is_settled = 'no',product_id = 'pl00282')
select `(is_settled|product_id)?+.+` from ods_new_s.repay_schedule where product_id = 'pl00282';




insert overwrite table ods_new_s.repay_schedule_pf_2020_11_03 partition(is_settled = 'no',product_id = 'pl00282')
select
t1.id as schedule_id
,t1.asset_id as due_bill_no
,t1.execute_date as loan_active_date
,t1.loan_init_principal
,(t1.period + t1.remainder_periods) as loan_init_term
,t1.period as loan_term
,date_sub(add_months(t1.repay_date,-1),1) as start_interest_date
,null as curr_bal
,t1.repay_date as should_repay_date
,null as should_repay_date_history
,null as grace_date
,(nvl(t1.repay_principal,0) + nvl(t1.repay_interest,0) + nvl(t1.repay_fee,0)) as should_repay_amount
,t1.repay_principal as should_repay_principal
,t1.repay_interest as should_repay_interest
,0 as should_repay_term_fee
,t1.repay_fee as should_repay_svc_fee
,0 as should_repay_penalty
,0 as should_repay_mult_amt
,0 as should_repay_penalty_acru
,case t3.account_status
 when '正常' then 'N'
 WHEN '逾期' then 'O'
 WHEN '提前还清' then 'F'
 END AS schedule_status
,t3.account_status as  schedule_status_cn
,t3.rel_pay_date as paid_out_date
,null as paid_out_type
,null as paid_out_type_cn
,(nvl(t3.rel_principal,0) + nvl(t3.rel_interest,0) + nvl(t3.rel_fee,0)) as paid_amount
,t3.rel_principal as paid_principal
,t3.rel_interest as paid_interest
,null as paid_term_fee
,t3.rel_fee as paid_svc_fee
,null as paid_penalty
,null as paid_mult
,null as reduce_amount
,null as reduce_principal
,null as reduce_interest
,null as reduce_term_fee
,null as reduce_svc_fee
,null as reduce_penalty
,null as reduce_mult_amt
,'2019-01-01' as s_d_date
,'2020-09-11' as e_d_date
,'2020-09-11' as effective_time
,'2020-09-11' as expire_time
from
(select *,
first_value(begin_loan_principal) over(partition by asset_id order by period) as loan_init_principal
from ods.t_repayment_schedule
where project_id = 'pl00282') t1
left join
(select distinct
concat( '10041', '_', sha256 ( decrypt_aes ( document_num, 'tencentabs123456' ), 'idNumber', 1 ) ) AS cust_id,
sha256 ( decrypt_aes ( document_num, 'tencentabs123456' ), 'idNumber', 1 ) AS user_hash_no,
asset_id
from ods.t_principal_borrower_info
where project_id = 'pl00282') t2
on t1.asset_id = t2.asset_id
left join
(select
asset_id
,period
,max(id) as id
,max(account_status) as account_status
,max(overdue_day) as overdue_day
,sum(rel_principal) as rel_principal
,sum(rel_interest) as rel_interest
,sum(rel_fee) as rel_fee
,max(rel_pay_date) as rel_pay_date
,max(`timestamp`) as `timestamp`
,max(create_time) as create_time
,max(update_time) as update_time
from
ods.t_repayment_info_temp where project_id = 'pl00282'
group by
asset_id,
period) t3
on t1.asset_id = t3.asset_id
and t1.period = t3.period;


--select
--cust_id,
--count(cust_id) as total
--from
--(select distinct
--concat( '10041', '_', sha256 ( decrypt_aes ( document_num, 'tencentabs123456' ), 'idNumber', 1 ) ) AS cust_id,
--sha256 ( decrypt_aes ( document_num, 'tencentabs123456' ), 'idNumber', 1 ) AS user_hash_no,
--asset_id
--from ods.t_principal_borrower_info
--where project_id = 'pl00282') t group by cust_id having count(cust_id) > 1
-- /**
--  * 还款计划表（做拉链表）
--  *
--  * 数据库主键 schedule_id
--  * 业务主键 cust_id,due_bill_no
--  *
--  * 按照 is_settled 分区
--  */
-- DROP TABLE IF EXISTS `ods_new_s.repay_schedule`;
CREATE TABLE IF NOT EXISTS `ods_new_s.repay_schedule`(
  `schedule_id`                  string        COMMENT '还款计划编号',
  `due_bill_no`                  string        COMMENT '借据编号',
  `loan_active_date`             string        COMMENT '放款日期',
  `loan_init_principal`          decimal(15,4) COMMENT '贷款本金',
  `loan_init_term`               decimal(3,0)  COMMENT '贷款期数（3、6、9等）',
  `loan_term`                    decimal(3,0)  COMMENT '当前期数',
  `start_interest_date`          string        COMMENT '起息日期',
  `curr_bal`                     decimal(15,4) COMMENT '当前余额（当前欠款）',
  `should_repay_date`            string        COMMENT '应还日期',         -- 对应 pmt_due_date 字段
  `should_repay_date_history`    string        COMMENT '修改前的应还日期', -- 对应 pmt_due_date 的上一次日期 字段
  `grace_date`                   string        COMMENT '宽限日期',
  `should_repay_amount`          decimal(15,4) COMMENT '应还金额',
  `should_repay_principal`       decimal(15,4) COMMENT '应还本金',
  `should_repay_interest`        decimal(15,4) COMMENT '应还利息',
  `should_repay_term_fee`        decimal(15,4) COMMENT '应还手续费',
  `should_repay_svc_fee`         decimal(15,4) COMMENT '应还服务费',
  `should_repay_penalty`         decimal(15,4) COMMENT '应还罚息',
  `should_repay_mult_amt`        decimal(15,4) COMMENT '应还滞纳金',
  `should_repay_penalty_acru`    decimal(15,4) COMMENT '应还累计罚息金额',
  `schedule_status`              string        COMMENT '还款计划状态（英文原值）（N：正常，O：逾期，F：已还清）',
  `schedule_status_cn`           string        COMMENT '还款计划状态（汉语解释）',
  `paid_out_date`                string        COMMENT '还清日期',
  `paid_out_type`                string        COMMENT '结清类型（英文原值）（BANK_REF：退票结清，BUY_BACK：资产回购，CAPITAL_VERI：资产核销，DISPOSAL：处置结束，NORMAL_SETTLE：正常结清，OVER_COMP：逾期代偿，OVERDUE_SETTLE：逾期结清，PRE_SETTLE：提前结清，REDEMPTION：赎回，REFUND：退车，REFUND_SETTLEMENT：退票结清）',
  `paid_out_type_cn`             string        COMMENT '结清类型（汉语解释）',
  `paid_amount`                  decimal(15,4) COMMENT '已还金额',
  `paid_principal`               decimal(15,4) COMMENT '已还本金',
  `paid_interest`                decimal(15,4) COMMENT '已还利息',
  `paid_term_fee`                decimal(15,4) COMMENT '已还手续费',
  `paid_svc_fee`                 decimal(15,4) COMMENT '已还服务费',
  `paid_penalty`                 decimal(15,4) COMMENT '已还罚息',
  `paid_mult`                    decimal(15,4) COMMENT '已还滞纳金',
  `reduce_amount`                decimal(15,4) COMMENT '减免金额',
  `reduce_principal`             decimal(15,4) COMMENT '减免本金',
  `reduce_interest`              decimal(15,4) COMMENT '减免利息',
  `reduce_term_fee`              decimal(15,4) COMMENT '减免手续费',
  `reduce_svc_fee`               decimal(15,4) COMMENT '减免服务费',
  `reduce_penalty`               decimal(15,4) COMMENT '减免罚息',
  `reduce_mult_amt`              decimal(15,4) COMMENT '减免滞纳金',
  `s_d_date`                     string        COMMENT 'ods层起始日期',
  `e_d_date`                     string        COMMENT 'ods层结束日期',
  `effective_time`               timestamp     COMMENT '生效时间',
  `expire_time`                  timestamp     COMMENT '失效时间'
) COMMENT '还款计划表'
PARTITIONED BY (`is_settled` string COMMENT '是否已结清',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;






--资产支付流水信息表
--支付明细
CREATE TABLE ods.t_asset_pay_flow (
id INT COMMENT '抵押物信息主键',
project_id STRING COMMENT '项目id',
agency_id STRING COMMENT '机构编号',
asset_id STRING COMMENT '资产借据号',
trade_channel STRING COMMENT '交易渠道',
trade_type STRING COMMENT '0-放款,1-代扣,2-主动还款,3-代偿,4-回购,5-差额补足,6-处置回收',
order_id STRING COMMENT '订单id',
order_amount DECIMAL ( 16, 2 ) COMMENT '订单金额',
trade_currency STRING COMMENT '交易币种，默认人民币',
NAME STRING COMMENT '姓名-银行户名',
bank_account STRING COMMENT '银行账号',
trade_time STRING COMMENT '交易时间',
trade_status STRING COMMENT '交易状态，0-成功，1-失败',
extra_info STRING COMMENT '扩展字段',
trad_desc STRING COMMENT '交易摘要',
create_time STRING COMMENT '创建时间',
update_time STRING COMMENT '更新时间'
) COMMENT '资产支付流水信息表' WITH SERDEPROPERTIES ( 'serialization.format' = '1' ) STORED AS PARQUET;

--实际还款信息表
--
select asset_id,repay_date,row number() over(partition by asset_id order by repay_date) as rank_overday
from
(select * from ods.t_repayment_info
where project_id = 'pl00282'
and
overdue_day > 0
) t1)

select
asset_id
,min(repay_date) overdue_date_first
from
 ods.t_repayment_info
where project_id = 'pl00282'
and
overdue_day > 0
group by
asset_id;

CREATE TABLE ods.t_repayment_info (
id INT COMMENT '实际还款信息表主键',
import_id STRING COMMENT '数据批次号-由接入系统生产',
project_id STRING COMMENT '项目id',
agency_id STRING COMMENT '机构编号',
asset_id STRING COMMENT '资产借据号',
repay_date STRING COMMENT '应还款日期',
repay_principal DECIMAL ( 16, 2 ),
repay_interest DECIMAL ( 16, 2 ),
repay_fee DECIMAL ( 16, 2 ),
rel_pay_date STRING COMMENT '实际还清日期',
rel_principal DECIMAL ( 16, 2 ) COMMENT '实还本金',
rel_interest  DECIMAL ( 16, 2 ) COMMENT '实还利息',
rel_fee       DECIMAL ( 16, 2 ) COMMENT '实还费用',
period INT COMMENT '期次',
TIMESTAMP STRING,
extra_info STRING COMMENT '扩展信息',
create_time STRING COMMENT '创建时间',
update_time STRING COMMENT '更新时间',
free_amount DECIMAL ( 16, 2 ) COMMENT '免息金额',
remainder_principal DECIMAL ( 16, 2 ) COMMENT '剩余本金',
remainder_interest DECIMAL ( 16, 2 ) COMMENT '剩余利息',
remainder_fee DECIMAL ( 16, 2 ) COMMENT '剩余费用',
remainder_periods INT COMMENT '剩余期数',
repay_type STRING COMMENT '还款类型',
current_loan_balance DECIMAL ( 16, 2 ) COMMENT '当期贷款余额',
account_status STRING COMMENT '当期账户状态',
current_status STRING COMMENT '当期状态',
overdue_day INT COMMENT '逾期天数',
finish_periods INT COMMENT '已还期数',
plan_begin_loan_principal DECIMAL ( 16, 2 ) COMMENT '期初剩余本金',
plan_end_loan_principal DECIMAL ( 16, 2 ) COMMENT '期末剩余本金',
plan_begin_loan_interest DECIMAL ( 16, 2 ) COMMENT '期初剩余利息',
plan_end_loan_interest DECIMAL ( 16, 2 ) COMMENT '期末剩余利息',
plan_begin_loan_fee DECIMAL ( 16, 2 ) COMMENT '期初剩余费用',
plan_end_loan_fee DECIMAL ( 16, 2 ) COMMENT '期末剩余费用',
plan_remainder_periods INT COMMENT '剩余期数',
plan_next_repay_date STRING COMMENT '下次应还日期',
real_interest_rate DECIMAL ( 16, 2 ) COMMENT '实际执行利率',
repay_penalty DECIMAL ( 16, 2 ) COMMENT '应还罚息',
rel_penalty DECIMAL ( 16, 2 ) COMMENT '实还罚息'
) COMMENT '实际还款信息表' WITH SERDEPROPERTIES ( 'serialization.format' = '1' ) STORED AS PARQUET;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=3000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.shuffle.memoryFraction=0.5;
set spark.maxRemoteBlockSizeFetchToMem=200m;
insert overwrite table ods_new_s.repay_detail partition(biz_date, product_id)
select
distinct
t1.asset_id as due_bill_no
,t4.loan_begin_date as loan_active_date
,t3.loan_init_term
,t1.period as repay_term
,t1.asset_id as order_id
,case t1.account_status
 when '正常' then 'N'
 WHEN '逾期' then 'O'
 WHEN '提前还清' then 'F'
 END AS loan_status
,t1.account_status as  loan_status_cn
,t1.overdue_day as overdue_days
,t1.asset_id as payment_id
,t1.`timestamp` as txn_time
,t1.`timestamp` as post_time
,t1.bnp_type
,t1.bnp_type_cn
,t1.repay_amount
,t1.create_time as batch_date
,t1.create_time as create_time
,t1.update_time as update_time
,t1.rel_pay_date as biz_date
,'pl00282' as product_id
from
(select
 asset_id
 ,period
 ,id
 ,account_status
 ,overdue_day
 ,rel_principal as repay_amount
 ,'Pricinpal' as bnp_type
 ,'本金' as bnp_type_cn
 ,rel_pay_date
 ,`timestamp`
 ,create_time
 ,update_time
from
(select
asset_id
,period
,max(id) as id
,max(account_status) as account_status
,max(overdue_day) as overdue_day
,sum(rel_principal) as rel_principal
,sum(rel_interest) as rel_interest
,sum(rel_fee) as rel_fee
,max(rel_pay_date) as rel_pay_date
,max(`timestamp`) as `timestamp`
,max(create_time) as create_time
,max(update_time) as update_time
from
ods.t_repayment_info_temp where project_id = 'pl00282'
group by
asset_id,
period) rr
union all
select
 asset_id
 ,period
 ,id
 ,account_status
 ,overdue_day
 ,rel_interest as repay_amount
 ,'Interest' as bnp_type
 ,'利息' as bnp_type_cn
 ,rel_pay_date
 ,`timestamp`
 ,create_time
 ,update_time
from
(select
asset_id
,period
,max(id) as id
,max(account_status) as account_status
,max(overdue_day) as overdue_day
,sum(rel_principal) as rel_principal
,sum(rel_interest) as rel_interest
,sum(rel_fee) as rel_fee
,max(rel_pay_date) as rel_pay_date
,max(`timestamp`) as `timestamp`
,max(create_time) as create_time
,max(update_time) as update_time
from
ods.t_repayment_info_temp where project_id = 'pl00282'
group by
asset_id,
period) xx
union all
select
 asset_id
 ,period
 ,id
 ,account_status
 ,overdue_day
 ,rel_fee as repay_amount
 ,'Fee' as bnp_type
 ,'费用' as bnp_type_cn
 ,rel_pay_date
 ,`timestamp`
 ,create_time
 ,update_time
from (select
asset_id
,period
,max(id) as id
,max(account_status) as account_status
,max(overdue_day) as overdue_day
,sum(rel_principal) as rel_principal
,sum(rel_interest) as rel_interest
,sum(rel_fee) as rel_fee
,max(rel_pay_date) as rel_pay_date
,max(`timestamp`) as `timestamp`
,max(create_time) as create_time
,max(update_time) as update_time
from
ods.t_repayment_info_temp where project_id = 'pl00282'
group by
asset_id,
period) zz
) t1
left join
(select due_bill_no, max(loan_init_term) as loan_init_term from ods_new_s.repay_schedule where product_id = 'pl00282' group by due_bill_no) t3
on t1.asset_id = t3.due_bill_no
left join
(SELECT
 asset_id
,loan_begin_date
 from
 ods.t_loan_contract_info
 where project_id = 'pl00282'
 ) t4
on t1.asset_id = t4.asset_id
;

(select
asset_id
,period
,max(id) as id
,max(account_status) as account_status
,max(overdue_day) as overdue_day
,sum(rel_principal) as rel_principal
,sum(rel_interest) as rel_interest
,sum(rel_fee) as rel_fee
,max(rel_pay_date) as rel_pay_date
,max(`timestamp`) as `timestamp`
,max(create_time) as create_time
,max(update_time) as update_time
from
ods.t_repayment_info_temp
group by
asset_id,
period) rr

CREATE TABLE IF NOT EXISTS `ods_new_s.repay_detail`(
  `due_bill_no`                  string        COMMENT '借据号',
  `loan_active_date`             string        COMMENT '放款日期',
  `loan_init_term`               decimal(3,0)  COMMENT '贷款期数（3、6、9等）',
  `repay_term`                   decimal(3,0)  COMMENT '实还期数',
  `order_id`                     string        COMMENT '订单号',
  `loan_status`                  string        COMMENT '借据状态（英文原值）（N：正常，O：逾期，F：已还清）',
  `loan_status_cn`               string        COMMENT '借据状态（汉语解释）',
  `overdue_days`                 decimal(4,0)  COMMENT '逾期天数',
  `payment_id`                   string        COMMENT '实还流水号',
  `txn_time`                     timestamp     COMMENT '交易时间',
  `post_time`                    timestamp     COMMENT '入账时间',
  `bnp_type`                     string        COMMENT '还款成分（英文原值）（Pricinpal：本金，Interest：利息，Penalty：罚息，Mulct：罚金，Compound：复利，CardFee：年费，OverLimitFee：超限费，LatePaymentCharge：滞纳金，NSFCharge：资金不足罚金，TXNFee：交易费，TERMFee：手续费，SVCFee：服务费，LifeInsuFee：寿险计划包费）',
  `bnp_type_cn`                  string        COMMENT '还款成分（汉语解释）',
  `repay_amount`                 decimal(15,4) COMMENT '还款金额',
  `batch_date`                   string        COMMENT '批量日期',
  `create_time`                  timestamp     COMMENT '创建时间',
  `update_time`                  timestamp     COMMENT '更新时间'
) COMMENT '实还明细表'
PARTITIONED BY (`biz_date` string COMMENT '实还日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


--CREATE TABLE ods.t_repayment_info_fix (
--id INT COMMENT '实际还款信息表主键',
--import_id STRING COMMENT '数据批次号-由接入系统生产',
--project_id STRING COMMENT '项目id',
--agency_id STRING COMMENT '机构编号',
--asset_id STRING COMMENT '资产借据号',
--repay_date STRING COMMENT '应还款日期',
--repay_principal DECIMAL ( 16, 2 ),
--repay_interest DECIMAL ( 16, 2 ),
--repay_fee DECIMAL ( 16, 2 ),
--rel_pay_date STRING COMMENT '实际还清日期',
--rel_principal DECIMAL ( 16, 2 ) COMMENT '实还本金',
--rel_interest DECIMAL ( 16, 2 ) COMMENT '实还利息',
--rel_fee DECIMAL ( 16, 2 ) COMMENT '实还费用',
--period INT COMMENT '期次',
--TIMESTAMP STRING,
--extra_info STRING COMMENT '扩展信息',
--create_time STRING COMMENT '创建时间',
--update_time STRING COMMENT '更新时间',
--free_amount DECIMAL ( 16, 2 ) COMMENT '免息金额',
--remainder_principal DECIMAL ( 16, 2 ) COMMENT '剩余本金',
--remainder_interest DECIMAL ( 16, 2 ) COMMENT '剩余利息',
--remainder_fee DECIMAL ( 16, 2 ) COMMENT '剩余费用',
--remainder_periods INT COMMENT '剩余期数',
--repay_type STRING COMMENT '还款类型',
--current_loan_balance DECIMAL ( 16, 2 ) COMMENT '当期贷款余额',
--account_status STRING COMMENT '当期账户状态',
--current_status STRING COMMENT '当期状态',
--overdue_day INT COMMENT '逾期天数',
--finish_periods INT COMMENT '已还期数',
--plan_begin_loan_principal DECIMAL ( 16, 2 ) COMMENT '期初剩余本金',
--plan_end_loan_principal DECIMAL ( 16, 2 ) COMMENT '期末剩余本金',
--plan_begin_loan_interest DECIMAL ( 16, 2 ) COMMENT '期初剩余利息',
--plan_end_loan_interest DECIMAL ( 16, 2 ) COMMENT '期末剩余利息',
--plan_begin_loan_fee DECIMAL ( 16, 2 ) COMMENT '期初剩余费用',
--plan_end_loan_fee DECIMAL ( 16, 2 ) COMMENT '期末剩余费用',
--plan_remainder_periods INT COMMENT '剩余期数',
--plan_next_repay_date STRING COMMENT '下次应还日期',
--real_interest_rate DECIMAL ( 16, 2 ) COMMENT '实际执行利率',
--repay_penalty DECIMAL ( 16, 2 ) COMMENT '应还罚息',
--rel_penalty DECIMAL ( 16, 2 ) COMMENT '实还罚息'
--) COMMENT '实际还款信息表' WITH SERDEPROPERTIES ( 'serialization.format' = '1' ) STORED AS PARQUET;


--资产处置过程信息表
--浦发项目资产借据号唯一
--CREATE TABLE ods.t_asset_disposal (
--id INT COMMENT '抵押物信息主键',
--project_id STRING COMMENT '项目id',
--agency_id STRING COMMENT '机构编号',
--asset_id STRING COMMENT '资产借据号',
--disposi_status STRING COMMENT '处置状态,0-已处置，1-未处置',
--disposi_type STRING COMMENT '处置类型，0-诉讼，1-非诉讼',
--litigate_node STRING COMMENT '诉讼节点，0-处置开始，1-诉讼准备，2-法院受理，3-执行拍卖，4-处置结束',
--litigate_node_time STRING COMMENT '诉讼节点时间',
--disposi_esult STRING COMMENT '处置结果,0-经处置无拖欠,1-经处置已结清,2-经处置已核销',
--create_time STRING COMMENT '创建时间',
--update_time STRING COMMENT '更新时间'
--) COMMENT '资产处置过程信息表' WITH SERDEPROPERTIES ( 'serialization.format' = '1' ) STORED AS PARQUET;

--资产补充交易信息表
--浦发项目资产借据号唯一
--CREATE TABLE ods.t_asset_supplement (
--id INT COMMENT '抵押物信息主键',
--project_id STRING COMMENT '项目id',
--agency_id STRING COMMENT '机构编号',
--asset_id STRING COMMENT '资产借据号',
--tradetype STRING COMMENT '交易类型,0-回购,1-处置回收,2-代偿,3-差额补足',
--tradereason STRING COMMENT '交易原因',
--tradedate STRING COMMENT '交易日期',
--trade_tol_amounts DECIMAL ( 16, 2 ) COMMENT '交易总金额',
--principal DECIMAL ( 16, 2 ) COMMENT '本金',
--interest DECIMAL ( 16, 2 ) COMMENT '利息',
--punish_interest DECIMAL ( 16, 2 ) COMMENT '罚息',
--oth_fee DECIMAL ( 16, 2 ) COMMENT '其他费用',
--create_time STRING COMMENT '创建时间',
--update_time STRING COMMENT '更新时间'
--) COMMENT '资产补充交易信息表' WITH SERDEPROPERTIES ( 'serialization.format' = '1' ) STORED AS PARQUET;

--资产对账信息 project_id = 'pl00282'  823
CREATE TABLE ods.t_asset_check (
id INT COMMENT '资产对账信息主键',
project_id STRING COMMENT '项目id',
agency_id STRING COMMENT '机构编号',
asset_id STRING COMMENT '资产借据号',
repayedperiod INT COMMENT '已还期数',
remain_period INT COMMENT '剩余期数',
remain_principal DECIMAL ( 16, 2 ) COMMENT '剩余本金(元)',
remain_interest  DECIMAL ( 16, 2 ) COMMENT '剩余利息(元)',
remain_othamounts DECIMAL ( 16, 2 ) COMMENT '剩余其他费用(元)',
next_pay_date STRING COMMENT '下一个还款日期',
assets_status STRING COMMENT '资产状态，0-正常，1-逾期，2-已结清',
settle_reason STRING COMMENT '结清原因,0-正常结清,1-提前结清,2-处置结束,3-资产回购',
current_overdue_principal DECIMAL ( 16, 2 ) COMMENT '当前逾期本金',
current_overdue_interest DECIMAL ( 16, 2 ) COMMENT '当前逾期利息',
current_overdue_fee DECIMAL ( 16, 2 ) COMMENT '当前逾期费用',
current_overdue_days INT COMMENT '当前逾期天数(天)',
accum_overdue_days INT COMMENT '累计逾期天数',
his_accum_overdue_days INT COMMENT '历史累计逾期天数',
his_overdue_mdays INT COMMENT '历史单次最长逾期天数',
current_overdue_period INT COMMENT '当前逾期期数',
accum_overdue_period INT COMMENT '累计逾期期数',
his_overdue_mperiods INT COMMENT '历史单次最长逾期期数',
his_overdue_mprincipal DECIMAL ( 16, 2 ) COMMENT '历史最大逾期本金',
extra_info STRING COMMENT '扩展信息',
remark STRING COMMENT '备注信息',
create_time STRING COMMENT '创建时间',
update_time STRING COMMENT '更新时间',
loan_total_amount DECIMAL ( 16, 2 ) COMMENT '贷款总金额',
loan_interest_rate DECIMAL ( 16, 2 ) COMMENT '贷款年利率(%)',
periods INT COMMENT '总期数',
recovered_total_amount DECIMAL ( 16, 2 ) COMMENT '实还当天回收款总金额',
rel_principal DECIMAL ( 16, 2 ) COMMENT '实还本金',
rel_interest DECIMAL ( 16, 2 ) COMMENT '实还利息',
rel_fee DECIMAL ( 16, 2 ) COMMENT '实还费用',
current_continuity_overdays INT COMMENT '当前连续逾期天数',
max_single_overduedays INT COMMENT '最长单期逾期天数',
max_continuity_overdays INT COMMENT '最长连续逾期天数',
accum_overdue_principal DECIMAL ( 16, 2 ) COMMENT '历史累计逾期本金',
remain_penalty DECIMAL ( 16, 2 ) COMMENT '剩余罚息',
loan_settlement_date STRING COMMENT '结清日期',
loss_principal DECIMAL ( 16, 2 ) COMMENT '损失本金',
total_rel_amount DECIMAL ( 16, 2 ) COMMENT '累计实还金额',
total_rel_principal DECIMAL ( 16, 2 ) COMMENT '累计实还本金',
total_rel_interest DECIMAL ( 16, 2 ) COMMENT '累计实还利息',
total_rel_fee DECIMAL ( 16, 2 ) COMMENT '累计实还费用',
total_rel_penalty DECIMAL ( 16, 2 ) COMMENT '累计实还罚息',
rel_penalty DECIMAL ( 16, 2 ) COMMENT '当天实还罚息',
curr_period INT COMMENT '当前期次',
first_term_overdue STRING COMMENT '首期是否逾期'
) PARTITIONED BY ( account_date STRING COMMENT '记账日期' ) COMMENT '资产对账信息表' WITH SERDEPROPERTIES ( 'serialization.format' = '1' ) STORED AS PARQUET;

-- /**
--  * 借据信息表（做拉链表）
--  *
--  * 数据库主键 loan_id
--  * 业务主键 due_bill_no
--  *
--  * 按照 is_settled 分区
--  */
--贷款合同信息表
--asset_id 唯一 没有重复一日游依然有 人
--CREATE TABLE ods.t_loan_contract_info (
--id INT COMMENT '贷款合同信息主键',
--import_id STRING COMMENT '数据批次号-由接入系统生产',
--project_id STRING COMMENT '项目id',
--agency_id STRING COMMENT '机构编号',
--asset_id STRING COMMENT '资产借据号',
--contract_code STRING COMMENT '贷款合同编号',
--loan_total_amount DECIMAL ( 16, 2 ) COMMENT '贷款总金额',
--periods INT COMMENT '总期数',
--repay_type STRING COMMENT '还款方式,0-等额本息,1-等额本金,2-等本等息3-先息后本4-一次性还本付息,5-气球贷,6-自定义还款计划',
--interest_rate_type STRING COMMENT '0-固定利率,1-浮动利率',
--loan_interest_rate DECIMAL ( 8, 6 ) COMMENT '贷款年利率，单位:%',
--contract_data_status INT COMMENT '合同数据与还款计划中的总额是否一致，0-一致，1-不一致',
--contract_status STRING COMMENT '0-生效,1-不生效',
--first_repay_date STRING COMMENT '首次还款日期，yyyy-MM-dd',
--extra_info STRING COMMENT '扩展信息',
--create_time STRING COMMENT '创建时间',
--update_time STRING COMMENT '更新时间',
--verify_status STRING COMMENT '数据校验状态',
--verify_mark STRING COMMENT '校验标志',
--first_loan_end_date STRING COMMENT '合同结束时间',
--per_loan_end_date STRING COMMENT '上一次合同结束时间',
--cur_loan_end_date STRING COMMENT '当前合同结束时间',
--loan_begin_date STRING COMMENT '合同开始时间',
--abs_push_flag STRING COMMENT 'abs推送标志',
--repay_frequency STRING COMMENT '还款频率',
--nominal_rate DECIMAL ( 16, 4 ) COMMENT '名义费率',
--daily_penalty_rate DECIMAL ( 16, 4 ) COMMENT '日罚息率',
--loan_use STRING COMMENT '贷款用途',
--guarantee_type STRING COMMENT '担保方式',
--loan_type STRING COMMENT '借款方类型',
--loan_total_interest DECIMAL ( 16, 2 ) COMMENT '贷款总利息',
--loan_total_fee DECIMAL ( 16, 2 ) COMMENT '贷款总费用',
--loan_penalty_rate DECIMAL ( 8, 6 ) COMMENT '贷款罚息利率'
--) COMMENT '贷款合同信息表' WITH SERDEPROPERTIES ( 'serialization.format' = '1' )
--STORED AS PARQUET;

--alter table ods_new_s.loan_info drop partition (is_settled = 'no',product_id = 'pl00282');
--select * from ods_new_s.loan_info where product_id = 'pl00282';
insert overwrite table ods_new_s.loan_info partition(is_settled = 'no',product_id = 'pl00282')
select
t2.user_hash_no
,t2.cust_id
,t2.age
,t1.id as loan_id
,t1.asset_id as due_bill_no
,t3.contract_code as contract_no
,null as apply_no
,t3.loan_use as loan_usage
,null as register_date
,null as request_time
,to_date(t3.loan_begin_date) as loan_active_date
,null as cycle_day
,t4.repay_date as loan_expire_date
,'0' as loan_type
--0-等额本息,1-等额本金,2-等本等息3-先息后本4-一次性还本付息,5-气球贷,6-自定义还款计划
,'等额本息' as loan_type_cn
,t3.periods as loan_init_term
,t1.curr_period as loan_term1
,t1.curr_period as loan_term2
,t1.next_pay_date as should_repay_date
,t1.repayedperiod as loan_term_repaid
,t1.remain_period as loan_term_remain
,case t1.assets_status
 when '正常' then 'N'
 WHEN '逾期' then 'O'
 WHEN '提前结清' then 'F'
 END AS loan_status
,t1.assets_status as  loan_status_cn
,null as loan_out_reason
-- '结清原因,0-正常结清,1-提前结清,2-处置结束,3-资产回购'
,case t1.settle_reason
when '正常结清' then 0
when '逾期结清' then 1
when '提前结清' then 2
end as paid_out_type
,t1.settle_reason as paid_out_type_cn
,t5.rel_pay_date as paid_out_date
,null as terminal_date
,t1.loan_total_amount as loan_init_principal
,t1.loan_interest_rate as loan_init_interest_rate
,t3.loan_total_interest as loan_init_interest
,null as loan_init_term_fee_rate
,null as loan_init_term_fee
,null as loan_init_svc_fee_rate
,null as loan_init_svc_fee
,t3.loan_penalty_rate as loan_init_penalty_rate
,t1.total_rel_amount as paid_amount
,t1.total_rel_principal as paid_principal
,t1.total_rel_interest as paid_interest
,t1.total_rel_penalty as paid_penalty
,null as paid_svc_fee
,null as paid_term_fee
,null as paid_mult
,(nvl(t1.remain_principal,0) + nvl(t1.remain_interest,0) + nvl(t1.remain_othamounts,0)) as remain_amount
,t1.remain_principal as remain_principal
,t1.remain_interest as remain_interest
,null as remain_svc_fee
,null as remain_term_fee
,t1.current_overdue_principal as  overdue_principal
,t1.current_overdue_interest as    overdue_interest
,null as   overdue_svc_fee
,null as   overdue_term_fee
,null as   overdue_penalty
,null as   overdue_mult_amt
,t6.overdue_date_first as   overdue_date_first
,t7.repay_date  as   overdue_date_start
,t1.current_overdue_days as overdue_days
,cast(date_add(t7.repay_date, t1.current_overdue_days - 1) as string) as overdue_date
,null as dpd_begin_date
,null as dpd_days
,null as dpd_days_count
,null as dpd_days_max
,null as collect_out_date
,t1.current_overdue_period as overdue_term
,t1.accum_overdue_period as overdue_terms_count
,t1.his_overdue_mperiods as overdue_terms_max
,t1.accum_overdue_principal as   overdue_principal_accumulate
,t1.his_overdue_mprincipal as overdue_principal_max
,account_date as sync_date
,account_date as s_d_date
,date_add(account_date,1) as e_d_date
,null as effective_time
,null as expire_time
from
(select
*
from ods.t_asset_check
where
project_id = 'pl00282'
) t1
left join
(select distinct
concat( '10041', '_', sha256 ( decrypt_aes ( document_num, 'tencentabs123456' ), 'idNumber', 1 ) ) AS cust_id,
sha256 ( decrypt_aes ( document_num, 'tencentabs123456' ), 'idNumber', 1 ) AS user_hash_no,
asset_id,
age_birth(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),datefmt ( substr( decrypt_aes ( document_num, 'tencentabs123456' ), 7, 8 ), 'yyyyMMdd', 'yyyy-MM-dd' )) as age
from ods.t_principal_borrower_info
where project_id = 'pl00282') t2
on t1.asset_id = t2.asset_id
left join
(SELECT
 asset_id
,contract_code
,loan_use
,repay_type
,periods
,loan_total_interest
,loan_penalty_rate
,loan_begin_date
 from ods.t_loan_contract_info where project_id = 'pl00282') t3
on t1.asset_id = t3.asset_id
left join
(SELECT asset_id,repay_date from ods.t_repayment_schedule
where project_id = 'pl00282'
and  remainder_periods = 0) t4
on t1.asset_id = t4.asset_id
left join
(SELECT asset_id,rel_pay_date from ods.t_repayment_info
where project_id = 'pl00282'
and plan_remainder_periods = 0) t5
on
t1.asset_id = t5.asset_id
left join
(select
asset_id
,min(repay_date) overdue_date_first
from
 ods.t_repayment_info
where project_id = 'pl00282'
and
overdue_day > 0
group by
asset_id) t6
on t1.asset_id = t6.asset_id
left join
(select
asset_id
,repay_date
,period
from
 ods.t_repayment_info
where project_id = 'pl00282'
and
overdue_day > 0 ) t7
on t1.asset_id = t7.asset_id
and t1.curr_period = t7.period
;



DROP TABLE IF EXISTS `ods_new_s.loan_info`;
CREATE TABLE IF NOT EXISTS `ods_new_s.loan_info`(
  `user_hash_no`                  string        COMMENT '用户编号',
  `cust_id`                       string        COMMENT '客户编号',
  `age`                           decimal(3,0)  COMMENT '年龄',
  `loan_id`                       string        COMMENT '借据ID',
  `due_bill_no`                   string        COMMENT '借据编号',
  `contract_no`                   string        COMMENT '合同编号',
  `apply_no`                      string        COMMENT '进件编号',
  `loan_usage`                    string        COMMENT '贷款用途',
  `register_date`                 string        COMMENT '注册日期',
  `request_time`                  timestamp     COMMENT '请求时间',
  `loan_active_date`              string        COMMENT '放款日期',
  `cycle_day`                     decimal(2,0)  COMMENT '账单日',
  `loan_expire_date`              string        COMMENT '贷款到期日期',
  `loan_type`                     string        COMMENT '分期类型（英文原值）（MCEP：等额本金，MCEI：等额本息，R：消费转分期，C：现金分期，B：账单分期，P：POS分期，M：大额分期（专项分期），MCAT：随借随还）',
  `loan_type_cn`                  string        COMMENT '分期类型（汉语解释）',
  `loan_init_term`                decimal(3,0)  COMMENT '贷款期数（3、6、9等）',
  `loan_term1`                    decimal(3,0)  COMMENT '当前期数（按起息日算）',
  `loan_term2`                    decimal(3,0)  COMMENT '当前期数（按应还日算）',
  `should_repay_date`             string        COMMENT '应还日期',
  `loan_term_repaid`              decimal(3,0)  COMMENT '已还期数',
  `loan_term_remain`              decimal(3,0)  COMMENT '剩余期数',
  `loan_status`                   string        COMMENT '借据状态（英文原值）（N：正常，O：逾期，F：已还清）',
  `loan_status_cn`                string        COMMENT '借据状态（汉语解释）',
  `loan_out_reason`               string        COMMENT '借据终止原因（P：提前还款，M：银行业务人员手工终止（manual），D：逾期自动终止（delinquency），R：锁定码终止（Refund），V：持卡人手动终止，C：理赔终止，T：退货终止，U：重组结清终止，F：强制结清终止，B：免息转分期）',
  `paid_out_type  `               string        COMMENT '结清类型（英文原值）（D：代偿结清，H：回购结清，T：退货（车）结清，P：提前结清，C：强制结清，F：正常到期结清）',
  `paid_out_type_cn`              string        COMMENT '结清类型（汉语解释）',
  `paid_out_date`                 string        COMMENT '还款日期',
  `terminal_date`                 string        COMMENT '提前终止日期',
  `loan_init_principal`           decimal(15,4) COMMENT '贷款本金',
  `loan_init_interest_rate`       decimal(15,8) COMMENT '利息利率',
  `loan_init_interest`            decimal(15,4) COMMENT '贷款利息',
  `loan_init_term_fee_rate`       decimal(15,8) COMMENT '手续费费率',
  `loan_init_term_fee`            decimal(15,4) COMMENT '贷款手续费',
  `loan_init_svc_fee_rate`        decimal(15,8) COMMENT '服务费费率',
  `loan_init_svc_fee`             decimal(15,4) COMMENT '贷款服务费',
  `loan_init_penalty_rate`        decimal(15,8) COMMENT '罚息利率',
  `paid_amount`                   decimal(15,4) COMMENT '已还金额',
  `paid_principal`                decimal(15,4) COMMENT '已还本金',
  `paid_interest`                 decimal(15,4) COMMENT '已还利息',
  `paid_penalty`                  decimal(15,4) COMMENT '已还罚息',
  `paid_svc_fee`                  decimal(15,4) COMMENT '已还服务费',
  `paid_term_fee`                 decimal(15,4) COMMENT '已还手续费',
  `paid_mult`                     decimal(15,4) COMMENT '已还滞纳金',
  `remain_amount`                 decimal(15,4) COMMENT '剩余金额：本息费',
  `remain_principal`              decimal(15,4) COMMENT '剩余本金',
  `remain_interest`               decimal(15,4) COMMENT '剩余利息',
  `remain_svc_fee`                decimal(15,4) COMMENT '剩余服务费',
  `remain_term_fee`               decimal(15,4) COMMENT '剩余手续费',
  `overdue_principal`             decimal(15,4) COMMENT '逾期本金',
  `overdue_interest`              decimal(15,4) COMMENT '逾期利息',
  `overdue_svc_fee`               decimal(15,4) COMMENT '逾期服务费',
  `overdue_term_fee`              decimal(15,4) COMMENT '逾期手续费',
  `overdue_penalty`               decimal(15,4) COMMENT '逾期罚息',
  `overdue_mult_amt`              decimal(15,4) COMMENT '逾期滞纳金',
  `overdue_date_first`            string        COMMENT '首次逾期日期',
  `overdue_date_start`            string        COMMENT '逾期起始日期',
  `overdue_days`                  decimal(5,0)  COMMENT '逾期天数',
  `overdue_date`                  string        COMMENT '逾期日期',
  `dpd_begin_date`                string        COMMENT 'DPD起始日期',
  `dpd_days`                      decimal(4,0)  COMMENT 'DPD天数',
  `dpd_days_count`                decimal(4,0)  COMMENT '累计DPD天数',
  `dpd_days_max`                  decimal(4,0)  COMMENT '历史最大DPD天数',
  `collect_out_date`              string        COMMENT '出催日期',
  `overdue_term`                  decimal(3,0)  COMMENT '当前逾期期数',
  `overdue_terms_count`           decimal(3,0)  COMMENT '累计逾期期数',
  `overdue_terms_max`             decimal(3,0)  COMMENT '历史单次最长逾期期数',
  `overdue_principal_accumulate`  decimal(15,4) COMMENT '累计逾期本金',
  `overdue_principal_max`         decimal(15,4) COMMENT '历史最大逾期本金',
  `sync_date`                     string        COMMENT '同步日期',
  `s_d_date`                      string        COMMENT 'ods层起始日期',
  `e_d_date`                      string        COMMENT 'ods层结束日期',
  `effective_time`                timestamp     COMMENT '生效日期',
  `expire_time`                   timestamp     COMMENT '失效日期'
) COMMENT '借据信息表'
PARTITIONED BY (`is_settled` string COMMENT '是否已结清',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


CREATE TABLE ods.t_project (
id INT COMMENT '主键id,自增',
project_id STRING COMMENT '项目编号',
project_name STRING COMMENT '项目名称',
project_type STRING COMMENT '项目类型',
chain_id STRING COMMENT '链id',
node_id STRING COMMENT '节点Id',
append_url STRING COMMENT '新增数据地址',
query_url STRING COMMENT '查询地址',
tx_url STRING COMMENT '根据高度获取数据',
private_key STRING COMMENT '密钥',
mch_id STRING COMMENT '机构id',
version STRING COMMENT '版本',
current_read_height INT COMMENT '当前读取区块高度',
max_height INT COMMENT '最大区块高度',
create_time STRING COMMENT '创建记录时间',
modify_time STRING COMMENT '修改时间',
block_sign STRING COMMENT '区块链标志',
agency_id STRING COMMENT '关联机构id',
action_data STRING COMMENT '行为控制',
calculate_data STRING COMMENT '计算控制',
packet_date STRING COMMENT '项目封包日',
time_difference STRING COMMENT '文件10计算时间差',
video_handle_startdate STRING COMMENT '影像文件处理开始日期',
video_handle_enddate STRING COMMENT '影像文件处理结束日期',
video_file_path STRING COMMENT '影像文件路径',
grace_days INT COMMENT '豁免天数',
check_difference INT
) WITH SERDEPROPERTIES ( 'serialization.format' = '1' ) STORED AS PARQUET;

select count(*) as num from ods.t_guaranty_car t1 join ods.t_loan_contract_info t2 on t1.bill_no = t2.asset_id
--CREATE TABLE ods.t_guaranty_car (
--id INT COMMENT '主键id,自增',
--bill_no STRING COMMENT '借据编号',
--frame_num STRING COMMENT '车架号',
--car_brand STRING COMMENT '车辆品牌',
--car_model STRING COMMENT '车辆型号',
--car_colour STRING COMMENT '车辆颜色',
--license_num STRING COMMENT '车辆号码',
--register_date STRING COMMENT '注册日期',
--pawn_value DECIMAL ( 15, 4 ) COMMENT '评估价值',
--create_time STRING,
--update_time STRING,
--car_series STRING COMMENT '车系',
--drive_years STRING COMMENT '车龄',
--import_id STRING COMMENT 'IMPORTID',
--gps_code STRING COMMENT 'GPS编号',
--gps_fee DECIMAL ( 16, 2 ) COMMENT 'GPS费用',
--TIMESTAMP STRING COMMENT 'TIMESTAMP',
--car_type STRING COMMENT '车类型',
--milage STRING COMMENT '里程数',
--insurance_type STRING COMMENT '保险种类',
--engine_num STRING COMMENT '发动机号',
--mortgage_order STRING COMMENT '抵押顺位',
--guarantee_method STRING COMMENT '担保方式',
--org_code STRING COMMENT '机构编号',
--product_date STRING COMMENT '生产日期',
--financing STRING COMMENT '融资方式',
--car_nature STRING COMMENT '车辆性质',
--project_num STRING COMMENT '项目编号',
--import_filetype STRING COMMENT 'IMPORTFILETYPE',
--mortage_num STRING COMMENT '抵押物编号',
--purchase_place STRING COMMENT '车辆购买地',
--fee_one DECIMAL ( 16, 2 ) COMMENT '责信保费用1',
--fee_two DECIMAL ( 16, 2 ) COMMENT '责信保费用2',
--total_investment DECIMAL ( 16, 2 ) COMMENT '投资总额(元)',
--mortage_status STRING COMMENT '抵押办理状态',
--energy_type STRING COMMENT '车辆能源类型',
--formalities_fee DECIMAL ( 16, 2 ) COMMENT '手续总费用(元)',
--guide_price DECIMAL ( 16, 2 ) COMMENT '新车指导价(元)',
--purchase_tax DECIMAL ( 16, 2 ) COMMENT '购置税金额(元)',
--insurance_fee DECIMAL ( 16, 2 ) COMMENT '汽车保险总费用',
--sales_price DECIMAL ( 16, 2 ) COMMENT '车辆销售价格(元)',
--total_trans_times INT COMMENT '累计车辆过户次数',
--year_trans_times INT COMMENT '一年内车辆过户次数'
--) COMMENT '' WITH SERDEPROPERTIES ( 'serialization.format' = '1' ) STORED AS PARQUET;
--
--浦发用户数据没有重复 且主键没有为空 ods.t_ods_credit 和 ods.t_credit_loan 都只有823条
CREATE TABLE ods.t_ods_credit (
credit_id STRING,
assessment_date STRING COMMENT '评估日期',
credit_result STRING COMMENT '授信结果',
credit_amount DECIMAL ( 16, 2 ) COMMENT '授信金额',
credit_validity STRING COMMENT '授信有效期',
refuse_reason STRING COMMENT '授信拒绝原因',
current_remain_credit_amount DECIMAL ( 16, 2 ) COMMENT '当前剩余额度',
current_credit_amount_utilization_rate DECIMAL ( 16, 4 ) COMMENT '当前额度使用率',
accumulate_credit_amount_utilization_rate DECIMAL ( 16, 4 ) COMMENT '累计额度使用率',
create_time STRING COMMENT '创建时间',
update_time STRING COMMENT '更新时间',
project_id STRING COMMENT '项目编号',
agency_id STRING COMMENT '机构编号',
ecif_no STRING COMMENT 'ecifNo',
card_no STRING COMMENT '身份证号码(脱敏处理)',
phone_no STRING COMMENT '手机号码(脱敏处理)',
imei STRING COMMENT 'imei号'
) COMMENT '' WITH SERDEPROPERTIES ( 'serialization.format' = '1' ) STORED AS PARQUET;
phone_num
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=3000;
insert overwrite table ods_new_s.credit_apply partition(biz_date,product_id)
select
concat( '10041', '_', sha256 ( t2.card_no, 'idNumber', 1 ) ) AS cust_id
,sha256 ( t2.card_no, 'idNumber', 1 ) AS user_hash_no
,t1.credit_id as apply_id
,t1.assessment_date as  credit_apply_time
,t1.credit_amount as apply_amount
,'1' as resp_code
,'授信通过' as resp_msg
,t1.assessment_date as credit_approval_time
,t1.credit_validity as credit_expire_date
,t1.credit_amount as credit_amount
,null as credit_interest_rate
,null as credit_interest_penalty_rate
,null as risk_assessment_time
,null as risk_type
,null as risk_result_validity
,null as risk_level
,null as risk_score
,null as ori_request
,null as ori_response
,t1.create_time
,t1.update_time
,to_date(t1.assessment_date) as biz_date
,'pl00282' as product_id
from
(select * from ods.t_ods_credit
where project_id = 'pl00282') t1
left join
(select
distinct
decrypt_aes ( document_num, 'tencentabs123456' ) as card_no
,decrypt_aes ( phone_num, 'tencentabs123456' ) as phone_no
from
ods.t_principal_borrower_info
where project_id = 'pl00282') t2
on decrypt_aes(t1.phone_no,'tencentabs123456') = t2.phone_no
;








-- /**
--  * 授信申请表
--  *
--  * 数据库主键 apply_id
--  * 业务主键 apply_id
--  *
--  * 可能有多次授信
--  *
--  * 按照 biz_date,product_id 分区
--  */
-- DROP TABLE IF EXISTS `ods_new_s.credit_apply`;
CREATE TABLE IF NOT EXISTS `ods_new_s.credit_apply`(
  `cust_id`                       string        COMMENT '客户编号',
  `user_hash_no`                  string        COMMENT '用户编号',
  `apply_id`                      string        COMMENT '授信申请编号',
  `credit_apply_time`             timestamp     COMMENT '授信申请时间',
  `apply_amount`                  decimal(15,4) COMMENT '授信申请金额',
  `resp_code`                     string        COMMENT '授信申请结果（1: 授信通过，2: 授信失败）',
  `resp_msg`                      string        COMMENT '授信结果描述',
  `credit_approval_time`          timestamp     COMMENT '授信通过时间',
  `credit_expire_date`            timestamp     COMMENT '授信有效时间',
  `credit_amount`                 decimal(15,4) COMMENT '授信通过额度',
  `credit_interest_rate`          decimal(15,8) COMMENT '授信利息利率',
  `credit_interest_penalty_rate`  decimal(15,8) COMMENT '授信罚息利率',
  `risk_assessment_time`          timestamp     COMMENT '风控评估时间',
  `risk_type`                     string        COMMENT '风控类型（用信风控、二次风控）',
  `risk_result_validity`          timestamp     COMMENT '风控结果有效期',
  `risk_level`                    string        COMMENT '风控等级',
  `risk_score`                    string        COMMENT '风控评分',
  `ori_request`                   string        COMMENT '原始请求',
  `ori_response`                  string        COMMENT '原始应答',
  `create_time`                   timestamp     COMMENT '创建时间',
  `update_time`                   timestamp     COMMENT '更新时间'
) COMMENT '授信申请表'
PARTITIONED BY (`biz_date` string COMMENT '授信申请日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;
CREATE TABLE IF NOT EXISTS `ods_new_s.loan_apply`(
  `cust_id`                       string        COMMENT '客户编号',
  `user_hash_no`                  string        COMMENT '用户编号',
  `birthday`                      string        COMMENT '出生日期',
  `pre_apply_no`                  string        COMMENT '预审申请编号',
  `apply_id`                      string        COMMENT '申请id',
  `due_bill_no`                   string        COMMENT '借据编号',
  `loan_apply_time`               timestamp     COMMENT '用信申请时间',
  `loan_amount_apply`             decimal(15,4) COMMENT '用信申请金额',
  `loan_terms`                    decimal(3,0)  COMMENT '贷款期数（3、6、9等）',
  `loan_usage`                    string        COMMENT '贷款用途（英文原值）（1：日常消费，2：汽车加油，3：修车保养，4：购买汽车，5：医疗支出，6：教育深造，7：房屋装修，8：旅游出行，9：其他消费）',
  `loan_usage_cn`                 string        COMMENT '贷款用途（汉语解释）',
  `repay_type`                    string        COMMENT '还款方式（英文原值）（1：等额本金，2：等额本息、等额等息等）',
  `repay_type_cn`                 string        COMMENT '还款方式（汉语解释）',
  `interest_rate`                 decimal(15,8) COMMENT '利息利率（8d/%）',
  `penalty_rate`                  decimal(15,8) COMMENT '罚息利率（8d/%）',
  `apply_status`                  decimal(2,0)  COMMENT '申请状态（1: 放款成功，2: 放款失败，3: 处理中，4：用信成功，5：用信失败）',
  `apply_resut_msg`               string        COMMENT '申请结果信息',
  `issue_time`                    timestamp     COMMENT '放款时间，放款成功后必填',
  `loan_amount_approval`          decimal(15,4) COMMENT '用信通过金额',
  `loan_amount`                   decimal(15,4) COMMENT '放款金额',
  `risk_level`                    string        COMMENT '风控等级',
  `risk_score`                    string        COMMENT '风控评分',
  `ori_request`                   string        COMMENT '原始请求',
  `ori_response`                  string        COMMENT '原始应答',
  `create_time`                   string        COMMENT '创建时间',
  `update_time`                   string        COMMENT '更新时间'
) COMMENT '用信申请表'
PARTITIONED BY (`biz_date` string COMMENT '用信申请日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=3000;
insert overwrite table ods_new_s.loan_apply partition(biz_date,product_id)
select
t2.cust_id
,t2.user_hash_no
,t2.birthday
,null as pre_apply_no
,t1.credit_id as apply_id
,t1.loan_id as due_bill_no
,t1.assessment_date as loan_apply_time
,t1.apply_amt as loan_amount_apply
,t1.product_terms as loan_terms
, '9' as loan_usage
,t3.loan_use as loan_usage_cn
,'2' as repay_type
,t3.repay_type as repay_type_cn
,t4.loan_interest_rate as interest_rate
,t3.loan_penalty_rate as penalty_rate
,t1.risk_control_result as apply_status
,null as apply_resut_msg
,to_date(t3.loan_begin_date) as issue_time
,t1.approval_amt as loan_amount_approval
,t3.loan_total_amount as loan_amount
,null as risk_level
,null as risk_score
,null as ori_request
,null as ori_response
,t1.create_time
,t1.update_time
,to_date(t1.assessment_date) as biz_date
,'pl00282' as product_id
from
(select * from ods.t_credit_loan where project_id = 'pl00282') t1
left join
(select distinct
concat( '10041', '_', sha256 ( decrypt_aes ( document_num, 'tencentabs123456' ), 'idNumber', 1 ) ) AS cust_id,
sha256 ( decrypt_aes ( document_num, 'tencentabs123456' ), 'idNumber', 1 ) AS user_hash_no,
asset_id,
datefmt ( substr( decrypt_aes ( document_num, 'tencentabs123456' ), 7, 8 ), 'yyyyMMdd', 'yyyy-MM-dd' ) as birthday
from ods.t_principal_borrower_info
where project_id = 'pl00282') t2
on t1.loan_id = t2.asset_id
left join
(SELECT
 asset_id
,loan_use
,repay_type
,periods
,loan_total_amount
,loan_total_interest
,loan_penalty_rate
,loan_begin_date
 from ods.t_loan_contract_info where project_id = 'pl00282') t3
 on t1.loan_id = t3.asset_id
left join
(select
asset_id
,loan_interest_rate
from ods.t_asset_check
where
project_id = 'pl00282'
and
account_date = '2020-09-07'
) t4
on t1.loan_id = t4.asset_id
;


-- 都只有823条
CREATE TABLE ods.t_ods_credit (
credit_id STRING COMMENT '授信编号',
assessment_date STRING COMMENT '评估日期',
credit_result STRING COMMENT '授信结果',
credit_amount DECIMAL ( 16, 2 ) COMMENT '授信金额',
credit_validity STRING COMMENT '授信有效期',
refuse_reason STRING COMMENT '授信拒绝原因',
current_remain_credit_amount DECIMAL ( 16, 2 ) COMMENT '当前剩余额度',
current_credit_amount_utilization_rate DECIMAL ( 16, 4 ) COMMENT '当前额度使用率',
accumulate_credit_amount_utilization_rate DECIMAL ( 16, 4 ) COMMENT '累计额度使用率',
create_time STRING COMMENT '创建时间',
update_time STRING COMMENT '更新时间',
project_id STRING COMMENT '项目编号',
agency_id STRING COMMENT '机构编号',
ecif_no STRING COMMENT 'ecifNo',
card_no STRING COMMENT '身份证号码(脱敏处理)',
phone_no STRING COMMENT '手机号码(脱敏处理)',
imei STRING COMMENT 'imei号'
) COMMENT '授信表' WITH SERDEPROPERTIES ( 'serialization.format' = '1' ) STORED AS PARQUET;

--浦发用户数据没有重复 且主键没有为空 都只有823条
CREATE TABLE ods.t_credit_loan (
id INT COMMENT '主键',
credit_id STRING COMMENT '授信编号',
loan_id STRING COMMENT '借据编号',
project_id STRING COMMENT '项目编号',
agency_id STRING COMMENT '机构编号',
assessment_date STRING COMMENT '评估日期',
asset_level STRING COMMENT '资产等级',
credit_level STRING COMMENT '信用等级',
anti_fraud_level STRING COMMENT '反欺诈等级',
risk_control_result STRING COMMENT '风控结果',
refuse_reason STRING COMMENT '风控结果',
create_time STRING COMMENT '创建时间',
update_time STRING COMMENT '更新时间',
product_terms INT COMMENT '产品总期数',
ecif_no STRING COMMENT 'ecifNo',
card_no STRING COMMENT '身份证号码(脱敏处理)',
phone_no STRING COMMENT '手机号码(脱敏处理)',
imei STRING COMMENT 'imei号',
apply_amt DECIMAL ( 16, 2 ) COMMENT '申请用信金额',
approval_amt DECIMAL ( 16, 2 ) COMMENT '用信审批通过金额',
first_use_credit INT COMMENT 'credit_id是否存在'
) COMMENT '授信借据映射表' WITH SERDEPROPERTIES ( 'serialization.format' = '1' ) STORED AS PARQUET;

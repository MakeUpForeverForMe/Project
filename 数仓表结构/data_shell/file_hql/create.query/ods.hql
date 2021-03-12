-- DROP DATABASE IF EXISTS `ods`;
CREATE DATABASE IF NOT EXISTS `ods` COMMENT 'ods标准层（代偿前）' location 'cosn://bigdata-center-prod-1253824322/user/hadoop/warehouse/ods.db';

-- DROP DATABASE IF EXISTS `ods_cps`;
CREATE DATABASE IF NOT EXISTS `ods_cps` COMMENT 'ods标准层（代偿后）' location 'cosn://bigdata-center-prod-1253824322/user/hadoop/warehouse/ods_cps.db';


-- 授信申请表
-- 数据库主键 apply_id
-- 业务主键 apply_id
-- 可能有多次授信
-- 按照 biz_date,product_id 分区
-- DROP TABLE IF EXISTS `ods.credit_apply`;
CREATE TABLE IF NOT EXISTS `ods.credit_apply`(
  `cust_id`                                           string         COMMENT '客户编号',
  `user_hash_no`                                      string         COMMENT '用户编号',
  `apply_id`                                          string         COMMENT '授信申请编号',
  `credit_apply_time`                                 timestamp      COMMENT '授信申请时间',
  `apply_amount`                                      decimal(15,4)  COMMENT '授信申请金额',
  `resp_code`                                         string         COMMENT '授信申请结果（1：授信通过，2：授信失败）',
  `resp_msg`                                          string         COMMENT '授信结果描述',
  `credit_approval_time`                              timestamp      COMMENT '授信通过时间',
  `credit_expire_date`                                timestamp      COMMENT '授信有效时间',
  `credit_amount`                                     decimal(15,4)  COMMENT '授信通过额度',
  `credit_interest_rate`                              decimal(15,8)  COMMENT '授信利息利率',
  `credit_interest_penalty_rate`                      decimal(15,8)  COMMENT '授信罚息利率',
  `risk_assessment_time`                              timestamp      COMMENT '风控评估时间',
  `risk_type`                                         string         COMMENT '风控类型（用信风控、二次风控）',
  `risk_result_validity`                              timestamp      COMMENT '风控结果有效期',
  `risk_level`                                        string         COMMENT '风控等级',
  `risk_score`                                        string         COMMENT '风控评分',
  `ori_request`                                       string         COMMENT '原始请求',
  `ori_response`                                      string         COMMENT '原始应答',
  `create_time`                                       timestamp      COMMENT '创建时间',
  `update_time`                                       timestamp      COMMENT '更新时间'
) COMMENT '授信申请表'
PARTITIONED BY (`biz_date` string COMMENT '授信申请日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 用信申请表
-- 数据库主键 apply_id
-- 业务主键 apply_id
-- 按照 biz_date 分区
-- DROP TABLE IF EXISTS `ods.loan_apply`;
CREATE TABLE IF NOT EXISTS `ods.loan_apply`(
  `cust_id`                                           string         COMMENT '客户编号',
  `user_hash_no`                                      string         COMMENT '用户编号',
  `birthday`                                          string         COMMENT '出生日期',
  `age`                                               decimal(3,0)   COMMENT '年龄',
  `pre_apply_no`                                      string         COMMENT '预审申请编号',
  `apply_id`                                          string         COMMENT '申请id',
  `due_bill_no`                                       string         COMMENT '借据编号',
  `loan_apply_time`                                   timestamp      COMMENT '用信申请时间',
  `loan_amount_apply`                                 decimal(15,4)  COMMENT '用信申请金额',
  `loan_terms`                                        decimal(3,0)   COMMENT '贷款期数（3、6、9等）',
  `loan_usage`                                        string         COMMENT '贷款用途（英文原值）（1：日常消费，2：汽车加油，3：修车保养，4：购买汽车，5：医疗支出，6：教育深造，7：房屋装修，8：旅游出行，9：其他消费）',
  `loan_usage_cn`                                     string         COMMENT '贷款用途（汉语解释）',
  `repay_type`                                        string         COMMENT '还款方式（英文原值）（1：等额本金，2：等额本息、等额等息等）',
  `repay_type_cn`                                     string         COMMENT '还款方式（汉语解释）',
  `interest_rate`                                     decimal(15,8)  COMMENT '利息利率（8d/%）',
  `credit_coef`                                       decimal(15,8)  COMMENT '综合融资成本（8d/%）',
  `penalty_rate`                                      decimal(15,8)  COMMENT '罚息利率（8d/%）',
  `apply_status`                                      decimal(2,0)   COMMENT '申请状态（1: 放款成功，2: 放款失败，3: 处理中，4：用信成功，5：用信失败）',
  `apply_resut_msg`                                   string         COMMENT '申请结果信息',
  `issue_time`                                        timestamp      COMMENT '放款时间，放款成功后必填',
  `loan_amount_approval`                              decimal(15,4)  COMMENT '用信通过金额',
  `loan_amount`                                       decimal(15,4)  COMMENT '放款金额',
  `risk_level`                                        string         COMMENT '风控等级',
  `risk_score`                                        string         COMMENT '风控评分',
  `ori_request`                                       string         COMMENT '原始请求',
  `ori_response`                                      string         COMMENT '原始应答',
  `create_time`                                       string         COMMENT '创建时间',
  `update_time`                                       string         COMMENT '更新时间'
) COMMENT '用信申请表'
PARTITIONED BY (`biz_date` string COMMENT '用信申请日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 客户信息表
-- 数据库主键 cust_id
-- DROP TABLE IF EXISTS `ods.customer_info`;
CREATE TABLE IF NOT EXISTS `ods.customer_info`(
  `due_bill_no`                                       string         COMMENT '借据号',
  `cust_id`                                           string         COMMENT '客户编号',
  `user_hash_no`                                      string         COMMENT '用户编号',
  `outer_cust_id`                                     string         COMMENT '外部客户号',
  `idcard_type`                                       string         COMMENT '证件类型（身份证等）',
  `idcard_no`                                         string         COMMENT '证件号码',
  `name`                                              string         COMMENT '客户姓名',
  `mobie`                                             string         COMMENT '客户电话',
  `card_phone`                                        string         COMMENT '客户银行卡绑定手机号',
  `sex`                                               string         COMMENT '客户性别（男、女）',
  `birthday`                                          string         COMMENT '出生日期',
  `age`                                               decimal(3,0)   COMMENT '客户年龄',
  `marriage_status`                                   string         COMMENT '婚姻状态',
  `education`                                         string         COMMENT '学历',
  `education_ws`                                      string         COMMENT '学历级别（硕士及以上、大学本科、大专及以下、未知）',
  `idcard_address`                                    string         COMMENT '身份证地址',
  `idcard_area`                                       string         COMMENT '身份证大区（东北地区、华北地区、西北地区、西南地区、华南地区、华东地区、华中地区、港澳台地区）',
  `idcard_province`                                   string         COMMENT '身份证省级（省/直辖市/特别行政区）',
  `idcard_city`                                       string         COMMENT '身份证地级（城市）',
  `idcard_county`                                     string         COMMENT '身份证县级（区县）',
  `idcard_township`                                   string         COMMENT '身份证乡级（乡/镇/街）（预留）',
  `resident_address`                                  string         COMMENT '常住地地址',
  `resident_area`                                     string         COMMENT '常住地大区（东北地区、华北地区、西北地区、西南地区、华南地区、华东地区、华中地区、港澳台地区）',
  `resident_province`                                 string         COMMENT '常住地省级（省/直辖市/特别行政区）',
  `resident_city`                                     string         COMMENT '常住地地级（城市）',
  `resident_county`                                   string         COMMENT '常住地县级（区县）',
  `resident_township`                                 string         COMMENT '常住地乡级（乡/镇/街）（预留）',
  `job_type`                                          string         COMMENT '工作类型',
  `job_year`                                          decimal(2,0)   COMMENT '工作年限',
  `income_month`                                      decimal(15,4)  COMMENT '月收入',
  `income_year`                                       decimal(15,4)  COMMENT '年收入',
  `cutomer_type`                                      string         COMMENT '客戶类型（个人或企业）',
  `cust_rating`                                       string         COMMENT '内部信用等级'
) COMMENT '客户信息表'
PARTITIONED BY (`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 联系人信息表
-- 数据库主键 linkman_id
-- 业务主键 cust_id,due_bill_no,relation_id,card_no
-- 分区表：每日分区存储新增数据
-- DROP TABLE IF EXISTS `ods.linkman_info`;
CREATE TABLE IF NOT EXISTS `ods.linkman_info`(
  `cust_id`                                           string         COMMENT '客户编号',
  `user_hash_no`                                      string         COMMENT '用户编号',
  `due_bill_no`                                       string         COMMENT '借据编号',
  `linkman_id`                                        string         COMMENT '联系人编号',
  `relational_type`                                   string         COMMENT '关联人类型（英文原值）',
  `relational_type_cn`                                string         COMMENT '关联人类型（汉语解释）',
  `relationship`                                      string         COMMENT '联系人关系（英文原值）（如：1：父母、2：配偶、3：子女、4：兄弟姐妹、5：亲属、6：同事、7：朋友、8：其他）',
  `relationship_cn`                                   string         COMMENT '联系人关系（汉语解释）',
  `relation_idcard_type`                              string         COMMENT '联系人证件类型',
  `relation_idcard_no`                                string         COMMENT '联系人证件号码',
  `relation_birthday`                                 string         COMMENT '联系人出生日期',
  `relation_name`                                     string         COMMENT '联系人姓名',
  `relation_sex`                                      string         COMMENT '联系人性别',
  `relation_mobile`                                   string         COMMENT '联系人电话',
  `relation_address`                                  string         COMMENT '联系人地址',
  `relation_province`                                 string         COMMENT '联系人省份',
  `relation_city`                                     string         COMMENT '联系人城市',
  `relation_county`                                   string         COMMENT '联系人区县',
  `corp_type`                                         string         COMMENT '工作类型',
  `corp_name`                                         string         COMMENT '公司名称',
  `corp_teleph_nbr`                                   string         COMMENT '公司电话',
  `corp_fax`                                          string         COMMENT '公司传真',
  `corp_position`                                     string         COMMENT '公司职务',
  `deal_date`                                         string         COMMENT '业务时间',
  `create_time`                                       string         COMMENT '创建时间',
  `update_time`                                       string         COMMENT '更新时间'
) COMMENT '联系人信息表'
PARTITIONED BY (`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- DROP TABLE IF EXISTS `ods.user_label`;
CREATE TABLE IF NOT EXISTS `ods.user_label`(
  `user_hash_no`                                      string         COMMENT '用户NO',
  `project_name`                                      string         COMMENT '项目名',
  `project_id`                                        string         COMMENT '项目ID',
  `order_id`                                          string         COMMENT '申请号',
  `pro_code`                                          string         COMMENT '产品编码',
  `ret_msg`                                           string         COMMENT '拒绝原因',
  `apply_by_id_last_year_fit`                         decimal(12,8)  COMMENT '跨平台数过去一年申请数量按身份证号码统计fit',
  `apply_by_id_last_year`                             int            COMMENT '跨平台数过去一年申请数量按身份证号码统计',
  `apps_of_con_fin_pro_last_two_year_fit`             decimal(12,8)  COMMENT '消费金融类过去两年申请产品数fit',
  `apps_of_con_fin_pro_last_two_year`                 int            COMMENT '消费金融类过去两年申请产品数',
  `usage_rate_of_compus_loan_app_fit`                 decimal(12,8)  COMMENT '校园类贷款app过去30天内使用强度fit',
  `usage_rate_of_compus_loan_app`                     int            COMMENT '校园类贷款app过去30天内使用强度',
  `apps_of_con_fin_org_last_two_year_fit`             decimal(12,8)  COMMENT '消费金融类机构过去两年申请数量fit',
  `apps_of_con_fin_org_last_two_year`                 int            COMMENT '消费金融类机构过去两年申请数量',
  `reject_times_of_credit_apply_last4_m_fit`          decimal(12,8)  COMMENT '过去四个月授信失败次数fit',
  `reject_times_of_credit_apply_last4_m`              int            COMMENT '过去四个月授信失败次数',
  `overdue_times_of_credit_apply_last4_m_fit`         decimal(12,8)  COMMENT '过去四个月逾期次数fit',
  `overdue_times_of_credit_apply_last4_m`             int            COMMENT '过去四个月逾期次数',
  `sum_credit_of_web_loan_fit`                        decimal(12,8)  COMMENT '网络贷款平台总授信额度fit',
  `sum_credit_of_web_loan`                            int            COMMENT '网络贷款平台总授信额度',
  `count_of_register_apps_of_fin_last_m_fit`          decimal(12,8)  COMMENT '过去一个月内金融类app注册总数量fit',
  `count_of_register_apps_of_fin_last_m`              int            COMMENT '过去一个月内金融类app注册总数量',
  `count_of_uninstall_apps_of_loan_last3_m_fit`       decimal(12,8)  COMMENT '过去三个月贷款类APP卸载总数量fit',
  `count_of_uninstall_apps_of_loan_last3_m`           int            COMMENT '过去三个月贷款类APP卸载总数量',
  `days_of_location_upload_lats9_m_fit`               decimal(12,8)  COMMENT '过去九个月地理位置上报天数fit',
  `days_of_location_upload_lats9_m`                   int            COMMENT '过去九个月地理位置上报天数',
  `account_for_in_town_in_work_day_last_m_fit`        decimal(12,8)  COMMENT '最近一个月工作日出现在村镇占比fit',
  `account_for_in_town_in_work_day_last_m`            int            COMMENT '最近一个月工作日出现在村镇占比',
  `count_of_installition_of_loan_app_last2_m_fit`     decimal(12,8)  COMMENT '过去两个月金融贷款类APP安装总个数fit',
  `count_of_installition_of_loan_app_last2_m`         int            COMMENT '过去两个月金融贷款类APP安装总个数',
  `risk_of_device_fit`                                decimal(12,8)  COMMENT '综合评估设备风险程度fit',
  `risk_of_device`                                    int            COMMENT '综合评估设备风险程度',
  `account_for_wifi_use_time_span_last5_m_fit`        decimal(12,8)  COMMENT '过去五个月wifi连接时间长度占比fit',
  `account_for_wifi_use_time_span_last5_m`            int            COMMENT '过去五个月wifi连接时间长度占比',
  `count_of_notices_of_fin_message_last9_m_fit`       decimal(12,8)  COMMENT '过去九个月金融类通知接收总数量fit',
  `count_of_notices_of_fin_message_last9_m`           int            COMMENT '过去九个月金融类通知接收总数量',
  `days_of_earliest_register_of_loan_app_last9_m_fit` decimal(12,8)  COMMENT '过去九个月最早注册贷款应用距今天数fit',
  `days_of_earliest_register_of_loan_app_last9_m`     int            COMMENT '过去九个月最早注册贷款应用距今天数',
  `loan_amt_last3_m_fit`                              decimal(12,8)  COMMENT '过去三个月贷款总金额fit',
  `loan_amt_last3_m`                                  int            COMMENT '过去三个月贷款总金额',
  `overdue_loans_of_more_than1_day_last6_m_fit`       decimal(12,8)  COMMENT '过去六个月发生一天以上的逾期贷款总笔数fit',
  `overdue_loans_of_more_than1_day_last6_m`           int            COMMENT '过去六个月发生一天以上的逾期贷款总笔数',
  `amt_of_perfermance_loans_last3_m_fit`              decimal(12,8)  COMMENT '过去三个月履约贷款总金额fit',
  `amt_of_perfermance_loans_last3_m`                  int            COMMENT '过去三个月履约贷款总金额',
  `last_financial_query_fit`                          decimal(12,8)  COMMENT '最近一次金融类查询距今时间（月）fit',
  `last_financial_query`                              int            COMMENT '最近一次金融类查询距今时间（月）',
  `average_daily_open_times_of_fin_apps_last_m_fit`   decimal(12,8)  COMMENT '过去一个月金融理财类APP每天打开次数平均值fit',
  `average_daily_open_times_of_fin_apps_last_m`       int            COMMENT '过去一个月金融理财类APP每天打开次数平均值',
  `times_of_uninstall_fin_apps_last15_d_fit`          decimal(12,8)  COMMENT '过去半个月金融理财类APP卸载总次数fit',
  `times_of_uninstall_fin_apps_last15_d`              int            COMMENT '过去半个月金融理财类APP卸载总次数',
  `account_for_install_bussiness_apps_last4_m_fit`    decimal(12,8)  COMMENT '过去四个月商务类APP安装数量占比fit',
  `account_for_install_bussiness_apps_last4_m`        int            COMMENT '过去四个月商务类APP安装数量占比',
  `city_level_model`                                  int            COMMENT '城市模型标签',
  `consume_model`                                     int            COMMENT '消费模型标签',
  `education_model`                                   int            COMMENT '学历模型标签',
  `marriage_model`                                    int            COMMENT '婚姻模型标签',
  `financial_model`                                   int            COMMENT '理财模型标签',
  `income_level`                                      string         COMMENT '收入等级',
  `blk_list1`                                         int            COMMENT '外部机构1黑名单',
  `blk_list2`                                         int            COMMENT '外部机构2黑名单',
  `blk_list_loc`                                      int            COMMENT '自有黑名单',
  `virtual_malicious_status`                          int            COMMENT '疑似涉黄涉恐',
  `counterfeit_agency_status`                         int            COMMENT '疑似资料伪造包装',
  `forgedid_status`                                   int            COMMENT '疑似资料伪冒行为',
  `gamer_arbitrage_status`                            int            COMMENT '疑似营销活动欺诈',
  `id_theft_status`                                   int            COMMENT '疑似资料被盗',
  `hit_deadbeat_list`                                 int            COMMENT '疑似公开信息失信',
  `fraud_industry`                                    int            COMMENT '疑似金融黑产相关',
  `cat_pool`                                          int            COMMENT '疑似手机猫池欺诈',
  `suspicious_device`                                 int            COMMENT '疑似风险设备环境',
  `abnormal_payment`                                  int            COMMENT '疑似异常支付行为',
  `abnormal_account`                                  int            COMMENT '疑似线上养号',
  `account_hacked`                                    int            COMMENT '疑似账号被盗风向',
  `score_level`                                       string         COMMENT '资产等级（A—E）',
  `pass`                                              string         COMMENT '是否通过（Yes，No）',
  `associated_partner_evaluation_rating`              int            COMMENT '关联方评估等级',
  `vendor_attributes`                                 int            COMMENT '场景方评分',
  `multi_borrowing`                                   int            COMMENT '多头借贷综合指数',
  `gambling_preference`                               int            COMMENT '博彩偏好综合指数',
  `gaming_preference`                                 int            COMMENT '游戏偏好综合指数',
  `multimedia_preference`                             int            COMMENT '视频偏好综合指数',
  `social_preference`                                 int            COMMENT '交友偏好综合指数',
  `create_time`                                       string         COMMENT '创建时间',
  `update_time`                                       bigint         COMMENT '修改时间'
) COMMENT '用户标签表'
STORED AS PARQUET;


-- DROP TABLE IF EXISTS `ods.guaranty_info`;
CREATE TABLE IF NOT EXISTS `ods.guaranty_info`(
  `due_bill_no`                                       string         COMMENT '借据号',
  `guaranty_code`                                     string         COMMENT '抵押物编号',
  `guaranty_handling_status`                          string         COMMENT '抵押办理状态 预定义字段：办理中 办理完成 尚未办理',
  `guaranty_alignment`                                string         COMMENT '抵押顺位 1-第一顺位 2-第二顺位 9-其他',
  `car_property`                                      string         COMMENT '车辆性质 预定义字段：非融资车分期非 融资车抵贷 融资租赁车分期 融资租赁车抵贷',
  `financing_type`                                    string         COMMENT '融资方式 预定义字段：正租 反租',
  `guarantee_type`                                    string         COMMENT '担保方式 预定义字段：质押担保，信用担保，保证担保，抵押担保',
  `pawn_value`                                        decimal(20,6)  COMMENT '评估价格(元)',
  `car_sales_price`                                   decimal(20,6)  COMMENT '车辆销售价格(元)',
  `car_new_price`                                     decimal(20,6)  COMMENT '新车指导价(元)',
  `total_investment`                                  decimal(20,6)  COMMENT '投资总额(元)',
  `purchase_tax_amouts`                               decimal(20,6)  COMMENT '购置税金额(元)',
  `insurance_type`                                    string         COMMENT '保险种类  交强险 第三者责任险 盗抢险 车损险 不计免赔 其他',
  `car_insurance_premium`                             decimal(20,6)  COMMENT '汽车保险总费用',
  `total_poundage`                                    decimal(20,6)  COMMENT '手续总费用(元)',
  `cumulative_car_transfer_number`                    decimal(11,0)  COMMENT '累计车辆过户次数',
  `one_year_car_transfer_number`                      decimal(11,0)  COMMENT '一年内车辆过户次数',
  `liability_insurance_cost1`                         decimal(20,6)  COMMENT '责信保费用1',
  `liability_insurance_cost2`                         decimal(20,6)  COMMENT '责信保费用2',
  `car_type`                                          string         COMMENT '车类型 预定义字段：新车 二手车',
  `frame_num`                                         string         COMMENT '车架号',
  `engine_num`                                        string         COMMENT '发动机号',
  `gps_code`                                          string         COMMENT 'GPS编号',
  `gps_cost`                                          decimal(20,6)  COMMENT 'GPS费用',
  `license_num`                                       string         COMMENT '车牌号码',
  `car_brand`                                         string         COMMENT '车辆品牌',
  `car_system`                                        string         COMMENT '车系',
  `car_model`                                         string         COMMENT '车型',
  `car_age`                                           decimal(20,6)  COMMENT '车龄',
  `car_energy_type`                                   string         COMMENT '车辆能源类型 预定义字段： 混合动力 纯电 非新能源车',
  `production_date`                                   string         COMMENT '生产日期',
  `mileage`                                           decimal(20,6)  COMMENT '里程数',
  `register_date`                                     string         COMMENT '注册日期',
  `buy_car_address`                                   string         COMMENT '车辆购买地',
  `car_colour`                                        string         COMMENT '车辆颜色',
  `create_time`                                       timestamp      COMMENT '创建时间',
  `update_time`                                       timestamp      COMMENT '更新时间'
) COMMENT '抵押物（车）信息表'
PARTITIONED BY (`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- DROP TABLE IF EXISTS `ods.t_enterprise_info`;
CREATE TABLE IF NOT EXISTS `ods.enterprise_info` (
  `due_bill_no`                                       string         COMMENT '资产借据号',
  `contract_role`                                     string         COMMENT '合同角色（0：主借款企业，1：共同借款企业，2：担保企业，3：无）',
  `enterprise_name`                                   string         COMMENT '企业，名称',
  `business_number`                                   string         COMMENT '工商注册号',
  `organizate_code`                                   string         COMMENT '组织机构代码',
  `taxpayer_number`                                   string         COMMENT '纳税人识别号',
  `unified_credit_code`                               string         COMMENT '统一信用代码',
  `registered_address`                                string         COMMENT '注册地址',
  `loan_type`                                         string         COMMENT '借款方类型',
  `industry`                                          string         COMMENT '企业行业',
  `legal_person_name`                                 string         COMMENT '法人代表姓名',
  `id_type`                                           string         COMMENT '法人证件类型',
  `id_no`                                             string         COMMENT '法人证件号码',
  `legal_person_phone`                                string         COMMENT '法人手机号码',
  `phone`                                             string         COMMENT '企业联系电话',
  `operate_years`                                     decimal(4,0)   COMMENT '企业运营年限',
  `is_linked`                                         string         COMMENT '是否挂靠企业',
  `province`                                          string         COMMENT '企业省份',
  `create_time`                                       timestamp      COMMENT '创建时间',
  `update_time`                                       timestamp      COMMENT '更新时间'
) COMMENT '企业信息表'
PARTITIONED BY (`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 风控信息表
-- 授信的时候没有产品，用项目编号做分区字段
-- 其他的会有产品编号，用产品编号做分区字段
-- DROP TABLE IF EXISTS `ods.risk_control`;
CREATE TABLE IF NOT EXISTS `ods.risk_control`(
  `risk_control_type`                                 decimal(2,0)   COMMENT '风控数据类型（1：授信，2：用信，3：星云）',
  `apply_id`                                          string         COMMENT '申请编号',
  `due_bill_no`                                       string         COMMENT '借据编号',
  `map_comment`                                       map<string,    string> COMMENT 'Map数据的Key解释字段',
  `map_value`                                         map<string,    string> COMMENT 'Map类型的风控数据',
  `create_time`                                       timestamp      COMMENT '创建时间',
  `update_time`                                       timestamp      COMMENT '更新时间'
) COMMENT '风控信息表'
PARTITIONED BY (`product_id` string COMMENT '产品编号',`source_table` string COMMENT '来源表')
STORED AS PARQUET;







set hivevar:db_suffix=;     -- 代偿前
set hivevar:db_suffix=_cps; -- 代偿后



-- DROP TABLE IF EXISTS `ods${db_suffix}.loan_lending`;
CREATE TABLE IF NOT EXISTS `ods${db_suffix}.loan_lending`(
  `apply_no`                                          string         COMMENT '进件编号',
  `contract_no`                                       string         COMMENT '合同编号',
  `contract_term`                                     decimal(3,0)   COMMENT '合同期限（按照月份计算）',
  `due_bill_no`                                       string         COMMENT '借据编号',
  `guarantee_type`                                    string         COMMENT '担保方式（信用担保，抵押担保）',
  `loan_usage`                                        string         COMMENT '贷款用途',
  `loan_issue_date`                                   string         COMMENT '合同开始日期',
  `loan_expiry_date`                                  string         COMMENT '合同结束日期',
  `loan_active_date`                                  string         COMMENT '放款日期',
  `loan_expire_date`                                  string         COMMENT '贷款到期日期',
  `cycle_day`                                         decimal(2,0)   COMMENT '账单日',
  `loan_type`                                         string         COMMENT '分期类型（英文原值）（MCEP：等额本金，MCEI：等额本息，R：消费转分期，C：现金分期，B：账单分期，P：POS分期，M：大额分期（专项分期），MCAT：随借随还，STAIR：阶梯还款）',
  `loan_type_cn`                                      string         COMMENT '分期类型（汉语解释）',
  `contract_daily_interest_rate_basis`                decimal(3,0)   COMMENT '日利率计算基础',
  `interest_rate_type`                                string         COMMENT '利率类型',
  `loan_init_interest_rate`                           decimal(25,10) COMMENT '利息利率',
  `loan_init_term_fee_rate`                           decimal(25,10) COMMENT '手续费费率',
  `loan_init_svc_fee_rate`                            decimal(25,10) COMMENT '服务费费率',
  `loan_init_penalty_rate`                            decimal(25,10) COMMENT '罚息利率',
  `tail_amount`                                       decimal(20,5)  COMMENT '尾款金额',
  `tail_amount_rate`                                  decimal(25,10) COMMENT '尾付比例',
  `bus_product_id`                                    string         COMMENT '产品方案编号',
  `bus_product_name`                                  string         COMMENT '产品方案名称',
  `mortgage_rate`                                     decimal(20,5)  COMMENT '抵押率',
  `biz_date`                                          string         COMMENT '放款日期'
) COMMENT '放款表'
PARTITIONED BY (`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- DROP TABLE IF EXISTS `ods${db_suffix}.loan_info_inter`;
CREATE TABLE IF NOT EXISTS `ods${db_suffix}.loan_info_inter`(
  `due_bill_no`                                       string         COMMENT '借据编号',
  `apply_no`                                          string         COMMENT '进件编号',
  `loan_active_date`                                  string         COMMENT '放款日期',
  `loan_init_term`                                    decimal(3,0)   COMMENT '贷款期数（3、6、9等）',
  `loan_init_principal`                               decimal(20,5)  COMMENT '贷款本金',
  `loan_init_interest`                                decimal(20,5)  COMMENT '贷款利息',
  `loan_init_term_fee`                                decimal(20,5)  COMMENT '贷款手续费',
  `loan_init_svc_fee`                                 decimal(20,5)  COMMENT '贷款服务费',
  `loan_term`                                         decimal(3,0)   COMMENT '当前期数（按应还日算）',
  `account_age`                                       decimal(3,0)   COMMENT '账龄（取当前期数）',
  `should_repay_date`                                 string         COMMENT '应还日期',
  `loan_term_repaid`                                  decimal(3,0)   COMMENT '已还期数',
  `loan_term_remain`                                  decimal(3,0)   COMMENT '剩余期数',
  `loan_status`                                       string         COMMENT '借据状态（英文原值）（N：正常，O：逾期，F：已还清）',
  `loan_status_cn`                                    string         COMMENT '借据状态（汉语解释）',
  `loan_out_reason`                                   string         COMMENT '借据终止原因（P：提前还款，M：银行业务人员手工终止（manual），D：逾期自动终止（delinquency），R：锁定码终止（Refund），V：持卡人手动终止，C：理赔终止，T：退货终止，U：重组结清终止，F：强制结清终止，B：免息转分期）',
  `paid_out_type`                                     string         COMMENT '结清类型（英文原值）（BANK_REF：退票结清，BUY_BACK：资产回购，CAPITAL_VERI：资产核销，DISPOSAL：处置结束，NORMAL_SETTLE：正常结清，OVER_COMP：逾期代偿，OVERDUE_SETTLE：逾期结清，PRE_SETTLE：提前结清，REDEMPTION：赎回，REFUND：退车，REFUND_SETTLEMENT：退票结清）',
  `paid_out_type_cn`                                  string         COMMENT '结清类型（汉语解释）',
  `paid_out_date`                                     string         COMMENT '还款日期',
  `terminal_date`                                     string         COMMENT '提前终止日期',
  `paid_amount`                                       decimal(20,5)  COMMENT '已还金额',
  `paid_principal`                                    decimal(20,5)  COMMENT '已还本金',
  `paid_interest`                                     decimal(20,5)  COMMENT '已还利息',
  `paid_penalty`                                      decimal(20,5)  COMMENT '已还罚息',
  `paid_svc_fee`                                      decimal(20,5)  COMMENT '已还服务费',
  `paid_term_fee`                                     decimal(20,5)  COMMENT '已还手续费',
  `paid_mult`                                         decimal(20,5)  COMMENT '已还滞纳金',
  `remain_amount`                                     decimal(20,5)  COMMENT '剩余金额：本息费',
  `remain_principal`                                  decimal(20,5)  COMMENT '剩余本金',
  `remain_interest`                                   decimal(20,5)  COMMENT '剩余利息',
  `remain_svc_fee`                                    decimal(20,5)  COMMENT '剩余服务费',
  `remain_term_fee`                                   decimal(20,5)  COMMENT '剩余手续费',
  `remain_othAmounts`                                 decimal(20,5)  COMMENT '剩余其他费用',
  `overdue_principal`                                 decimal(20,5)  COMMENT '逾期本金',
  `overdue_interest`                                  decimal(20,5)  COMMENT '逾期利息',
  `overdue_svc_fee`                                   decimal(20,5)  COMMENT '逾期服务费',
  `overdue_term_fee`                                  decimal(20,5)  COMMENT '逾期手续费',
  `overdue_penalty`                                   decimal(20,5)  COMMENT '逾期罚息',
  `overdue_mult_amt`                                  decimal(20,5)  COMMENT '逾期滞纳金',
  `overdue_date_start`                                string         COMMENT '逾期起始日期',
  `overdue_days`                                      decimal(5,0)   COMMENT '逾期天数',
  `overdue_date`                                      string         COMMENT '逾期日期',
  `collect_out_date`                                  string         COMMENT '出催日期',
  `dpd_days_max`                                      decimal(4,0)   COMMENT '历史最大DPD天数',
  `overdue_term`                                      decimal(3,0)   COMMENT '当前逾期期数',
  `overdue_terms_count`                               decimal(3,0)   COMMENT '累计逾期期数',
  `overdue_terms_max`                                 decimal(3,0)   COMMENT '历史单次最长逾期期数',
  `overdue_principal_accumulate`                      decimal(20,5)  COMMENT '累计逾期本金',
  `overdue_principal_max`                             decimal(20,5)  COMMENT '历史最大逾期本金',
  `create_time`                                       string         COMMENT '创建时间',
  `update_time`                                       string         COMMENT '更新时间'
) COMMENT '借据信息增量表'
PARTITIONED BY (`biz_date` string COMMENT '增量日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 借据表现表（做拉链表）
-- 业务主键 due_bill_no
-- 按照 is_settled 分区
-- DROP TABLE IF EXISTS `ods${db_suffix}.loan_info`;
CREATE TABLE IF NOT EXISTS `ods${db_suffix}.loan_info`(
  `due_bill_no`                                       string         COMMENT '借据编号',
  `apply_no`                                          string         COMMENT '进件编号',
  `loan_active_date`                                  string         COMMENT '放款日期',
  `loan_init_principal`                               decimal(20,5)  COMMENT '贷款本金',
  `account_age`                                       decimal(3,0)   COMMENT '账龄',
  `loan_init_term`                                    decimal(3,0)   COMMENT '贷款期数（3、6、9等）',
  `loan_term`                                         decimal(3,0)   COMMENT '当前期数（按应还日算）',
  `should_repay_date`                                 string         COMMENT '应还日期',
  `loan_term_repaid`                                  decimal(3,0)   COMMENT '已还期数',
  `loan_term_remain`                                  decimal(3,0)   COMMENT '剩余期数',
  `loan_init_interest`                                decimal(20,5)  COMMENT '贷款利息',
  `loan_init_term_fee`                                decimal(20,5)  COMMENT '贷款手续费',
  `loan_init_svc_fee`                                 decimal(20,5)  COMMENT '贷款服务费',
  `loan_status`                                       string         COMMENT '借据状态（英文原值）（N：正常，O：逾期，F：已还清）',
  `loan_status_cn`                                    string         COMMENT '借据状态（汉语解释）',
  `loan_out_reason`                                   string         COMMENT '借据终止原因（P：提前还款，M：银行业务人员手工终止（manual），D：逾期自动终止（delinquency），R：锁定码终止（Refund），V：持卡人手动终止，C：理赔终止，T：退货终止，U：重组结清终止，F：强制结清终止，B：免息转分期）',
  `paid_out_type`                                     string         COMMENT '结清类型（英文原值）（BANK_REF：退票结清，BUY_BACK：资产回购，CAPITAL_VERI：资产核销，DISPOSAL：处置结束，NORMAL_SETTLE：正常结清，OVER_COMP：逾期代偿，OVERDUE_SETTLE：逾期结清，PRE_SETTLE：提前结清，REDEMPTION：赎回，REFUND：退车，REFUND_SETTLEMENT：退票结清）',
  `paid_out_type_cn`                                  string         COMMENT '结清类型（汉语解释）',
  `paid_out_date`                                     string         COMMENT '还款日期',
  `terminal_date`                                     string         COMMENT '提前终止日期',
  `paid_amount`                                       decimal(20,5)  COMMENT '已还金额',
  `paid_principal`                                    decimal(20,5)  COMMENT '已还本金',
  `paid_interest`                                     decimal(20,5)  COMMENT '已还利息',
  `paid_penalty`                                      decimal(20,5)  COMMENT '已还罚息',
  `paid_svc_fee`                                      decimal(20,5)  COMMENT '已还服务费',
  `paid_term_fee`                                     decimal(20,5)  COMMENT '已还手续费',
  `paid_mult`                                         decimal(20,5)  COMMENT '已还滞纳金',
  `remain_amount`                                     decimal(20,5)  COMMENT '剩余金额：本息费',
  `remain_principal`                                  decimal(20,5)  COMMENT '剩余本金',
  `remain_interest`                                   decimal(20,5)  COMMENT '剩余利息',
  `remain_svc_fee`                                    decimal(20,5)  COMMENT '剩余服务费',
  `remain_term_fee`                                   decimal(20,5)  COMMENT '剩余手续费',
  `remain_othAmounts`                                 decimal(20,5)  COMMENT '剩余其他费用',
  `overdue_principal`                                 decimal(20,5)  COMMENT '逾期本金',
  `overdue_interest`                                  decimal(20,5)  COMMENT '逾期利息',
  `overdue_svc_fee`                                   decimal(20,5)  COMMENT '逾期服务费',
  `overdue_term_fee`                                  decimal(20,5)  COMMENT '逾期手续费',
  `overdue_penalty`                                   decimal(20,5)  COMMENT '逾期罚息',
  `overdue_mult_amt`                                  decimal(20,5)  COMMENT '逾期滞纳金',
  `overdue_date_first`                                string         COMMENT '首次逾期日期',
  `overdue_date_start`                                string         COMMENT '逾期起始日期',
  `overdue_days`                                      decimal(5,0)   COMMENT '逾期天数',
  `overdue_date`                                      string         COMMENT '逾期日期',
  `dpd_begin_date`                                    string         COMMENT 'DPD起始日期',
  `dpd_days`                                          decimal(4,0)   COMMENT 'DPD天数',
  `dpd_days_count`                                    decimal(4,0)   COMMENT '累计DPD天数',
  `dpd_days_max`                                      decimal(4,0)   COMMENT '历史最大DPD天数',
  `collect_out_date`                                  string         COMMENT '出催日期',
  `overdue_term`                                      decimal(3,0)   COMMENT '当前逾期期数',
  `overdue_terms_count`                               decimal(3,0)   COMMENT '累计逾期期数',
  `overdue_terms_max`                                 decimal(3,0)   COMMENT '历史单次最长逾期期数',
  `overdue_principal_accumulate`                      decimal(20,5)  COMMENT '累计逾期本金',
  `overdue_principal_max`                             decimal(20,5)  COMMENT '历史最大逾期本金',
  `s_d_date`                                          string         COMMENT 'ods层起始日期',
  `e_d_date`                                          string         COMMENT 'ods层结束日期',
  `effective_time`                                    timestamp      COMMENT '生效日期',
  `expire_time`                                       timestamp      COMMENT '失效日期'
) COMMENT '借据信息表'
PARTITIONED BY (`is_settled` string COMMENT '是否已结清',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 还款计划增量表
-- DROP TABLE IF EXISTS `ods${db_suffix}.repay_schedule_inter`;
CREATE TABLE IF NOT EXISTS `ods${db_suffix}.repay_schedule_inter`(
  `due_bill_no`                                       string         COMMENT '借据编号',
  `loan_active_date`                                  string         COMMENT '放款日期',
  `loan_init_principal`                               decimal(20,5)  COMMENT '贷款本金',
  `loan_init_term`                                    decimal(3,0)   COMMENT '贷款期数（3、6、9等）',
  `loan_term`                                         decimal(3,0)   COMMENT '当前期数',
  `start_interest_date`                               string         COMMENT '起息日期',
  `curr_bal`                                          decimal(20,5)  COMMENT '当前余额（当前欠款）',
  `should_repay_date`                                 string         COMMENT '应还日期',
  `should_repay_date_history`                         string         COMMENT '修改前的应还日期',
  `grace_date`                                        string         COMMENT '宽限日期',
  `should_repay_amount`                               decimal(20,5)  COMMENT '应还金额',
  `should_repay_principal`                            decimal(20,5)  COMMENT '应还本金',
  `should_repay_interest`                             decimal(20,5)  COMMENT '应还利息',
  `should_repay_term_fee`                             decimal(20,5)  COMMENT '应还手续费',
  `should_repay_svc_fee`                              decimal(20,5)  COMMENT '应还服务费',
  `should_repay_penalty`                              decimal(20,5)  COMMENT '应还罚息',
  `should_repay_mult_amt`                             decimal(20,5)  COMMENT '应还滞纳金',
  `should_repay_penalty_acru`                         decimal(20,5)  COMMENT '应还累计罚息金额',
  `schedule_status`                                   string         COMMENT '还款计划状态（英文原值）（N：正常，O：逾期，F：已还清）',
  `schedule_status_cn`                                string         COMMENT '还款计划状态（汉语解释）',
  `repay_status`                                      string         COMMENT '还款状态（1：入池前已还，2：入池前未还）',
  `paid_out_date`                                     string         COMMENT '还清日期',
  `paid_out_type`                                     string         COMMENT '结清类型（英文原值）（BANK_REF：退票结清，BUY_BACK：资产回购，CAPITAL_VERI：资产核销，DISPOSAL：处置结束，NORMAL_SETTLE：正常结清，OVER_COMP：逾期代偿，OVERDUE_SETTLE：逾期结清，PRE_SETTLE：提前结清，REDEMPTION：赎回，REFUND：退车，REFUND_SETTLEMENT：退票结清）',
  `paid_out_type_cn`                                  string         COMMENT '结清类型（汉语解释）',
  `paid_amount`                                       decimal(20,5)  COMMENT '已还金额',
  `paid_principal`                                    decimal(20,5)  COMMENT '已还本金',
  `paid_interest`                                     decimal(20,5)  COMMENT '已还利息',
  `paid_term_fee`                                     decimal(20,5)  COMMENT '已还手续费',
  `paid_svc_fee`                                      decimal(20,5)  COMMENT '已还服务费',
  `paid_penalty`                                      decimal(20,5)  COMMENT '已还罚息',
  `paid_mult`                                         decimal(20,5)  COMMENT '已还滞纳金',
  `reduce_amount`                                     decimal(20,5)  COMMENT '减免金额',
  `reduce_principal`                                  decimal(20,5)  COMMENT '减免本金',
  `reduce_interest`                                   decimal(20,5)  COMMENT '减免利息',
  `reduce_term_fee`                                   decimal(20,5)  COMMENT '减免手续费',
  `reduce_svc_fee`                                    decimal(20,5)  COMMENT '减免服务费',
  `reduce_penalty`                                    decimal(20,5)  COMMENT '减免罚息',
  `reduce_mult_amt`                                   decimal(20,5)  COMMENT '减免滞纳金',
  `effective_date`                                    string         COMMENT '生效日期'
) COMMENT '还款计划增量表'
PARTITIONED BY (`biz_date` string COMMENT '增量日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 还款计划表
-- DROP TABLE IF EXISTS `ods${db_suffix}.repay_schedule`;
CREATE TABLE IF NOT EXISTS `ods${db_suffix}.repay_schedule`(
  `due_bill_no`                                       string         COMMENT '借据编号',
  `loan_active_date`                                  string         COMMENT '放款日期',
  `loan_init_principal`                               decimal(20,5)  COMMENT '贷款本金',
  `loan_init_term`                                    decimal(3,0)   COMMENT '贷款期数（3、6、9等）',
  `loan_term`                                         decimal(3,0)   COMMENT '当前期数',
  `start_interest_date`                               string         COMMENT '起息日期',
  `curr_bal`                                          decimal(20,5)  COMMENT '当前余额（当前欠款）',
  `should_repay_date`                                 string         COMMENT '应还日期',
  `should_repay_date_history`                         string         COMMENT '修改前的应还日期',
  `grace_date`                                        string         COMMENT '宽限日期',
  `should_repay_amount`                               decimal(20,5)  COMMENT '应还金额',
  `should_repay_principal`                            decimal(20,5)  COMMENT '应还本金',
  `should_repay_interest`                             decimal(20,5)  COMMENT '应还利息',
  `should_repay_term_fee`                             decimal(20,5)  COMMENT '应还手续费',
  `should_repay_svc_fee`                              decimal(20,5)  COMMENT '应还服务费',
  `should_repay_penalty`                              decimal(20,5)  COMMENT '应还罚息',
  `should_repay_mult_amt`                             decimal(20,5)  COMMENT '应还滞纳金',
  `should_repay_penalty_acru`                         decimal(20,5)  COMMENT '应还累计罚息金额',
  `schedule_status`                                   string         COMMENT '还款计划状态（英文原值）（N：正常，O：逾期，F：已还清）',
  `schedule_status_cn`                                string         COMMENT '还款计划状态（汉语解释）',
  `repay_status`                                      string         COMMENT '还款状态（1：入池前已还，2：入池前未还）',
  `paid_out_date`                                     string         COMMENT '还清日期',
  `paid_out_type`                                     string         COMMENT '结清类型（英文原值）（BANK_REF：退票结清，BUY_BACK：资产回购，CAPITAL_VERI：资产核销，DISPOSAL：处置结束，NORMAL_SETTLE：正常结清，OVER_COMP：逾期代偿，OVERDUE_SETTLE：逾期结清，PRE_SETTLE：提前结清，REDEMPTION：赎回，REFUND：退车，REFUND_SETTLEMENT：退票结清）',
  `paid_out_type_cn`                                  string         COMMENT '结清类型（汉语解释）',
  `paid_amount`                                       decimal(20,5)  COMMENT '已还金额',
  `paid_principal`                                    decimal(20,5)  COMMENT '已还本金',
  `paid_interest`                                     decimal(20,5)  COMMENT '已还利息',
  `paid_term_fee`                                     decimal(20,5)  COMMENT '已还手续费',
  `paid_svc_fee`                                      decimal(20,5)  COMMENT '已还服务费',
  `paid_penalty`                                      decimal(20,5)  COMMENT '已还罚息',
  `paid_mult`                                         decimal(20,5)  COMMENT '已还滞纳金',
  `reduce_amount`                                     decimal(20,5)  COMMENT '减免金额',
  `reduce_principal`                                  decimal(20,5)  COMMENT '减免本金',
  `reduce_interest`                                   decimal(20,5)  COMMENT '减免利息',
  `reduce_term_fee`                                   decimal(20,5)  COMMENT '减免手续费',
  `reduce_svc_fee`                                    decimal(20,5)  COMMENT '减免服务费',
  `reduce_penalty`                                    decimal(20,5)  COMMENT '减免罚息',
  `reduce_mult_amt`                                   decimal(20,5)  COMMENT '减免滞纳金',
  `effective_date`                                    string         COMMENT '生效日期',
  `s_d_date`                                          string         COMMENT '数据生效日期',
  `e_d_date`                                          string         COMMENT '数据失效日期'
) COMMENT '还款计划表'
PARTITIONED BY (`is_settled` string COMMENT '是否已结清',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 订单流水表
-- 实还明细表
-- 数据库主键 payment_id
-- 业务主键 cust_id,due_bill_no,order_id
-- 按照 biz_date 分区
-- DROP TABLE IF EXISTS `ods${db_suffix}.repay_detail`;
CREATE TABLE IF NOT EXISTS `ods${db_suffix}.repay_detail`(
  `due_bill_no`                                       string         COMMENT '借据号',
  `repay_term`                                        decimal(3,0)   COMMENT '实还期数',
  `order_id`                                          string         COMMENT '订单号',
  `repay_type`                                        string         COMMENT '还款类型（英文原值）（NORMAL：正常还款，PRE：提前还款，OVERDUE：逾期还款，PRE_SETTLE：用户提前结清，COMP：代偿还款，RECOVER：逾期追偿还款，REFUND：退票，BUYBACK：回购，REDUCE：减免，DISCOUNT：商户贴息，RECEIVABLE：退款回款，INTERNAL_REFUND：退款退息，RECOVERY_REPAYMENT：降额还本，INTEREST_REBATE：降额退息）',
  `repay_type_cn`                                     string         COMMENT '还款类型（汉语解释）',
  `payment_id`                                        string         COMMENT '实还流水号',
  `txn_time`                                          timestamp      COMMENT '交易时间',
  `post_time`                                         timestamp      COMMENT '入账时间',
  `bnp_type`                                          string         COMMENT '还款成分（英文原值）（Pricinpal：本金，Interest：利息，Penalty：罚息，TXNFee：交易费，TERMFee：手续费，SVCFee：服务费，LatePaymentCharge：滞纳金，RepayFee：实还费用，EarlyRepayFee：提前还款手续费，OtherFee：其它相关费用，CardFee：年费，Mulct：罚金，Compensation：赔偿金，Damages：违约金，Compound：复利，OverLimitFee：超限费，NSFCharge：资金不足罚金，LifeInsuFee：寿险计划包费）',
  `bnp_type_cn`                                       string         COMMENT '还款成分（汉语解释）',
  `repay_amount`                                      decimal(20,5)  COMMENT '还款金额',
  `batch_date`                                        string         COMMENT '批量日期',
  `create_time`                                       timestamp      COMMENT '创建时间',
  `update_time`                                       timestamp      COMMENT '更新时间'
) COMMENT '实还明细表'
PARTITIONED BY (`biz_date` string COMMENT '实还日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;


-- 数据库主键 order_id
-- 按照 biz_date 分区
-- DROP TABLE IF EXISTS `ods${db_suffix}.order_info`;
CREATE TABLE IF NOT EXISTS `ods${db_suffix}.order_info`(
  `order_id`                                          string         COMMENT '订单编号',
  `apply_no`                                          string         COMMENT '申请件编号',
  `due_bill_no`                                       string         COMMENT '借据号',
  `term`                                              decimal(3,0)   COMMENT '处理期数',
  `pay_channel`                                       string         COMMENT '支付渠道',
  `command_type`                                      string         COMMENT '支付指令类型（SPA：单笔代付，SDB：单笔代扣，QSP：单笔代付查询，QSD：单笔代扣查询，BDB：批量代扣，BDA：批量代付）',
  `order_status`                                      string         COMMENT '订单状态（C：已提交，P：待提交，Q：待审批，W：处理中，S：已完成，V：已失效，E：失败，T：超时，R：已重提，G：拆分处理中，D：拆分已完成，B：撤销，X：已受理待入账）',
  `repay_way`                                         string         COMMENT '还款方式（ONLINE：线上，OFFLINE：线下）',
  `txn_amt`                                           decimal(20,5)  COMMENT '交易金额',
  `success_amt`                                       decimal(20,5)  COMMENT '成功金额',
  `currency`                                          string         COMMENT '币种',
  `business_date`                                     string         COMMENT '业务日期',
  `loan_usage`                                        string         COMMENT '贷款用途（B：回购，C：差额补足，D：代偿，F：追偿代扣，H：处置回收，I：强制结清扣款，L：放款申请，M：预约提前结清扣款，N：提前还当期，O：逾期扣款，P：打款通知，R：退货，T：退票，W：赎回结清，X：账务调整，Z：委托转付）',
  `purpose`                                           string         COMMENT '支付用途',
  `bank_trade_act_no`                                 string         COMMENT '银行付款账号',
  `bank_trade_act_name`                               string         COMMENT '银行付款账户名称',
  `bank_trade_act_phone`                              string         COMMENT '银行预留手机号',
  `txn_time`                                          timestamp      COMMENT '交易时间',
  `txn_date`                                          string         COMMENT '交易日期',
  `create_time`                                       timestamp      COMMENT '创建时间',
  `update_time`                                       timestamp      COMMENT '更新时间'
) COMMENT '订单流水表'
PARTITIONED BY (`biz_date` string COMMENT '交易日期',`product_id` string COMMENT '产品编号')
STORED AS PARQUET;







-- DROP VIEW IF EXISTS `ods.customer_info_abs`;
CREATE VIEW IF NOT EXISTS `ods.customer_info_abs`(
  `due_bill_no`                        COMMENT '借据号',
  `cust_id`                            COMMENT '客户编号',
  `user_hash_no`                       COMMENT '用户编号',
  `outer_cust_id`                      COMMENT '外部客户号',
  `idcard_type`                        COMMENT '证件类型（身份证等）',
  `idcard_no`                          COMMENT '证件号码',
  `name`                               COMMENT '客户姓名',
  `mobie`                              COMMENT '客户电话',
  `card_phone`                         COMMENT '客户银行卡绑定手机号',
  `sex`                                COMMENT '客户性别（男、女）',
  `birthday`                           COMMENT '出生日期',
  `age`                                COMMENT '客户年龄',
  `marriage_status`                    COMMENT '婚姻状态',
  `education`                          COMMENT '学历',
  `education_ws`                       COMMENT '学历级别（硕士及以上、大学本科、大专及以下、未知）',
  `idcard_address`                     COMMENT '身份证地址',
  `idcard_area`                        COMMENT '身份证大区（东北地区、华北地区、西北地区、西南地区、华南地区、华东地区、华中地区、港澳台地区）',
  `idcard_province`                    COMMENT '身份证省级（省/直辖市/特别行政区）',
  `idcard_city`                        COMMENT '身份证地级（城市）',
  `idcard_county`                      COMMENT '身份证县级（区县）',
  `idcard_township`                    COMMENT '身份证乡级（乡/镇/街）（预留）',
  `resident_address`                   COMMENT '常住地地址',
  `resident_area`                      COMMENT '常住地大区（东北地区、华北地区、西北地区、西南地区、华南地区、华东地区、华中地区、港澳台地区）',
  `resident_province`                  COMMENT '常住地省级（省/直辖市/特别行政区）',
  `resident_city`                      COMMENT '常住地地级（城市）',
  `resident_county`                    COMMENT '常住地县级（区县）',
  `resident_township`                  COMMENT '常住地乡级（乡/镇/街）（预留）',
  `job_type`                           COMMENT '工作类型',
  `job_year`                           COMMENT '工作年限',
  `income_month`                       COMMENT '月收入',
  `income_year`                        COMMENT '年收入',
  `cutomer_type`                       COMMENT '客戶类型（个人或企业）',
  `cust_rating`                        COMMENT '内部信用等级',
  `project_id`                         COMMENT '项目编号'
) COMMENT '客户信息表' AS select
  due_bill_no,
  cust_id,
  user_hash_no,
  outer_cust_id,
  idcard_type,
  idcard_no,
  name,
  mobie,
  card_phone,
  sex,
  birthday,
  age,
  marriage_status,
  education,
  education_ws,
  idcard_address,
  idcard_area,
  idcard_province,
  idcard_city,
  idcard_county,
  idcard_township,
  resident_address,
  resident_area,
  resident_province,
  resident_city,
  resident_county,
  resident_township,
  job_type,
  job_year,
  income_month,
  income_year,
  cutomer_type,
  cust_rating,
  abs_project_id as project_id
from ods.customer_info as t1
join dim_new.biz_conf as t2
on t1.product_id = t2.product_id;


-- DROP VIEW IF EXISTS `ods.guaranty_info_abs`;
CREATE VIEW IF NOT EXISTS `ods.guaranty_info_abs`(
  `due_bill_no`                        COMMENT '借据号',
  `guaranty_code`                      COMMENT '抵押物编号',
  `guaranty_handling_status`           COMMENT '抵押办理状态 预定义字段：办理中 办理完成 尚未办理',
  `guaranty_alignment`                 COMMENT '抵押顺位 1-第一顺位 2-第二顺位 9-其他',
  `car_property`                       COMMENT '车辆性质 预定义字段：非融资车分期非 融资车抵贷 融资租赁车分期 融资租赁车抵贷',
  `financing_type`                     COMMENT '融资方式 预定义字段：正租 反租',
  `guarantee_type`                     COMMENT '担保方式 预定义字段：质押担保，信用担保，保证担保，抵押担保',
  `pawn_value`                         COMMENT '评估价格(元)',
  `car_sales_price`                    COMMENT '车辆销售价格(元)',
  `car_new_price`                      COMMENT '新车指导价(元)',
  `total_investment`                   COMMENT '投资总额(元)',
  `purchase_tax_amouts`                COMMENT '购置税金额(元)',
  `insurance_type`                     COMMENT '保险种类  交强险 第三者责任险 盗抢险 车损险 不计免赔 其他',
  `car_insurance_premium`              COMMENT '汽车保险总费用',
  `total_poundage`                     COMMENT '手续总费用(元)',
  `cumulative_car_transfer_number`     COMMENT '累计车辆过户次数',
  `one_year_car_transfer_number`       COMMENT '一年内车辆过户次数',
  `liability_insurance_cost1`          COMMENT '责信保费用1',
  `liability_insurance_cost2`          COMMENT '责信保费用2',
  `car_type`                           COMMENT '车类型 预定义字段：新车 二手车',
  `frame_num`                          COMMENT '车架号',
  `engine_num`                         COMMENT '发动机号',
  `gps_code`                           COMMENT 'GPS编号',
  `gps_cost`                           COMMENT 'GPS费用',
  `license_num`                        COMMENT '车牌号码',
  `car_brand`                          COMMENT '车辆品牌',
  `car_system`                         COMMENT '车系',
  `car_model`                          COMMENT '车型',
  `car_age`                            COMMENT '车龄',
  `car_energy_type`                    COMMENT '车辆能源类型 预定义字段： 混合动力 纯电 非新能源车',
  `production_date`                    COMMENT '生产日期',
  `mileage`                            COMMENT '里程数',
  `register_date`                      COMMENT '注册日期',
  `buy_car_address`                    COMMENT '车辆购买地',
  `car_colour`                         COMMENT '车辆颜色',
  `create_time`                        COMMENT '创建时间',
  `update_time`                        COMMENT '更新时间',
  `project_id`                         COMMENT '项目编号'
) COMMENT '抵押物（车）信息表' AS select
  due_bill_no,
  guaranty_code,
  guaranty_handling_status,
  guaranty_alignment,
  car_property,
  financing_type,
  guarantee_type,
  pawn_value,
  car_sales_price,
  car_new_price,
  total_investment,
  purchase_tax_amouts,
  insurance_type,
  car_insurance_premium,
  total_poundage,
  cumulative_car_transfer_number,
  one_year_car_transfer_number,
  liability_insurance_cost1,
  liability_insurance_cost2,
  car_type,
  frame_num,
  engine_num,
  gps_code,
  gps_cost,
  license_num,
  car_brand,
  car_system,
  car_model,
  car_age,
  car_energy_type,
  production_date,
  mileage,
  register_date,
  buy_car_address,
  car_colour,
  create_time,
  update_time,
  abs_project_id as project_id
from ods.guaranty_info as t1
join dim_new.biz_conf as t2
on t1.product_id = t2.product_id;


-- DROP VIEW IF EXISTS `ods.t_enterprise_info_abs`;
CREATE VIEW IF NOT EXISTS `ods.enterprise_info_abs` (
  `due_bill_no`                        COMMENT '资产借据号',
  `contract_role`                      COMMENT '合同角色（0：主借款企业，1：共同借款企业，2：担保企业，3：无）',
  `enterprise_name`                    COMMENT '企业，名称',
  `business_number`                    COMMENT '工商注册号',
  `organizate_code`                    COMMENT '组织机构代码',
  `taxpayer_number`                    COMMENT '纳税人识别号',
  `unified_credit_code`                COMMENT '统一信用代码',
  `registered_address`                 COMMENT '注册地址',
  `loan_type`                          COMMENT '借款方类型',
  `industry`                           COMMENT '企业行业',
  `legal_person_name`                  COMMENT '法人姓名',
  `id_type`                            COMMENT '法人证件类型',
  `id_no`                              COMMENT '法人证件号',
  `legal_person_phone`                 COMMENT '法人手机号码',
  `phone`                              COMMENT '企业联系电话',
  `operate_years`                      COMMENT '企业运营年限',
  `is_linked`                          COMMENT '是否挂靠企业',
  `province`                           COMMENT '企业省份',
  `create_time`                        COMMENT '创建时间',
  `update_time`                        COMMENT '更新时间',
  `project_id`                         COMMENT '项目编号'
) COMMENT '企业信息表' AS select
  due_bill_no,
  contract_role,
  enterprise_name,
  business_number,
  organizate_code,
  taxpayer_number,
  unified_credit_code,
  registered_address,
  loan_type,
  industry,
  legal_person_name,
  id_type,
  id_no,
  legal_person_phone,
  phone,
  operate_years,
  is_linked,
  province,
  create_time,
  update_time,
  abs_project_id as project_id
from ods.enterprise_info as t1
join dim_new.biz_conf as t2
on t1.product_id = t2.product_id;


-- DROP VIEW IF EXISTS `ods.linkman_info_abs`;
CREATE VIEW IF NOT EXISTS `ods.linkman_info_abs`(
  `cust_id`                            COMMENT '客户编号',
  `user_hash_no`                       COMMENT '用户编号',
  `due_bill_no`                        COMMENT '借据编号',
  `linkman_id`                         COMMENT '联系人编号',
  `relational_type`                    COMMENT '关联人类型（英文原值）',
  `relational_type_cn`                 COMMENT '关联人类型（汉语解释）',
  `relationship`                       COMMENT '联系人关系（英文原值）（如：1：父母、2：配偶、3：子女、4：兄弟姐妹、5：亲属、6：同事、7：朋友、8：其他）',
  `relationship_cn`                    COMMENT '联系人关系（汉语解释）',
  `relation_idcard_type`               COMMENT '联系人证件类型',
  `relation_idcard_no`                 COMMENT '联系人证件号码',
  `relation_birthday`                  COMMENT '联系人出生日期',
  `relation_name`                      COMMENT '联系人姓名',
  `relation_sex`                       COMMENT '联系人性别',
  `relation_mobile`                    COMMENT '联系人电话',
  `relation_address`                   COMMENT '联系人地址',
  `relation_province`                  COMMENT '联系人省份',
  `relation_city`                      COMMENT '联系人城市',
  `relation_county`                    COMMENT '联系人区县',
  `corp_type`                          COMMENT '工作类型',
  `corp_name`                          COMMENT '公司名称',
  `corp_teleph_nbr`                    COMMENT '公司电话',
  `corp_fax`                           COMMENT '公司传真',
  `corp_position`                      COMMENT '公司职务',
  `deal_date`                          COMMENT '业务时间',
  `effective_time`                     COMMENT '生效时间',
  `expire_time`                        COMMENT '失效时间',
  `project_id`                         COMMENT '项目编号'
) COMMENT '联系人信息表' AS select
  cust_id,
  user_hash_no,
  due_bill_no,
  linkman_id,
  relational_type,
  relational_type_cn,
  relationship,
  relationship_cn,
  relation_idcard_type,
  relation_idcard_no,
  relation_birthday,
  relation_name,
  relation_sex,
  relation_mobile,
  relation_address,
  relation_province,
  relation_city,
  relation_county,
  corp_type,
  corp_name,
  corp_teleph_nbr,
  corp_fax,
  corp_position,
  deal_date,
  effective_time,
  expire_time,
  abs_project_id as project_id
from ods.linkman_info as t1
join dim_new.biz_conf as t2
on t1.product_id = t2.product_id;


-- DROP VIEW IF EXISTS `ods.loan_lending_abs`;
CREATE VIEW IF NOT EXISTS `ods.loan_lending_abs`(
  `apply_no`                           COMMENT '进件编号',
  `contract_no`                        COMMENT '合同编号',
  `contract_term`                      COMMENT '合同期限',
  `due_bill_no`                        COMMENT '借据编号',
  `guarantee_type`                     COMMENT '担保方式',
  `loan_usage`                         COMMENT '贷款用途',
  `loan_issue_date`                    COMMENT '合同开始日期',
  `loan_expiry_date`                   COMMENT '合同结束日期',
  `loan_active_date`                   COMMENT '放款日期',
  `loan_expire_date`                   COMMENT '贷款到期日期',
  `cycle_day`                          COMMENT '账单日',
  `loan_type`                          COMMENT '分期类型（英文原值）（MCEP：等额本金，MCEI：等额本息，R：消费转分期，C：现金分期，B：账单分期，P：POS分期，M：大额分期（专项分期），MCAT：随借随还，STAIR：阶梯还款）',
  `loan_type_cn`                       COMMENT '分期类型（汉语解释）',
  `contract_daily_interest_rate_basis` COMMENT '日利率计算基础',
  `loan_init_term`                     COMMENT '贷款期数（3、6、9等）',
  `loan_init_principal`                COMMENT '贷款本金',
  `interest_rate_type`                 COMMENT '利率类型',
  `loan_init_interest_rate`            COMMENT '利息利率',
  `loan_init_term_fee_rate`            COMMENT '手续费费率',
  `loan_init_svc_fee_rate`             COMMENT '服务费费率',
  `loan_init_penalty_rate`             COMMENT '罚息利率',
  `biz_date`                           COMMENT '放款日期',
  `project_id`                         COMMENT '项目编号'
) COMMENT '放款表' AS select
  apply_no,
  contract_no,
  contract_term,
  due_bill_no,
  guarantee_type,
  loan_usage,
  loan_issue_date,
  loan_expiry_date,
  loan_active_date,
  loan_expire_date,
  cycle_day,
  loan_type,
  loan_type_cn,
  contract_daily_interest_rate_basis,
  loan_init_term,
  loan_init_principal,
  interest_rate_type,
  loan_init_interest_rate,
  loan_init_term_fee_rate,
  loan_init_svc_fee_rate,
  loan_init_penalty_rate,
  biz_date,
  abs_project_id as project_id
from ods.loan_lending as t1
join dim_new.biz_conf as t2
on t1.product_id = t2.product_id;


-- DROP VIEW IF EXISTS `ods.loan_info_abs`;
CREATE VIEW IF NOT EXISTS `ods.loan_info_abs`(
  `due_bill_no`                        COMMENT '借据编号',
  `apply_no`                           COMMENT '进件编号',
  `loan_active_date`                   COMMENT '放款日期',
  `loan_init_principal`                COMMENT '贷款本金',
  `account_age`                        COMMENT '账龄',
  `loan_init_term`                     COMMENT '贷款期数（3、6、9等）',
  `loan_term`                          COMMENT '当前期数（按应还日算）',
  `should_repay_date`                  COMMENT '应还日期',
  `loan_term_repaid`                   COMMENT '已还期数',
  `loan_term_remain`                   COMMENT '剩余期数',
  `loan_init_interest`                 COMMENT '贷款利息',
  `loan_init_term_fee`                 COMMENT '贷款手续费',
  `loan_init_svc_fee`                  COMMENT '贷款服务费',
  `loan_status`                        COMMENT '借据状态（英文原值）（N：正常，O：逾期，F：已还清）',
  `loan_status_cn`                     COMMENT '借据状态（汉语解释）',
  `loan_out_reason`                    COMMENT '借据终止原因（P：提前还款，M：银行业务人员手工终止（manual），D：逾期自动终止（delinquency），R：锁定码终止（Refund），V：持卡人手动终止，C：理赔终止，T：退货终止，U：重组结清终止，F：强制结清终止，B：免息转分期）',
  `paid_out_type`                      COMMENT '结清类型（英文原值）（BANK_REF：退票结清，BUY_BACK：资产回购，CAPITAL_VERI：资产核销，DISPOSAL：处置结束，NORMAL_SETTLE：正常结清，OVER_COMP：逾期代偿，OVERDUE_SETTLE：逾期结清，PRE_SETTLE：提前结清，REDEMPTION：赎回，REFUND：退车，REFUND_SETTLEMENT：退票结清）',
  `paid_out_type_cn`                   COMMENT '结清类型（汉语解释）',
  `paid_out_date`                      COMMENT '还款日期',
  `terminal_date`                      COMMENT '提前终止日期',
  `paid_amount`                        COMMENT '已还金额',
  `paid_principal`                     COMMENT '已还本金',
  `paid_interest`                      COMMENT '已还利息',
  `paid_penalty`                       COMMENT '已还罚息',
  `paid_svc_fee`                       COMMENT '已还服务费',
  `paid_term_fee`                      COMMENT '已还手续费',
  `paid_mult`                          COMMENT '已还滞纳金',
  `remain_amount`                      COMMENT '剩余金额：本息费',
  `remain_principal`                   COMMENT '剩余本金',
  `remain_interest`                    COMMENT '剩余利息',
  `remain_svc_fee`                     COMMENT '剩余服务费',
  `remain_term_fee`                    COMMENT '剩余手续费',
  `remain_othAmounts`                  COMMENT '剩余其他费用',
  `overdue_principal`                  COMMENT '逾期本金',
  `overdue_interest`                   COMMENT '逾期利息',
  `overdue_svc_fee`                    COMMENT '逾期服务费',
  `overdue_term_fee`                   COMMENT '逾期手续费',
  `overdue_penalty`                    COMMENT '逾期罚息',
  `overdue_mult_amt`                   COMMENT '逾期滞纳金',
  `overdue_date_first`                 COMMENT '首次逾期日期',
  `overdue_date_start`                 COMMENT '逾期起始日期',
  `overdue_days`                       COMMENT '逾期天数',
  `overdue_date`                       COMMENT '逾期日期',
  `dpd_begin_date`                     COMMENT 'DPD起始日期',
  `dpd_days`                           COMMENT 'DPD天数',
  `dpd_days_count`                     COMMENT '累计DPD天数',
  `dpd_days_max`                       COMMENT '历史最大DPD天数',
  `collect_out_date`                   COMMENT '出催日期',
  `overdue_term`                       COMMENT '当前逾期期数',
  `overdue_terms_count`                COMMENT '累计逾期期数',
  `overdue_terms_max`                  COMMENT '历史单次最长逾期期数',
  `overdue_principal_accumul`          COMMENT '累计逾期本金',
  `overdue_principal_max`              COMMENT '历史最大逾期本金',
  `s_d_date`                           COMMENT 'ods层起始日期',
  `e_d_date`                           COMMENT 'ods层结束日期',
  `effective_time`                     COMMENT '生效日期',
  `expire_time`                        COMMENT '失效日期',
  `is_settled`                         COMMENT '是否已结清',
  `project_id`                         COMMENT '项目编号'
) COMMENT '借据信息表' AS select
  due_bill_no,
  apply_no,
  loan_active_date,
  loan_init_principal,
  account_age,
  loan_init_term,
  loan_term,
  should_repay_date,
  loan_term_repaid,
  loan_term_remain,
  loan_init_interest,
  loan_init_term_fee,
  loan_init_svc_fee,
  loan_status,
  loan_status_cn,
  loan_out_reason,
  paid_out_type,
  paid_out_type_cn,
  paid_out_date,
  terminal_date,
  paid_amount,
  paid_principal,
  paid_interest,
  paid_penalty,
  paid_svc_fee,
  paid_term_fee,
  paid_mult,
  remain_amount,
  remain_principal,
  remain_interest,
  remain_svc_fee,
  remain_term_fee,
  remain_othAmounts,
  overdue_principal,
  overdue_interest,
  overdue_svc_fee,
  overdue_term_fee,
  overdue_penalty,
  overdue_mult_amt,
  overdue_date_first,
  overdue_date_start,
  overdue_days,
  overdue_date,
  dpd_begin_date,
  dpd_days,
  dpd_days_count,
  dpd_days_max,
  collect_out_date,
  overdue_term,
  overdue_terms_count,
  overdue_terms_max,
  overdue_principal_accumulate,
  overdue_principal_max,
  s_d_date,
  if(paid_out_date is null,if(e_d_date = '3000-12-31',to_date(date_sub(current_timestamp(),1)),e_d_date),paid_out_date) as e_d_date,
  effective_time,
  expire_time,
  is_settled,
  abs_project_id as project_id
from ods.loan_info as t1
join dim_new.biz_conf as t2
on t1.product_id = t2.product_id;


-- 还款计划表（做拉链表）
-- 数据库主键 schedule_id
-- 业务主键 cust_id,due_bill_no
-- 按照 is_settled 分区
-- DROP VIEW IF EXISTS `ods.repay_schedule_abs`;
CREATE ViEW IF NOT EXISTS `ods.repay_schedule_abs`(
  `due_bill_no`                        COMMENT '借据编号',
  `loan_active_date`                   COMMENT '放款日期',
  `loan_init_principal`                COMMENT '贷款本金',
  `loan_init_term`                     COMMENT '贷款期数（3、6、9等）',
  `loan_term`                          COMMENT '当前期数',
  `start_interest_date`                COMMENT '起息日期',
  `curr_bal`                           COMMENT '当前余额（当前欠款）',
  `should_repay_date`                  COMMENT '应还日期',
  `should_repay_date_history`          COMMENT '修改前的应还日期',
  `grace_date`                         COMMENT '宽限日期',
  `should_repay_amount`                COMMENT '应还金额',
  `should_repay_principal`             COMMENT '应还本金',
  `should_repay_interest`              COMMENT '应还利息',
  `should_repay_term_fee`              COMMENT '应还手续费',
  `should_repay_svc_fee`               COMMENT '应还服务费',
  `should_repay_penalty`               COMMENT '应还罚息',
  `should_repay_mult_amt`              COMMENT '应还滞纳金',
  `should_repay_penalty_acru`          COMMENT '应还累计罚息金额',
  `schedule_status`                    COMMENT '还款计划状态（英文原值）（N：正常，O：逾期，F：已还清）',
  `schedule_status_cn`                 COMMENT '还款计划状态（汉语解释）',
  `repay_status`                       COMMENT '还款状态（1：入池前已还，2：入池前未还）',
  `paid_out_date`                      COMMENT '还清日期',
  `paid_out_type`                      COMMENT '结清类型（英文原值）（BANK_REF：退票结清，BUY_BACK：资产回购，CAPITAL_VERI：资产核销，DISPOSAL：处置结束，NORMAL_SETTLE：正常结清，OVER_COMP：逾期代偿，OVERDUE_SETTLE：逾期结清，PRE_SETTLE：提前结清，REDEMPTION：赎回，REFUND：退车，REFUND_SETTLEMENT：退票结清）',
  `paid_out_type_cn`                   COMMENT '结清类型（汉语解释）',
  `paid_amount`                        COMMENT '已还金额',
  `paid_principal`                     COMMENT '已还本金',
  `paid_interest`                      COMMENT '已还利息',
  `paid_term_fee`                      COMMENT '已还手续费',
  `paid_svc_fee`                       COMMENT '已还服务费',
  `paid_penalty`                       COMMENT '已还罚息',
  `paid_mult`                          COMMENT '已还滞纳金',
  `reduce_amount`                      COMMENT '减免金额',
  `reduce_principal`                   COMMENT '减免本金',
  `reduce_interest`                    COMMENT '减免利息',
  `reduce_term_fee`                    COMMENT '减免手续费',
  `reduce_svc_fee`                     COMMENT '减免服务费',
  `reduce_penalty`                     COMMENT '减免罚息',
  `reduce_mult_amt`                    COMMENT '减免滞纳金',
  `effective_date`                     COMMENT '生效日期',
  `s_d_date`                           COMMENT 'ods层起始日期',
  `e_d_date`                           COMMENT 'ods层结束日期',
  `effective_time`                     COMMENT '生效时间',
  `expire_time`                        COMMENT '失效时间',
  `is_settled`                         COMMENT '是否已结清',
  `project_id`                         COMMENT '项目编号'
) COMMENT '还款计划表' as select
  due_bill_no,
  loan_active_date,
  loan_init_principal,
  loan_init_term,
  loan_term,
  start_interest_date,
  curr_bal,
  should_repay_date,
  should_repay_date_history,
  grace_date,
  should_repay_amount,
  should_repay_principal,
  should_repay_interest,
  should_repay_term_fee,
  should_repay_svc_fee,
  should_repay_penalty,
  should_repay_mult_amt,
  should_repay_penalty_acru,
  schedule_status,
  schedule_status_cn,
  repay_status,
  paid_out_date,
  paid_out_type,
  paid_out_type_cn,
  paid_amount,
  paid_principal,
  paid_interest,
  paid_term_fee,
  paid_svc_fee,
  paid_penalty,
  paid_mult,
  reduce_amount,
  reduce_principal,
  reduce_interest,
  reduce_term_fee,
  reduce_svc_fee,
  reduce_penalty,
  reduce_mult_amt,
  effective_date,
  biz_date                    as s_d_date,
  nvl(lead(biz_date) over(partition by abs_project_id,due_bill_no,loan_term order by biz_date),'3000-12-31') as e_d_date,
  cast(biz_date as timestamp) as effective_time,
  cast(nvl(lead(biz_date) over(partition by abs_project_id,due_bill_no,loan_term order by biz_date),'3000-12-31') as timestamp) as expire_time,
  'no'                        as is_settled,
  abs_project_id              as project_id
from ods.repay_schedule_inter as t1
join dim_new.biz_conf as t2
on t1.product_id = t2.product_id
;


-- DROP VIEW IF EXISTS `ods.repay_detail_abs`;
CREATE VIEW IF NOT EXISTS `ods.repay_detail_abs`(
  `due_bill_no`                        COMMENT '借据号',
  `loan_active_date`                   COMMENT '放款日期',
  `loan_init_term`                     COMMENT '贷款期数（3、6、9等）',
  `repay_term`                         COMMENT '实还期数',
  `order_id`                           COMMENT '订单号',
  `loan_status`                        COMMENT '借据状态（英文原值）（N：正常，O：逾期，F：已还清，NORMAL_SETTLE：正常结清，OVERDUE_SETTLE：逾期结清，PRE_SETTLE：提前结清）',
  `loan_status_cn`                     COMMENT '借据状态（汉语解释）',
  `overdue_days`                       COMMENT '逾期天数',
  `payment_id`                         COMMENT '实还流水号',
  `txn_time`                           COMMENT '交易时间',
  `post_time`                          COMMENT '入账时间',
  `bnp_type`                           COMMENT '还款成分（英文原值）（Pricinpal：本金，Interest：利息，Penalty：罚息，Mulct：罚金，Compound：复利，CardFee：年费，OverLimitFee：超限费，LatePaymentCharge：滞纳金，NSFCharge：资金不足罚金，TXNFee：交易费，TERMFee：手续费，SVCFee：服务费，Compensation：赔偿金，Damages：违约金，LifeInsuFee：寿险计划包费）',
  `bnp_type_cn`                        COMMENT '还款成分（汉语解释）',
  `repay_amount`                       COMMENT '还款金额',
  `batch_date`                         COMMENT '批量日期',
  `create_time`                        COMMENT '创建时间',
  `update_time`                        COMMENT '更新时间',
  `biz_date`                           COMMENT '交易时间',
  `project_id`                         COMMENT '项目编号'
) COMMENT '实还明细表' AS select
  due_bill_no,
  loan_active_date,
  loan_init_term,
  repay_term,
  order_id,
  loan_status,
  loan_status_cn,
  overdue_days,
  payment_id,
  txn_time,
  post_time,
  bnp_type,
  bnp_type_cn,
  repay_amount,
  batch_date,
  create_time,
  update_time,
  biz_date,
  abs_project_id as project_id
from ods.repay_detail as t1
join dim_new.biz_conf as t2
on t1.product_id = t2.product_id;


-- DROP VIEW IF EXISTS `ods.order_info_abs`;
CREATE VIEW IF NOT EXISTS `ods.order_info_abs`(
  `order_id`                           COMMENT '订单编号',
  `ori_order_id`                       COMMENT '原订单编号',
  `apply_no`                           COMMENT '申请件编号',
  `due_bill_no`                        COMMENT '借据号',
  `term`                               COMMENT '处理期数',
  `channel_id`                         COMMENT '服务渠道编号（VISA：VISA，MC：MC，JCB：JCB，CUP：CUP，AMEX：AMEX，BANK：本行，ICL：ic系统，THIR：第三方，SUNS：阳光，AG：客服）',
  `pay_channel`                        COMMENT '支付渠道',
  `command_type`                       COMMENT '支付指令类型（SPA：单笔代付，SDB：单笔代扣，QSP：单笔代付查询，QSD：单笔代扣查询，BDB：批量代扣，BDA：批量代付）',
  `order_status`                       COMMENT '订单状态（C：已提交，P：待提交，Q：待审批，W：处理中，S：已完成，V：已失效，E：失败，T：超时，R：已重提，G：拆分处理中，D：拆分已完成，B：撤销，X：已受理待入账）',
  `order_time`                         COMMENT '订单时间',
  `repay_serial_no`                    COMMENT '还款流水号',
  `service_id`                         COMMENT '交易服务码',
  `assign_repay_ind`                   COMMENT '指定余额成分还款标志（Y：是，N：否）',
  `repay_way`                          COMMENT '还款方式（ONLINE：线上，OFFLINE：线下）',
  `txn_type`                           COMMENT '交易类型（Inq：查询，Cash：取现，AgentDebit：付款，Loan：分期，Auth：消费，PreAuth：预授权，PAComp：预授权完成，Load：圈存，Credit：存款，AgentCredit：收款，TransferCredit：转入，TransferDeditDepos：转出，AdviceSettle：结算通知，BigAmountLoan：大）',
  `txn_amt`                            COMMENT '交易金额',
  `original_txn_amt`                   COMMENT '原始交易金额',
  `success_amt`                        COMMENT '成功金额',
  `currency`                           COMMENT '币种',
  `code`                               COMMENT '状态码',
  `message`                            COMMENT '描述',
  `response_code`                      COMMENT '对外返回码',
  `response_message`                   COMMENT '对外返回描述',
  `business_date`                      COMMENT '业务日期',
  `send_time`                          COMMENT '发送时间',
  `opt_datetime`                       COMMENT '更新时间',
  `setup_date`                         COMMENT '创建日期',
  `loan_usage`                         COMMENT '贷款用途（B：回购，C：差额补足，D：代偿，F：追偿代扣，H：处置回收，I：强制结清扣款，L：放款申请，M：预约提前结清扣款，N：提前还当期，O：逾期扣款，P：打款通知，R：退货，T：退票，W：赎回结清，X：账务调整，Z：委托转付）',
  `purpose`                            COMMENT '支付用途',
  `online_flag`                        COMMENT '联机标识（Y：是，N：否）',
  `online_allow`                       COMMENT '允许联机标识（Y：是，N：否）',
  `order_pay_no`                       COMMENT '支付流水号',
  `bank_trade_no`                      COMMENT '银行交易流水号',
  `bank_trade_time`                    COMMENT '线下银行订单交易时间',
  `bank_trade_act_no`                  COMMENT '银行付款账号',
  `bank_trade_act_name`                COMMENT '银行付款账户名称',
  `bank_trade_act_phone`               COMMENT '银行预留手机号',
  `service_sn`                         COMMENT '流水号',
  `outer_no`                           COMMENT '外部凭证号',
  `confirm_flag`                       COMMENT '确认标志',
  `txn_time`                           COMMENT '交易时间',
  `txn_date`                           COMMENT '交易日期',
  `capital_plan_no`                    COMMENT '资金计划编号',
  `memo`                               COMMENT '备注',
  `create_time`                        COMMENT '创建时间',
  `update_time`                        COMMENT '更新时间',
  `biz_date`                           COMMENT '交易日期',
  `project_id`                         COMMENT '项目编号'
) COMMENT '订单流水表' AS
select
  order_id,
  ori_order_id,
  apply_no,
  due_bill_no,
  term,
  t1.channel_id,
  pay_channel,
  command_type,
  order_status,
  order_time,
  repay_serial_no,
  service_id,
  assign_repay_ind,
  repay_way,
  txn_type,
  txn_amt,
  original_txn_amt,
  success_amt,
  currency,
  code,
  message,
  response_code,
  response_message,
  business_date,
  send_time,
  opt_datetime,
  setup_date,
  loan_usage,
  purpose,
  online_flag,
  online_allow,
  order_pay_no,
  bank_trade_no,
  bank_trade_time,
  bank_trade_act_no,
  bank_trade_act_name,
  bank_trade_act_phone,
  service_sn,
  outer_no,
  confirm_flag,
  txn_time,
  txn_date,
  capital_plan_no,
  memo,
  create_time,
  update_time,
  biz_date,
  abs_project_id as project_id
from ods.order_info as t1
join dim_new.biz_conf as t2
on t1.product_id = t2.product_id;





-- DROP VIEW IF EXISTS `ods.t_05_repaymentplan`;
CREATE VIEW IF NOT EXISTS `ods.t_05_repaymentplan`(
  `project_id`                         COMMENT '项目编号',
  `serial_number`                      COMMENT '借据号',
  `period`                             COMMENT '期次',
  `should_repay_date`                  COMMENT '应还款日',
  `should_repay_principal`             COMMENT '应还本金',
  `should_repay_interest`              COMMENT '应还利息',
  `should_repay_cost`                  COMMENT '应还费用',
  `effective_date`                     COMMENT '生效日期',
  `repay_status`                       COMMENT '还款状态（1：入池前已还，2：入池前未还）',
  `schedule_status`                    COMMENT '还款计划状态（英文原值）（N：正常，O：逾期，F：已还清）',
  `schedule_status_cn`                 COMMENT '还款计划状态（汉语解释）',
  `paid_out_date`                      COMMENT '还清日期',
  `paid_out_type`                      COMMENT '结清类型（英文原值）（BANK_REF：退票结清，BUY_BACK：资产回购，CAPITAL_VERI：资产核销，DISPOSAL：处置结束，NORMAL_SETTLE：正常结清，OVER_COMP：逾期代偿，OVERDUE_SETTLE：逾期结清，PRE_SETTLE：提前结清，REDEMPTION：赎回，REFUND：退车，REFUND_SETTLEMENT：退票结清）',
  `paid_out_type_cn`                   COMMENT '结清类型（汉语解释）',
  `paid_amount`                        COMMENT '已还金额',
  `paid_principal`                     COMMENT '已还本金',
  `paid_interest`                      COMMENT '已还利息',
  `paid_term_fee`                      COMMENT '已还手续费',
  `paid_svc_fee`                       COMMENT '已还服务费',
  `paid_penalty`                       COMMENT '已还罚息',
  `paid_mult`                          COMMENT '已还滞纳金',
  `reduce_amount`                      COMMENT '减免金额',
  `reduce_principal`                   COMMENT '减免本金',
  `reduce_interest`                    COMMENT '减免利息',
  `reduce_term_fee`                    COMMENT '减免手续费',
  `reduce_svc_fee`                     COMMENT '减免服务费',
  `reduce_penalty`                     COMMENT '减免罚息',
  `reduce_mult_amt`                    COMMENT '减免滞纳金',
  `import_id`                          COMMENT '导入Id',
  `data_source`                        COMMENT '数据来源（1：startLink，2：excelImport，3：systemgenerated）',
  `s_d_date`                           COMMENT '生效日期',
  `e_d_date`                           COMMENT '失效日期'
) COMMENT '还款计划表-文件五' as select
  project_id                                                                                  as project_id,
  due_bill_no                                                                                 as serial_number,
  loan_term                                                                                   as period,
  should_repay_date                                                                           as should_repay_date,
  should_repay_principal                                                                      as should_repay_principal,
  should_repay_interest                                                                       as should_repay_interest,
  should_repay_term_fee + should_repay_svc_fee + should_repay_penalty + should_repay_mult_amt as should_repay_cost,
  effective_date                                                                              as effective_date,
  repay_status                                                                                as repay_status,
  schedule_status                                                                             as schedule_status,
  schedule_status_cn                                                                          as schedule_status_cn,
  paid_out_date                                                                               as paid_out_date,
  paid_out_type                                                                               as paid_out_type,
  paid_out_type_cn                                                                            as paid_out_type_cn,
  paid_amount                                                                                 as paid_amount,
  paid_principal                                                                              as paid_principal,
  paid_interest                                                                               as paid_interest,
  paid_term_fee                                                                               as paid_term_fee,
  paid_svc_fee                                                                                as paid_svc_fee,
  paid_penalty                                                                                as paid_penalty,
  paid_mult                                                                                   as paid_mult,
  reduce_amount                                                                               as reduce_amount,
  reduce_principal                                                                            as reduce_principal,
  reduce_interest                                                                             as reduce_interest,
  reduce_term_fee                                                                             as reduce_term_fee,
  reduce_svc_fee                                                                              as reduce_svc_fee,
  reduce_penalty                                                                              as reduce_penalty,
  reduce_mult_amt                                                                             as reduce_mult_amt,
  cast(null as string)                                                                        as import_id,
  cast(null as string)                                                                        as data_source,
  s_d_date                                                                                    as s_d_date,
  e_d_date                                                                                    as e_d_date
from (
  select
    project_id as dim_project_id,
    data_source
  from dim_new.project_info
) as project_info
join (
  select * from ods.repay_schedule_abs
  where 1 > 0
    and effective_date = s_d_date
) as repay_schedule_abs
on dim_project_id = project_id
;


-- DROP VIEW IF EXISTS `ods.t_07_actualrepayinfo`;
CREATE VIEW IF NOT EXISTS `ods.t_07_actualrepayinfo`(
  `project_id`                         COMMENT '项目编号',
  `serial_number`                      COMMENT '借据号',
  `term`                               COMMENT '期次',
  `is_borrowers_oneself_repayment`     COMMENT '是否借款人本人还款（预定义字段：Y、N，默认：Y）',
  `actual_repay_time`                  COMMENT '实际还清日期（若没有还清传还款日期，若还清就传实际还清的日期）',
  `current_period_loan_balance`        COMMENT '当期贷款余额（各期还款日（T+1）更新该字段，即截至当期还款日资产的剩余（未偿还）贷款本金余额）',
  `repay_type`                         COMMENT '还款类型 1，提前还款 2，正常还款 3，部分还款 4，逾期还款',
  `current_account_status`             COMMENT '当期账户状态（各期还款日（T+1）更新该字段，正常：在还款日前该期应还已还清，提前还清（早偿）：贷款在该期还款日前提前全部还清，逾期：贷款在该期还款日时，实还金额小于应还金额）',
  `actual_repay_principal`             COMMENT '实还本金（元）',
  `actual_repay_interest`              COMMENT '实还利息（元）',
  `actual_repay_fee`                   COMMENT '实还费用（元）',
  `penalbond`                          COMMENT '违约金',
  `penalty_interest`                   COMMENT '罚息',
  `compensation`                       COMMENT '赔偿金（提前还款/逾期所产生的赔偿金）',
  `advanced_commission_charge`         COMMENT '提前还款手续费',
  `other_fee`                          COMMENT '其他相关费用 （违约金、罚款、赔偿金和提前还款手续费以外的费用）',
  `actual_work_interest_rate`          COMMENT '实际还款执行利率',
  `data_source`                        COMMENT '数据来源（1：startLink，2：excelImport）',
  `import_id`                          COMMENT '导入Id',
  `create_time`                        COMMENT '创建时间',
  `update_time`                        COMMENT '更新时间'
) COMMENT '实际还款信息表-文件七' as select
  project_id                     as project_id,
  due_bill_no                    as serial_number,
  term                           as term,
  is_borrowers_oneself_repayment as is_borrowers_oneself_repayment,
  biz_date                       as actual_repay_time,
  remain_principal               as current_period_loan_balance,
  repay_type                     as repay_type,
  current_account_status         as current_account_status,
  actual_repay_principal         as actual_repay_principal,
  actual_repay_interest          as actual_repay_interest,
  actual_repay_fee               as actual_repay_fee,
  penalbond                      as penalbond,
  penalty_interest               as penalty_interest,
  compensation                   as compensation,
  advanced_commission_charge     as advanced_commission_charge,
  other_fee                      as other_fee,
  actual_work_interest_rate      as actual_work_interest_rate,
  import_id                      as import_id,
  -- cast(null as string)           as import_id,
  data_source                    as data_source,
  create_time                    as create_time,
  update_time                    as update_time
from (
  select
    project_id as dim_project_id,
    data_source
  from dim_new.project_info
) as project_info
join (
  select
    split_part(project_id,'@',1)                                      as project_id,
    due_bill_no                                                       as due_bill_no,
    repay_term                                                        as term,
    'Y'                                                               as is_borrowers_oneself_repayment,
    biz_date                                                          as biz_date,
    case loan_status_cn
    when '正常结清' then '正常还款'
    when '逾期结清' then '逾期还款'
    when '提前结清' then '提前还款'
    else loan_status_cn end                                           as repay_type,
    case loan_status_cn
    when '正常还款' then '正常'
    when '逾期还款' then '正常'
    when '提前还款' then '提前还清'
    else '正常' end                                                   as current_account_status,
    sum(if(bnp_type = 'Pricinpal', repay_amount,0))                   as actual_repay_principal,
    sum(if(bnp_type = 'Interest',  repay_amount,0))                   as actual_repay_interest,
    sum(if(bnp_type in ('TXNFee','TERMFee','SVCFee'),repay_amount,0)) as actual_repay_fee,
    sum(if(bnp_type = 'Damages',   repay_amount,0))                   as penalbond,
    sum(if(bnp_type = 'Penalty',    repay_amount,0))                  as penalty_interest,
    sum(if(bnp_type = 'Compensation',   repay_amount,0))              as compensation,
    sum(if(bnp_type = 'TERMFee',   repay_amount,0))                   as advanced_commission_charge,
    sum(if(bnp_type = 'SVCFee',   repay_amount,0))                    as other_fee,
    0                                                                 as actual_work_interest_rate,
    nvl(cast(split_part(project_id,'@',2) as string))                 as import_id,
    create_time                                                       as create_time,
    update_time                                                       as update_time
  from ods.repay_detail_abs
  group by
    split_part(project_id,'@',1), -- project_id
    due_bill_no,
    repay_term,
    biz_date,
    case loan_status_cn
    when '正常结清' then '正常还款'
    when '逾期结清' then '逾期还款'
    when '提前结清' then '提前还款'
    else loan_status_cn end,
    case loan_status_cn
    when '正常结清' then '正常'
    when '逾期结清' then '正常'
    when '提前结清' then '提前还清'
    else '正常' end,
    nvl(cast(split_part(project_id,'@',2) as string)), -- import_id
    create_time,
    update_time
) as repay_detail_abs
on dim_project_id = project_id
left join (
  select
    project_id       as loan_project_id,
    due_bill_no      as loan_due_bill_no,
    remain_principal as remain_principal,
    s_d_date,
    e_d_date
  from ods.loan_info_abs
) as loan_info
on  project_id  = loan_project_id
and due_bill_no = loan_due_bill_no
where biz_date between s_d_date and date_sub(e_d_date,1)
;



-- DROP VIEW IF EXISTS `ods.t_07_actualrepayinfo`;
CREATE VIEW IF NOT EXISTS `ods.t_07_actualrepayinfo`(
  `project_id`                         COMMENT '项目编号',
  `serial_number`                      COMMENT '借据号',
  `term`                               COMMENT '期次',
  `is_borrowers_oneself_repayment`     COMMENT '是否借款人本人还款（预定义字段：Y、N，默认：Y）',
  `actual_repay_time`                  COMMENT '实际还清日期（若没有还清传还款日期，若还清就传实际还清的日期）',
  `current_period_loan_balance`        COMMENT '当期贷款余额（各期还款日（T+1）更新该字段，即截至当期还款日资产的剩余（未偿还）贷款本金余额）',
  `repay_type`                         COMMENT '还款类型 1，提前还款 2，正常还款 3，部分还款 4，逾期还款',
  `current_account_status`             COMMENT '当期账户状态（各期还款日（T+1）更新该字段，正常：在还款日前该期应还已还清，提前还清（早偿）：贷款在该期还款日前提前全部还清，逾期：贷款在该期还款日时，实还金额小于应还金额）',
  `actual_repay_principal`             COMMENT '实还本金（元）',
  `actual_repay_interest`              COMMENT '实还利息（元）',
  `actual_repay_fee`                   COMMENT '实还费用（元）',
  `penalbond`                          COMMENT '违约金',
  `penalty_interest`                   COMMENT '罚息',
  `compensation`                       COMMENT '赔偿金（提前还款/逾期所产生的赔偿金）',
  `advanced_commission_charge`         COMMENT '提前还款手续费',
  `other_fee`                          COMMENT '其他相关费用 （违约金、罚款、赔偿金和提前还款手续费以外的费用）',
  `actual_work_interest_rate`          COMMENT '实际还款执行利率',
  `data_source`                        COMMENT '数据来源（1：startLink，2：excelImport）',
  `import_id`                          COMMENT '导入Id',
  `create_time`                        COMMENT '创建时间',
  `update_time`                        COMMENT '更新时间'
) COMMENT '实际还款信息表-文件七' as select
  project_id                                                        as project_id,
  due_bill_no                                                       as serial_number,
  repay_term                                                        as term,
  'Y'                                                               as is_borrowers_oneself_repayment,
  biz_date                                                          as actual_repay_time,
  remain_principal                                                  as current_period_loan_balance,
  loan_status_cn                                                    as repay_type,
  case loan_status_cn
  when '正常还款' then '正常'
  when '逾期还款' then '正常'
  when '提前还款' then '提前还清'
  else '正常' end                                                   as current_account_status,
  sum(if(bnp_type = 'Pricinpal', repay_amount,0))                   as actual_repay_principal,
  sum(if(bnp_type = 'Interest',  repay_amount,0))                   as actual_repay_interest,
  sum(if(bnp_type in ('TXNFee','TERMFee','SVCFee'),repay_amount,0)) as actual_repay_fee,
  sum(if(bnp_type = 'Damages',   repay_amount,0))                   as penalbond,
  sum(if(bnp_type = 'Penalty',    repay_amount,0))                  as penalty_interest,
  sum(if(bnp_type = 'Compensation',   repay_amount,0))              as compensation,
  sum(if(bnp_type = 'TERMFee',   repay_amount,0))                   as advanced_commission_charge,
  sum(if(bnp_type = 'SVCFee',   repay_amount,0))                    as other_fee,
  0                                                                 as actual_work_interest_rate,
  cast(null as string)                                              as import_id,
  cast(null as string)                                              as data_source,
  create_time                                                       as create_time,
  update_time                                                       as update_time
from (
  select
    project_id as dim_project_id,
    data_source
  from dim_new.project_info
) as project_info
join ods.repay_detail_abs
on dim_project_id = project_id
left join (
  select
    project_id       as loan_project_id,
    due_bill_no      as loan_due_bill_no,
    remain_principal as remain_principal,
    s_d_date,
    e_d_date
  from ods.loan_info_abs
) as loan_info
on  project_id  = loan_project_id
and due_bill_no = loan_due_bill_no
where biz_date between s_d_date and date_sub(e_d_date,1)
group by
  project_id,
  due_bill_no,
  repay_term,
  biz_date,
  remain_principal,
  loan_status_cn,
  case loan_status_cn
  when '正常还款' then '正常'
  when '逾期还款' then '正常'
  when '提前还款' then '提前还清'
  else '正常' end,
  create_time,
  update_time
;







create table IF NOT EXISTS `ods.t_10_basic_asset_stage`(
  `id`                                                int            COMMENT '主键id',
  `import_id`                                         string         COMMENT '导入记录编号',
  `project_id`                                        string         COMMENT '项目编号',
  `asset_type`                                        string         COMMENT '资产类型',
  `serial_number`                                     string         COMMENT '借据号',
  `contract_code`                                     string         COMMENT '贷款合同编号',
  `contract_amount`                                   decimal(32,2)  COMMENT '贷款合同金额',
  `interest_rate_type`                                string         COMMENT '利率类型',
  `contract_interest_rate`                            decimal(20,4)  COMMENT '合同利率',
  `base_interest_rate`                                decimal(20,4)  COMMENT '基准利率',
  `fixed_interest_rate`                               decimal(20,4)  COMMENT '固定利率',
  `fixed_interest_diff`                               string         COMMENT '固定利差',
  `interest_rate_ajustment`                           string         COMMENT '利率调整方式',
  `loan_issue_date`                                   string         COMMENT '贷款发放日',
  `loan_expiry_date`                                  string         COMMENT '贷款到期日',
  `frist_repayment_date`                              string         COMMENT '首次还款日',
  `repayment_type`                                    string         COMMENT '还款方式',
  `repayment_frequency`                               string         COMMENT '还款频率',
  `loan_repay_date`                                   int            COMMENT '贷款还款日',
  `tail_amount`                                       decimal(32,2)  COMMENT '尾款金额',
  `tail_amount_rate`                                  decimal(32,2)  COMMENT '尾款金额占比',
  `consume_use`                                       string         COMMENT '消费用途',
  `guarantee_type`                                    string         COMMENT '担保方式',
  `extract_date`                                      string         COMMENT '提取日期',
  `extract_date_principal_amount`                     decimal(32,2)  COMMENT '提取日本金金额',
  `loan_cur_interest_rate`                            decimal(20,4)  COMMENT '贷款现行利率',
  `borrower_type`                                     string         COMMENT '借款人类型',
  `borrower_name`                                     string         COMMENT '借款人姓名',
  `document_type`                                     string         COMMENT '证件类型',
  `document_num`                                      string         COMMENT '证件号码',
  `borrower_rating`                                   string         COMMENT '借款人评级',
  `borrower_industry`                                 string         COMMENT '借款人行业',
  `phone_num`                                         string         COMMENT '手机号码',
  `sex`                                               string         COMMENT '性别',
  `birthday`                                          string         COMMENT '出生日期',
  `age`                                               int            COMMENT '年龄',
  `province`                                          string         COMMENT '所在省份',
  `city`                                              string         COMMENT '所在城市',
  `marital_status`                                    string         COMMENT '婚姻状况',
  `country`                                           string         COMMENT '国籍',
  `annual_income`                                     decimal(32,2)  COMMENT '年收入',
  `income_debt_rate`                                  decimal(32,4)  COMMENT '收入债务比',
  `education_level`                                   string         COMMENT '教育程度',
  `period_exp`                                        int            COMMENT '提取日剩余还款期数',
  `account_age`                                       decimal(11,2)  COMMENT '帐龄',
  `residual_maturity_con`                             decimal(11,2)  COMMENT '合同期限',
  `residual_maturity_ext`                             decimal(11,2)  COMMENT '提取日剩余期限',
  `statistics_date`                                   string         COMMENT '统计日期',
  `extract_interest_date`                             string         COMMENT '提取日计息日',
  `curr_over_days`                                    int            COMMENT '当前逾期天数',
  `remain_counts`                                     int            COMMENT '当前剩余期数',
  `remain_amounts`                                    decimal(20,2)  COMMENT '当前剩余本金(元)',
  `remain_interest`                                   decimal(20,2)  COMMENT '当前剩余利息(元)',
  `remain_other_amounts`                              decimal(20,2)  COMMENT '当前剩余费用(元)',
  `periods`                                           int            COMMENT '总期数',
  `period_amounts`                                    decimal(20,2)  COMMENT '每期固定费用',
  `bus_product_id`                                    string         COMMENT '业务产品编号',
  `bus_product_name`                                  string         COMMENT '业务产品名称',
  `shoufu_amount`                                     decimal(32,2)  COMMENT '首付款金额',
  `selling_price`                                     decimal(32,2)  COMMENT '销售价格',
  `contract_daily_interest_rate`                      decimal(20,4)  COMMENT '合同日利率',
  `repay_plan_cal_rule`                               string         COMMENT '还款计划计算规则',
  `contract_daily_interest_rate_count`                int            COMMENT '日利率计算基础',
  `total_investment_amount`                           decimal(32,2)  COMMENT '投资总额(元)',
  `contract_month_interest_rate`                      decimal(20,4)  COMMENT '合同月利率',
  `status_change_log`                                 string         COMMENT '资产状态变更日志',
  `package_filter_id`                                 string         COMMENT '虚拟过滤包id',
  `virtual_asset_bag_id`                              string         COMMENT '虚拟资产包id',
  `wind_control_status`                               string         COMMENT '当前风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
  `wind_control_status_pool`                          string         COMMENT '入池时风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
  `cheat_level`                                       int            COMMENT '反欺诈等级:1~5,-1:null,其它:异常值',
  `score_range`                                       int            COMMENT '评分等级:1~20,-1:null,其它:异常值',
  `score_level`                                       int            COMMENT '评分区间:1~5,-1:null,其它:异常值',
  `create_time`                                       string         COMMENT '创建记录时间',
  `update_time`                                       string         COMMENT '修改时间',
  `data_source`                                       int            COMMENT '数据来源 1:startLink,2:excelImport',
  `address`                                           string         COMMENT '居住地址',
  `mortgage_rates`                                    decimal(20,4)  COMMENT '抵押率(%)'
)
stored as parquet;





--DROP view IF EXISTS ods.t_10_basic_asset;
-- impala创建视图中文乱码问题 : 到mysql元数据库中执行如下语句.然后重建视图
-- alter table TBLS modify column VIEW_EXPANDED_TEXT mediumtext character set utf8;
-- alter table TBLS modify column VIEW_ORIGINAL_TEXT mediumtext character set utf8;
CREATE VIEW IF NOT EXISTS ods.t_10_basic_asset(
  `id`                                 COMMENT '主键id',
  `import_id`                          COMMENT '导入记录编号',
  `project_id`                         COMMENT '项目编号',
  `asset_bag_id`                       COMMENT '资产包编号',
  `asset_type`                         COMMENT '资产类型',
  `serial_number`                      COMMENT '借据号',
  `contract_code`                      COMMENT '贷款合同编号',
  `contract_amount`                    COMMENT '贷款合同金额',
  `interest_rate_type`                 COMMENT '利率类型',
  `contract_interest_rate`             COMMENT '合同利率',
  `base_interest_rate`                 COMMENT '基准利率',
  `fixed_interest_rate`                COMMENT '固定利率',
  `fixed_interest_diff`                COMMENT '固定利差',
  `interest_rate_ajustment`            COMMENT '利率调整方式',
  `loan_issue_date`                    COMMENT '贷款发放日',
  `loan_expiry_date`                   COMMENT '贷款到期日',
  `frist_repayment_date`               COMMENT '首次还款日',
  `repayment_type`                     COMMENT '还款方式',
  `repayment_frequency`                COMMENT '还款频率',
  `loan_repay_date`                    COMMENT '贷款还款日',
  `tail_amount`                        COMMENT '尾款金额',
  `tail_amount_rate`                   COMMENT '尾款金额占比',
  `consume_use`                        COMMENT '消费用途',
  `guarantee_type`                     COMMENT '担保方式',
  `extract_date`                       COMMENT '提取日期',
  `extract_date_principal_amount`      COMMENT '提取日本金金额',
  `loan_cur_interest_rate`             COMMENT '贷款现行利率',
  `borrower_type`                      COMMENT '借款人类型',
  `borrower_name`                      COMMENT '借款人姓名',
  `document_type`                      COMMENT '证件类型',
  `document_num`                       COMMENT '证件号码',
  `borrower_rating`                    COMMENT '借款人评级',
  `borrower_industry`                  COMMENT '借款人行业',
  `phone_num`                          COMMENT '手机号码',
  `sex`                                COMMENT '性别',
  `birthday`                           COMMENT '出生日期',
  `age`                                COMMENT '年龄',
  `province`                           COMMENT '所在省份',
  `city`                               COMMENT '所在城市',
  `marital_status`                     COMMENT '婚姻状况',
  `country`                            COMMENT '国籍',
  `annual_income`                      COMMENT '年收入',
  `income_debt_rate`                   COMMENT '收入债务比',
  `education_level`                    COMMENT '教育程度',
  `period_exp`                         COMMENT '提取日剩余还款期数',
  `account_age`                        COMMENT '帐龄',
  `residual_maturity_con`              COMMENT '合同期限',
  `residual_maturity_ext`              COMMENT '提取日剩余期限',
  `package_principal_balance`          COMMENT '封包本金余额',
  `statistics_date`                    COMMENT '统计日期',
  `extract_interest_date`              COMMENT '提取日计息日',
  `curr_over_days`                     COMMENT '当前逾期天数',
  `remain_counts`                      COMMENT '当前剩余期数',
  `remain_amounts`                     COMMENT '当前剩余本金(元)',
  `remain_interest`                    COMMENT '当前剩余利息(元)',
  `remain_other_amounts`               COMMENT '当前剩余费用(元)',
  `periods`                            COMMENT '总期数',
  `period_amounts`                     COMMENT '每期固定费用',
  `bus_product_id`                     COMMENT '业务产品编号',
  `bus_product_name`                   COMMENT '业务产品名称',
  `shoufu_amount`                      COMMENT '首付款金额',
  `selling_price`                      COMMENT '销售价格',
  `contract_daily_interest_rate`       COMMENT '合同日利率',
  `repay_plan_cal_rule`                COMMENT '还款计划计算规则',
  `contract_daily_interest_rate_count` COMMENT '日利率计算基础',
  `total_investment_amount`            COMMENT '投资总额(元)',
  `contract_month_interest_rate`       COMMENT '合同月利率',
  `status_change_log`                  COMMENT '资产状态变更日志',
  `package_filter_id`                  COMMENT '虚拟过滤包id',
  `virtual_asset_bag_id`               COMMENT '虚拟资产包id',
  `package_remain_principal`           COMMENT '封包时当前剩余本金(元)',
  `package_remain_periods`             COMMENT '封包时当前剩余期数',
  `status`                             COMMENT '资产状态(-1征信异常 0准入 1在池 2封包 3发行 4历史',
  `wind_control_status`                COMMENT '当前风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
  `wind_control_status_pool`           COMMENT '入池时风控状态 Yes:风控通过,No:风控未通过,NA:风控未查得,Without:未跑风控)',
  `cheat_level`                        COMMENT '反欺诈等级:1~5,-1:null,其它:异常值',
  `score_range`                        COMMENT '评分等级:1~20,-1:null,其它:异常值',
  `score_level`                        COMMENT '评分区间:1~5,-1:null,其它:异常值',
  `create_time`                        COMMENT '创建记录时间',
  `update_time`                        COMMENT '修改时间',
  `data_source`                        COMMENT '数据来源 1:startLink,2:excelImport',
  `address`                            COMMENT '居住地址',
  `mortgage_rates`                     COMMENT '抵押率(%)'
) COMMENT '资产对账信息表-文件十' AS SELECT
  id,
  import_id,
  project_id,
  asset_bag_id,
  asset_type,
  stage.serial_number as serial_number,
  contract_code,
  contract_amount,
  interest_rate_type,
  contract_interest_rate,
  base_interest_rate,
  fixed_interest_rate,
  fixed_interest_diff,
  interest_rate_ajustment,
  loan_issue_date,
  loan_expiry_date,
  frist_repayment_date,
  repayment_type,
  repayment_frequency,
  loan_repay_date,
  tail_amount,
  tail_amount_rate,
  consume_use,
  guarantee_type,
  extract_date,
  extract_date_principal_amount,
  loan_cur_interest_rate,
  borrower_type,
  borrower_name,
  document_type,
  document_num,
  borrower_rating,
  borrower_industry,
  phone_num,
  sex,
  birthday,
  age,
  province,
  city,
  marital_status,
  country,
  annual_income,
  income_debt_rate,
  education_level,
  period_exp,
  account_age,
  residual_maturity_con,
  residual_maturity_ext,
  package_principal_balance,
  statistics_date,
  extract_interest_date,
  curr_over_days,
  remain_counts,
  remain_amounts,
  remain_interest,
  remain_other_amounts,
  periods,
  period_amounts,
  bus_product_id,
  bus_product_name,
  shoufu_amount,
  selling_price,
  contract_daily_interest_rate,
  repay_plan_cal_rule,
  contract_daily_interest_rate_count,
  total_investment_amount,
  contract_month_interest_rate,
  status_change_log,
  package_filter_id,
  virtual_asset_bag_id,
  package_remain_principal,
  package_remain_periods,
  cast(case
  when bag_status = '已封包'   then 2
  when bag_status = '已发行'   then 3
  else 1 end as int) as status,
  wind_control_status,
  wind_control_status_pool,
  cheat_level,
  score_range,
  score_level,
  create_time,
  update_time,
  data_source,
  address,
  mortgage_rates
from (
  select * from ods.t_10_basic_asset_stage
) stage
left join (
  select
    bag_due.due_bill_no                 as due_bill_no,
    bag_info.bag_id                     as asset_bag_id,
    bag_info.bag_status                 as bag_status,
    bag_info.bag_remain_principal       as package_principal_balance,
    package_remain_principal            as package_remain_principal,
    package_remain_periods              as package_remain_periods
  from dim_new.bag_due_bill_no bag_due
  inner join dim_new.bag_info bag_info
  on bag_due.bag_id = bag_info.bag_id
  where bag_info.bag_date between s_d_date and date_sub(e_d_date,1)
) bag_snapshot
on cur.serial_number = bag_snapshot.due_bill_no;

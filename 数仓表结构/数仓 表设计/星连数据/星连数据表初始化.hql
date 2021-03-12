/**
 * 星云系统对接
 * 建表语句
 */

CREATE DATABASE IF NOT EXISTS ods_starconnect COMMENT '星连数据源数据库'
WITH DBPROPERTIES('CREATOR'='yunan.huang','DATE'='2018-11-16');


CREATE TABLE if not exists ods_starconnect.01_starconnect_loancontract (
project_no                        string  COMMENT 'ABS系统的项目编号',
institutions_no                   string  COMMENT 'ABS系统资产服务机构的机构编号',
iou_no                            string  COMMENT '进件流水号-借据号',
assets_type                       string  COMMENT '资产类型',
loan_contract_no                  string  COMMENT '贷款合同编号',
product_no                        string  COMMENT '内部产品编号',
loan_total_amount                 string  COMMENT '贷款总金额(元)',
interest_rate_type                string  COMMENT '利率类型',
loan_annual_interest_rate         string  COMMENT '贷款年利率(%)',
repayment_mode                    string  COMMENT '还款方式',
repayment_frequency               string  COMMENT '还款频率',
total_nper                        string  COMMENT '总期数',
total_yield                       string  COMMENT '总收益率(%)',
remaining_principal               string  COMMENT '剩余本金(元)',
residual_interest                 string  COMMENT '剩余利息(元)',
remaining_other_expenses          string  COMMENT '剩余其他费用(元)',
first_payment                     string  COMMENT '首付款金额(元)',
first_payment_rate                string  COMMENT '首付比例(%)',
final_payment_amount              string  COMMENT '尾付款金额(元)',
final_payment_rate                string  COMMENT '尾付比例(%)',
poundage                          string  COMMENT '手续费',
poundage_rate                     string  COMMENT '手续费利率',
fee_deduction_mode                string  COMMENT '手续费扣款方式',
settlement_fee_rate               string  COMMENT '结算手续费率',
subsidy_fee                       string  COMMENT '补贴手续费',
discount_amount                   string  COMMENT '贴息金额',
security_deposit_ratio            string  COMMENT '保证金比例',
security_deposit                  string  COMMENT '保证金',
security_deposit_mode             string  COMMENT '保证金冲抵方式',
account_manage_cost               string  COMMENT '帐户管理费',
mortgage_rate                     string  COMMENT '抵押率(%)',
contract_start_time               string  COMMENT '合同开始时间',
contract_end_time                 string  COMMENT '合同结束时间',
actual_loan_time                  string  COMMENT '实际放款时间',
first_payment_time                string  COMMENT '首次还款时间',
loan_payment_time                 string  COMMENT '贷款还款日',
latest_loan_payment_time          string  COMMENT '最后一次预计扣款时间',
month_payment_amount              string  COMMENT '月还款额',
loan_use                          string  COMMENT '贷款用途',
guarantee_mode                    string  COMMENT '担保方式',
contract_status                   string  COMMENT '合同状态',
cumulative_overdue_nper           string  COMMENT '累计逾期期数',
history_highest_overdue_daysnum   string  COMMENT '历史最高逾期天数',
history_cumulative_overdue_daysnu string  COMMENT '历史累计逾期天数',
current_cumulative_daysnum        string  COMMENT '当前逾期天数',
application_status_code           string  COMMENT '申请状态代码',
application_channel               string  COMMENT '申请渠道',
application_position              string  COMMENT '申请地点',
borrower_status                   string  COMMENT '借款人状态',
dealer_name                       string  COMMENT '经销商名称',
dealer_store_position             string  COMMENT '经销商卖场地址',
store_province                    string  COMMENT '店面省份',
store_city                        string  COMMENT '店面城市',
data_extract_day                  string  COMMENT '数据提取日',
message_uuid                      string  COMMENT '消息唯一标识',
message_timestamp                 string  COMMENT '消息生产时间',
product_plan_name                 string  COMMENT '产品方案名称',
application_use                   string  COMMENT '申请用途',
insert_timestamp                  string  COMMENT '入库时间'

)
COMMENT '借款合同信息_01'
row format delimited
fields terminated by '\001'


CREATE TABLE if not exists ods_starconnect.02_starconnect_mainborrower(
project_no                        string COMMENT '项目编号',
institutions_no                   string COMMENT '机构编号',
iou_no                            string COMMENT '借据号',
customer_name                     string COMMENT '客户姓名',
id_card_no                        string COMMENT '身份证号',
cell_phone_number                 string COMMENT '手机号',
age                               string COMMENT '年龄',
sex                               string COMMENT '性别',
marriage_status                   string COMMENT '婚姻状况',
children_number                   string COMMENT '子女数量',
working_life                      string COMMENT '工作年限',
customer_type                     string COMMENT '客户类型',
education                         string COMMENT '学历',
degree                            string COMMENT '学位',
is_civil_capabiliy                string COMMENT '是否具有完全民事行为能力',
living_status                     string COMMENT '居住状态(1-自置 2-按揭 3-亲属楼宇 4-集体宿舍 5-租房 6-共有住宅 7-其他 9-未知)',
customer_living_province          string COMMENT '客户居住所在省',
customer_living_city              string COMMENT '客户居住所在市',
customer_living_address           string COMMENT '客户居住地址',
customer_census_register_province string COMMENT '客户户籍所在省',
customer_census_register_city     string COMMENT '客户户籍所在市',
customer_census_register_address  string COMMENT '客户户籍地址',
communication_postal_code         string COMMENT '通讯邮编',
customer_communication_address    string COMMENT '客户通讯地址',
customer_occuopation              string COMMENT '客户职业',
working_status                    string COMMENT '工作状态',
working_level                     string COMMENT '职务(1-高级领导（行政级别局级及局级以上领导或大公司高级管理人员）2-中级领导（行政级别局级以下领导或大公司中级管理人员）3-一般员工4-其他9-未知。)',
working_title                     string COMMENT '职称',
borrower_industry                 string COMMENT '借款人行业(NIL--空 A--农、林、牧、渔业 B--采矿业 C--制造业 D--电力、热力、燃气及水生产和供应业 E--建筑业 F--批发和零售业 G--交通运输、仓储和邮政业 H--住宿和餐饮业 I--信息传输、软件和信息技术服务业 J--金融业 K--房地产业 L--租赁和商务服务业 M--科学研究和技术服务业 N--水利、环境和公共设施管理业 O--居民服务、修理和其他服务业 P--教育 Q--卫生和社会工作 R--文化、体育和娱乐业 S--公共管理、社会保障和社会组织 T--国际组织 Z--其他)',
ishave_car                        string COMMENT '是否有车',
ishave_mortgage_car_loan          string COMMENT '是否有按揭车贷',
ishave_house                      string COMMENT '是否有房',
ishave_mortgage_house_loan        string COMMENT '是否有按揭房贷',
ishave_credit_car                 string COMMENT '是否有信用卡',
credit_car_limit                  string COMMENT '信用卡额度',
annual_income                     string COMMENT '年收入(元)',
internal_credit_grade             string COMMENT '内部信用等级',
black_list_grade                  string COMMENT '黑名单等级',
unit_name                         string COMMENT '单位名称',
fixed_telephone                   string COMMENT '固定电话',
postal_code                       string COMMENT '邮编',
unit_detail_address               string COMMENT '单位详细地址',
message_uuid                      string COMMENT '消息唯一标识',
message_timestamp                 string COMMENT '消息生产时间',
insert_timestamp                  string COMMENT '入库时间'

)
COMMENT '主借款人信息_02'
row format delimited
fields terminated by '\001'


CREATE TABLE if not exists ods_starconnect.03_starconnect_correlation_person_info (
project_no                   string COMMENT '项目编号',
institutions_no              string COMMENT '机构编号',
iou_no                       string COMMENT '借据号',
contract_role                string COMMENT '合同角色',
customer_name                string COMMENT '客户姓名',
id_card_no                   string COMMENT '身份证号',
cell_phone_number            string COMMENT '手机号',
age                          string COMMENT '年龄',
sex                          string COMMENT '性别',
relationship_master_borrower string COMMENT '与主借款人关系',
occupation                   string COMMENT '职业',
working_status               string COMMENT '工作状态',
annual_income                string COMMENT '年收入(元)',
communication_address        string COMMENT '通讯地址',
unit_address                 string COMMENT '单位地址',
unit_contact_info            string COMMENT '单位联系方式',
message_uuid                 string COMMENT '消息唯一标识',
message_timestamp            string COMMENT '消息生产时间',
insert_timestamp             string COMMENT '入库时间'

)
COMMENT '关联人信息_03'
row format delimited
fields terminated by '\001'


CREATE TABLE if not exists ods_starconnect.04_starconnect_mortgage_car_info (
project_no                        string COMMENT '项目编号',
institutions_no                   string COMMENT '机构编号',
iou_no                            string COMMENT '借据号',
pawn_no                           string COMMENT '抵押物编号',
pawn_handle_status                string COMMENT '抵押办理状态',
pawn_sequence                     string COMMENT '抵押顺位',
car_property                      string COMMENT '车辆性质',
financing_type                    string COMMENT '融资方式',
guarantee_type                    string COMMENT '担保方式',
assessment_price                  string COMMENT '评估价格(元)',
car_sale_price                    string COMMENT '车辆销售价格(元)',
newcar_guidance_price             string COMMENT '新车指导价(元)',
investmen_total_sum               string COMMENT '投资总额(元)',
purchase_tax_money                string COMMENT '购置税金额(元)',
insurance_type                    string COMMENT '保险种类',
car_insurance_total_sum           string COMMENT '汽车保险总费用',
poundage_total_sum                string COMMENT '手续总费用(元)',
cumulative_car_transfer_frequency string COMMENT '累计车辆过户次数',
annual_car_transfer_frequency     string COMMENT '一年内车辆过户次数',
responsibility_credit_premium1    string COMMENT '责信保费用1',
responsibility_credit_premium2    string COMMENT '责信保费用2',
car_type                          string COMMENT '车类型',
farme_no                          string COMMENT '车架号',
engine_no                         string COMMENT '发动机号',
gps_no                            string COMMENT 'GPS编号',
gps_cost                          string COMMENT 'GPS费用',
car_no                            string COMMENT '车牌号码',
car_brand                         string COMMENT '车辆品牌',
car_series                        string COMMENT '车系',
car_models                        string COMMENT '车型',
car_age                           string COMMENT '车龄',
car_energy_type                   string COMMENT '车辆能源类型',
product_date                      string COMMENT '生产日期',
mileage                           string COMMENT '里程数',
registration_date                 string COMMENT '注册日期',
car_purchase_place                string COMMENT '车辆购买地',
car_color                         string COMMENT '车辆颜色',
message_uuid                      string COMMENT '消息唯一标识',
message_timestamp                 string COMMENT '消息生产时间',
insert_timestamp                  string COMMENT '入库时间'

)
COMMENT '抵押物（车）信息_04'
row format delimited
fields terminated by '\001'



CREATE TABLE if not exists ods_starconnect.05_starconnect_repayment_plan_info (
project_no          string COMMENT '项目编号',
institutions_no     string COMMENT '机构编号',
iou_no              string COMMENT '借据号',
period              string COMMENT '期次',
repayment_date      string COMMENT '应还款日',
repayment_principal string COMMENT '应还本金(元)',
repayment_interest  string COMMENT '应还利息(元)',
repayment_cost      string COMMENT '应还费用(元)',
effected_date       string COMMENT '生效日期',
message_uuid        string COMMENT '消息唯一标识',
message_timestamp   string COMMENT '消息生产时间',
insert_timestamp    string COMMENT '入库时间'

)
COMMENT '还款计划_05'
row format delimited
fields terminated by '\001'

CREATE TABLE if not exists ods_starconnect.06_starconnect_assets_paymentflow_info (
project_no             string COMMENT '项目编号',
institutions_no        string COMMENT '机构编号',
iou_no                 string COMMENT '借据号',
transation_channel     string COMMENT '交易渠道',
transation_type        string COMMENT '交易类型',
order_no               string COMMENT '订单号',
order_price            string COMMENT '订单金额(元)',
transation_currency    string COMMENT '交易币种',
name                   string COMMENT '姓名',
bank_account           string COMMENT '银行帐号',
trading_time           string COMMENT '交易时间',
trading_status         string COMMENT '交易状态',
trading_abstract       string COMMENT '交易摘要',
confirm_repayment_date string COMMENT '确认还款日期',
message_uuid           string COMMENT '消息唯一标识',
message_timestamp      string COMMENT '消息生产时间',
insert_timestamp       string COMMENT '入库时间'

)
COMMENT '资产交易支付流水信息_06'
row format delimited
fields terminated by '\001'

CREATE TABLE if not exists ods_starconnect.07_starconnect_actual_repayment_info (
project_no               string COMMENT '项目编号',
institutions_no          string COMMENT '机构编号',
iou_no                   string COMMENT '借据号',
period                   string COMMENT '期次',
repayment_date           string COMMENT '应还款日',
repayment_principal      string COMMENT '应还本金(元)',
repayment_interest       string COMMENT '应还利息(元)',
repayment_cost           string COMMENT '应还费用(元)',
repayment_type           string COMMENT '还款类型',
real_interest_rate       string COMMENT '实还执行利率',
real_repayment_principal string COMMENT '实还本金(元)',
real_repayment_interest  string COMMENT '实还利息(元)',
real_repayment_cost      string COMMENT '实还费用(元)',
real_repayment_off_date  string COMMENT '实际还清日期',
current_loan_balance     string COMMENT '当期贷款余额',
current_account_status   string COMMENT '当期账户状态',
liquidated               string COMMENT '违约金',
penalty_interest         string COMMENT '罚息',
compensation             string COMMENT '赔偿金',
repayment_fee            string COMMENT '提前还款手续费',
other_related_expenses   string COMMENT '其它相关费用',
message_uuid             string COMMENT '消息唯一标识',
message_timestamp        string COMMENT '消息生产时间',
insert_timestamp         string COMMENT '入库时间'

)
COMMENT '实际还款信息_07'
row format delimited
fields terminated by '\001'

CREATE TABLE if not exists ods_starconnect.08_starconnect_assets_deal_process_info (
project_no           string COMMENT '项目编号',
institutions_no      string COMMENT '机构编号',
iou_no               string COMMENT '借据号',
disposal_status      string COMMENT '处置状态',
disposal_type        string COMMENT '处置类型',
litigation_node      string COMMENT '诉讼节点',
litigation_node_time string COMMENT '诉讼节点时间',
disposal_result      string COMMENT '处置结果',
message_uuid         string COMMENT '消息唯一标识',
message_timestamp    string COMMENT '消息生产时间',
insert_timestamp     string COMMENT '入库时间'

)
COMMENT '资产处置过程信息_08'
row format delimited
fields terminated by '\001'

CREATE TABLE if not exists ods_starconnect.09_starconnect_assets_supplement_transaction_info (
project_no              string COMMENT '项目编号',
institutions_no         string COMMENT '机构编号',
iou_no                  string COMMENT '借据号',
transation_type         string COMMENT '交易类型',
transation_reason       string COMMENT '交易原因',
transation_date         string COMMENT '交易日期',
transation_total_amount string COMMENT '交易总金额',
principal               string COMMENT '本金',
interest                string COMMENT '利息',
penalty_interest        string COMMENT '罚息',
other_cost              string COMMENT '其他费用',
message_uuid            string COMMENT '消息唯一标识',
message_timestamp       string COMMENT '消息生产时间',
insert_timestamp        string COMMENT '入库时间'

)
COMMENT '资产补充交易信息_09'
row format delimited
fields terminated by '\001'

CREATE TABLE if not exists ods_starconnect.10_starconnect_assets_reconciliation_info (
project_no                        string  COMMENT '项目编号',
institutions_no                   string  COMMENT '机构编号',
iou_no                            string  COMMENT '借据号',
ischange_repayment_plan           string  COMMENT '是否变更还款计划',
change_time                       string  COMMENT '变更时间',
loan_total_amount                 string  COMMENT '贷款总金额(元)',
loan_annual_interest_rate         string  COMMENT '贷款年利率(%)',
total_nper                        string  COMMENT '总期数',
returned_nper                     string  COMMENT '已还期数',
surplus_nper                      string  COMMENT '剩余期数',
remaining_principal               string  COMMENT '剩余本金(元)',
residual_interest                 string  COMMENT '剩余利息(元)',
residual_other_cost               string  COMMENT '剩余其他费用(元)',
next_repayment_date               string  COMMENT '下一期应还款日',
assets_status                     string  COMMENT '资产状态',
settle_reason                     string  COMMENT '结清原因',
current_overdue_principal         string  COMMENT '当前逾期本金',
current_overdue_interest          string  COMMENT '当前逾期利息',
current_overdue_cost              string  COMMENT '当前逾期费用',
current_overdue_days              string  COMMENT '当前逾期天数(天)',
cumulative_overdue_days           string  COMMENT '累计逾期天数',
history_cumulative_overdue_days   string  COMMENT '历史累计逾期天数',
history_single_overdue_max_days   string  COMMENT '历史单次最长逾期天数(天)',
current_overdue_nper              string  COMMENT '当前逾期期数',
cumulative_overdue_nper           string  COMMENT '累计逾期期数',
history_single_overdue_max_nper   string  COMMENT '历史单次最长逾期期数',
history_maximum_overdue_principal string  COMMENT '历史最大逾期本金',
data_fetch_date                   string  COMMENT '数据提取日',
message_uuid                      string  COMMENT '消息唯一标识',
message_timestamp                 string  COMMENT '消息生产时间',
insert_timestamp                  string  COMMENT '入库时间',
current_consecutive_overdue_days  string  COMMENT '当前连续逾期天数',
maximum_consecutive_overdue_days  string  COMMENT '最长连续逾期天数',
longest_single_overdue_days       string  COMMENT '最长单期逾期天数'
) COMMENT '资产对账信息_10'
row format delimited fields terminated by '\001'
##字段新增

CREATE TABLE if not exists ods_starconnect.11_starconnect_project_reconciliation_info (
project_no                                        string COMMENT '项目编号',
institutions_no                                   string COMMENT '机构编号',
loan_total_number                                 string COMMENT '贷款总笔数',
loan_remaining_principal                          string COMMENT '贷款剩余本金',
loan_contract_total_amount                        string COMMENT '贷款合同总金额',
1to7_overdue_assets_number                        string COMMENT '1~7逾期资产数',
1to7_overdue_assets_remaining_unpaid_principal    string COMMENT '1~7逾期资产剩余未还本金',
8to30_overdue_assets_number                       string COMMENT '8~30逾期资产数',
8to30_overdue_assets_remaining_unpaid_principal   string COMMENT '8~30逾期资产剩余未还本金',
31to60_overdue_assets_number                      string COMMENT '31~60逾期资产数',
31to60_overdue_assets_remaining_unpaid_principal  string COMMENT '31~60逾期资产剩余未还本金',
61to90_overdue_assets_number                      string COMMENT '61~90逾期资产数',
61to90_overdue_assets_remaining_unpaid_principal  string COMMENT '61~90逾期资产剩余未还本金',
91to180_overdue_assets_number                     string COMMENT '91~180逾期资产数',
91to180_overdue_assets_remaining_unpaid_principal string COMMENT '91~180逾期资产剩余未还本金',
180plus_overdue_assets_number                     string COMMENT '180+逾期资产数',
180plus_overdue_assets_remaining_unpaid_principal string COMMENT '180+逾期资产剩余未还本金',
cunrent_new_loan_number                           string COMMENT '当日新增贷款笔数',
cunrent_new_loan_total_amount                     string COMMENT '当日新增贷款总金额',
cunrent_actual_return_assets_number               string COMMENT '当日实还资产笔数',
cunrent_actual_return_total_amount                string COMMENT '当日实还总金额',
current_buyback_assets_number                     string COMMENT '当日回购资产笔数',
current_buyback_total_amount                      string COMMENT '当日回购总金额',
current_disposal_assets_number                    string COMMENT '当日处置资产笔数',
current_disposal_total_amount                     string COMMENT '当日处置回收总金额',
current_balance_assets_number                     string COMMENT '当日差额补足资产笔数',
current_balance_total_amount                      string COMMENT '当日差额补足总金额',
current_compensation_assets_number                string COMMENT '当日代偿资产笔数',
current_compensation_total_amount                 string COMMENT '当日代偿总金额',
data_fetch_date                                   string COMMENT '数据提取日',
message_uuid                                      string COMMENT '消息唯一标识',
message_timestamp                                 string COMMENT '消息生产时间',
insert_timestamp                                  string COMMENT '入库时间'

)
COMMENT '项目对账信息_11'
row format delimited
fields terminated by '\001'


CREATE TABLE if not exists ods_starconnect.12_starconnect_enterprise_info (
project_no                                string COMMENT '项目编号',
institutions_no                           string COMMENT '机构编号',
iou_no                                    string COMMENT '借据号',
contract_role                             string COMMENT '合同角色',
enterprise_name                           string COMMENT '企业姓名',
industrial_commercial_registration_number string COMMENT '工商注册号',
organization_code                         string COMMENT '组织机构代码',
taxpayer_identification_no                string COMMENT '纳税人识别号',
uniform_credit_code                       string COMMENT '统一信用代码',
registered_address                        string COMMENT '注册地址',
message_uuid                              string COMMENT '消息唯一标识',
message_timestamp                         string COMMENT '消息生产时间',
insert_timestamp                          string COMMENT '入库时间'

)
COMMENT '企业信息_12'
row format delimited
fields terminated by '\001'


CREATE TABLE if not exists ods_starconnect.13_starconnect_mortgage_house_info (
project_no                                         string COMMENT '项目编号',
institutions_no                                    string COMMENT '机构编号',
iou_no                                             string COMMENT '借据号',
pawn_no                                            string COMMENT '抵押物编号',
pawn_name                                          string COMMENT '抵押物名称',
pawn_describe                                      string COMMENT '抵押物描述',
pawn_handle_status                                 string COMMENT '抵押办理状态',
pawn_sequence                                      string COMMENT '抵押顺位',
pawn_balance                                       string COMMENT '前手抵押余额',
pawn_type                                          string COMMENT '抵押类型',
owner_name                                         string COMMENT '所有权人姓名',
owner_certificates_type                            string COMMENT '所有权人证件类型',
owner_id_card_no                                   string COMMENT '所有权人证件号码',
owner_occupation                                   string COMMENT '所有人职业(0-国家机关、党群组织、企业、事业单位负责人 1-专业技术人员 3-办事人员和有关人员 4-商业、服务业人员 5-农、林、牧、渔、水利业生产人员 6-生产、运输设备操作人员及有关人员 X-军人 Y-不便分类的其他从业人员 Z-未知)',
is_owner_pawn                                      string COMMENT '押品是否为所有权人/借款人名下唯一住所(1-否 2-是，有第三方可提供住所 3-是，无第三方可提供住所 9-未知)',
building_area                                      string COMMENT '房屋建筑面积',
building_age                                       string COMMENT '楼龄',
building_located_province                          string COMMENT '房屋所在省',
building_located_city                              string COMMENT '房屋所在城市',
building_located_county                            string COMMENT '房屋所在区县',
building_address                                   string COMMENT '房屋地址',
property_right                                     string COMMENT '产权年限',
house_purchase_contract_no                         string COMMENT '购房合同编号',
warrant_type                                       string COMMENT '权证类型',
property_ownership_certificate_no                  string COMMENT '房产证编号',
house_other_warrants_no                            string COMMENT '房屋他项权证编号',
house_type                                         string COMMENT '房屋类别(01-普通住宅 02-非普通住宅-经济适用房、保障房、福利房、小产权房等 03-别墅 04-商铺 05-70年公寓 06-50年公寓 07-商住 08-商住两用 09-办公 00-其他)',
ishave_joint_owners_property_right                 string COMMENT '是否有产权共有人',
owners_property_right_know                         string COMMENT '产权共有人知情情况(1-已共同签署《信托贷款合同》及抵押合同，需填写共同借款人信息 2-已出具同意房屋抵押登记的相关文件 3-未签署合同且未出具同意文件 9-无产权共有人)',
pawn_registration                                  string COMMENT '抵押登记办理',
compulsory_notarization                            string COMMENT '强制执行公证',
network_arbitration_office_arbitration_certificate string COMMENT '网络仲裁办仲裁证明',
evaluation_company_valuation                       string COMMENT '评估价格-评估公司(元)',
housing_agency_valuation                           string COMMENT '评估价格-房屋中介(元)',
internal_evaluation_valuation                      string COMMENT '评估价格-原始权益日内部评估(元)',
house_sale_amount                                  string COMMENT '房屋销售价格(元)',
message_uuid                                       string COMMENT '消息唯一标识',
message_timestamp                                  string COMMENT '消息生产时间',
insert_timestamp                                   string COMMENT '入库时间'

)
COMMENT '抵押物(房)信息_13'
row format delimited
fields terminated by '\001'



/**
 * 数据库存储的转换语句
 */
/* file-01 */
`项目编号` as project_no,
`机构编号` as institutions_no,
`借据号` as iou_no,
`资产类型` as assets_type,
`贷款合同编号` as loan_contract_no,
`产品编号` as product_no,
`贷款总金额(元)` as loan_total_amount,
`利率类型` as interest_rate_type,
`贷款年利率(%)` as loan_annual_interest_rate,
`还款方式` as repayment_mode,
`还款频率` as repayment_frequency,
`总期数` as total_nper,
`总收益率(%)` as total_yield,
`剩余本金(元)` as remaining_principal,
`剩余利息(元)` as residual_interest,
`剩余其他费用(元)` as remaining_other_expenses,
`首付款金额(元)` as first_payment,
`首付比例(%)` as first_payment_rate,
`尾付款金额(元)` as final_payment_amount,
`尾付比例(%)` as final_payment_rate,
`手续费` as poundage,
`手续费利率` as poundage_rate,
`手续费扣款方式` as fee_deduction_mode,
`结算手续费率` as settlement_fee_rate,
`补贴手续费` as subsidy_fee,
`贴息金额` as discount_amount,
`保证金比例` as security_deposit_ratio,
`保证金` as security_deposit,
`保证金冲抵方式` as security_deposit_mode,
`帐户管理费` as account_manage_cost,
`抵押率(%)` as mortgage_rate,
`合同开始时间` as contract_start_time,
`合同结束时间` as contract_end_time,
`实际放款时间` as actual_loan_time,
`首次还款时间` as first_payment_time,
`贷款还款日` as loan_payment_time,
`最后一次预计扣款时间` as latest_loan_payment_time,
`月还款额` as month_payment_amount,
`贷款用途` as loan_use,
`担保方式` as guarantee_mode,
`合同状态` as contract_status,
`累计逾期期数` as cumulative_overdue_nper,
`历史最高逾期天数` as history_highest_overdue_daysnum,
`历史累计逾期天数` as history_cumulative_overdue_daysnu,
`当前逾期天数` as current_cumulative_daysnum,
`申请状态代码` as application_status_code,
`申请渠道` as application_channel,
`申请地点` as application_position,
`借款人状态` as borrower_status,
`经销商名称` as dealer_name,
`经销商卖场地址` as dealer_store_position,
`店面省份` as store_province,
`店面城市` as store_city,
`数据提取日` as data_extract_day,
`UUID` as message_uuid,
`TIMESTAMP` as message_timestamp,
`产品方案名称` as product_plan_name,
`申请用途` as application_use,
FROM_UNIXTIME(UNIX_TIMESTAMP()) as insert_timestamp

/* file-02 */
`项目编号` as project_no,
`机构编号` as institutions_no,
`借据号` as iou_no,
`客户姓名` as customer_name,
`身份证号` as id_card_no,
`手机号` as cell_phone_number,
`年龄` as age,
`性别` as sex,
`婚姻状况` as marriage_status,
`子女数量` as children_number,
`工作年限` as working_life,
`客户类型` as customer_type,
`学历` as education,
`学位` as degree,
`是否具有完全民事行为能力` as is_civil_capabiliy,
`居住状态` as living_status,
`客户居住所在省` as customer_living_province,
`客户居住所在市` as customer_living_city,
`客户居住地址` as customer_living_address,
`客户户籍所在省` as customer_census_register_province,
`客户户籍所在市` as customer_census_register_city,
`客户户籍地址` as customer_census_register_address,
`通讯邮编` as communication_postal_code,
`客户通讯地址` as customer_communication_address,
`客户职业` as customer_occuopation,
`工作状态` as working_status,
`职务` as working_level,
`职称` as working_title,
`借款人行业` as borrower_industry,
`是否有车` as ishave_car,
`是否有按揭车贷` as ishave_mortgage_car_loan,
`是否有房` as ishave_house,
`是否有按揭房贷` as ishave_mortgage_house_loan,
`是否有信用卡` as ishave_credit_car,
`信用卡额度` as credit_car_limit,
`年收入(元)` as annual_income,
`内部信用等级` as internal_credit_grade,
`黑名单等级` as black_list_grade,
`单位名称` as unit_name,
`固定电话` as fixed_telephone,
`邮编` as postal_code,
`单位详细地址` as unit_detail_address,
`UUID` as message_uuid,
`TIMESTAMP` as message_timestamp,
FROM_UNIXTIME(UNIX_TIMESTAMP()) as insert_timestamp

/* file-03，待数据验证 */
`项目编号` as project_no,
`机构编号` as institutions_no,
`借据号` as iou_no,
`合同角色` as contract_role,
`客户姓名` as customer_name,
`身份证号` as id_card_no,
`手机号` as cell_phone_number,
`年龄` as age,
`性别` as sex,
`与主借款人关系` as relationship_master_borrower,
`职业` as occupation,
`工作状态` as working_status,
`年收入(元)` as annual_income,
`通讯地址` as communication_address,
`单位地址` as unit_address,
`单位联系方式` as unit_contact_info,
`UUID` as message_uuid,
`TIMESTAMP` as message_timestamp,
FROM_UNIXTIME(UNIX_TIMESTAMP()) as insert_timestamp


/* file-04，待数据验证 */
`项目编号` as project_no,
`机构编号` as institutions_no,
`借据号` as iou_no,
`抵押物编号` as pawn_no,
`抵押办理状态` as pawn_handle_status,
`抵押顺位` as pawn_sequence,
`车辆性质` as car_property,
`融资方式` as financing_type,
`担保方式` as guarantee_type,
`评估价格(元)` as assessment_price,
`车辆销售价格(元)` as car_sale_price,
`新车指导价(元)` as newcar_guidance_price,
`投资总额(元)` as investmen_total_sum,
`购置税金额(元)` as purchase_tax_money,
`保险种类` as insurance_type,
`汽车保险总费用` as car_insurance_total_sum,
`手续总费用(元)` as poundage_total_sum,
`累计车辆过户次数` as cumulative_car_transfer_frequency,
`一年内车辆过户次数` as annual_car_transfer_frequency,
`责信保费用1` as responsibility_credit_premium1,
`责信保费用2` as responsibility_credit_premium2,
`车类型` as car_type,
`车架号` as farme_no,
`发动机号` as engine_no,
`GPS编号` as gps_no,
`GPS费用` as gps_cost,
`车牌号码` as car_no,
`车辆品牌` as car_brand,
`车系` as car_series,
`车型` as car_models,
`车龄` as car_age,
`车辆能源类型` as car_energy_type,
`生产日期` as product_date,
`里程数` as mileage,
`注册日期` as registration_date,
`车辆购买地` as car_purchase_place,
`车辆颜色` as car_color,
`UUID` as message_uuid,
`TIMESTAMP` as message_timestamp,
FROM_UNIXTIME(UNIX_TIMESTAMP()) as insert_timestamp


/* file-05 */
`项目编号` as project_no,
`机构编号` as institutions_no,
`借据号` as iou_no,
`期次` as period,
`应还款日` as repayment_date,
`应还本金(元)` as repayment_principal,
`应还利息(元)` as repayment_interest,
`应还费用(元)` as repayment_cost,
`生效日期` as effected_date,
`UUID` as message_uuid,
`TIMESTAMP` as message_timestamp,
FROM_UNIXTIME(UNIX_TIMESTAMP()) as insert_timestamp


/* file-06 */
`项目编号` as project_no,
`机构编号` as institutions_no,
`借据号` as iou_no,
`交易渠道` as transation_channel,
`交易类型` as transation_type,
`订单号` as order_no,
`订单金额(元)` as order_price,
`交易币种` as transation_currency,
`姓名` as name,
`银行帐号` as bank_account,
`交易时间` as trading_time,
`交易状态` as trading_status,
`交易摘要` as trading_abstract,
`确认还款日期` as confirm_repayment_date,
`UUID` as message_uuid,
`TIMESTAMP` as message_timestamp,
FROM_UNIXTIME(UNIX_TIMESTAMP()) as insert_timestamp


/* file-07 */
`项目编号` as project_no,
`机构编号` as institutions_no,
`借据号` as iou_no,
`期次` as period,
`应还款日` as repayment_date,
`应还本金(元)` as repayment_principal,
`应还利息(元)` as repayment_interest,
`应还费用(元)` as repayment_cost,
`还款类型` as repayment_type,
`实还执行利率` as real_interest_rate,
`实还本金(元)` as real_repayment_principal,
`实还利息(元)` as real_repayment_interest,
`实还费用(元)` as real_repayment_cost,
`实际还清日期` as real_repayment_off_date,
`当期贷款余额` as current_loan_balance,
`当期账户状态` as current_account_status,
`违约金` as liquidated,
`罚息` as penalty_interest,
`赔偿金` as compensation,
`提前还款手续费` as repayment_fee,
`其它相关费用` as other_related_expenses,
`UUID` as message_uuid,
`TIMESTAMP` as message_timestamp,
FROM_UNIXTIME(UNIX_TIMESTAMP()) as insert_timestamp

/* file-08，待数据验证 */
`项目编号` as project_no,
`机构编号` as institutions_no,
`借据号` as iou_no,
`处置状态` as disposal_status,
`处置类型` as disposal_type,
`诉讼节点` as litigation_node,
`诉讼节点时间` as litigation_node_time,
`处置结果` as disposal_result,
`UUID` as message_uuid,
`TIMESTAMP` as message_timestamp,
FROM_UNIXTIME(UNIX_TIMESTAMP()) as insert_timestamp


/* file-09，待数据验证 */
`项目编号` as project_no,
`机构编号` as institutions_no,
`借据号` as iou_no,
`交易类型` as transation_type,
`交易原因` as transation_reason,
`交易日期` as transation_date,
`交易总金额` as transation_total_amount,
`本金` as principal,
`利息` as interest,
`罚息` as penalty_interest,
`其他费用` as other_cost,
`UUID` as message_uuid,
`TIMESTAMP` as message_timestamp,
FROM_UNIXTIME(UNIX_TIMESTAMP()) as insert_timestamp


/* file-10 */
`项目编号` as project_no,
`机构编号` as institutions_no,
`借据号` as iou_no,
`是否变更还款计划` as ischange_repayment_plan,
`变更时间` as change_time,
`贷款总金额(元)` as loan_total_amount,
`贷款年利率(%)` as loan_annual_interest_rate,
`总期数` as total_nper,
`已还期数` as returned_nper,
`剩余期数` as surplus_nper,
`剩余本金(元)` as remaining_principal,
`剩余利息(元)` as residual_interest,
`剩余其他费用(元)` as residual_other_cost,
`下一期应还款日` as next_repayment_date,
`资产状态` as assets_status,
`结清原因` as settle_reason,
`当前逾期本金` as current_overdue_principal,
`当前逾期利息` as current_overdue_interest,
`当前逾期费用` as current_overdue_cost,
`当前逾期天数(天)` as current_overdue_days,
`累计逾期天数` as cumulative_overdue_days,
`历史累计逾期天数` as history_cumulative_overdue_days,
`历史单次最长逾期天数(天)` as history_single_overdue_max_days,
`当前逾期期数` as current_overdue_nper,
`累计逾期期数` as cumulative_overdue_nper,
`历史单次最长逾期期数` as history_single_overdue_max_nper,
`历史最大逾期本金` as history_maximum_overdue_principal,
`数据提取日` as data_fetch_date,
`UUID` as message_uuid,
`TIMESTAMP` as message_timestamp,
FROM_UNIXTIME(UNIX_TIMESTAMP()) as insert_timestamp,
`当前连续逾期天数` as current_consecutive_overdue_days,
`最长连续逾期天数` as maximum_consecutive_overdue_days,
`最长单期逾期天数` as longest_single_overdue_days


/* file-11 */
`项目编号` as project_no,
`机构编号` as institutions_no,
`贷款总笔数` as loan_total_number,
`贷款剩余本金` as loan_remaining_principal,
`贷款合同总金额` as loan_contract_total_amount,
`1~7逾期资产数` as 1to7_overdue_assets_number,
`1~7逾期资产剩余未还本金` as 1to7_overdue_assets_remaining_unpaid_principal,
`8~30逾期资产数` as 8to30_overdue_assets_number,
`8~30逾期资产剩余未还本金` as 8to30_overdue_assets_remaining_unpaid_principal,
`31~60逾期资产数` as 31to60_overdue_assets_number,
`31~60逾期资产剩余未还本金` as 31to60_overdue_assets_remaining_unpaid_principal,
`61~90逾期资产数` as 61to90_overdue_assets_number,
`61~90逾期资产剩余未还本金` as 61to90_overdue_assets_remaining_unpaid_principal,
`91~180逾期资产数` as 91to180_overdue_assets_number,
`91~180逾期资产剩余未还本金` as 91to180_overdue_assets_remaining_unpaid_principal,
`180+逾期资产数` as 180plus_overdue_assets_number,
`180+逾期资产剩余未还本金` as 180plus_overdue_assets_remaining_unpaid_principal,
`当日新增贷款笔数` as cunrent_new_loan_number,
`当日新增贷款总金额` as cunrent_new_loan_total_amount,
`当日实还资产笔数` as cunrent_actual_return_assets_number,
`当日实还总金额` as cunrent_actual_return_total_amount,
`当日回购资产笔数` as current_buyback_assets_number,
`当日回购总金额` as current_buyback_total_amount,
`当日处置资产笔数` as current_disposal_assets_number,
`当日处置回收总金额` as current_disposal_total_amount,
`当日差额补足资产笔数` as current_balance_assets_number,
`当日差额补足总金额` as current_balance_total_amount,
`当日代偿资产笔数` as current_compensation_assets_number,
`当日代偿总金额` as current_compensation_total_amount,
`数据提取日` as data_fetch_date,
`UUID` as message_uuid,
`TIMESTAMP` as message_timestamp,
FROM_UNIXTIME(UNIX_TIMESTAMP()) as insert_timestamp


/* file-12，待数据验证 */
`项目编号` as project_no,
`机构编号` as institutions_no,
`借据号` as iou_no,
`合同角色` as contract_role,
`企业姓名` as enterprise_name,
`工商注册号` as industrial_commercial_registration_number,
`组织机构代码` as organization_code,
`纳税人识别号` as taxpayer_identification_no,
`统一信用代码` as uniform_credit_code,
`注册地址` as registered_address,
`UUID` as message_uuid,
`TIMESTAMP` as message_timestamp,
FROM_UNIXTIME(UNIX_TIMESTAMP()) as insert_timestamp


/* file-13，待数据验证 */
`项目编号` as project_no,
`机构编号` as institutions_no,
`借据号` as iou_no,
`抵押物编号` as pawn_no,
`抵押物名称` as pawn_name,
`抵押物描述` as pawn_describe,
`抵押办理状态` as pawn_handle_status,
`抵押顺位` as pawn_sequence,
`前手抵押余额` as pawn_balance,
`抵押类型` as pawn_type,
`所有权人姓名` as owner_name,
`所有权人证件类型` as owner_certificates_type,
`所有权人证件号码` as owner_id_card_no,
`所有人职业` as owner_occupation,
`押品是否为所有权人/借款人名下唯一住所` as is_owner_pawn,
`房屋建筑面积` as building_area,
`楼龄` as building_age,
`房屋所在省` as building_located_province,
`房屋所在城市` as building_located_city,
`房屋所在区县` as building_located_county,
`房屋地址` as building_address,
`产权年限` as property_right,
`购房合同编号` as house_purchase_contract_no,
`权证类型` as warrant_type,
`房产证编号` as property_ownership_certificate_no,
`房屋他项权证编号` as house_other_warrants_no,
`房屋类别` as house_type,
`是否有产权共有人` as ishave_joint_owners_property_right,
`产权共有人知情情况` as owners_property_right_know,
`抵押登记办理` as pawn_registration,
`强制执行公证` as compulsory_notarization,
`网络仲裁办仲裁证明` as network_arbitration_office_arbitration_certificate,
`评估价格-评估公司(元)` as evaluation_company_valuation,
`评估价格-房屋中介(元)` as housing_agency_valuation,
`评估价格-原始权益日内部评估(元)` as internal_evaluation_valuation,
`房屋销售价格(元)` as house_sale_amount,
`UUID` as message_uuid,
`TIMESTAMP` as message_timestamp,
FROM_UNIXTIME(UNIX_TIMESTAMP()) as insert_timestamp


/**
 *ods层数据去重预处理
 *
 *
 */
-- 01_distinct_starconnect_loancontract 按 iou_no 去重
insert overwrite TABLE ods_starconnect.`01_distinct_starconnect_loancontract`
-- select iou_no,count(iou_no) as cnt from (
  select * from (
    select * ,row_number() over(partition by iou_no order by message_timestamp desc) as od from ods_starconnect.`01_starconnect_loancontract`
  ) t where t.od=1
-- ) as tmp group by iou_no having cnt > 1
;
-- 02_distinct_starconnect_mainborrower 按 iou_no 去重
insert overwrite TABLE ods_starconnect.`02_distinct_starconnect_mainborrower`
-- select iou_no,count(iou_no) as cnt from (
  select * from(
    select * ,row_number() over(partition by iou_no order by message_timestamp desc) as od from ods_starconnect.`02_starconnect_mainborrower`
  ) t where t.od=1
-- ) as tmp group by iou_no having cnt > 1
;
-- 03_distinct_starconnect_correlation_person_info 按 iou_no 去重
insert overwrite TABLE ods_starconnect.`03_distinct_starconnect_correlation_person_info`
-- select iou_no,count(iou_no) as cnt from (
  select * from(
    select * ,row_number() over(partition by iou_no order by message_timestamp desc) as od from ods_starconnect.`03_starconnect_correlation_person_info`
  ) t where t.od=1
-- ) as tmp group by iou_no having cnt > 1
;
-- 04_distinct_starconnect_mortgage_car_info 按 pawn_no 去重
insert overwrite TABLE ods_starconnect.`04_distinct_starconnect_mortgage_car_info`
-- select iou_no,count(iou_no) as cnt from (
  select * from(
    select * ,row_number() over(partition by pawn_no order by message_timestamp desc) as od from ods_starconnect.`04_starconnect_mortgage_car_info`
  ) t where t.od=1
-- ) as tmp group by iou_no having cnt > 1
;
-- 05_distinct_starconnect_repayment_plan_info 按 iou_no,period 去重
insert overwrite TABLE ods_starconnect.`05_distinct_starconnect_repayment_plan_info`
-- select iou_no,count(iou_no) as cnt from (
  select * from(
    select * ,row_number() over(partition by iou_no,period order by message_timestamp desc) as od from ods_starconnect.`05_starconnect_repayment_plan_info`
  ) t where t.od=1
-- ) as tmp group by iou_no,period having cnt > 1
;
-- 06_distinct_starconnect_assets_paymentflow_info 按 order_no 去重
insert overwrite TABLE ods_starconnect.`06_distinct_starconnect_assets_paymentflow_info`
-- select iou_no,count(iou_no) as cnt from (
  select * from(
    select * ,row_number() over(partition by order_no order by message_timestamp desc) as od from ods_starconnect.`06_starconnect_assets_paymentflow_info`
  ) t where t.od=1
-- ) as tmp group by iou_no,order_no having cnt > 1
;
-- 07_distinct_starconnect_actual_repayment_info 按 iou_no,period 去重
insert overwrite TABLE ods_starconnect.`07_distinct_starconnect_actual_repayment_info`
-- select iou_no,count(iou_no) as cnt from (
  select * from(
    select * ,row_number() over(partition by iou_no,period order by message_timestamp desc) as od from ods_starconnect.`07_starconnect_actual_repayment_info`
  ) t where t.od=1
-- ) as tmp group by iou_no,period having cnt > 1
;
-- 08_distinct_starconnect_assets_deal_process_info 按 iou_no,litigation_node 去重
insert overwrite TABLE ods_starconnect.`08_distinct_starconnect_assets_deal_process_info`
-- select iou_no,count(iou_no) as cnt from (
  select * from(
    select * ,row_number() over(partition by iou_no,litigation_node order by message_timestamp desc) as od from ods_starconnect.`08_starconnect_assets_deal_process_info`
  ) t where t.od=1
-- ) as tmp group by iou_no having cnt > 1
;
-- 09_distinct_starconnect_assets_supplement_transaction_info 按 iou_no,transation_type 去重
insert overwrite TABLE ods_starconnect.`09_distinct_starconnect_assets_supplement_transaction_info`
-- select iou_no,count(iou_no) as cnt from (
  select * from(
    select * ,row_number() over(partition by iou_no,transation_type order by message_timestamp desc) as od from ods_starconnect.`09_starconnect_assets_supplement_transaction_info`
  ) t where t.od=1
-- ) as tmp group by iou_no having cnt > 1
;
-- 10_distinct_starconnect_assets_reconciliation_info 按 iou_no,data_fetch_date 去重
insert overwrite TABLE ods_starconnect.`10_distinct_starconnect_assets_reconciliation_info`
-- select iou_no,count(iou_no) as cnt from (
  select * from(
    select * ,row_number() over(partition by iou_no,data_fetch_date order by message_timestamp desc) as od
    from ods_starconnect.`10_starconnect_assets_reconciliation_info`
  ) t where t.od=1
-- ) as tmp group by iou_no,data_fetch_date having cnt > 1
;
-- 11_distinct_starconnect_project_reconciliation_info 按 message_uuid 去重
insert overwrite TABLE ods_starconnect.`11_distinct_starconnect_project_reconciliation_info`
-- select message_uuid,count(message_uuid) as cnt from (
  select * from(
    select * ,row_number() over(partition by message_uuid order by message_timestamp desc) as od from ods_starconnect.`11_starconnect_project_reconciliation_info`
  ) t where t.od=1
-- ) as tmp group by message_uuid having cnt > 1
;
-- 12_distinct_starconnect_enterprise_info 按 iou_no 去重
insert overwrite TABLE ods_starconnect.`12_distinct_starconnect_enterprise_info`
-- select iou_no,count(iou_no) as cnt from (
  select * from(
    select * ,row_number() over(partition by iou_no order by message_timestamp desc) as od from ods_starconnect.`12_starconnect_enterprise_info`
  ) t where t.od=1
-- ) as tmp group by iou_no having cnt > 1
;
-- 13_distinct_starconnect_mortgage_house_info 按 property_ownership_certificate_no 去重 无数据
insert overwrite TABLE ods_starconnect.`13_distinct_starconnect_mortgage_house_info`
-- select iou_no,count(iou_no) as cnt from (
  select * from(
    select * ,row_number() over(partition by property_ownership_certificate_no order by message_timestamp desc) as od from ods_starconnect.`13_starconnect_mortgage_house_info`
  ) t where t.od=1
-- ) as tmp group by iou_no having cnt > 1
;















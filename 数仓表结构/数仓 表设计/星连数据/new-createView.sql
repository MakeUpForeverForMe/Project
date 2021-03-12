
--------------------------------------------------------凤金视图集合--------------------------------------------------------
-- 一.fj_credit_info   NULL是 bank_name
drop view if exists drip_loan_ods.fj_credit_info;
create view if not exists drip_loan_ods.fj_credit_info as
with t1 as(
select
aps_credit.apply_id as appl_no,
aps_application.product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
aps_application.phone_num as phone,
aps_application.id_num as id,
aps_application.channel,
cast(aps_credit.create_date_time as timestamp) as create_date_time,
cast(aps_credit.credit_loan_date as timestamp) as credit_loan_date,
cast(aps_credit.credit_loan_limit as bigint) as credit_loan_limit,
aps_credit.institution,
cast(aps_credit.paid_tenor as bigint) as paid_tenor,
cast(aps_credit.sequence as bigint) as sequence,
aps_application.update_date_time as update_date_time
from drip_loan_ods.app_application_aps_credit aps_credit
left join drip_loan_ods.app_application_aps_application aps_application
on aps_credit.apply_id = aps_application.apply_id),

t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
channel,
create_date_time,
credit_loan_date,
credit_loan_limit,
institution,
paid_tenor,
sequence,
row_number() over(partition by appl_no,sequence order by update_date_time desc) as rank
from t1)

select
appl_no,
product_code,
appl_time,
phone,
id,
channel,
create_date_time,
credit_loan_date,
credit_loan_limit,
institution,
paid_tenor,
sequence
from t2
where rank = 1;

-- 二.fj_house_info
drop view if exists drip_loan_ods.fj_house_info;
create view if not exists drip_loan_ods.fj_house_info as
with t1 as(
select
aps_house.apply_id as appl_no,
cast(aps_application.create_date_time as timestamp) as appl_time,
aps_application.phone_num as phone,
aps_application.id_num as id,
aps_application.channel,
cast(aps_house.create_date_time as timestamp) as create_date_time,
aps_house.house_address_detail,
aps_house.house_city,
aps_house.house_county,
aps_house.house_is,
aps_house.house_province,
aps_house.house_street,
aps_house.house_tenor,
aps_house.house_type,
cast(aps_house.mortgage_payment as decimal(15,2)) as mortgage_payment,
cast(aps_house.paid_tenor as bigint) as paid_tenor,
aps_application.update_date_time as update_date_time
from drip_loan_ods.app_application_aps_house aps_house
left join drip_loan_ods.app_application_aps_application aps_application
on aps_house.apply_id = aps_application.apply_id),


t2 as(
select
appl_no,
appl_time,
phone,
id,
channel,
create_date_time,
house_address_detail,
house_city,
house_county,
house_is,
house_province,
house_street,
house_tenor,
house_type,
mortgage_payment,
paid_tenor,
row_number() over(partition by appl_no order by update_date_time desc) as rank
from t1)


select
appl_no,
appl_time,
phone,
id,
channel,
create_date_time,
house_address_detail,
house_city,
house_county,
house_is,
house_province,
house_street,
house_tenor,
house_type,
mortgage_payment,
paid_tenor
from t2
where rank = 1;





-- 三.fj_insurance_info

drop view if exists drip_loan_ods.fj_insurance_info;
create view if not exists drip_loan_ods.fj_insurance_info as
with t1 as(
select
aps_insurance.apply_id as appl_no,
aps_application.product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
aps_application.phone_num as phone,
aps_application.id_num as id,
aps_application.channel,
cast(aps_insurance.create_date_time as timestamp) as create_date_time,
aps_insurance.company_name,
cast(aps_insurance.effective_date as timestamp) as effective_date,
cast(aps_insurance.payment_amt as decimal(15,2)) as payment_amt,
aps_insurance.payment_method,
cast(aps_insurance.payment_tenor as bigint) as payment_tenor,
cast(aps_insurance.sequence as bigint) as sequence,
aps_application.update_date_time as update_date_time
from drip_loan_ods.app_application_aps_insurance aps_insurance
left join drip_loan_ods.app_application_aps_application aps_application
on aps_insurance.apply_id = aps_application.apply_id),


t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
channel,
create_date_time,
company_name,
effective_date,
payment_amt,
payment_method,
payment_tenor,
sequence,
row_number() over(partition by appl_no,sequence order by update_date_time desc) as rank
from t1)



select
appl_no,
product_code,
appl_time,
phone,
id,
channel,
create_date_time,
company_name,
effective_date,
payment_amt,
payment_method,
payment_tenor,
sequence
from t2
where rank = 1;






-- 四.aps_profession
drop view if exists drip_loan_ods.fj_profession_info;
create view if not exists drip_loan_ods.fj_profession_info as
with t1 as(
select
aps_profession.apply_id as appl_no,
aps_application.product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
aps_application.phone_num as phone,
aps_application.id_num as id,
aps_application.channel,
cast(aps_profession.create_date_time as timestamp) as create_date_time,
aps_profession.profession_type,
cast(aps_profession.mortgage_payment as decimal(15,2)) as mortgage_payment,
aps_application.update_date_time as update_date_time
from drip_loan_ods.app_application_aps_profession aps_profession
left join drip_loan_ods.app_application_aps_application aps_application
on aps_profession.apply_id = aps_application.apply_id),


t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
channel,
create_date_time,
profession_type,
mortgage_payment,
row_number() over(partition by appl_no order by update_date_time desc) as rank
from t1)


select
appl_no,
product_code,
appl_time,
phone,
id,
channel,
create_date_time,
profession_type,
mortgage_payment
from t2
where rank = 1;




--------------------------------------------------------反欺诈视图集合--------------------------------------------------------

-- ===反欺诈，没有按顺序，标志的表名是关联后得到的表名
-- ===aps_application.apply_id
--  request_mapping_datasource.apply_no

-- 1.view_fraud_td_op_gps  已经第一次改
drop view if exists drip_loan_ods.fraud_td_op_gps;
create view if not exists drip_loan_ods.fraud_td_op_gps as
with t1 as(
select
fraud_td_op_gps.appl_no,
fraud_td_op_gps.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
fraud_td_op_gps.phone,
fraud_td_op_gps.id,
'' as cust_id,
aps_application.channel as channel,
fraud_td_op_gps.swift_no,
fraud_td_op_gps.req_id,
cast(fraud_td_op_gps.report_time as timestamp) as report_time,
fraud_td_op_gps.inquire_module,
cast(fraud_td_op_gps.latitude as decimal(15,6)) as latitude,
cast(fraud_td_op_gps.longitude as decimal(15,6)) as longitude
from drip_loan_ods.iboxchain_db_fraud_td_op_gps fraud_td_op_gps
left join drip_loan_ods.app_application_aps_application aps_application
on fraud_td_op_gps.appl_no = aps_application.apply_id),

t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
latitude,
longitude,
row_number() over(partition by appl_no,req_id order by report_time desc) as rank
from t1)

select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
latitude,
longitude
from t2
where rank = 1;


-- 2.view_fraud_td_op_ios  已经第一次改
drop view if exists drip_loan_ods.fraud_td_op_ios;
create view if not exists drip_loan_ods.fraud_td_op_ios as
with t1 as(
select
fraud_td_op_ios.appl_no,
fraud_td_op_ios.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
fraud_td_op_ios.phone,
fraud_td_op_ios.id,
'' as cust_id,
aps_application.channel,
fraud_td_op_ios.swift_no,
fraud_td_op_ios.req_id,
cast(fraud_td_op_ios.report_time as timestamp) as report_time,
fraud_td_op_ios.inquire_module,
fraud_td_op_ios.token_id,
fraud_td_op_ios.device_id,
fraud_td_op_ios.os,
fraud_td_op_ios.fp_version,
fraud_td_op_ios.os_version,
fraud_td_op_ios.idfa,
fraud_td_op_ios.idfv,
fraud_td_op_ios.uuid,
fraud_td_op_ios.device_name,
fraud_td_op_ios.country_iso,
fraud_td_op_ios.mcc,
fraud_td_op_ios.mnc,
fraud_td_op_ios.bundle_id,
fraud_td_op_ios.app_version,
fraud_td_op_ios.carrier,
fraud_td_op_ios.wifi_ip,
fraud_td_op_ios.mac,
fraud_td_op_ios.ssid,
fraud_td_op_ios.bssid,
fraud_td_op_ios.wifi_netmask,
fraud_td_op_ios.vpn_ip,
fraud_td_op_ios.vpn_netmask,
fraud_td_op_ios.cell_ip,
fraud_td_op_ios.network_type,
fraud_td_op_ios.proxy_type,
fraud_td_op_ios.proxy_url,
fraud_td_op_ios.platform,
cast(from_unixtime(cast(fraud_td_op_ios.current_time/1000000 as bigint)) as timestamp) as current_time,
cast(fraud_td_op_ios.up_time/1000 as decimal(15,3)) as up_time,
cast(from_unixtime(cast(fraud_td_op_ios.boot_time/1000000 as bigint)) as timestamp) as boot_time,
fraud_td_op_ios.sign_md5,
fraud_td_op_ios.time_zone,
fraud_td_op_ios.languages,
fraud_td_op_ios.kernel_version,
cast(fraud_td_op_ios.brightness as bigint) as brightness,
fraud_td_op_ios.battery_status,
cast(fraud_td_op_ios.battery_level as decimal(15,1)) as battery_level,
cast(fraud_td_op_ios.memory as bigint) as memory,
cast(fraud_td_op_ios.total_space as bigint) as total_space,
cast(fraud_td_op_ios.free_space as bigint) as free_space,
fraud_td_op_ios.gps_switch,
fraud_td_op_ios.gps_auth_status,
fraud_td_op_ios.env,
fraud_td_op_ios.attached,
fraud_td_op_ios.true_ip,
fraud_td_op_ios.app_os,
fraud_td_op_ios.error
from drip_loan_ods.iboxchain_db_fraud_td_op_ios fraud_td_op_ios
left join drip_loan_ods.app_application_aps_application aps_application
on fraud_td_op_ios.appl_no = aps_application.apply_id),

t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
token_id,
device_id,
os,
fp_version,
os_version,
idfa,
idfv,
uuid,
device_name,
country_iso,
mcc,
mnc,
bundle_id,
app_version,
carrier,
wifi_ip,
mac,
ssid,
bssid,
wifi_netmask,
vpn_ip,
vpn_netmask,
cell_ip,
network_type,
proxy_type,
proxy_url,
platform,
current_time,
up_time,
boot_time,
sign_md5,
time_zone,
languages,
kernel_version,
brightness,
battery_status,
battery_level,
memory,
total_space,
free_space,
gps_switch,
gps_auth_status,
env,
attached,
true_ip,
app_os,
error,
row_number() over(partition by appl_no,req_id order by report_time desc) as rank
from t1)

select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
token_id,
device_id,
os,
fp_version,
os_version,
idfa,
idfv,
uuid,
device_name,
country_iso,
mcc,
mnc,
bundle_id,
app_version,
carrier,
wifi_ip,
mac,
ssid,
bssid,
wifi_netmask,
vpn_ip,
vpn_netmask,
cell_ip,
network_type,
proxy_type,
proxy_url,
platform,
current_time,
up_time,
boot_time,
sign_md5,
time_zone,
languages,
kernel_version,
brightness,
battery_status,
battery_level,
memory,
total_space,
free_space,
gps_switch,
gps_auth_status,
env,
attached,
true_ip,
app_os,
error
from t2
where rank = 1;

-- 3.fraud_td_op_android_cell  已经第一次改  已经第二次改
-- 去重逻辑修正
drop view if exists drip_loan_ods.fraud_td_op_android_cell;
create view if not exists drip_loan_ods.fraud_td_op_android_cell as
with t1 as(
select
fraud_td_op_android_cell.cell_id,
fraud_td_op_android_cell.appl_no,
fraud_td_op_android_cell.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
fraud_td_op_android_cell.phone,
fraud_td_op_android_cell.id,
'' as cust_id,
aps_application.channel as channel,
fraud_td_op_android_cell.swift_no,
fraud_td_op_android_cell.req_id,
cast(fraud_td_op_android_cell.report_time as timestamp) as report_time,
fraud_td_op_android_cell.inquire_module,
fraud_td_op_android_cell.lac,
fraud_td_op_android_cell.cid,
fraud_td_op_android_cell.nid,
fraud_td_op_android_cell.bid,
fraud_td_op_android_cell.sid,
fraud_td_op_android_cell.type
from drip_loan_ods.iboxchain_db_fraud_td_op_android_cell fraud_td_op_android_cell
left join drip_loan_ods.app_application_aps_application aps_application
on fraud_td_op_android_cell.appl_no = aps_application.apply_id),

t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
lac,
cid,
nid,
bid,
sid,
type,
row_number() over(partition by appl_no,cell_id order by report_time desc) as rank
from t1)

select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
lac,
cid,
nid,
bid,
sid,
type
from t2
where rank = 1;

-- 4.fraud_td_op_android_phone  已经第一次改  已经第二次改
-- 去重逻辑修正
drop view if exists drip_loan_ods.fraud_td_op_android_phone;
create view if not exists drip_loan_ods.fraud_td_op_android_phone as
with t1 as(
select
fraud_td_op_android_phone.phone_id,
fraud_td_op_android_phone.appl_no,
fraud_td_op_android_phone.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
fraud_td_op_android_phone.phone,
fraud_td_op_android_phone.id,
'' as cust_id,
aps_application.channel as channel,
fraud_td_op_android_phone.swift_no,
fraud_td_op_android_phone.req_id,
cast(fraud_td_op_android_phone.report_time as timestamp) as report_time,
fraud_td_op_android_phone.inquire_module,
fraud_td_op_android_phone.imsi,
fraud_td_op_android_phone.phone_number,
fraud_td_op_android_phone.imei,
fraud_td_op_android_phone.voice_mall,
fraud_td_op_android_phone.sim_serial,
fraud_td_op_android_phone.country_iso,
fraud_td_op_android_phone.carrier,
fraud_td_op_android_phone.mnc,
fraud_td_op_android_phone.mcc,
fraud_td_op_android_phone.sim_operator,
fraud_td_op_android_phone.phone_type,
fraud_td_op_android_phone.radio_type
from drip_loan_ods.iboxchain_db_fraud_td_op_android_phone fraud_td_op_android_phone
left join drip_loan_ods.app_application_aps_application aps_application
on fraud_td_op_android_phone.appl_no = aps_application.apply_id),

t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
imsi,
phone_number,
imei,
voice_mall,
sim_serial,
country_iso,
carrier,
mnc,
mcc,
sim_operator,
phone_type,
radio_type,
row_number() over(partition by appl_no,phone_id order by report_time desc) as rank
from t1)

select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
imsi,
phone_number,
imei,
voice_mall,
sim_serial,
country_iso,
carrier,
mnc,
mcc,
sim_operator,
phone_type,
radio_type
from t2
where rank = 1;

-- 5.fraud_td_op_android  已经第一次改  已经第二次改
drop view if exists drip_loan_ods.fraud_td_op_android;
create view if not exists drip_loan_ods.fraud_td_op_android as
with t1 as(
select
fraud_td_op_android.appl_no,
fraud_td_op_android.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
fraud_td_op_android.phone,
fraud_td_op_android.id,
'' as cust_id,
aps_application.channel as channel,
fraud_td_op_android.swift_no,
fraud_td_op_android.req_id,
cast(fraud_td_op_android.report_time as timestamp) as report_time,
fraud_td_op_android.inquire_module,
fraud_td_op_android.token_id,
fraud_td_op_android.device_id,
fraud_td_op_android.os,
fraud_td_op_android.fp_version,
fraud_td_op_android.fm_version,
fraud_td_op_android.sdk_version,
fraud_td_op_android.release_version,
fraud_td_op_android.model,
fraud_td_op_android.product,
fraud_td_op_android.brand,
fraud_td_op_android.serial_no,
fraud_td_op_android.display,
fraud_td_op_android.host,
fraud_td_op_android.device_name,
fraud_td_op_android.hardware,
fraud_td_op_android.tags,
fraud_td_op_android.device_svn,
fraud_td_op_android.wifi_ip,
fraud_td_op_android.wifi_mac,
fraud_td_op_android.ssid,
fraud_td_op_android.bssid,
fraud_td_op_android.gateway,
fraud_td_op_android.wifi_netmask,
fraud_td_op_android.proxy_info,
fraud_td_op_android.dns_address,
fraud_td_op_android.vpn_ip,
fraud_td_op_android.vpn_netmask,
fraud_td_op_android.cell_ip,
fraud_td_op_android.network_type,
cast(from_unixtime(cast(fraud_td_op_android.current_time/1000000 as bigint)) as timestamp) as current_time,
cast(fraud_td_op_android.up_time / 1000 as decimal(15,3)) as up_time,
cast(from_unixtime(cast(fraud_td_op_android.boot_time/1000000 as bigint)) as timestamp) as boot_time,
cast(fraud_td_op_android.active_time / 1000 as decimal(15,3)) as active_time,
fraud_td_op_android.root,
fraud_td_op_android.package_name,
fraud_td_op_android.apk_version,
fraud_td_op_android.sdk_md5,
fraud_td_op_android.sign_md5,
fraud_td_op_android.apk_md5,
fraud_td_op_android.time_zone,
fraud_td_op_android.language,
cast(fraud_td_op_android.brightness as bigint) as brightness,
fraud_td_op_android.battery_status,
cast(fraud_td_op_android.battery_level as decimal(15,1)) as battery_level,
cast(fraud_td_op_android.battery_temp as decimal(15,1)) as battery_temp,
fraud_td_op_android.screen_res,
fraud_td_op_android.font_hash,
fraud_td_op_android.blue_mac,
fraud_td_op_android.android_id,
cast(fraud_td_op_android.cpu_frequency as bigint) as cpu_frequency,
fraud_td_op_android.cpu_hardware,
fraud_td_op_android.cpu_type,
cast(fraud_td_op_android.total_memory as bigint) as total_memory,
cast(fraud_td_op_android.available_memory as bigint) as available_memory,
cast(fraud_td_op_android.total_storage as bigint) as total_storage,
cast(fraud_td_op_android.available_storage as bigint) as available_storage,
fraud_td_op_android.baseband_version,
fraud_td_op_android.allow_mockLocation,
fraud_td_op_android.true_ip,
fraud_td_op_android.app_os,
fraud_td_op_android.error,
fraud_td_op_android.kernel_version
from drip_loan_ods.iboxchain_db_fraud_td_op_android fraud_td_op_android
left join drip_loan_ods.app_application_aps_application aps_application
on fraud_td_op_android.appl_no = aps_application.apply_id),

t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
token_id,
device_id,
os,
fp_version,
fm_version,
sdk_version,
release_version,
model,
product,
brand,
serial_no,
display,
host,
device_name,
hardware,
tags,
device_svn,
wifi_ip,
wifi_mac,
ssid,
bssid,
gateway,
wifi_netmask,
proxy_info,
dns_address,
vpn_ip,
vpn_netmask,
cell_ip,
network_type,
current_time,
up_time,
boot_time,
active_time,
root,
package_name,
apk_version,
sdk_md5,
sign_md5,
apk_md5,
time_zone,
language,
brightness,
battery_status,
battery_level,
battery_temp,
screen_res,
font_hash,
blue_mac,
android_id,
cpu_frequency,
cpu_hardware,
cpu_type,
total_memory,
available_memory,
total_storage,
available_storage,
baseband_version,
allow_mockLocation,
true_ip,
app_os,
error,
kernel_version,
row_number() over(partition by appl_no,req_id order by report_time desc) as rank
from t1)

select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
token_id,
device_id,
os,
fp_version,
fm_version,
sdk_version,
release_version,
model,
product,
brand,
serial_no,
display,
host,
device_name,
hardware,
tags,
device_svn,
wifi_ip,
wifi_mac,
ssid,
bssid,
gateway,
wifi_netmask,
proxy_info,
dns_address,
vpn_ip,
vpn_netmask,
cell_ip,
network_type,
current_time,
up_time,
boot_time,
active_time,
root,
package_name,
apk_version,
sdk_md5,
sign_md5,
apk_md5,
time_zone,
language,
brightness,
battery_status,
battery_level,
battery_temp,
screen_res,
font_hash,
blue_mac,
android_id,
cpu_frequency,
cpu_hardware,
cpu_type,
total_memory,
available_memory,
total_storage,
available_storage,
baseband_version,
allow_mockLocation,
true_ip,
app_os,
error,
kernel_version
from t2
where rank = 1;

-- 6.fraud_td_op_web  第一次上视图  这个源表没数据
drop view if exists drip_loan_ods.fraud_td_op_web;
create view if not exists drip_loan_ods.fraud_td_op_web as
with t1 as(
select
fraud_td_op_web.appl_no,
fraud_td_op_web.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
fraud_td_op_web.phone,
fraud_td_op_web.id,
'' as cust_id,
aps_application.channel as channel,
fraud_td_op_web.swift_no,
fraud_td_op_web.req_id,
cast(fraud_td_op_web.report_time as timestamp) as report_time,
fraud_td_op_web.inquire_module,
fraud_td_op_web.token_id,
fraud_td_op_web.device_id,
fraud_td_op_web.browser,
fraud_td_op_web.browser_type,
fraud_td_op_web.browser_version,
fraud_td_op_web.canvas,
fraud_td_op_web.cookie_enabled,
fraud_td_op_web.device_type,
fraud_td_op_web.flash_enabled,
fraud_td_op_web.font_list_hash,
fraud_td_op_web.os,
fraud_td_op_web.plugin_list_hash,
fraud_td_op_web.referer,
fraud_td_op_web.screen_res,
fraud_td_op_web.smart_id,
fraud_td_op_web.tcp_os,
fraud_td_op_web.true_ip,
fraud_td_op_web.user_agent,
fraud_td_op_web.accept,
fraud_td_op_web.accept_encoding,
fraud_td_op_web.accept_language,
fraud_td_op_web.fp_version,
fraud_td_op_web.language_res,
fraud_td_op_web.time_zone,
fraud_td_op_web.screen,
fraud_td_op_web.web_debugger_status,
fraud_td_op_web.enabled_js,
fraud_td_op_web.app_os,
fraud_td_op_web.error
from drip_loan_ods.iboxchain_db_fraud_td_op_web fraud_td_op_web
left join drip_loan_ods.app_application_aps_application aps_application
on fraud_td_op_web.appl_no = aps_application.apply_id),

t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
token_id,
device_id,
browser,
browser_type,
browser_version,
canvas,
cookie_enabled,
device_type,
flash_enabled,
font_list_hash,
os,
plugin_list_hash,
referer,
screen_res,
smart_id,
tcp_os,
true_ip,
user_agent,
accept,
accept_encoding,
accept_language,
fp_version,
language_res,
time_zone,
screen,
web_debugger_status,
enabled_js,
app_os,
error,
row_number() over(partition by appl_no,req_id order by report_time desc) as rank
from t1)

select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
token_id,
device_id,
browser,
browser_type,
browser_version,
canvas,
cookie_enabled,
device_type,
flash_enabled,
font_list_hash,
os,
plugin_list_hash,
referer,
screen_res,
smart_id,
tcp_os,
true_ip,
user_agent,
accept,
accept_encoding,
accept_language,
fp_version,
language_res,
time_zone,
screen,
web_debugger_status,
enabled_js,
app_os,
error
from t2
where rank = 1;

-- 7.fraud_td_op_address  已经第一次改  已经第二次改
drop view if exists drip_loan_ods.fraud_td_op_address;
create view if not exists drip_loan_ods.fraud_td_op_address as
-- create view view_fraud_td_op_address as
with t1 as(
select
fraud_td_op_address.appl_no,
fraud_td_op_address.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
fraud_td_op_address.phone,
fraud_td_op_address.id,
'' as cust_id,
aps_application.channel as channel,
fraud_td_op_address.swift_no,
fraud_td_op_address.req_id,
cast(fraud_td_op_address.report_time as timestamp) as report_time,
fraud_td_op_address.inquire_module,
fraud_td_op_address.id_card_address,
fraud_td_op_address.true_ip_address,
fraud_td_op_address.wifi_address,
fraud_td_op_address.cell_address,
fraud_td_op_address.bank_card_address,
fraud_td_op_address.mobile_address,
fraud_td_op_address.id_card_province,
fraud_td_op_address.id_card_city,
fraud_td_op_address.mobile_address_province,
fraud_td_op_address.mobile_address_city
from drip_loan_ods.iboxchain_db_fraud_td_op_address fraud_td_op_address
left join drip_loan_ods.app_application_aps_application aps_application
on fraud_td_op_address.appl_no = aps_application.apply_id),

t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
id_card_address,
true_ip_address,
wifi_address,
cell_address,
bank_card_address,
mobile_address,
id_card_province,
id_card_city,
mobile_address_province,
mobile_address_city,
row_number() over(partition by appl_no,req_id order by report_time desc) as rank
from t1)

select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
id_card_address,
true_ip_address,
wifi_address,
cell_address,
bank_card_address,
mobile_address,
id_card_province,
id_card_city,
mobile_address_province,
mobile_address_city
from t2
where rank = 1;

-- 8.fraud_td_op_riskitems  已经第二次改
drop view if exists drip_loan_ods.fraud_td_op_riskitems;
create view if not exists drip_loan_ods.fraud_td_op_riskitems as
with t1 as(
select
fraud_td_op_riskitems.appl_no,
fraud_td_op_riskitems.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
fraud_td_op_riskitems.phone,
fraud_td_op_riskitems.id,
'' as cust_id,
aps_application.channel,
fraud_td_op_riskitems.swift_no,
fraud_td_op_riskitems.req_id,
cast(fraud_td_op_riskitems.report_time as timestamp) as report_time,
fraud_td_op_riskitems.inquire_module,
fraud_td_op_riskitems.rule_id,
cast(fraud_td_op_riskitems.score as bigint) as score,
fraud_td_op_riskitems.decision,
fraud_td_op_riskitems.risk_name,
fraud_td_op_riskitems.policy_decision,
fraud_td_op_riskitems.policy_mode,
cast(fraud_td_op_riskitems.policy_score as bigint) as policy_score,
fraud_td_op_riskitems.policy_name,
fraud_td_op_riskitems.risk_detail
from drip_loan_ods.iboxchain_db_fraud_td_op_riskitems fraud_td_op_riskitems
left join drip_loan_ods.app_application_aps_application aps_application
on fraud_td_op_riskitems.appl_no = aps_application.apply_id),

t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
rule_id,
score,
decision,
risk_name,
policy_decision,
policy_mode,
policy_score,
policy_name,
risk_detail,
row_number() over(partition by appl_no,req_id,rule_id order by report_time desc) as rank
from t1)

select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
rule_id,
score,
decision,
risk_name,
policy_decision,
policy_mode,
policy_score,
policy_name,
risk_detail
from t2
where rank = 1;


-- 9.fraud_td_op  已经第一次改  已经第二次改
drop view if exists drip_loan_ods.fraud_td_op;
create view if not exists drip_loan_ods.fraud_td_op as
with t1 as(
select
fraud_td_op.appl_no,
fraud_td_op.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
fraud_td_op.phone,
fraud_td_op.id,
'' as cust_id,
aps_application.channel as channel,
fraud_td_op.swift_no,
fraud_td_op.req_id,
cast(fraud_td_op.report_time as timestamp) as report_time,
fraud_td_op.inquire_module,
cast(fraud_td_op.final_score as bigint) as final_score,
fraud_td_op.final_decision,
cast(fraud_td_op.credit_score as bigint) as credit_score,
fraud_td_op.success,
fraud_td_op.reason_code,
fraud_td_op.reason_desc
from drip_loan_ods.iboxchain_db_fraud_td_op fraud_td_op
left join drip_loan_ods.app_application_aps_application aps_application
on fraud_td_op.appl_no =  aps_application.apply_id),

t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
final_score,
final_decision,
credit_score,
success,
reason_code,
reason_desc,
row_number() over(partition by appl_no,req_id order by report_time desc) as rank
from t1)

select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
final_score,
final_decision,
credit_score,
success,
reason_code,
reason_desc
from t2
where rank = 1;

-- 10.fraud_td_input---这个视图刚放进test库  第一次改
drop view if exists drip_loan_ods.fraud_td_input;
create view if not exists drip_loan_ods.fraud_td_input as
with t1 as(
select
fraud_td_input.appl_no,
fraud_td_input.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
fraud_td_input.phone,
fraud_td_input.id,
'' as cust_id,
aps_application.channel,
fraud_td_input.swift_no,
fraud_td_input.req_id,
cast(fraud_td_input.report_time as timestamp) as report_time,
fraud_td_input.inquire_module,
fraud_td_input.id_number,
fraud_td_input.account_mobile,
fraud_td_input.account_name,
fraud_td_input.registered_address,
fraud_td_input.apply_channel,
fraud_td_input.apply_province,
fraud_td_input.apply_city,
fraud_td_input.contact1_name,
fraud_td_input.contact1_mobile,
fraud_td_input.contact1_relation,
fraud_td_input.contact2_name,
fraud_td_input.contact2_mobile,
fraud_td_input.contact2_relation,
fraud_td_input.contact3_name,
fraud_td_input.contact3_mobile,
fraud_td_input.contact3_relation,
fraud_td_input.home_address,
fraud_td_input.marriage,
fraud_td_input.organization_address,
fraud_td_input.card_number,
fraud_td_input.organization,
fraud_td_input.account_phone,
fraud_td_input.black_box,
fraud_td_input.token_id
from drip_loan_ods.iboxchain_db_fraud_td_input fraud_td_input
left join drip_loan_ods.app_application_aps_application aps_application
on fraud_td_input.appl_no = aps_application.apply_id),

t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
id_number,
account_mobile,
account_name,
registered_address,
apply_channel,
apply_province,
apply_city,
contact1_name,
contact1_mobile,
contact1_relation,
contact2_name,
contact2_mobile,
contact2_relation,
contact3_name,
contact3_mobile,
contact3_relation,
home_address,
marriage,
organization_address,
card_number,
organization,
account_phone,
black_box,
token_id,
row_number() over(partition by appl_no,req_id order by report_time desc) as rank
from t1)

select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
id_number,
account_mobile,
account_name,
registered_address,
apply_channel,
apply_province,
apply_city,
contact1_name,
contact1_mobile,
contact1_relation,
contact2_name,
contact2_mobile,
contact2_relation,
contact3_name,
contact3_mobile,
contact3_relation,
home_address,
marriage,
organization_address,
card_number,
organization,
account_phone,
black_box,
token_id
from t2
where rank = 1;

-- ===============================

-- 11.fraud_ins_input_device  已经第一次改  已经第二次改
drop view if exists drip_loan_ods.fraud_ins_input_device;
create view if not exists drip_loan_ods.fraud_ins_input_device as
with t1 as(
select
fraud_ins_input_device.appl_no,
fraud_ins_input_device.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
fraud_ins_input_device.phone,
fraud_ins_input_device.id,
'' as cust_id,
aps_application.channel as channel,
fraud_ins_input_device.batch_id,
cast(fraud_ins_input_device.create_time as timestamp) as create_time,
fraud_ins_input_device.inquire_scene,
fraud_ins_input_device.node,
cast(fraud_ins_input_device.node_time  as timestamp) as node_time,
cast(fraud_ins_input_device.longtitude as decimal(15,6)) as longtitude,
cast(fraud_ins_input_device.latitude as decimal(15,6)) as latitude,
fraud_ins_input_device.gps_prov_city,
fraud_ins_input_device.phone_system,
fraud_ins_input_device.phone_ip,
fraud_ins_input_device.phone_type,
fraud_ins_input_device.phone_imei
from drip_loan_ods.iboxchain_db_fraud_ins_input_device fraud_ins_input_device
left join drip_loan_ods.app_application_aps_application aps_application
on fraud_ins_input_device.appl_no = aps_application.apply_id),

t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
batch_id,
create_time,
inquire_scene,
node,
node_time,
longtitude,
latitude,
gps_prov_city,
phone_system,
phone_ip,
phone_type,
phone_imei,
row_number() over(partition by appl_no,batch_id,node order by create_time desc) as rank
from t1)

select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
batch_id,
create_time,
inquire_scene,
node,
node_time,
longtitude,
latitude,
gps_prov_city,
phone_system,
phone_ip,
phone_type,
phone_imei
from t2
where rank = 1;


-- 12.fraud_ins_input_linkman  已经第一次改  已经第二次改

drop view if exists drip_loan_ods.fraud_ins_input_linkman;
create view if not exists drip_loan_ods.fraud_ins_input_linkman as
with t1 as(
select
fraud_ins_input_linkman.appl_no,
fraud_ins_input_linkman.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
fraud_ins_input_linkman.phone,
fraud_ins_input_linkman.id,
'' as cust_id,
aps_application.channel as channel,
fraud_ins_input_linkman.batch_id,
cast(fraud_ins_input_linkman.create_time as timestamp) as create_time,
fraud_ins_input_linkman.inquire_scene,
fraud_ins_input_linkman.linkman_name,
fraud_ins_input_linkman.linkman_relation,
fraud_ins_input_linkman.linkman_phone,
fraud_ins_input_linkman.linkman_prov_city,
cast(fraud_ins_input_linkman.linkman_no as bigint) as linkman_no,
cast(fraud_ins_input_linkman.duration_sum_90d as decimal(15,3)) as duration_sum_90d,
cast(fraud_ins_input_linkman.cnt_90d as bigint) as cnt_90d,
cast(fraud_ins_input_linkman.duration_avg_90d as decimal(15,3)) as duration_avg_90d,
cast(concat_ws('-',split(fraud_ins_input_linkman.lst_calltime_90d,'/')[2],split(fraud_ins_input_linkman.lst_calltime_90d,'/')[1],split(fraud_ins_input_linkman.lst_calltime_90d,'/')[0]) as timestamp) as lst_calltime_90d,
cast(fraud_ins_input_linkman.duration_ratio_90d as decimal(15,3)) as duration_ratio_90d,
cast(fraud_ins_input_linkman.cnt_ratio_90d as decimal(15,3)) as cnt_ratio_90d,
fraud_ins_input_linkman.is_consist
from drip_loan_ods.iboxchain_db_fraud_ins_input_linkman fraud_ins_input_linkman
left join drip_loan_ods.app_application_aps_application aps_application
on fraud_ins_input_linkman.appl_no = aps_application.apply_id),

t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
batch_id,
create_time,
inquire_scene,
linkman_name,
linkman_relation,
linkman_phone,
linkman_prov_city,
linkman_no,
duration_sum_90d,
cnt_90d,
duration_avg_90d,
lst_calltime_90d,
duration_ratio_90d,
cnt_ratio_90d,
is_consist,
row_number() over(partition by appl_no,batch_id,linkman_phone order by create_time desc) as rank
from t1)

select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
batch_id,
create_time,
inquire_scene,
linkman_name,
linkman_relation,
linkman_phone,
linkman_prov_city,
linkman_no,
duration_sum_90d,
cnt_90d,
duration_avg_90d,
lst_calltime_90d,
duration_ratio_90d,
cnt_ratio_90d,
is_consist
from t2
where rank = 1;



-- 13.fraud_ins_input_top20  已经第一次改  已经第二次改
drop view if exists drip_loan_ods.fraud_ins_input_top20;
create view if not exists drip_loan_ods.fraud_ins_input_top20 as
with t1 as(
select
fraud_ins_input_top20.appl_no,
fraud_ins_input_top20.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
fraud_ins_input_top20.phone,
fraud_ins_input_top20.id,
'' as cust_id,
aps_application.channel as channel,
fraud_ins_input_top20.batch_id,
cast(fraud_ins_input_top20.create_time as timestamp) as create_time,
fraud_ins_input_top20.inquire_scene,
fraud_ins_input_top20.top20_name,
fraud_ins_input_top20.top20_phone,
fraud_ins_input_top20.top20_prov_city,
cast(fraud_ins_input_top20.top20_no as bigint) as top20_no,
cast(fraud_ins_input_top20.duration_avg_90d as decimal(15,3)) as duration_avg_90d,
cast(fraud_ins_input_top20.cnt_90d as bigint) as cnt_90d,
cast(fraud_ins_input_top20.duration_sum_90d as decimal(15,3)) as duration_sum_90d,
cast(concat_ws('-',split(fraud_ins_input_top20.lst_calltime_90d,'/')[2],split(fraud_ins_input_top20.lst_calltime_90d,'/')[1],split(fraud_ins_input_top20.lst_calltime_90d,'/')[0]) as timestamp) as lst_calltime_90d,
cast(fraud_ins_input_top20.duration_ratio_90d as decimal(15,3)) as duration_ratio_90d,
cast(fraud_ins_input_top20.cnt_ratio_90d as decimal(15,3)) as cnt_ratio_90d
from drip_loan_ods.iboxchain_db_fraud_ins_input_top20 fraud_ins_input_top20
left join drip_loan_ods.app_application_aps_application aps_application
on fraud_ins_input_top20.appl_no = aps_application.apply_id),

t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
batch_id,
create_time,
inquire_scene,
top20_name,
top20_phone,
top20_prov_city,
top20_no,
duration_avg_90d,
cnt_90d,
duration_sum_90d,
lst_calltime_90d,
duration_ratio_90d,
cnt_ratio_90d,
row_number() over(partition by appl_no,batch_id,top20_phone order by create_time desc) as rank
from t1)

select *
from t2
where rank = 1;


-- 14.fraud_td_op_geoip  已经第一次改  已经第二次改
drop view if exists drip_loan_ods.fraud_td_op_geoip;
create view if not exists drip_loan_ods.fraud_td_op_geoip as
with t1 as(
select
fraud_td_op_geoip.appl_no,
fraud_td_op_geoip.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
fraud_td_op_geoip.phone,
fraud_td_op_geoip.id,
'' as cust_id,
aps_application.channel as channel,
fraud_td_op_geoip.swift_no,
fraud_td_op_geoip.req_id,
cast(fraud_td_op_geoip.report_time as timestamp) as report_time,
fraud_td_op_geoip.inquire_module,
fraud_td_op_geoip.position,
fraud_td_op_geoip.isp,
cast(fraud_td_op_geoip.latitude as decimal(15,6)) as latitude,
cast(fraud_td_op_geoip.longitude as decimal(15,6)) as longitude,
fraud_td_op_geoip.port,
fraud_td_op_geoip.proxy_protocol,
fraud_td_op_geoip.proxy_type
from drip_loan_ods.iboxchain_db_fraud_td_op_geoip fraud_td_op_geoip
left join drip_loan_ods.app_application_aps_application aps_application
on fraud_td_op_geoip.appl_no = aps_application.apply_id),

t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
position,
isp,
latitude,
longitude,
port,
proxy_protocol,
proxy_type,
row_number() over(partition by appl_no,req_id order by appl_time desc) as rank
from t1)

select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
position,
isp,
latitude,
longitude,
port,
proxy_protocol,
proxy_type
from t2
where rank = 1;




-- 15.fraud_td_op_geotrueip  已经第一次改  已经第二次改
drop view if exists drip_loan_ods.fraud_td_op_geotrueip;
create view if not exists drip_loan_ods.fraud_td_op_geotrueip as
with t1 as(
select
fraud_td_op_geotrueip.appl_no,
fraud_td_op_geotrueip.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
fraud_td_op_geotrueip.phone,
fraud_td_op_geotrueip.id,
'' as cust_id,
aps_application.channel as channel,
fraud_td_op_geotrueip.swift_no,
fraud_td_op_geotrueip.req_id,
cast(fraud_td_op_geotrueip.report_time as timestamp) as report_time,
fraud_td_op_geotrueip.inquire_module,
fraud_td_op_geotrueip.position,
fraud_td_op_geotrueip.isp,
cast(fraud_td_op_geotrueip.latitude as decimal(15,6)) as latitude,
cast(fraud_td_op_geotrueip.longitude as decimal(15,6)) as longitude
from drip_loan_ods.iboxchain_db_fraud_td_op_geotrueip fraud_td_op_geotrueip
left join drip_loan_ods.app_application_aps_application aps_application
on fraud_td_op_geotrueip.appl_no = aps_application.apply_id),

t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
position,
isp,
latitude,
longitude,
row_number() over(partition by appl_no,req_id order by report_time desc) as rank
from t1)

select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
swift_no,
req_id,
report_time,
inquire_module,
position,
isp,
latitude,
longitude
from t2
where rank = 1;

-- 16.fraud_ins_input   这个视图刚刚上去

drop view if exists drip_loan_ods.fraud_ins_input;
create view if not exists drip_loan_ods.fraud_ins_input as
with temp_fraud_ins_input as(
select
fraud_ins_input.appl_no as appl_no,
fraud_ins_input.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
fraud_ins_input.phone as phone,
fraud_ins_input.id as id,
'' as cust_id,
fraud_ins_input.channel as channel,
fraud_ins_input.batch_id as batch_id,
fraud_ins_input.create_time as create_time,
fraud_ins_input.inquire_scene as inquire_scene,
fraud_ins_input.loan_date as loan_date,
fraud_ins_input.appl_date as appl_date,
fraud_ins_input.application_type as application_type,
fraud_ins_input.channel_ins as channel_ins,
fraud_ins_input.appl_result as appl_result,
fraud_ins_input.rj_code as rj_code,
fraud_ins_input.appl_result_date as appl_result_date,
fraud_ins_input.subchannel_ins as subchannel_ins,
fraud_ins_input.cs_id as cs_id,
fraud_ins_input.appl_status as appl_status,
fraud_ins_input.blackapp_cnt as blackapp_cnt,
fraud_ins_input.fico_score as fico_score,
fraud_ins_input.credit_score as credit_score
from drip_loan_ods.iboxchain_db_fraud_ins_input fraud_ins_input
left join drip_loan_ods.app_application_aps_application aps_application
on fraud_ins_input.appl_no = aps_application.apply_id),

t1 as(
select
temp_fraud_ins_input.appl_no,
temp_fraud_ins_input.product_code,
temp_fraud_ins_input.appl_time,
temp_fraud_ins_input.phone,
temp_fraud_ins_input.id,
temp_fraud_ins_input.cust_id,
temp_fraud_ins_input.channel,
temp_fraud_ins_input.batch_id,
cast(temp_fraud_ins_input.create_time as timestamp) as create_time,
temp_fraud_ins_input.inquire_scene,
cast(temp_fraud_ins_input.loan_date as timestamp) as loan_date,
cast(concat_ws('-',split(temp_fraud_ins_input.appl_date,'/')[2],split(temp_fraud_ins_input.appl_date,'/')[1],split(temp_fraud_ins_input.appl_date,'/')[0]) as timestamp) as appl_date,
temp_fraud_ins_input.application_type,
temp_fraud_ins_input.channel_ins,
temp_fraud_ins_input.appl_result,
temp_fraud_ins_input.rj_code,
cast(temp_fraud_ins_input.appl_result_date as timestamp) as appl_result_date,
temp_fraud_ins_input.subchannel_ins,
temp_fraud_ins_input.cs_id,
temp_fraud_ins_input.appl_status,
cast(temp_fraud_ins_input.blackapp_cnt as bigint) as blackapp_cnt,
cast(temp_fraud_ins_input.fico_score as bigint) as fico_score,
cast(temp_fraud_ins_input.credit_score as bigint) as credit_score,
fraud_ins_input_Applicant.id_ins,
fraud_ins_input_Applicant.cust_name_ins,
fraud_ins_input_Applicant.id_prov_city,
fraud_ins_input_Applicant.residence_address,
fraud_ins_input_Applicant.gender,
cast(concat_ws('-',split(fraud_ins_input_Applicant.birthday,'/')[2],split(fraud_ins_input_Applicant.birthday,'/')[1],split(fraud_ins_input_Applicant.birthday,'/')[0]) as timestamp) as birthday,
fraud_ins_input_Applicant.family_prov_city,
fraud_ins_input_Applicant.family_distr_addr,
fraud_ins_input_Applicant.family_phone,
fraud_ins_input_Applicant.phone_ins,
fraud_ins_input_Applicant.company_name,
fraud_ins_input_Applicant.company_prov_city,
fraud_ins_input_Applicant.company_distr_addr,
fraud_ins_input_Applicant.phone_prov_city,
fraud_ins_input_Applicant.company_phone,
fraud_ins_input_Applicant.marital_status,
fraud_ins_input_Applicant.is_SE,
cast(fraud_ins_input_Applicant.phone_used_term as bigint) as phone_used_term,
fraud_ins_input_Applicant.isp,
fraud_ins_input_Applicant.fr_fail_cnt,
cast(fraud_ins_input_User2.duration_sum_3d as decimal(15,2)) as duration_sum_3d,
cast(fraud_ins_input_User2.duration_sum_7d as decimal(15,2)) as duration_sum_7d,
cast(fraud_ins_input_User2.duration_sum_30d as decimal(15,2)) as duration_sum_30d,
cast(fraud_ins_input_User2.duration_sum_90d as decimal(15,2)) as duration_sum_90d,
cast(fraud_ins_input_User2.cnt_3d as bigint) as cnt_3d,
cast(fraud_ins_input_User2.cnt_7d as bigint) as cnt_7d,
cast(fraud_ins_input_User2.cnt_30d as bigint) as cnt_30d,
cast(fraud_ins_input_User2.cnt_90d as bigint) as cnt_90d,
cast(fraud_ins_input_User2.telcalls_cnt_3d as bigint) as telcalls_cnt_3d,
cast(fraud_ins_input_User2.telcalls_cnt_7d as bigint) as telcalls_cnt_7d,
cast(fraud_ins_input_User2.telcalls_cnt_30d as bigint) as telcalls_cnt_30d,
cast(fraud_ins_input_User2.telcalls_cnt_90d as bigint) as telcalls_cnt_90d,
cast(fraud_ins_input_User2.duration_avg_3d as decimal(15,2)) as duration_avg_3d,
cast(fraud_ins_input_User2.duration_avg_7d as decimal(15,2)) as duration_avg_7d,
cast(fraud_ins_input_User2.duration_avg_30d as decimal(15,2)) as duration_avg_30d,
cast(fraud_ins_input_User2.duration_avg_90d as decimal(15,2)) as duration_avg_90d,
cast(fraud_ins_input_User2.nightcall_ratio_30d as decimal(15,3)) as nightcall_ratio_30d,
cast(fraud_ins_input_User2.nightcall_ratio_60d as decimal(15,3)) as nightcall_ratio_60d,
cast(fraud_ins_input_User2.nightcall_ratio_90d as decimal(15,3)) as nightcall_ratio_90d,
cast(fraud_ins_input_User2.interval_fstcall as bigint) as interval_fstcall,
cast(fraud_ins_input_User2.interval_lstcall as bigint) as interval_lstcall,
cast(fraud_ins_input_User2.interval_fstcall_1M as bigint) as interval_fstcall_1M,
cast(fraud_ins_input_User2.interval_lstcall_1M as bigint) as interval_lstcall_1M,
cast(fraud_ins_input_User2.selfcall_cnt_3d as bigint) as selfcall_cnt_3d,
cast(fraud_ins_input_User2.selfcall_cnt_7d as bigint) as selfcall_cnt_7d,
cast(fraud_ins_input_User2.selfcall_cnt_30d as bigint) as selfcall_cnt_30d,
cast(fraud_ins_input_User2.selfcall_cnt_90d as bigint) as selfcall_cnt_90d,
cast(fraud_ins_input_User2.telbook_cnt as bigint) as telbook_cnt,
cast(fraud_ins_input_User2.telbook_cnt_loan as bigint) as telbook_cnt_loan,
cast(fraud_ins_input_User2.telbook_cnt_notnum as bigint) as telbook_cnt_notnum
from temp_fraud_ins_input
left join drip_loan_ods.iboxchain_db_fraud_ins_input_applicant fraud_ins_input_Applicant
on temp_fraud_ins_input.batch_id = fraud_ins_input_Applicant.batch_id
left join drip_loan_ods.iboxchain_db_fraud_ins_input_user2 fraud_ins_input_User2
on temp_fraud_ins_input.batch_id = fraud_ins_input_User2.batch_id),

t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
batch_id,
create_time,
inquire_scene,
loan_date,
appl_date,
application_type,
channel_ins,
appl_result,
rj_code,
appl_result_date,
subchannel_ins,
cs_id,
appl_status,
blackapp_cnt,
fico_score,
credit_score,
id_ins,
cust_name_ins,
id_prov_city,
residence_address,
gender,
birthday,
family_prov_city,
family_distr_addr,
family_phone,
phone_ins,
company_name,
company_prov_city,
company_distr_addr,
phone_prov_city,
company_phone,
marital_status,
is_SE,
phone_used_term,
isp,
fr_fail_cnt,
duration_sum_3d,
duration_sum_7d,
duration_sum_30d,
duration_sum_90d,
cnt_3d,
cnt_7d,
cnt_30d,
cnt_90d,
telcalls_cnt_3d,
telcalls_cnt_7d,
telcalls_cnt_30d,
telcalls_cnt_90d,
duration_avg_3d,
duration_avg_7d,
duration_avg_30d,
duration_avg_90d,
nightcall_ratio_30d,
nightcall_ratio_60d,
nightcall_ratio_90d,
interval_fstcall,
interval_lstcall,
interval_fstcall_1M,
interval_lstcall_1M,
selfcall_cnt_3d,
selfcall_cnt_7d,
selfcall_cnt_30d,
selfcall_cnt_90d,
telbook_cnt,
telbook_cnt_loan,
telbook_cnt_notnum,
row_number() over(partition by appl_no,batch_id order by create_time desc) as rank
from t1)

select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
batch_id,
create_time,
inquire_scene,
loan_date,
appl_date,
application_type,
channel_ins,
appl_result,
rj_code,
appl_result_date,
subchannel_ins,
cs_id,
appl_status,
blackapp_cnt,
fico_score,
credit_score,
id_ins,
cust_name_ins,
id_prov_city,
residence_address,
gender,
birthday,
family_prov_city,
family_distr_addr,
family_phone,
phone_ins,
company_name,
company_prov_city,
company_distr_addr,
phone_prov_city,
company_phone,
marital_status,
is_SE,
phone_used_term,
isp,
cast(fr_fail_cnt as bigint) as fr_fail_cnt,
duration_sum_3d,
duration_sum_7d,
duration_sum_30d,
duration_sum_90d,
cnt_3d,
cnt_7d,
cnt_30d,
cnt_90d,
telcalls_cnt_3d,
telcalls_cnt_7d,
telcalls_cnt_30d,
telcalls_cnt_90d,
duration_avg_3d,
duration_avg_7d,
duration_avg_30d,
duration_avg_90d,
nightcall_ratio_30d,
nightcall_ratio_60d,
nightcall_ratio_90d,
interval_fstcall,
interval_lstcall,
interval_fstcall_1M,
interval_lstcall_1M,
selfcall_cnt_3d,
selfcall_cnt_7d,
selfcall_cnt_30d,
selfcall_cnt_90d,
telbook_cnt,
telbook_cnt_loan,
telbook_cnt_notnum
from t2
where rank = 1;

-- 17.fraud_ins_output  这个视图刚放上去
drop view if exists drip_loan_ods.fraud_ins_output;
create view if not exists drip_loan_ods.fraud_ins_output as
with t1 as(
select
ins_rsp.apply_no as appl_no,
ins_rsp.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
ins_rsp.mobile as phone,
ins_rsp.card_no as id,
'' as cust_id,
aps_application.channel,
ins_rsp.swift_no as batch_id,
cast(ins_rsp.create_time as timestamp) as create_time,
ins_rsp.inquire_scene,
ins_rsp.fraud_score,
ins_rsp.action_token as user_result,
ins_rsp.user_defined_alert as user_alert,
ins_rsp.rule_triggered1 as rule_code1,
ins_rsp.rule_triggered2 as rule_code2,
ins_rsp.rule_triggered3 as rule_code3,
ins_rsp.rule_triggered4 as rule_code4,
ins_rsp.rule_triggered5 as rule_code5,
ins_rsp.rule_triggered6 as rule_code6,
ins_rsp.rule_triggered7 as rule_code7,
ins_rsp.rule_triggered8 as rule_code8,
ins_rsp.rule_triggered9 as rule_code9,
ins_rsp.rule_triggered10 as rule_code10,
ins_rsp.rule_triggered11 as rule_code11,
ins_rsp.rule_triggered12 as rule_code12,
ins_rsp.rule_triggered13 as rule_code13,
ins_rsp.rule_triggered14 as rule_code14,
ins_rsp.rule_triggered15 as rule_code15,
ins_rsp.rule_triggered16 as rule_code16,
ins_rsp.rule_triggered17 as rule_code17,
ins_rsp.rule_triggered18 as rule_code18,
ins_rsp.rule_triggered19 as rule_code19,
ins_rsp.rule_triggered20 as rule_code20,
ins_rsp.description_rule_triggered_1 as rule_desc1,
ins_rsp.description_rule_triggered_2 as rule_desc2,
ins_rsp.description_rule_triggered_3 as rule_desc3,
ins_rsp.description_rule_triggered_4 as rule_desc4,
ins_rsp.description_rule_triggered_5 as rule_desc5,
ins_rsp.description_rule_triggered_6 as rule_desc6,
ins_rsp.description_rule_triggered_7 as rule_desc7,
ins_rsp.description_rule_triggered_8 as rule_desc8,
ins_rsp.description_rule_triggered_9 as rule_desc9,
ins_rsp.description_rule_triggered_10 as rule_desc10,
ins_rsp.description_rule_triggered_11 as rule_desc11,
ins_rsp.description_rule_triggered_12 as rule_desc12,
ins_rsp.description_rule_triggered_13 as rule_desc13,
ins_rsp.description_rule_triggered_14 as rule_desc14,
ins_rsp.description_rule_triggered_15 as rule_desc15,
ins_rsp.description_rule_triggered_16 as rule_desc16,
ins_rsp.description_rule_triggered_17 as rule_desc17,
ins_rsp.description_rule_triggered_18 as rule_desc18,
ins_rsp.description_rule_triggered_19 as rule_desc19,
ins_rsp.description_rule_triggered_20 as rule_desc20,
ins_rsp.decision_reason
from drip_loan_ods.iboxchain_db_ins_rsp ins_rsp
left join drip_loan_ods.app_application_aps_application aps_application
on ins_rsp.apply_no = aps_application.apply_id),

t2 as(
select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
batch_id,
create_time,
inquire_scene,
fraud_score,
user_result,
user_alert,
rule_code1,
rule_code2,
rule_code3,
rule_code4,
rule_code5,
rule_code6,
rule_code7,
rule_code8,
rule_code9,
rule_code10,
rule_code11,
rule_code12,
rule_code13,
rule_code14,
rule_code15,
rule_code16,
rule_code17,
rule_code18,
rule_code19,
rule_code20,
rule_desc1,
rule_desc2,
rule_desc3,
rule_desc4,
rule_desc5,
rule_desc6,
rule_desc7,
rule_desc8,
rule_desc9,
rule_desc10,
rule_desc11,
rule_desc12,
rule_desc13,
rule_desc14,
rule_desc15,
rule_desc16,
rule_desc17,
rule_desc18,
rule_desc19,
rule_desc20,
decision_reason,
row_number() over(partition by appl_no,batch_id,rule_code1,rule_code2,rule_code3,rule_code4,rule_code5,rule_code6,rule_code7,rule_code8,rule_code9,rule_code10,rule_code11,rule_code12,rule_code13,rule_code14,rule_code15,rule_code16,rule_code17,rule_code18,rule_code19,rule_code20 order by create_time desc) as rank
from t1)

select
appl_no,
product_code,
appl_time,
phone,
id,
cust_id,
channel,
batch_id,
create_time,
inquire_scene,
cast(fraud_score as bigint) as fraud_score,
user_result,
user_alert,
rule_code1,
rule_code2,
rule_code3,
rule_code4,
rule_code5,
rule_code6,
rule_code7,
rule_code8,
rule_code9,
rule_code10,
rule_code11,
rule_code12,
rule_code13,
rule_code14,
rule_code15,
rule_code16,
rule_code17,
rule_code18,
rule_code19,
rule_code20,
rule_desc1,
rule_desc2,
rule_desc3,
rule_desc4,
rule_desc5,
rule_desc6,
rule_desc7,
rule_desc8,
rule_desc9,
rule_desc10,
rule_desc11,
rule_desc12,
rule_desc13,
rule_desc14,
rule_desc15,
rule_desc16,
rule_desc17,
rule_desc18,
rule_desc19,
rule_desc20,
decision_reason
from t2
where rank = 1;



--------------------------------------------------------外部数据视图集合--------------------------------------------------------
-- 1.view_extra_hd_3match  第一次改
drop view if exists drip_loan_ods.extra_hd_3match;
create view if not exists drip_loan_ods.extra_hd_3match as
with t1 as(
select
db_hd_three_param_verify.apply_no as apply_no,
db_hd_three_param_verify.project_name as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
db_hd_three_param_verify.tel_no as mobile,
db_hd_three_param_verify.id_card_no as card_no,
'' as cust_id,
aps_application.channel,
db_hd_three_param_verify.riskengine_swift_no,
db_hd_three_param_verify.other_swift_no,
cast(db_hd_three_param_verify.create_time as timestamp) as get_data_time,
db_hd_three_param_verify.message as message,
db_hd_three_param_verify.isp,
db_hd_three_param_verify.option as match
from drip_loan_ods.iboxchain_db_db_hd_three_param_verify db_hd_three_param_verify
left join drip_loan_ods.app_application_aps_application aps_application
on db_hd_three_param_verify.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
message,
isp,
match,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
message,
isp,
match
from t2
where rank = 1;

-- 2.view_extra_hd_status  第一次改
drop view if exists drip_loan_ods.extra_hd_status;
create view if not exists drip_loan_ods.extra_hd_status as
with t1 as(
select
db_hd_phone_status.apply_no,
db_hd_phone_status.project_name as project_code ,
cast(aps_application.create_date_time as timestamp) as appl_time,
db_hd_phone_status.tel_no as mobile,
db_hd_phone_status.id_card_no as card_no,
'' as cust_id,
aps_application.channel as channel,
db_hd_phone_status.riskengine_swift_no as riskengine_swift_no,
db_hd_phone_status.other_swift_no as other_swift_no,
cast(db_hd_phone_status.create_time as timestamp) as get_data_time,
db_hd_phone_status.message as message,
db_hd_phone_status.isp,
db_hd_phone_status.option as status
from drip_loan_ods.iboxchain_db_db_hd_phone_status db_hd_phone_status
left join drip_loan_ods.app_application_aps_application aps_application
on db_hd_phone_status.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
project_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
message,
isp,
status,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)

select
apply_no,
project_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
message,
isp,
status
from t2
where rank = 1;

-- 3.view_extra_hd_online  第一次改
drop view if exists drip_loan_ods.extra_hd_online;
create view if not exists drip_loan_ods.extra_hd_online as
with t1 as(
select
db_hd_phone_duration.apply_no,
db_hd_phone_duration.project_name as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
db_hd_phone_duration.tel_no as mobile,
db_hd_phone_duration.id_card_no as card_no,
'' as cust_id,
aps_application.channel,
db_hd_phone_duration.riskengine_swift_no,
db_hd_phone_duration.other_swift_no,
cast(db_hd_phone_duration.create_time as timestamp) as get_data_time,
db_hd_phone_duration.message,
db_hd_phone_duration.isp,
db_hd_phone_duration.option as online
from drip_loan_ods.iboxchain_db_db_hd_phone_duration db_hd_phone_duration
left join drip_loan_ods.app_application_aps_application aps_application
on db_hd_phone_duration.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
message,
isp,
online,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
message,
isp,
online
from t2
where rank = 1;



-- 4.view_extra_py_grfxxx  第一次改
-- 去重逻辑修正
drop view if exists drip_loan_ods.extra_py_grfxxx;
create view if not exists drip_loan_ods.extra_py_grfxxx as
with t1 as(
select
py_indiv_risk_assess.apply_no,
py_indiv_risk_assess.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
py_indiv_risk_assess.mobile,
py_indiv_risk_assess.card_no,
'' as cust_id,
aps_application.channel,
py_indiv_risk_assess.swift_no as riskengine_swift_no,
py_indiv_risk_assess.other_swift_no as other_swift_no,
cast(py_indiv_risk_assess.create_time as timestamp) as get_data_time,
py_indiv_risk_assess.bat_no,
cast(concat_ws(' ',concat_ws('-',substring(split(py_indiv_risk_assess.receive_time,' ')[0],0,4),substring(split(py_indiv_risk_assess.receive_time,' ')[0],5,2),substring(split(py_indiv_risk_assess.receive_time,' ')[0],7,2)),split(py_indiv_risk_assess.receive_time,' ')[1]) as timestamp) as receive_time,
py_indiv_risk_assess.report_id,
cast(py_indiv_risk_assess.build_end_time as timestamp) as build_end_time,
py_indiv_risk_assess.has_system_error,
py_indiv_risk_assess.is_frozen,
py_indiv_risk_assess.treat_result,
py_indiv_risk_assess.treat_error_code,
py_indiv_risk_assess.error_message,
cast(py_indiv_risk_assess.al_count as bigint) as al_count,
cast(py_indiv_risk_assess.zx_count as bigint) as zx_count,
cast(py_indiv_risk_assess.sx_count as bigint) as sx_count,
cast(py_indiv_risk_assess.sw_count as bigint) as sw_count,
cast(py_indiv_risk_assess.cqgg_count as bigint) as cqgg_count,
cast(py_indiv_risk_assess.wdyq_count as bigint) as wdyq_count
from drip_loan_ods.iboxchain_db_py_indiv_risk_assess py_indiv_risk_assess
left join drip_loan_ods.app_application_aps_application aps_application
on py_indiv_risk_assess.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
bat_no,
receive_time,
report_id,
build_end_time,
has_system_error,
is_frozen,
treat_result,
treat_error_code,
error_message,
al_count,
zx_count,
sx_count,
sw_count,
cqgg_count,
wdyq_count,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
bat_no,
receive_time,
report_id,
build_end_time,
has_system_error,
is_frozen,
treat_result,
treat_error_code,
error_message,
al_count,
zx_count,
sx_count,
sw_count,
cqgg_count,
wdyq_count
from t2
where rank = 1;


-- 5.view_extra_py_grfxxx_al  第一次改

drop view if exists drip_loan_ods.extra_py_grfxxx_al;
create view if not exists drip_loan_ods.extra_py_grfxxx_al as
with t1 as(
select
extra_py_grfxxx_al.apply_no,
extra_py_grfxxx_al.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
extra_py_grfxxx_al.mobile,
extra_py_grfxxx_al.card_no,
'' as cust_id,
aps_application.channel,
extra_py_grfxxx_al.riskengine_swift_no,
extra_py_grfxxx_al.other_swift_no,
cast(extra_py_grfxxx_al.create_time as timestamp) as get_data_time,
extra_py_grfxxx_al.record_no,
extra_py_grfxxx_al.bt,
extra_py_grfxxx_al.ajlx,
extra_py_grfxxx_al.sjnf,
extra_py_grfxxx_al.dsrlx
from drip_loan_ods.iboxchain_db_extra_py_grfxxx_al extra_py_grfxxx_al
left join drip_loan_ods.app_application_aps_application aps_application
on extra_py_grfxxx_al.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
record_no,
bt,
ajlx,
sjnf,
dsrlx,
row_number() over(partition by apply_no,other_swift_no,record_no order by get_data_time) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
record_no,
bt,
ajlx,
sjnf,
dsrlx
from t2
where rank = 1;


-- 6.view_extra_py_grfxxx_zx  第一次改

drop view if exists drip_loan_ods.extra_py_grfxxx_zx;
create view if not exists drip_loan_ods.extra_py_grfxxx_zx as
--create view drip_loan_ods.extra_py_grfxxx_zx as
with t1 as(
select
extra_py_grfxxx_zx.apply_no,
extra_py_grfxxx_zx.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
extra_py_grfxxx_zx.mobile,
extra_py_grfxxx_zx.card_no,
'' as cust_id,
aps_application.channel,
extra_py_grfxxx_zx.riskengine_swift_no,
extra_py_grfxxx_zx.other_swift_no,
cast(extra_py_grfxxx_zx.create_time as timestamp) as get_data_time,
extra_py_grfxxx_zx.record_no,
extra_py_grfxxx_zx.bt,
extra_py_grfxxx_zx.zxbd,
cast(extra_py_grfxxx_zx.larq as timestamp) as larq
from drip_loan_ods.iboxchain_db_extra_py_grfxxx_zx extra_py_grfxxx_zx
left join drip_loan_ods.app_application_aps_application aps_application
on extra_py_grfxxx_zx.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
record_no,
bt,
zxbd,
larq,
row_number() over(partition by apply_no,other_swift_no,record_no order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
record_no,
bt,
zxbd,
larq
from t2
where rank = 1;


-- 7.view_extra_py_grfxxx_sx  第一次改
drop view if exists drip_loan_ods.extra_py_grfxxx_sx;
create view if not exists drip_loan_ods.extra_py_grfxxx_sx as
with t1 as(
select
extra_py_grfxxx_sx.apply_no,
extra_py_grfxxx_sx.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
extra_py_grfxxx_sx.mobile,
extra_py_grfxxx_sx.card_no,
'' as cust_id,
aps_application.channel,
extra_py_grfxxx_sx.riskengine_swift_no,
extra_py_grfxxx_sx.other_swift_no,
cast(extra_py_grfxxx_sx.create_time as timestamp) as get_data_time,
extra_py_grfxxx_sx.record_no,
extra_py_grfxxx_sx.bt,
cast(extra_py_grfxxx_sx.larq as timestamp) as larq,
cast(extra_py_grfxxx_sx.fbrq as timestamp) as fbrq
from drip_loan_ods.iboxchain_db_extra_py_grfxxx_sx extra_py_grfxxx_sx
left join drip_loan_ods.app_application_aps_application aps_application
on extra_py_grfxxx_sx.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
record_no,
bt,
larq,
fbrq,
row_number() over(partition by apply_no,other_swift_no,record_no order by get_data_time) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
record_no,
bt,
larq,
fbrq
from t2
where rank = 1;

-- 8.view_extra_py_grfxxx_sw  第一次改

drop view if exists drip_loan_ods.extra_py_grfxxx_sw;
create view if not exists drip_loan_ods.extra_py_grfxxx_sw as
with t1 as(
select
extra_py_grfxxx_sw.apply_no,
extra_py_grfxxx_sw.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
extra_py_grfxxx_sw.mobile,
extra_py_grfxxx_sw.card_no,
'' as cust_id,
aps_application.channel,
extra_py_grfxxx_sw.riskengine_swift_no,
extra_py_grfxxx_sw.other_swift_no,
cast(extra_py_grfxxx_sw.create_time as timestamp) as get_data_time,
extra_py_grfxxx_sw.record_no,
extra_py_grfxxx_sw.bt,
cast(extra_py_grfxxx_sw.ggrq as timestamp) as ggrq
from drip_loan_ods.iboxchain_db_extra_py_grfxxx_sw extra_py_grfxxx_sw
left join drip_loan_ods.app_application_aps_application aps_application
on extra_py_grfxxx_sw.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
record_no,
bt,
ggrq,
row_number() over(partition by apply_no,other_swift_no,record_no order by get_data_time) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
record_no,
bt,
ggrq
from t2
where rank = 1;


-- 9.view_extra_py_grfxxx_cqgg  第一次改  extra_py_grfxxx_cqgg.swift_no,  extra_py_grfxxx_cqgg.req_id,
drop view if exists drip_loan_ods.extra_py_grfxxx_cqgg;
create view if not exists drip_loan_ods.extra_py_grfxxx_cqgg as
with t1 as(
select
extra_py_grfxxx_cqgg.apply_no,
extra_py_grfxxx_cqgg.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
extra_py_grfxxx_cqgg.mobile,
extra_py_grfxxx_cqgg.card_no,
'' as cust_id,
aps_application.channel,
extra_py_grfxxx_cqgg.riskengine_swift_no,
extra_py_grfxxx_cqgg.other_swift_no,
cast(extra_py_grfxxx_cqgg.create_time as timestamp) as get_data_time,
extra_py_grfxxx_cqgg.record_no,
extra_py_grfxxx_cqgg.bt,
cast(extra_py_grfxxx_cqgg.fbrq as timestamp) as fbrq
from drip_loan_ods.iboxchain_db_extra_py_grfxxx_cqgg extra_py_grfxxx_cqgg
left join drip_loan_ods.app_application_aps_application aps_application
on extra_py_grfxxx_cqgg.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
record_no,
bt,
fbrq,
row_number() over(partition by apply_no,other_swift_no,record_no order by get_data_time) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
record_no,
bt,
fbrq
from t2
where rank = 1;




-- 10.view_extra_py_grfxxx_wdyq
drop view if exists drip_loan_ods.extra_py_grfxxx_wdyq;
create view if not exists drip_loan_ods.extra_py_grfxxx_wdyq as
with t1 as(
select
extra_py_grfxxx_wdyq.apply_no,
extra_py_grfxxx_wdyq.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
extra_py_grfxxx_wdyq.mobile,
extra_py_grfxxx_wdyq.card_no,
'' as cust_id,
aps_application.channel,
extra_py_grfxxx_wdyq.riskengine_swift_no,
extra_py_grfxxx_wdyq.other_swift_no,
cast(extra_py_grfxxx_wdyq.create_time as timestamp) as get_data_time,
extra_py_grfxxx_wdyq.record_no,
extra_py_grfxxx_wdyq.bt,
cast(extra_py_grfxxx_wdyq.fbrq as timestamp) as fbrq
from drip_loan_ods.iboxchain_db_extra_py_grfxxx_wdyq extra_py_grfxxx_wdyq
left join drip_loan_ods.app_application_aps_application aps_application
on extra_py_grfxxx_wdyq.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
record_no,
bt,
fbrq,
row_number() over(partition by apply_no,other_swift_no,record_no order by get_data_time) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
record_no,
bt,
fbrq
from t2
where rank = 1;


-- 11.view_extra_dw_person_score  第一次改（但是发现了后面的几个字段有问题所以没放上去）
drop view if exists drip_loan_ods.extra_dw_person_score;
create view if not exists drip_loan_ods.extra_dw_person_score as
with t1 as(
select
db_dw_personnel_score_rsp.apply_no,
db_dw_personnel_score_rsp.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
db_dw_personnel_score_rsp.mobile,
db_dw_personnel_score_rsp.card_no,
'' as cust_id,
aps_application.channel,
db_dw_personnel_score_rsp.riskengine_swift_no,
db_dw_personnel_score_rsp.other_swift_no,
cast(db_dw_personnel_score_rsp.get_data_time as timestamp) as get_data_time,
call_inter_status_code as code,
call_inter_status_code_des as msg,
db_dw_personnel_score_rsp.detail
from drip_loan_ods.iboxchain_db_db_dw_personnel_score_rsp db_dw_personnel_score_rsp
left join drip_loan_ods.app_application_aps_application aps_application
on db_dw_personnel_score_rsp.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
code,
msg,
detail,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
code,
msg,
detail
from t2
where rank = 1;




-- 12.view_extra_jg_48factors  第一次改
drop view if exists drip_loan_ods.extra_jg_48factors;
create view if not exists drip_loan_ods.extra_jg_48factors as
with t1 as(
select
jg_48factors.apply_no,
jg_48factors.project_name as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
aps_application.phone_num,
aps_application.id_num,
'' as customer_id,
aps_application.channel,
jg_48factors.mobile,
jg_48factors.card_no,
jg_48factors.name,
jg_48factors.riskengine_swift_no,
jg_48factors.other_swift_no,
cast(jg_48factors.get_data_time as timestamp) as get_data_time,
jg_48factors.code,
jg_48factors.message,
cast(jg_48factors.fin_loan_plantform_uninstall_0m_1m as bigint) as fin_loan_plantform_uninstall_0m_1m,
cast(jg_48factors.fin_loan_student_all_0m_1m as bigint) as fin_loan_student_all_0m_1m,
cast(jg_48factors.fin_loan_student_uninstall_0m_1m as bigint) as fin_loan_student_uninstall_0m_1m,
cast(jg_48factors.muti_have_overdue_avg_day_0m_1m as bigint) as muti_have_overdue_avg_day_0m_1m,
cast(jg_48factors.register_earliest_loan_app_days as bigint) as register_earliest_loan_app_days,
cast(jg_48factors.register_loan_app_cnt as bigint) as register_loan_app_cnt,
cast(jg_48factors.register_loan_app_cnt_6m as bigint) as register_loan_app_cnt_6m,
cast(jg_48factors.register_finance_app_cnt_1m as bigint) as register_finance_app_cnt_1m,
cast(jg_48factors.msg_cnt as bigint) as msg_cnt,
cast(jg_48factors.msg_loan_cnt as bigint) as msg_loan_cnt,
cast(jg_48factors.stall_loan_install_app_cnt_1m as bigint) as stall_loan_install_app_cnt_1m,
cast(jg_48factors.stall_loan_uninstall_app_cnt_3m as bigint) as stall_loan_uninstall_app_cnt_3m,
cast(jg_48factors.stall_loan_uninstall_app_cnt_9m as bigint) as stall_loan_uninstall_app_cnt_9m,
cast(jg_48factors.lbs_day_cnt as bigint) as lbs_day_cnt,
cast(jg_48factors.stall_loan_install_app_cnt_6m as bigint) as stall_loan_install_app_cnt_6m,
cast(jg_48factors.msg_cnt_perapp_3m as bigint) as msg_cnt_perapp_3m,
cast(jg_48factors.stall_health_install_app_cnt_9m as bigint) as stall_health_install_app_cnt_9m,
cast(jg_48factors.unknown_0_holiday_gen_pct as bigint) as unknown_0_holiday_gen_pct,
cast(jg_48factors.workdayhour_town_count_pct as bigint) as workdayhour_town_count_pct,
cast(jg_48factors.wifi_5_m_gen_pct as bigint) as wifi_5_m_gen_pct,
cast(jg_48factors.app_shopping_offer_pct_c1_all_60 as bigint) as app_shopping_offer_pct_c1_all_60,
cast(jg_48factors.c1_8_usecount_day_std_180d as bigint) as 180d_c1_8_usecount_day_std,
cast(jg_48factors.app_shopping_offer_pct_c1_all_120 as bigint) as app_shoopping_offer_pct_c1_all_120,
cast(jg_48factors.classfin_history_app_num_180d as bigint) as 180d_classfin_history_app_num,
cast(jg_48factors.micro_loan_c3_frist_install_60 as bigint) as micro_loan_c3_frist_install_60,
cast(jg_48factors.c2_5_usecount_day_std_60d as bigint) as 60d_c2_5_usecount_day_std,
cast(jg_48factors.classfin_install_app_num_60d as bigint) as 60d_classfin_install_app_num,
cast(jg_48factors.classfin_history_app_num_60d as bigint) as 60d_classfin_history_app_num,
cast(jg_48factors.class1_1_uninstall_app_num_90d as bigint) as 90d_class1_1_uninstall_app_num,
cast(jg_48factors.class1_1_history_app_num_90d as bigint) as 90d_class1_1_history_app_num,
cast(jg_48factors.micro_loan_c3_num_uninstall_total_30 as bigint) as micro_loan_c3_num_uninstall_total_30,
cast(jg_48factors.c1_8_usecount_day_mean_30d as bigint) as 30d_c1_8_usecount_day_mean,
cast(jg_48factors.app_shopping_offer_c1_num_install_max_90 as bigint) as app_shopping_offer_C1_num_install_max_90,
cast(jg_48factors.class1_1_install_app_times_60d as bigint) as 60d_class1_1_install_app_times,
cast(jg_48factors.class1_1_uninstall_app_times_15d as bigint) as 15d_class1_1_uninstall_app_times,
cast(jg_48factors.c2_5_usetime_day_std_180d as bigint) as 180d_c2_5_usetime_day_std,
cast(jg_48factors.c2_5_usecount_sum_90d as bigint) as 90d_c2_5_usecount_sum,
cast(jg_48factors.credit_fail_times_30d as bigint) as credit_fail_times_30d,
cast(jg_48factors.app_shopping_offer_pct_c1_installing_30 as bigint) as app_shopping_offer_c1_installing_30,
cast(jg_48factors.loan_succ_times_30d as bigint) as loan_succ_times_30d,
cast(jg_48factors.credit_fail_times_120d as bigint) as credit_fail_times_120d,
cast(jg_48factors.all_usecount_day_std_180d as bigint) as 180d_all_usecount_day_std,
cast(jg_48factors.cfin_usecount_sum_90d as bigint) as 90d_cfin_usecount_sum,
cast(jg_48factors.overdue_times_60d as bigint) as overdue_times_60d,
cast(jg_48factors.app_travel_pct_c1_all_120 as bigint) as app_travel_pct_c1_all_120,
cast(jg_48factors.multi_loan_30d as bigint) as multi_loan_30d,
cast(jg_48factors.overdue_times_120d as bigint) as overdue_times_120d,
cast(jg_48factors.micro_loan_pct_c3_installing_120 as bigint) as micro_loan_pct_c3_installing_120,
jg_48factors.level_stay_days,
jg_48factors.device_model,
cast(jg_48factors.score1 as decimal(38,9)) as score1,
cast(jg_48factors.score2 as decimal(38,9)) as score2
from drip_loan_ods.iboxchain_db_jg_48factors jg_48factors
left join drip_loan_ods.app_application_aps_application aps_application
on jg_48factors.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
phone_num,
id_num,
customer_id,
channel,
mobile,
card_no,
name,
riskengine_swift_no,
other_swift_no,
get_data_time,
code,
message,
fin_loan_plantform_uninstall_0m_1m,
fin_loan_student_all_0m_1m,
fin_loan_student_uninstall_0m_1m,
muti_have_overdue_avg_day_0m_1m,
register_earliest_loan_app_days,
register_loan_app_cnt,
register_loan_app_cnt_6m,
register_finance_app_cnt_1m,
msg_cnt,
msg_loan_cnt,
stall_loan_install_app_cnt_1m,
stall_loan_uninstall_app_cnt_3m,
stall_loan_uninstall_app_cnt_9m,
lbs_day_cnt,
stall_loan_install_app_cnt_6m,
msg_cnt_perapp_3m,
stall_health_install_app_cnt_9m,
unknown_0_holiday_gen_pct,
workdayhour_town_count_pct,
wifi_5_m_gen_pct,
app_shopping_offer_pct_c1_all_60,
180d_c1_8_usecount_day_std,
app_shoopping_offer_pct_c1_all_120,
180d_classfin_history_app_num,
micro_loan_c3_frist_install_60,
60d_c2_5_usecount_day_std,
60d_classfin_install_app_num,
60d_classfin_history_app_num,
90d_class1_1_uninstall_app_num,
90d_class1_1_history_app_num,
micro_loan_c3_num_uninstall_total_30,
30d_c1_8_usecount_day_mean,
app_shopping_offer_C1_num_install_max_90,
60d_class1_1_install_app_times,
15d_class1_1_uninstall_app_times,
180d_c2_5_usetime_day_std,
90d_c2_5_usecount_sum,
credit_fail_times_30d,
app_shopping_offer_c1_installing_30,
loan_succ_times_30d,
credit_fail_times_120d,
180d_all_usecount_day_std,
90d_cfin_usecount_sum,
overdue_times_60d,
app_travel_pct_c1_all_120,
multi_loan_30d,
overdue_times_120d,
micro_loan_pct_c3_installing_120,
level_stay_days,
device_model,
score1,
score2,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
phone_num,
id_num,
customer_id,
channel,
mobile,
card_no,
name,
riskengine_swift_no,
other_swift_no,
get_data_time,
code,
message,
fin_loan_plantform_uninstall_0m_1m,
fin_loan_student_all_0m_1m,
fin_loan_student_uninstall_0m_1m,
muti_have_overdue_avg_day_0m_1m,
register_earliest_loan_app_days,
register_loan_app_cnt,
register_loan_app_cnt_6m,
register_finance_app_cnt_1m,
msg_cnt,
msg_loan_cnt,
stall_loan_install_app_cnt_1m,
stall_loan_uninstall_app_cnt_3m,
stall_loan_uninstall_app_cnt_9m,
lbs_day_cnt,
stall_loan_install_app_cnt_6m,
msg_cnt_perapp_3m,
stall_health_install_app_cnt_9m,
unknown_0_holiday_gen_pct,
workdayhour_town_count_pct,
wifi_5_m_gen_pct,
app_shopping_offer_pct_c1_all_60,
180d_c1_8_usecount_day_std,
app_shoopping_offer_pct_c1_all_120,
180d_classfin_history_app_num,
micro_loan_c3_frist_install_60,
60d_c2_5_usecount_day_std,
60d_classfin_install_app_num,
60d_classfin_history_app_num,
90d_class1_1_uninstall_app_num,
90d_class1_1_history_app_num,
micro_loan_c3_num_uninstall_total_30,
30d_c1_8_usecount_day_mean,
app_shopping_offer_C1_num_install_max_90,
60d_class1_1_install_app_times,
15d_class1_1_uninstall_app_times,
180d_c2_5_usetime_day_std,
90d_c2_5_usecount_sum,
credit_fail_times_30d,
app_shopping_offer_c1_installing_30,
loan_succ_times_30d,
credit_fail_times_120d,
180d_all_usecount_day_std,
90d_cfin_usecount_sum,
overdue_times_60d,
app_travel_pct_c1_all_120,
multi_loan_30d,
overdue_times_120d,
micro_loan_pct_c3_installing_120,
level_stay_days,
device_model,
score1,
score2
from t2
where rank = 1;


-- 13.view_extra_ty_fqz  第一次改
drop view if exists drip_loan_ods.extra_ty_fqz;
create view if not exists drip_loan_ods.extra_ty_fqz as
with t1 as(
select
csig_anti_fraud.apply_no,
csig_anti_fraud.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
csig_anti_fraud.mobile,
csig_anti_fraud.card_no,
'' as cust_id,
aps_application.channel,
csig_anti_fraud.swift_no as riskengine_swift_no,
csig_anti_fraud.other_swift_no,
cast(csig_anti_fraud.create_time as timestamp) as get_data_time,
cast(csig_anti_fraud.risk_score as bigint) as risk_score,
csig_anti_fraud.code,
csig_anti_fraud.code_desc,
csig_anti_fraud.message,
csig_anti_fraud.found,
csig_anti_fraud.id_found
from drip_loan_ods.iboxchain_db_csig_anti_fraud csig_anti_fraud
left join drip_loan_ods.app_application_aps_application aps_application
on csig_anti_fraud.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
risk_score,
code,
code_desc,
message,
found,
id_found,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
risk_score,
code,
code_desc,
message,
found,
id_found
from t2
where rank = 1;


-- 14.view_extra_ty_fqz_detail  第一次改
-- 去重逻辑修复
drop view if exists drip_loan_ods.extra_ty_fqz_detail;
create view if not exists drip_loan_ods.extra_ty_fqz_detail as
with t1 as(
select
csig_anti_fraud_detail.id,
csig_anti_fraud_detail.apply_no,
csig_anti_fraud_detail.project_name as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
csig_anti_fraud_detail.mobile,
csig_anti_fraud_detail.card_no,
'' as cust_id,
aps_application.channel,
csig_anti_fraud_detail.swift_no as riskengine_swift_no,
csig_anti_fraud_detail.anti_fraud_id as other_swift_no,
'' as req_id_detail,
cast(csig_anti_fraud_detail.get_data_time as timestamp) as get_data_time,
csig_anti_fraud_detail.risk_code
from drip_loan_ods.iboxchain_db_csig_anti_fraud_detail csig_anti_fraud_detail
left join drip_loan_ods.app_application_aps_application aps_application
on csig_anti_fraud_detail.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
req_id_detail,
get_data_time,
risk_code,
row_number() over(partition by apply_no,other_swift_no,risk_code order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
req_id_detail,
get_data_time,
risk_code
from t2
where rank = 1;


-- 15.view_extra_xy_qjld  第一次改
drop view if exists drip_loan_ods.extra_xy_qjld;
create view if not exists drip_loan_ods.extra_xy_qjld as
with t1 as(
select
xinyan_behavior_report.apply_no,
xinyan_behavior_report.project_name as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
xinyan_behavior_report.mobile,
xinyan_behavior_report.card_no,
'' as cust_id,
aps_application.channel,
xinyan_behavior_report.swift_no as riskengine_swift_no,
xinyan_behavior_report.other_swift_no,
cast(xinyan_behavior_report.create_time as timestamp) as get_data_time,
cast(xinyan_behavior_report.apply_apply_score as int) as apply_score,
cast(xinyan_behavior_report.apply_apply_credibility as int) as apply_credibility,
cast(xinyan_behavior_report.apply_query_org_count as int) as query_org_count,
cast(xinyan_behavior_report.apply_query_finance_count as int) as query_finance_count,
cast(xinyan_behavior_report.apply_query_cash_count as int) as query_cash_count,
cast(xinyan_behavior_report.apply_query_sum_count as int) as query_sum_count,
cast(xinyan_behavior_report.apply_latest_query_time as timestamp) as latest_query_time,
cast(xinyan_behavior_report.apply_latest_one_month as int) as latest_one_month,
cast(xinyan_behavior_report.apply_latest_three_month as int) as latest_three_month,
cast(xinyan_behavior_report.apply_latest_six_month as int) as latest_six_month,
cast(xinyan_behavior_report.loans_score as int) as loans_score,
cast(xinyan_behavior_report.loans_credibility as int) as loans_credibility,
cast(xinyan_behavior_report.loans_count as int) as loans_count,
cast(xinyan_behavior_report.loans_settle_count as int) as loans_settle_count,
cast(xinyan_behavior_report.loans_overdue_count as int) as loans_overdue_count,
cast(xinyan_behavior_report.loans_org_count as int) as loans_org_count,
cast(xinyan_behavior_report.consfin_org_count as int) as consfin_org_count,
cast(xinyan_behavior_report.loans_cash_count as int) as loans_cash_count,
cast(xinyan_behavior_report.latest_one_month as int) as latest_one_month_used,
cast(xinyan_behavior_report.latest_three_month as int) as latest_three_month_used,
cast(xinyan_behavior_report.latest_six_month as int) as latest_six_month_used,
cast(xinyan_behavior_report.history_suc_fee as int) as history_suc_fee,
cast(xinyan_behavior_report.history_fail_fee as int) as history_fail_fee,
cast(xinyan_behavior_report.latest_one_month_suc as int) as latest_one_month_suc,
cast(xinyan_behavior_report.latest_one_month_fail as int) as latest_one_month_fail,
cast(xinyan_behavior_report.loans_long_time as int) as loans_long_time,
cast(xinyan_behavior_report.loans_latest_time as int) as loans_latest_time,
cast(xinyan_behavior_report.current_loans_credit_limit as int) as loans_credit_limit,
cast(xinyan_behavior_report.current_loans_credibility as int) as loans_credibility_used,
cast(xinyan_behavior_report.current_loans_org_count as int) as loans_org_count_used,
cast(xinyan_behavior_report.current_loans_product_count as int) as loans_product_count,
cast(xinyan_behavior_report.current_loans_max_limit as int) as loans_max_limit,
cast(xinyan_behavior_report.current_loans_avg_limit as int) as loans_avg_limit,
cast(xinyan_behavior_report.current_consfin_credit_limit as int) as consfin_credit_limit,
cast(xinyan_behavior_report.current_consfin_credibility as int) as consfin_credibility,
cast(xinyan_behavior_report.current_consfin_org_count as int) as consfin_org_count_used,
cast(xinyan_behavior_report.current_consfin_product_count as int) as consfin_product_count,
cast(xinyan_behavior_report.current_consfin_max_limit as int) as consfin_max_limit,
cast(xinyan_behavior_report.current_consfin_avg_limit as int) as consfin_avg_limit,
xinyan_behavior_report.success,
xinyan_behavior_report.error_code,
xinyan_behavior_report.error_msg,
xinyan_behavior_report.trans_id,
xinyan_behavior_report.trans_no,
xinyan_behavior_report.code,
xinyan_behavior_report.desc
from drip_loan_ods.iboxchain_db_xinyan_behavior_report xinyan_behavior_report
left join drip_loan_ods.app_application_aps_application aps_application
on xinyan_behavior_report.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
apply_score,
apply_credibility,
query_org_count,
query_finance_count,
query_cash_count,
query_sum_count,
latest_query_time,
latest_one_month,
latest_three_month,
latest_six_month,
loans_score,
loans_credibility,
loans_count,
loans_settle_count,
loans_overdue_count,
loans_org_count,
consfin_org_count,
loans_cash_count,
latest_one_month_used,
latest_three_month_used,
latest_six_month_used,
history_suc_fee,
history_fail_fee,
latest_one_month_suc,
latest_one_month_fail,
loans_long_time,
loans_latest_time,
loans_credit_limit,
loans_credibility_used,
loans_org_count_used,
loans_product_count,
loans_max_limit,
loans_avg_limit,
consfin_credit_limit,
consfin_credibility,
consfin_org_count_used,
consfin_product_count,
consfin_max_limit,
consfin_avg_limit,
success,
error_code,
error_msg,
trans_id,
trans_no,
code,
desc,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
apply_score,
apply_credibility,
query_org_count,
query_finance_count,
query_cash_count,
query_sum_count,
latest_query_time,
latest_one_month,
latest_three_month,
latest_six_month,
loans_score,
loans_credibility,
loans_count,
loans_settle_count,
loans_overdue_count,
loans_org_count,
consfin_org_count,
loans_cash_count,
latest_one_month_used,
latest_three_month_used,
latest_six_month_used,
history_suc_fee,
history_fail_fee,
latest_one_month_suc,
latest_one_month_fail,
loans_long_time,
loans_latest_time,
loans_credit_limit,
loans_credibility_used,
loans_org_count_used,
loans_product_count,
loans_max_limit,
loans_avg_limit,
consfin_credit_limit,
consfin_credibility,
consfin_org_count_used,
consfin_product_count,
consfin_max_limit,
consfin_avg_limit,
success,
error_code,
error_msg,
trans_id,
trans_no,
code,
desc
from t2
where rank = 1;



-- 17.view_extra_fico_dsjpf  第一次改
drop view if exists drip_loan_ods.extra_fico_dsjpf;
create view if not exists drip_loan_ods.extra_fico_dsjpf as
with t1 as(
select
fico_rsp.apply_no,
fico_rsp.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
fico_rsp.mobile,
fico_rsp.card_no,
'' as cust_id,
aps_application.channel,
fico_rsp.swift_no as riskengine_swift_no,
fico_rsp.other_swift_no,
cast(fico_rsp.create_time as timestamp) as get_data_time,
fico_rsp.score_id,
cast(fico_rsp.score as int) as score,
fico_rsp.reason,
fico_rsp.code,
fico_rsp.msg
from drip_loan_ods.iboxchain_db_fico_rsp fico_rsp
left join drip_loan_ods.app_application_aps_application aps_application
on fico_rsp.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
score_id,
score,
reason,
code,
msg,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
score_id,
score,
reason,
code,
msg
from t2
where rank = 1;



-- 18.view_extra_py_grcz  ========================从这里开始在测试环境上测试        1  自己改完   cast(py_indiv_debt_assess.receive_time as timestamp) as receive_time,

drop view if exists drip_loan_ods.extra_py_grcz;
create view if not exists drip_loan_ods.extra_py_grcz as
with t1 as(
select
py_indiv_debt_assess.apply_no,
py_indiv_debt_assess.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
py_indiv_debt_assess.mobile,
py_indiv_debt_assess.card_no,
'' as cust_id,
aps_application.channel,
py_indiv_debt_assess.riskengine_swift_no,
py_indiv_debt_assess.other_swift_no,
cast(py_indiv_debt_assess.create_time as timestamp) as get_data_time,
py_indiv_debt_assess.bat_no,
cast(concat_ws('-',substring(py_indiv_debt_assess.receive_time,0,4),substring(py_indiv_debt_assess.receive_time,5,2),substring(py_indiv_debt_assess.receive_time,7,11)) as timestamp) as receive_time,
py_indiv_debt_assess.report_id,
cast(py_indiv_debt_assess.build_end_time as timestamp) as build_end_time,
py_indiv_debt_assess.has_system_error,
py_indiv_debt_assess.is_frozen,
py_indiv_debt_assess.treat_result,
py_indiv_debt_assess.treat_error_code,
py_indiv_debt_assess.error_message,
py_indiv_debt_assess.evaluate_result
from drip_loan_ods.iboxchain_db_py_indiv_debt_assess py_indiv_debt_assess
left join drip_loan_ods.app_application_aps_application aps_application
on py_indiv_debt_assess.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
bat_no,
receive_time,
report_id,
build_end_time,
has_system_error,
is_frozen,
treat_result,
treat_error_code,
error_message,
evaluate_result,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
bat_no,
receive_time,
report_id,
build_end_time,
has_system_error,
is_frozen,
treat_result,
treat_error_code,
error_message,
evaluate_result
from t2
where rank = 1;


-- 19.view_extra_moxie_phone			2	自己改完
drop view if exists drip_loan_ods.extra_moxie_phone;
create view if not exists drip_loan_ods.extra_moxie_phone as
with t1 as(
select
db_moxie_phone_rsp.apply_no,
db_moxie_phone_rsp.project_name as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
db_moxie_phone_rsp.mobile,
db_moxie_phone_rsp.card_no,
'' as cust_id,
aps_application.channel,
db_moxie_phone_rsp.riskengine_swift_no,
db_moxie_phone_rsp.other_swift_no,
cast(db_moxie_phone_rsp.create_time as timestamp) as get_data_time,
db_moxie_phone_rsp.query_mobile,
db_moxie_phone_rsp.request_swift_no,
db_moxie_phone_rsp.response_swift_no,
db_moxie_phone_rsp.query_result,
db_moxie_phone_rsp.phone_state,
db_moxie_phone_rsp.is_fee,
db_moxie_phone_rsp.is_success,
db_moxie_phone_rsp.response_code,
db_moxie_phone_rsp.response_msg
from drip_loan_ods.iboxchain_db_db_moxie_phone_rsp db_moxie_phone_rsp
left join drip_loan_ods.app_application_aps_application aps_application
on db_moxie_phone_rsp.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
query_mobile,
request_swift_no,
response_swift_no,
query_result,
phone_state,
is_fee,
is_success,
response_code,
response_msg,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
query_mobile,
request_swift_no,
response_swift_no,
query_result,
phone_state,
is_fee,
is_success,
response_code,
response_msg
from t2
where rank = 1;


-- 20.view_extra_hd_location_check			3    自己改完
drop view if exists drip_loan_ods.extra_hd_location_check;
create view if not exists drip_loan_ods.extra_hd_location_check as
with t1 as(
select
db_hd_address_check_rsp.apply_no,
db_hd_address_check_rsp.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
db_hd_address_check_rsp.mobile,
db_hd_address_check_rsp.card_no,
'' as cust_id,
aps_application.channel,
db_hd_address_check_rsp.riskengine_swift_no,
db_hd_address_check_rsp.other_swift_no,
cast(db_hd_address_check_rsp.create_time as timestamp) as get_data_time,
db_hd_address_check_rsp.longitude as lng,
db_hd_address_check_rsp.latitude as lat,
db_hd_address_check_rsp.inquire_module,
db_hd_address_check_rsp.response_time,
db_hd_address_check_rsp.mobile as tel_no,
'' as option,
db_hd_address_check_rsp.code,
db_hd_address_check_rsp.error_msg as message
from drip_loan_ods.iboxchain_db_db_hd_address_check_rsp db_hd_address_check_rsp
left join drip_loan_ods.app_application_aps_application aps_application
on db_hd_address_check_rsp.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
lng,
lat,
inquire_module,
response_time,
tel_no,
option,
code,
message,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
lng,
lat,
inquire_module,
response_time,
tel_no,
option,
code,
message
from t2
where rank = 1;


-- 23.view_extra_hd_two_check  第一次改  serie_no
drop view if exists drip_loan_ods.extra_hd_two_check;
create view if not exists drip_loan_ods.extra_hd_two_check as
with t1 as(
select
db_hd_two_check_rsp.apply_no,
db_hd_two_check_rsp.project_name as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
db_hd_two_check_rsp.mobile,
db_hd_two_check_rsp.card_no,
'' as cust_id,
aps_application.channel,
db_hd_two_check_rsp.riskengine_swift_no,
db_hd_two_check_rsp.other_swift_no,
cast(db_hd_two_check_rsp.create_time as timestamp) as get_data_time,
db_hd_two_check_rsp.isp,
db_hd_two_check_rsp.terminal_number,
db_hd_two_check_rsp.name,
db_hd_two_check_rsp.inquire_module,
db_hd_two_check_rsp.response_time,
db_hd_two_check_rsp.check_result,
db_hd_two_check_rsp.code,
db_hd_two_check_rsp.error_msg
from drip_loan_ods.iboxchain_db_db_hd_two_check_rsp db_hd_two_check_rsp
left join drip_loan_ods.app_application_aps_application aps_application
on db_hd_two_check_rsp.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
isp,
terminal_number,
name,
inquire_module,
response_time,
check_result,
code,
error_msg,
row_number() over(partition by apply_no,other_swift_no,terminal_number order by get_data_time desc) as rank
from t1)


select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
isp,
terminal_number,
name,
inquire_module,
response_time,
check_result,
code,
error_msg
from t2
where rank = 1;


-- 24.view_extra_xy_cjld_xf				5   自己改完
drop view if exists drip_loan_ods.extra_xy_cjld_xf;
create view if not exists drip_loan_ods.extra_xy_cjld_xf as
with t1 as(
select
xinyan_consumption_stage.apply_no,
xinyan_consumption_stage.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
xinyan_consumption_stage.mobile,
xinyan_consumption_stage.card_no,
'' as cust_id,
aps_application.channel,
xinyan_consumption_stage.riskengine_swift_no,
xinyan_consumption_stage.other_swift_no,
cast(xinyan_consumption_stage.create_time as timestamp) as get_data_time,
cast(xinyan_consumption_stage.xy_140001 as bigint) as xy_140001,
cast(xinyan_consumption_stage.xy_140002 as bigint) as xy_140002,
cast(xinyan_consumption_stage.xy_140003 as bigint) as xy_140003,
cast(xinyan_consumption_stage.xy_140004 as bigint) as xy_140004,
cast(xinyan_consumption_stage.xy_140005 as bigint) as xy_140005,
cast(xinyan_consumption_stage.xy_140006 as bigint) as xy_140006,
cast(xinyan_consumption_stage.xy_140007 as bigint) as xy_140007,
cast(xinyan_consumption_stage.xy_140008 as bigint) as xy_140008,
cast(xinyan_consumption_stage.xy_140009 as bigint) as xy_140009,
cast(xinyan_consumption_stage.xy_140010 as bigint) as xy_140010,
cast(xinyan_consumption_stage.xy_140011 as bigint) as xy_140011,
cast(xinyan_consumption_stage.xy_140012 as bigint) as xy_140012,
cast(xinyan_consumption_stage.xy_140013 as bigint) as xy_140013,
cast(xinyan_consumption_stage.xy_140014 as bigint) as xy_140014,
cast(xinyan_consumption_stage.xy_140015 as bigint) as xy_140015,
cast(xinyan_consumption_stage.xy_140016 as bigint) as xy_140016,
cast(xinyan_consumption_stage.xy_140017 as bigint) as xy_140017,
cast(xinyan_consumption_stage.xy_140018 as bigint) as xy_140018,
cast(xinyan_consumption_stage.xy_140019 as bigint) as xy_140019,
cast(xinyan_consumption_stage.xy_140020 as bigint) as xy_140020,
cast(xinyan_consumption_stage.xy_140021 as bigint) as xy_140021,
cast(xinyan_consumption_stage.xy_140022 as decimal(15,2)) as xy_140022,
cast(xinyan_consumption_stage.xy_140023 as decimal(15,2)) as xy_140023,
cast(xinyan_consumption_stage.xy_140024 as bigint) as xy_140024,
cast(xinyan_consumption_stage.xy_140025 as decimal(15,2)) as xy_140025,
cast(xinyan_consumption_stage.xy_140026 as bigint) as xy_140026,
xinyan_consumption_stage.xy_140027,
xinyan_consumption_stage.xy_140028,
cast(xinyan_consumption_stage.xy_140029 as bigint) as xy_140029,
cast(xinyan_consumption_stage.xy_140030 as bigint) as xy_140030,
cast(xinyan_consumption_stage.xy_140031 as bigint) as xy_140031,
xinyan_consumption_stage.success,
xinyan_consumption_stage.error_code,
xinyan_consumption_stage.error_msg,
xinyan_consumption_stage.trans_id,
xinyan_consumption_stage.trans_no,
xinyan_consumption_stage.code,
xinyan_consumption_stage.desc
from drip_loan_ods.iboxchain_db_xinyan_consumption_stage xinyan_consumption_stage
left join drip_loan_ods.app_application_aps_application aps_application
on xinyan_consumption_stage.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
xy_140001,
xy_140002,
xy_140003,
xy_140004,
xy_140005,
xy_140006,
xy_140007,
xy_140008,
xy_140009,
xy_140010,
xy_140011,
xy_140012,
xy_140013,
xy_140014,
xy_140015,
xy_140016,
xy_140017,
xy_140018,
xy_140019,
xy_140020,
xy_140021,
xy_140022,
xy_140023,
xy_140024,
xy_140025,
xy_140026,
xy_140027,
xy_140028,
xy_140029,
xy_140030,
xy_140031,
success,
error_code,
error_msg,
trans_id,
trans_no,
code,
desc,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
xy_140001,
xy_140002,
xy_140003,
xy_140004,
xy_140005,
xy_140006,
xy_140007,
xy_140008,
xy_140009,
xy_140010,
xy_140011,
xy_140012,
xy_140013,
xy_140014,
xy_140015,
xy_140016,
xy_140017,
xy_140018,
xy_140019,
xy_140020,
xy_140021,
xy_140022,
xy_140023,
xy_140024,
xy_140025,
xy_140026,
xy_140027,
xy_140028,
xy_140029,
xy_140030,
xy_140031,
success,
error_code,
error_msg,
trans_id,
trans_no,
code,
desc
from t2
where rank = 1;


-- 25.view_extra_xy_cjld_xyk			6    自己改完
drop view if exists drip_loan_ods.extra_xy_cjld_xyk;
create view if not exists drip_loan_ods.extra_xy_cjld_xyk as
with t1 as(
select
xinyan_credit_card_compensation.apply_no,
xinyan_credit_card_compensation.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
xinyan_credit_card_compensation.mobile,
xinyan_credit_card_compensation.card_no,
'' as cust_id,
aps_application.channel,
xinyan_credit_card_compensation.riskengine_swift_no,
xinyan_credit_card_compensation.other_swift_no,
cast(xinyan_credit_card_compensation.create_time as timestamp) as get_data_time,
cast(xinyan_credit_card_compensation.xy_130001 as bigint) as xy_130001,
cast(xinyan_credit_card_compensation.xy_130002 as bigint) as xy_130002,
cast(xinyan_credit_card_compensation.xy_130003 as bigint) as xy_130003,
cast(xinyan_credit_card_compensation.xy_130004 as bigint) as xy_130004,
cast(xinyan_credit_card_compensation.xy_130005 as bigint) as xy_130005,
cast(xinyan_credit_card_compensation.xy_130006 as bigint) as xy_130006,
cast(xinyan_credit_card_compensation.xy_130007 as bigint) as xy_130007,
cast(xinyan_credit_card_compensation.xy_130008 as bigint) as xy_130008,
cast(xinyan_credit_card_compensation.xy_130009 as bigint) as xy_130009,
cast(xinyan_credit_card_compensation.xy_130010 as bigint) as xy_130010,
cast(xinyan_credit_card_compensation.xy_130011 as bigint) as xy_130011,
cast(xinyan_credit_card_compensation.xy_130012 as bigint) as xy_130012,
cast(xinyan_credit_card_compensation.xy_130013 as bigint) as xy_130013,
cast(xinyan_credit_card_compensation.xy_130014 as bigint) as xy_130014,
cast(xinyan_credit_card_compensation.xy_130015 as bigint) as xy_130015,
cast(xinyan_credit_card_compensation.xy_130016 as bigint) as xy_130016,
cast(xinyan_credit_card_compensation.xy_130017 as bigint) as xy_130017,
cast(xinyan_credit_card_compensation.xy_130018 as bigint) as xy_130018,
cast(xinyan_credit_card_compensation.xy_130019 as bigint) as xy_130019,
cast(xinyan_credit_card_compensation.xy_130020 as bigint) as xy_130020,
cast(xinyan_credit_card_compensation.xy_130021 as bigint) as xy_130021,
cast(xinyan_credit_card_compensation.xy_130022 as decimal(15,2)) as xy_130022,
cast(xinyan_credit_card_compensation.xy_130023 as decimal(15,2)) as xy_130023,
cast(xinyan_credit_card_compensation.xy_130024 as decimal(15,2)) as xy_130024,
cast(xinyan_credit_card_compensation.xy_130025 as decimal(15,2)) as xy_130025,
cast(xinyan_credit_card_compensation.xy_130026 as decimal(15,2)) as xy_130026,
cast(xinyan_credit_card_compensation.xy_130027 as decimal(15,2)) as xy_130027,
cast(xinyan_credit_card_compensation.xy_130028 as decimal(15,2)) as xy_130028,
cast(xinyan_credit_card_compensation.xy_130029 as decimal(15,2)) as xy_130029,
cast(xinyan_credit_card_compensation.xy_130030 as decimal(15,2)) as xy_130030,
xinyan_credit_card_compensation.xy_130031,
xinyan_credit_card_compensation.success,
xinyan_credit_card_compensation.error_code,
xinyan_credit_card_compensation.error_msg,
xinyan_credit_card_compensation.trans_id,
xinyan_credit_card_compensation.trans_no,
xinyan_credit_card_compensation.code,
xinyan_credit_card_compensation.desc
from drip_loan_ods.iboxchain_db_xinyan_credit_card_compensation xinyan_credit_card_compensation
left join drip_loan_ods.app_application_aps_application aps_application
on xinyan_credit_card_compensation.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
xy_130001,
xy_130002,
xy_130003,
xy_130004,
xy_130005,
xy_130006,
xy_130007,
xy_130008,
xy_130009,
xy_130010,
xy_130011,
xy_130012,
xy_130013,
xy_130014,
xy_130015,
xy_130016,
xy_130017,
xy_130018,
xy_130019,
xy_130020,
xy_130021,
xy_130022,
xy_130023,
xy_130024,
xy_130025,
xy_130026,
xy_130027,
xy_130028,
xy_130029,
xy_130030,
xy_130031,
success,
error_code,
error_msg,
trans_id,
trans_no,
code,
desc,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
xy_130001,
xy_130002,
xy_130003,
xy_130004,
xy_130005,
xy_130006,
xy_130007,
xy_130008,
xy_130009,
xy_130010,
xy_130011,
xy_130012,
xy_130013,
xy_130014,
xy_130015,
xy_130016,
xy_130017,
xy_130018,
xy_130019,
xy_130020,
xy_130021,
xy_130022,
xy_130023,
xy_130024,
xy_130025,
xy_130026,
xy_130027,
xy_130028,
xy_130029,
xy_130030,
xy_130031,
success,
error_code,
error_msg,
trans_id,
trans_no,
code,
desc
from t2
where rank = 1;


-- 26.view_extra_xy_cjld_zde			7	自己改完
drop view if exists drip_loan_ods.extra_xy_cjld_zde;
create view if not exists drip_loan_ods.extra_xy_cjld_zde as
with t1 as(
select
xinyan_medium_large_scale_phased.apply_no,
xinyan_medium_large_scale_phased.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
xinyan_medium_large_scale_phased.mobile,
xinyan_medium_large_scale_phased.card_no,
'' as cust_id,
aps_application.channel,
xinyan_medium_large_scale_phased.riskengine_swift_no,
xinyan_medium_large_scale_phased.other_swift_no,
cast(xinyan_medium_large_scale_phased.create_time as timestamp) as get_data_time,
cast(xinyan_medium_large_scale_phased.xy_120001 as bigint) as xy_120001,
cast(xinyan_medium_large_scale_phased.xy_120002 as decimal(15,2)) as xy_120002,
cast(xinyan_medium_large_scale_phased.xy_120003 as decimal(15,2)) as xy_120003,
cast(xinyan_medium_large_scale_phased.xy_120004 as bigint) as xy_120004,
cast(xinyan_medium_large_scale_phased.xy_120005 as bigint) as xy_120005,
cast(xinyan_medium_large_scale_phased.xy_120006 as bigint) as xy_120006,
cast(xinyan_medium_large_scale_phased.xy_120007 as bigint) as xy_120007,
cast(xinyan_medium_large_scale_phased.xy_120008 as bigint) as xy_120008,
cast(xinyan_medium_large_scale_phased.xy_120009 as bigint) as xy_120009,
cast(xinyan_medium_large_scale_phased.xy_120010 as bigint) as xy_120010,
cast(xinyan_medium_large_scale_phased.xy_120011 as bigint) as xy_120011,
cast(xinyan_medium_large_scale_phased.xy_120012 as bigint) as xy_120012,
cast(xinyan_medium_large_scale_phased.xy_120013 as bigint) as xy_120013,
cast(xinyan_medium_large_scale_phased.xy_120014 as bigint) as xy_120014,
cast(xinyan_medium_large_scale_phased.xy_120015 as bigint) as xy_120015,
cast(xinyan_medium_large_scale_phased.xy_120016 as bigint) as xy_120016,
cast(xinyan_medium_large_scale_phased.xy_120017 as bigint) as xy_120017,
cast(xinyan_medium_large_scale_phased.xy_120018 as bigint) as xy_120018,
cast(xinyan_medium_large_scale_phased.xy_120019 as bigint) as xy_120019,
cast(xinyan_medium_large_scale_phased.xy_120020 as bigint) as xy_120020,
cast(xinyan_medium_large_scale_phased.xy_120021 as bigint) as xy_120021,
cast(xinyan_medium_large_scale_phased.xy_120022 as decimal(15,2)) as xy_120022,
cast(xinyan_medium_large_scale_phased.xy_120023 as decimal(15,2)) as xy_120023,
cast(xinyan_medium_large_scale_phased.xy_120024 as decimal(15,2)) as xy_120024,
xinyan_medium_large_scale_phased.xy_120025,
xinyan_medium_large_scale_phased.xy_120026,
cast(xinyan_medium_large_scale_phased.xy_120027 as decimal(15,2)) as xy_120027,
cast(xinyan_medium_large_scale_phased.xy_120028 as bigint) as xy_120028,
xinyan_medium_large_scale_phased.xy_120029,
cast(xinyan_medium_large_scale_phased.xy_120030 as bigint) as y_120030,
cast(xinyan_medium_large_scale_phased.xy_120031 as bigint) as y_120031,
xinyan_medium_large_scale_phased.success,
xinyan_medium_large_scale_phased.error_code,
xinyan_medium_large_scale_phased.error_msg,
xinyan_medium_large_scale_phased.trans_id,
xinyan_medium_large_scale_phased.trans_no
from drip_loan_ods.iboxchain_db_xinyan_medium_large_scale_phased xinyan_medium_large_scale_phased
left join drip_loan_ods.app_application_aps_application aps_application
on xinyan_medium_large_scale_phased.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
xy_120025
from drip_loan_ods.iboxchain_db_xinyan_medium_large_scale_phased
where xy_120025 >= 120001 and xy_120025 <= 120031),

t3 as(
select
apply_no,
xy_120029
from drip_loan_ods.iboxchain_db_xinyan_medium_large_scale_phased
where xy_120029 >= 120001 and xy_120029 <= 120031),

t4 as(
select
t1.apply_no,
t1.product_code,
t1.appl_time,
t1.mobile,
t1.card_no,
t1.cust_id,
t1.channel,
t1.riskengine_swift_no,
t1.other_swift_no,
t1.get_data_time,
t1.xy_120001,
t1.xy_120002,
t1.xy_120003,
t1.xy_120004,
t1.xy_120005,
t1.xy_120006,
t1.xy_120007,
t1.xy_120008,
t1.xy_120009,
t1.xy_120010,
t1.xy_120011,
t1.xy_120012,
t1.xy_120013,
t1.xy_120014,
t1.xy_120015,
t1.xy_120016,
t1.xy_120017,
t1.xy_120018,
t1.xy_120019,
t1.xy_120020,
t1.xy_120021,
t1.xy_120022,
t1.xy_120023,
t1.xy_120024,
t2.xy_120025,
t1.xy_120026,
t1.xy_120027,
t1.xy_120028,
t3.xy_120029,
t1.y_120030,
t1.y_120031,
t1.success,
t1.error_code,
t1.error_msg,
t1.trans_id,
t1.trans_no
from t1
left join t2
on t1.apply_no = t2.apply_no
left join t3
on t1.apply_no = t3.apply_no),

t5 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
xy_120001,
xy_120002,
xy_120003,
xy_120004,
xy_120005,
xy_120006,
xy_120007,
xy_120008,
xy_120009,
xy_120010,
xy_120011,
xy_120012,
xy_120013,
xy_120014,
xy_120015,
xy_120016,
xy_120017,
xy_120018,
xy_120019,
xy_120020,
xy_120021,
xy_120022,
xy_120023,
xy_120024,
xy_120025,
xy_120026,
xy_120027,
xy_120028,
xy_120029,
y_120030,
y_120031,
success,
error_code,
error_msg,
trans_id,
trans_no,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t4)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
xy_120001,
xy_120002,
xy_120003,
xy_120004,
xy_120005,
xy_120006,
xy_120007,
xy_120008,
xy_120009,
xy_120010,
xy_120011,
xy_120012,
xy_120013,
xy_120014,
xy_120015,
xy_120016,
xy_120017,
xy_120018,
xy_120019,
xy_120020,
xy_120021,
xy_120022,
xy_120023,
xy_120024,
xy_120025,
xy_120026,
xy_120027,
xy_120028,
xy_120029,
y_120030,
y_120031,
success,
error_code,
error_msg,
trans_id,
trans_no
from t5
where rank = 1;


-- 27.view_extra_xy_cjld_xe			8	自己改完
drop view if exists drip_loan_ods.extra_xy_cjld_xe;
create view if not exists drip_loan_ods.extra_xy_cjld_xe as
with t1 as(
select
xinyan_microfinance.apply_no,
xinyan_microfinance.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
xinyan_microfinance.mobile,
xinyan_microfinance.card_no,
'' as cust_id,
aps_application.channel,
xinyan_microfinance.riskengine_swift_no,
xinyan_microfinance.other_swift_no,
cast(xinyan_microfinance.create_time as timestamp) as get_data_time,
cast(xinyan_microfinance.xy_110001 as bigint) as xy_110001,
cast(xinyan_microfinance.xy_110002 as bigint) as xy_110002,
cast(xinyan_microfinance.xy_110003 as bigint) as xy_110003,
cast(xinyan_microfinance.xy_110004 as bigint) as xy_110004,
cast(xinyan_microfinance.xy_110005 as bigint) as xy_110005,
cast(xinyan_microfinance.xy_110006 as bigint) as xy_110006,
cast(xinyan_microfinance.xy_110007 as bigint) as xy_110007,
cast(xinyan_microfinance.xy_110008 as bigint) as xy_110008,
cast(xinyan_microfinance.xy_110009 as bigint) as xy_110009,
cast(xinyan_microfinance.xy_110010 as bigint) as xy_110010,
cast(xinyan_microfinance.xy_110011 as bigint) as xy_110011,
cast(xinyan_microfinance.xy_110012 as bigint) as xy_110012,
cast(xinyan_microfinance.xy_110013 as bigint) as xy_110013,
cast(xinyan_microfinance.xy_110014 as bigint) as xy_110014,
cast(xinyan_microfinance.xy_110015 as bigint) as xy_110015,
cast(xinyan_microfinance.xy_110016 as bigint) as xy_110016,
cast(xinyan_microfinance.xy_110017 as bigint) as xy_110017,
cast(xinyan_microfinance.xy_110018 as bigint) as xy_110018,
cast(xinyan_microfinance.xy_110019 as bigint) as xy_110019,
cast(xinyan_microfinance.xy_110020 as bigint) as xy_110020,
cast(xinyan_microfinance.xy_110021 as bigint) as xy_110021,
cast(xinyan_microfinance.xy_110022 as decimal(15,2)) as xy_110022,
cast(xinyan_microfinance.xy_110023 as decimal(15,2)) as xy_110023,
cast(xinyan_microfinance.xy_110024 as bigint) as xy_110024,
xinyan_microfinance.xy_110025,
cast(xinyan_microfinance.xy_110026 as bigint) as xy_110026,
cast(xinyan_microfinance.xy_110027 as bigint) as xy_110027,
xinyan_microfinance.xy_110028,
xinyan_microfinance.xy_110029,
xinyan_microfinance.xy_110030,
cast(xinyan_microfinance.xy_110031 as bigint) as xy_110031,
xinyan_microfinance.success,
xinyan_microfinance.error_code,
xinyan_microfinance.error_msg,
xinyan_microfinance.trans_id,
xinyan_microfinance.trans_no,
xinyan_microfinance.code,
xinyan_microfinance.desc
from drip_loan_ods.iboxchain_db_xinyan_microfinance xinyan_microfinance
left join drip_loan_ods.app_application_aps_application aps_application
on xinyan_microfinance.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
xy_110001,
xy_110002,
xy_110003,
xy_110004,
xy_110005,
xy_110006,
xy_110007,
xy_110008,
xy_110009,
xy_110010,
xy_110011,
xy_110012,
xy_110013,
xy_110014,
xy_110015,
xy_110016,
xy_110017,
xy_110018,
xy_110019,
xy_110020,
xy_110021,
xy_110022,
xy_110023,
xy_110024,
xy_110025,
xy_110026,
xy_110027,
xy_110028,
xy_110029,
xy_110030,
xy_110031,
success,
error_code,
error_msg,
trans_id,
trans_no,
code,
desc,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
xy_110001,
xy_110002,
xy_110003,
xy_110004,
xy_110005,
xy_110006,
xy_110007,
xy_110008,
xy_110009,
xy_110010,
xy_110011,
xy_110012,
xy_110013,
xy_110014,
xy_110015,
xy_110016,
xy_110017,
xy_110018,
xy_110019,
xy_110020,
xy_110021,
xy_110022,
xy_110023,
xy_110024,
xy_110025,
xy_110026,
xy_110027,
xy_110028,
xy_110029,
xy_110030,
xy_110031,
success,
error_code,
error_msg,
trans_id,
trans_no,
code,
desc
from t2
where rank = 1;



-- 29.view_extra_xy_tz			9	自己改完
drop view if exists drip_loan_ods.extra_xy_tz;
create view if not exists drip_loan_ods.extra_xy_tz as
with t1 as(
select
xinyan_probe_detail.apply_no,
xinyan_probe_detail.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
xinyan_probe_detail.mobile,
xinyan_probe_detail.card_no,
'' as cust_id,
aps_application.channel,
xinyan_probe_detail.riskengine_swift_no,
xinyan_probe_detail.other_swift_no,
cast(xinyan_probe_detail.create_time as timestamp) as get_data_time,
xinyan_probe_detail.result_code,
xinyan_probe_detail.max_overdue_amt,
xinyan_probe_detail.max_overdue_days,
xinyan_probe_detail.latest_overdue_time,
xinyan_probe_detail.max_performance_amt,
xinyan_probe_detail.latest_performance_time,
cast(xinyan_probe_detail.count_performance as bigint) as count_performance,
cast(xinyan_probe_detail.currently_overdue as bigint) as currently_overdue,
cast(xinyan_probe_detail.currently_performance as bigint) as currently_performance,
cast(xinyan_probe_detail.acc_exc as bigint) as acc_exc,
cast(xinyan_probe_detail.acc_sleep as bigint) as acc_sleep,
xinyan_probe_detail.success,
xinyan_probe_detail.error_code,
xinyan_probe_detail.error_msg,
xinyan_probe_detail.trans_id,
xinyan_probe_detail.trans_no,
xinyan_probe_detail.code,
xinyan_probe_detail.desc
from drip_loan_ods.iboxchain_db_xinyan_probe_detail xinyan_probe_detail
left join drip_loan_ods.app_application_aps_application aps_application
on xinyan_probe_detail.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
result_code,
max_overdue_amt,
max_overdue_days,
latest_overdue_time,
max_performance_amt,
latest_performance_time,
count_performance,
currently_overdue,
currently_performance,
acc_exc,
acc_sleep,
success,
error_code,
error_msg,
trans_id,
trans_no,
code,
desc,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
result_code,
max_overdue_amt,
max_overdue_days,
latest_overdue_time,
max_performance_amt,
latest_performance_time,
count_performance,
currently_overdue,
currently_performance,
acc_exc,
acc_sleep,
success,
error_code,
error_msg,
trans_id,
trans_no,
code,
desc
from t2
where rank = 1;



-- 30.view_extra_py_gzdw			10	自己改完
drop view if exists drip_loan_ods.extra_py_gzdw;
create view if not exists drip_loan_ods.extra_py_gzdw as
with t1 as(
select
extra_py_gzdw.apply_no,
extra_py_gzdw.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
extra_py_gzdw.mobile,
extra_py_gzdw.card_no,
'' as cust_id,
aps_application.channel,
extra_py_gzdw.riskengine_swift_no,
extra_py_gzdw.other_swift_no,
cast(extra_py_gzdw.create_time as timestamp) as get_data_time,
extra_py_gzdw.bat_no,
cast(extra_py_gzdw.receive_time as timestamp) as receive_time,
extra_py_gzdw.report_id,
cast(extra_py_gzdw.build_end_time as timestamp) as build_end_time,
extra_py_gzdw.has_system_error,
extra_py_gzdw.is_frozen,
extra_py_gzdw.treat_result,
extra_py_gzdw.treat_error_code,
extra_py_gzdw.error_message,
extra_py_gzdw.corp_name
from drip_loan_ods.iboxchain_db_extra_py_gzdw extra_py_gzdw
left join drip_loan_ods.app_application_aps_application aps_application
on extra_py_gzdw.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
bat_no,
receive_time,
report_id,
build_end_time,
has_system_error,
is_frozen,
treat_result,
treat_error_code,
error_message,
corp_name,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
bat_no,
receive_time,
report_id,
build_end_time,
has_system_error,
is_frozen,
treat_result,
treat_error_code,
error_message,
corp_name
from t2
where rank = 1;





-- 31.view_extra_dw_xsz			10	自己改完
drop view if exists drip_loan_ods.extra_dw_xsz;
create view if not exists drip_loan_ods.extra_dw_xsz as
with t1 as(
select
dw_8403_driving_card.apply_no,
dw_8403_driving_card.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
dw_8403_driving_card.mobile,
dw_8403_driving_card.card_no,
'' as cust_id,
aps_application.channel,
dw_8403_driving_card.swift_no as riskengine_swift_no,
dw_8403_driving_card.other_swift_no,
cast(dw_8403_driving_card.create_time as timestamp) as get_data_time,
dw_8403_driving_card.name,
dw_8403_driving_card.car_no,
dw_8403_driving_card.flapper_type,
dw_8403_driving_card.motor_status,
cast(concat_ws('-',substring(dw_8403_driving_card.fist_register_date,0,4),substring(dw_8403_driving_card.fist_register_date,5,2),substring(dw_8403_driving_card.fist_register_date,7,2)) as timestamp) as fist_register_date,
dw_8403_driving_card.resp_code,
dw_8403_driving_card.resp_desc,
dw_8403_driving_card.code,
dw_8403_driving_card.msg
from drip_loan_ods.iboxchain_db_dw_8403_driving_card dw_8403_driving_card
left join drip_loan_ods.app_application_aps_application aps_application
on dw_8403_driving_card.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
name,
car_no,
flapper_type,
motor_status,
fist_register_date,
resp_code,
resp_desc,
code,
msg,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
name,
car_no,
flapper_type,
motor_status,
fist_register_date,
resp_code,
resp_desc,
code,
msg
from t2
where rank = 1;



-- 32.view_extra_dw_cljz			11	自己改完
drop view if exists drip_loan_ods.extra_dw_cljz;
create view if not exists drip_loan_ods.extra_dw_cljz as
with t1 as(
select
dw_8409_car.apply_no,
dw_8409_car.project as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
aps_application.phone_num as mobile,
aps_application.id_num as card_no,
'' as cust_id,
aps_application.channel,
dw_8409_car.swift_no as riskengine_swift_no,
dw_8409_car.other_swift_no,
cast(dw_8409_car.create_time as timestamp) as get_data_time,
dw_8409_car.car_no,
dw_8409_car.car_type,
dw_8409_car.brand_name,
dw_8409_car.body_color,
dw_8409_car.use_type,
dw_8409_car.vehicle_modelc,
dw_8409_car.vehicle_type,
dw_8409_car.engine_number,
dw_8409_car.engine_model,
dw_8409_car.vin,
cast(dw_8409_car.first_issue_date as timestamp) as first_issue_date,
cast(dw_8409_car.validity_day_end as timestamp) as validity_day_end,
dw_8409_car.owner,
dw_8409_car.vehicle_status,
cast(dw_8409_car.passengers as bigint) as passengers,
cast(dw_8409_car.retirement_date as timestamp) as retirement_date,
dw_8409_car.fuel_type,
cast(dw_8409_car.cc as decimal(15,2)) as cc,
cast(dw_8409_car.pps_date as timestamp) as pps_date,
cast(dw_8409_car.max_journey as decimal(15,2)) as max_journey,
cast(dw_8409_car.shaft as bigint) as shaft,
cast(dw_8409_car.wheel_base as decimal(15,2)) as wheel_base,
cast(dw_8409_car.front_tread as decimal(15,2)) as front_tread,
cast(dw_8409_car.rear_tread as decimal(15,2)) as rear_tread,
cast(dw_8409_car.cross_weight as decimal(15,2)) as cross_weight,
cast(dw_8409_car.curb_weight as decimal(15,2)) as curb_weight,
cast(dw_8409_car.load_weight as decimal(15,2)) as load_weight,
dw_8409_car.resp_code,
dw_8409_car.resp_desc
from drip_loan_ods.iboxchain_db_dw_8409_car dw_8409_car
left join drip_loan_ods.app_application_aps_application aps_application
on dw_8409_car.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
car_no,
car_type,
brand_name,
body_color,
use_type,
vehicle_modelc,
vehicle_type,
engine_number,
engine_model,
vin,
first_issue_date,
validity_day_end,
owner,
vehicle_status,
passengers,
retirement_date,
fuel_type,
cc,
pps_date,
max_journey,
shaft,
wheel_base,
front_tread,
rear_tread,
cross_weight,
curb_weight,
load_weight,
resp_code,
resp_desc,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)


select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
car_no,
car_type,
brand_name,
body_color,
use_type,
vehicle_modelc,
vehicle_type,
engine_number,
engine_model,
vin,
first_issue_date,
validity_day_end,
owner,
vehicle_status,
passengers,
retirement_date,
fuel_type,
cc,
pps_date,
max_journey,
shaft,
wheel_base,
front_tread,
rear_tread,
cross_weight,
curb_weight,
load_weight,
resp_code,
resp_desc
from t2
where rank = 1;




-- 33.view_extra_ceg_vin			12	自己改完
drop view if exists drip_loan_ods.extra_ceg_vin;
create view if not exists drip_loan_ods.extra_ceg_vin as
with t1 as(
select
cheegu_vin_message.apply_no,
cheegu_vin_message.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
cheegu_vin_message.mobile,
cheegu_vin_message.card_no,
'' as cust_id,
aps_application.channel,
cheegu_vin_message.swift_no as riskengine_swift_no,
cheegu_vin_message.other_swift_no,
cast(cheegu_vin_message.create_time as timestamp) as get_data_time,
cheegu_vin_message.vin,
cheegu_vin_message.brandid,
cheegu_vin_message.brandname,
cheegu_vin_message.masterbrand,
cheegu_vin_message.serialid,
cheegu_vin_message.serialname,
cheegu_vin_message.modelid,
cheegu_vin_message.yyyy,
cheegu_vin_message.fullname,
cheegu_vin_message.name,
cheegu_vin_message.listedtime,
cheegu_vin_message.classtype,
cheegu_vin_message.engine,
cast(cheegu_vin_message.engine_exhaust_for_float as decimal(15,1)) as engine_exhaust_for_float,
cast(cheegu_vin_message.engine_exhaust as bigint) as engine_exhaust,
cheegu_vin_message.engine_envir_standard,
cheegu_vin_message.engine_type,
cheegu_vin_message.engine_intake,
cheegu_vin_message.engine_fuel,
cast(cheegu_vin_message.referprice_new as decimal(15,2)) as referprice_new,
cheegu_vin_message.gearbox_type,
cheegu_vin_message.struct,
cheegu_vin_message.drive_type,
cheegu_vin_message.tag,
cheegu_vin_message.code,
cheegu_vin_message.msg
from drip_loan_ods.iboxchain_db_cheegu_vin_message cheegu_vin_message
left join drip_loan_ods.app_application_aps_application aps_application
on cheegu_vin_message.apply_no = aps_application.apply_id),


t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
vin,
brandid,
brandname,
masterbrand,
serialid,
serialname,
modelid,
yyyy,
fullname,
name,
listedtime,
classtype,
engine,
engine_exhaust_for_float,
engine_exhaust,
engine_envir_standard,
engine_type,
engine_intake,
engine_fuel,
referprice_new,
gearbox_type,
struct,
drive_type,
tag,
code,
msg,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)


select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
vin,
brandid,
brandname,
masterbrand,
serialid,
serialname,
modelid,
yyyy,
fullname,
name,
listedtime,
classtype,
engine,
engine_exhaust_for_float,
engine_exhaust,
engine_envir_standard,
engine_type,
engine_intake,
engine_fuel,
referprice_new,
gearbox_type,
struct,
drive_type,
tag,
code,
msg
from t2
where rank = 1;




-- 34.view_extra_hd_address_check		13	自己改完
drop view if exists drip_loan_ods.extra_hd_address_check;
create view if not exists drip_loan_ods.extra_hd_address_check as
with t1 as(
select
db_hd_address_check_rsp.apply_no,
db_hd_address_check_rsp.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
db_hd_address_check_rsp.mobile,
db_hd_address_check_rsp.card_no,
'' as cust_id,
db_hd_address_check_rsp.riskengine_swift_no,
db_hd_address_check_rsp.other_swift_no,
cast(db_hd_address_check_rsp.create_time as timestamp) as get_data_time,
db_hd_address_check_rsp.longitude,
db_hd_address_check_rsp.latitude,
db_hd_address_check_rsp.inquire_module,
db_hd_address_check_rsp.response_time,
db_hd_address_check_rsp.terminal_number,
db_hd_address_check_rsp.distance_difference,
db_hd_address_check_rsp.code,
db_hd_address_check_rsp.error_msg
from drip_loan_ods.iboxchain_db_db_hd_address_check_rsp db_hd_address_check_rsp
left join drip_loan_ods.app_application_aps_application aps_application
on db_hd_address_check_rsp.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
riskengine_swift_no,
other_swift_no,
get_data_time,
longitude,
latitude,
inquire_module,
response_time,
terminal_number,
distance_difference,
code,
error_msg,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)

select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
riskengine_swift_no,
other_swift_no,
get_data_time,
longitude,
latitude,
inquire_module,
response_time,
terminal_number,
distance_difference,
code,
error_msg
from t2
where rank = 1;







-- 35.view_extra_hd_working_check		14	自己改完
drop view if exists drip_loan_ods.extra_hd_working_check;
create view if not exists drip_loan_ods.extra_hd_working_check as
--create view drip_loan_ods.extra_hd_working_check as
with t1 as(
select
db_hd_work_address_check_rsp.apply_no,
db_hd_work_address_check_rsp.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
db_hd_work_address_check_rsp.mobile,
db_hd_work_address_check_rsp.card_no,
'' as cust_id,
aps_application.channel,
db_hd_work_address_check_rsp.swift_no as riskengine_swift_no,
db_hd_work_address_check_rsp.other_swift_no,
cast(db_hd_work_address_check_rsp.create_time as timestamp) as get_data_time,
db_hd_work_address_check_rsp.longitude as lng,
db_hd_work_address_check_rsp.latitude as lat,
db_hd_work_address_check_rsp.inquire_module,
db_hd_work_address_check_rsp.response_time,
db_hd_work_address_check_rsp.tel_no,
'' as option,
db_hd_work_address_check_rsp.code,
db_hd_work_address_check_rsp.error_msg as message
from drip_loan_ods.iboxchain_db_db_hd_work_address_check_rsp db_hd_work_address_check_rsp
left join drip_loan_ods.app_application_aps_application aps_application
on db_hd_work_address_check_rsp.apply_no = aps_application.apply_id),


t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
lng,
lat,
inquire_module,
response_time,
tel_no,
option,
code,
message,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)


select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
lng,
lat,
inquire_module,
response_time,
tel_no,
option,
code,
message
from t2
where rank = 1;




-- 16.view_db_geo_t40301_rsp		15	自己改完

drop view if exists drip_loan_ods.db_geo_t40301_rsp;
create view if not exists drip_loan_ods.db_geo_t40301_rsp as
--create view drip_loan_ods.db_geo_t40301_rsp as
with t1 as(
select
db_geo_t40301_rsp.apply_no,
db_geo_t40301_rsp.channel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
db_geo_t40301_rsp.mobile,
db_geo_t40301_rsp.card_no,
'' as cust_id,
aps_application.channel,
db_geo_t40301_rsp.riskengine_swift_no,
db_geo_t40301_rsp.other_swift_no,
cast(db_geo_t40301_rsp.create_time as timestamp) as get_data_time,
db_geo_t40301_rsp.province,
db_geo_t40301_rsp.city,
db_geo_t40301_rsp.isp,
db_geo_t40301_rsp.tjxx_time,
cast(db_geo_t40301_rsp.regtimes as bigint) as regtimes,
cast(db_geo_t40301_rsp.regplatforms as bigint) as regplatforms,
cast(db_geo_t40301_rsp.reglasttime as timestamp) as reglasttime,
cast(db_geo_t40301_rsp.regfirsttime as timestamp) as regfirsttime,
cast(db_geo_t40301_rsp.regtimes_bank as bigint) as regtimes_bank,
cast(db_geo_t40301_rsp.regplatforms_bank as bigint) as regplatforms_bank,
cast(db_geo_t40301_rsp.reglasttime_bank as timestamp) as reglasttime_bank,
cast(db_geo_t40301_rsp.regfirsttime_bank as timestamp) as regfirsttime_bank,
cast(db_geo_t40301_rsp.regtimes_nonbank as bigint) as regtimes_nonbank,
cast(db_geo_t40301_rsp.regplatforms_nonbank as bigint) as regplatforms_nonbank,
cast(db_geo_t40301_rsp.reglasttime_nonbank as timestamp) as reglasttime_nonbank,
cast(db_geo_t40301_rsp.regfirsttime_nonbank as timestamp) as regfirsttime_nonbank,
cast(db_geo_t40301_rsp.apptimes as bigint) as apptimes,
cast(db_geo_t40301_rsp.appplatforms as bigint) as appplatforms,
db_geo_t40301_rsp.appmoney,
cast(db_geo_t40301_rsp.applasttime as timestamp) as applasttime,
cast(db_geo_t40301_rsp.appfirsttime as timestamp) as appfirsttime,
cast(db_geo_t40301_rsp.apptimes_bank as bigint) as apptimes_bank,
cast(db_geo_t40301_rsp.appplatforms_bank as bigint) as appplatforms_bank,
db_geo_t40301_rsp.appmoney_bank,
cast(db_geo_t40301_rsp.applasttime_bank as timestamp) as applasttime_bank,
cast(db_geo_t40301_rsp.appfirsttime_bank as timestamp) as appfirsttime_bank,
cast(db_geo_t40301_rsp.apptimes_nonbank as bigint) as apptimes_nonbank,
cast(db_geo_t40301_rsp.appplatforms_nonbank as bigint) as appplatforms_nonbank,
db_geo_t40301_rsp.appmoney_nonbank,
cast(db_geo_t40301_rsp.applasttime_nonbank as timestamp) as applasttime_nonbank,
cast(db_geo_t40301_rsp.appfirsttime_nonbank as timestamp) as appfirsttime_nonbank,
cast(db_geo_t40301_rsp.loantimes as bigint) as loantimes,
cast(db_geo_t40301_rsp.loanplatforms as bigint) as loanplatforms,
db_geo_t40301_rsp.loanmoney,
cast(db_geo_t40301_rsp.loanfirsttime as timestamp) as loanfirsttime,
cast(db_geo_t40301_rsp.loanlasttime as timestamp) as loanlasttime,
cast(db_geo_t40301_rsp.loantimes_bank as bigint) as loantimes_bank,
cast(db_geo_t40301_rsp.loanplatforms_bank as bigint) as loanplatforms_bank,
db_geo_t40301_rsp.loanmoney_bank,
cast(db_geo_t40301_rsp.loanfirsttime_bank as timestamp) as loanfirsttime_bank,
cast(db_geo_t40301_rsp.loanlasttime_bank as timestamp) as loanlasttime_bank,
cast(db_geo_t40301_rsp.loantimes_nonbank as bigint) as loantimes_nonbank,
cast(db_geo_t40301_rsp.loanplatforms_nonbank as bigint) as loanplatforms_nonbank,
db_geo_t40301_rsp.loanmoney_nonbank,
cast(db_geo_t40301_rsp.loanfirsttime_nonbank as timestamp) as loanfirsttime_nonbank,
cast(db_geo_t40301_rsp.loanlasttime_nonbank as timestamp) as loanlasttime_nonbank,
cast(db_geo_t40301_rsp.rejtimes as bigint) as rejtimes,
cast(db_geo_t40301_rsp.rejplatforms as bigint) as rejplatforms,
cast(db_geo_t40301_rsp.rejfirsttime as timestamp) as rejfirsttime,
cast(db_geo_t40301_rsp.rejlasttime as timestamp) as rejlasttime,
cast(db_geo_t40301_rsp.rejtimes_bank as bigint) as rejtimes_bank,
cast(db_geo_t40301_rsp.rejplatforms_bank as bigint) as rejplatforms_bank,
cast(db_geo_t40301_rsp.rejfirsttime_bank as timestamp) as rejfirsttime_bank,
cast(db_geo_t40301_rsp.rejlasttime_bank as timestamp) as rejlasttime_bank,
cast(db_geo_t40301_rsp.rejtimes_nonbank as bigint) as rejtimes_nonbank,
cast(db_geo_t40301_rsp.rejplatforms_nonbank as bigint) as rejplatforms_nonbank,
cast(db_geo_t40301_rsp.rejfirsttime_nonbank as timestamp) as rejfirsttime_nonbank,
cast(db_geo_t40301_rsp.rejlasttime_nonbank as timestamp) as rejlasttime_nonbank,
db_geo_t40301_rsp.code,
db_geo_t40301_rsp.msg,
db_geo_t40301_rsp.rsp_type,
db_geo_t40301_rsp.rsp_code
from drip_loan_ods.iboxchain_db_db_geo_t40301_rsp db_geo_t40301_rsp
left join drip_loan_ods.app_application_aps_application aps_application
on db_geo_t40301_rsp.apply_no = aps_application.apply_id),


t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
province,
city,
isp,
tjxx_time,
regtimes,
regplatforms,
reglasttime,
regfirsttime,
regtimes_bank,
regplatforms_bank,
reglasttime_bank,
regfirsttime_bank,
regtimes_nonbank,
regplatforms_nonbank,
reglasttime_nonbank,
regfirsttime_nonbank,
apptimes,
appplatforms,
appmoney,
applasttime,
appfirsttime,
apptimes_bank,
appplatforms_bank,
appmoney_bank,
applasttime_bank,
appfirsttime_bank,
apptimes_nonbank,
appplatforms_nonbank,
appmoney_nonbank,
applasttime_nonbank,
appfirsttime_nonbank,
loantimes,
loanplatforms,
loanmoney,
loanfirsttime,
loanlasttime,
loantimes_bank,
loanplatforms_bank,
loanmoney_bank,
loanfirsttime_bank,
loanlasttime_bank,
loantimes_nonbank,
loanplatforms_nonbank,
loanmoney_nonbank,
loanfirsttime_nonbank,
loanlasttime_nonbank,
rejtimes,
rejplatforms,
rejfirsttime,
rejlasttime,
rejtimes_bank,
rejplatforms_bank,
rejfirsttime_bank,
rejlasttime_bank,
rejtimes_nonbank,
rejplatforms_nonbank,
rejfirsttime_nonbank,
rejlasttime_nonbank,
code,
msg,
rsp_type,
rsp_code,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)


select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
province,
city,
isp,
tjxx_time,
regtimes,
regplatforms,
reglasttime,
regfirsttime,
regtimes_bank,
regplatforms_bank,
reglasttime_bank,
regfirsttime_bank,
regtimes_nonbank,
regplatforms_nonbank,
reglasttime_nonbank,
regfirsttime_nonbank,
apptimes,
appplatforms,
appmoney,
applasttime,
appfirsttime,
apptimes_bank,
appplatforms_bank,
appmoney_bank,
applasttime_bank,
appfirsttime_bank,
apptimes_nonbank,
appplatforms_nonbank,
appmoney_nonbank,
applasttime_nonbank,
appfirsttime_nonbank,
loantimes,
loanplatforms,
loanmoney,
loanfirsttime,
loanlasttime,
loantimes_bank,
loanplatforms_bank,
loanmoney_bank,
loanfirsttime_bank,
loanlasttime_bank,
loantimes_nonbank,
loanplatforms_nonbank,
loanmoney_nonbank,
loanfirsttime_nonbank,
loanlasttime_nonbank,
rejtimes,
rejplatforms,
rejfirsttime,
rejlasttime,
rejtimes_bank,
rejplatforms_bank,
rejfirsttime_bank,
rejlasttime_bank,
rejtimes_nonbank,
rejplatforms_nonbank,
rejfirsttime_nonbank,
rejlasttime_nonbank,
code,
msg,
rsp_type,
rsp_code
from t2
where rank = 1;



-- 37.由于extra_xy_qjda拆成了extra_xy_qjda_cur、extra_xy_qjda_debt、extra_xy_qjda_totaldebt
drop view if exists drip_loan_ods.extra_xy_qjda_cur;
create view if not exists drip_loan_ods.extra_xy_qjda_cur as
--create view drip_loan_ods.extra_xy_qjda_cur as
with t1 as(
select
xinyan_archives_detail.apply_no,
xinyan_archives_detail.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
xinyan_archives_detail.mobile,
xinyan_archives_detail.card_no,
'' as cust_id,
aps_application.channel,
xinyan_archives_detail.riskengine_swift_no,
xinyan_archives_detail.other_swift_no,
cast(xinyan_archives_detail.create_time as timestamp) as get_data_time,
cast(xinyan_archives_detail.debt_amount as decimal(15,2)) as debt_amount,
cast(xinyan_archives_detail.order_count as bigint) as order_count,
cast(xinyan_archives_detail.member_count as bigint) as member_count,
cast(xinyan_archives_detail.current_org_count as bigint) as current_org_count,
xinyan_archives_detail.current_order_amt,
xinyan_archives_detail.current_order_lend_amt,
cast(xinyan_archives_detail.current_order_count as bigint) as current_order_count,
xinyan_archives_detail.success,
xinyan_archives_detail.error_code,
xinyan_archives_detail.error_msg,
xinyan_archives_detail.trans_id,
xinyan_archives_detail.trans_no,
xinyan_archives_detail.code,
xinyan_archives_detail.desc
from drip_loan_ods.iboxchain_db_xinyan_archives_detail xinyan_archives_detail
left join drip_loan_ods.app_application_aps_application aps_application
on xinyan_archives_detail.apply_no = aps_application.apply_id),

t2 as(
select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
debt_amount,
order_count,
member_count,
current_org_count,
current_order_amt,
current_order_lend_amt,
current_order_count,
success,
error_code,
error_msg,
trans_id,
trans_no,
code,
desc,
row_number() over(partition by apply_no,other_swift_no order by get_data_time desc) as rank
from t1)


select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
debt_amount,
order_count,
member_count,
current_org_count,
current_order_amt,
current_order_lend_amt,
current_order_count,
success,
error_code,
error_msg,
trans_id,
trans_no,
code,
desc
from t2
where rank = 1;



drop view if exists drip_loan_ods.extra_xy_qjda_debt;
create view if not exists drip_loan_ods.extra_xy_qjda_debt as
--create view drip_loan_ods.extra_xy_qjda_debt as
with t1 as(
select
xinyan_archives_debt_detail.id,
xinyan_archives_debt_detail.apply_no,
xinyan_archives_debt_detail.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
xinyan_archives_debt_detail.mobile,
xinyan_archives_debt_detail.card_no,
'' as cust_id,
aps_application.channel,
xinyan_archives_debt_detail.riskengine_swift_no,
xinyan_archives_debt_detail.other_swift_no,
cast(xinyan_archives_debt_detail.get_data_time as timestamp) as get_data_time,
xinyan_archives_debt_detail.end_day,
xinyan_archives_debt_detail.bill_type,
xinyan_archives_debt_detail.end_flag,
xinyan_archives_debt_detail.end_money,
xinyan_archives_debt_detail.success,
xinyan_archives_debt_detail.error_code,
xinyan_archives_debt_detail.error_msg,
xinyan_archives_debt_detail.trans_id,
xinyan_archives_debt_detail.trans_no,
xinyan_archives_debt_detail.code,
xinyan_archives_debt_detail.desc
from drip_loan_ods.iboxchain_db_xinyan_archives_debt_detail xinyan_archives_debt_detail
left join drip_loan_ods.app_application_aps_application aps_application
on xinyan_archives_debt_detail.apply_no = aps_application.apply_id),


t2 as(
select
id,
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
end_day,
bill_type,
end_flag,
end_money,
success,
error_code,
error_msg,
trans_id,
trans_no,
code,
desc,
row_number() over(partition by id,apply_no,other_swift_no order by get_data_time desc) as rank
from t1)


select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
end_day,
bill_type,
end_flag,
end_money,
success,
error_code,
error_msg,
trans_id,
trans_no,
code,
desc
from t2
where rank = 1;







drop view if exists drip_loan_ods.extra_xy_qjda_totaldebt;
create view if not exists drip_loan_ods.extra_xy_qjda_totaldebt as
--create view drip_loan_ods.extra_xy_qjda_totaldebt as
with t1 as(
select
xinyan_archives_totaldebt_detail.id,
xinyan_archives_totaldebt_detail.apply_no,
xinyan_archives_totaldebt_detail.chanel as product_code,
cast(aps_application.create_date_time as timestamp) as appl_time,
xinyan_archives_totaldebt_detail.mobile,
xinyan_archives_totaldebt_detail.card_no,
'' as cust_id,
aps_application.channel,
xinyan_archives_totaldebt_detail.riskengine_swift_no,
xinyan_archives_totaldebt_detail.other_swift_no,
cast(xinyan_archives_totaldebt_detail.create_time as timestamp) as get_data_time,
xinyan_archives_totaldebt_detail.totaldebt_date,
cast(xinyan_archives_totaldebt_detail.totaldebt_org_count as bigint) as totaldebt_org_count,
cast(xinyan_archives_totaldebt_detail.totaldebt_order_count as bigint) as totaldebt_order_count,
xinyan_archives_totaldebt_detail.totaldebt_order_amt,
xinyan_archives_totaldebt_detail.new_or_old,
xinyan_archives_totaldebt_detail.totaldebt_order_lend_amt,
xinyan_archives_totaldebt_detail.success,
xinyan_archives_totaldebt_detail.error_code,
xinyan_archives_totaldebt_detail.error_msg,
xinyan_archives_totaldebt_detail.trans_id,
xinyan_archives_totaldebt_detail.trans_no,
xinyan_archives_totaldebt_detail.code,
xinyan_archives_totaldebt_detail.desc
from drip_loan_ods.iboxchain_db_xinyan_archives_totaldebt_detail xinyan_archives_totaldebt_detail
left join drip_loan_ods.app_application_aps_application aps_application
on xinyan_archives_totaldebt_detail.apply_no = aps_application.apply_id),


t2 as(
select
id,
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
totaldebt_date,
totaldebt_org_count,
totaldebt_order_count,
totaldebt_order_amt,
new_or_old,
totaldebt_order_lend_amt,
success,
error_code,
error_msg,
trans_id,
trans_no,
code,
desc,
row_number() over(partition by id,apply_no,other_swift_no order by get_data_time desc) as rank
from t1)


select
apply_no,
product_code,
appl_time,
mobile,
card_no,
cust_id,
channel,
riskengine_swift_no,
other_swift_no,
get_data_time,
totaldebt_date,
totaldebt_org_count,
totaldebt_order_count,
totaldebt_order_amt,
new_or_old,
totaldebt_order_lend_amt,
success,
error_code,
error_msg,
trans_id,
trans_no,
code,
desc
from t2
where rank = 1;


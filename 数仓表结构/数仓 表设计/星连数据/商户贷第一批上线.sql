-- fxd_appl_view
DROP VIEW IF EXISTS drip_loan_ods.fxd_appl_view;
CREATE VIEW IF NOT EXISTS drip_loan_ods.fxd_appl_view AS 
	SELECT  application.apply_id as appl_no,
			cast(application.create_date_time as timestamp) as appl_time,
			application.phone_num as phone,
			application.id_num as id,
			"" as cust_id,
			application.name as cust_name,
			application.channel as channel,
			application.sub_channel as sub_channel,
			'' as cs_id,
			'' as clerk_id,
			application.account_type as acct_type,
			'' as product_type,
			application.phase as current_node_code,
			application.state as apply_status,
			application.reject_code as rj_code,
			application.review,
			cast(application.expiry_date as timestamp) as rj_deadline,
			application.risk_pricing as risk_pricing,
			cast(credit_limit1.risk_limit12 as decimal(16, 2)) as risk_limit12,
  			cast(credit_limit1.risk_limit36 as decimal(16, 2)) as risk_limit36,
   			cast(credit_limit2.approval_limit12 as decimal(16, 2)) as approval_limit12,
    		cast(credit_limit2.approval_limit36 as decimal(16, 2)) as approval_limit36,
			cast(application.loan_amount as decimal(16, 2)) as loan_amount,
			cast(application.repayment_period as int) as period,
			cast(application.age as int) as age,
			customer_detail.marital_state as marital_status,
			case when customer_detail.private_owners like '%true%' then '1' when customer_detail.private_owners like '%false%' then '0' else customer_detail.private_owners end as is_SE,
			customer_detail.home_telephone as family_phone,
			customer_detail.home_province           as family_province,
			customer_detail.home_city               as family_city,
			customer_detail.home_county             as family_county,
			customer_detail.home_street             as family_street,
			customer_detail.home_address_detail     as family_address,
			cast(customer_detail.home_latitude as double)as family_latitude,
			cast(customer_detail.home_longitude as double)as family_longtitude,
			customer_detail.company_name            as company,
			customer_detail.company_telephone       as company_phone,
			customer_detail.company_province        as company_province,
			customer_detail.company_city            as company_city,
			customer_detail.company_county          as company_county,
			customer_detail.company_street          as company_street,
			customer_detail.company_address_detail  as company_address,
			cast(customer_detail.company_latitude as double) as company_latitude,
			cast(customer_detail.company_longitude as double)      as company_longtitude,
			aps_phone_area.province as phone_province,
			aps_phone_area.city as phone_city,
			regexp_replace(application.modified_address,'\t|\r|\n','') as modified_address,
			application.product_code     as product_code,
			cast(application.update_date_time as timestamp) as update_time,
			cast(order.loan_amount as decimal(26, 2)) as loan_limit
	FROM (SELECT * FROM (
			SELECT *,row_number() over(partition by apply_id order by in_hive_time desc) as od FROM drip_loan_ods.app_application_aps_application
			) t  where t.od = 1 
		 ) application
	LEFT JOIN 
		(SELECT * FROM (
			SELECT *,row_number() over(partition by apply_id order by update_date_time desc) as od FROM drip_loan_ods.app_application_aps_customer_detail
			) t  where t.od = 1 
		 ) customer_detail on application.apply_id = customer_detail.apply_id
	 LEFT JOIN 
	  (SELECT t.apply_id,
	            collect_list(t.amount)[0] as risk_limit12,collect_list(t.amount)[1] as risk_limit36,
	            collect_list(t.create_date_time)[0] as create_date_time12,collect_list(t.create_date_time)[1] as create_date_time36
	   FROM (
	    SELECT *,row_number() over(partition by apply_id,term order by update_date_time desc) as od FROM drip_loan_ods.app_application_aps_credit_limit where credit_limit_type = 'MACHINE_REVIEW'
	   ) t where t.od= 1 group by t.apply_id 
	  ) credit_limit1 on application.apply_id = credit_limit1.apply_id 
	 LEFT JOIN
	  (SELECT t.apply_id,
	             collect_list(t.amount)[0] as approval_limit12,collect_list(t.amount)[1] as approval_limit36,
	             collect_list(t.create_date_time)[0] as create_date_time12,collect_list(t.create_date_time)[1] as create_date_time36
	    FROM (
	     SELECT *,row_number() over(partition by apply_id,term order by update_date_time desc) as od FROM drip_loan_ods.app_application_aps_credit_limit where credit_limit_type = 'PEOPLE_REVIEW'
	    ) t where t.od= 1 group by t.apply_id 
	   ) credit_limit2 on application.apply_id= credit_limit2.apply_id
	LEFT JOIN 
		(SELECT * FROM(
				SELECT *, row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_application_aps_phone_area
				
			) t where t.od=1
		) aps_phone_area on substr(application.phone_num,0,7) = aps_phone_area.num_segment
	LEFT JOIN 
  		(SELECT * FROM (
      		SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_gateway_gw_order
   			) t where t.od = 1
  		) order on application.apply_id=order.apply_id;

-- fxd_appl_linkman
DROP VIEW IF EXISTS drip_loan_ods.fxd_appl_linkman;
CREATE VIEW IF NOT EXISTS drip_loan_ods.fxd_appl_linkman AS 
	SELECT 	application.apply_id as appl_no,
			application.product_code as product_code,
			cast(application.create_date_time as timestamp) as appl_time,
			application.phone_num as phone,
			application.id_num as id, 
			'' as cust_id,
			application.channel as channel,
			cast(aps_contact.sequence as int) as linkman_no,
			aps_contact.contact_phone_num as linkman_phone,
			aps_contact.contact_name as linkman_name,
			aps_contact.relation as linkman_relation,
			aps_phone_area.province as phone_province,
			aps_phone_area.city as phone_city,
			cast(aps_contact.create_date_time as timestamp) as create_time		
	FROM(SELECT * FROM (
			SELECT *, row_number() over(partition by apply_id,sequence order by update_date_time desc) as od FROM drip_loan_ods.app_application_aps_contact
			) t where t.od = 1

		) aps_contact
	LEFT JOIN 
		(SELECT * FROM (
			SELECT *,row_number() over(partition by apply_id order by in_hive_time desc) as od FROM  drip_loan_ods.app_application_aps_application 
			)  t where t.od =1 
		) application on application.apply_id = aps_contact.apply_id
	LEFT JOIN 
		(SELECT * FROM(
				SELECT *, row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_application_aps_phone_area
				
			) t where t.od=1
		) aps_phone_area on substr(aps_contact.contact_phone_num,0,7) = aps_phone_area.num_segment;

-- fxd_appl_log
DROP VIEW IF EXISTS drip_loan_ods.fxd_appl_log;
CREATE VIEW IF NOT EXISTS drip_loan_ods.fxd_appl_log AS
	SELECT 	application.apply_id as appl_no,
			cast(application.create_date_time as timestamp) as appl_time,
			application.phone_num as phone,
			application.id_num as id,
			'' as cust_id,
			application.channel as channel,
			trace.phase as node_code,
			trace.state as node_status,
			cast(terminal_environment_start.create_date_time as timestamp) as start_time,
			cast(terminal_environment_end.create_date_time as timestamp) as finish_time
	FROM(SELECT * FROM (
			SELECT *,row_number() over(partition BY apply_id order BY update_date_time desc) as od FROM drip_loan_ods.app_application_aps_application
			) t where t.od = 1
		) application
	LEFT JOIN 
		(SELECT * FROM (
			SELECT *,row_number() over(partition by apply_id,phase order by update_date_time desc) as od FROM drip_loan_ods.app_gateway_gw_apply_trace
			)t where t.od = 1
		) trace on trace.apply_id = application.apply_id
	LEFT JOIN 
	  (select *
	        from (select * from(
	                    select *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_gateway_gw_terminal_environment
	                    ) t where t.od=1 
	             ) environment 
	  )  terminal_environment_start on terminal_environment_start.id = trace.pre_record_id
  	LEFT JOIN 
	  (select *
	        from (select * from(
	                    select *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_gateway_gw_terminal_environment
	                    ) t where t.od=1 
	             ) environment 
	  )  terminal_environment_end on terminal_environment_end.id = trace.finish_record_id;

-- fxd_appl_car_auth
DROP VIEW IF EXISTS drip_loan_ods.fxd_appl_car_auth;
CREATE VIEW IF NOT EXISTS drip_loan_ods.fxd_appl_car_auth AS
	SELECT 	policy_car.apply_id as appl_no,
			cast(application.create_date_time as timestamp) as appl_time,
			application.phone_num as phone,
			application.id_num as id, 
			'' as cust_id,
			application.channel as channel,
			policy_car.car_no as plate_no,
			policy_car.reject_code as au_reason,
			cast(policy_car.create_date_time as timestamp) as au_time
	FROM(SELECT * FROM (
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_application_aps_policy_car
			)t where t.od = 1
		) policy_car
	LEFT JOIN 
		 (SELECT * FROM (
			SELECT *,row_number() over(partition BY id order BY update_date_time desc) as od FROM drip_loan_ods.app_application_aps_application
			) t where t.od = 1
		) application on policy_car.apply_id = application.apply_id;


DROP VIEW IF EXISTS drip_loan_ods.fxd_dv_gps_by_node;
CREATE VIEW IF NOT EXISTS drip_loan_ods.fxd_dv_gps_by_node AS
 SELECT  application.apply_id as appl_no,
   cast(application.create_date_time as timestamp) as appl_time, 
   application.phone_num as phone,
   application.id_num as id,
   '' as cust_id,
   application.channel as channel,
   apply_trace.phase as node_code,
   cast(apply_trace.create_date_time as timestamp) as create_time,
   terminal_environment.ip as phone_ip,
   terminal_environment.imei as phone_imei,
   terminal_environment.system as phone_system,
   terminal_environment.phone_type as phone_type,
   terminal_environment.wifi_mac as wifiMac,
   terminal_environment.wifi_ssid as wifiSSID,
   terminal_environment.wifi_bssid as wifiBSSID,
   tongdun.black_box as td_blackbox,
   tongdun.token_id as td_tokenid,
   cast(terminal_environment.latitude as decimal(15,6)) as latitude,
   cast(terminal_environment.longitude as decimal(15,6)) as longtitude,
   terminal_environment.gps_county as gps_country,
   terminal_environment.gps_province,
   terminal_environment.gps_city,
   terminal_environment.gps_county as gps_district,
   terminal_environment.gps_street,
   '' as gps_address
 FROM (SELECT * FROM (
   SELECT *,row_number() over(partition by id order BY update_date_time desc) as od FROM drip_loan_ods.app_application_aps_application
   ) t where t.od = 1
  ) application
 LEFT JOIN 
  (SELECT * FROM (
   SELECT *,row_number() over(partition by id,phase,create_date_time order BY update_date_time desc) as od FROM drip_loan_ods.app_gateway_gw_apply_trace
   ) t where t.od = 1
  ) apply_trace on apply_trace.apply_id = application.apply_id
 LEFT JOIN 
  (SELECT * FROM (
   SELECT *, row_number() over(partition by id order by update_date_time desc) as od  FROM drip_loan_ods.app_gateway_gw_tong_dun
   ) t where t.od = 1
  ) tongdun on tongdun.apply_id = application.apply_id
 LEFT JOIN 
  (select *
        from (select * from(
                    select *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_gateway_gw_terminal_environment
                    ) t where t.od=1 
             ) environment 
  )  terminal_environment on terminal_environment.id = apply_trace.pre_record_id;

-- fxd_dv_tel_calls
DROP VIEW IF EXISTS drip_loan_ods.fxd_dv_tel_calls;
CREATE VIEW IF NOT EXISTS drip_loan_ods.fxd_dv_tel_calls AS
	SELECT 	record.apply_id as appl_no,
			cast(record.create_date_time as timestamp) as appl_time,
			record.phone_num as phone,
			application.id_num as id,
			'' as cust_id,
			application.channel as channel,
			'' as batch_id,
			cast(record.create_date_time as timestamp) as create_time,
			'' as telbook_num,
			record.phone_num as telcall_num,
			record.name as telcall_name,
			cast(record.time as timestamp) as telcall_time,
			cast(record.duration as int) as telcall_duration,
			cast(record.type as int )as telcall_type
	FROM (SELECT * FROM (
			SELECT *, row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_gateway_gw_phone_call_record
		) t where t.od =1
	) record
	LEFT JOIN (SELECT * FROM (
			SELECT *, row_number() over(partition by apply_id order by update_date_time desc) as od from drip_loan_ods.app_application_aps_application
		) t where t.od =1
	) application on record.apply_id = application.apply_id;

-- fxd_dv_tel_book
DROP VIEW IF EXISTS drip_loan_ods.fxd_dv_tel_book;
CREATE VIEW IF NOT EXISTS drip_loan_ods.fxd_dv_tel_book AS
	SELECT 	record.apply_id as appl_no,
			cast(record.create_date_time as timestamp) as appl_time,
			application.phone_num as phone,
			application.id_num as id,
			'' as cust_id,
			application.channel as channel,
			'' as batch_id,
			cast(record.create_date_time as timestamp) as create_time,
			record.phone_num as telcall_num,
			record.name as telbook_name
	FROM (SELECT * FROM (
			SELECT *, row_number() over(partition by apply_id,phone_num,name order by update_date_time desc) as od from drip_loan_ods.app_gateway_gw_phone_book
		) t where t.od =1
	) record
	LEFT JOIN (SELECT * FROM (
			SELECT *, row_number() over(partition by apply_id order by update_date_time desc) as od from drip_loan_ods.app_application_aps_application
		) t where t.od =1
	) application on record.apply_id = application.apply_id;

--fxd_dv_app
DROP VIEW IF EXISTS drip_loan_ods.fxd_dv_app;
CREATE VIEW IF NOT EXISTS drip_loan_ods.fxd_dv_app AS
	SELECT 	app.apply_id as appl_no,
			cast(app.create_date_time as timestamp) as appl_time,
			application.phone_num as phone,
			application.id_num as id,
			'' as cust_id,
			application.channel as channel,
			'' as batch_id,
			cast(app.create_date_time as timestamp) as create_time,
			app.app_name,
			app.package_name
	FROM (SELECT * FROM (
			SELECT *, row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_gateway_gw_phone_app
		) t where t.od =1
	) app
	LEFT JOIN (SELECT * FROM (
			SELECT *, row_number() over(partition by apply_id order by update_date_time desc) as od from drip_loan_ods.app_application_aps_application
		) t where t.od =1
	) application on app.apply_id = application.apply_id;

-- fxd_dv_telcallsstat
DROP VIEW IF EXISTS drip_loan_ods.fxd_dv_telcallsstat;
CREATE VIEW IF NOT EXISTS drip_loan_ods.fxd_dv_telcallsstat as 
	select 	record_reduce.apply_id	as appl_no,
			cast(record_reduce.create_date_time as timestamp) as appl_time,
			application.phone_num as phone,
			application.id_num as id,
			'' as cust_id,
			application.channel as channel,
			'' as batch_id,
			cast(record_reduce.create_date_time as timestamp) as	create_time,
			cast(record_reduce.count_distinct_3	as int) as duration_sum_3d,
			cast(record_reduce.count_distinct_7	as int) as duration_sum_7d,
			cast(record_reduce.count_distinct_30	as int) as duration_sum_30d,
			cast(record_reduce.count_distinct_90	as int) as duration_sum_90d,
			cast(record_reduce.count_distinct_3	as int) as cnt_3d,
			cast(record_reduce.count_distinct_7	as int) as cnt_7d,
			cast(record_reduce.count_distinct_30	as int) as cnt_30d,
			cast(record_reduce.count_distinct_90	as int) as cnt_90d,
			cast(record_reduce.call_count_3	as int) as telcalls_cnt_3d,
			cast(record_reduce.call_count_7	as int) as telcalls_cnt_7d,
			cast(record_reduce.call_count_30	as int) as telcalls_cnt_30d,
			cast(record_reduce.call_count_90	as int) as telcalls_cnt_90d,
			cast(record_reduce.count_distinct	as int) as telcalls_cnt_total,
			cast(record_reduce.avg_call_duration_3	as int) as duration_avg_3d,
			cast(record_reduce.avg_call_duration_7	as int) as duration_avg_7d,
			cast(record_reduce.avg_call_duration_30	as int) as duration_avg_30d,
			cast(record_reduce.avg_call_duration_90	as int) as duration_avg_90d,
			cast(record_reduce.night_call_duration_ratio_30	as double) as nightcall_ratio_30d,
			cast(record_reduce.night_call_duration_ratio_60	as double) as nightcall_ratio_60d,
			cast(record_reduce.night_call_duration_ratio_90	as double) as nightcall_ratio_90d,
			cast(record_reduce.diff_days_since_first_call	as int) as interval_fstcall,
			cast(record_reduce.diff_days_since_last_call	as int) as interval_lstcall,
			cast('' as int) as	interval_fstcall_1M,
			cast('' as int) as	interval_lstcall_1M,
			cast(record_reduce.self_call_count_3	as int) as selfcall_cnt_3d,
			cast(record_reduce.self_call_count_7	as int) as selfcall_cnt_7d,
			cast(record_reduce.self_call_count_30	as int) as selfcall_cnt_30d,
			cast(record_reduce.self_call_count_90	as int) as selfcall_cnt_90d 
	from(select * from(
			select *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_gateway_gw_phone_call_record_reduce
		) t where t.od =1
	) record_reduce
	LEFT JOIN (SELECT * FROM (
			SELECT *, row_number() over(partition by apply_id order by update_date_time desc) as od from drip_loan_ods.app_application_aps_application
		) t where t.od =1
	) application on record_reduce.apply_id = application.apply_id;

-- fxd_dv_telbookstat
DROP VIEW IF EXISTS drip_loan_ods.fxd_dv_telbookstat;
CREATE VIEW IF NOT EXISTS drip_loan_ods.fxd_dv_telbookstat as 
	select 	app_reduce.apply_id	as appl_no,
			cast(app_reduce.create_date_time as timestamp) as appl_time,
			application.phone_num as phone,
			application.id_num as id,
			'' as cust_id,
			application.channel as channel,
			'' as batch_id,
			cast(app_reduce.create_date_time as timestamp) as create_time,
			cast(record_reduce.count_distinct as int) as	telbook_cnt,
			cast(record_reduce.count_loan_industry as int) as telbook_cnt_loan,
			cast(record_reduce.count_normal_name as int) as	telbook_cnt_notnum
	from (select * from (
			select *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_gateway_gw_phone_app_reduce
			)t where t.od = 1
	) app_reduce
	LEFT JOIN (
		select * from(
			select *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_gateway_gw_phone_book_reduce
		) t where t.od =1
	) record_reduce on app_reduce.apply_id = record_reduce.apply_id
	LEFT JOIN (SELECT * FROM (
			SELECT *, row_number() over(partition by apply_id order by update_date_time desc) as od from drip_loan_ods.app_application_aps_application
		) t where t.od =1
	) application on app_reduce.apply_id = application.apply_id;

DROP VIEW IF EXISTS drip_loan_ods.fxd_dv_appstat;
CREATE VIEW IF NOT EXISTS drip_loan_ods.fxd_dv_appstat as 
	select 	app_reduce.apply_id	as appl_no,
			cast(app_reduce.create_date_time as timestamp) as appl_time,
			application.phone_num as phone,
			application.id_num as id,
			'' as cust_id,
			application.channel as channel,
			'' as batch_id,
			cast(app_reduce.create_date_time	as timestamp) as create_time,
			cast(malicious_app_count as int) as	blackapp_cnt
	from (select * from (
			select *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_gateway_gw_phone_app_reduce
			)t where t.od = 1
	) app_reduce
	LEFT JOIN (SELECT * FROM (
			SELECT *, row_number() over(partition by apply_id order by update_date_time desc) as od from drip_loan_ods.app_application_aps_application
		) t where t.od =1
	) application on app_reduce.apply_id = application.apply_id;

--fxd_dv_telcalls_statdetail
DROP VIEW IF EXISTS drip_loan_ods.fxd_dv_telcalls_statdetail;
CREATE VIEW IF NOT EXISTS drip_loan_ods.fxd_dv_telcalls_statdetail as 
 SELECT  summary.apply_id as appl_no,
   cast(summary.create_date_time as timestamp) as appl_time,
   application.phone_num as phone,
   application.id_num as id,
   application.channel as channel,
   '' as cust_id,
   '' as batch_id,
   cast(summary.create_date_time as timestamp) as create_time,
   summary.phone_num as telcall_num,
   '' as telcall_prov_city,
   cast(summary.total_call_duration_90 as double) as duration_sum_90d,
   cast(summary.call_count_90 as double) as cnt_90d,
   cast(summary.avg_call_duration_90 as double) as duration_avg_90d,
   cast(summary.call_duration_ratio_90 as double) as duration_ratio_90d,
   cast(summary.call_count_ratio_90 as double) as cnt_ratio_90d,
   cast(summary.last_call_time as timestamp) as lst_calltime_90d,
   summary.nick_name as telbook_name
 FROM (select * from (
   select *,row_number() over(partition by apply_id,phone_num order by update_date_time desc) as od from drip_loan_ods.app_gateway_gw_phone_call_record_summary
   )t where t.od = 1
     ) summary
 LEFT JOIN (
  SELECT * FROM (
   SELECT *, row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_application_aps_application
   ) t where t.od = 1
  ) application on application.apply_id = summary.apply_id;



DROP VIEW IF EXISTS drip_loan_ods.fxd_risk_rule_log;
CREATE VIEW IF NOT EXISTS drip_loan_ods.fxd_risk_rule_log as 
	select 	rule_result.apply_no as appl_no,
			rule_result.project_name,
			cast(application.create_date_time as timestamp) as appl_time,
			application.phone_num as phone,
			application.id_num as id,
			'' as cust_id,
			application.channel as channel,
			rule_result.swift_no as batch_id,
			rule_result.module_no as module,
			rule_result.batch_no as order,
			rule_result.rule_no as rule_code,
			rule_result.rule_result as result,
			cast(rule_result.expired_date as int) as validity,
			rule_result.car_no as car_plate_no,
			rule_result.result_code as rj_reason,
			cast(rule_result.excute_time as timestamp) as operate_time
	from (
		SELECT * FROM (
			select *,row_number() over(partition by apply_no,rule_no,excute_time order by id desc) as od FROM drip_loan_ods.iboxchain_db_db_risk_rule_result
			) t  where t.od = 1 
		) rule_result 
	 left join (
		select * from (
			SELECT *, row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_application_aps_application
			) t where t.od = 1
		) application on application.apply_id = rule_result.apply_no;
¡¾
CREATE VIEW IF NOT EXISTS drip_loan_ods.fxd_risk_rule_log as 
	select 	rule_result.apply_no as appl_no,
			rule_result.project_name,
			cast(application.create_date_time as timestamp) as appl_time,
			application.phone_num as phone,
			application.id_num as id,
			'' as cust_id,
			application.channel as channel,
			rule_result.swift_no as batch_id,
			rule_result.module_no as module,
			rule_result.batch_no as order,
			rule_result.rule_no as rule_code,
			rule_result.rule_result as result,
			cast(rule_result.expired_date as int) as validity,
			rule_result.car_no as car_plate_no,
			rule_result.result_code as rj_reason,
			cast(from_unixtime(unix_timestamp(rule_result.excute_time)+28800,'yyyy-MM-dd HH:mm:ss') as timestamp) as operate_time
	from (
		SELECT * FROM (
			select *,row_number() over(partition by apply_no,rule_no,excute_time order by id desc) as od FROM drip_loan_ods.iboxchain_db_db_risk_rule_result
			) t  where t.od = 1 
		) rule_result 
	 left join (
		select * from (
			SELECT *, row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_application_aps_application
			) t where t.od = 1
		) application on application.apply_id = rule_result.apply_no;
¡¿



DROP VIEW IF EXISTS drip_loan_ods.fxd_risk_line_log;
CREATE VIEW IF NOT EXISTS drip_loan_ods.fxd_risk_line_log as 
	select 	credit_line.apply_no as appl_no,
			cast(application.create_date_time as timestamp) as appl_time,
			application.phone_num as phone,
			application.id_num as id,
			'' as cust_id,
			application.channel as channel,
			credit_line.swift_no as  swift_no,
			cast(credit_line.credit_update_time as timestamp) as credit_update_time,
			credit_line.credit_update_app_link as credit_update_app_link,
			cast(credit_line.risk_price as decimal(16,2)) as risk_price,
			cast(credit_line.total_amount_12 as decimal(16,2)) as total_amount_12,
			cast(credit_line.total_amount_36 as decimal(16,2)) as total_amount_36,
			cast(credit_line.high_credit_12 as decimal(16,2)) as high_credit_12,
			cast(credit_line.amount_36_car as  decimal(16,2)) as amount_36_car,
			cast(credit_line.amount_36_house as  decimal(16,2)) as amount_36_house,
			cast(credit_line.total_liability  as decimal(16,2)) as total_liability,
			cast(credit_line.bank_report_liability  as decimal(16,2)) as bank_report_liability,
			cast(credit_line.approval_liability as decimal(16,2)) as approval_liability,
			cast(credit_line.house_revenue as decimal(16,2)) as house_revenue,
			credit_line.housing_stability_index  as housing_stability_index,
			cast(credit_line.car_revenue as decimal(16,2)) as car_revenue,
			credit_line.car_no as car_no,
			cast(credit_line.registration_first_time as timestamp) as registration_first_time,
			credit_line.car_year as car_year,
			cast(credit_line.car_factory_price as decimal(16,2)) as car_factory_price,
			cast(credit_line.fico as double) as fico,
			credit_line.approval_state as approval_state,
			credit_line.house_state as house_state,
			credit_line.car_state as  car_state,
			credit_line.age_state as age_state,
			credit_line.house_user_state as house_user_state,
			credit_line.car_user_state as car_user_state,
			credit_line.age_user_state as age_user_state

	from (select * from(
			SELECT *,row_number() over(partition by apply_no,credit_update_time order by in_hive_time desc) as od FROM drip_loan_ods.iboxchain_db_db_risk_credit_line
			) t  where t.od = 1 
	) credit_line 
	left join (
		SELECT * FROM (
			SELECT *, row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_application_aps_application
			) t where t.od = 1
	) application on application.apply_id = credit_line.apply_no;



-- uw_video_records
DROP VIEW IF EXISTS drip_loan_ods.uw_video_records;
CREATE VIEW IF NOT EXISTS drip_loan_ods.uw_video_records as
 	SELECT  video_log.apply_id as appl_no,
	   cast(application.application_date as TIMESTAMP) as appl_time,
	   application.phone_num as phone,
	   customer.id_num as id,
	   '' as cust_id,
	   application.channel_code as channel,
	   cast(video_log.video_start_time as TIMESTAMP) as video_start_time,
	   cast(video_log.video_end_time as TIMESTAMP) as video_finish_time,
	   acc_account.name as record_name,
	   video_log.operator as record_acc,
	   video_log.self_flag as is_self,
	   video_log.company_info as company_info,
	   video_log.contact_info as linkman_info,
	   video_log.income_info as income_info,
	   video_log.house_info as family_addr_info,
	   video_log.debt_info as debt_info,
	   video_log.loan_purpose as debt_usage,
	   video_log.other_info as other_info,
	   video_log.video_state as video_finish_type
	 FROM (SELECT * FROM (
	   SELECT *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_approver_video_log
	   ) t where t.od =1
	  ) video_log
	 LEFT JOIN (
	  SELECT * FROM (
	   SELECT *, row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_approver_aps_application
	   ) t where t.od = 1
	 ) application on video_log.apply_id = application.apply_id
	 LEFT JOIN (
	  select * from(
	   select *,row_number() over(partition by user_name order by update_date_time desc) as od from drip_loan_ods.app_approver_acc_account
	  ) t where t.od =1
	 ) acc_account on video_log.operator = acc_account.user_name
	 LEFT JOIN (
	  SELECT * FROM (
	   SELECT *, row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_approver_aps_customer_info
	   ) t where t.od = 1
	  ) customer on video_log.apply_id = customer.apply_id;
-- uw_basic_info
DROP VIEW IF EXISTS drip_loan_ods.uw_basic_info;
CREATE VIEW IF NOT EXISTS drip_loan_ods.uw_basic_info as 
	SELECT 	application.apply_id as appl_no,
			cast(application.application_date as timestamp) as appl_time,
			application.phone_num as phone,
			customer_info.id_num as id,
			'' as cust_id,
			application.channel_code as channel,
			customer_info.name as name,
			customer_info.sex as gender,
			cast(customer_info.age as int) as age,
			basic_info.address as registered_address,
			application.location_address as gps_address,
			cast(application.longitude as decimal(15, 2)) as longitude,
			cast(application.latitude as decimal(15, 2)) as latitude,
			basic_info.home_province as family_address1,
			basic_info.home_city as family_address2,
			basic_info.home_county as family_address3,
			basic_info.home_street as family_address4,
			basic_info.home_address_detail as family_address5,
			cast(basic_info.home_telephone as int) as family_phone,
			basic_info.company_name as company,
			basic_info.company_province as company_address1,
			basic_info.company_city as company_address2,
			basic_info.company_county as company_address3,
			basic_info.company_street as company_address4,
			basic_info.company_address_detail as company_address5,
			cast(basic_info.company_telephone as int) as company_phone,
			basic_info.marital_state as marital_status,
			application.account_type as acct_type,
			basic_info.private_owners as is_SE,
			application.funnel_flag as is_hitted,
			cast('' as int) as  credit_score,
			application.external_work_unit as extra_company_name,
			cast(approve_task.work_address_option as int) as extra_company_addr_diff,
			cast(approve_task.home_address_option as int) as extra_family_addr_diff,
            account.name as operate_name,
			basic_info.modifier as operate_acc,
			cast(basic_info.update_date_time as timestamp) as update_time
	FROM(SELECT * FROM (
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_approver_aps_application
			)t where t.od = 1
		) application 
	LEFT JOIN (
		SELECT * FROM (
			SELECT *,row_number() over(partition by apply_id order by update_date_time desc) as od from drip_loan_ods.app_approver_apv_basic_info
			) t where t.od =1
	) basic_info on application.apply_id = basic_info.apply_id
	LEFT JOIN(
		SELECT * FROM (
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_approver_aps_customer_info
			) t where t.od =1
		) customer_info on application.apply_id = customer_info.apply_id
	LEFT JOIN (
		SELECT * FROM (
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_approver_apv_approve_task
			) t where t.od =1
		) approve_task on application.apply_id = approve_task.apply_id
	LEFT JOIN (
		SELECT * FROM (
			SELECT *,row_number() over(partition by user_name order by update_date_time desc) as od from drip_loan_ods.app_approver_acc_account
			) t where t.od =1
		) account on basic_info.modifier = account.user_name;
	

-- uw_confirm_info
DROP VIEW IF EXISTS drip_loan_ods.uw_confirm_info;
CREATE VIEW IF NOT EXISTS drip_loan_ods.uw_confirm_info as
	SELECT 	application.apply_id as appl_no,
			cast(application.application_date as timestamp) as appl_time,
			application.phone_num as phone,
			customer_info.id_num as id,
			'' as cust_id,
			application.channel_code as channel,
			additional.permanent_city as resident_city,
			cast(additional.city_live_start_time as timestamp) as current_city_start_time,
			cast(additional.address_live_start_time as timestamp) as current_address_start_time,
			cast(additional.company_start_time as timestamp) as current_company_start_time,
			additional.industry as industry_type,
			additional.company_nature as company_type,
			additional.professional_title as compamy_duty,
			account.name as operate_name,
			basic_info.modifier as operate_acc,
			cast(basic_info.update_date_time as timestamp) as update_time
	FROM(SELECT * FROM (
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_approver_aps_application
			)t where t.od = 1
		) application 
	LEFT join (
		SELECT * FROM (
			SELECT *,row_number() over(partition by apply_id order by update_date_time desc) as od from drip_loan_ods.app_approver_apv_additional_info
			) t where t.od =1
		) additional on application.apply_id = additional.apply_id
	LEFT JOIN(
		SELECT * FROM (
			SELECT *,row_number() over(partition by apply_id order by update_date_time desc) as od from drip_loan_ods.app_approver_aps_customer_info
			) t where t.od =1
		) customer_info on application.apply_id = customer_info.apply_id
	LEFT JOIN (
		SELECT * FROM (
			SELECT *,row_number() over(partition by apply_id order by update_date_time desc) as od from drip_loan_ods.app_approver_apv_basic_info
			) t where t.od =1
	) basic_info on application.apply_id = basic_info.apply_id
	LEFT JOIN (
		SELECT * FROM (
			SELECT *,row_number() over(partition by user_name order by update_date_time desc) as od from drip_loan_ods.app_approver_acc_account
			) t where t.od =1
	) account on basic_info.modifier = account.user_name;
	
-- uw_asset_info
DROP VIEW IF EXISTS drip_loan_ods.uw_asset_info;
CREATE VIEW IF NOT EXISTS drip_loan_ods.uw_asset_info as
	SELECT 	application.apply_id as appl_no,
			cast(application.application_date as timestamp) as appl_time,
			application.phone_num as phone,
			customer_info.id_num as id,
			'' as cust_id,
			application.channel_code as channel,
			aps_qualifi.policy_house_valid as house_qualif_sys,
			qualifi.policy_house_valid as house_qualif_appr,
			aps_qualifi.policy_car_valid as car_qualif_sys,
			qualifi.policy_car_valid as car_qualif_appr,
			'' as life_ins_quailif_sys,
			'' as life_ins_qualif_appr,
			account.name as operate_name,
			basic_info.modifier as operate_acc,
			cast(basic_info.update_date_time as timestamp) as update_time
	FROM (SELECT * FROM(
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_approver_aps_application
			) t where t.od =1
	) application
	LEFT JOIN (
		SELECT * FROM (
			SELECT *,row_number() over(partition by id order by update_date_time desc)as od from drip_loan_ods.app_approver_apv_qualification
			) t where t.od = 1
		) qualifi on application.apply_id = qualifi.apply_id
	LEFT JOIN (
		SELECT * FROM (
			SELECT *,row_number() over(partition by id order by update_date_time desc)as od from drip_loan_ods.app_approver_aps_qualification
			) t where t.od = 1
		) aps_qualifi on application.apply_id = aps_qualifi.apply_id
	LEFT JOIN(
		SELECT * FROM (
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_approver_aps_customer_info
			) t where t.od =1
		) customer_info on application.apply_id = customer_info.apply_id
	LEFT JOIN (
		SELECT * FROM (
			SELECT *,row_number() over(partition by apply_id order by update_date_time desc) as od from drip_loan_ods.app_approver_apv_basic_info
			) t where t.od =1
	) basic_info on application.apply_id = basic_info.apply_id
	LEFT JOIN (
		SELECT * FROM (
			SELECT *,row_number() over(partition by user_name order by update_date_time desc) as od from drip_loan_ods.app_approver_acc_account
			) t where t.od =1
	) account on basic_info.modifier = account.user_name;

-- uw_linkman
DROP VIEW IF EXISTS drip_loan_ods.uw_linkman;
CREATE VIEW IF NOT EXISTS drip_loan_ods.uw_linkman as
	SELECT 	application.apply_id as appl_no,
			cast(application.application_date as timestamp) as appl_time,
			application.phone_num as phone,
			customer.id_num as id,
			'' as cust_id,
			application.channel_code as channel,
			cast(contacts_info.sequence as int) as linkman_no,
			'' as linkman_resource,
			contacts_info.contact_name as linkman_name,
			contacts_info.contact_phone_num as linkman_phone,
			contacts_info.contact_relation as linkman_relation,
			contacts_info.phone_num_state as non_exist_check,
			contacts_info.hd_two_ele_result as phone_name_check,
			contacts_info.del_flag as is_delete,
			contacts_info.creator as creator,
			contacts_info.modifier as modifier,
			cast(contacts_info.create_date_time as timestamp) as create_time,
			cast(contacts_info.update_date_time as timestamp) as update_time
	FROM (SELECT * FROM(
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_approver_aps_application
			) t where t.od =1
	) application
	LEFT JOIN (
		SELECT * FROM (
			SELECT *,row_number() over(partition by id order by update_date_time desc)as od from drip_loan_ods.app_approver_apv_contacts_info)
			t where t.od = 1
		) contacts_info on application.apply_id = contacts_info.apply_id
	LEFT JOIN (
		SELECT * FROM (
			SELECT *,row_number() over(partition by id order by update_date_time desc)as od from drip_loan_ods.app_approver_aps_customer_info)
			t where t.od = 1
		) customer on application.apply_id = customer.apply_id;

-- uw_telchk_records
DROP VIEW IF EXISTS drip_loan_ods.uw_telchk_records;
CREATE VIEW IF NOT EXISTS drip_loan_ods.uw_telchk_records as
 SELECT  application.apply_id as appl_no,
   cast(application.application_date as TIMESTAMP) as appl_time,
   application.phone_num as phone,
   customer_info.id_num as id,
   '' as cust_id,
   application.channel_code as channel,
   cast(tel_log.create_date_time as timestamp) as create_date_time,
   cast(tel_log.update_date_time as timestamp) as update_date_time,
   tel_log.phone_type as check_type,
   tel_log.name as check_name,
   tel_log.phone_num as check_phone,
   tel_log.relation as check_relation,
   cast(tel_log.call_create_time as TIMESTAMP) as call_create_time,
   cast(tel_log.call_duration as int) as call_time,
   tel_log.sys_connect_state as is_connect,
   account.name as operate_name,
   tel_log.operator as operate_acc,
   cast(tel_log.call_start_time as TIMESTAMP) as start_time,
   cast(tel_log.call_end_time as TIMESTAMP) as finish_time,
   tel_log.remark as comment
 FROM (
  SELECT * FROM(
   SELECT *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_approver_aps_application
   ) t where t.od =1
  ) application
 LEFT JOIN (
  SELECT * FROM (
   SELECT *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_approver_tel_log
   ) t where t.od =1 and  (t.called != '' or t.phone_num != '')
  ) tel_log on application.apply_id = tel_log.apply_id 
 LEFT JOIN (
  SELECT * FROM (
   SELECT *,row_number() over(partition by user_name order by update_date_time desc) as od from drip_loan_ods.app_approver_acc_account
   ) t where t.od =1
 ) account on tel_log.operator = account.user_name
 LEFT JOIN (
  SELECT *
   FROM (
    SELECT *,row_number() over(partition by apply_id order by update_date_time desc) as od FROM drip_loan_ods.app_approver_aps_customer_info
   ) t where t.od= 1 
 ) customer_info on application.apply_id = customer_info.apply_id;

	
-- uw_conclusion
DROP VIEW IF EXISTS drip_loan_ods.uw_conclusion;
CREATE VIEW IF NOT EXISTS drip_loan_ods.uw_conclusion as 
 SELECT  application.apply_id as appl_no,
   cast(application.application_date as TIMESTAMP) as appl_time,
   application.phone_num as phone,
   customer_info.id_num as id,
   '' as cust_id,
   application.channel_code as channel, 
   cast(conclusion.approve_time as TIMESTAMP) as operate_time,
   account.name as operate_name,
   conclusion.operator as operate_acc,
   conclusion.approve_node as uw_node,
   conclusion.approve_status as uw_result,
   conclusion.first_reason as reason_1,
   conclusion.second_reason as reason_2,
   conclusion.comment as appr_comment,
   cast(credit_line.current_limit12 as decimal(15, 2)) as current_limit12,
   cast(credit_line.current_limit36 as decimal(15, 2)) as current_limit36,
   cast(credit_limit.final_credit_line_m12 as decimal(15, 2)) as uw_limit12,
   cast(credit_limit.final_credit_line_m36 as decimal(15, 2)) as uw_limit36
 FROM (SELECT * FROM(
   SELECT *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_approver_aps_application
   ) t where t.od =1
  ) application 
 LEFT JOIN (
  SELECT * FROM(
   SELECT *,regexp_replace(conclusion,'\n|\t|\r', '|') as comment, row_number() over(partition by apply_id order by update_date_time desc) as od from drip_loan_ods.app_approver_apv_conclusion
   ) t where t.od =1
 ) conclusion on application.apply_id = conclusion.apply_id
 LEFT JOIN (
  SELECT * FROM(
   SELECT *,row_number() over(partition by user_name order by update_date_time desc) as od from drip_loan_ods.app_approver_acc_account
   ) t where t.od =1
 ) account on conclusion.operator = account.user_name
 LEFT JOIN (
  SELECT *        
   FROM (
    SELECT *,row_number() over(partition by apply_id order by update_date_time desc) as od FROM drip_loan_ods.app_approver_apv_credit_line
   ) t where t.od= 1 
 ) credit_limit on application.apply_id = credit_limit.apply_id
 LEFT JOIN (
  SELECT t.apply_id,
        collect_list(t.amount)[0] as current_limit12,collect_list(t.amount)[1] as current_limit36
   FROM (
    SELECT *,row_number() over(partition by apply_id,term order by update_date_time desc) as od FROM drip_loan_ods.app_approver_aps_credit_line
   ) t where t.od= 1 group by t.apply_id 
 ) credit_line on application.apply_id = credit_line.apply_id
 LEFT JOIN (
  SELECT *
   FROM (
    SELECT *,row_number() over(partition by apply_id order by update_date_time desc) as od FROM drip_loan_ods.app_approver_aps_customer_info
   ) t where t.od= 1 
 ) customer_info on application.apply_id = customer_info.apply_id;


-- pboc_yx
DROP VIEW IF EXISTS drip_loan_ods.pboc_yx;
CREATE VIEW IF NOT EXISTS drip_loan_ods.pboc_yx as
	SELECT 	pboc.apply_id as appl_no,
			application.product_code as product_code,
        	application.phone_num as phone,
            application.id_num as id,
            '' as cust_id,
            application.channel as channel,
			cast(application.create_date_time as timestamp) as appl_time,
			cast(pboc.create_date_time as timestamp) as create_time,
			cast(pboc.activated_credit_maturity_predict as int) as activ_card_maturity_predict,
			cast(pboc.additional_indebtedness_index as int) as addition_debt_index,
			cast(pboc.asset_status_index as int) as asset_status_index,
			cast(pboc.company_city_index as int)  as company_city_index,
			cast(pboc.credit_account_index as int) as credit_account_index,
			cast(pboc.credit_card_amount_level_predict as int) as card_level_predict,
			cast(pboc.credit_card_amount_usage_predict as int) as card_usage_predict,
			cast(pboc.credit_card_bad_record_12m as int) as card_bad_record_12m,
			cast(pboc.credit_card_bad_record_24m as int) as credit_bad_record_24m,
			cast(pboc.credit_demand_index as int) as credit_demand_index,
			cast(pboc.credit_maturity_predict as int) as card_maturity_predict,
			cast(pboc.credit_overdue_index as int)as credit_overdue_index,
			cast(pboc.housing_stability_index as int)as	housing_stability_index,
			cast(pboc.identity_prediction_index as int)as	identity_prediction_index,
			cast(pboc.living_city_index as int)as	living_city_index,
			cast(pboc.loan_bad_record_12m as int)as	loan_bad_record_12m,
			cast(pboc.loan_type_prediction as int)as	loan_type_prediction,
			cast(pboc.marital_status_index as int)as	marital_status_index,
			cast(pboc.total_liability_index as int)as	total_liability_index,
			cast(pboc.uncancelled_account_index as int)as	uncancelled_card_index

	FROM  (
		SELECT * FROM(
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_application_aps_yntrust_pboc
			) t where t.od =1
	) pboc
	LEFT JOIN 
	(SELECT * FROM(
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_application_aps_application
			) t where t.od =1
		) application  on application.apply_id = pboc.apply_id
	LEFT JOIN (
		SELECT * FROM (
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_application_aps_customer_detail
			) t where t.od = 1
		) detail on detail.apply_id = application.apply_id;


DROP VIEW IF EXISTS drip_loan_ods.loan_view;
CREATE VIEW IF NOT EXISTS drip_loan_ods.loan_view AS
 SELECT  application.apply_id as appl_no,
	application.product_code as product_code,
   cast(application.create_date_time as timestamp) as register_date,
   application.phone_num as mobile_no,
   application.id_num as id_nbr,
   application.name as name,
   '' as cust_id,
   application.channel as channel,
   application.sub_channel as subchannel,
   '' as cs_id,
   application.account_type as acct_type,
   '' as product_type,
   cast(credit_limit1.risk_limit12 as decimal(16, 2)) as risk_limit12,
   cast(credit_limit1.risk_limit36 as decimal(16, 2)) as risk_limit36,
   cast(credit_limit2.approval_limit12 as decimal(16, 2)) as approval_limit12,
   cast(credit_limit2.approval_limit36 as decimal(16, 2)) as approval_limit36,
   cast(view.LOAN_DATE as timestamp) as loan_date,
   cast(view.LOAN_INIT_PRINCIPAL as decimal(16, 2))as loan_init_principal,
   cast(view.TENOR as int)as tenor,
   view.RP_PATTERN as rp_pattern,
   cast(view.RISK_PRICING as decimal(16, 2))as risk_pricing,
   cast(view.INTEREST_RATE as  decimal(16, 6)) as interest_rate,
   cast(view.D_PNT_RATE as decimal(16, 6)) as d_pnt_rate,
   cast(view.REMAIN_PRINCIPAL as decimal(16, 2)) as REMAIN_PRINCIPAL,
   cast(view.OVERDUE_DAYS as int) as overdue_days,
   cast(view.DELQ_AMT as  decimal(16, 6))as delq_amt,
   cast(view.DELQ_BAL as  decimal(16, 6))as delq_bal,
   cast(view.DELQ_INT as  decimal(16, 6))as delq_int,
   cast(view.DELQ_PNT as decimal(16, 6)) as delq_pnt,
   cast(view.MAX_OVERDUE_DAYS as int) max_overdue_days,
   cast(view.PAID_OUT_DATE as TIMESTAMP) as paid_out_date,
   view.SETTLE_TYPE as settle_type,
   view.DD_BANK_NAME as dd_bank_name,
   view.DD_BANK_ACCT_NBR as dd_bank_acct_nbr,
   view.RE_BANK_NAME as re_bank_name,
   view.RE_BANK_ACCT_NBR as re_bank_acct_nbr,
   cast(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as timestamp) as stat_date,
   cast(order.loan_amount as decimal(26, 2)) as loan_limit
 FROM(
  SELECT * FROM (
   SELECT *,row_number() over(partition BY apply_no order BY lst_upd_time desc) as od FROM drip_loan_ods.ccsdb_sas_loan_view
   ) t where t.od = 1
  ) view
 
 LEFT JOIN (
  SELECT * FROM (
   SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_application_aps_application
   )t where t.od = 1
  ) application on view.APPLY_NO = application.unique_id
 LEFT JOIN 
  (SELECT t.apply_id,
            collect_list(t.amount)[0] as risk_limit12,collect_list(t.amount)[1] as risk_limit36,
            collect_list(t.create_date_time)[0] as create_date_time12,collect_list(t.create_date_time)[1] as create_date_time36
   FROM (
    SELECT *,row_number() over(partition by apply_id,term order by update_date_time desc) as od FROM drip_loan_ods.app_application_aps_credit_limit where credit_limit_type = 'MACHINE_REVIEW'
   ) t where t.od= 1 group by t.apply_id 
  ) credit_limit1 on application.apply_id = credit_limit1.apply_id 
 LEFT JOIN
  (SELECT t.apply_id,
             collect_list(t.amount)[0] as approval_limit12,collect_list(t.amount)[1] as approval_limit36,
             collect_list(t.create_date_time)[0] as create_date_time12,collect_list(t.create_date_time)[1] as create_date_time36
    FROM (
     SELECT *,row_number() over(partition by apply_id,term order by update_date_time desc) as od FROM drip_loan_ods.app_application_aps_credit_limit where credit_limit_type = 'PEOPLE_REVIEW'
    ) t where t.od= 1 group by t.apply_id 
   ) credit_limit2 on application.apply_id= credit_limit2.apply_id
 LEFT JOIN 
  (SELECT * FROM (
      SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_gateway_gw_order
   ) t where t.od = 1
  ) order on application.apply_id=order.apply_id;

DROP VIEW IF EXISTS drip_loan_ods.loan_rp_plan;
CREATE VIEW IF NOT EXISTS drip_loan_ods.loan_rp_plan as 
	SELECT 	application.apply_id as appl_no,
			application.product_code as product_code,
			cast(filter_result.TERM as int) as term,
			cast(filter_result.LOAN_PMT_DATE as timestamp) as loan_pmt_date,
			cast(filter_result.LOAN_TERM_TOTAL  as decimal(15, 2))as loan_term_total,
			cast(filter_result.LOAN_TERM_PRINCIPAL as decimal(15, 2)) as loan_term_principal,
			cast(filter_result.LOAN_TERM_INT  as decimal(15, 2))as loan_term_int,
			cast(filter_result.STAT_DATE as timestamp) as stat_date
	FROM  (SELECT * FROM(
			SELECT *,row_number() over(partition by schedule_id order by lst_upd_time desc) as od from drip_loan_ods.ccsdb_sas_loan_rp_plan
			) t where t.od =1 and (t.is_del !='Y' or t.is_del is null)
		) filter_result 
		LEFT JOIN (SELECT * FROM (
			SELECT *, row_number() over(partition by apply_id order by update_date_time desc) as od from drip_loan_ods.app_application_aps_application
		) t where t.od =1
	) application on filter_result.apply_no = application.unique_id;
-- fxd_register_account
DROP VIEW IF EXISTS drip_loan_ods.fxd_register_account;
CREATE VIEW IF NOT EXISTS drip_loan_ods.fxd_register_account AS
	SELECT 	account.phone_num as phone,
			cast(account.create_date_time as timestamp) as create_time,
			channel.channel,
			channel.sub_channel,
			channel.recommend_code
	FROM(SELECT * FROM (
			SELECT *,row_number() over(partition BY phone_num order BY update_date_time desc) as od FROM drip_loan_ods.app_gateway_gw_account
			) t where t.od = 1
		) account
	LEFT JOIN 
		(SELECT * FROM (
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_gateway_gw_account_channel
			) t  where t.od = 1 and t.is_valid = 1
		 ) channel on channel.account_id = account.account_id;

-- fxd_register_customer
DROP VIEW IF EXISTS drip_loan_ods.fxd_register_customer;
CREATE VIEW IF NOT EXISTS drip_loan_ods.fxd_register_customer AS
	SELECT 	account.phone_num as phone,
			cast(customer.create_date_time as timestamp) as create_time,
			customer.customer_id as cust_id,
			customer.name as cust_name,
			customer.id_num as id,
			customer.nation as nation,
			customer.sex as gender,
			cast(customer.birth as timestamp) as birthday,
			customer.address as residence_address,
			customer.authority as issue_machanism,
			cast(customer.valid_date as timestamp) as validity_end,
			customer.front_doc_id as front_url,
			customer.back_doc_id as back_url,
			card_area.province as id_province,
			card_area.city as id_city,
			card_area.area as id_district
	FROM(SELECT * FROM (
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_gateway_gw_customer
			) t  where t.od = 1 
		 ) customer
	LEFT JOIN 
		(SELECT * FROM (
			SELECT *,row_number() over(partition BY phone_num order BY update_date_time desc) as od FROM drip_loan_ods.app_gateway_gw_account
			) t where t.od = 1
		) account on customer.account_id = account.account_id
	LEFT JOIN 
		(SELECT * FROM (
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_application_aps_id_card_area
			) t  where t.od = 1 
		 ) card_area on substr(customer.id_num,0,6) = card_area.card_code;


-- fxd_appl_bank_card
DROP VIEW IF EXISTS drip_loan_ods.fxd_appl_bank_card;
CREATE VIEW IF NOT EXISTS drip_loan_ods.fxd_appl_bank_card AS
	SELECT 	bind_card.apply_id as appl_no,
			cast(application.create_date_time as timestamp) as appl_time,
			application.phone_num as phone,
			application.id_num as id, 
			'' as cust_id,
			application.channel as channel,
			bind_card.account_no as card_no,
			card_bin.card_name as bank_name,
			card_bin.swift_code as bank_code,
			cast(bind_card.create_date_time as timestamp) as bind_time
	FROM
	(SELECT * FROM (
			SELECT *,row_number() over(partition by apply_id,create_date_time order by update_date_time desc) as od FROM drip_loan_ods.app_gateway_gw_yntrust_binding_card
			)t where t.od = 1
		)bind_card
	LEFT JOIN 
		(SELECT * FROM (
			SELECT *,row_number() over(partition BY apply_id order BY update_date_time desc) as od FROM drip_loan_ods.app_application_aps_application
			) t where t.od = 1
		) application on bind_card.apply_id = application.apply_id
	LEFT JOIN 
		(SELECT * FROM (
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_gateway_gw_bank_card_bin
			)t where t.od = 1
		) card_bin on card_bin.card_no_segment = substr(bind_card.account_no,0,6);

-- uw_fraud_check
DROP VIEW IF EXISTS drip_loan_ods.uw_fraud_check;
CREATE VIEW IF NOT EXISTS drip_loan_ods.uw_fraud_check as
	SELECT 	application.apply_id as appl_no,
			cast(application.application_date as timestamp) as appl_time,
			application.phone_num as phone,
			gw_customer.id_num as id,
			''  as cust_id,
			application.channel_code as channel,
			instinct.rule_code as rule,
			instinct.risk_state as is_risky,
			instinct.verify_state as is_adequate,
			instinct.remark as comment,
			account.name as operate_name,
			instinct.modifier as operate_acc,
			cast(instinct.update_date_time as timestamp) as operate_time 
	FROM 
	(SELECT * FROM (
		SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_approver_apv_instinct
			) t where t.od =1
		) instinct
	LEFT JOIN
		(SELECT * FROM (
   			SELECT *,row_number() over(partition by id order BY update_date_time desc) as od FROM drip_loan_ods.app_approver_aps_application
   		) t where t.od = 1
  	) application on instinct.apply_id = application.apply_id
	LEFT JOIN 
  		(SELECT * FROM (
   			SELECT *,row_number() over(partition by id order BY update_date_time desc) as od FROM drip_loan_ods.app_gateway_gw_customer
   		) t where t.od = 1
  	) gw_customer on gw_customer.account_id = application.phone_num
	LEFT JOIN
	(SELECT * FROM (
		SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_approver_apv_approve_task
			) t where t.od =1
		) task on instinct.task_id = task.task_id
  	LEFT JOIN 
  		(SELECT * FROM (
   			SELECT *,row_number() over(partition by user_name order BY update_date_time desc) as od FROM drip_loan_ods.app_approver_acc_account
   		) t where t.od = 1
  	) account on instinct.modifier = account.user_name
	LEFT JOIN 
  		(SELECT * FROM (
   			SELECT *,row_number() over(partition by id order BY update_date_time desc) as od FROM drip_loan_ods.app_approver_apv_person_credit
   		) t where t.od = 1
  	) credit on credit.apply_id = application.apply_id;




-- uw_fraud_check
DROP VIEW IF EXISTS drip_loan_ods.uw_pboc_check;
CREATE VIEW IF NOT EXISTS drip_loan_ods.uw_pboc_check as
	SELECT 	application.apply_id as appl_no,
			cast(application.create_date_time as timestamp) as appl_time,
			application.phone_num as phone,
			gw_customer.id_num as id,
			''  as cust_id,
			application.channel_code as channel,
			credit.index_code as variable_name,
			credit.risk_state as is_risky,
			credit.verify_state as is_checked,
			credit.remark as comment,
			'' as operate_name,
			'' as operate_acc,
			cast('' as timestamp) as operate_time 
	FROM 
		(SELECT * FROM (
   				SELECT *,row_number() over(partition by id order BY update_date_time desc) as od FROM drip_loan_ods.app_approver_aps_application
   			) t where t.od = 1
  		) application
	LEFT JOIN
		(SELECT * FROM (
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_approver_apv_person_credit
			) t where t.od =1
		) credit  on credit.apply_id = application.apply_id
	LEFT JOIN 
  		(SELECT * FROM (
   			SELECT *,row_number() over(partition by id order BY update_date_time desc) as od FROM drip_loan_ods.app_gateway_gw_customer
   		) t where t.od = 1
  	) gw_customer on gw_customer.account_id = application.phone_num;

-- uw_aic_company_info
DROP VIEW IF EXISTS drip_loan_ods.uw_aic_company_info;
CREATE VIEW IF NOT EXISTS drip_loan_ods.uw_aic_company_info as 
	SELECT 	application.apply_id as appl_no,
			cast(application.application_date as timestamp) as appl_time,
			application.phone_num as phone,
			gw_customer.id_num as id,
			'' as cust_id,
			application.channel_code as channel,
			bus_basic_inc.ent_name as company_name,
			bus_basic_inc.reg_no as company_id,
			bus_basic_inc.user_name as corporation,
			bus_basic_inc.run_status as status,
			cast(bus_basic_inc.reg_cap_amt as double) as register_revenue,
			bus_basic_inc.ent_type as company_type,
			cast(bus_basic_inc.open_date as timestamp)as register_time,
			cast(bus_basic_inc.run_date_from as timestamp) as run_time,
			account.name as operate_name,
			bus_basic_inc.modifier as operate_acc,
			cast(bus_basic_inc.update_date_time as timestamp) as operate_time 
	FROM 
	(SELECT * FROM (
		SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_approver_bus_basic_inc
			) t where t.od =1
		) bus_basic_inc
	LEFT JOIN
	(SELECT * FROM (
		SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_approver_apv_approve_task
			) t where t.od =1
		) task on bus_basic_inc.task_id = task.task_id
	LEFT JOIN
	 (SELECT * FROM(
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_approver_aps_application
			) t where t.od =1
		) application on task.apply_id = application.apply_id
	LEFT JOIN 
  		(SELECT * FROM (
   			SELECT *,row_number() over(partition by id order BY update_date_time desc) as od FROM drip_loan_ods.app_gateway_gw_customer
   		) t where t.od = 1
  	) gw_customer on gw_customer.account_id = application.phone_num
  	LEFT JOIN 
  		(SELECT * FROM (
   			SELECT *,row_number() over(partition by user_name order BY update_date_time desc) as od FROM drip_loan_ods.app_approver_acc_account
   		) t where t.od = 1
  	) account on bus_basic_inc.modifier = account.user_name;

-- uw_aic_abnormal_info
DROP VIEW IF EXISTS drip_loan_ods.uw_aic_abnormal_info;
CREATE VIEW IF NOT EXISTS drip_loan_ods.uw_aic_abnormal_info as 
	SELECT 	application.apply_id as appl_no,
			cast(application.application_date as timestamp) as appl_time,
			application.phone_num as phone,
			gw_customer.id_num as id,
			'' as cust_id,
			application.channel_code as channel,
			bus_basic_inc.ent_name as company_name,
			bus_basic_inc.reg_no as company_id,
			bus_basic_inc.user_name as corporation,
			cast(bus_exception_inc.in_date as timestamp) as register_time,
			bus_exception_inc.ent_name as register_org,
			cast(bus_exception_inc.out_date as timestamp) as delete_time,
			bus_exception_inc.detail as comment,
			account.name as operate_name,
			bus_exception_inc.modifier as operate_acc,
			cast(bus_exception_inc.update_date_time as timestamp) as operate_time 
	FROM (SELECT * FROM (
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_approver_bus_exception_inc
			) t where t.od =1
		) bus_exception_inc 
	LEFT JOIN
	(SELECT * FROM (
		SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_approver_bus_basic_inc
			) t where t.od =1
		) bus_basic_inc on bus_exception_inc.batch_no = bus_basic_inc.batch_no
	LEFT JOIN
	(SELECT * FROM (
		SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_approver_apv_approve_task
			) t where t.od =1
		) task on bus_basic_inc.task_id = task.task_id
	LEFT JOIN
	 	(SELECT * FROM(
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_approver_aps_application
			) t where t.od =1
		) application on task.apply_id = application.apply_id
  	LEFT JOIN 
  		(SELECT * FROM (
   			SELECT *,row_number() over(partition by id order BY update_date_time desc) as od FROM drip_loan_ods.app_gateway_gw_customer
   		) t where t.od = 1
  	) gw_customer on gw_customer.account_id = application.phone_num
  	LEFT JOIN 
  		(SELECT * FROM (
   			SELECT *,row_number() over(partition by user_name order BY update_date_time desc) as od FROM drip_loan_ods.app_approver_acc_account
   		) t where t.od = 1
  	) account on bus_exception_inc.modifier = account.user_name;



-- uw_aic_shareholder_info
DROP VIEW IF EXISTS drip_loan_ods.uw_aic_shareholder_info;
CREATE VIEW IF NOT EXISTS drip_loan_ods.uw_aic_shareholder_info as 
	SELECT 	application.apply_id as appl_no,
			cast(application.application_date as timestamp) as appl_time,
			application.phone_num as phone,
			gw_customer.id_num as id,
			'' as cust_id,
			application.channel_code as channel,
			bus_basic_inc.ent_name as company_name,
			bus_basic_inc.reg_no as company_id,
			bus_holder_inc.user_name as stockholder_name,
			bus_holder_inc.con_rate as stockholder_ratio,
			cast(bus_holder_inc.con_date as timestamp) as stock_start_time,
			account.user_name as operate_name,
			bus_holder_inc.modifier as operate_acc,
			cast(bus_holder_inc.update_date_time as timestamp) as operate_time 
	FROM (SELECT * FROM (
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_approver_bus_holder_inc
			) t where t.od =1
		) bus_holder_inc 
	 LEFT JOIN
	(SELECT * FROM (
		SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_approver_bus_basic_inc
			) t where t.od =1
		) bus_basic_inc on bus_holder_inc.batch_no = bus_basic_inc.batch_no
	LEFT JOIN
	(SELECT * FROM (
		SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_approver_apv_approve_task
			) t where t.od =1
		) task on bus_basic_inc.task_id = task.task_id
	LEFT JOIN
	 (SELECT * FROM(
			SELECT *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_approver_aps_application
			) t where t.od =1
		) application on task.apply_id = application.apply_id
	 LEFT JOIN 
  		(SELECT * FROM (
   			SELECT *,row_number() over(partition by id order BY update_date_time desc) as od FROM drip_loan_ods.app_gateway_gw_customer
   		) t where t.od = 1
  	) gw_customer on gw_customer.account_id = application.phone_num
	LEFT JOIN 
  		(SELECT * FROM (
   			SELECT *,row_number() over(partition by user_name order BY update_date_time desc) as od FROM drip_loan_ods.app_approver_acc_account
   		) t where t.od = 1
  	) account on task.primary_operator = account.user_name;


DROP VIEW IF EXISTS drip_loan_ods.loan_dpd_view;
CREATE VIEW IF NOT EXISTS drip_loan_ods.loan_dpd_view as 
	SELECT 	application.APPLY_ID as appl_no,
			application.product_code,
			cast(filter_result.CURR_DPD as int) as curr_dpd,
			cast(filter_result.MAX_DPD_EVER as int) as max_dpd_ever,
			cast(filter_result.lst_upd_time as timestamp) as stat_date
	FROM  (SELECT * FROM(
			SELECT *,row_number() over(partition by APPLY_NO order by LST_UPD_TIME desc) as od from drip_loan_ods.ccsdb_sas_loan_dpd_view
			) t where t.od =1
		) filter_result
 	LEFT JOIN (
  		SELECT * FROM (
   			SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_application_aps_application
   )t where t.od = 1
  ) application on filter_result.APPLY_NO = application.unique_id;

DROP VIEW IF EXISTS drip_loan_ods.loan_dpd_detail;
CREATE VIEW IF NOT EXISTS drip_loan_ods.loan_dpd_detail as 
	SELECT 	application.APPLY_ID as appl_no,
			application.product_code as product_code,
			cast(filter_result.lst_upd_time as timestamp) as stat_date,
			filter_result.CURR_DPD as curr_dpd
	FROM  (SELECT * FROM(
			SELECT *,row_number() over(partition by APPLY_NO,stat_date order by LST_UPD_TIME desc) as od from drip_loan_ods.ccsdb_sas_loan_dpd_detail
			) t where t.od =1
		) filter_result
	LEFT JOIN (
  		SELECT * FROM (
   			SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_application_aps_application
   )t where t.od = 1
  ) application on filter_result.APPLY_NO = application.unique_id;


DROP VIEW IF EXISTS drip_loan_ods.loan_rp_actual;
CREATE VIEW IF NOT EXISTS drip_loan_ods.loan_rp_actual as 
	SELECT 	application.APPLY_ID as appl_no,
	        application.product_code,
			cast(filter_result.LOAN_PMT_DATE as timestamp) as loan_pmt_date,
			cast(filter_result.LOAN_TERM_TOTAL as decimal(15, 2)) as loan_term_total,
			cast(filter_result.LOAN_TERM_PRINCIPAL as decimal(15, 2)) as loan_term_principal,
			cast(filter_result.LOAN_TERM_INT as decimal(15, 2)) as loan_term_int,
			cast(filter_result.LOAN_TERM_PENALTY as decimal(15, 2)) as loan_term_penalty,
			cast(filter_result.TERM as int)as term,
			cast(filter_result.REAL_DATE as timestamp) as real_date,
			cast(filter_result.REAL_TOTAL as decimal(15, 2)) as real_total,
			cast(filter_result.REAL_PRINCIPAL as decimal(15, 2)) as real_principal,
			cast(filter_result.REAL_INT as decimal(15, 2)) as real_int,
			cast(filter_result.REAL_PENALTY as decimal(15, 2)) as real_penalty
	FROM  (SELECT * FROM(
			SELECT *,row_number() over(partition by real_id order by LST_UPD_TIME desc) as od from drip_loan_ods.ccsdb_sas_loan_rp_actual_new
			) t where t.od =1
		) filter_result
	LEFT JOIN (
  		SELECT * FROM (
   			SELECT *,row_number() over(partition by id order by update_date_time desc) as od FROM drip_loan_ods.app_application_aps_application
   )t where t.od = 1
  ) application on filter_result.APPLY_NO = application.unique_id;


-- 开始配置表
DROP VIEW IF EXISTS drip_loan_ods.match_tel_area;
create view IF NOT EXISTS drip_loan_ods.match_tel_area as
	SELECT  phone_area.num_segment as phone_prefix,
			phone_area.province as province,
			phone_area.city as city,
			cast(phone_area.create_date_time as timestamp) as create_time,
			cast(phone_area.update_date_time as timestamp) as update_time
	FROM(SELECT * FROM(
		SELECT *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_application_aps_phone_area
			) t where t.od =1
	) phone_area ;
		
DROP VIEW IF EXISTS drip_loan_ods.match_id_area;
create view IF NOT EXISTS drip_loan_ods.match_id_area as
	SELECT  id_area.card_code as id_prefix,
			id_area.province as province,
			id_area.city as city,
			id_area.area as area,
			cast(id_area.create_date_time as timestamp) as create_time,
			cast(id_area.update_date_time as timestamp) as update_time
	FROM(SELECT * FROM(
		SELECT *,row_number() over(partition by id order by update_date_time desc) as od from drip_loan_ods.app_application_aps_id_card_area

			) t where t.od =1
	) id_area ;
-- 结束配置表

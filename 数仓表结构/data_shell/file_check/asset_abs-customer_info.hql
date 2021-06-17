-- select count(1) from (
select distinct
  stage.project_id                     as product_id_stage,                     ods.product_id                     as product_id_ods,                     if(nvl(stage.project_id,                    'a') != nvl(ods.product_id,                    'a'),1,0) as product_id,
  stage.serial_number                  as due_bill_no_stage,                    ods.due_bill_no                    as due_bill_no_ods,                    if(nvl(stage.serial_number,                 'a') != nvl(ods.due_bill_no,                   'a'),1,0) as due_bill_no,
  stage.guaranty_code                  as guaranty_code_stage,                  ods.guaranty_code                  as guaranty_code_ods,                  if(nvl(stage.guaranty_code,                 'a') != nvl(ods.guaranty_code,                 'a'),1,0) as guaranty_code,

  stage.guaranty_handling_status       as guaranty_handling_status_stage,       ods.guaranty_handling_status       as guaranty_handling_status_ods,       if(nvl(stage.guaranty_handling_status,      'a') != nvl(ods.guaranty_handling_status,      'a'),1,0) as guaranty_handling_status,
  stage.guaranty_alignment             as guaranty_alignment_stage,             ods.guaranty_alignment             as guaranty_alignment_ods,             if(nvl(stage.guaranty_alignment,            'a') != nvl(ods.guaranty_alignment,            'a'),1,0) as guaranty_alignment,
  stage.car_property                   as car_property_stage,                   ods.car_property                   as car_property_ods,                   if(nvl(stage.car_property,                  'a') != nvl(ods.car_property,                  'a'),1,0) as car_property,
  stage.financing_type                 as financing_type_stage,                 ods.financing_type                 as financing_type_ods,                 if(nvl(stage.financing_type,                'a') != nvl(ods.financing_type,                'a'),1,0) as financing_type,
  stage.guarantee_type                 as guarantee_type_stage,                 ods.guarantee_type                 as guarantee_type_ods,                 if(nvl(stage.guarantee_type,                'a') != nvl(ods.guarantee_type,                'a'),1,0) as guarantee_type,
  stage.pawn_value                     as pawn_value_stage,                     ods.pawn_value                     as pawn_value_ods,                     if(nvl(stage.pawn_value,                    'a') != nvl(ods.pawn_value,                    'a'),1,0) as pawn_value,
  stage.car_sales_price                as car_sales_price_stage,                ods.car_sales_price                as car_sales_price_ods,                if(nvl(stage.car_sales_price,               'a') != nvl(ods.car_sales_price,               'a'),1,0) as car_sales_price,
  stage.car_new_price                  as car_new_price_stage,                  ods.car_new_price                  as car_new_price_ods,                  if(nvl(stage.car_new_price,                 'a') != nvl(ods.car_new_price,                 'a'),1,0) as car_new_price,
  stage.total_investment               as total_investment_stage,               ods.total_investment               as total_investment_ods,               if(nvl(stage.total_investment,              'a') != nvl(ods.total_investment,              'a'),1,0) as total_investment,
  stage.purchase_tax_amouts            as purchase_tax_amouts_stage,            ods.purchase_tax_amouts            as purchase_tax_amouts_ods,            if(nvl(stage.purchase_tax_amouts,           'a') != nvl(ods.purchase_tax_amouts,           'a'),1,0) as purchase_tax_amouts,
  stage.insurance_type                 as insurance_type_stage,                 ods.insurance_type                 as insurance_type_ods,                 if(nvl(stage.insurance_type,                'a') != nvl(ods.insurance_type,                'a'),1,0) as insurance_type,
  stage.car_insurance_premium          as car_insurance_premium_stage,          ods.car_insurance_premium          as car_insurance_premium_ods,          if(nvl(stage.car_insurance_premium,         'a') != nvl(ods.car_insurance_premium,         'a'),1,0) as car_insurance_premium,
  stage.total_poundage                 as total_poundage_stage,                 ods.total_poundage                 as total_poundage_ods,                 if(nvl(stage.total_poundage,                'a') != nvl(ods.total_poundage,                'a'),1,0) as total_poundage,
  stage.cumulative_car_transfer_number as cumulative_car_transfer_number_stage, ods.cumulative_car_transfer_number as cumulative_car_transfer_number_ods, if(nvl(stage.cumulative_car_transfer_number,'a') != nvl(ods.cumulative_car_transfer_number,'a'),1,0) as cumulative_car_transfer_number,
  stage.one_year_car_transfer_number   as one_year_car_transfer_number_stage,   ods.one_year_car_transfer_number   as one_year_car_transfer_number_ods,   if(nvl(stage.one_year_car_transfer_number,  'a') != nvl(ods.one_year_car_transfer_number,  'a'),1,0) as one_year_car_transfer_number,
  stage.liability_insurance_cost1      as liability_insurance_cost1_stage,      ods.liability_insurance_cost1      as liability_insurance_cost1_ods,      if(nvl(stage.liability_insurance_cost1,     'a') != nvl(ods.liability_insurance_cost1,     'a'),1,0) as liability_insurance_cost1,
  stage.liability_insurance_cost2      as liability_insurance_cost2_stage,      ods.liability_insurance_cost2      as liability_insurance_cost2_ods,      if(nvl(stage.liability_insurance_cost2,     'a') != nvl(ods.liability_insurance_cost2,     'a'),1,0) as liability_insurance_cost2,
  stage.car_type                       as car_type_stage,                       ods.car_type                       as car_type_ods,                       if(nvl(stage.car_type,                      'a') != nvl(ods.car_type,                      'a'),1,0) as car_type,
  stage.gps_code                       as gps_code_stage,                       ods.gps_code                       as gps_code_ods,                       if(nvl(stage.gps_code,                      'a') != nvl(ods.gps_code,                      'a'),1,0) as gps_code,
  stage.gps_cost                       as gps_cost_stage,                       ods.gps_cost                       as gps_cost_ods,                       if(nvl(stage.gps_cost,                      'a') != nvl(ods.gps_cost,                      'a'),1,0) as gps_cost,
  stage.car_system                     as car_system_stage,                     ods.car_system                     as car_system_ods,                     if(nvl(stage.car_system,                    'a') != nvl(ods.car_system,                    'a'),1,0) as car_system,
  stage.car_age                        as car_age_stage,                        ods.car_age                        as car_age_ods,                        if(nvl(stage.car_age,                       'a') != nvl(ods.car_age,                       'a'),1,0) as car_age,
  stage.car_energy_type                as car_energy_type_stage,                ods.car_energy_type                as car_energy_type_ods,                if(nvl(stage.car_energy_type,               'a') != nvl(ods.car_energy_type,               'a'),1,0) as car_energy_type,
  stage.production_date                as production_date_stage,                ods.production_date                as production_date_ods,                if(nvl(stage.production_date,               'a') != nvl(ods.production_date,               'a'),1,0) as production_date,
  stage.mileage                        as mileage_stage,                        ods.mileage                        as mileage_ods,                        if(nvl(stage.mileage,                       'a') != nvl(ods.mileage,                       'a'),1,0) as mileage,
  stage.register_date                  as register_date_stage,                  ods.register_date                  as register_date_ods,                  if(nvl(stage.register_date,                 'a') != nvl(ods.register_date,                 'a'),1,0) as register_date,
  stage.buy_car_address                as buy_car_address_stage,                ods.buy_car_address                as buy_car_address_ods,                if(nvl(stage.buy_car_address,               'a') != nvl(ods.buy_car_address,               'a'),1,0) as buy_car_address,
  stage.car_colour                     as car_colour_stage,                     ods.car_colour                     as car_colour_ods,                     if(nvl(stage.car_colour,                    'a') != nvl(ods.car_colour,                    'a'),1,0) as car_colour,
  stage.car_model                      as car_model_stage,                      ods.car_model                      as car_model_ods,                      if(nvl(stage.car_model,                     'a') != nvl(ods.car_model,                     'a'),1,0) as car_model,

  stage.car_brand                      as car_brand_stage,                      ods.car_brand                      as car_brand_ods,                      if(nvl(stage.car_brand,                     'a') != nvl(ods.car_brand,                     'a'),1,0) as car_brand,

  stage.frame_num_code                 as frame_num_code_stage,                 ods.frame_num_code                 as frame_num_code_ods,                 if(nvl(stage.frame_num_code,                'a') != nvl(ods.frame_num_code,                'a'),1,0) as frame_num_code,
  stage.engine_num_code                as engine_num_code_stage,                ods.engine_num_code                as engine_num_code_ods,                if(nvl(stage.engine_num_code,               'a') != nvl(ods.engine_num_code,               'a'),1,0) as engine_num_code,
  stage.license_num_code               as license_num_code_stage,               ods.license_num_code               as license_num_code_ods,               if(nvl(stage.license_num_code,              'a') != nvl(ods.license_num_code,              'a'),1,0) as license_num_code,

  stage.frame_num                      as frame_num_stage,                      ods.frame_num                      as frame_num_ods,                      if(nvl(stage.frame_num,                     'a') != nvl(ods.frame_num,                     'a'),1,0) as frame_num,
  stage.engine_num                     as engine_num_stage,                     ods.engine_num                     as engine_num_ods,                     if(nvl(stage.engine_num,                    'a') != nvl(ods.engine_num,                    'a'),1,0) as engine_num,
  stage.license_num                    as license_num_stage,                    ods.license_num                    as license_num_ods,                    if(nvl(stage.license_num,                   'a') != nvl(ods.license_num,                   'a'),1,0) as license_num,

  stage.create_time                    as create_time_stage,                    ods.create_time                    as create_time_ods,                    if(nvl(stage.create_time,                   'a') != nvl(ods.create_time,                   'a'),1,0) as create_time,
  stage.update_time                    as update_time_stage,                    ods.update_time                    as update_time_ods,                    if(nvl(stage.update_time,                   'a') != nvl(ods.update_time,                   'a'),1,0) as update_time,

  '123' as aa
from (
  select distinct
    is_empty(serial_number)                                           as serial_number,
    is_empty(guaranty_code)                                           as guaranty_code,
    is_empty(guaranty_handling_status)                                as guaranty_handling_status,
    is_empty(guaranty_alignment)                                      as guaranty_alignment,
    is_empty(car_property)                                            as car_property,
    is_empty(financing_type)                                          as financing_type,
    is_empty(guarantee_type)                                          as guarantee_type,
    cast(is_empty(pawn_value,0) as decimal(25,5))                     as pawn_value,
    cast(is_empty(car_sales_price,0) as decimal(25,5))                as car_sales_price,
    cast(is_empty(car_new_price,0) as decimal(25,5))                  as car_new_price,
    cast(is_empty(total_investment,0) as decimal(25,5))               as total_investment,
    cast(is_empty(purchase_tax_amouts,0) as decimal(25,5))            as purchase_tax_amouts,
    is_empty(insurance_type)                                          as insurance_type,
    cast(is_empty(car_insurance_premium,0) as decimal(25,5))          as car_insurance_premium,
    cast(is_empty(total_poundage,0) as decimal(25,5))                 as total_poundage,
    cast(is_empty(cumulative_car_transfer_number,0) as decimal(20,0)) as cumulative_car_transfer_number,
    cast(is_empty(one_year_car_transfer_number,0) as decimal(20,0))   as one_year_car_transfer_number,
    cast(is_empty(liability_insurance_cost1,0) as decimal(25,5))      as liability_insurance_cost1,
    cast(is_empty(liability_insurance_cost2,0) as decimal(25,5))      as liability_insurance_cost2,
    is_empty(car_type)                                                as car_type,
    is_empty(gps_code)                                                as gps_code,
    cast(is_empty(gps_cost,0) as decimal(25,5))                       as gps_cost,
    is_empty(car_system)                                              as car_system,
    ptrim(is_empty(car_model))                                        as car_model,
    cast(is_empty(car_age,0) as decimal(20,0))                        as car_age,
    is_empty(car_energy_type)                                         as car_energy_type,
    is_empty(production_date)                                         as production_date,
    cast(is_empty(mileage,0) as decimal(25,5))                        as mileage,
    is_empty(register_date)                                           as register_date,
    is_empty(buy_car_address)                                         as buy_car_address,
    is_empty(car_colour)                                              as car_colour,
    is_empty(create_time)                                             as create_time,
    is_empty(update_time)                                             as update_time,

    is_empty(car_brand)                                               as car_brand,

    is_empty(ptrim(frame_num))                                        as frame_num_code,
    is_empty(ptrim(engine_num))                                       as engine_num_code,
    is_empty(ptrim(license_num))                                      as license_num_code,

    sha256(ptrim(frame_num),'frameNumber',1)                          as frame_num,
    sha256(ptrim(engine_num),'engineNumber',1)                        as engine_num,
    sha256(ptrim(license_num),'plateNumber',1)                        as license_num,

    is_empty(project_id)                                              as project_id
  from stage.abs_01_t_loancontractinfo
  where 1 > 0
    and project_id in (select distinct is_empty(map_from_str(extra_info)['项目编号'],project_id) from stage.asset_01_t_loan_contract_info)
) as stage
left join (
  select
    is_empty(map_from_str(extra_info)['借据号'],asset_id)                                        as apply_no,
    is_empty(map_from_str(extra_info)['贷款合同编号'],contract_code)                             as contract_no,
    is_empty(map_from_str(extra_info)['借据号'],asset_id)                                        as due_bill_no,
    is_empty(map_from_str(extra_info)['担保方式'],guarantee_type)                                as guarantee_type,
    is_empty(map_from_str(extra_info)['贷款用途'],loan_use)                                      as loan_usage,
    case when length(map_from_str(extra_info)['合同开始时间']) = 19 then to_date(map_from_str(extra_info)['合同开始时间'])
    else is_empty(datefmt(map_from_str(extra_info)['合同开始时间'],'','yyyy-MM-dd')) end         as loan_issue_date,
    case when length(map_from_str(extra_info)['合同结束时间']) = 19 then to_date(map_from_str(extra_info)['合同结束时间'])
    else is_empty(datefmt(map_from_str(extra_info)['合同结束时间'],'','yyyy-MM-dd')) end         as loan_expiry_date,
    case
    when length(map_from_str(extra_info)['贷款还款日']) = 1 then datefmt(map_from_str(extra_info)['贷款还款日'],'d','dd')
    when length(map_from_str(extra_info)['贷款还款日']) = 2 then map_from_str(extra_info)['贷款还款日']
    else is_empty(map_from_str(extra_info)['贷款还款日']) end                                    as cycle_day,
    is_empty(
      case when length(map_from_str(extra_info)['实际放款时间']) = 19 then to_date(map_from_str(extra_info)['实际放款时间'])
      else is_empty(datefmt(map_from_str(extra_info)['实际放款时间'],'','yyyy-MM-dd')) end,
      case when length(map_from_str(extra_info)['合同开始时间']) = 19 then to_date(map_from_str(extra_info)['合同开始时间'])
      else is_empty(datefmt(map_from_str(extra_info)['合同开始时间'],'','yyyy-MM-dd')) end
    )                                                                                            as loan_active_date,
    is_empty(
      case when length(map_from_str(extra_info)['最后一次预计扣款时间']) = 19 then to_date(map_from_str(extra_info)['最后一次预计扣款时间'])
      else is_empty(datefmt(map_from_str(extra_info)['最后一次预计扣款时间'],'','yyyy-MM-dd')) end,
      case when length(map_from_str(extra_info)['合同结束时间']) = 19 then to_date(map_from_str(extra_info)['合同结束时间'])
      else is_empty(datefmt(map_from_str(extra_info)['合同结束时间'],'','yyyy-MM-dd')) end
    )                                                                                            as loan_expire_date,
    case is_empty(map_from_str(extra_info)['还款方式'],repay_type)
    when '等额本金'             then 'MCEP'
    when '等额本息'             then 'MCEI'
    when '消费转分期'           then 'R'
    when '现金分期'             then 'C'
    when '账单分期'             then 'B'
    when 'POS分期'              then 'P'
    when '大额分期（专项分期）' then 'M'
    when '随借随还'             then 'MCAT'
    when '阶梯还款'             then 'STAIR'
    else is_empty(map_from_str(extra_info)['还款方式'],repay_type) end                           as loan_type,
    is_empty(map_from_str(extra_info)['还款方式'],repay_type)                                    as loan_type_cn,
    is_empty(map_from_str(extra_info)['日利率计算基础'])                                         as contract_daily_interest_rate_basis,
    is_empty(map_from_str(extra_info)['总期数'],periods)                                         as loan_init_term,
    is_empty(map_from_str(extra_info)['贷款总金额(元)'],is_empty(loan_total_amount,0))           as loan_init_principal,
    is_empty(map_from_str(extra_info)['利率类型'],interest_rate_type)                            as interest_rate_type,
    is_empty(loan_interest_rate,is_empty(map_from_str(extra_info)['贷款年利率(%)'],0))           as loan_init_interest_rate,
    is_empty(map_from_str(extra_info)['手续费利率'],0)                                           as loan_init_term_fee_rate,
    0                                                                                            as loan_init_svc_fee_rate,
    is_empty(map_from_str(extra_info)['贷款罚息利率(%)'],is_empty(loan_penalty_rate,0))          as loan_init_penalty_rate,
    is_empty(map_from_str(extra_info)['尾付款金额'])                                             as tail_amount,
    is_empty(map_from_str(extra_info)['尾付比例'])                                               as tail_amount_rate,
    is_empty(map_from_str(extra_info)['产品编号'])                                               as bus_product_id,
    is_empty(map_from_str(extra_info)['产品方案名称'])                                           as bus_product_name,
    is_empty(map_from_str(extra_info)['抵押率(%)'])                                              as mortgage_rate,
    is_empty(map_from_str(extra_info)['项目编号'],project_id)                                    as product_id
  from stage.asset_01_t_loan_contract_info
) as asset
on  stage.project_id    = ods.product_id
and stage.serial_number = ods.due_bill_no
and is_empty(stage.guaranty_code,'a') = is_empty(ods.guaranty_code,'a')
and is_empty(stage.frame_num,'a')     = is_empty(ods.frame_num,'a')
and is_empty(stage.engine_num,'a')    = is_empty(ods.engine_num,'a')
and is_empty(stage.license_num,'a')   = is_empty(ods.license_num,'a')
where 1 > 0
  -- and stage.project_id = 'CL202101260095'
  and (
    nvl(stage.serial_number,                 'a') != nvl(ods.due_bill_no,                   'a') or
    nvl(stage.guaranty_code,                 'a') != nvl(ods.guaranty_code,                 'a') or
    nvl(stage.guaranty_handling_status,      'a') != nvl(ods.guaranty_handling_status,      'a') or
    nvl(stage.guaranty_alignment,            'a') != nvl(ods.guaranty_alignment,            'a') or
    nvl(stage.car_property,                  'a') != nvl(ods.car_property,                  'a') or
    nvl(stage.financing_type,                'a') != nvl(ods.financing_type,                'a') or
    nvl(stage.guarantee_type,                'a') != nvl(ods.guarantee_type,                'a') or
    nvl(stage.pawn_value,                    'a') != nvl(ods.pawn_value,                    'a') or
    nvl(stage.car_sales_price,               'a') != nvl(ods.car_sales_price,               'a') or
    nvl(stage.car_new_price,                 'a') != nvl(ods.car_new_price,                 'a') or
    nvl(stage.total_investment,              'a') != nvl(ods.total_investment,              'a') or
    nvl(stage.purchase_tax_amouts,           'a') != nvl(ods.purchase_tax_amouts,           'a') or
    nvl(stage.insurance_type,                'a') != nvl(ods.insurance_type,                'a') or
    nvl(stage.car_insurance_premium,         'a') != nvl(ods.car_insurance_premium,         'a') or
    nvl(stage.total_poundage,                'a') != nvl(ods.total_poundage,                'a') or
    nvl(stage.cumulative_car_transfer_number,'a') != nvl(ods.cumulative_car_transfer_number,'a') or
    nvl(stage.one_year_car_transfer_number,  'a') != nvl(ods.one_year_car_transfer_number,  'a') or
    nvl(stage.liability_insurance_cost1,     'a') != nvl(ods.liability_insurance_cost1,     'a') or
    nvl(stage.liability_insurance_cost2,     'a') != nvl(ods.liability_insurance_cost2,     'a') or
    nvl(stage.car_type,                      'a') != nvl(ods.car_type,                      'a') or
    nvl(stage.gps_code,                      'a') != nvl(ods.gps_code,                      'a') or
    nvl(stage.gps_cost,                      'a') != nvl(ods.gps_cost,                      'a') or
    nvl(stage.car_system,                    'a') != nvl(ods.car_system,                    'a') or
    nvl(stage.car_model,                     'a') != nvl(ods.car_model,                     'a') or
    nvl(stage.car_age,                       'a') != nvl(ods.car_age,                       'a') or
    nvl(stage.car_energy_type,               'a') != nvl(ods.car_energy_type,               'a') or
    nvl(stage.production_date,               'a') != nvl(ods.production_date,               'a') or
    nvl(stage.mileage,                       'a') != nvl(ods.mileage,                       'a') or
    nvl(stage.register_date,                 'a') != nvl(ods.register_date,                 'a') or
    nvl(stage.buy_car_address,               'a') != nvl(ods.buy_car_address,               'a') or
    nvl(stage.car_colour,                    'a') != nvl(ods.car_colour,                    'a') or

    nvl(stage.car_brand,                     'a') != nvl(ods.car_brand,                     'a') or

    nvl(stage.frame_num,                     'a') != nvl(ods.frame_num,                     'a') or
    nvl(stage.engine_num,                    'a') != nvl(ods.engine_num,                    'a') or
    nvl(stage.license_num,                   'a') != nvl(ods.license_num,                   'a') or

    nvl(stage.frame_num_code,                'a') != nvl(ods.frame_num_code,                'a') or
    nvl(stage.engine_num_code,               'a') != nvl(ods.engine_num_code,               'a') or
    nvl(stage.license_num_code,              'a') != nvl(ods.license_num_code,              'a') or
    nvl(stage.project_id,                    'a') != nvl(ods.product_id,                    'a')
  )
order by product_id_stage,due_bill_no_stage,guaranty_code_stage
-- ) as tmp
-- order by car_brand_en_ods
-- limit 50
;




select distinct
  is_empty(map_from_str(extra_info)['项目编号'],project_id)                         as product_id,
  is_empty(map_from_str(extra_info)['借据号'],asset_id)                             as due_bill_no,
  is_empty(map_from_str(extra_info)['抵押物编号'],guaranty_umber)                   as guaranty_code,
  is_empty(map_from_str(extra_info)['抵押办理状态'],mortgage_handle_status)         as guaranty_handling_status,
  is_empty(map_from_str(extra_info)['抵押顺位'],mortgage_alignment)                 as guaranty_alignment,
  is_empty(map_from_str(extra_info)['车辆性质'])                                    as car_property,
  is_empty(map_from_str(extra_info)['融资方式'])                                    as financing_type,
  is_empty(map_from_str(extra_info)['担保方式'])                                    as guarantee_type,
  cast(is_empty(map_from_str(extra_info)['评估价格(元)'],0) as decimal(25,5))       as pawn_value,
  cast(is_empty(map_from_str(extra_info)['车辆销售价格(元)'],0) as decimal(25,5))   as car_sales_price,
  cast(is_empty(map_from_str(extra_info)['新车指导价(元)'],0) as decimal(25,5))     as car_new_price,
  cast(is_empty(map_from_str(extra_info)['投资总额(元)'],0) as decimal(25,5))       as total_investment,
  cast(is_empty(map_from_str(extra_info)['购置税金额(元)'],0) as decimal(25,5))     as purchase_tax_amouts,
  is_empty(map_from_str(extra_info)['保险种类'])                                    as insurance_type,
  cast(is_empty(map_from_str(extra_info)['汽车保险总费用'],0) as decimal(25,5))     as car_insurance_premium,
  cast(is_empty(map_from_str(extra_info)['手续总费用(元)'],0) as decimal(25,5))     as total_poundage,
  cast(is_empty(map_from_str(extra_info)['累计车辆过户次数'],0) as decimal(20,0))   as cumulative_car_transfer_number,
  cast(is_empty(map_from_str(extra_info)['一年内车辆过户次数'],0) as decimal(20,0)) as one_year_car_transfer_number,
  cast(is_empty(map_from_str(extra_info)['责信保费用1'],0) as decimal(25,5))        as liability_insurance_cost1,
  cast(is_empty(map_from_str(extra_info)['责信保费用2'],0) as decimal(25,5))        as liability_insurance_cost2,
  is_empty(map_from_str(extra_info)['车类型'])                                      as car_type,
  is_empty(map_from_str(extra_info)['GPS编号'])                                     as gps_code,
  cast(is_empty(map_from_str(extra_info)['GPS费用'],0) as decimal(25,5))            as gps_cost,
  is_empty(map_from_str(extra_info)['车系'])                                        as car_system,
  ptrim(is_empty(map_from_str(extra_info)['车型']))                                 as car_model,
  cast(is_empty(map_from_str(extra_info)['车龄'],0) as decimal(20,0))               as car_age,
  is_empty(map_from_str(extra_info)['车辆能源类型'])                                as car_energy_type,
  is_empty(map_from_str(extra_info)['生产日期'])                                    as production_date,
  cast(is_empty(map_from_str(extra_info)['里程数'],0) as decimal(25,5))             as mileage,
  is_empty(map_from_str(extra_info)['注册日期'])                                    as register_date,
  is_empty(map_from_str(extra_info)['车辆购买地'])                                  as buy_car_address,
  is_empty(map_from_str(extra_info)['车辆颜色'])                                    as car_colour,

  is_empty(t_04_car_brand,map_from_str(extra_info)['车辆品牌'])                     as car_brand,

  is_empty(ptrim(map_from_str(extra_info)['车架号']))                               as frame_num_code,
  is_empty(ptrim(map_from_str(extra_info)['发动机号']))                             as engine_num_code,
  is_empty(ptrim(map_from_str(extra_info)['车牌号码']))                             as license_num_code,

  sha256(ptrim(map_from_str(extra_info)['车架号']),'frameNumber',1)                 as frame_num,
  sha256(ptrim(map_from_str(extra_info)['发动机号']),'engineNumber',1)              as engine_num,
  sha256(ptrim(map_from_str(extra_info)['车牌号码']),'plateNumber',1)               as license_num,

  create_time                                                                       as create_time,
  update_time                                                                       as update_time,

  '123' as aa
from (
  select
    *
  from ods.t_guaranty_info
  where 1 > 0
    -- and is_empty(map_from_str(extra_info)['项目编号'],project_id) = 'CL202101260095'
    and is_empty(map_from_str(extra_info)['借据号'],asset_id) = '1103563315'
    and is_empty(map_from_str(extra_info)['抵押物编号'],guaranty_umber) = '1103563315'
    -- and (
    --   is_empty(map_from_str(extra_info)['借据号'],asset_id) = '1103563315' or
    --   is_empty(map_from_str(extra_info)['抵押物编号'],guaranty_umber) = '1103563315' or
    --   false
    -- )
) as t_guaranty_info
left join (
  select distinct
    project_id    as t_04_project_id,
    serial_number as t_04_serial_number,
    guaranty_code as t_04_guaranty_code,
    car_brand     as t_04_car_brand
  from stage.t_04_guarantycarinfo
) as stage_04
on  is_empty(map_from_str(extra_info)['项目编号'],project_id)                = t_04_project_id
and is_empty(map_from_str(extra_info)['借据号'],asset_id)                    = t_04_serial_number
and nvl(is_empty(map_from_str(extra_info)['抵押物编号'],guaranty_umber),'a') = is_empty(t_04_guaranty_code,'a')
limit 10
;



select
  is_empty(id)                             as id,
  is_empty(project_id)                     as project_id,
  is_empty(agency_id)                      as agency_id,
  is_empty(serial_number)                  as serial_number,
  is_empty(guaranty_code)                  as guaranty_code,
  is_empty(guaranty_handling_status)       as guaranty_handling_status,
  is_empty(guaranty_alignment)             as guaranty_alignment,
  is_empty(car_property)                   as car_property,
  is_empty(financing_type)                 as financing_type,
  is_empty(guarantee_type)                 as guarantee_type,
  is_empty(pawn_value)                     as pawn_value,
  is_empty(car_sales_price)                as car_sales_price,
  is_empty(car_new_price)                  as car_new_price,
  is_empty(total_investment)               as total_investment,
  is_empty(purchase_tax_amouts)            as purchase_tax_amouts,
  is_empty(insurance_type)                 as insurance_type,
  is_empty(car_insurance_premium)          as car_insurance_premium,
  is_empty(total_poundage)                 as total_poundage,
  is_empty(cumulative_car_transfer_number) as cumulative_car_transfer_number,
  is_empty(one_year_car_transfer_number)   as one_year_car_transfer_number,
  is_empty(liability_insurance_cost1)      as liability_insurance_cost1,
  is_empty(liability_insurance_cost2)      as liability_insurance_cost2,
  is_empty(car_type)                       as car_type,
  is_empty(frame_num)                      as frame_num,
  is_empty(engine_num)                     as engine_num,
  is_empty(gps_code)                       as gps_code,
  is_empty(gps_cost)                       as gps_cost,
  is_empty(license_num)                    as license_num,
  is_empty(car_brand)                      as car_brand,
  is_empty(car_system)                     as car_system,
  is_empty(car_model)                      as car_model,
  is_empty(car_age)                        as car_age,
  is_empty(car_energy_type)                as car_energy_type,
  is_empty(production_date)                as production_date,
  is_empty(mileage)                        as mileage,
  is_empty(register_date)                  as register_date,
  is_empty(buy_car_address)                as buy_car_address,
  is_empty(car_colour)                     as car_colour,
  is_empty(create_time)                    as create_time,
  is_empty(update_time)                    as update_time,
  is_empty(import_id)                      as import_id,
  is_empty(data_source)                    as data_source
from stage.t_04_guarantycarinfo
where 1 > 0
  -- and project_id = 'CL202101260095'
  and serial_number = '1103563315'
  and guaranty_code = '1103563315'
  -- and (
  --   serial_number = '1103563315' or
  --   guaranty_code = '1103563315' or
  --   false
  -- )
;

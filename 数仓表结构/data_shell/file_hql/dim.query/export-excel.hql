select distinct
  case t_project.project_id
    when 'DIDI201908161538' then 1
    when 'WS0005200001'     then 2
    when '001601'           then 3
    when 'CL202104010103'   then 4
    when 'CL202106070113'   then 5
    when 'CL202011090089'   then 6
    when 'CL202007020086'   then 7
    when 'CL202003230083'   then 8
    when 'CL202011090088'   then 9
    when 'CL202012160091'   then 10
    when 'CL202103160101'   then 11
    when 'CL202106070111'   then 12
    when 'CL202011090090'   then 13
    when 'CL202101220094'   then 14
    when 'CL202012280092'   then 15
    when 'CL202102010097'   then 16
    when 'CL202102240099'   then 17
    when 'CL202102240100'   then 18
    when 'CL202103260102'   then 19
    when 'CL202104160106'   then 20
    when 'CL202104300107'   then 21
    when 'CL202105270109'   then 22
    when 'CL202105310110'   then 23
    when 'CL202106110115'   then 24
    when 'cl00297'          then 25
    when 'cl00306'          then 26
    when 'cl00309'          then 27
    when 'CL201911130070'   then 28
    when 'CL202002240081'   then 29
    when 'CL202104020104'   then 30
    when 'CL202106090114'   then 31
    when 'CL202105060108'   then 32
    when 'cl00333'          then 33
    when 'PL202101200093'   then 34
    when 'cl00326'          then 35
    when 'CL201912100072'   then 36
    when 'PL201905080051'   then 37
    when 'CL201912260074'   then 38
    when 'CL201905310055'   then 39
    when 'CL202003200082'   then 40
    when 'CL201906040057'   then 41
    when 'CL201906040058'   then 42
    when 'CL201905220053'   then 43
    when 'CL201912170073'   then 44
    when 'PL201908210066'   then 45
    when 'PL201904110050'   then 46
    when 'CL201906050059'   then 47
    when 'CL201906040056'   then 48
    when 'cl00265'          then 49
    when 'cl00187'          then 50
    when 'cl00185'          then 51
    when 'cl00186'          then 52
    when 'cl00199'          then 53
    when 'cl00217'          then 54
    when 'cl00229'          then 55
    when 'cl00243'          then 56
    when 'cl00232'          then 57
    when 'cl00233'          then 58
    when 'CL202101260095'   then 59
    when 'CL202102050098'   then 60
    when 'CL202104080105'   then 61
    when '001503'           then 62
    when '001505'           then 63
    when '001504'           then 64
    when 'pl00282'          then 65
    when 'CL201905240054'   then 66
    else 999
  end as asc_id,
  project_full_name as full_name,
  project_name,
  t_project.project_id,
  case project_type
    when 1 then '存量'
    when 2 then '增量'
    else null
  end as type,
  t_related_assets.project_id as related_project_id,
  case data_source
    when 1 then '接口导入'
    when 2 then 'Excel导入'
    else null
  end as data_source,
  case
    when t_project.project_id in (
      '001601',
      'WS0005200001',
      'CL202104010103',
      'CL202011090089',
      'CL202007020086',
      'CL202003230083',
      'CL202011090088',
      'CL202012160091',
      'CL202103160101',
      'CL202011090090',
      'CL202101220094',
      'CL202012280092',
      'CL202102010097',
      'CL202102240099',
      'CL202102240100',
      'CL202103260102',
      'CL202104160106',
      'CL202104300107',
      'CL202105270109',
      'CL202105310110',
      'CL202106070111',
      'CL202106070113',
      'DIDI201908161538'
    ) then '核心'
    when t_project.project_id in (
      'CL201905240054',
      'CL201912100072',
      'PL201905080051',
      'CL201912260074',
      'CL201905310055',
      'CL202003200082',
      'CL201906040057',
      'CL201906040058',
      'CL201905220053',
      'CL201912170073',
      'PL201908210066',
      'PL201904110050',
      'CL201906050059',
      'CL201906040056'
    ) then '星云'
    when t_project.project_id in (
      '001503',
      '001505',
      '001504'
    ) then '校验平台(老老核心)'
    else '校验平台'
  end as project_from,
  project_begin_date as begin_date,
  project_end_date   as end_date,
  project_id_map['channel_id'] as channel_id,
  project_id_map['channel_name'] as channel_name
from (
  select
    *,
    case project_id
      when '001503'           then map('channel_id','0001'   ,'channel_name','盒子支付')
      when '001504'           then map('channel_id','0001'   ,'channel_name','盒子支付')
      when '001505'           then map('channel_id','0001'   ,'channel_name','盒子支付')
      when 'cl00232'          then map('channel_id','abs0002','channel_name','先锋太盟')
      when 'cl00233'          then map('channel_id','abs0002','channel_name','先锋太盟')
      when 'cl00185'          then map('channel_id','abs0001','channel_name','天津恒通')
      when 'cl00186'          then map('channel_id','abs0001','channel_name','天津恒通')
      when 'cl00187'          then map('channel_id','abs0001','channel_name','天津恒通')
      when 'cl00199'          then map('channel_id','abs0001','channel_name','天津恒通')
      when 'cl00217'          then map('channel_id','abs0001','channel_name','天津恒通')
      when 'cl00229'          then map('channel_id','abs0001','channel_name','天津恒通')
      when 'cl00243'          then map('channel_id','abs0001','channel_name','天津恒通')
      when 'cl00265'          then map('channel_id','abs0001','channel_name','天津恒通')
      when 'CL201906040056'   then map('channel_id','abs0001','channel_name','天津恒通')
      when 'CL201905220053'   then map('channel_id','0010'   ,'channel_name','上海易鑫')
      when 'CL201905310055'   then map('channel_id','0010'   ,'channel_name','上海易鑫')
      when 'CL201906040057'   then map('channel_id','0010'   ,'channel_name','上海易鑫')
      when 'CL201906040058'   then map('channel_id','0010'   ,'channel_name','上海易鑫')
      when 'CL201912170073'   then map('channel_id','0010'   ,'channel_name','上海易鑫')
      when 'CL201912260074'   then map('channel_id','0010'   ,'channel_name','上海易鑫')
      when 'CL202003200082'   then map('channel_id','0010'   ,'channel_name','上海易鑫')
      when 'PL201905080051'   then map('channel_id','0010'   ,'channel_name','上海易鑫')
      when '001601'           then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'cl00297'          then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'cl00306'          then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'cl00309'          then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'cl00326'          then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'cl00333'          then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL201911130070'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL201912100072'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202002240081'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202003230083'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202007020086'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202011090088'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202011090089'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202011090090'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202012160091'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202012280092'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202101220094'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202102010097'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202102240099'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202102240100'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202103160101'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202103260102'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202104010103'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202104020104'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202104160106'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202104300107'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202105060108'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202105270109'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202105310110'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202106070111'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202106070113'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202106090114'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202106110115'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'PL202101200093'   then map('channel_id','0003'   ,'channel_name','汇通信诚')
      when 'CL202101260095'   then map('channel_id','0007'   ,'channel_name','广汽租赁')
      when 'CL202102050098'   then map('channel_id','0007'   ,'channel_name','广汽租赁')
      when 'CL202104080105'   then map('channel_id','0007'   ,'channel_name','广汽租赁')
      when 'CL201905240054'   then map('channel_id','abs0004','channel_name','沣邦租赁')
      when 'CL201906050059'   then map('channel_id','abs0008','channel_name','有车有家')
      when 'pl00282'          then map('channel_id','10041'  ,'channel_name','浦发'    )
      when 'WS0005200001'     then map('channel_id','0005'   ,'channel_name','瓜子'    )
      when 'PL201904110050'   then map('channel_id','abs0006','channel_name','东亚银行')
      when 'PL201908210066'   then map('channel_id','abs0006','channel_name','东亚银行')
      when 'DIDI201908161538' then map('channel_id','10000'  ,'channel_name','滴滴中航')
    end as project_id_map
  from stage.abs_t_project
) as t_project
left join (select distinct project_id,related_project_id from stage.abs_t_related_assets) as t_related_assets
on t_project.project_id = t_related_assets.related_project_id
where t_project.project_id not in (
  'PL202102010096', -- 1-1-1-1年第1期
  'PL201907050063', -- WY-中航-消费分期-2019年第1期
  'PL201908220067', -- 东亚中国-银登-车位分期-2019年第1期-te
  ''
)
order by asc_id,t_project.project_id;

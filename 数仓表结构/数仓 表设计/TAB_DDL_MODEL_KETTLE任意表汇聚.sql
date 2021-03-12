
-- 汇聚表信息
-- drop table kt_dm_aggre_table;
create table kt_dm_aggre_table(
  AGGRE_TABLE         varchar(100) not null, -- 如果同时调度多种数据库类型且有多个数据库时配置成且不同数据库之间有同名表时配置成(DBTYPE.DBNAME.AGGRE_TABLE)或DBNAME.AGGRE_TABLE,前两段可以为空(..KETTLE_TEST_{CYCLE}),默认情况下配置成(KETTLE_TEST_{CYCLE}即可),即这里得保证唯一
  DBTYPE              varchar(50) not null, -- 数据库类型(全部小写)，决定定义kettle时选用适应的数据库连接，和决定默认SQL引擎，临时表创建和未配置SQL引擎时匹配引擎用,和临时表创建是用于判断 前缀：gbase greenplum hive sparksql impala infobright mysql oracle postgresql sybaseiq
  JOB_INTERVAL        varchar(255), -- 临时表记录信息维护,cron表达式，目前都改在InitAggConf里面调用所以该处配置失效
  USER_ID             numeric(18) , -- 用户ID
  USER_CODE           varchar(70) , -- 用户登入代码
  USER_NAME           varchar(40)  -- 用户昵称
);
alter table kt_dm_aggre_table add primary key (AGGRE_TABLE) ;

-- 汇聚表延迟及补汇设置信息,存放详细的粒度，一个汇聚表有几个时间汇聚粒度改表里面就有几条记录,初始化汇聚SQL表，以该表为准外关联其他表
-- drop table kt_dm_initsql;
create table kt_dm_initsql(
  AGGRE_TABLE         varchar(100) not null,
  CYCLE_KEY           varchar(100) not null, -- @20200129默认为CYCLE_P||'~'||SEVERAL_CYCLES||'~'||TIME_OFFSET，也可以是其他字符，只要保证AGGRE_TABLE下唯一即可
  CYCLE_P             varchar(38) not null, -- 汇聚粒度
  SEVERAL_CYCLES      numeric(9) default 1 not null,
  CUSTOM_CYCLE        numeric(18) default 0 not null,
  TIME_OFFSET         numeric(18) default 0 not null,
  CYCLE_NAME          varchar(100), -- 为空时使用CYCLE_KEY(@20200129),用来给性能数据表替换粒度名
  DIM_FIELD           varchar(800), -- @20200129其他维度.多个字段使用";"号分隔
  DIM_NO              varchar(1000), -- 多个字段使用";"号分隔
  DIM_DATATYPE        varchar(300), -- @20200129维度数据类型 0数字 1字符串 多个字段使用";"号分隔
  TIME_FIELD          varchar(800) default 'TIME_ID',  -- 以下两个字段过程自动维护非BIZMAN表的时间字段字段名;多个字段使用";"号分隔--(TOTABLE_TIMEFIELD)
  TIME_TYPE           varchar(300),   -- 时间字段的字段类型;和字段一一对应多个使用";"号分隔--(TOTABLE TIMETYPE)
  JOIN_TYPE           numeric(1) default 1 not null, -- 宽表OLAP时多个数据源的关联方式 0内关联 1以主数据表为准的外关联 2全外关联,性能也依次下降
  TABLE_NAME          varchar(100), -- 留空即可、代码会自动维护AGGRE_TABLE替换{CYCLE}后原样存放到这里,OLAP会根据这个字段做任务依赖处理，转入到KT_DM_CAL_SCHEDULER.TABLE_NAME后再做处理
  DYNAMIC_LAG         varchar(800), -- 不为空时动态延迟使用该配置(如果不是特殊情况(例如月粒度需要每天一更新[配合RE_AGGRE使用])默认留空即可)
  RE_AGGRE            numeric(9) default 0 not null, -- 对应的各个汇聚表的补汇粒度 (半小时;小时;天;周;月;季;年)，为空时为0,非自定义函数计算时配置该字段一般是为了实现粗粒度数据实时更新(需要配合JOB_INTERVAL/REPEAT_INTERVAL一起使用)
  ISINCREMENT         numeric(2) not null, -- 代表是否增量型数据9:CUBE配置时hive/spark-sql使用insert overwrite table tabName partition(a='xxx');
  CUSTOM_PARKEY       varchar(800), -- 自定义参数KEY,多个参数使用";"号分隔开,{SYS_FILTER}为个性化的过滤条件
  CUSTOM_PARSQL       varchar(2000), -- 自定义参数SQL,多个参数SQL使用";"号分隔开,这里和CUSTOM_PARKEY按顺序对应,汇聚时{SYS_FILTER_SQL}会被替换成当前的DATA_SOURCE_ID,如果是自定义汇聚函数就会被替换成CS_ID
  JOB_INTERVAL        varchar(255), -- JOB运行周期,也就是扫描周期，为空时不起任务,计算任务的扫描周期
  CONTROLER_PRO_GROUP varchar(1000), -- 任务组前置子过程多个用";"分隔开,这里配置会初始化到KT_DM_TASK_GROUP.CONTROLER_PRO
  BEFORE_SUBPRO_GROUP varchar(2000), -- 任务组前置子过程多个用";"分隔开,这里配置会初始化到KT_DM_TASK_GROUP.BEFORE_SUBPRO
  AFTER_SUBPRO_GROUP  varchar(2000), -- 任务组后置子过程多个用";"分隔开,这里配置会初始化到KT_DM_TASK_GROUP.AFTER_SUBPRO
  BEFORE_SUBPRO       varchar(2000), -- 任务前置子过程多个用";"分隔开,这里配置会初始化到KT_DM_CAL_SCHEDULER.BEFORE_SUBPRO
  AFTER_SUBPRO        varchar(2000), -- 任务后置子过程多个用";"分隔开,这里配置会初始化到KT_DM_CAL_SCHEDULER.AFTER_SUBPRO
  CUSTOM_SQLENGINE    varchar(300), -- 拼接SQL自定义函数 如果为空则通过DBTYPE来判断
  SETENV              varchar(3000), -- 类似hive的SQL执行前的参数设置，有利于优化任务，直接拼接在sql最前端多个使用";"号隔开
  MEMO                varchar(1000),
  USER_ID             numeric(18) , -- 用户ID
  USER_CODE           varchar(70) , -- 用户登入代码
  USER_NAME           varchar(40)  -- 用户昵称
);
alter table kt_dm_initsql add primary key (AGGRE_TABLE,CYCLE_KEY) ;
create index kt_dm_initsql_tname on kt_dm_initsql(TABLE_NAME) ; -- 任务依赖关系查找时需要查该字段，不建议多种粒度使用相同的表名或者多个数据库使用相同的表名，因为这样自动拼接出来的任务依赖语句时会把一样的表名的任务都找出来取最小的LASTTIME，所以这样只能手动写任务依赖语句(动态延迟SQL)了。

-- 汇聚组里面的任务组排序信息，该表由过程自动维护，若想手工维护需要关闭INIT_AGGTAB_REGROUP_CONF开关,(目前暂时放弃自动维护该表功能，下个版本设计该功能)
-- drop table kt_dm_aggtab_regroup;
create table kt_dm_aggtab_regroup(
  AGGRE_TABLE         varchar(100) not null,
  CYCLE_KEY           varchar(100) not null, -- @20200129
  RE_GROUP            numeric(18) not null, -- 所属的汇聚组ID 可以属于多个汇聚组，最好是RE_GROUP，AGGRE_TABLE一对多
  TASK_ORDER          numeric(18) not null, -- 补汇时任务组先后顺序
  USER_ID             numeric(18) , -- 用户ID
  USER_CODE           varchar(70) , -- 用户登入代码
  USER_NAME           varchar(40)  -- 用户昵称
);
alter table kt_dm_aggtab_regroup add primary key (RE_GROUP,AGGRE_TABLE,CYCLE_KEY) ;
create unique index kt_dm_reagg_order on kt_dm_aggtab_regroup(RE_GROUP,TASK_ORDER) ;

-- 宽表到宽表汇聚配置(这张表相当于FROM表的字段名和字段类型)
-- PM性能类指标(度量)，CM资源类指标(维度)
-- drop table kt_dm_kpi_info;
create table kt_dm_kpi_info(
  K_ID              numeric(18) not null,
  KPI_NO            varchar(100) not null, -- 有业务含义的指标名
  KPI_EN_NAME       varchar(100),  -- 指标英文名
  KPI_CN_NAME       varchar(100),  -- 指标中文名
  KPI_EN_DESCR      varchar(800), -- 指标英文描述
  KPI_CN_DESCR      varchar(800), -- 指标中文描述
  KPI_FORMULA       varchar(4000), -- 内聚公式(如果是CM或者TM指标时直接写上字段名)，空间维度的聚合，这里直接写上聚合函数，比如MAX(A)+MAX(B)或者MAX(A+B)或者SUM(CASE WHEN A>1 AND A<8 THEN B END)，可以使用${AGGRE_FIELD}来复用计算公式
  KPI_FORMULA_LOCAL varchar(4000), -- 本地计算公式如果该字段不为空则以该字段为准,添加该字段意图为KPI_FORMULA算法可能需要对外进行页面展示比如计算某个指标的平均值时avg(X),实际算法应该是sum(a)/count(1)或sum(a)/sum(b),这时上面的字段配置成avg(X),本字段配置成sum(a)/count(1)或sum(a)/sum(b)
  KPI_TYPE          varchar(10) not null, -- 指标类型PM或者CM/KM(用于解决汇聚类似BIZMAN窄表时配置的KPI字段)或者TM,如果不需要聚合就配置成CM2,TM2,KM2(比如说两张表进行字段关联成大宽表场景不需要group by的)
  KPI_INTERVAL      numeric(18) not null, -- 数据采样周期,单位秒
  INDEX_SYS         varchar(200) not null, -- 指标体系,一个有含义的字符串，主要用于管理指标算法配置
  KPI_UNIT          varchar(20), -- 指标单位
  CAL_COLUMN        varchar(1000), -- 指标涉及到的计算列多个用";"号隔开，用于在对指标过滤统计时段时用到，需要第三方接口来初始化FORMULA_DIM,FORMULA_TIM配置
  CAL_FORMULA       varchar(2000), -- 指标未做时段过滤时计算公式,配合上面两个字段使用
  CAL_COLUMN_LOCAL  varchar(1000), -- 指标涉及到的计算列多个用";"号隔开，用于在对指标过滤统计时段时用到，需要第三方接口来初始化FORMULA_DIM,FORMULA_TIM配置
  CAL_FORMULA_LOCAL varchar(2000), -- 指标未做时段过滤时计算公式,配合上面两个字段使用
  KPI_COMMENT       varchar(1000), -- 指标特殊说明
  USER_ID           numeric(18) , -- 用户ID
  USER_CODE         varchar(70) , -- 用户登入代码
  USER_NAME         varchar(40)  -- 用户昵称
);
alter table kt_dm_kpi_info add primary key (K_ID) ; 
create unique index kt_dm_kpi_no on kt_dm_kpi_info(KPI_NO) ;
create index kt_dm_kpi_index_sys on kt_dm_kpi_info(INDEX_SYS) ;

-- 数据来源表信息
-- drop table kt_dm_ds_table;
create table kt_dm_ds_table(
  DATA_SOURCE_ID      varchar(50) not null, -- 数据来源表所属的组,一个汇聚有可能同时来时几张表的数据
  DATA_SOURCE_TABLE   varchar(100) not null,  -- 如果是横向聚合时{CYCLE}会被替换成各种粒度名称,指标对应的原始表表名,保证替换后能关联上KT_DM_INITSQL.TABLE_NAME
  USER_ID             numeric(18) , -- 用户ID
  USER_CODE           varchar(70) , -- 用户登入代码
  USER_NAME           varchar(40)  -- 用户昵称
);
alter table kt_dm_ds_table add primary key (DATA_SOURCE_ID,DATA_SOURCE_TABLE) ;

-- 数据来源组信息
-- drop table kt_dm_ds_group;
create table kt_dm_ds_group(
  DATA_SOURCE_ID      varchar(50) not null, -- 数据来源表所属的组,一个汇聚有可能同时来时几张表的数据
  DATA_SOURCE_GROUP   varchar(4000) not null, -- 一个有含义的字符串，可以是中文
  DATA_SOURCE         varchar(4000) not null, -- 如果是横向聚合时{CYCLE}会被替换成各种粒度名称,如果需要关联多表的话这里写成(SELECT * FROM A,B WHERE A.A=B.A AND A.TIME_ID >= ${HH1} AND A.TIME < ${HH2}),如果单张表的话那么和DATA_SOURCE_TABLE一样
  ADD_TIME_FILTER     numeric(1) default 1 not null, -- 是否对拼接后的SQL进行时间字段过滤,1添加0不添加,对于DATA_SOURCE已经有配置的时间过滤已经放到子查询里面了.所以外面没必要在加时间过滤了
  USER_ID             numeric(18) , -- 用户ID
  USER_CODE           varchar(70) , -- 用户登入代码
  USER_NAME           varchar(40)  -- 用户昵称
);
alter table kt_dm_ds_group add primary key (DATA_SOURCE_ID) ;

-- (这张表相当于TO表的表名和字段名),由于性能问题不支持distinct、对于从事实表里面抽取维度表使用group by 维度表全字段解决:https://www.cnblogs.com/wswang/p/7718085.html
-- drop table kt_dm_dstable_aggtable;
create table kt_dm_dstable_aggtable(
  K_ID                  numeric(18) not null, -- 关联KT_DM_KPI_INFO.K_ID
  ISJOIN                numeric(1), -- 当指标类型为CM或者TM时横向聚合和纵向聚合是否是关联字段1是0或者空为否
  AGGRE_TABLE           varchar(100) not null, -- 使用{CYCLE}标示时间粒度
  CYCLE_KEY             varchar(100) not null, -- @20200129第一次横向聚合时配置成数字(自定义)，也就是元数据表的数据粒度
  AGGRE_FIELD           varchar(100), -- 汇聚表对应的字段名,关联字段的话是通过字段名称相同的来进行关联的，如果该字段是用来进行时间过滤的话，且汇聚上去后不再存在该字段那么配置为空
  REPROCESS             varchar(4000), -- 一般用于对数据源外关联时从表指标为空时处理方式，不过也可以用于其他用途只要是正确的SQL表达式就支持，该字段不为空时拼接到永久表SQL时使用AGGRE_FIELD
  TIME_TYPE             numeric(18), -- 当是TM指标时这个字段不能为空，代表该内聚的时间维度的时间格式(FROMTABLE TIMETYPE)
  TIME_TYPE2            numeric(18), -- 汇聚表的时间维度的时间格式(TOTABLE TIMETYPE)
  TIME_FILTER2          numeric(1) default 1, -- 该时间字段在汇聚到目标表时是否是目标表数据清理时间字段1：是  0：否，根据表情况有选择性的选择时间过滤字段可以提高性能
  TM_TYPE               varchar(20), -- TMMCHAR/TMMNUM(用来过滤的时间字段(数据源表过滤)，字符串型或者数字型),TMSCHAR/TMSNUM(非过滤时间字段一般是比任务周期粗的时间字段),TMSORT(相当于CM配置，一般为比任务周期细的时间字段)
  ET_EQUAL_SIGN         numeric(1) default 0, -- 当TM_TYPE为TMMCHAR/TMMNUM且过滤字段是粗粒度字段(例如2天粒度时出现跨月时月过滤结束时间为MONTH_TIME<=XXXX)或TIME_OFFSET不为0时需要配置该字段 1添加"="号 0不添加
  DATA_TYPE             varchar(300) not null, -- 汇聚表该字段的字段类型 比如varchar(200)、numeric(30,5)、DATE、CLOB；该字段用于新增指标时动态创建字段
  DATA_SOURCE_ID        varchar(50) not null, -- 数据来源表所属的组
  KPI_LEVEL             numeric(18) not null,  -- 指标级别，数字越小级别越高，最高级别的指标能决定主数据表
  ISPARTITION           numeric(1) default 0, -- 1是(hive里面静态分区分区字段不拼接到select) 2(hive里面动态分区字段需要拼接到select) 3(一般数据库分区字段，暂时没用途)  0(NULL)否 由于hive语句对于分区表需要拼接insert overwrite table partition_test partition(stat_date='20110728',province='henan')所以得知道分区字段
  DISTRIBUTE_BY         numeric(1) default 0, -- 是否分发，hive里面才有分发概念DISTRIBUTE BY(”CLUSTER BY”等于”DISTRIBUTE BY”+”SORT BY”。) 1是 0(NULL)否
  SORT_BY               numeric(1) default 0, -- 是否排序，没有DISTRIBUTE_BY只有SORT_BY时sql就会拼接成order by。请尽量不要在hive里面做全排序即order by 1是 0(NULL)否
  SORT_FIELD_ORDER      numeric(18), -- DISTRIBUTE_BY、SORT_BY的先后，为空时使用KPIORDER
  SEQUENCE_TYPE         numeric(1) default 1, -- 正序或者倒序 1(NULL)正序 0倒序
  KPIORDER              numeric(18),  -- 汇聚表指标排序，就是AGG表列排序，同时也是用来决定数据源汇聚的先后、DISTRIBUTE_BY、SORT_BY的先后
  IFAGGRE               numeric(1) default 1 not null, -- 是否进行汇聚1：进行汇聚 0：不进行汇聚 2：对于进行二次计算时只需汇聚到临时表用于参加二次计算的指标不需要汇聚到永久表
  KPI_PRECISION         numeric(3) default 0, -- 指标精确度，保留几位小数
  IFDROPAGG_FIELD       numeric(1) default 0 not null, -- 是否需要删除汇聚表该字段1：删除 0：不删除，删除完后该条记录也跟着删除，也就是说一般情况下这个字段都是0
  USER_ID               numeric(18) , -- 用户ID
  USER_CODE             varchar(70) , -- 用户登入代码
  USER_NAME             varchar(40)  -- 用户昵称
);
alter table kt_dm_dstable_aggtable add primary key (AGGRE_TABLE,CYCLE_KEY,K_ID,DATA_SOURCE_ID); 
create index kt_dm_dsagg_kid on kt_dm_dstable_aggtable(K_ID);





-- 动态延迟SQL参数字典表
-- drop table kt_dm_dynamic_dict;
create table kt_dm_dynamic_dict(
  PARKEY varchar(100) not null,  -- 系统延迟SQL自带参数 
  PARNAME varchar(500),  -- 参数名 
  PARDESCR varchar(2000)  -- 参数描述 
) ; 
alter table kt_dm_dynamic_dict add primary key (PARKEY) ;
-- DML
insert into kt_dm_dynamic_dict(PARKEY,PARNAME,PARDESCR)
values('${SYS_DATE_FIELD}','时间字段参数','数据表的时间戳字段参数,在生成任务时替换');
insert into kt_dm_dynamic_dict(PARKEY,PARNAME,PARDESCR)
values('${SYS_TABLE_NAME}','数据表表名参数','数据表表名参数,在生成任务时替换');
insert into kt_dm_dynamic_dict(PARKEY,PARNAME,PARDESCR)
values('${SYS_LAST_TASK_GROUP_ID}','上个任务组ID','上个任务组ID,在生成任务时替换');
insert into kt_dm_dynamic_dict(PARKEY,PARNAME,PARDESCR)
values('${SYS_MONTH_DAYS}','本月的天数参数','本月的天数参数,在运行时替换,动态参数使用当前系统时间计算任务执行使用任务时间计算');
insert into kt_dm_dynamic_dict(PARKEY,PARNAME,PARDESCR)
values('${SYS_SEASON_DAYS}','本季度的天数参数','本季度的天数参数,在运行时替换,动态参数使用当前系统时间计算任务执行使用任务时间计算');
insert into kt_dm_dynamic_dict(PARKEY,PARNAME,PARDESCR)
values('${SYS_YEAR_DAYS}','本年的天数参数','本年的天数参数,在运行时替换,动态参数使用当前系统时间计算任务执行使用任务时间计算');
insert into kt_dm_dynamic_dict(PARKEY,PARNAME,PARDESCR)
values('${SYS_LASTTIME}','本任务的上次结束时间','本任务的上次结束时间,在运行时替换');
commit;


-- 宽表多个数据源时汇聚的先后顺序初始化汇聚SQL配置时初始化该表(默认该表自动维护)
-- drop table kt_dm_dsaggorder;
create table kt_dm_dsaggorder(
  AGGRE_TABLE        varchar(100) not null,
  CYCLE_KEY          varchar(100) not null, -- @20200129
  DATA_SOURCE_ID     varchar(50) not null,
  DATA_SOURCE_LEVEL  numeric(18) not null, -- MIN(KPI_LEVEL)数字越小级别越高，默认最高级别的为主数据表
  DATA_SOURCE_ORDER  numeric(18) not null -- MIN(KPIORDER)用来决定数据源汇聚的先后
);
alter table kt_dm_dsaggorder add primary key (AGGRE_TABLE,CYCLE_KEY,DATA_SOURCE_ID); 

-- 宽表OLAP临时表表名对应表,该表由过程自动维护
-- drop table kt_dm_temptables;
create table kt_dm_temptables(
TEMPTAB_ID varchar(255) not null, -- AGGRE_TABLE||'_'||CYCLE_KEY||'_'||DATA_SOURCE_ID来确定临时表ID
TEMP_TABNAME varchar(50) not null -- 由序列得到的临时表表名
);
alter table kt_dm_temptables add primary key (TEMPTAB_ID); 
create unique index kt_dm_tmptabname on kt_dm_temptables(TEMP_TABNAME);

-- 当前有用的临时表,用于清理废弃的临时表,该表由过程自动维护
-- drop table kt_dm_temptab;
create table kt_dm_temptab(
TEMPTAB_ID varchar(255) not null
) ;
alter table kt_dm_temptab add primary key (TEMPTAB_ID); 

-- 删除永久表字段前备份记录表(目前还没用到该表)
-- drop table kt_dm_bktabs;
create table kt_dm_bktabs(
BKTAB_NAME varchar(100) not null, -- 备份表新表名
TAB_NAME varchar(100) not null, -- 原备份表表名
TIME_ID numeric(18) not null, -- 备份时间，格式YYYYMMDDHH24MISS
CAUSE_DESCR varchar(4000), -- 删除原因
DROP_COLUMNS varchar(4000) -- 本次删除字段列表多个用 ";" 号隔开
);
create index kt_dm_bktabs_t on kt_dm_bktabs(TIME_ID);


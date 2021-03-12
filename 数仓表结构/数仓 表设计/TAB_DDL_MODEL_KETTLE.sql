-- create database dm DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
-- 由于脚本需要考虑支持多数据库，所以都走默认表空间，因为配置表数据量小且需要有事务支持mysql最好使用InnoDB引擎
-- 计算任务组配置表
-- drop table kt_dm_task_group ;
create table kt_dm_task_group(
  TASK_GROUP_ID       numeric(18) not null, -- ID字段关联KT_DM_CAL_SCHEDULER表的TASK_GROUP_ID字段
  TASK_GROUP_NAME     varchar(255) not null, -- 任务组名称
  TASK_GROUP_DESCR    varchar(800), -- 任务组描述
  DBTYPE              varchar(50) not null, -- 数据库类型(全部小写)，决定定义kettle时选用适应的数据库连接,如果是通过指标计算生成的那么是从KT_DM_AGGRE_TABLE.DBTYPE来
  TASK_JOBID          varchar(500) not null,    -- 任务对应的JOBID,就是文件名不包括后缀名,当配置成classpath:com.kevin.taskmanagement.task.custom.impl.GenericDBTask，就不走kettle了,当配置成shell:/opt/shell/aaa.sh aaa bbb ccc ${ST2} ${ET2} 走本地shell
  TASK_LEVEL          numeric(10) default 1000,  -- 任务组级别，以后前台添加计算任务时需要选择已有任务组时过滤掉级别高的任务组，从1开始数值越小级别越高；默认1000
  IS_RUN              numeric(1) default 1 not null, -- 是否跑 1跑 0不跑

  LASTTIME            numeric(18) not null,   -- 时间标记 yyyyMMddHHmmssSSS

  JOB_INTERVAL        varchar(255), -- 就是cron表达式,任务扫描周期,为空时就不起任务
  CYCLE_P             varchar(20) not null,  -- 运行粒度
  SEVERAL_CYCLES      numeric(9) default 1 not null,  -- 运行粒度倍数,比如说两个小时的运行一次的任务:CYCLE_P=HOU,SEVERAL_CYCLES=2
  CUSTOM_CYCLE        numeric(18) default 0 not null,   -- 自定义粒度，单位毫秒当CYCLE_P为CUSTOM时才有效
  TIME_OFFSET         numeric(18) default 0 not null,  -- 任务时间窗口偏移量,单位毫秒
  DELAY               numeric(18),  -- 数据延迟设置，单位毫秒，为空时代表延迟0


  DYNAMIC_LAG         varchar(4000),  -- 使用SQL语句设置的动态时延，即运行的结束时间13位毫秒 别名必须取为：SCH_ENDTIME，配置成classpath:com.*.*时需要实现com.kevin.taskmanagement.task.custom.itf.DynamicLag,该字段不为空时延迟以该字段为准，为空时以DELAY为准
  -- RUN_TYPE 为 1 时， select min(g.LASTTIME) as DATA_MAXTIME from kt_dm_task_group g where g.TASK_GROUP_NAME in ('ods_new_s.loan_apply.hql')
  -- RUN_TYPE 为 0 时， select min(g.LASTTIME) as DATA_MAXTIME from kt_dm_task_group g where g.TASK_GROUP_NAME in ('ods_new_s.loan_apply.hql') and LAST_RUN_TIME >= ${SYS_TIME5}000000000


  DP_GROUP_NAME_LIST  varchar(2000),  -- 依赖的任务TASK_GROUP_NAME列表","分隔
  DP_RAW_TABLE_LIST   varchar(2000),  -- 依赖的原始数据源表列表","分隔，可以同时依赖TASK_GROUP_NAME和原始数据源表
  RE_CALCULATION      numeric(9) default 0 not null, -- 重新计算，重新计算几个周期，假如是月粒度，这里填写3的话代表每次都补算前三个月，为空时不补算(0)



  RUN_TYPE            numeric(1) default 0 not null, -- 当RE_CALCULATION>0时每次扫描是否都要启动任务，1是  0否(启动任务的粒度按照CYCLE_P来，例子:CYCLE_P=DAY时每天只启动一次),下游(天任务时，其他任务更具情况选择合适的系统时间变量${SYS_TIME5})依赖这种每次自动重跑的任务时依赖语句为:select min(g.LASTTIME) as DATA_MAXTIME from kt_dm_task_group g where g.TASK_GROUP_NAME in ('ods_new_s.loan_apply.hql') and LAST_RUN_TIME >= ${SYS_TIME5}000000000




  CONTROLER_PRO       varchar(2000), -- 控制过程，调用任务时的最前面执行,主要用于一些初始化操作；例：每天几点到几点需要补汇的操作(支持多个插件过程使用";"号隔开),配置类路径(classpath:)继承com.kevin.taskmanagement.task.custom.itf.CtrlPro,默认调用kettle里面的job，例:classpath:com.a.a.A;classpath:com.b.b.B;1475849580000106(kjb的文件名，不包含后缀)
  BEFORE_SUBPRO       varchar(2000),   -- 在计算之前执行的子过程(支持多个插件过程使用";"号隔开),配置类路径(classpath:)继承com.kevin.taskmanagement.task.custom.itf.SubPro,默认调用kettle里面的job
  AFTER_SUBPRO        varchar(2000),   -- 在计算之后执行的子过程(支持多个插件过程使用";"号隔开),配置类路径(classpath:)继承com.kevin.taskmanagement.task.custom.itf.SubPro
  BUILTIN_PARA        varchar(200) default 'com.kevin.taskmanagement.task.custom.impl.BuiltinParaImpl' not null, -- 时间参数转换，java类路径
  EXC_STOP_SIGN       numeric(1) default 1 not null,  -- 出现异常是否停止更新lasttime 1 代表 出现异常后停止更新 0 代表出现异常后继续走,按组调度时起作用
  RESCH_ISPARALLEL    numeric(2) default 1 not null, -- 补汇任务和正常汇聚任务是否并行（不并行可以减少资源抢占问题），1是0否(选择放到这张表里面是因为一个组的所有任务要么都跑要么都不跑)
  NORMAL_ISPARALLEL   numeric(2) default 1 not null,
  RESCH_MAX_WAIT      numeric(18) default 3600000 not null, -- 正常任务跑时补汇任务正在跑该计算时且配置了不并行这时需要等待的时长，单位毫秒
  NORMAL_MAX_WAIT     numeric(18) default 3600000 not null, -- 补汇时出现时间交叉且正常任务正在跑需要等待的时长 ，单位毫秒
  LAST_LAUNCH_TIME    numeric(18) ,  -- 上次正常调度启动时间 yyyyMMddHHmmssSSS;任务的运行状态在redis里面,冗余一份到表字段里
  LAST_RELAUNCH_TIME  numeric(18) ,  -- 上次补汇调度启动时间 yyyyMMddHHmmssSSS;任务的运行状态在redis里面,冗余一份到表字段里
  LAST_MLAUNCH_TIME   numeric(18) ,  -- 上次手工调用启动时间 yyyyMMddHHmmssSSS;任务的运行状态在redis里面,冗余一份到表字段里
  LAUNCH_STATUS       numeric(1),  -- 1正在运行;0(null)没有运行
  RELAUNCH_STATUS     numeric(1),
  MLAUNCH_STATUS      numeric(1),


  LAST_RUN_TIME       numeric(18),   -- 最近一次正常汇聚运行结束时系统时间yyyyMMddHHmmssSSS   以下两个字段一般配合RUN_TYPE来使用
  LAST_RERUN_TIME     numeric(18),   -- 最近一次补汇运行结束时系统时间yyyyMMddHHmmssSSS


  CRETIME             numeric(18),   -- 任务创建时间 yyyyMMddHHmmssSSS
  MODTIME             numeric(18),   -- 任务上次修改时间 yyyyMMddHHmmssSSS
  MEMO                varchar(800),  -- 备用字段
  MACHINE_ID          varchar(60),   -- 任务组运行机器ID，一般为机器IP+"_"+进程ID,用于分布式部署，多个任务组分散在多个机器上运行
  USER_ID             numeric(18) ,  -- 用户ID
  USER_CODE           varchar(70) ,  -- 用户登入代码
  USER_NAME           varchar(40)    -- 用户昵称
);
alter table kt_dm_task_group add primary key (TASK_GROUP_ID) ;
create unique index kt_dm_ix_group_name on kt_dm_task_group (TASK_GROUP_NAME) ;  -- mysql超过1000字节就不能建立唯一索引约束
create index kt_dm_ix_group_machine_id on kt_dm_task_group (MACHINE_ID) ;
create index kt_dm_ix_group_task_level on kt_dm_task_group (TASK_LEVEL) ;
create index kt_dm_ix_task_cycle on kt_dm_task_group (CYCLE_P) ;

-- 计算任务配置表(HiveTask/GenericDBTask时使用到该表)
-- drop table kt_dm_cal_scheduler;
create table kt_dm_cal_scheduler
(
  CS_ID              numeric(18) not null,      -- ID，不能为0
  TASK_GROUP_ID      numeric(18) not null,  -- 任务组ID
  CS_TASK_NAME       varchar(255) not null,    -- 计算任务名
  CS_TASK_DESCR      varchar(800),  -- 计算任务描述
  ISINCREMENT        numeric(2) not null,      -- 代表是否增量型数据，1:增量型数据;2:更新型数据DELETE+INSERT+COMMIT方式更新数据;3:TRUNCATE+INSERT+COMMIT方式更新数据;4 = 1 + commit;5: UPDATE 或 DELETE SQL,6: 清除子分区; 7: 清除父分区 8: 应用级临时表; 9:CUBE配置时hive/spark-sql使用insert overwrite table tabName partition(a='xxx');10:未定义
  TABLE_NAME         varchar(1000),  -- 表名非BIZMAN结构的表这里不能为空,有些配置并不需要汇聚或计算了，比如说生成文件等，这时这里就相当于数据源了(SELECT A,B FROM TABNAME WHERE T >= ${0ST} AND T < ${0E}),也可以通过配置DIM_NO、NOT_BIZ_DIM_FIELD、NOT_BIZ_TIME_FIELD、NOT_BIZ_TIME_TYPE 这几个字段来实现该功能
  DIM_FIELD          varchar(2000),  -- 非BIZMAN表的KPI字段字段名，多个字段使用";"号分隔开，和DIM_NO一一对应，可以使用"\"转义字符
  DIM_NO             varchar(4000),  -- KPI_NO，相当于KEY,多个KEY确定一条记录时使用";"号分隔开,原来字符KEY中有";"号的,可以使用"\"转义字符
  DIM_DATATYPE       varchar(1000), -- 指标字段的数据类型0为numeric 1为字符串，多个KEY的类型使用";"号分隔开和上面的顺序一一对应,可以使用"\"转义字符
  TIME_FIELD         varchar(1000),  -- 非BIZMAN表的时间字段字段名;多个字段使用";"号分隔
  TIME_TYPE          varchar(500),   -- 非BIZMAN表的时间字段的字段类型;和字段一一对应多个使用";"号分隔
  IS_RUN             numeric(1) default 1 not null,    -- 该计算是否跑，1时跑
  DATA_SQL           varchar(4000),  -- 脚本文件路径sql:file://home/a.sql，或者直接配置需要执行的shell脚本：shell:file://home/a.sh，或者是hive / spark-sql时的:sqlShell:file://home/a.sql
  DATA_SQL_ID        varchar(50), -- 脚本的MD5码+脚本字节长度 用于判断脚本是否发生改变
  BEFORE_SUBPRO      varchar(2000),   -- 在计算之前执行的子过程(支持多个插件过程使用";"号隔开)
  AFTER_SUBPRO       varchar(2000),   -- 在计算之后执行的子过程(支持多个插件过程使用";"号隔开)
  CALCULATEDORDER    numeric(18) not null,   -- 计算顺序，如果某几个步骤需要并行那么那几个步骤该值需要配置成一样
  MEMO               varchar(800),    -- 备用字段
  USER_ID            numeric(18) , -- 用户ID
  USER_CODE          varchar(70) , -- 用户登入代码
  USER_NAME          varchar(40)  -- 用户昵称
) ;
-- CREATE/RECREATE PRIMARY, UNIQUE AND FOREIGN KEY CONSTRAINTS
alter table kt_dm_cal_scheduler add primary key (CS_ID) ;
create unique index kt_dm_ix_name_calsl on kt_dm_cal_scheduler (CS_TASK_NAME) ;

create index kt_dm_ix_tid_calsl on kt_dm_cal_scheduler (TASK_GROUP_ID) ;
create index kt_dm_ix_kno_calsl on kt_dm_cal_scheduler (DIM_NO(255)) ;
create index kt_dm_ix_isit_calsl on kt_dm_cal_scheduler (ISINCREMENT) ;
create index kt_dm_ix_tname_calsl on kt_dm_cal_scheduler (TABLE_NAME(255)) ;
create index kt_dm_ix_isrun_calsl on kt_dm_cal_scheduler (IS_RUN) ;

-- 自定义参数配置表
-- drop table kt_dm_parameter ;
create table kt_dm_parameter(
   P_ID           numeric(18) not null,              -- 主键
   P_NAME         varchar(255) not null,        -- 参数名
   P_DESCR        varchar(1000),       -- 参数描述
   PARAKEY        varchar(500),        -- 需要替换的关键字 key带${}号
   PARASQL        varchar(4000),       -- 通过SQL得到具体字符串(或者java类路径classpath:com.*.*，需实现com.kevin.taskmanagement.task.custom.itf.CustomParameter)，这个SQL里面也有时间参数开始结束时间取主SQL的开始结束时间,系统会把该SQL里面{SYS_PID}、{SYS_CSID}、{SYS_PARATYPE}这三个变量为当前记录的这三个字段值
   TASK_GROUP_ID  numeric(18) not null,
   PARAORDER      numeric(18) not null,  -- 参数排序字段
   MEMO           varchar(800),          -- 备用字段
   USER_ID        numeric(18) , -- 用户ID
   USER_CODE      varchar(70) , -- 用户登入代码
   USER_NAME      varchar(40)  -- 用户昵称
);
alter table kt_dm_parameter add primary key (P_ID) ;
create unique index kt_dm_ix_name_para on kt_dm_parameter (P_NAME) ;
create index kt_dm_ix_parameter_group_id on kt_dm_parameter (TASK_GROUP_ID) ;

-- 数据源表,不考虑汇聚组补汇可以不用配置该表
-- drop table kt_dm_recs_info;
create table kt_dm_recs_info(
TABLE_NAME           varchar(100) not null,  -- 采集表表名,当有多个数据库时为了能智能生成动态延迟这里可以配置成DBTYPE.DBNAME.TABLE_NAME或DBNAME.TABLE_NAME或TABLE_NAME总之使前后能关联上即可
TIME_FIELD           varchar(4000) not null,  -- 采集表表时间字段名;多个字段使用";"号分隔，多个字段时以第一个字段为触发器的收集时间
TIME_TYPE            varchar(4000) not null,  -- 采集表表时间字段值格式类型;和字段一一对应多个使用";"号分隔
DIM_FIELD            varchar(100), -- KPI字段字段名 OLAP窄表汇聚时用到, 删除废弃的数据来源配置时用到
DIM_TYPE             numeric(2),     -- 指标字段的数据类型0为numeric 1为字符串, 删除废弃的数据来源配置时用到
DATA_MAXTIME         numeric(18),        -- 采集表表数据的最大时间，供汇聚的结束时间判定，数值从原始记录收集表来，yyyyMMddHHmmssSSS
CYCLE_P              varchar(20), -- 数据源表的数据粒度，该字段为空时DATA_MAXTIME就交由项目维护,如果为 DELAY 那么DATA_MAXTIME字段等于系统时间减去INDB_DURATION
SEVERAL_CYCLES       numeric(9) default 1,
CUSTOM_CYCLE         numeric(18) default 0 not null, -- 单位毫秒
TIME_OFFSET          numeric(18) default 0, -- 单位毫秒
LAST_UPDATE_MAXTIME  numeric(18), -- 上次DATA_MAXTIME字段更新时间 yyyyMMddHHmmssSSS
INDB_DURATION        numeric(18), -- 超过多长时间data_maxtime没有变化就添加上一个粒度，其实就是采集入库时长，单位毫秒
MXT_JOB_INTERVAL     varchar(255), -- 更新DATA_MAXTIME：cron表达式,为空时就不起任务
LASTTIME             numeric(18) not null, -- 上一次按writetime查询KT_DM_RAW_RECORD_INFO(该表已经采用分表处理)表补汇时间信息的时间戳记录也就是下一次的开始时间，yyyyMMddHHmmssSSS
END_DATE             numeric(18) default 600000 not null, -- 每次找回补汇信息按writetime查找的结束时间
RE_INTERVAL          numeric(18) default 1800000 not null,   -- (适当的调大一点就可以避免找出重复的补汇记录)循环粒度,单位毫秒,按时间(WRITETIME)循环找回补汇记录，分批次找回,以下两个字段在性能允许下尽量配置小些
JOB_INTERVAL         varchar(255), -- 分JOB找回补汇记录的JOB运行周期配置,为空时就不起任务
RETASK_TABLE         varchar(100), -- @NEW格式化补汇时间存储的表名,为空时：'RF'||SUBSTR(TABLE_NAME,1,28)
RETASK_TABSPACE      varchar(100) default 'KT_DM_RETASK',  -- @NEW记录格式表空间名
RETASK_IDXSPACE      varchar(100) default 'KT_DM_RETASK_IDX',  -- @NEW记录格式表索引空间名
RECORD_CP_TABLE      varchar(100), -- 因为数据需要经过java所以这里是经过项目化压缩了的同时里面增加一个COUNTNUM字段'CP'||SUBSTR(TABLE_NAME,1,28))
CRE_TRIGGER          numeric(1) default 1 not null, -- 是否使用触发器来收集表记录以实现补汇记录收集 1使用0不使用，如果为1的话那么会有一个过程在定时轮寻看是否创建了该表的触发器，没有创建的话则创建，记录信息插到KT_DM_RAW_RECORD_INFO(该表已经采用分表处理),数据表的最大时间收集也受是否创建触发器影响
TRIGGER_NAME         varchar(100), -- 为空时触发器名字为'T'||SUBSTR(TABLE_NAME,1,29),不为空时使用这个名字
RECORD_JOB_INTERVAL  varchar(255), -- cron表达式,采集记录表数据压缩周期配置该处为了性能考虑最好项目化使用存储过程实现,为空时就不起任务
RECORD_LASTTIME      numeric(18), -- 采集记录表上次压缩结束时间
RECORD_TABLE         varchar(100), -- dctime和writetime数据存放的表名(分表存储可以减少IO负担，且存储的时候又少了一个表名字段，减少了数据冗余,为空时：'R'||SUBSTR(TABLE_NAME,1,29))
RECORD_TIME_START    numeric(18), -- 数据记录表分区最小时间
RECORD_TIME_END      numeric(18), -- 数据记录表分区结束时间
RECORD_TIME_RESERVED numeric(10) default 2 not null, -- 数据记录表数据保留时长,单位天
RECORD_FUT_RESERVED  numeric(10) default 5 not null, -- 预留几个分区,以数据为准5代表多存5个粒度的数据也就是说预留6个分区
RECORD_TABSPACE      varchar(100) default 'KT_DM_RECORD',  -- 记录表表空间名
RECORD_IDXSPACE      varchar(100) default 'KT_DM_RECORD_IDX',  -- 记录表索引空间名
REPEAT_TABLE         varchar(100), -- (该字段为空时表示不需要进行数据回迁操作)采集数据发生了主键冲突时入的表表名(该表的表结构和采集表一样且需要按照WRITETIME分区,回迁时按WRITETIME进行回迁并记录回迁时间戳,回迁到采集数据表的时候赋予当前的数据库系统时间当做数据采集表的WRITETIME)
REPEAT_TYPE          numeric(1) default 0, -- 剔重数据的回迁方式,是把重复的数据更新还是把重复的数据抛弃掉,把重复的数据更新这个比较耗性能,0:表示丢弃,1:表示更新
REPEAT_LASTTIME      numeric(18), -- 上一次按writetime查询REPEAT表信息的时间戳记录也就是下一次的开始时间
REPEAT_END_DATE      numeric(18) default 600000, -- 每次回迁REPEAT_TABLE按writetime查找的结束时间
REPEAT_WT_FIELD      varchar(100) default 'WRITETIME', -- 剔重表的数据写入时间字段名
REPEAT_WT_TID        numeric(18) default 2, -- WRITETIME_FIELD数值字段格式TIMETYPE_ID
REPEATTAB_INTERVAL   numeric(18), -- 每次回迁REPEAT_TABLE循环粒度,单位毫秒,该字段为空时默认回迁周期为天，该字段值决定数据剔重表分区粒度
REPEATJOB_INTERVAL   varchar(255),  -- 分JOB回迁剔重数据的JOB运行周期配置,cron表达式,为空时就不起任务
USER_ID              numeric(18) , -- 用户ID
USER_CODE            varchar(70) , -- 用户登入代码
USER_NAME            varchar(40)  -- 用户昵称
) ;
alter table kt_dm_recs_info add primary key (TABLE_NAME) ;
create unique index kt_dm_ix_repeat_tab on kt_dm_recs_info(REPEAT_TABLE) ;
create unique index kt_dm_ix_record_tab on kt_dm_recs_info(RECORD_TABLE) ;
create unique index kt_dm_ix_record_cp_tab on kt_dm_recs_info(RECORD_CP_TABLE) ;
create unique index kt_dm_ix_retask_tab on kt_dm_recs_info(RETASK_TABLE) ;

-- 汇聚组表,不考虑汇聚组补汇可以不用配置该表
-- drop table kt_dm_regroup_info;
create table kt_dm_regroup_info(
RE_GROUP         numeric(18) not null,  -- 涉及到需要补汇的汇聚组，需要在汇聚任务组表里面添加一个汇聚组字段
RE_NAME          varchar(255) not null,
REGROUP_JOBID    varchar(1000),    -- 补汇任务对应的JOBID，在kettle里面配置补汇流程时配置该处
REGROUP_LEVEL    numeric(10) default 1000,
JOB_INTERVAL     varchar(255), -- 补汇记录迁移到汇聚组表里面JOB周期,这个参数为空时job的执行周期以该任务组里面最小的任务计算周期为准,为空时就不起任务
LASTTIME         numeric(18) not null,   -- 补汇记录迁移到汇聚组表里面，WRITETIME时间戳 yyyyMMddHHmmssSSS
END_DATE         numeric(18) default 600000 not null, -- 每次迁移补汇记录和补汇按writetime查找的结束时间，共用一个字段
RECS_COUNTNUM    numeric(18) default 1 not null, -- 大于等于多少条延迟数据才进行补汇
RE_INTERVAL      numeric(18) default 3600000 not null, -- 补汇循环粒度,单位毫秒(放这里是希望所有的任务组的补汇记录能一一对应)
EXC_STOP_SIGN    numeric(1) default 1 not null,  -- 出现异常是否停止更新lasttime 1 代表 出现异常后停止更新 0 代表出现异常后继续走
REGROUP_INTERVAL numeric(18) default 3600000 not null,   -- (适当的调大一点就可以避免找出重复的补汇记录)汇聚组补汇记录迁移循环粒度,单位毫秒
MULTI_JOB        numeric(1) default 1 not null, -- 是否按汇聚组*任务组建立任务开关 1 是 0 否
MAX_WAIT         numeric(18) default 3600000 not null, -- 两个补汇组用同一个汇聚任务时等待时长
REGROUP_TABLE    varchar(100), -- 汇聚组补汇记录表名,为空时：'RG'||SUBSTR(RE_GROUP,1,28)
REGROUP_TABSPACE varchar(100) default 'KT_DM_REGROUP',  -- 汇聚组补汇记录表空间名
REGROUP_IDXSPACE varchar(100) default 'KT_DM_REGROUP_IDX',  -- 汇聚组补汇记录索引空间名
REJOB_INTERVAL   varchar(255),  -- 补汇扫描REGROUP_TABLE表周期,为空时就不起任务
USER_ID          numeric(18) , -- 用户ID
USER_CODE        varchar(70) , -- 用户登入代码
USER_NAME        varchar(40)  -- 用户昵称
) ;
alter table kt_dm_regroup_info add primary key (RE_GROUP) ;
create unique index kt_dm_regroup_i_name on kt_dm_regroup_info(RE_NAME) ;
create unique index kt_dm_regroup_tab on kt_dm_regroup_info(REGROUP_TABLE) ;
-- 汇聚组和采集数据来源表关联关系表,不考虑汇聚组补汇可以不用配置该表
-- drop table kt_dm_regroup_table;
create table kt_dm_regroup_table(
RE_GROUP   numeric(18) not null,
TABLE_NAME varchar(100) not null  -- 格式和KT_DM_RECS_INFO.TABLE_NAME一样
) ;
alter table kt_dm_regroup_table add primary key (RE_GROUP,TABLE_NAME) ;  -- 多对多
-- 关联汇聚任务组表(这里最好RE_GROUP，TASK_GROUP_ID是一对多，不然等待锁也比较耗性能),不考虑汇聚组补汇可以不用配置该表
-- drop table kt_dm_regroup_task;
create table kt_dm_regroup_task(
RE_GROUP      numeric(18) not null,
TASK_GROUP_ID numeric(18) not null,
LASTTIME      numeric(18), -- lasttime (以下字段都是为了补汇能并行而设计的) yyyyMMddHHmmssSSS
JOB_INTERVAL  varchar(255), -- job cron,为空时就不起任务
RE_GROUPORDER numeric(18) not null -- 汇聚组，串行补汇顺序，如果补汇流程在kettle里面配置了这里的循序将失效
) ;
alter table kt_dm_regroup_task add primary key (RE_GROUP,TASK_GROUP_ID) ;
create unique index kt_dm_grouporder on kt_dm_regroup_task(RE_GROUP,RE_GROUPORDER) ;



-- ******************************************提供给插件过程的信息表(主要用于在查询前对数据来源表进行表分析和判断哪个分区有数据)**************************
-- 计算源数据表信息(TaskDSManager里面维护)
-- drop table kt_dm_datasource_info;
create table kt_dm_datasource_info(
DATASOURCE_ID varchar(225) not null, -- TABLE_NAME||'~'||TIME_FIELD||'~'||DIM_FIELD||'~'||DIM_NO ，这里也许会不够长度，但是mysql建立约束只能是(INNODB:767;MYISAM:1000)个字节所以暂时225
TABLE_NAME    varchar(100) not null, -- 计算任务数据来源表表名 DBTYPE.DBNAME.TABLE_NAME或DBNAME.TABLE_NAME或TABLE_NAME
TIME_FIELD    varchar(2000), -- 计算任务数据来源表时间字段名,多个使用";"号分隔,多个字段时取第一个当做分区字段
TIME_TYPE     varchar(600), -- 计算任务数据来源表时间字段值格式类型,多个使用";"号分隔,多个类型时取第一个当做分区字段类型
DIM_FIELD     varchar(2000), -- 数据来源表其他维度字段字段名多个使用";"号分隔
DIM_NO        varchar(2000), -- 数据来源表其他维度数值多个使用";"号分隔
DIM_TYPE      varchar(600), -- 数据来源表其他维度字段的数据类型0为NUMBER 1为字符串;多个使用";"号分隔
DATA_MAXTIME  numeric(18) -- 计算任务数据来源表数据的最大时间,目前这个字段暂时没有使用到
) ;
alter table kt_dm_datasource_info add primary key (DATASOURCE_ID) ;
-- 计算源数据表和计算任务的关联关系表(DATA_MODEL_DS_MANAGE过程里面维护)
-- drop table kt_dm_dstable_calsch;
create table kt_dm_dstable_calsch(
CS_ID          numeric(18) not null,    -- 关联计算任务表的CS_ID
DATASOURCE_ID  varchar(225) not null    -- TABLE_NAME||'~'||TIME_FIELD||'~'||DIM_FIELD||'~'||DIM_NO
) ;
alter table kt_dm_dstable_calsch add primary key (CS_ID,DATASOURCE_ID) ;
-- 以上两张表和补汇功能没有多大关系，主要是给插件过程提供任务涉及的表的信息






-- 功能开关表
-- drop table kt_dm_sys_parameter;
create table kt_dm_sys_parameter(
  PAR_KEY   varchar(200) not null, -- 参数KEY
  PAR_NAME  varchar(300), -- 参数名称
  PAR_DESCR varchar(1000), -- 参数描述
  PAR_TYPE  varchar(300) not null, -- 参数类型
  PAR_VALUE varchar(4000) not null  -- 开关值或其他参数值
) ;
alter table kt_dm_sys_parameter add primary key (PAR_KEY) ;

-- 字典相关
-- 数据计算粒度字典表
-- drop table kt_dm_cycle_dict;
create table kt_dm_cycle_dict(
CYCLE_ID    numeric(2) not null,   -- ID
CYCLE_P     varchar(20) not null, -- SS,MI,HALFHOU,HOU,DAY,WEEK,MONTH,SEASON,YEAR, CUSTOM
CYCLE_NAME  varchar(100),  -- 秒 ，分钟 ，半小时 ，小时 ，天 ，周 ，月 ，季 ，年,自定义
CYCLE_DESCR varchar(1000), -- 描述
CYCLE_TIME  numeric(18)   -- 多少毫秒
) ;
alter table kt_dm_cycle_dict add primary key (CYCLE_ID) ;

-- DML
insert into kt_dm_cycle_dict(CYCLE_ID,CYCLE_P,CYCLE_NAME,CYCLE_DESCR, CYCLE_TIME)
values (1,'SS','秒','秒',1000);
insert into kt_dm_cycle_dict(CYCLE_ID,CYCLE_P,CYCLE_NAME,CYCLE_DESCR, CYCLE_TIME)
values (2,'MI','分钟','分钟',60000);
insert into kt_dm_cycle_dict(CYCLE_ID,CYCLE_P,CYCLE_NAME,CYCLE_DESCR, CYCLE_TIME)
values (3,'HALFHOU','半小时','半小时',1800000);
insert into kt_dm_cycle_dict(CYCLE_ID,CYCLE_P,CYCLE_NAME,CYCLE_DESCR, CYCLE_TIME)
values (4,'HOU','小时','小时',3600000);
insert into kt_dm_cycle_dict(CYCLE_ID,CYCLE_P,CYCLE_NAME,CYCLE_DESCR, CYCLE_TIME)
values (5,'DAY','天','天',86400000);
insert into kt_dm_cycle_dict(CYCLE_ID,CYCLE_P,CYCLE_NAME,CYCLE_DESCR, CYCLE_TIME)
values (6,'WEEK','周','周',604800000);
insert into kt_dm_cycle_dict(CYCLE_ID,CYCLE_P,CYCLE_NAME,CYCLE_DESCR, CYCLE_TIME)
values (7,'MONTH','月','月',2592000000);
insert into kt_dm_cycle_dict(CYCLE_ID,CYCLE_P,CYCLE_NAME,CYCLE_DESCR, CYCLE_TIME)
values (8,'SEASON','季','季',7776000000);
insert into kt_dm_cycle_dict(CYCLE_ID,CYCLE_P,CYCLE_NAME,CYCLE_DESCR, CYCLE_TIME)
values (9,'YEAR','年','年',31536000000);
insert into kt_dm_cycle_dict(CYCLE_ID,CYCLE_P,CYCLE_NAME,CYCLE_DESCR, CYCLE_TIME)
values (10,'CUSTOM','自定义','自定义',NULL);
commit ;

-- DATA_SQL内置参数字典表
-- drop table kt_dm_sql_timepar_dict ;
create table kt_dm_sql_timepar_dict(
TID          numeric(18) not null,
TIMETYPE_ID  numeric(18) not null, -- 关联KT_DM_TIMETYPE_DICT.TID
START_OR_END varchar(100) not null, -- 开始或者结束标示
TIMEPARKEY   varchar(100) not null,
TIMEPARNAME  varchar(1000),
TIMEPARDESCR varchar(2000)
) ;
alter table kt_dm_sql_timepar_dict add primary key (TID) ;
create unique index kt_dm_ix_sql_timeparkey on kt_dm_sql_timepar_dict(TIMEPARKEY) ;
create unique index kt_dm_ix_sql_timetype_se on kt_dm_sql_timepar_dict(TIMETYPE_ID,START_OR_END) ;
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (0,0,'START','${ST0}','13位毫秒开始时间','13位毫秒开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (1,0,'END','${ET0}','13位毫秒结束时间','13位毫秒结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (2,1,'START','${ST1}','10BITS开始时间','10位秒开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (3,1,'END','${ET1}','10BITS结束时间','10位秒结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (4,2,'START','${ST2}','yyyyMMddHHmmss格式开始时间','yyyyMMddHHmmss格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (5,2,'END','${ET2}','yyyyMMddHHmmss格式结束时间','yyyyMMddHHmmss格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (6,3,'START','${ST3}','yyyyMMddHHmm格式开始时间','yyyyMMddHHmm格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (7,3,'END','${ET3}','yyyyMMddHHmm格式结束时间','yyyyMMddHHmm格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (8,4,'START','${ST4}','yyyyMMddHH格式开始时间','yyyyMMddHH格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (9,4,'END','${ET4}','yyyyMMddHH格式结束时间','yyyyMMddHH格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (10,5,'START','${ST5}','yyyyMMdd格式开始时间','yyyyMMdd格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (11,5,'END','${ET5}','yyyyMMdd格式结束时间','yyyyMMdd格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (12,6,'START','${ST6}','yyyyMM格式开始时间','yyyyMM格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (13,6,'END','${ET6}','yyyyMM格式结束时间','yyyyMM格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (14,7,'START','${ST7}','yyyy格式开始时间','yyyy格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (15,7,'END','${ET7}','yyyy格式结束时间','yyyy格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (16,8,'START','${ST8}','yyyy-MM格式开始时间','yyyy-MM格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (17,8,'END','${ET8}','yyyy-MM格式结束时间','yyyy-MM格式结束时间');

insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (18,9,'START','${ST9}','yyyy-MM-dd格式开始时间','yyyy-MM-dd格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (19,9,'END','${ET9}','yyyy-MM-dd格式结束时间','yyyy-MM-dd格式结束时间');

insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (20,10,'START','${ST10}','yyyy-MM-dd HH格式开始时间','yyyy-MM-dd HH格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (21,10,'END','${ET10}','yyyy-MM-dd HH格式结束时间','yyyy-MM-dd HH格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (22,11,'START','${ST11}','yyyy-MM-dd HH:mm格式开始时间','yyyy-MM-dd HH:mm格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (23,11,'END','${ET11}','yyyy-MM-dd HH:mm格式结束时间','yyyy-MM-dd HH:mm格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (24,12,'START','${ST12}','yyyy-MM-dd HH:mm:ss格式开始时间','yyyy-MM-dd HH:mm:ss格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (25,12,'END','${ET12}','yyyy-MM-dd HH:mm:ss格式结束时间','yyyy-MM-dd HH:mm:ss格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (26,13,'START','${ST13}','yyyy-MM-dd HH:mm:ss.SSS格式开始时间','yyyy-MM-dd HH:mm:ss.SSS格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (27,13,'END','${ET13}','yyyy-MM-dd HH:mm:ss.SSS格式结束时间','yyyy-MM-dd HH:mm:ss.SSS格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (28,14,'START','${ST14}','yyyy年格式开始时间','yyyy年格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (29,14,'END','${ET14}','yyyy年格式结束时间','yyyy年格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (30,15,'START','${ST15}','yyyy年MM月格式开始时间','yyyy年MM月格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (31,15,'END','${ET15}','yyyy年MM月格式结束时间','yyyy年MM月格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (32,16,'START','${ST16}','yyyy年MM月dd日格式开始时间','yyyy年MM月dd日格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (33,16,'END','${ET16}','yyyy年MM月dd日格式结束时间','yyyy年MM月dd日格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (34,17,'START','${ST17}','yyyy年MM月dd日 HH时格式开始时间','yyyy年MM月dd日 HH时格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (35,17,'END','${ET17}','yyyy年MM月dd日 HH时格式结束时间','yyyy年MM月dd日 HH时格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (36,18,'START','${ST18}','yyyy年MM月dd日 HH时mm分格式开始时间','yyyy年MM月dd日 HH时mm分格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (37,18,'END','${ET18}','yyyy年MM月dd日 HH时mm分格式结束时间','yyyy年MM月dd日 HH时mm分格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (38,19,'START','${ST19}','yyyy年MM月dd日 HH时mm分ss秒格式开始时间','yyyy年MM月dd日 HH时mm分ss秒格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (39,19,'END','${ET19}','yyyy年MM月dd日 HH时mm分ss秒格式结束时间','yyyy年MM月dd日 HH时mm分ss秒格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (40,20,'START','${ST20}','yyyyMMddHHmmssSSS格式开始时间','yyyyMMddHHmmssSSS格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (41,20,'END','${ET20}','yyyyMMddHHmmssSSS格式结束时间','yyyyMMddHHmmssSSS格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (42,21,'START','${ST21}','yyyy/MM/dd HH:mm:ss.SSS格式开始时间','yyyy/MM/dd HH:mm:ss.SSS格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (43,21,'END','${ET21}','yyyy/MM/dd HH:mm:ss.SSS格式结束时间','yyyy/MM/dd HH:mm:ss.SSS格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (44,22,'START','${ST22}','yyyy/MM/dd HH:mm:ss:SSS格式开始时间','yyyy/MM/dd HH:mm:ss:SSS格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (45,22,'END','${ET22}','yyyy/MM/dd HH:mm:ss:SSS格式结束时间','yyyy/MM/dd HH:mm:ss:SSS格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (46,23,'START','${ST23}','yyyy/MM/dd HH:mm:ss格式开始时间','yyyy/MM/dd HH:mm:ss格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (47,23,'END','${ET23}','yyyy/MM/dd HH:mm:ss格式结束时间','yyyy/MM/dd HH:mm:ss格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (48,24,'START','${ST24}','yyyy/MM/dd HH:mm格式开始时间','yyyy/MM/dd HH:mm格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (49,24,'END','${ET24}','yyyy/MM/dd HH:mm格式结束时间','yyyy/MM/dd HH:mm格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (50,25,'START','${ST25}','yyyy/MM/dd HH格式开始时间','yyyy/MM/dd HH格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (51,25,'END','${ET25}','yyyy/MM/dd HH格式结束时间','yyyy/MM/dd HH格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (52,26,'START','${ST26}','yyyy/MM/dd格式开始时间','yyyy/MM/dd格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (53,26,'END','${ET26}','yyyy/MM/dd格式结束时间','yyyy/MM/dd格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (54,27,'START','${ST27}','yyyy/MM格式开始时间','yyyy/MM格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (55,27,'END','${ET27}','yyyy/MM格式结束时间','yyyy/MM格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (56,28,'START','${ST28}','MM格式开始时间','MM格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (57,28,'END','${ET28}','MM格式结束时间','MM格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (58,29,'START','${ST29}','dd格式开始时间','dd格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (59,29,'END','${ET29}','dd格式结束时间','dd格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (60,30,'START','${ST30}','HH格式开始时间','HH格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (61,30,'END','${ET30}','HH格式结束时间','HH格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (62,31,'START','${ST31}','mm格式开始时间','mm格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (63,31,'END','${ET31}','mm格式结束时间','mm格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (64,32,'START','${ST32}','ss格式开始时间','ss格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (65,32,'END','${ET32}','ss格式结束时间','ss格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (200,100,'START','${ST100}','DATE格式开始时间','DATE格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (201,100,'END','${ET100}','DATE格式结束时间','DATE格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (202,101,'START','${ST101}','TIMESTAMP格式开始时间','TIMESTAMP格式开始时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (203,101,'END','${ET101}','TIMESTAMP格式结束时间','TIMESTAMP格式结束时间');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (10000,-1,'OTHER','${DIM_NO}','指标编号','指标编号');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (10001,-2,'OTHER','${DCTIME}','采集时间13位毫秒格式','采集时间13位毫秒格式,BIZMAN表格式时有效');
insert into kt_dm_sql_timepar_dict(TID, TIMETYPE_ID, START_OR_END, TIMEPARKEY, TIMEPARNAME, TIMEPARDESCR)
values (10002,-3,'OTHER','${WRITETIME}','入库时间13位毫秒格式','入库时间13位毫秒格式,BIZMAN表格式时有效');
commit;
-- NOT_BIZ_TIME_TYPE字段格式说明-- 系统内置的ID和TIMETYPE不允许变更
-- drop table kt_dm_timetype_dict ;
create table kt_dm_timetype_dict(
TID           numeric(18) not null,
TIMETYPE      varchar(255) not null,
TIMETYPENAME  varchar(100),
TIMETYPEDESCR varchar(1000)
) ;
alter table kt_dm_timetype_dict add primary key (TID) ;
create unique index kt_dm_ix_time_type on kt_dm_timetype_dict(TIMETYPE) ;  -- 里面有MM和mm mysql数据库默认不区分大小写所以在建库时需要选择 utf8 -- UTF-8 Unicode utf8_bin
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (0,'DMMS','13位毫秒格式','13位毫秒格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (1,'DMSC','10BITS','10位秒格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (2,'yyyyMMddHHmmss','yyyyMMddHHmmss格式','yyyyMMddHHmmss格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (3,'yyyyMMddHHmm','yyyyMMddHHmm格式','yyyyMMddHHmm格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (4,'yyyyMMddHH','yyyyMMddHH格式','yyyyMMddHH格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (5,'yyyyMMdd','yyyyMMdd格式','yyyyMMdd格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (6,'yyyyMM','yyyyMM格式','yyyyMM格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (7,'yyyy','yyyy格式','yyyy格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (8,'yyyy-MM','yyyy-MM格式','yyyy-MM格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (9,'yyyy-MM-dd','yyyy-MM-dd格式','yyyy-MM-dd格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (10,'yyyy-MM-dd HH','yyyy-MM-dd HH格式','yyyy-MM-dd HH格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (11,'yyyy-MM-dd HH:mm','yyyy-MM-dd HH:mm格式','yyyy-MM-dd HH:mm格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (12,'yyyy-MM-dd HH:mm:ss','yyyy-MM-dd HH:mm:ss格式','yyyy-MM-dd HH:mm:ss格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (13,'yyyy-MM-dd HH:mm:ss.SSS','yyyy-MM-dd HH:mm:ss.SSS格式','yyyy-MM-dd HH:mm:ss.SSS格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (14,'yyyy年','yyyy年格式','yyyy年格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (15,'yyyy年MM月','yyyy年MM月格式','yyyy年MM月格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (16,'yyyy年MM月dd日','yyyy年MM月dd日格式','yyyy年MM月dd日格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (17,'yyyy年MM月dd日 HH时','yyyy年MM月dd日 HH时格式','yyyy年MM月dd日 HH时格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (18,'yyyy年MM月dd日 HH时mm分','yyyy年MM月dd日 HH时mm分格式','yyyy年MM月dd日 HH时mm分格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (19,'yyyy年MM月dd日 HH时mm分ss秒','yyyy年MM月dd日 HH时mm分ss秒格式','yyyy年MM月dd日 HH时mm分ss秒格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (20,'yyyyMMddHHmmssSSS','yyyyMMddHHmmssSSS格式','yyyyMMddHHmmssSSS格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (21,'yyyy/MM/dd HH:mm:ss.SSS','yyyy/MM/dd HH:mm:ss.SSS格式','yyyy/MM/dd HH:mm:ss.SSS格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (22,'yyyy/MM/dd HH:mm:ss:SSS','yyyy/MM/dd HH:mm:ss:SSS格式','yyyy/MM/dd HH:mm:ss:SSS格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (23,'yyyy/MM/dd HH:mm:ss','yyyy/MM/dd HH:mm:ss格式','yyyy/MM/dd HH:mm:ss格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (24,'yyyy/MM/dd HH:mm','yyyy/MM/dd HH:mm格式','yyyy/MM/dd HH:mm格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (25,'yyyy/MM/dd HH','yyyy/MM/dd HH格式','yyyy/MM/dd HH格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (26,'yyyy/MM/dd','yyyy/MM/dd格式','yyyy/MM/dd格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (27,'yyyy/MM','yyyy/MM格式','yyyy/MM格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (28,'MM','MM格式','MM格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (29,'dd','dd格式','dd格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (30,'HH','HH格式','HH格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (31,'mm','mm格式','mm格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (32,'ss','ss格式','ss格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (100,'DATE','DATE格式','DATE格式');
insert into kt_dm_timetype_dict(TID, TIMETYPE, TIMETYPENAME, TIMETYPEDESCR)
values (101,'TIMESTAMP','TIMESTAMP格式','TIMESTAMP格式');
commit;

-- 2020-01-06 start
-- 不记录当前的主从信息表了。应为ID字段不好确定一般为IP+进程号为唯一，但是每次重启进程号都会变，所以update不了，如果是IP+端口的话。对于java应用那么很可能没有端口
-- 主从日志表
-- DROP TABLE IF EXISTS t_sys_master_slave_log ;
CREATE TABLE t_sys_master_slave_log (
  LOG_ID            numeric(18) NOT NULL,   -- 13位毫秒+5位循环递增序列,时间字段从这里解析
  IP                varchar(80) ,  -- IP
  P_NUM             varchar(50) ,  -- 进程号
  MS                numeric(1),    -- 1主  2从
  APP_TYPE          varchar(50)    -- 类型,主从服务的类型,如果一个应用里面只有一种主从状态的话那么为APP_SYS
) ENGINE=InnoDB;
alter table t_sys_master_slave_log add primary key (LOG_ID) ;

-- 2020-01-06 end

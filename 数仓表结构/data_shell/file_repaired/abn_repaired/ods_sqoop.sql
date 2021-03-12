create table flink_config.task_scheduler_emr(
job_id varchar(100) primary key,
job_name varchar(100) ,
oozieAdress varchar(500),
rn int
);



create table flink_config.task_info_emr(
id int primary key auto_increment,
job_id varchar(100) ,
job_name varchar(100) ,
batch_date varchar(500),
run_status varchar(100),
start_time timestamp,
end_time  timestamp
);

create table flink_config.task_schedule_date_emr(
batch_date STRING COMMENT '批次日期',
PRIMARY KEY (batch_date)
);


CREATE TABLE ods.task_schedule_date (
  batch_date STRING NOT NULL ENCODING AUTO_ENCODING COMPRESSION DEFAULT_COMPRESSION COMMENT '批次日期',
  PRIMARY KEY (batch_date)
)
PARTITION BY HASH (batch_date) PARTITIONS 8
 COMMENT '系统状态表'
STORED AS KUDU
TBLPROPERTIES ('kudu.master_addresses'='node172:7051');

--机器人消息通知
create table flink_config.robot_person_info(
id int primary key auto_increment,
hookurl  varchar(500),
isEnable int
);



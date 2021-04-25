
create table flink_config.ht_repair_table(
id varchar(100) primary key,
due_bill_no varchar(100) ,
register_date varchar(100),
active_date  varchar(100),
repair_flag  varchar(100),
repair_date  varchar(100)
);


CREATE TABLE `stage_data_check_ods` (
  `table_name` varchar(100) DEFAULT NULL COMMENT '表名',
  `p_type` varchar(100) DEFAULT NULL COMMENT '分区名',
  `d_date` varchar(100) DEFAULT NULL COMMENT '分区日期',
   `num` int(11) DEFAULT NULL COMMENT '数据条数'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `stage_data_check_stage` (
  `table_name` varchar(100) DEFAULT NULL COMMENT '表名',
  `p_type` varchar(100) DEFAULT NULL COMMENT '分区名',
  `d_date` varchar(100) DEFAULT NULL COMMENT '分区日期',
   `num` int(11) DEFAULT NULL COMMENT '数据条数'
) ENGINE=InnoDB DEFAULT CHARSET=utf8
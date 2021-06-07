CREATE TABLE `data_asset_pool` (
   `project_name` varchar(100) comment '项目名称',
   `project_id` varchar(100) DEFAULT NULL,
   `project_start_date` varchar (100) comment '项目起始日期',
   `project_end_date` varchar (100) comment '项目结束日期',
   `init_total_amount` decimal(65,0) DEFAULT NULL comment '项目初始本金',
   `cycle_start_date`  varchar(100) comment '循环期开始日期',
   `cycle_end_date`varchar(100) comment '循环期结束日期',
   `amortization_period_start_date` varchar(100) comment '摊还期开始日期',
   `amortization_period_end_date` varchar(100) comment '摊还期结束日期',
   `loan_terms` varchar(100) comment '可放期次',
   `available_amount` decimal(65,0) DEFAULT 0 comment '项目可用于放款金额',
   `pmml_url` varchar(255) DEFAULT NULL comment '模型文件地址',
   `update_time` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
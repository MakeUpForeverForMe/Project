CREATE DATABASE IF NOT EXISTS java_web;

use java_web;

DROP TABLE IF EXISTS `examstudent`;
CREATE TABLE IF NOT EXISTS `examstudent`
(
    `flow_id`      int(11)      NOT NULL PRIMARY KEY AUTO_INCREMENT COMMENT '学生编号',
    `type`         int(11)      NOT NULL COMMENT '学生类型',
    `id_card`      varchar(255) NOT NULL COMMENT '身份证号',
    `exam_card`    varchar(255) NOT NULL COMMENT '出生日期',
    `student_name` varchar(255) NOT NULL COMMENT '学生姓名',
    `location`     varchar(255) NOT NULL COMMENT '学生生活地址',
    `grade`        int(11)      NOT NULL DEFAULT '0' COMMENT '学生成绩',
    `create_time`  datetime              DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time`  datetime              DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT = '学生信息表';

insert into examstudent(type, id_card, exam_card, student_name, location, grade)
values (0, '210001199001010101', '1990-01-01', '张三', '沈阳', 90),
       (1, '210102200010110242', '2000-10-11', '李玲', '大连', 100)
;

DROP TABLE IF EXISTS `customers`;
CREATE TABLE IF NOT EXISTS `customers`
(
    `id`          int         NOT NULL PRIMARY KEY AUTO_INCREMENT COMMENT '主键自增ID',
    `name`        varchar(30) NOT NULL UNIQUE COMMENT '姓名',
    `address`     varchar(30) COMMENT '地址',
    `phone`       varchar(30) COMMENT '联系电话',
    `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='客户信息表';

insert into customers (name, address, phone)
values ('Mike', 'BeiJing', '18012341234'),
       ('Jerry', 'ShangHai', '18112341234'),
       ('Att', '盛京', '18813801380'),
       ('mmd', 'çäº¬', ''),
       ('pcc', '盛京', '')
;

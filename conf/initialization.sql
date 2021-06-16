CREATE
    DATABASE IF NOT EXISTS java_web;

use
    java_web;

DROP TABLE IF EXISTS student;
CREATE TABLE IF NOT EXISTS student
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

insert into student(type, id_card, exam_card, student_name, location, grade)
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
values ('Mike', '北京', '18012341234'),
       ('Jerry', '上海', '18112341234'),
       ('Att', '盛京', '18813801380'),
       ('MMd', '徽京', '18800000000'),
       ('pcc', 'HrBin', '12312341234'),
       ('Mik', '深圳', '13311223344')
;


CREATE TABLE user
(
    id    BIGINT(20)  NOT NULL COMMENT '主键ID',
    name  VARCHAR(30) NULL DEFAULT NULL COMMENT '姓名',
    age   INT(11)     NULL DEFAULT NULL COMMENT '年龄',
    email VARCHAR(50) NULL DEFAULT NULL COMMENT '邮箱',
    PRIMARY KEY (id)
);

INSERT INTO user (id, name, age, email)
VALUES (1, 'Jone', 18, 'test1@baomidou.com'),
       (2, 'Jack', 20, 'test2@baomidou.com'),
       (3, 'Tom', 28, 'test3@baomidou.com'),
       (4, 'Sandy', 21, 'test4@baomidou.com'),
       (5, 'Billie', 24, 'test5@baomidou.com')
;

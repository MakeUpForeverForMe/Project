# 腾讯emr运维记录
### 一、hive shell提交卡住问题
解决方式：
增加export HADOOP_CLIENT_OPTS="-Djline.terminal=jline.UnsupportedTerminal"

### 二、hdfs 传 cos 需要桶类型
指定上传文件的存储的类型：Standard,Standard_IA,Archive
storage_class=Standard
多 AZ 存储：标准（MAZ_STANDARD）、低频（MAZ_STANDARD_IA）
非多 AZ 存储：标准（STANDARD）、低频（STANDARD_IA）、归档（ARCHIVE）

### 三、hdfs 扩容问题
hdfs只部署在core节点，core扩容只能增加服务器cpu、内存，无法增加数据盘。hdfs扩容只能增加core节点。
core缩容需要开白名单功能，不会产生数据丢失（tx底层做了数据迁移）

### 四、emr服务增加及删除
组件服务增加后无法删除，只能停止服务。

### 五、emr自动伸缩功能的扩容耗时过长
emr技术人员修复中

### 六、hive任务最终阶段movetask失败
1、master节点内存不足，任务提交失败
2、分区字段值区分大小写导致，分区手动修复调整

### 七、emr上的flink组件部署后跑任务空指针异常
自搭flink组件可以提交yarn任务

### 八、自动伸缩扩容失败
1、单次扩容有数量限制，需联系腾讯云的人提高限制
2、同类型的机器不够了，需要增加其他类型的机器规格

### 九、hive on tez执行报错No work found for tablescan TS[310] (state=,code=0)
关闭mapjoin
set hive.auto.convert.join=false;                    -- 关闭自动 MapJoin
set hive.auto.convert.join.noconditionaltask=false;  -- 关闭自动 MapJoin

### 十、cos访问请求QPS存在上限，并发访问过多时会出现50X
暂时无解决方案，影响跑批速度

### 十一、跑批时task节点100G数据盘占用率超过80%
原因未知


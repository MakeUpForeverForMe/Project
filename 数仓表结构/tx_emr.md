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
master节点内存不足，任务提交失败

### 七、emr上的flink组件部署后跑任务空指针异常
自搭flink组件可以提交yarn任务
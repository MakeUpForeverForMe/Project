#!/bin/bash
biz_date=$1
project_id=$2


spark-submit \
--class com.weshare.pmml.app.MockDataAndInvokePmml \
--deploy-mode client \
--driver-memory 8g \
--executor-memory 6g \
--executor-cores 2 \
--num-executors 50 \
--master yarn \
--conf "spark.sql.shuffle.partitions=100" \
--conf "spark.default.parallelism=100" \
--conf "spark.driver.memoryOverhead=4096" \
--conf "spark.executor.memoryOverhead=2048" \
--conf "spark.executor.userClassPathFirst=true" \
--conf "spark.storage.memoryFraction=0.2" \
--conf "spark.shuffle.memoryFraction=0.6" \
--conf "spark.broadcast.blockSize=50M" \
--jars hdfs:///user/hadoop/pmml/pmml-evaluator-1.5.14.jar,hdfs:///user/hadoop/pmml/pmml-model-1.5.14.jar,hdfs:///user/hadoop/pmml/fastjson-1.2.47.jar,hdfs:///user/hadoop/pmml/guava-22.0.jar,hdfs:///user/hadoop/pmml/json4s-jackson_2.11-3.2.11.jar,hdfs:///user/hadoop/data_watch/druid-1.1.6.jar,hdfs:///user/hadoop/data_watch/mysql-connector-java-5.1.24.jar \
hdfs:///user/hadoop/pmml/common-1.0-SNAPSHOT.jar "$biz_date" "$project_id" EMR



#!/bin/bash
spark-submit \
--class com.weshare.core.AssetFileToHive \
--deploy-mode client \
--driver-memory 1g \
--executor-cores 2 \
--executor-memory 2g \
--master yarn \
--driver-java-options "-Dlog4j.configuration=file:/root/data_shell/bin_abs/driver-log4j.properties" \
hdfs:///user/root/data_shell/bin_abs/asset_package-1.0-SNAPSHOT-shaded.jar \
$1 \
$2

#!/bin/bash
spark-submit \
--driver-java-options "-Dlog4j.configuration=file:/data/data_shell/bin_abs/driver-log4j.properties" \
--class com.weshare.ABSFileToHive \
--master yarn \
--deploy-mode client \
--driver-memory 1g \
--executor-cores 2 \
--executor-memory 2g \
/data/data_shell/bin_abs/asset_package-1.0-SNAPSHOT-shaded.jar \
$1 \
$2 \
$3

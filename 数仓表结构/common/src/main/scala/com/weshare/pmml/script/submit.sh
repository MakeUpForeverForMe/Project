--driver-java-options -verbose:class \
#测试环境提交任务脚本


spark-submit \
--class com.weshare.pmml.app.PmmlMain \
--deploy-mode client \
--driver-memory 2g \
--executor-memory 2g \
--executor-cores 2 \
--num-executors 4 \
--master local[*] \
--conf "spark.driver.userClassPathFirst=true" \
--jars /root/guochao/test/pmml-evaluator-1.5.14.jar,/root/guochao/test/pmml-model-1.5.14.jar,/root/guochao/test/fastjson-1.2.47.jar,/root/guochao/test/guava-22.0.jar \
/root/guochao/test/common-1.0-SNAPSHOT.jar 2021-02-27 eagle.one_million_random_risk_data 0





--conf "spark.executor.userClassPathFirst=true" \


spark-submit \
--class com.weshare.pmml.app.MockDataAndInvokePmml \
--deploy-mode client \
--driver-memory 2g \
--executor-memory 2g \
--executor-cores 2 \
--num-executors 4 \
--master yarn \
--conf "spark.driver.userClassPathFirst=true" \
--jars /root/guochao/test/pmml-evaluator-1.5.14.jar,/root/guochao/test/pmml-model-1.5.14.jar,/root/guochao/test/fastjson-1.2.47.jar,/root/guochao/test/guava-22.0.jar \
/root/guochao/test/common-1.0-SNAPSHOT.jar 2021-02-27 2

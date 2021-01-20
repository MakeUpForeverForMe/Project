package com.mine

import org.apache.spark.sql.SparkSession


object HiveSpark {

    def main(args: Array[String]): Unit = {

        System.setProperty("HADOOP_USER_NAME", "hive")

        val spark = SparkSession.builder()
                .appName(this.getClass.getSimpleName.filter(!_.equals('$')))
                .master("local[*]")
                .config("fs.defaultFS", "hdfs://node5")
                .enableHiveSupport()
                .getOrCreate()
        spark.sql("show databases").show()
    }
}

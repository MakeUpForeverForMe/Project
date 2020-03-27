package com.mine

import org.apache.spark.sql.SparkSession

object UseHDFSFile {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.filter(!_.equals('$')))
      .master("yarn")
      .config("spark.submit.deployMode", "client")
      .config("fs.defaultFS", "hdfs://mycluster")
      .config("dfs.nameservices", "mycluster")
      .config("dfs.ha.namenodes.mycluster", "nn1,nn2")
      .config("dfs.namenode.rpc-address.mycluster.nn1", "10.83.96.9:8020")
      .config("dfs.namenode.rpc-address.mycluster.nn2", "10.83.96.13:8020")
      .config("dfs.client.failover.proxy.provider.mycluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
      .getOrCreate()

    //    spark.conf.getAll.foreach(println)

    //    println(spark.read.textFile("hdfs://mycluster/tmp/dm_user_info_tmp/dm_user_info.csv").count)

    val sc = spark.sparkContext


    println(sc.textFile("hdfs://mycluster/tmp/dm_user_info_tmp/dm_user_info.csv").count())


    //    val writer = new FileWriter("D:\\Users\\ximing.wei\\Desktop\\txt")
    //
    //    writer.write(count + "\n" + count)
    //
    //    writer.flush()
    //    writer.close()
  }
}

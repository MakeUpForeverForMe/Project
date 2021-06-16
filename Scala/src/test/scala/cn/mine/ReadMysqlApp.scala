package cn.mine

import java.io.FileInputStream
import java.io.File
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

/**
  * @author ximing.wei
  */
object ReadMysqlApp {

  private def runJdbcDataSetExample(spark: SparkSession): Unit = {


    val props = new Properties()
    props.load(new FileInputStream("config.properties"))


    val tables = props.getProperty("mysql.table").split(",")
    for (t <- tables) {
      // Loading data from a JDBC source
      val jdbcDF = spark.read
        .format("jdbc")
        .option("url", props.getProperty("mysql.url"))
        .option("dbtable", t)
        .option("user", props.getProperty("mysql.username"))
        .option("password", props.getProperty("mysql.password"))
        //.option("customSchema","extra_info MAP<string,string>")
        .load()
      val url = t.split("\\.")
      val df1 = jdbcDF.filter(props.getProperty("read.condition"))
      df1.printSchema()
      if (df1.schema.fieldNames.toSet.contains("extra_info")) {
        val df2 = df1.withColumn("in_hive_time", functions.expr("0")) //.drop("extra_info")
        df2.write.format("json").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").mode("append")
          .save(props.getProperty("write.url") + url(0) + File.separator + url(1) + File.separator)
      } else {
        val df2 = df1.withColumn("in_hive_time", functions.expr("0"))
        df2.write.format("json").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").mode("append")
          .save(props.getProperty("write.url") + url(0) + File.separator + url(1) + File.separator)
      }
    }

  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkReadMysqlApp")
      //.master("local")
      .config("files", "./config.properties")
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()

    runJdbcDataSetExample(spark)
  }

}

package com.weshare.interface

import com.weshare.entity.Args.{projectId, rowType, tableName, logBase => logB}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
  * @author ximing.wei 2021-05-27 14:45:20
  */
trait AssetFile extends Serializable {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    val logBase: String = logB
    //    private val filePath = s"file:///data/data_shell/file_import/abs_cloud/$tableName/$tableName@$projectId.json"
    private final val filePath = s"D:\\Users\\ximing.wei\\Desktop\\tar/$tableName/$tableName@$projectId.json"
    logger.info(logBase, s"执行任务的文件路径为：$filePath！")

    logger.info(logBase, "创建 SparkSession！")
    val sparkSession: SparkSession = SparkSession.builder()
            .appName(s"ABSFileToHive -- $tableName@$projectId-$rowType")
            .master("local[*]")
            //            .master("yarn")
            //            .config("deploy-mode", "client")
            .config("hive.execution.engine", "spark")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .enableHiveSupport()
            .getOrCreate()

    logger.info(logBase, "获取数据！")
    val data: DataFrame = sparkSession.read.json(filePath)

    def insertData()

    def updateData()

    def deleteData()
}

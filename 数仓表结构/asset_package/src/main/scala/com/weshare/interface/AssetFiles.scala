package com.weshare.interface

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @author ximing.wei 2021-05-27 14:45:20
  */
trait AssetFiles {

    def insertData(sparkSession: SparkSession, dataset: Dataset[String], projectId: String)

    def updateData(sparkSession: SparkSession, dataset: Dataset[String], projectId: String)

    def deleteData(sparkSession: SparkSession, dataset: Dataset[String], projectId: String, importId: String)
}

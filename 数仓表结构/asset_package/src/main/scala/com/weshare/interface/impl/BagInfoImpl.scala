package com.weshare.interface.impl

import com.weshare.interface.AssetFile
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @author ximing.wei 2021-05-27 21:21:46
  */
class BagInfoImpl extends AssetFile {
    override def insertData(): Unit = {
        logger.info(logBase, "执行 插入 语句！")

        logger.info(logBase, "将获取的数据，按字段创建临时表  bag_info_tmp ！")
        data.select(
            lit("project_id"),
            lit("bag_id"),
            lit("bag_name"),
            lit("bag_status"),
            lit("bag_remain_principal"),
            lit("bag_date"),
            lit("insert_date")
        ).createOrReplaceTempView("bag_info_tmp")

        logger.info(logBase, "执行 插入 语句 HQL！")
        sparkSession.sql(
            """
              |insert overwrite table dim.bag_info partition(bag_id)
              |select
              |  project_id,
              |  bag_name,
              |  bag_status,
              |  bag_remain_principal,
              |  bag_date,
              |  insert_date,
              |  bag_id
              |from bag_info_tmp
            """.stripMargin)
        logger.info(logBase, "插入 语句执行 完成！")
    }

    override def updateData(): Unit = {
        logger.info(logBase, "执行 更新 语句！")

        logger.info(logBase, "调用 插入 语句！")
        insertData()
        logger.info(logBase, "更新 语句执行 完成！")
    }

    override def deleteData(): Unit = {
        logger.info(logBase, "执行 删除 语句！")

        logger.info(logBase, "开始执行 删除 语句！")
        data.select(lit("bag_id")).collect().foreach(
            row => {
                val fieldsMap = row.getValuesMap[Any](List("bag_id"))
                val bagId = fieldsMap("bag_id").toString

                logger.info(logBase, s"删除 bag_info 的包编号：$bagId！")
                sparkSession.sql(s"ALTER TABLE dim.bag_info DROP IF EXISTS PARTITION (import_id = '$bagId')")

                logger.info(logBase, s"删除 bag_due_bill_no 的包编号：$bagId！")
                sparkSession.sql(s"ALTER TABLE dim.bag_due_bill_no DROP IF EXISTS PARTITION (import_id = '$bagId')")
            }
        )
        logger.info(logBase, "删除 语句执行 完成！")
    }
}

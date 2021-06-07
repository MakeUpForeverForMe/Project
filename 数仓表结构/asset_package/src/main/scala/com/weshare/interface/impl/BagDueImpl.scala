package com.weshare.interface.impl

import com.weshare.interface.AssetFile
import org.apache.spark.sql.functions.{explode, lit}

/**
  * @author ximing.wei 2021-05-27 21:21:07
  */
class BagDueImpl extends AssetFile {

    override def insertData(): Unit = {
        logger.info(logBase, "执行 插入 语句！")

        logger.info(logBase, "导入 Spark 的隐式转换 ！")
        import sparkSession.implicits._

        logger.info(logBase, "将获取的数据，按字段创建临时表  bag_due ！")
        data.select(
            lit("project_id"),
            lit("bag_id"),
            explode($"due_bill_no").as("due_bill_no")
        ).select(
            lit("project_id"),
            lit("bag_id"),
            lit("due_bill_no.serialNumber").as("due_bill_no"),
            lit("due_bill_no.packageRemainPrincipal").as("package_remain_principal"),
            lit("due_bill_no.packageRemainPeriods").as("package_remain_periods")
        ).createOrReplaceTempView("bag_due")

        logger.info(logBase, "执行 插入 语句 HQL！")
        sparkSession.sql(
            """
              |insert overwrite table dim.bag_due_bill_no partition(bag_id)
              |select
              |  project_id,
              |  due_bill_no,
              |  package_remain_principal,
              |  package_remain_periods,
              |  bag_id
              |from bag_due
            """.stripMargin)
        logger.info(logBase, "插入 语句执行 完成！")
    }

    override def updateData(): Unit = {
        logger.info(logBase, "执行 更新 语句！")

        logger.info(logBase, "对于表 bag_due_bill_no 不应该存在 更新 操作！并抛出异常！")
        throw new IllegalArgumentException("对于表 bag_due_bill_no 不应该存在 删除 操作！")
    }

    override def deleteData(): Unit = {
        logger.info(logBase, "执行 删除 语句！")

        logger.info(logBase, "对于表 bag_due_bill_no 不应该存在 删除 操作！并抛出异常！")
        throw new IllegalArgumentException("对于表 bag_due_bill_no 不应该存在 删除 操作！")
    }
}

package com.weshare.interface.impl

import com.weshare.interface.AssetFile
import org.apache.spark.sql.functions.{explode, lit}

/**
  * @author ximing.wei 2021-05-27 21:10:56
  */
class ProjectDueImpl extends AssetFile {

    override def insertData(): Unit = {
        logger.info(logBase, "执行 插入 语句！")

        logger.info(logBase, "导入 Spark 的隐式转换 ！")
        import sparkSession.implicits._

        logger.info(logBase, "将获取的数据，按字段创建临时表  project_due ！")
        data.select(
            $"project_id",
            $"import_id",
            explode($"due_bill_no").as("due_bill_no")
        ).select(
            lit("project_id"),
            lit("import_id"),
            lit("due_bill_no.serialNumber").as("due_bill_no"),
            lit("due_bill_no.relatedProjectId").as("related_project_id"),
            lit("due_bill_no.relatedDate").as("related_date")
        ).createOrReplaceTempView("project_due")

        logger.info(logBase, "执行 插入 语句 HQL！")
        sparkSession.sql(
            """
              |insert overwrite table dim.project_due_bill_no partition(project_id,import_id)
              |select
              |  project_due.due_bill_no,
              |  project_due.related_project_id,
              |  project_due.related_date,
              |  coalesce(
              |    loan_info.product_id,
              |    project_due.related_project_id,
              |    project_due.project_id
              |  ) as partition_id,
              |  project_due.project_id,
              |  project_due.import_id
              |from project_due
              |left join (
              |  select
              |    product_id,
              |    due_bill_no
              |  from ods.loan_info
              |  where 1 > 0
              |    and product_id in ('002201','002202','002203','001601','001602','001603','DIDI201908161538')
              |) as loan_info
              |on project_due.due_bill_no = loan_info.due_bill_no
            """.stripMargin)
        logger.info(logBase, "插入 语句执行 完成！")
    }

    override def updateData(): Unit = {
        logger.info(logBase, "执行 更新 语句！")

        logger.info(logBase, "对于表 project_due_bill_no 不应该存在 更新 操作！并抛出异常！")
        throw new IllegalArgumentException("对于表 project_due_bill_no 不应该存在 更新 操作！")
    }

    override def deleteData(): Unit = {
        logger.info(logBase, "执行 删除 语句！")

        logger.info(logBase, "开始执行 删除 语句！")
        data.select(lit("project_id"), lit("import_id")).collect().foreach(
            row => {
                val fieldsMap = row.getValuesMap[Any](List("project_id", "import_id"))
                val projectId = fieldsMap("project_id").toString
                val importId = fieldsMap("import_id").toString
                sparkSession.sql(s"ALTER TABLE dim.project_due_bill_no DROP IF EXISTS PARTITION (project_id = '$projectId',import_id = '$importId')")
            }
        )
        logger.info(logBase, "删除 语句执行 完成！")
    }
}

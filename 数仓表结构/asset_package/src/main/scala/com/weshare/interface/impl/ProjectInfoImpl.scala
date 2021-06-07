package com.weshare.interface.impl

import com.weshare.interface.AssetFile
import org.apache.spark.sql.functions.lit

/**
  * @author ximing.wei 2021-05-27 14:50:07
  */
class ProjectInfoImpl extends AssetFile {

    override def insertData(): Unit = {
        logger.info(logBase, "执行 插入 语句！")

        logger.info(logBase, "将获取的数据，按字段创建临时表  project_info_tmp ！")
        data.select(
            lit("project_id"),
            lit("project_name"),
            lit("project_stage"),
            lit("asset_side"),
            lit("fund_side"),
            lit("year"),
            lit("term"),
            lit("remarks"),
            lit("project_full_name"),
            lit("asset_type"),
            lit("project_type"),
            lit("mode"),
            lit("project_time"),
            lit("project_begin_date"),
            lit("project_end_date"),
            lit("asset_pool_type"),
            lit("public_offer"),
            lit("data_source"),
            lit("create_user"),
            lit("create_time"),
            lit("update_time")
        ).createOrReplaceTempView("project_info_tmp")

        logger.info(logBase, "执行 插入 语句 HQL！")
        sparkSession.sql(
            """
              |insert overwrite table dim.project_info partition(project_id)
              |select
              |  project_name,
              |  project_stage,
              |  asset_side,
              |  fund_side,
              |  year,
              |  term,
              |  remarks,
              |  project_full_name,
              |  asset_type,
              |  project_type,
              |  mode,
              |  project_time,
              |  project_begin_date,
              |  project_end_date,
              |  asset_pool_type,
              |  public_offer,
              |  data_source,
              |  create_user,
              |  create_time,
              |  update_time,
              |  project_id
              |from project_info_tmp
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

        logger.info(logBase, "对于表 project_info 不应该存在 删除 操作！并抛出异常！")
        throw new IllegalArgumentException("对于表 project_info 不应该存在 删除 操作！")
    }
}

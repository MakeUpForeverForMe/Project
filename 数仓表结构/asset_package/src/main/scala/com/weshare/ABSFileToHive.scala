package com.weshare

import com.weshare.entity.Args.{logBase, projectId, rowType, tableName}
import com.weshare.entity.InitFields
import com.weshare.interface.AssetFile
import com.weshare.interface.impl.{BagDueImpl, BagInfoImpl, ProjectDueImpl, ProjectInfoImpl}
import org.slf4j.LoggerFactory

/**
  * @author ximing.wei 2021-05-27 14:21:33
  */
object ABSFileToHive {
    private val logger = LoggerFactory.getLogger(this.getClass)
    private var assetFiles: AssetFile = _

    def main(args: Array[String]): Unit = {
        logger.info(logBase, "ABSFileToHive 开始")

        System.setProperty("HADOOP_USER_NAME", "hadoop")

        logger.info(logBase, "判断程序传入的参数个数是否正确！")
        if (args.length != 3) throw new IllegalArgumentException("参数数量不为 3 个！第一个为文件名，第二个为项目编号，第三个为操作类型。")

        logger.info(logBase, "将传入的参数赋值给 Args 对象")
        tableName = args(0)
        projectId = args(1)
        rowType = args(2)

        logger.info(logBase, s"校验 项目编号 $projectId 是否为空！")
        if (projectId == null || projectId.trim == "") throw new RuntimeException("项目编号参数不应该为空！")

        logger.info(logBase, s"校验 文件名 $tableName 并创建相应的对象并获取数据！")
        tableName match {
            case InitFields.projectInfo => assetFiles = new ProjectInfoImpl()
            case InitFields.projectDue => assetFiles = new ProjectDueImpl()
            case InitFields.bagInfo => assetFiles = new BagInfoImpl()
            case InitFields.bagDue => assetFiles = new BagDueImpl()
            case _ => throw new RuntimeException(s"文件名参数（$tableName）不符合要求${InitFields.fileNameList}！")
        }

        logger.info(logBase, s"校验 操作类型 $rowType 是否符合要求，并执行相应的操作！")
        rowType match {
            case InitFields.insert => assetFiles.insertData()
            case InitFields.update => assetFiles.updateData()
            case InitFields.delete => assetFiles.deleteData()
            case _ => throw new RuntimeException(s"未匹配的场景！请添加新的场景（$tableName@$projectId-$rowType)")
        }
        logger.info(logBase, s"程序 $tableName@$projectId-$rowType 执行完成！")
    }
}

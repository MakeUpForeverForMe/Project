package com.weshare.util

/**
  * @author ximing.wei 2021-05-27 14:26:08
  */
object FileCheckUtils {
    private val fileNameList: List[String] = List("project_info", "project_due_bill_no", "bag_info", "bag_due_bill_no")
    private val rowTypeList: List[String] = List("insert", "delete", "update")

    /**
      * 校验文件名
      *
      * @param fileName 文件名
      * @return 成功返回 true ，失败抛出异常
      */
    def checkFileName(fileName: String): Boolean = {
        if (fileName == null || fileName.trim == "") throw new RuntimeException("文件名参数不应该为空！")
        if (!fileNameList.contains(fileName)) throw new RuntimeException("文件名参数（" + fileName + "）不符合要求" + fileNameList + "！")
        true
    }

    /**
      * 校验项目编号
      *
      * @param fileId 项目编号
      * @return 成功返回 true ，失败抛出异常
      */
    def checkFileId(fileId: String): Boolean = {
        if (fileId == null || fileId.trim == "") throw new RuntimeException("项目编号参数不应该为空！")
        true
    }

    /**
      * 校验操作类型
      *
      * @param rowType 操作类型
      * @return 成功返回 true ，失败抛出异常
      */
    def checkRowType(rowType: String): Boolean = {
        if (rowType == null || rowType.trim == "") throw new RuntimeException("操作类型参数不应该为空！")
        if (!rowTypeList.contains(rowType)) throw new RuntimeException("操作类型参数（" + rowType + "）不符合要求" + rowTypeList + "！")
        true
    }
}

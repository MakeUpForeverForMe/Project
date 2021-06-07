package com.weshare.entity

/**
  * @author ximing.wei 2021-05-27 21:06:55
  */

object InitFields {
    val projectInfo: String = "project_info"
    val projectDue: String = "project_due_bill_no"
    val bagInfo: String = "bag_info"
    val bagDue: String = "bag_due_bill_no"

    val insert: String = "insert"
    val delete: String = "delete"
    val update: String = "update"

    val fileNameList: List[String] = List(projectInfo, projectDue, bagInfo, bagDue)
    val rowTypeList: List[String] = List(insert, delete, update)
}

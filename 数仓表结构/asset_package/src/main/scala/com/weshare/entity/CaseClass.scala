package com.weshare.entity

/**
  * @author ximing.wei 2021-05-27 14:52:24
  */
object InitFields {
    val projectInfo: String = "project_info"
    val project_due_bill_no: String = "project_due_bill_no"
    val bag_info: String = "bag_info"
    val bag_due_bill_no: String = "bag_due_bill_no"

    val insert: String = "insert"
    val delete: String = "delete"
    val update: String = "update"

    val fileNameList: List[String] = List(projectInfo, "project_due_bill_no", "bag_info", "bag_due_bill_no")
    val rowTypeList: List[String] = List("insert", "delete", "update")
}


case class ProjectInfo(
                              projectId: String,
                              projectName: String,
                              projectStage: String,
                              assetSide: String,
                              fundSide: String,
                              year: String,
                              term: String,
                              projectFullName: String,
                              assetType: String,
                              projectType: String,
                              mode: String,
                              projectTime: String,
                              projectBeginDate: String,
                              projectEndDate: String,
                              assetPoolType: String,
                              publicOffer: String,
                              dataSource: String,
                              createUser: String,
                              createTime: String,
                              updateTime: String,
                              remarks: String
                      )

case class ProjectDueBillNo(
                                   row_type: String,
                                   project_id: String,
                                   import_id: String,
                                   due_bill_no: List[ProjectDueBillNoHelper]
                           )

case class ProjectDueBillNoHelper(
                                         relatedDate: String,
                                         relatedProjectId: String,
                                         serialNumber: String
                                 )
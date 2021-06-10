package com.weshare.repair_data.mode

/**
 * created by chao.guo on 2020/11/26
 **/
case class RepairMode(
                     id:String,
                      due_bill_no:String,
                      register_date:String,
                      active_date:String,
                      var repair_flag:java.lang.Integer,
                      var repair_date:String,
                      var p_type :String
                     ){
  override def toString: String = {
    s"""
      |借据号:${due_bill_no},注册日期:${register_date},放款日期:${active_date},平移资产数据:${register_date}->${active_date} \n
      |""".stripMargin


  }
}

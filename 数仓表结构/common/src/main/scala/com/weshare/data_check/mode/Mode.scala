package com.weshare.data_check.mode

/**
 * created by chao.guo on 2021/5/10
 **/
object Mode {


  case class DataWatchRule(

                            var task_name:String,
                            mode_name:String,
                            var param_list:String,
                            var sql1:String,
                            var sql2:String,
                            engine_type:String,
                            recivers_email:String,
                            reboot_ids:String
                          )



  case class RebootPerson(id :Int,hookurl:String,isEnable:Int) // isEnable 1 发送 0 不发送
}

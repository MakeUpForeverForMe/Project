package com.travel.utils

import com.travel.loggings.Logging


/**
  * Created by laowang
  */
object ReflectBean extends Logging{

  def reflect(data: Any , tableName:String):Map[String , String] ={

    tableName match{
        //订单表
      case tableName if(GlobalConfigUtils.getProp("syn.table.order_info").equals(tableName) || GlobalConfigUtils.getProp("syn.table.order_info") == tableName) =>
        val clazz = Class.forName("com.travel.bean.OrderInfo")
        val fields = clazz.getDeclaredFields
        var map = Map[String , String]()
        try{
          if (fields != null && fields.size > 0) {
            for (field <- fields) {
              field.setAccessible(true)
              val column = field.getName//列名
              val declaredField = data.getClass.getDeclaredField(column)
              //取消java的安全检查，通过反射去改变字段值的时候，需要将这个属性设置为true，表示允许我们去改变字段值
              declaredField.setAccessible(true)
              val value = declaredField.get(data).toString
              map += (column -> value)
            }
          }
        }catch {
          case e:Exception =>
            error("##################################################################")
            info("**********订单表，处理数据出错********** :  " , data)
            error("##################################################################")
        }
        map
        //用户表
      case tableName if(GlobalConfigUtils.getProp("syn.table.renter_info").equals(tableName) || GlobalConfigUtils.getProp("syn.table.renter_info") == tableName) =>
        val clazz = Class.forName("com.travel.bean.RegisterUsers")
        val fields = clazz.getDeclaredFields
        var map = Map[String , String]()
        try{
          if (fields != null && fields.size > 0) {
            for (field <- fields) {
              field.setAccessible(true)
              val column = field.getName//列明
              val declaredField = data.getClass.getDeclaredField(column)
              declaredField.setAccessible(true)
              val value = declaredField.get(data).toString
              map += (column -> value)
            }
          }
        }catch {
          case e:Exception =>
            error("##################################################################")
            info("**********用户表，处理数据出错********** :  " , data)
            error("##################################################################")
        }
        map
        //司机表
      case tableName if(GlobalConfigUtils.getProp("syn.table.driver_info").equals(tableName) || GlobalConfigUtils.getProp("syn.table.driver_info") == tableName) =>
        val clazz = Class.forName("com.travel.bean.DriverInfo")
        val fields = clazz.getDeclaredFields
        var map = Map[String , String]()
        try{
          if (fields != null && fields.size > 0) {
            for (field <- fields) {
              field.setAccessible(true)
              val column = field.getName//列明
              val declaredField = data.getClass.getDeclaredField(column)
              declaredField.setAccessible(true)
              val value = declaredField.get(data).toString
              map += (column -> value)
            }
          }
        }catch {
          case e:Exception =>
            error("##################################################################")
            info("**********司机表，处理数据出错********** :  " , data)
            error("##################################################################")
        }
        map
        //加盟表
      case tableName if(GlobalConfigUtils.getProp("syn.table.opt_alliance_business").equals(tableName) || GlobalConfigUtils.getProp("syn.table.opt_alliance_business") == tableName) =>
        val clazz = Class.forName("com.travel.bean.Opt_alliance_business")
        val fields = clazz.getDeclaredFields
        var map = Map[String , String]()
        try{
          if (fields != null && fields.size > 0) {
            for (field <- fields) {
              field.setAccessible(true)
              val column = field.getName//列明
              val declaredField = data.getClass.getDeclaredField(column)
              declaredField.setAccessible(true)
              val value = declaredField.get(data).toString
              map += (column -> value)
            }
          }
        }catch {
          case e:Exception =>
            error("##################################################################")
            info("**********加盟表，处理数据出错********** :  " , data)
            error("##################################################################")
        }
        map


    }




  }
}

package com.mine.test.util

import com.mine.date.DateFormat
import com.mine.jdbc.dao.BaseDAO
import com.mine.jdbc.utils.JDBCUtils
import com.mine.test.bean.Customer
import org.junit.Test

class UtilTest extends BaseDAO[Customer] {

    @Test
    def testJdbcUtils(): Unit = {
        val connection = JDBCUtils.getConnectionByFile("config.properties")
        println(connection)
    }

    @Test
    def testUpdate(): Unit = {
//        println(if (daoUpdate(JDBCUtils.getConnectionByFile("config.properties"), "alter table client_info change column `_id` `id` varchar(255);")) "修改列名成功" else "修改列名失败")
        println(if (daoUpdate(JDBCUtils.getConnectionByFile("config.properties"), "alter table client_info change column `id` `_id` varchar(255);")) "修改列名成功" else "修改列名失败")
    }

    @Test
    def testGetValue(): Unit = {
        println(daoGetValue(JDBCUtils.getConnectionByFile("config.properties"), "select count(1) from client_info"))
    }

    @Test
    def testCustomerGetDataOne(): Unit = {
        val customer = daoGetDataOne(JDBCUtils.getConnectionByFile("config.properties"), "select `_id` as id,cTime,uTime,birthDate,sex,city,expectation,province,sourceChannel from client_info where `_id` = '5ced41ffef6f9c4aaf5a9d3f14087026'")
        customer.cTime = DateFormat.dt_2_ft(customer.cTime)
        customer.uTime = DateFormat.dt_2_ft(customer.uTime)
        println(customer)
    }

    @Test
    def testCustomerGetDataList(): Unit = {
        daoGetDataList(JDBCUtils.getConnectionByFile("config.properties"), "select `_id` as id,cTime,uTime,birthDate,sex,city,expectation,province,sourceChannel from client_info").map {
            customer: Customer =>
                customer.cTime = DateFormat.dt_2_ft(customer.cTime)
                customer.uTime = DateFormat.dt_2_ft(customer.uTime)
                customer
        }.foreach(println)
    }
}

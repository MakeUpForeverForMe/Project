package com.mine.dao

import java.sql.DriverManager
import java.text.SimpleDateFormat

import com.mine.dao.bean.Customer
import com.mine.dao.dao.BaseDAO
import com.mine.dao.jdbc.{JDBCUtils, PropertyFields}
import com.mine.date.DateFormat
import org.junit.Test

class DAOTest extends BaseDAO[Customer] {

    @Test
    def testJdbc(): Unit = {
        Class.forName("com.mysql.jdbc.Driver")
        val connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/dm_cf?useSSL=false&useUnicode=true&characterEncoding=utf8", "root", "password")
        println(connection)
    }

    @Test
    def testJdbcUtils(): Unit = {
        val connection = JDBCUtils.getConnection(PropertyFields.MYSQL_DRIVER, PropertyFields.MYSQL_URL)
        println(connection)
    }

    @Test
    def testUpdate(): Unit = {

    }

    @Test
    def testGetValue(): Unit = {
        println(getValue(JDBCUtils.getConnection(PropertyFields.MYSQL_DRIVER, PropertyFields.MYSQL_URL), "select count(1) from client_info"))
    }

    @Test
    def testCustomerGetDataOne(): Unit = {
        val customer = getDataOne(JDBCUtils.getConnection(PropertyFields.MYSQL_DRIVER, PropertyFields.MYSQL_URL), "select `_id` as id,cTime,uTime,birthDate,sex,city,expectation,province,sourceChannel from client_info where `_id` = '5ced41ffef6f9c4aaf5a9d3f14087026'")
        customer.cTime = DateFormat.dt_2_ft(customer.cTime)
        customer.uTime = DateFormat.dt_2_ft(customer.uTime)
        println(customer)
    }

    @Test
    def testCustomerGetDataList(): Unit = {
        getDataList(JDBCUtils.getConnection(PropertyFields.MYSQL_DRIVER, PropertyFields.MYSQL_URL), "select `_id` as id,cTime,uTime,birthDate,sex,city,expectation,province,sourceChannel from client_info").map {
            customer: Customer =>
                customer.cTime = DateFormat.dt_2_ft(customer.cTime)
                customer.uTime = DateFormat.dt_2_ft(customer.uTime)
                customer
        }.foreach(println)
    }
}

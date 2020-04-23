package com.mine.test.util

import com.mine.dateutil.DateFormat
import com.mine.jdbcutil.dao.BaseDAO
import com.mine.jdbcutil.utils.JDBCUtils
import com.mine.propertyutil.PropertyUtil
import com.mine.test.bean.Customer
import org.junit.Test

class UtilTest extends BaseDAO[Customer] {

    private val props = PropertyUtil("config.properties")


    @Test
    def test1(): Unit = {
        println(props.getProps("mysql.driver"))
    }

    @Test
    def test2(): Unit = {
        println(props.MYSQL_DRIVER)
    }

    @Test
    def test3(): Unit = {
        println(props.MYSQL_URL)
    }

    @Test
    def testJdbcUtils(): Unit = {
        val connection = JDBCUtils.getConnection(props)
        println(connection)
    }

    @Test
    def testUpdate(): Unit = {
        println(if (daoUpdate(JDBCUtils.getConnection(props), "alter table client_info change column `_id` `id` varchar(255);")) "修改列名成功" else "修改列名失败")
        println(if (daoUpdate(JDBCUtils.getConnection(props), "alter table client_info change column `id` `_id` varchar(255);")) "修改列名成功" else "修改列名失败")
    }

    @Test
    def testGetValue(): Unit = {
        println(daoGetValue(JDBCUtils.getConnection(props), "select count(1) from client_info"))
    }

    @Test
    def testCustomerGetDataOne(): Unit = {
        val customer = daoGetDataOne(JDBCUtils.getConnection(props), "select `_id` as id,cTime,uTime,birthDate,sex,city,expectation,province,sourceChannel from client_info where `_id` = '5ced41ffef6f9c4aaf5a9d3f14087026'")
        customer.cTime = DateFormat.dt_2_ft(customer.cTime)
        customer.uTime = DateFormat.dt_2_ft(customer.uTime)
        println(customer)
    }

    @Test
    def testCustomerGetDataList(): Unit = {
        daoGetDataList(JDBCUtils.getConnection(props), "select `_id` as id,cTime,uTime,birthDate,sex,city,expectation,province,sourceChannel from client_info").map {
            customer: Customer =>
                customer.cTime = DateFormat.dt_2_ft(customer.cTime)
                customer.uTime = DateFormat.dt_2_ft(customer.uTime)
                customer
        }.foreach(println)
    }
}

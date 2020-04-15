package com.mine.flinkKafaka.mysql2kafka

import com.mine.flinkKafaka.mysql2kafka.bean.Customer
import com.mine.jdbcutil.dao.BaseDAO
import com.mine.jdbcutil.utils.JDBCUtils
import com.mine.kafkautil.KafkaUtil
import com.mine.propertyutil.PropertyUtil

object MySQL2KafkaProducer extends BaseDAO[Customer] {
    private lazy val props = PropertyUtil("conf.properties")

    def main(args: Array[String]): Unit = {
        // 获取 MySQL 数据
        val connection = JDBCUtils.getConnection(props)
        // val customer = daoGetDataList(connection,
        val customer = daoGetDataOne(connection,
            """
              |select
              |  keywords
              |from drip_loan_binlog_filter
              |limit 1;
            """.stripMargin)
        //        println(customer)
        println(customer.keywords)
        // customer.foreach(item => println(item.keywords))
        // 发送到 Kafka
        //        KafkaUtil(props).producerSendMsg[String, String](customer.keywords)
    }
}

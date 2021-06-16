package cn.mine

import cn.mine.dao.DAO
import cn.mine.flinkKafaka.mysql2kafka.bean.Customer
import org.junit.jupiter.api.Test


/**
  *
  * @author ximing.wei
  */
class GetMySQLValueToKafka extends DAO[Customer] {
    private lazy val props = ConfigUtil("conf.properties")

    // 测试环境 旧
    @Test
    def test1(): Unit = {
        // SQL
        val sql =
            """
              |select
              |  keywords
              |from binlog_filter
              |limit 1;
            """.stripMargin
        // 获取 MySQL 数据
        val customer = daoGetDataOne(JDBCUtils.getConnection(props), sql)
        if (customer != null) println(customer) else System.exit(1)
    }

    // 测试环境 新
    @Test
    def test2(): Unit = {
        // SQL
        val sql =
            """
              |select
              |  id,server_id,black_databases,black_tables,create_time
              |from black_binlog_config
              |limit 1;
            """.stripMargin
        // 获取 MySQL 数据
        val customer = daoGetDataOne(JDBCUtils.getConnection(props), sql)
        if (customer != null) println(customer) else System.exit(1)
    }

    // 公司电脑
    @Test
    def test3(): Unit = {
        // SQL
        val sql =
            """
              |select
              |  `_id` as id,cTime,uTime,birthday,sex,city,expectation,province,sourceChannel
              |from client_info
              |limit 1;
            """.stripMargin
        // 获取 MySQL 数据
        val customer = daoGetDataOne(JDBCUtils.getConnection(props), sql)
        if (customer != null) println(customer) else System.exit(1)
    }

    def kafkaProducer(customer: Customer): Unit = {
        // 发送到 Kafka
        KafkaUtil(props).producerSendMsg[String, String](customer.keywords)
    }

}
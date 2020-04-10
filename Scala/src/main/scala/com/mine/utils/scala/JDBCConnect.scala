package com.mine.utils.scala

import java.sql.{Connection, DriverManager, Statement}

import org.junit.{After, Before, Test}

object JDBCConnect extends JDBCConnect

sealed class JDBCConnect {
  private val DRIVER = "oracle.jdbc.driver.OracleDriver"
  private val HOST = "10.83.16.3"
  private val USER = "ABSBANK"
  private val PASSWD = "absbank"
  private val DATABASE = "orcl"
  private val JDBC_URL = s"jdbc:oracle:thin:@$HOST:1521:$DATABASE"

  private var connection: Connection = _
  private var statement: Statement = _


  @Before
  def init() {
    Class.forName(DRIVER)
    connection = DriverManager.getConnection(JDBC_URL, USER, PASSWD)
    statement = connection.createStatement()
  }

  @After
  def close() {
    statement.close()
    connection.close()
  }

  @Test
  def showTables(): Unit = {
    println(statement.executeQuery("select * from DUAL"))
  }
}

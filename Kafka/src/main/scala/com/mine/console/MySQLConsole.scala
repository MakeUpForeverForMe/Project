package com.mine.console

import java.sql.{Connection, DriverManager, Statement}

import com.mine.propertyprepare.PropertyPrepare


object MySQLConsole extends MySQLConsole

sealed class MySQLConsole {
    def getConnection(driver: String, urlWithNameAndPassword: String): Connection = {
        Class.forName(driver)
        DriverManager.getConnection(urlWithNameAndPassword)
    }

    def executeSelect(connection: Connection, sql: String, args: String*) = {
        val prepareStatement = if (args.isEmpty) connection.prepareStatement(sql) else connection.prepareStatement(sql, args.toArray)
        val resultSet = prepareStatement.executeQuery()
        val metaData = resultSet.getMetaData
        for (i <- 0 until args.length) prepareStatement.setObject(i + 1, args(i))
        val columnList = (for (i <- 1 to metaData.getColumnCount) yield metaData.getColumnLabel(i)).toList

        //        while (resultSet.next()) {
        //            println(resultSet.getObject(metaData.getColumnLabel(i)))
        //        }
    }

    def close(connection: Connection, statement: Statement): Unit = {
        close(statement)
        close(connection)
    }

    def close(statement: Statement): Unit = if (null != statement) statement.close()

    def close(connection: Connection): Unit = if (null != connection) connection.close()
}

object ConsoleTest {
    def main(args: Array[String]): Unit = {
        // val connection = MySQLConsole.connection(PropertyPrepare.MYSQL_DRIVER, PropertyPrepare.MYSQL_URL)
        val connection = MySQLConsole.getConnection(PropertyPrepare.MYSQL_DRIVER, "jdbc:mysql://localhost:3306/dm_cf?user=root&password=password&characterEncoding=utf8&useSSL=false")
        MySQLConsole.executeSelect(connection, "select cTime,sex,city from client_info")
    }
}
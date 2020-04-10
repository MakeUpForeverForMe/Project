package com.mine.console

import java.sql.{Connection, DriverManager, Statement}

import com.mine.propertyprepare.PropertyPrepare


object MySQLConsole extends MySQLConsole

sealed class MySQLConsole {
    def connection(driver: String, urlWithNameAndPassword: String): Connection = {
        Class.forName(driver)
        DriverManager.getConnection(urlWithNameAndPassword)
    }

    def executeSelect(connection: Connection, sql: String, args: String*) = {
        val ppStatement = if (args.isEmpty) connection.prepareStatement(sql) else connection.prepareStatement(sql, args.toArray)
        ppStatement.executeQuery()
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
        val connection = MySQLConsole.connection(PropertyPrepare.MYSQL_DRIVER, PropertyPrepare.MYSQL_URL)

    }
}
package com.mine.console

import java.sql.{Connection, PreparedStatement, Statement}

trait MySQLConsoleTrait {

    def connect(driver: String, urlWithNameAndPassword: String): Connection

    def executeSQL(connection: Connection, sql: String, args: String*)

    def close(connection: Connection, statement: Statement): Unit

    def close(statement: Statement): Unit

    def close(connection: Connection): Unit
}

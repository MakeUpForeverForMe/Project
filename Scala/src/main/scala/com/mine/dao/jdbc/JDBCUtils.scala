package com.mine.dao.jdbc

import java.sql._

object JDBCUtils {

    def getConnection(driver: String, urlWithNameAndPassword: String): Connection = {
        try {
            Class.forName(driver)
            return DriverManager.getConnection(urlWithNameAndPassword)
        } catch {
            case e: ClassNotFoundException => e.printStackTrace()
            case e: SQLException => e.printStackTrace()
        }
        null
    }

    def close(c: Connection, s: Statement): Unit = {
        if (s != null) try s.close() catch {
            case e: SQLException => e.printStackTrace()
        }
        if (c != null) try c.close() catch {
            case e: SQLException => e.printStackTrace()
        }
    }

    def close(c: Connection, s: Statement, r: ResultSet): Unit = {
        if (r != null) try r.close() catch {
            case e: SQLException => e.printStackTrace()
        }
        if (s != null) try s.close() catch {
            case e: SQLException => e.printStackTrace()
        }
        if (c != null) try c.close() catch {
            case e: SQLException => e.printStackTrace()
        }
    }
}

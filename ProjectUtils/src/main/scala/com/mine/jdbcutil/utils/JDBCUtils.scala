package com.mine.jdbcutil.utils

import java.sql._

import com.mine.propertyutil.PropertyUtil

object JDBCUtils {

    def getConnection(props: PropertyUtil): Connection = {
        try {
            Class.forName(props.MYSQL_DRIVER)
            return DriverManager.getConnection(props.MYSQL_URL)
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

package com.mine.jdbcutil.utils

import java.sql._

import com.mine.propertyutil.ConfigUtil

object JDBCUtils {

  def getConnection(props: ConfigUtil): Connection = {
    try {
      Class.forName(props.MYSQL_DRIVER)
      return DriverManager.getConnection(props.MYSQL_URL, props.MYSQL_USER, props.MYSQL_PASSWORD)
    } catch {
      case e: ClassNotFoundException => e.printStackTrace()
      case e: SQLException => e.printStackTrace()
    }
    null
  }

  def close(c: Connection, s: Statement, r: ResultSet = null): Unit = {
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

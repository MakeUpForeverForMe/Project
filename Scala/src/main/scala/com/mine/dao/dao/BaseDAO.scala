package com.mine.dao.dao

import java.lang.reflect.{Field, ParameterizedType}
import java.sql._

import com.mine.dao.jdbc.JDBCUtils

import scala.collection.mutable

abstract class BaseDAO[T >: Null] {
    private val clazz: Class[T] = this.getClass.getGenericSuperclass.asInstanceOf[ParameterizedType].getActualTypeArguments()(0).asInstanceOf[Class[T]]

    /**
      * 通用的增删改（考虑了事务）
      *
      * @param connection 传入连接
      * @param sql        传入要执行的 sql
      * @param args       传入要执行的 sql 中的占位符对应的参数
      * @return 返回执行结果（true：成功，false：失败）
      */
    def update(connection: Connection, sql: String, args: String*): Boolean = {
        var i: Int = 0
        var preparedStatement: PreparedStatement = null
        try {
            // 通过连接获取 PreparedStatement
            preparedStatement = connection.prepareStatement(sql)
            // 对 sql 中的占位符进行填充
            for (i <- 0 until args.length) preparedStatement.setObject(i + 1, args(i))
            // 执行语句
            i = preparedStatement.executeUpdate()
        } catch {
            case e: SQLException => e.printStackTrace()
        } finally JDBCUtils.close(null, preparedStatement)
        if (0 == i) true else false
    }

    /**
      * 通用的查，返回一条数据的情况（考虑了事务）
      *
      * @param connection 传入连接
      * @param sql        传入要执行的 sql
      * @param args       传入要执行的 sql 中的占位符对应的参数
      * @return 返回执行结果
      */
    def getDataOne(connection: Connection, sql: String, args: Any*): T = {
        var preparedStatement: PreparedStatement = null
        var resultSet: ResultSet = null
        try {
            // 通过连接获取 PreparedStatement
            preparedStatement = connection.prepareStatement(sql)
            // 对 sql 中的占位符进行填充
            for (i <- 0 until args.length) preparedStatement.setObject(i + 1, args(i))
            // 获取执行结果（一条结果）
            resultSet = preparedStatement.executeQuery()
            // 获取元数据
            val metaData: ResultSetMetaData = preparedStatement.getMetaData
            // 一条结果填写到实体类中
            if (resultSet.next()) {
                // 获取实体类
                val t: T = clazz.newInstance()
                // 根据每个字段分别进行设置
                for (i <- 0 until metaData.getColumnCount) {
                    // 按列获取各个字段值
                    val columnValue: AnyRef = resultSet.getObject(i + 1)
                    // 获取各个字段的名称（别名）
                    val columnLabel: String = metaData.getColumnLabel(i + 1)
                    // 获取实体类中的声明字段
                    val label: Field = clazz.getDeclaredField(columnLabel)
                    label.setAccessible(true)
                    // 将字段值设置到实体类中
                    label.set(t, columnValue)
                }
                return t
            }
        } catch {
            case e: SQLException => e.printStackTrace()
        } finally JDBCUtils.close(null, preparedStatement, resultSet)
        null
    }

    /**
      * 通用的查，返回多条数据的情况（考虑了事务）
      *
      * @param connection 传入连接
      * @param sql        传入要执行的 sql
      * @param args       传入要执行的 sql 中的占位符对应的参数
      * @return 返回执行结果
      */
    def getDataList(connection: Connection, sql: String, args: Any*): List[T] = {
        var preparedStatement: PreparedStatement = null
        var resultSet: ResultSet = null
        try {
            // 通过连接获取 PreparedStatement
            preparedStatement = connection.prepareStatement(sql)
            // 对 sql 中的占位符进行填充
            for (i <- 0 until args.length) preparedStatement.setObject(i + 1, args(i))
            // 获取执行结果（一条结果）
            resultSet = preparedStatement.executeQuery()
            // 获取元数据
            val metaData: ResultSetMetaData = preparedStatement.getMetaData
            val list: mutable.ListBuffer[T] = mutable.ListBuffer[T]()
            // 一条结果填写到实体类中
            while (resultSet.next()) {
                // 获取实体类
                val t: T = clazz.newInstance()
                // 根据每个字段分别进行设置
                for (i <- 0 until metaData.getColumnCount) {
                    // 按列获取各个字段值
                    val columnValue: AnyRef = resultSet.getObject(i + 1)
                    // 获取各个字段的名称（别名）
                    val columnLabel: String = metaData.getColumnLabel(i + 1)
                    // 获取实体类中的声明字段
                    val label: Field = clazz.getDeclaredField(columnLabel)
                    label.setAccessible(true)
                    // 将字段值设置到实体类中
                    label.set(t, columnValue)
                }
                list += t
            }
            return list.toList
        } catch {
            case e: SQLException => e.printStackTrace()
        } finally JDBCUtils.close(null, preparedStatement, resultSet)
        null
    }

    /**
      * 特殊查询，只返回一行一列（考虑了事务）
      *
      * @param connection 传入连接
      * @param sql        传入要执行的 sql
      * @param args       传入要执行的 sql 中的占位符对应的参数
      * @return 返回特殊查询的值
      */
    def getValue(connection: Connection, sql: String, args: Any*): T = {
        var preparedStatement: PreparedStatement = null
        var resultSet: ResultSet = null
        try {
            // 通过连接获取 PreparedStatement
            preparedStatement = connection.prepareStatement(sql)
            // 对 sql 中的占位符进行填充
            for (i <- 0 until args.length) preparedStatement.setObject(i + 1, args(i))
            // 执行语句
            resultSet = preparedStatement.executeQuery()
            if (resultSet.next()) return resultSet.getObject(1).asInstanceOf[T]
        } catch {
            case e: SQLException => e.printStackTrace()
        } finally JDBCUtils.close(null, preparedStatement, resultSet)
        null
    }
}

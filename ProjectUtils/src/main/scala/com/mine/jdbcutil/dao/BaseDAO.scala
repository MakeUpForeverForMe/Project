package com.mine.jdbcutil.dao

import java.lang.reflect.{Field, ParameterizedType}
import java.sql._

import com.mine.jdbcutil.utils.JDBCUtils

import scala.collection.mutable

/**
  * 数据（库）访问对象
  * DAO：data(base) access  object
  *
  * 只有查的时候需要泛型
  *
  * @tparam T 要操作的对象
  */
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
    def daoUpdate(connection: Connection, sql: String, args: String*): Boolean = {
        var i: Int = -1
        var preparedStatement: PreparedStatement = null
        try {
            // 通过连接获取 PreparedStatement
            preparedStatement = connection.prepareStatement(sql)
            // 对 sql 中的占位符进行填充
            for (i <- 0 until args.length) preparedStatement.setObject(i + 1, args(i))
            // 执行语句
            i = preparedStatement.executeUpdate()
        } catch {
            case e: Exception => e.printStackTrace()
        } finally JDBCUtils.close(null, preparedStatement)
        if (0 == i) true else false
    }

    /**
      * 特殊查询，只返回一行一列（考虑了事务）
      *
      * @param connection 传入连接
      * @param sql        传入要执行的 sql
      * @param args       传入要执行的 sql 中的占位符对应的参数
      * @return 返回特殊查询的值
      */
    def daoGetValue(connection: Connection, sql: String, args: Any*): T = {
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
            case e: Exception => e.printStackTrace()
        } finally JDBCUtils.close(null, preparedStatement, resultSet)
        null
    }

    /**
      * 通用的查，返回一条数据的情况（考虑了事务）
      *
      * @param connection 传入连接
      * @param sql        传入要执行的 sql
      * @param args       传入要执行的 sql 中的占位符对应的参数
      * @return 返回执行结果
      */
    def daoGetDataOne(connection: Connection, sql: String, args: Any*): T = {
        var preparedStatement: PreparedStatement = null
        var resultSet: ResultSet = null
        try {
            // 通过连接获取 PreparedStatement
            preparedStatement = connection.prepareStatement(sql)
            // 对 sql 中的占位符进行填充
            for (i <- 0 until args.length)
                // 判断是否有占位符，有则添加，无则跳过
                if (0 != preparedStatement.getParameterMetaData.getParameterCount)
                    preparedStatement.setObject(i + 1, args(i))
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
            case e: Exception => e.printStackTrace()
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
    def daoGetDataList(connection: Connection, sql: String, args: Any*): List[T] = {
        var preparedStatement: PreparedStatement = null
        var resultSet: ResultSet = null
        try {
            // 通过连接获取 PreparedStatement
            preparedStatement = connection.prepareStatement(sql)
            // 对 sql 中的占位符进行填充
            for (i <- 0 until args.length)
                // 判断是否有占位符，有则添加，无则跳过
                if (0 != preparedStatement.getParameterMetaData.getParameterCount)
                    preparedStatement.setObject(i + 1, args(i))
            // 获取执行结果（多条结果）
            resultSet = preparedStatement.executeQuery()
            // 获取元数据
            val metaData: ResultSetMetaData = preparedStatement.getMetaData
            val list: mutable.ListBuffer[T] = mutable.ListBuffer[T]()
            // 一条结果填写到实体类中
            while (resultSet.next()) {
                // 获取实体类
                val t: T = clazz.newInstance()
                // 根据每个字段分别进行设置
                for (i <- 0 until metaData.getColumnCount; j = i + 1) {
                    // 按列获取各个字段值
                    val columnValue: AnyRef = resultSet.getObject(j)
                    // 获取各个字段的名称（别名）
                    val columnLabel: String = metaData.getColumnLabel(j)
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
            case e: Exception => e.printStackTrace()
        } finally JDBCUtils.close(null, preparedStatement, resultSet)
        null
    }
}

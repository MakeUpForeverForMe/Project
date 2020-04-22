package com.mine

import java.sql.{Connection, DriverManager}

import org.junit.Test


class TestJDBCDemo {
    def getConnection: Connection = {
        Class.forName("com.mysql.jdbc.Driver")
        val url = "jdbc:mysql://10.83.96.10:3306/flink_properties?characterEncoding=utf8&useSSL=false"
        val user = "flink"
        val pass = "weshare2019"
        DriverManager.getConnection(url, user, pass)
    }

    @Test
    // 占位符只能在“值”的位置使用 字段位置使用占位符后，只能打印出输入的占位符的值，而不是表中字段的结果
    def main(): Unit = {
        val conn = getConnection
        val sql = "select keywords from drip_loan_binlog_filter where createTime = ?"
        val preparedStatement = conn.prepareStatement(sql)
        preparedStatement.setObject(1, "2019-09-16 14:20:51")
        val resultSet = preparedStatement.executeQuery()
        val metaData = resultSet.getMetaData

        if (resultSet.next()) {
            println(resultSet.getString(1))
        }

        println("获得所有列的数目及实际列数" + metaData.getColumnCount + "\n")

        for (i <- 1 to metaData.getColumnCount) {
            // 获得所有列的数目及实际列数
            val columnCount = metaData.getColumnCount
            // 获得指定列的列名
            val columnName = metaData.getColumnName(i)
            // 获得指定列的列值
            val columnType = metaData.getColumnType(i)
            // 获得指定列的数据类型名
            val columnTypeName = metaData.getColumnTypeName(i)
            // 所在的Catalog名字
            val catalogName = metaData.getCatalogName(i)
            // 对应数据类型的类
            val columnClassName = metaData.getColumnClassName(i)
            // 在数据库中类型的最大字符个数
            val columnDisplaySize = metaData.getColumnDisplaySize(i)
            // 默认的列的标题
            val columnLabel = metaData.getColumnLabel(i)
            // 获得列的模式
            val schemaName = metaData.getSchemaName(i)
            // 某列类型的精确度(类型的长度)
            val precision = metaData.getPrecision(i)
            // 小数点后的位数
            val scale = metaData.getScale(i)
            // 获取某列对应的表名
            val tableName = metaData.getTableName(i)
            // 是否自动递增
            val isAutoInctement = metaData.isAutoIncrement(i)
            // 在数据库中是否为货币型
            val isCurrency = metaData.isCurrency(i)
            // 是否为空
            val isNullable = metaData.isNullable(i)
            // 是否为只读
            val isReadOnly = metaData.isReadOnly(i)
            // 能否出现在where中
            val isSearchable = metaData.isSearchable(i)

            println("获得列 " + i + " 所在的 Catalog 名字（库名）:" + catalogName)
            println("获得列 " + i + " 对应的表名:" + tableName)
            println("获得列 " + i + " 的字段名称:" + columnName)
            println("获得列 " + i + " 的数据类型名:" + columnTypeName)
            println("获得列 " + i + " 对应数据类型的 Java 类:" + columnClassName)

            println("获得列 " + i + " 的类型,返回 SqlType 中的编号:" + columnType)
            println("获得列 " + i + " 在数据库中类型的最大字符个数:" + columnDisplaySize)
            println("获得列 " + i + " 的默认的列的标题:" + columnLabel)
            println("获得列 " + i + " 的模式:" + schemaName)
            println("获得列 " + i + " 类型的精确度(类型的长度):" + precision)
            println("获得列 " + i + " 小数点后的位数:" + scale)
            println("获得列 " + i + " 是否自动递增:" + isAutoInctement)
            println("获得列 " + i + " 在数据库中是否为货币型:" + isCurrency)
            println("获得列 " + i + " 是否为空:" + isNullable)
            println("获得列 " + i + " 是否为只读:" + isReadOnly)
            println("获得列 " + i + " 能否出现在 where 中:" + isSearchable)
            println()
        }
    }
}

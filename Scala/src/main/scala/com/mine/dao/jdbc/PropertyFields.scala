package com.mine.dao.jdbc

import com.mine.property.PropertyUtil

object PropertyFields {
    // 获取配置文件
    private lazy final val props = new PropertyUtil("jdbc_conf.properties")
    // MYSQL 配置
    private lazy final val MYSQL_HOST: String = props.getPropertyValueByKey("mysql.host")
    private lazy final val MYSQL_DATABASE: String = props.getPropertyValueByKey("mysql.database")
    private lazy final val MYSQL_USER: String = props.getPropertyValueByKey("mysql.user")
    private lazy final val MYSQL_PASSWORD: String = props.getPropertyValueByKey("mysql.password")
    lazy final val MYSQL_DRIVER: String = props.getPropertyValueByKey("mysql.driver")
    lazy final val MYSQL_URL: String = s"jdbc:mysql://$MYSQL_HOST:3306/$MYSQL_DATABASE?user=$MYSQL_USER&password=$MYSQL_PASSWORD&useSSL=false&useUnicode=true&characterEncoding=utf8" // serverTimezone=UTC&
}

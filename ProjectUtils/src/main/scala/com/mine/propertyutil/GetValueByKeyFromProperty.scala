package com.mine.propertyutil

object GetValueByKeyFromProperty {
    def apply(): GetValueByKeyFromProperty = new GetValueByKeyFromProperty()

    def apply(fileName: String): GetValueByKeyFromProperty = new GetValueByKeyFromProperty(fileName)
}

class GetValueByKeyFromProperty {
    // 获取配置文件
    private var props: PropertyUtil = _

    def this(fileName: String) = {
        this
        this.props = new PropertyUtil(fileName)
    }

    // Kafka 配置
    final lazy val KAFKA_SERVERS: String = props.getPropertyValueByKey("kafka.bootstrap.servers")
    final lazy val KAFKA_TOPIC: String = props.getPropertyValueByKey("kafka.topic")
    final lazy val KAFKA_GROUP: String = props.getPropertyValueByKey("kafka.group.id")
    final lazy val KAFKA_KEY_SERIAL: String = props.getPropertyValueByKey("kafka.key.serializer")
    final lazy val KAFKA_VALUE_SERIAL: String = props.getPropertyValueByKey("kafka.value.serializer")
    final lazy val KAFKA_KEY_DESERIAL: String = props.getPropertyValueByKey("kafka.key.deserializer")
    final lazy val KAFKA_VALUE_DESERIAL: String = props.getPropertyValueByKey("kafka.value.deserializer")

    // MYSQL 配置
    final lazy val MYSQL_DRIVER: String = props.getPropertyValueByKey("mysql.driver")
    final lazy val MYSQL_HOST: String = props.getPropertyValueByKey("mysql.host")
    final lazy val MYSQL_DATABASE: String = props.getPropertyValueByKey("mysql.database")
    final lazy val MYSQL_USER: String = props.getPropertyValueByKey("mysql.user")
    final lazy val MYSQL_PASSWORD: String = props.getPropertyValueByKey("mysql.password")
    final lazy val MYSQL_URL: String = s"jdbc:mysql://$MYSQL_HOST:3306/$MYSQL_DATABASE?user=$MYSQL_USER&password=$MYSQL_PASSWORD&useSSL=false&useUnicode=true&characterEncoding=utf8" // serverTimezone=UTC&
}

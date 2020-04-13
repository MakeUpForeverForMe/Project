package com.mine.property

object GetValueByKeyFromProperty {
    def apply: GetValueByKeyFromProperty = new GetValueByKeyFromProperty()

    def apply(fileName: String): GetValueByKeyFromProperty = new GetValueByKeyFromProperty(fileName)
}

class GetValueByKeyFromProperty {
    private final var fileName: String = _

    def this(fileName: String) = {
        this
        this.fileName = fileName
    }

    // 获取配置文件
    lazy final val props = new PropertyUtil(fileName)

    // Kafka 配置
    lazy final val KAFKA_SERVERS: String = props.getPropertyValueByKey("kafka.bootstrap.servers")
    lazy final val KAFKA_TOPIC: String = props.getPropertyValueByKey("kafka.topic")
    lazy final val KAFKA_GROUP: String = props.getPropertyValueByKey("kafka.group.id")
    lazy final val KAFKA_KEY_SERIAL: String = props.getPropertyValueByKey("kafka.key.serializer")
    lazy final val KAFKA_VALUE_SERIAL: String = props.getPropertyValueByKey("kafka.value.serializer")
    lazy final val KAFKA_KEY_DESERIAL: String = props.getPropertyValueByKey("kafka.key.deserializer")
    lazy final val KAFKA_VALUE_DESERIAL: String = props.getPropertyValueByKey("kafka.value.deserializer")

    // MYSQL 配置
    lazy final val MYSQL_DRIVER: String = props.getPropertyValueByKey("mysql.driver")
    lazy final val MYSQL_HOST: String = props.getPropertyValueByKey("mysql.host")
    lazy final val MYSQL_DATABASE: String = props.getPropertyValueByKey("mysql.database")
    lazy final val MYSQL_USER: String = props.getPropertyValueByKey("mysql.user")
    lazy final val MYSQL_PASSWORD: String = props.getPropertyValueByKey("mysql.password")
    lazy final val MYSQL_URL: String = s"jdbc:mysql://$MYSQL_HOST:3306/$MYSQL_DATABASE?user=$MYSQL_USER&password=$MYSQL_PASSWORD&useSSL=false&useUnicode=true&characterEncoding=utf8" // serverTimezone=UTC&
}

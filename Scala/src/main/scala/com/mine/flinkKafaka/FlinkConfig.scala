package com.mine.flinkKafaka

//object FlinkConfig {
//    import com.typesafe.config.{ Config, ConfigFactory }
//    import net.ceedubs.ficus.Ficus._
//
//
//    def apply(): MyFlinkConfig = apply(ConfigFactory.load)
//
//    def apply(applicationConfig: Config): MyFlinkConfig = {
//
//        val config = applicationConfig.getConfig("MyFlinkConfig")
//
//        FlinkConfig(config.as[List[String]]("myTopicList"))
//    }
//}

case class FlinkConfig (myTopicList: List[String]) extends Serializable
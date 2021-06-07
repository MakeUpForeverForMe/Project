package com.weshare

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.junit.Test

import scala.collection.JavaConversions._

/**
  * @author ximing.wei 2021-05-27 14:35:23
  */
class ABSFileToHiveTest extends Serializable {
    private val objectMapper = new ObjectMapper
    objectMapper.configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
    objectMapper.registerModule(DefaultScalaModule)

    @Test
    def test1(): Unit = {
        case class Item(@JsonProperty("bw") bw: String, @JsonProperty("cdn") cdn: Long, @JsonProperty("ct") ct: Long)
        case class Outer(@JsonProperty("video_bandwidths") items: java.util.List[Item])

        val ss = "{\"video_bandwidths\":[ { \"bw\" : \"484456834\" , \"cdn\" : 0 , \"ct\" : 0} , { \"bw\" : \"160477600\" , \"cdn\" : 0 , \"ct\" : 1} , { \"bw\" : \"603954332\" , \"cdn\" : 0 , \"ct\" : 2}]}"

        val jsonMap = objectMapper.readValue(ss, classOf[Outer]) //转换为HashMap对象

        println(jsonMap)

        jsonMap.items.foreach(println)
        println(jsonMap.items.get(0).bw)
        jsonMap.items.foreach(a => println(a.bw))
    }

    @Test
    def test2(): Unit = {
        val json = "{\"import_id\": \"xxxx\", \"row_type\": \"delete\", \"project_id\": \"CL202012250044\"}"
        val jsonNode = objectMapper.readTree(json)
        println(jsonNode.get("row_type").asText())
    }

    @Test
    def test3(): Unit = {
        println()
    }

    @Test
    def test4(): Unit = {
        class A(@JsonProperty("a") a: String, @JsonProperty("b") b: String)
        val ss = "{\"a\": \"aa\", \"b\": \"bb\"}"
        val a = objectMapper.readValue(ss, classOf[A])
        println(a)
    }
}

package com.mine

import com.mine.code.JavaByteArray

import scala.collection.JavaConverters.asScalaBufferConverter

object ConfigTest {
    implicit def int2Long(int: Int): Long = int.toLong

    def main(args: Array[String]): Unit = {
        //        val logger = org.apache.log4j.Logger.getLogger(this.getClass)
        //        logger.info("哈哈哈")
        //        val (ll: Long, aa: Int) = (10.toLong, 20)
        //        println(ll, aa)
        //        println(new String(JavaByteArray.getByteArray))
        println(10L.toString)
    }
}

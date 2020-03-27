package com.mine

import java.util.Properties

import org.apache.log4j.Logger

object ConfigTest {
  private val properties = new Properties()
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass)
    logger.info("哈哈哈")
  }
}

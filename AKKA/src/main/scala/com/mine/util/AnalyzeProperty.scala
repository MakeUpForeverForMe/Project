package com.mine.util

import java.util.Properties

object AnalyzeProperty {
  def apply(path: String): AnalyzeProperty = new AnalyzeProperty(path)
}

sealed class AnalyzeProperty(path: String) {
  private val properties = new Properties()

  properties.load(this.getClass.getClassLoader.getResourceAsStream(path))

  def getPropertyValue(key: String): String = properties.getProperty(key)
}

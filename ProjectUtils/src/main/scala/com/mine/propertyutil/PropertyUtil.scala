package com.mine.propertyutil

import java.util.Properties

/**
  * 只能这样写，要不然写不了
  */
object PropertyUtil {
    def apply: PropertyUtil = new PropertyUtil()

    def apply(fileName: String): PropertyUtil = new PropertyUtil(fileName)
}

class PropertyUtil {
    private lazy val properties = new Properties()

    def this(fileName: String) {
        this
        properties.load(getClass.getClassLoader.getResourceAsStream(fileName))
    }

    def getPropertyValueByKey(key: String): String = properties.getProperty(key)
}

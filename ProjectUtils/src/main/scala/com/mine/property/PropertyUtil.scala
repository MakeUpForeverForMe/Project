package com.mine.property

import java.util.Properties

/**
  * 只能这样写，要不然写不了
  */
object PropertyUtil {
    def apply: PropertyUtil = new PropertyUtil()

    def apply(resourcesPath: String): PropertyUtil = new PropertyUtil(resourcesPath)
}

class PropertyUtil {
    private val properties = new Properties()

    def this(resourcesPath: String) {
        this
        properties.load(getClass.getClassLoader.getResourceAsStream(resourcesPath))
    }

    def getPropertyValueByKey(key: String): String = properties.getProperty(key)
}

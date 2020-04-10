package com.mine.propertyutil

import java.util.Properties

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

package com.mine.projectUtils

import java.util.Properties

class PropertyUtil {
    private val properties = new Properties()

    def this(resourcesPath: String) {
        this
        properties.load(getClass.getClassLoader.getResourceAsStream(resourcesPath))
    }

    def getPropertyValueByKey(key: String): String = properties.getProperty(key)
}

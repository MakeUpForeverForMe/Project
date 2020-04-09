package com.mine.utils;

import java.io.IOException;
import java.util.Properties;

public class PropertyGet {

    private Properties properties = null;

    public PropertyGet() {
    }

    private void init(String name) {
        properties = new Properties();
        try {
            properties.load(this.getClass().getClassLoader().getResourceAsStream(name));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public PropertyGet(String name) {
        init(name);
    }

    public String get(String key) {
        return properties.getProperty(key);
    }

    public void set(String key, String value) {
        properties.setProperty(key, value);
    }
}

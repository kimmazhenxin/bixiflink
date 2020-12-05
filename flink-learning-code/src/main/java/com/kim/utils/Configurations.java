package com.kim.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置文件读取
 * @Author: mazhenxin
 * @File: Configurations.java
 * @Date: 2020/12/2 13:43
 */
public class Configurations {

    private static  final Logger logger = LoggerFactory.getLogger(Configurations.class);

    private static final Properties properties = new Properties();


    static {
        try {
            InputStream configStream = Configurations.class.getClassLoader().getResourceAsStream("app.properties");
            Throwable throwable = null;

            try {
                if (null != configStream) {
                    properties.load(configStream);
                    logger.info("加载配置文件: " + properties.toString());
                }
            } catch (Throwable var) {
                throwable = var;
                throw throwable;
            } finally {
                if (null != configStream) {
                    if (null != throwable) {
                        try {
                            configStream.close();
                        } catch (Throwable thr) {
                            throwable.addSuppressed(thr);
                        }
                    } else {
                        configStream.close();
                    }
                }
            }
        } catch (Throwable ioex) {
            ioex.printStackTrace();
        }
    }


    public Configurations() {
    }


    public static String getString(String key) {
        return properties.getProperty(key);
    }

    public static Integer getInteger(String key) {
        return Integer.parseInt(properties.getProperty(key));
    }

    public static Long getLong(String key) {
        return Long.parseLong(properties.getProperty(key));
    }

    public static Double getDouble(String key) {
        return Double.valueOf(properties.getProperty(key));
    }


    public static void main(String[] args) {
        System.out.println("ok");
    }

}

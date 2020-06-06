package com.kim.utils

import java.util.Properties

/**
  * @Author zhenxin.ma
  * @Date   2020/01/30 11:59
  * @Version 1.0
  */
object PropertiesUtil {

  private val properties = new Properties

  /**
    *
    * 获取配置文件Properties对象
    *
    * @author zhenxin.ma
    * @return java.util.Properties
    * @date 2020/01/30 11:59
    */
  def getProperties() :Properties = {
    if(properties.isEmpty){
      //读取源码中resource文件夹下的my.properties配置文件
      val reader = this.getClass.getResourceAsStream("/myconfig.properties")
      properties.load(reader)
    }
    properties
  }

  /**
    *
    * 获取配置文件中key对应的字符串值
    *
    * @author zhenxin.ma
    * @return java.util.Properties
    * @date 2020/01/30 11:59
    */
  def getPropString(key : String) : String = getProperties().getProperty(key)

  /**
    *
    * 获取配置文件中key对应的整数值
    *
    * @author zhenxin.ma
    * @return java.util.Properties
    * @date 2020/01/30 11:59
    */
  def getPropInt(key : String) : Int = getProperties().getProperty(key).toInt

  /**
    *
    * 获取配置文件中key对应的布尔值
    *
    * @author zhenxin.ma
    * @return java.util.Properties
    * @date 2020/01/30 11:59
    */
  def getPropBoolean(key : String) : Boolean = getProperties().getProperty(key).toBoolean

}

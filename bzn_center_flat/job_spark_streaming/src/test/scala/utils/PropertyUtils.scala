package utils

import java.util.Properties

import scala.io.Source
/**
  * Created with IntelliJ IDEA.
  * Author: fly_elephant@163.com
  * Description:PropertyUtils工具类
  * Date: Created in 2018-11-17 11:43
  */
object PropertyUtils {
//  def getFileProperties(fileName: String, propertyKey: String): String = {
//    val result = this.getClass.getClassLoader.getResourceAsStream(fileName)
//    val prop = new Properties
//    prop.load(result)
//    prop.getProperty(propertyKey)
//  }

  /**
    * 获取配置文件
    *
    * @return
    */
  def getFileProperties(fileName: String, propertyKey: String):String = {
    val lines_source = Source.fromURL(getClass.getResource("/"+fileName)).getLines.toSeq
    val properties: Properties = new Properties()
    for (elem <- lines_source) {
      val split = elem.split("==")
      if(split.length>1){
        val key = split(0)
        val value = split(1)
        properties.setProperty(key,value)
      }
    }
    properties.getProperty(propertyKey)
  }
}
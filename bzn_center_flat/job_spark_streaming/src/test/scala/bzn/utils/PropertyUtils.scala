package bzn.utils

import java.util.Properties

import scala.io.Source
/**
  * Created with IntelliJ IDEA.
  * Author:
  * Description:MySQL DDL 和DML 工具类
  * Date: Created in 2019-11-12
  */
object PropertyUtils {

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
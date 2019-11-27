package bzn.piwik

import java.sql.{Date, Timestamp}
import java.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

/**
  * author:xiaoYuanRen
  * Date:2019/11/27
  * Time:16:57
  * describe: this is new class
  **/
object test {

  def main (args: Array[String]): Unit = {

    val str = "{\"data\":[{\"name\":\"3b0fe256-10f6-11ea-b3ee-1866daf21618\",\"idzz\":\"29\",\"value\":\"632\"},{\"name\":\"3b11f3e1-10f6-11ea-b3ee-1866daf21618\",\"idzz\":\"30\",\"value\":\"61.770262906113885\"}],\"database\":\"dwdb\",\"es\":1574846032000,\"id\":5951,\"isDdl\":false,\"mysqlType\":{\"name\":\"varchar(250)\",\"idzz\":\"int\",\"value\":\"varchar(250)\"},\"old\":[{\"name\":\"xingwanc\"},{\"name\":\"4e30c951-10f3-11ea-b3ee-1866daf21618\"}],\"pkNames\":[\"idzz\"],\"sql\":\"\",\"sqlType\":{\"name\":12,\"idzz\":4,\"value\":12},\"table\":\"canal_test\",\"ts\":1574846032925,\"type\":\"UPDATE\"}"

    val jsonData: JSONObject = JSON.parseObject(str)

    val set: util.Set[String] = jsonData.keySet()
    val value = jsonData.get ("data").toString
    val array: JSONArray = JSON.parseArray(value)
    for(x <- 0 until  array.size){
      val jsonData = JSON.parseObject(array.get(x).toString)

      println (jsonData)
    }
    set.toArray().foreach(println)

    /**
      * 用到的key
      */
    val data = ""
    val database = ""
    val pkNames = ""
    val table = ""
    val `type` = ""
  }
}

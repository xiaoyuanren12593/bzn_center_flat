package bzn.piwik

import java.sql.Timestamp

import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.commons.lang.mutable.Mutable
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


/**
  * author:xiaoYuanRen
  * Date:2019/11/28
  * Time:16:17
  * describe: this is new class
  **/
object test{
  case class Canal(objectRes:String,database:String,pkNames:String,table:String)

  def main (args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(getClass.getSimpleName).set("spark.testing.memory", "3147480000")
    val sparkContext = new SparkContext(conf)
    val hiveContext = new SQLContext(sparkContext)
    import hiveContext.implicits._

    val json = "{\"data\":[{\"name\":\"3b0fe256-10f6-11ea-b3ee-1866daf21618\",\"idzz\":\"29\",\"value\":\"632\"},{\"name\":\"3b11f3e1-10f6-11ea-b3ee-1866daf21618\",\"idzz\":\"30\",\"value\":\"61.770262906113885\"}],\"database\":\"dwdb\",\"es\":1574846032000,\"id\":5951,\"isDdl\":false,\"mysqlType\":{\"name\":\"varchar(250)\",\"idzz\":\"int\",\"value\":\"varchar(250)\"},\"old\":[{\"name\":\"xingwanc\"},{\"name\":\"4e30c951-10f3-11ea-b3ee-1866daf21618\"}],\"pkNames\":[\"idzz\"],\"sql\":\"\",\"sqlType\":{\"name\":12,\"idzz\":4,\"value\":12},\"table\":\"canal_test\",\"ts\":1574846032925,\"type\":\"UPDATE\"}"

    val jsonObject = JSON.parseObject(json).get("data")
    val jsonArray = JSON.parseObject(jsonObject.toString)

    val arr1 : ArrayBuffer[Canal] = new ArrayBuffer[Canal]()

    val objectRes = JSON.parseArray(jsonArray.toJSONString).toArray.mkString("\001")
    val database = JSON.parseObject(json).get("database").toString
    val pkNames = JSON.parseObject(json).get("pkNames").toString
    val table = JSON.parseObject(json).get("table").toString
    arr1 += Canal(objectRes,database,pkNames,table)

    val memberSeq = arr1
    memberSeq.toDF().foreach(println)
  }
}

package bzn.piwik

import java.lang
import java.sql.Timestamp

import com.alibaba.fastjson.{JSON}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/11/28
  * Time:16:17
  * describe: this is new class
  **/
object test{
  case class Canal(name:String,idzz:Int,value:Double,database:String,pkNames:String,table:String)

  def main (args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(getClass.getSimpleName).set("spark.testing.memory", "3147480000")
    val sparkContext = new SparkContext(conf)
    val hiveContext = new SQLContext(sparkContext)
    import hiveContext.implicits._

    val json = "{\"data\":[{\"name\":\"12a9f959-11ae-11ea-b3ee-1866daf21618\",\"idzz\":\"32\",\"value\":null}],\"database\":\"dwdb\",\"es\":1574924992000,\"id\":6260,\"isDdl\":false,\"mysqlType\":{\"name\":\"varchar(250)\",\"idzz\":\"int\",\"value\":\"varchar(250)\"},\"old\":null,\"pkNames\":[\"idzz\"],\"sql\":\"\",\"sqlType\":{\"name\":12,\"idzz\":4,\"value\":12},\"table\":\"canal_test\",\"ts\":1574924992509,\"type\":\"INSERT\"}"

    /**
      * 获取data数据-存储插入和更新的值
      */
    val jsonDataObject = JSON.parseObject(json).get("data")

    /**
      * 将data中的存储的数组解析成单个的数据，便于分开
      */
    val jsonArray = JSON.parseArray(jsonDataObject.toString)

    val array = jsonArray.toArray

    val res: Seq[(String, Integer, lang.Double, String, String, String, String)] = array.map(t => {
      val data = JSON.parseObject(t.toString)
      val database = JSON.parseObject(json).get("database").toString
      val pkNames = JSON.parseArray(JSON.parseObject(json).get("pkNames").toString).toArray()(0).toString
      val table = JSON.parseObject(json).get("table").toString
      val sqlType = JSON.parseObject(json).get("type").toString
      val name = data.getString("name")
      val idzz = data.getInteger("idzz")
      val value = data.getDouble("value")
      (name,idzz,value,database,pkNames,table,sqlType)
    }).toSeq

    val insertColumns = Array("name","idzz","value")
    println (insertColumns.indexOf ("idzz"))
    val df: DataFrame = res.toDF("name","idzz","value","database","pkNames","table","sql_type")

    df.where("value is null").show()

    df.schema.foreach(println)

    df.show()
  }
}

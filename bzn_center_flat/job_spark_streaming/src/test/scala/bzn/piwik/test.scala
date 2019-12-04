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
    conf.set("spark.sql.parquet.binaryAsString","true")
    val sparkContext = new SparkContext(conf)
    val hiveContext = new SQLContext(sparkContext)
    import hiveContext.implicits._

    val json = "{\"data\":[{\"idlink_va\":\"5964159\",\"idsite\":\"1\",\"idvisitor\":\"ËU\\u0080X\\u001Eíâ,\",\"idvisit\":\"946601\",\"idaction_url_ref\":\"1011507\",\"idaction_name_ref\":\"4752\",\"custom_float\":\"21.0\",\"server_time\":\"2019-12-04 06:11:56\",\"idpageview\":\"sf8ZOt\",\"interaction_position\":\"72\",\"idaction_name\":\"61813\",\"idaction_url\":\"64871\",\"time_spent_ref_action\":\"158\",\"idaction_event_action\":null,\"idaction_event_category\":null,\"idaction_content_interaction\":null,\"idaction_content_name\":null,\"idaction_content_piece\":null,\"idaction_content_target\":null,\"custom_var_k1\":null,\"custom_var_v1\":null,\"custom_var_k2\":null,\"custom_var_v2\":null,\"custom_var_k3\":null,\"custom_var_v3\":null,\"custom_var_k4\":null,\"custom_var_v4\":null,\"custom_var_k5\":null,\"custom_var_v5\":null}],\"database\":\"piwik\",\"es\":1575439916000,\"id\":41589,\"isDdl\":false,\"mysqlType\":{\"idlink_va\":\"bigint(10) unsigned\",\"idsite\":\"int(10) unsigned\",\"idvisitor\":\"binary(8)\",\"idvisit\":\"bigint(10) unsigned\",\"idaction_url_ref\":\"int(10) unsigned\",\"idaction_name_ref\":\"int(10) unsigned\",\"custom_float\":\"float\",\"server_time\":\"datetime\",\"idpageview\":\"char(6)\",\"interaction_position\":\"smallint(5) unsigned\",\"idaction_name\":\"int(10) unsigned\",\"idaction_url\":\"int(10) unsigned\",\"time_spent_ref_action\":\"int(10) unsigned\",\"idaction_event_action\":\"int(10) unsigned\",\"idaction_event_category\":\"int(10) unsigned\",\"idaction_content_interaction\":\"int(10) unsigned\",\"idaction_content_name\":\"int(10) unsigned\",\"idaction_content_piece\":\"int(10) unsigned\",\"idaction_content_target\":\"int(10) unsigned\",\"custom_var_k1\":\"varchar(200)\",\"custom_var_v1\":\"varchar(200)\",\"custom_var_k2\":\"varchar(200)\",\"custom_var_v2\":\"varchar(200)\",\"custom_var_k3\":\"varchar(200)\",\"custom_var_v3\":\"varchar(200)\",\"custom_var_k4\":\"varchar(200)\",\"custom_var_v4\":\"varchar(200)\",\"custom_var_k5\":\"varchar(200)\",\"custom_var_v5\":\"varchar(200)\"},\"old\":null,\"pkNames\":[\"idlink_va\"],\"sql\":\"\",\"sqlType\":{\"idlink_va\":-5,\"idsite\":4,\"idvisitor\":2004,\"idvisit\":-5,\"idaction_url_ref\":4,\"idaction_name_ref\":4,\"custom_float\":7,\"server_time\":93,\"idpageview\":1,\"interaction_position\":5,\"idaction_name\":4,\"idaction_url\":4,\"time_spent_ref_action\":4,\"idaction_event_action\":4,\"idaction_event_category\":4,\"idaction_content_interaction\":4,\"idaction_content_name\":4,\"idaction_content_piece\":4,\"idaction_content_target\":4,\"custom_var_k1\":12,\"custom_var_v1\":12,\"custom_var_k2\":12,\"custom_var_v2\":12,\"custom_var_k3\":12,\"custom_var_v3\":12,\"custom_var_k4\":12,\"custom_var_v4\":12,\"custom_var_k5\":12,\"custom_var_v5\":12},\"table\":\"piwik_log_link_visit_action\",\"ts\":1575439917692,\"type\":\"INSERT\"}"

    /**
      * 获取data数据-存储插入和更新的值
      */
    val jsonDataObject = JSON.parseObject(json).get("data")

    /**
      * 将data中的存储的数组解析成单个的数据，便于分开
      */
    val jsonArray = JSON.parseArray(jsonDataObject.toString)

    val array = jsonArray.toArray

    val res = array.map(t => {
      val data = JSON.parseObject(t.toString)
      val database = JSON.parseObject(json).get("database").toString
      val pkNames = JSON.parseArray(JSON.parseObject(json).get("pkNames").toString).toArray()(0).toString
      val table = JSON.parseObject(json).get("table").toString
      val sqlType = JSON.parseObject(json).get("type").toString
      val idvisitor = data.getString("idvisitor")

      val idlink_va = data.getInteger("idlink_va")
      val idsite = data.getDouble("idsite")
      val server_time = data.getString("server_time")
      println (Timestamp.valueOf(server_time))
      (idvisitor,idlink_va,idsite,server_time,database,pkNames,table,sqlType)
    }).toSeq

    val insertColumns = Array("name","idzz","value")
    println (insertColumns.indexOf ("idzz"))
    val df: DataFrame = res.toDF("name","idzz","value","database","pkNames","table","sql_type")

    df.where("value is null").show()

    df.schema.foreach(println)

    df.show()
  }
}

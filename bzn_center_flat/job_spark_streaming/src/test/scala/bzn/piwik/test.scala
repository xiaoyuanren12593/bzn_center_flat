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

    val json = "{\"data\":[{\"idvisit\":\"948008\",\"idsite\":\"4\",\"idvisitor\":\"àó}OJ\\u0012\\u0017\\f\",\"visit_last_action_time\":\"2019-12-05 03:34:07\",\"config_id\":\"\\tÎ\\u008B\\u009F÷\\u009F\\u008Cª\",\"location_ip\":\"u\\u0088?\\u008B\",\"user_id\":null,\"visit_first_action_time\":\"2019-12-05 02:51:56\",\"visit_goal_buyer\":\"0\",\"visit_goal_converted\":\"0\",\"visitor_days_since_first\":\"2\",\"visitor_days_since_order\":\"0\",\"visitor_returning\":\"1\",\"visitor_count_visits\":\"6\",\"visit_entry_idaction_name\":\"297118\",\"visit_entry_idaction_url\":\"64\",\"visit_exit_idaction_name\":\"467366\",\"visit_exit_idaction_url\":\"467367\",\"visit_total_actions\":\"55\",\"visit_total_interactions\":\"55\",\"visit_total_searches\":\"0\",\"referer_keyword\":null,\"referer_name\":null,\"referer_type\":\"1\",\"referer_url\":\"\",\"location_browser_lang\":\"zh-cn\",\"config_browser_engine\":\"\",\"config_browser_name\":\"UNK\",\"config_browser_version\":\"7.0\",\"config_device_brand\":\"AP\",\"config_device_model\":\"iPhone\",\"config_device_type\":\"1\",\"config_os\":\"IOS\",\"config_os_version\":\"12.4\",\"visit_total_events\":\"0\",\"visitor_localtime\":\"10:51:57\",\"visitor_days_since_last\":\"0\",\"config_resolution\":\"414x736\",\"config_cookie\":\"1\",\"config_director\":\"0\",\"config_flash\":\"0\",\"config_gears\":\"0\",\"config_java\":\"0\",\"config_pdf\":\"0\",\"config_quicktime\":\"0\",\"config_realplayer\":\"0\",\"config_silverlight\":\"0\",\"config_windowsmedia\":\"0\",\"visit_total_time\":\"2532\",\"location_city\":\"Chengdu\",\"location_country\":\"cn\",\"location_latitude\":\"30.667000\",\"location_longitude\":\"104.067000\",\"location_region\":\"32\",\"custom_var_k1\":null,\"custom_var_v1\":null,\"custom_var_k2\":null,\"custom_var_v2\":null,\"custom_var_k3\":null,\"custom_var_v3\":null,\"custom_var_k4\":null,\"custom_var_v4\":null,\"custom_var_k5\":null,\"custom_var_v5\":null}],\"database\":\"piwik\",\"es\":1575516847000,\"id\":56014,\"isDdl\":false,\"mysqlType\":{\"idvisit\":\"bigint(10) unsigned\",\"idsite\":\"int(10) unsigned\",\"idvisitor\":\"binary(8)\",\"visit_last_action_time\":\"datetime\",\"config_id\":\"binary(8)\",\"location_ip\":\"varbinary(16)\",\"user_id\":\"varchar(200)\",\"visit_first_action_time\":\"datetime\",\"visit_goal_buyer\":\"tinyint(1)\",\"visit_goal_converted\":\"tinyint(1)\",\"visitor_days_since_first\":\"smallint(5) unsigned\",\"visitor_days_since_order\":\"smallint(5) unsigned\",\"visitor_returning\":\"tinyint(1)\",\"visitor_count_visits\":\"int(11) unsigned\",\"visit_entry_idaction_name\":\"int(10) unsigned\",\"visit_entry_idaction_url\":\"int(11) unsigned\",\"visit_exit_idaction_name\":\"int(10) unsigned\",\"visit_exit_idaction_url\":\"int(10) unsigned\",\"visit_total_actions\":\"int(11) unsigned\",\"visit_total_interactions\":\"smallint(5) unsigned\",\"visit_total_searches\":\"smallint(5) unsigned\",\"referer_keyword\":\"varchar(255)\",\"referer_name\":\"varchar(70)\",\"referer_type\":\"tinyint(1) unsigned\",\"referer_url\":\"text\",\"location_browser_lang\":\"varchar(20)\",\"config_browser_engine\":\"varchar(10)\",\"config_browser_name\":\"varchar(10)\",\"config_browser_version\":\"varchar(20)\",\"config_device_brand\":\"varchar(100)\",\"config_device_model\":\"varchar(100)\",\"config_device_type\":\"tinyint(100)\",\"config_os\":\"char(3)\",\"config_os_version\":\"varchar(100)\",\"visit_total_events\":\"int(11) unsigned\",\"visitor_localtime\":\"time\",\"visitor_days_since_last\":\"smallint(5) unsigned\",\"config_resolution\":\"varchar(18)\",\"config_cookie\":\"tinyint(1)\",\"config_director\":\"tinyint(1)\",\"config_flash\":\"tinyint(1)\",\"config_gears\":\"tinyint(1)\",\"config_java\":\"tinyint(1)\",\"config_pdf\":\"tinyint(1)\",\"config_quicktime\":\"tinyint(1)\",\"config_realplayer\":\"tinyint(1)\",\"config_silverlight\":\"tinyint(1)\",\"config_windowsmedia\":\"tinyint(1)\",\"visit_total_time\":\"int(11) unsigned\",\"location_city\":\"varchar(255)\",\"location_country\":\"char(3)\",\"location_latitude\":\"decimal(9,6)\",\"location_longitude\":\"decimal(9,6)\",\"location_region\":\"char(2)\",\"custom_var_k1\":\"varchar(200)\",\"custom_var_v1\":\"varchar(200)\",\"custom_var_k2\":\"varchar(200)\",\"custom_var_v2\":\"varchar(200)\",\"custom_var_k3\":\"varchar(200)\",\"custom_var_v3\":\"varchar(200)\",\"custom_var_k4\":\"varchar(200)\",\"custom_var_v4\":\"varchar(200)\",\"custom_var_k5\":\"varchar(200)\",\"custom_var_v5\":\"varchar(200)\"},\"old\":[{\"visit_last_action_time\":\"2019-12-05 03:33:31\",\"visit_exit_idaction_name\":\"299557\",\"visit_exit_idaction_url\":\"52594\",\"visit_total_actions\":\"54\",\"visit_total_interactions\":\"54\",\"visit_total_time\":\"2496\"}],\"pkNames\":[\"idvisit\"],\"sql\":\"\",\"sqlType\":{\"idvisit\":-5,\"idsite\":4,\"idvisitor\":2004,\"visit_last_action_time\":93,\"config_id\":2004,\"location_ip\":2004,\"user_id\":12,\"visit_first_action_time\":93,\"visit_goal_buyer\":-7,\"visit_goal_converted\":-7,\"visitor_days_since_first\":5,\"visitor_days_since_order\":5,\"visitor_returning\":-7,\"visitor_count_visits\":4,\"visit_entry_idaction_name\":4,\"visit_entry_idaction_url\":4,\"visit_exit_idaction_name\":4,\"visit_exit_idaction_url\":4,\"visit_total_actions\":4,\"visit_total_interactions\":5,\"visit_total_searches\":5,\"referer_keyword\":12,\"referer_name\":12,\"referer_type\":-7,\"referer_url\":2005,\"location_browser_lang\":12,\"config_browser_engine\":12,\"config_browser_name\":12,\"config_browser_version\":12,\"config_device_brand\":12,\"config_device_model\":12,\"config_device_type\":-6,\"config_os\":1,\"config_os_version\":12,\"visit_total_events\":4,\"visitor_localtime\":92,\"visitor_days_since_last\":5,\"config_resolution\":12,\"config_cookie\":-7,\"config_director\":-7,\"config_flash\":-7,\"config_gears\":-7,\"config_java\":-7,\"config_pdf\":-7,\"config_quicktime\":-7,\"config_realplayer\":-7,\"config_silverlight\":-7,\"config_windowsmedia\":-7,\"visit_total_time\":4,\"location_city\":12,\"location_country\":1,\"location_latitude\":3,\"location_longitude\":3,\"location_region\":1,\"custom_var_k1\":12,\"custom_var_v1\":12,\"custom_var_k2\":12,\"custom_var_v2\":12,\"custom_var_k3\":12,\"custom_var_v3\":12,\"custom_var_k4\":12,\"custom_var_v4\":12,\"custom_var_k5\":12,\"custom_var_v5\":12},\"table\":\"piwik_log_visit\",\"ts\":1575516848914,\"type\":\"UPDATE\"}"

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

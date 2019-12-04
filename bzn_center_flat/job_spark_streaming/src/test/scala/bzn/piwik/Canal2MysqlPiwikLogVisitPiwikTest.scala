package bzn.piwik

import bzn.util.SparkUtil
import bzn.utils.ToMysqlUtils
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/11/27
  * Time:14:10
  * describe: 实时抽取将piwik数据写入mysql
  **/
object Canal2MysqlPiwikLogVisitPiwikTest extends SparkUtil  with ToMysqlUtils {
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext,StreamingContext) = sparkConfInfo(appName, "local[*]")

    val hqlContext = sparkConf._4
    val strContext = sparkConf._5

    /**
      * ############################---kafka的配置----##########################################
      */
    val groupId = "piwik_example"
    val clientId = "client_example"

    val kafkaParam: Map[String, String] = Map[String, String](
      //-----------kafka低级api配置-----------
      "zookeeper.connect" -> "namenode2.cdh:2181,namenode1.cdh:2181", //----------配置zookeeper-----------
      "metadata.broker.list" -> "namenode1.cdh:9092,datanode3.cdh:9092",
      "group.id" -> groupId, //设置一下group id
      "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString, //----------从该topic最新的位置开始读数------------
      "client.id" -> clientId,
      "zookeeper.connection.timeout.ms" -> "10000"
    )

    val topicSet: Set[String] = Set("piwik")

    /**
      * 读取kafka的数据
      */
    val directKafka: InputDStream[(String, String)] =
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](strContext, kafkaParam, topicSet)
    val lines: DStream[String] = directKafka.map(x => x._2)

    val table = "piwik_log_action"

    val insertType = "INSERT"
    val updateType = "UPDATE"
    val deleteType = "DELETE"

    /**
      * 得到插入和更细的数据，
      */
    val canalTestInsertOrUpdate: DStream[String] = lines
      .filter (x => x.contains (table))
      .filter (x => x.contains (insertType) || x.contains (updateType))

    /**
      * 得到删忽的数据
      */
    val canalTestDelete: DStream[String] = lines
      .filter (x => x.contains (table))
      .filter (x => x.contains (deleteType))

    //############################### 删除 ######################################
    /**
      * 主键的字段
      */
    val idColumnsDelete = Array ("idvisit")

    val colNumbersDelete = idColumnsDelete.length

    /**
      * 字段类型
      */
    val columnDataTypesDelete = Array [String]("Long")

    /**
      * 表名
      */
    val tableName = "piwik_log_visit_piwik"

    /**
      * 主键
      */
    val idDelete = "idvisit"

    /**
      * 删除需要的属性：插入的字段，字段长度，字段类型，表名，主键
      */
    val deleteArray: (Array[String], Int, Array[String], String, String) =
      (idColumnsDelete,colNumbersDelete,columnDataTypesDelete,tableName,idDelete)

    /**
      * 处理删除操作
      */
    getPiwikDataDelete(strContext,canalTestDelete,deleteArray)

    //############################### 插入和更新 ######################################

    /**
      * 插入的字段
      */
    val insertColumns = Array ("idvisit",
      "idsite",
      "idvisitor",
      "visit_last_action_time",
      "config_id",
      "location_ip",
      "user_id",
      "visit_first_action_time",
      "visit_goal_buyer",
      "visit_goal_converted",
      "visitor_days_since_first",
      "visitor_days_since_order",
      "visitor_returning",
      "visitor_count_visits",
      "visit_entry_idaction_name",
      "visit_entry_idaction_url",
      "visit_exit_idaction_name",
      "visit_exit_idaction_url",
      "visit_total_actions",
      "visit_total_interactions",
      "visit_total_searches",
      "referer_keyword",
      "referer_name",
      "referer_type",
      "referer_url",
      "location_browser_lang",
      "config_browser_engine",
      "config_browser_name",
      "config_browser_version",
      "config_device_brand",
      "config_device_model",
      "config_device_type",
      "config_os",
      "config_os_version",
      "visit_total_events",
      "visitor_localtime",
      "visitor_days_since_last",
      "config_resolution",
      "config_cookie",
      "config_director",
      "config_flash",
      "config_gears",
      "config_java",
      "config_pdf",
      "config_quicktime",
      "config_realplayer",
      "config_silverlight",
      "config_windowsmedia",
      "visit_total_time",
      "location_city",
      "location_country",
      "location_latitude",
      "location_longitude",
      "location_region",
      "custom_var_k1",
      "custom_var_v1",
      "custom_var_k2",
      "custom_var_v2",
      "custom_var_k3",
      "custom_var_v3",
      "custom_var_k4",
      "custom_var_v4",
      "custom_var_k5",
      "custom_var_v5"

    )

    /**
      * 字段个数
      */
    val colNumbersInsert = insertColumns.length

    /**
      * 更新的字段
      */
    val updateColumns = Array (
      "idsite",
      "idvisitor",
      "visit_last_action_time",
      "config_id",
      "location_ip",
      "user_id",
      "visit_first_action_time",
      "visit_goal_buyer",
      "visit_goal_converted",
      "visitor_days_since_first",
      "visitor_days_since_order",
      "visitor_returning",
      "visitor_count_visits",
      "visit_entry_idaction_name",
      "visit_entry_idaction_url",
      "visit_exit_idaction_name",
      "visit_exit_idaction_url",
      "visit_total_actions",
      "visit_total_interactions",
      "visit_total_searches",
      "referer_keyword",
      "referer_name",
      "referer_type",
      "referer_url",
      "location_browser_lang",
      "config_browser_engine",
      "config_browser_name",
      "config_browser_version",
      "config_device_brand",
      "config_device_model",
      "config_device_type",
      "config_os",
      "config_os_version",
      "visit_total_events",
      "visitor_localtime",
      "visitor_days_since_last",
      "config_resolution",
      "config_cookie",
      "config_director",
      "config_flash",
      "config_gears",
      "config_java",
      "config_pdf",
      "config_quicktime",
      "config_realplayer",
      "config_silverlight",
      "config_windowsmedia",
      "visit_total_time",
      "location_city",
      "location_country",
      "location_latitude",
      "location_longitude",
      "location_region",
      "custom_var_k1",
      "custom_var_v1",
      "custom_var_k2",
      "custom_var_v2",
      "custom_var_k3",
      "custom_var_v3",
      "custom_var_k4",
      "custom_var_v4",
      "custom_var_k5",
      "custom_var_v5"

    )

    /**
      * 字段类型
      */
    val columnDataTypesInsert = Array [String]("Int", "String", "Int","Int","Int")

    val insertArray: (Array[String], Int, Array[String], Array[String], String) =
      (insertColumns,colNumbersInsert,updateColumns,columnDataTypesInsert,tableName)

    /**
      * 处理插入操作
      */
    getPiwikDataToMysql(strContext,canalTestInsertOrUpdate,insertArray)

    strContext.start()
    strContext.awaitTermination()
  }
}

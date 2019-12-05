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
object Canal2MysqlPiwikLogLinkVisitActionPiwikTest extends SparkUtil  with ToMysqlUtils {
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext,StreamingContext) = sparkConfInfo(appName, "local[*]")

    val hqlContext = sparkConf._4
    val strContext = sparkConf._5

    /**
      * ############################---kafka的配置----##########################################
      */
    val groupId = "piwik_example_piwik_log_link_visit_action"
    val clientId = "client_example_piwik_log_link_visit_action"

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

    val table = "piwik_log_link_visit_action"

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
    val idColumnsDelete = Array ("idlink_va")

    val colNumbersDelete = idColumnsDelete.length

    /**
      * 字段类型
      */
    val columnDataTypesDelete = Array [String]("Long")

    /**
      * 表名
      */
    val tableName = "piwik_log_link_visit_action_piwik"

    /**
      * 主键
      */
    val idDelete = "idlink_va"

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
    val insertColumns = Array (
      "idlink_va",
      "idsite",
      "idvisitor",
      "idvisit",
      "idaction_url_ref",
      "idaction_name_ref",
      "custom_float",
      "server_time",
      "idpageview",
      "interaction_position",
      "idaction_name",
      "idaction_url",
      "time_spent_ref_action",
      "idaction_event_action",
      "idaction_event_category",
      "idaction_content_interaction",
      "idaction_content_name",
      "idaction_content_piece",
      "idaction_content_target",
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
      "idlink_va",
      "idsite",
      "idvisitor",
      "idvisit",
      "idaction_url_ref",
      "idaction_name_ref",
      "custom_float",
      "server_time",
      "idpageview",
      "interaction_position",
      "idaction_name",
      "idaction_url",
      "time_spent_ref_action",
      "idaction_event_action",
      "idaction_event_category",
      "idaction_content_interaction",
      "idaction_content_name",
      "idaction_content_piece",
      "idaction_content_target",
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
    val columnDataTypesInsert = Array [String]("Long",
      "Int",
      "String",
      "Long",
      "Int",
      "Int",
      "Float",
      "String",
      "String",
      "Int",
      "Int",
      "Int",
      "Int",
      "Int",
      "Int",
      "Int",
      "Int",
      "Int",
      "Int",
      "String",
      "String",
      "String",
      "String",
      "String",
      "String",
      "String",
      "String",
      "String",
      "String"
    )

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

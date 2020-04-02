package bzn.blackgray

import bzn.util.SparkUtil
import bzn.utils.{ToMysqlUtils, ToMysqlUtilsPare}
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
  * describe: 实时抽取将black_gray数据写入mysql
  **/
object Canal2MysqlDmBlacklistWorktypeTableTest extends SparkUtil  with ToMysqlUtilsPare {
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext,StreamingContext) = sparkConfInfo(appName, "local[*]")

    val hqlContext = sparkConf._4
    val strContext = sparkConf._5

    /**
      * ############################---kafka的配置----##########################################
      */
    val groupId = "group_dm_blacklist_worktype"
    val clientId = "client_dm_blacklist_worktype"

    val kafkaParam: Map[String, String] = Map[String, String](
      //-----------kafka低级api配置-----------
      "zookeeper.connect" -> "172.16.11.105:2181,172.16.11.106:2181,172.16.11.103:2181", //----------配置zookeeper-----------
      "metadata.broker.list" -> "172.16.11.103:9092,172.16.11.105:9092,172.16.11.106:9092",
      "group.id" -> groupId, //设置一下group id
      "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString, //----------从该topic最新的位置开始读数------------
      "client.id" -> clientId,
      "zookeeper.connection.timeout.ms" -> "10000"
    )

    val topicSet: Set[String] = Set("black_gray")

    /**
      * 读取kafka的数据
      */
    val directKafka: InputDStream[(String, String)] =
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](strContext, kafkaParam, topicSet)
    val lines: DStream[String] = directKafka.map(x => x._2)

    val table = "dm_blacklist_worktype"

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
      * 得到删除的数据
      */
    val canalTestDelete: DStream[String] = lines
      .filter (x => x.contains (table))
      .filter (x => x.contains (deleteType))

    //############################### 删除 ######################################
    /**
      * 主键的字段
      */
    val idColumnsDelete = Array ("id")

    val colNumbersDelete = idColumnsDelete.length

    /**
      * 字段类型
      */
    val columnDataTypesDelete = Array [String]("Int")

    /**
      * 表名
      */
    val tableName = "dm_blacklist_worktype"

    /**
      * 主键
      */
    val idDelete = "id"

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
    val insertColumns =
      Array (
        "id",
        "work_name",
        "guide_words",
        "type",
        "status",
        "start_date",
        "create_time",
        "update_time"
      )

    /**
      * 字段个数
      */
    val colNumbersInsert = insertColumns.length

    /**
      * 更新的字段
      */
    val updateColumns =
      Array (
        "work_name",
        "guide_words",
        "type",
        "status",
        "start_date",
        "update_time"
      )

    /**
      * 字段类型
      */
    val columnDataTypesInsert = Array [String](
      "Int",
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

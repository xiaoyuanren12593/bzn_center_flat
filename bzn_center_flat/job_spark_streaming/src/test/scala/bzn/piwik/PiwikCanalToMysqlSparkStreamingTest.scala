package bzn.piwik

import java.sql.{Date, ResultSet, Timestamp}

import bzn.job.common.WareUntil
import bzn.util.SparkUtil
import bzn.utils.{MySQLPoolManager, ToMysqlUtils}
import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable.ArrayBuffer

/**
  * author:xiaoYuanRen
  * Date:2019/11/27
  * Time:14:10
  * describe: 实时抽取将piwik数据写入mysql
  **/
object PiwikCanalToMysqlSparkStreamingTest extends SparkUtil  with ToMysqlUtils {
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext,StreamingContext) = sparkConfInfo(appName, "")

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

    val topicSet: Set[String] = Set("example")

    /**
      * 读取kafka的数据
      */
    val directKafka: InputDStream[(String, String)] =
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](strContext, kafkaParam, topicSet)
    val lines: DStream[String] = directKafka.map(x => x._2)

    /**
      * 解析每条json数据，过滤需要处理的数据
      */
    val canalTestInsertOrUpdate: DStream[String] = lines
      .filter (x => x.contains ("canal_test"))
      .filter (x => x.contains ("INSERT") || x.contains ("UPDATE"))

    val canalTestDelete: DStream[String] = lines
      .filter (x => x.contains ("canal_test"))
      .filter (x => x.contains ("DELETE"))


    //getPiwikDataToMysql(strContext,canalTestInsertOrUpdate)
    getPiwikDataDelete(strContext,canalTestDelete)
    strContext.start()
    strContext.awaitTermination()
  }

  /**
    * 实时删除mysql数据
    * @param strContext sparkStreaming上下文
    * @param dsTreamData streamingData
    */
  def getPiwikDataDelete(strContext:StreamingContext,dsTreamData:DStream[String]) = {
    /**
      * 主键的字段
      */
    val idColumns = Array ("idzz")

    val colNumbers = idColumns.length

    /**
      * 字段类型
      */
    val columnDataTypes = Array [String]("String")

    /**
      * 表名
      */
    val tableName = "canal_res"

    /**
      * 主键
      */
    val id = "idzz"

    /**
      * 删除的sql
      */
    val sql = getDeleteSql(tableName,id)
    println(sql)

    val jsonToDStream: DStream[Array[ArrayBuffer[Any]]] = getJsonToDStream(dsTreamData:DStream[String],idColumns:Array[String],columnDataTypes:Array[String])

    jsonToDStream.foreachRDD(rdds => {
      if(!rdds.isEmpty()) {
        /**
          * 删除数据
          */
        deleteMysqlTableDataBatch(strContext:StreamingContext,tableName: String, rdds: RDD[Array[ArrayBuffer[Any]]],idColumns:Array[String],
                                  columnDataTypes:Array[String],colNumbers:Int,sql:String)
      }
    })
  }

  /**
    * 实时读取kafka数据到MySQL
    * @param strContext streaming 上下文
    */
  def getPiwikDataToMysql(strContext:StreamingContext,dsTreamData:DStream[String]): Unit = {

    /**
      * 插入的字段
      */
    val insertColumns = Array ("name", "idzz", "value")

    val colNumbers = insertColumns.length

    /**
      * 更新的字段
      */
    val updateColumns = Array ("name", "value")

    /**
      * 字段类型
      */
    val columnDataTypes = Array [String]("String", "Int", "Double")

    /**
      * 插入的表名
      */
    val tableName = "canal_res"

    /**
      * 插入和更新的sql
      */
    val sql = getInsertOrUpdateSql (tableName, insertColumns, updateColumns)

    val jsonToDStream: DStream[Array[ArrayBuffer[Any]]] = getJsonToDStream(dsTreamData:DStream[String],insertColumns:Array[String],columnDataTypes:Array[String])

    jsonToDStream.foreachRDD(rdds => {
      if(!rdds.isEmpty()) {
        /**
          * 插入更新数据
          */
        insertOrUpdateDFtoDBUsePool (strContext,tableName,rdds:RDD[Array[ArrayBuffer[Any]]], insertColumns, columnDataTypes, updateColumns, colNumbers, sql)
      }
    })
  }
}

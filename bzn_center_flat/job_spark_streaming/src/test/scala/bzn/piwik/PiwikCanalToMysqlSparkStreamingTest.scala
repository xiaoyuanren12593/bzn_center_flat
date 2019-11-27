package bzn.piwik

import java.sql.{Date, Timestamp}

import bzn.job.common.WareUntil
import bzn.util.SparkUtil
import com.alibaba.fastjson.{JSON, JSONObject}
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * author:xiaoYuanRen
  * Date:2019/11/27
  * Time:14:10
  * describe: 实时抽取将piwik数据写入mysql
  **/
object PiwikCanalToMysqlSparkStreamingTest extends SparkUtil  with WareUntil {
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext,StreamingContext) = sparkConfInfo(appName, "")

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

    getPiwikDataToMysql(strContext,kafkaParam,topicSet)
  }
  case class CardMember(m_id: Int, card_type: String, expire: Timestamp, duration: Int, is_sale: Int, date: Date, user: String, salary: Float,tableName:String)
  /**
    * 实时读取kafka数据到MySQL
    * @param strContext
    */
  def getPiwikDataToMysql(strContext:StreamingContext,kafkaParam: Map[String, String],topicSet:Set[String]) = {
    /**
      * 读取kafka的数据
      */
    val directKafka: InputDStream[(String, String)] =
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](strContext, kafkaParam, topicSet)
    val lines: DStream[String] = directKafka.map(x => x._2)

    lines.foreachRDD(rdds => {
      if(!rdds.isEmpty()){
        rdds.map(x => {
          val jsonData = JSON.parse(x)

        })

        rdds.foreach(println)
      }else{
        println("null")
      }
    })

    strContext.start()
    strContext.awaitTermination()
  }

}

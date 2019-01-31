package test

import Util.Spark_Util
import com.alibaba.fastjson.{JSON, JSONArray}
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import test.ParseJson._

object SparkStreamingReadKafka {
  def main(args: Array[String]): Unit = {
    val util = new Spark_Util

    var sc = util.sparkConf("SparkStreamingReadKafka","")

    //sparkStreaming上下文
    var ssc = new StreamingContext(sc,Seconds(2))

    /**
      * kafka conf
      **/
    val kafkaParam: Map[String, String] = Map[String, String](
      //-----------kafka低级api配置-----------
      //----------配置zookeeper-----------
      "zookeeper.connect" -> "namenode2.cdh:2181,datanode3.cdh:2181,namenode1.cdh:2181",
      "metadata.broker.list" -> "namenode1.cdh:9092",
      //设置一下group id
      "group.id" -> "spark_xing",
      //----------从该topic最新的位置开始读数------------
      //"auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString,
      "client.id" -> "spark_xing",
      "zookeeper.connection.timeout.ms" -> "10000"
    )

    val topicSet: Set[String] = Set("crm_datasync_customer")
    val directKafka: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicSet)
    val lines: DStream[String] = directKafka.map((x) =>  x._2)

    lines.map(preJson => {

      if(preJson.toString.isEmpty){
        println("我是空的")
      }

      var json: String = preJson.mkString("")

      //获取三级目录s
      var res = JSON.parseObject(JSON.parseObject(json).get("data").toString).get("list").toString

      val nObject: JSONArray = JSON.parseArray(res)

      val value: Array[AnyRef] = nObject.toArray()

      //获取基础字段信息baseInfo
      val baseInfo: Array[String]= getBaseInfo(value)

      //获取可变字段函数
      val customField: Array[String] = getCustomField(value)

      //合并baseInfo和可变字段数据
      var baseInfo_CustomField: Map[String, String] =  getMerge(baseInfo,customField)

      var baseInfo_CustomField_List: List[(String, String)] = baseInfo_CustomField.toList

      baseInfo_CustomField_List

    }).foreachRDD(rdd => {

      if(rdd.isEmpty()){
        println("我是空的")
      }

      if(!rdd.isEmpty()){

        rdd.foreachPartition(partitionOfRecords  => {
          //HBaseConf
          val conf = HbaseConf("crm_customer")._1
          val tableName = "crm_customer"
          val columnFamily1 = "baseInfo"
          val columnFamily2 = "customField"

          var connection: Connection = ConnectionFactory.createConnection(conf)

          var table = connection.getTable(TableName.valueOf(tableName))

          println(table.getName)

          partitionOfRecords.foreach(logData =>{

            var list: List[(String, String)] = logData

            for(res <- list){

              println((res._1,res._2))

              val put = new Put(Bytes.toBytes(String.valueOf(res._1)))

              //获得全部数据
              var mergeData: Array[String] = res._2.split("\\|")

              //获得数组长度
              var len = mergeData.length
              for (x <- 0 to len-2){
                var keyValue = mergeData(x).split("=")
                //                  println(keyValue(0))
                if(keyValue.length == 2) {
                  if(keyValue(1) != null && !"null".equals(keyValue(1))){
                    put.addColumn(Bytes.toBytes(columnFamily1),Bytes.toBytes(keyValue(0)),Bytes.toBytes(keyValue(1)))
                  }
                }
              }

              //用户自定义数据
              val field = mergeData(len-1).split("\\^")

              val fieldLen = field.length

              println(fieldLen)

              for(z <- 0 to fieldLen-1){
                val keyValue = field(z).split("=")
                if(keyValue.length == 2) {
                  if(keyValue(1) != null && !"null".equals(keyValue(1))){
                    put.addColumn(Bytes.toBytes(columnFamily2),Bytes.toBytes(keyValue(0)),Bytes.toBytes(keyValue(1)))
                  }
                }
              }

              table.put(put)
            }

            table.close()
          })
        })
      }

    })

    ssc.start()
    ssc.awaitTermination()
  }
}


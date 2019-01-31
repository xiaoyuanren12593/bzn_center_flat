package test

import Util.Spark_Util
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import test.ParseJson._

object SparkStreamingReadKaf2HbaseBuss {
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
      "group.id" -> "spark_xing_buss",
      //----------从该topic最新的位置开始读数------------
      //"auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString,
      "client.id" -> "spark_xing_buss",
      "zookeeper.connection.timeout.ms" -> "10000"
    )

    //topic
    val topicSet: Set[String] = Set("crm_datasync_niche")

    //direct方式连接kafka
    val directKafka: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicSet)

    //取出消息
    val lines: DStream[String] = directKafka.map((x) =>  x._2)

    lines.map(preJson => {

      //将json数据拉平
      var json: String = preJson.mkString("")

      //获取三级目录s
      var res = JSON.parseObject(JSON.parseObject(json).get("data").toString).get("list").toString

      //解析json数组
      val nObject: JSONArray = JSON.parseArray(res)

      val value: Array[AnyRef] = nObject.toArray()

      //获取基础字段信息baseInfo
      val baseInfo: Array[String]= getBaseInfo_Buss(value)

      //获取可变字段函数
      val customField: Array[String] = getCustomField(value)

      //合并baseInfo和可变字段数据
      var baseInfo_CustomField: Map[String, String] =  getMerge(baseInfo,customField)

      var baseInfo_CustomField_List: List[(String, String)] = baseInfo_CustomField.toList

      baseInfo_CustomField_List

    }).foreachRDD(rdd => {

      //如果rdd不是空的情况下，执行数据处理操作，防止空的请求hbase
      if(!rdd.isEmpty()){
        rdd.foreachPartition(partitionOfRecords  => {

          //HBaseConf
          val conf = HbaseConf("crm_niche")._1
          val tableName = "crm_niche"
          val columnFamily1 = "baseInfo"
          val columnFamily2 = "customField"

          //获取连接
          var connection: Connection = ConnectionFactory.createConnection(conf)

          //获取表
          var table = connection.getTable(TableName.valueOf(tableName))

          println(table.getName)

          partitionOfRecords.foreach(logData =>{

            var list: List[(String, String)] = logData

            //遍历每一条数据_1  key   _2是value
            for(res <- list){

//              println((res._1,res._2))

              var keys = res._1.split("=")

              //存储key到Put
              val put = new Put(Bytes.toBytes(String.valueOf(keys(1))))

              //获得全部数据
              var mergeData: Array[String] = res._2.split("\\|")

              //获得数组长度
              var len = mergeData.length
              for (x <- 0 to len-2){
                var keyValue = mergeData(x).split("=")
                //println(keyValue(0))
                if(keyValue.length == 2) {
                  if(keyValue(1) != null && !"null".equals(keyValue(1))){
                    put.addColumn(Bytes.toBytes(columnFamily1),Bytes.toBytes(keyValue(0)),Bytes.toBytes(keyValue(1)))
                  }
                }
              }

              //用户自定义数据
              val field = mergeData(len-1).split("\\^")

              val fieldLen = field.length

//              println(fieldLen)

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

  /**
    * 获取baseInfo和可变字段的数据
    * @param baseInfo
    * @param customField
    * @return
    */
  def getMerge(baseInfo:Array[String],customField:Array[String]): Map[String, String] ={
    //定义可变map存储json结果
    var emptyMap: Map[String, String] = new scala.collection.immutable.HashMap[String, String]

    var i = 0
    var baseInfo_CustomField =  baseInfo.map(x => {
      var builder = new StringBuilder
      val spr = x.split("&")
      var key: String = spr(0)
      var value: String = spr(1)+customField(i)
      i += 1
      emptyMap += (key -> value)
    })

    emptyMap
  }
  /**
    * 获取可变字段函数
    * @param value
    * @return
    */
  def getCustomField(value : Array[AnyRef]): Array[String] ={
    var res1: Array[String] = value.map(x => {
      //解析json  list下的key-value
      var res = JSON.parseObject(JSON.parseObject(x.toString).toString).toString
      //存储可变字段key和value
      val builder = new StringBuilder
      //key
      var fieldCode = ""
      //value
      var context = ""
      //判断key中是否有新增字段，并进行解析，然后拼接成字符串
      if( (JSON.parseObject(res.toString).get("fieldMap").toString).length != 2){
        res = JSON.parseObject(res.toString).get("fieldMap").toString
        var re: JSONObject = JSON.parseObject(res.toString)
        var keys = re.keySet()
        val itr = keys.iterator()
        while(itr.hasNext()){
          var key = itr.next()
          context = JSON.parseObject(re.get(key).toString).get("content").toString
          builder.append(key+"="+context+"^")
        }
        builder.toString().substring(0,builder.toString().length-1)
      }else{
        builder.toString()
      }
    })
    //    res1.foreach(println)
    res1
  }

  /**
    *  获取baseInfo数据
    * @param value
    */
  def getBaseInfo_Buss(value : Array[AnyRef]): Array[String] ={
    var baseInfo: Array[String] = value.map(x => {
      val id = JSON.parseObject(x.toString).get("id")//客户id
      val name = JSON.parseObject(x.toString).get("name")//客户名称
      val remark = JSON.parseObject(x.toString).get("remark")//备注
      val customerId = JSON.parseObject(x.toString).get("customerId")//客户组
      val saleProcess = JSON.parseObject(x.toString).get("saleProcess")//销售阶段
      val preSignDate = JSON.parseObject(x.toString).get("preSignDate")//结束时间
      val loseReason = JSON.parseObject(x.toString).get("loseReason")//输单原因
      val winRate = JSON.parseObject(x.toString).get("winRate")//赢率
      val createTime = JSON.parseObject(x.toString).get("createTime")//创建时间

      var updateTime = ""
      if(x.toString.contains("updateTime")){
        val updateTime = JSON.parseObject(x.toString).get("updateTime")//修改时间.toString//最近修改时间
      }

      val changeTimestamp = JSON.parseObject(x.toString).get("changeTimestamp")//最近修改时间
      val recentActivityRecordTime = JSON.parseObject(x.toString).get("recentActivityRecordTime")//最新活动记录时间
      val customer_name = JSON.parseObject(JSON.parseObject(x.toString).get("customer").toString).get("name")
      val businessCategory_name = JSON.parseObject(JSON.parseObject(x.toString).get("businessCategory").toString).get("name")//商机类型
      val master_realname = JSON.parseObject(JSON.parseObject(x.toString).get("master").toString).get("realname")//所有人
      val createUser_realname = JSON.parseObject(JSON.parseObject(x.toString).get("createUser").toString).get("realname")//创建人

      var updateUser_realname  = ""
      if(x.toString.contains("updateUser")){
        updateUser_realname = JSON.parseObject(JSON.parseObject(x.toString).get("updateUser").toString).get("realname").toString//最近修改人
      }

      var test  = "id="+id+
        "&name=" +name+
        "|remark=" +remark+
        "|customerId=" +customerId+
        "|customer_name=" +customer_name+
        "|saleProcess=" +saleProcess+
        "|preSignDate=" +preSignDate+
        "|loseReason=" +loseReason+
        "|businessCategory_name=" +businessCategory_name+
        "|winRate=" +winRate+
        "|master_realname=" +master_realname+
        "|createUser_realname=" +createUser_realname+
        "|createTime=" +createTime+
        "|updateTime=" +updateTime+
        "|updateUser_realname=" +updateUser_realname+
        "|changeTimestamp=" +changeTimestamp+
        "|recentActivityRecordTime=" +recentActivityRecordTime+"|"

      test
    })
    baseInfo
  }
}

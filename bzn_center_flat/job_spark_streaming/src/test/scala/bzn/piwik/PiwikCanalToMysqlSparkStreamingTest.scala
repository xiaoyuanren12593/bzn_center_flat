package bzn.piwik

import java.lang
import java.sql.{Date, Timestamp}

import bzn.job.common.WareUntil
import bzn.util.SparkUtil
import bzn.utils.MySQLPoolManager
import bzn.utils.MySQLUtils.getInsertOrUpdateSql
import com.alibaba.fastjson.{JSON}
import kafka.serializer.StringDecoder
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
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

    getPiwikDataToMysql(hqlContext,strContext,kafkaParam,topicSet)
  }
  /**
    * 实时读取kafka数据到MySQL
    * @param strContext
    */
  def getPiwikDataToMysql(sqlContext:HiveContext,strContext:StreamingContext,kafkaParam: Map[String, String],topicSet:Set[String]) = {
    /**
      * 读取kafka的数据
      */
    val directKafka: InputDStream[(String, String)] =
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](strContext, kafkaParam, topicSet)
    val lines: DStream[String] = directKafka.map(x => x._2)

    import sqlContext.implicits._

    lines.foreachRDD(rdds => {
      if(!rdds.isEmpty()){
        val canalTestDataRdd = rdds.filter(x => x.contains("canal_test"))
        val canalTestData1Rdd = rdds.filter(x => x.contains("canal_test"))
        canalTestDataRdd.filter(x => x.contains("INSERT") || x.contains("UPDATE")).foreachPartition( partitons => {
          /**
            * 列的个数
            */
//          val colNumbers = 2
//          val sql = getInsertOrUpdateSql(tableName, resultDateFrame.columns, updateColumns)
//          val columnDataTypes = dataType:Array[String]
          partitons.foreach(x => {
            println(x)
            /**
              * 获取data数据-存储插入和更新的值
              */
            val jsonDataObject = JSON.parseObject(x).get("data")

            /**
              * 将data中的存储的数组解析成单个的数据，便于分开
              */
            val jsonArray = JSON.parseArray(jsonDataObject.toString)

            val array = jsonArray.toArray

            val res = array.map(t => {
              val data = JSON.parseObject(t.toString)
              val database = JSON.parseObject(x).get("database").toString
              val pkNames = JSON.parseArray(JSON.parseObject(x).get("pkNames").toString).toArray()(0).toString
              val table = JSON.parseObject(x).get("table").toString
              val sqlType = JSON.parseObject(x).get("type").toString
              val name = data.getString("name")
              val idzz = data.getInteger("idzz")
              val value = data.getDouble("value")
              (database,pkNames,table,sqlType,name,idzz,value)
            }).toSeq

            res.foreach(println)
          })
        })
      }else{

      }
    })


    strContext.start()
    strContext.awaitTermination()
  }

  /**
    * 通过insertOrUpdate的方式把DataFrame写入到MySQL中，注意：此方式，必须对表设置主键
    * @param tableName
    * @param resultDateFrame
    * @param updateColumns
    */
  def insertOrUpdateDFtoDBUsePool(tableName: String, resultDateFrame: DataFrame,insertColumns:Array[String],dataType:Array[String], updateColumns: Array[String]) {
    /**
      * 列的个数
      */
    val colNumbers = insertColumns.length-1
    val sql = getInsertOrUpdateSql(tableName, resultDateFrame.columns, updateColumns)
    val columnDataTypes = dataType:Array[String]
    println("############## sql = " + sql)
    resultDateFrame.foreachPartition(partitionRecords => {
      val conn = MySQLPoolManager.getMysqlManager.getConnection //从连接池中获取一个连接
      val preparedStatement = conn.prepareStatement(sql)
      val metaData = conn.getMetaData.getColumns(null, "%", tableName, "%") //通过连接获取表名对应数据表的元数据
      try {
        conn.setAutoCommit(false)
        partitionRecords.foreach(f = record => {
          //注意:setString方法从1开始，record.getString()方法从0开始
          for(i <- 1 to colNumbers) {
            val value = record.get (i - 1)
            val dateType = columnDataTypes (i - 1)
            if( value != null ) { //如何值不为空,将类型转换为String
              preparedStatement.setString (i, value.toString)
              dateType match {
                case  "Byte" => preparedStatement.setInt (i, record.getAs [Int](i - 1))
                case  "Short" => preparedStatement.setInt (i, record.getAs [Int](i - 1))
                case  "Integer" => preparedStatement.setInt (i, record.getAs [Int](i - 1))
                case  "Long" => preparedStatement.setLong (i, record.getAs [Long](i - 1))
                case  "Boolean" => preparedStatement.setInt (i, if( record.getAs [Boolean](i - 1) ) 1 else 0)
                case  "Float" => preparedStatement.setFloat (i, record.getAs [Float](i - 1))
                case  "Double" => preparedStatement.setDouble (i, record.getAs [Double](i - 1))
                case  "String" => preparedStatement.setString (i, record.getAs [String](i - 1))
                case  "Timestamp" => preparedStatement.setTimestamp (i, record.getAs [Timestamp](i - 1))
                case  "Date" => preparedStatement.setDate (i, record.getAs [Date](i - 1))
                case _ => throw new RuntimeException (s"nonsupport ${dateType} !!!")
              }
            } else { //如果值为空,将值设为对应类型的空值
              metaData.absolute (i)
              preparedStatement.setNull (i, metaData.getInt ("DATA_TYPE"))
            }
          }
          //设置需要更新的字段值
          for(i <- 1 to updateColumns.length) {
            val fieldIndex = record.fieldIndex (updateColumns (i - 1))
            val value = record.get (fieldIndex)
            val dataType = columnDataTypes (fieldIndex)
            //println(s"@@ $fieldIndex,$value,$dataType")
            if( value != null ) { //如何值不为空,将类型转换为String
              dataType match {
                case "Byte" => preparedStatement.setInt (colNumbers + i, record.getAs [Int](fieldIndex))
                case "Short" => preparedStatement.setInt (colNumbers + i, record.getAs [Int](fieldIndex))
                case "Integer" => preparedStatement.setInt (colNumbers + i, record.getAs [Int](fieldIndex))
                case "Long" => preparedStatement.setLong (colNumbers + i, record.getAs [Long](fieldIndex))
                case "Boolean" => preparedStatement.setBoolean (colNumbers + i, record.getAs [Boolean](fieldIndex))
                case "Float" => preparedStatement.setFloat (colNumbers + i, record.getAs [Float](fieldIndex))
                case "Double" => preparedStatement.setDouble (colNumbers + i, record.getAs [Double](fieldIndex))
                case "String" => preparedStatement.setString (colNumbers + i, record.getAs [String](fieldIndex))
                case "Timestamp" => preparedStatement.setTimestamp (colNumbers + i, record.getAs [Timestamp](fieldIndex))
                case "Date" => preparedStatement.setDate (colNumbers + i, record.getAs [Date](fieldIndex))
                case _ => throw new RuntimeException (s"nonsupport ${dataType} !!!")
              }
            } else { //如果值为空,将值设为对应类型的空值
              metaData.absolute (fieldIndex)
              preparedStatement.setNull (colNumbers + i, metaData.getInt ("DATA_TYPE"))
            }
          }
          preparedStatement.addBatch ()
        })
        preparedStatement.executeBatch()
        conn.commit()
      } catch {
        case e: Exception => println(s"@@ insertOrUpdateDFtoDBUsePool ${e.getMessage}")
        // do some log
      } finally {
        preparedStatement.close()
        conn.close()
      }
    })
  }

}

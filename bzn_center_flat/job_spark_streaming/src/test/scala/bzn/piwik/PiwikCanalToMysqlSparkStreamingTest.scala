package bzn.piwik

import java.{io, lang}
import java.sql.{Date, Timestamp}

import bzn.job.common.WareUntil
import bzn.util.SparkUtil
import bzn.utils.MySQLPoolManager
import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
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
      if(!rdds.isEmpty()) {
        val canalTestDataRdd = rdds.filter (x => x.contains ("canal_test"))

        val canalData = canalTestDataRdd.filter (x => x.contains ("INSERT") || x.contains ("UPDATE"))

        val colNumbers = 3
        val insertColumns = Array ("name", "idzz", "value")
        val updateColumns = Array ("name", "value")
        val sql = getInsertOrUpdateSql ("canal_test", insertColumns, updateColumns)
        val columnDataTypes = Array [String]("String", "Int", "Double")
        val tableName = "canal_test"

        insertOrUpdateDFtoDBUsePool (strContext,tableName, canalData, insertColumns, columnDataTypes, updateColumns, colNumbers, sql)
      }else{

      }
    })
    strContext.start()
    strContext.awaitTermination()
  }
  
  /**
    * 通过insertOrUpdate的方式把DataFrame写入到MySQL中，注意：此方式，必须对表设置主键
    * @param tableName
    * @param rdd
    * @param updateColumns
    */
  def insertOrUpdateDFtoDBUsePool(strContext:StreamingContext,tableName: String, rdd: RDD[String],insertColumns:Array[String],columnDataTypes:Array[String],
                                  updateColumns: Array[String],colNumbers:Int,sql:String) {
    println("############## sql = " + sql)
    rdd.foreachPartition(partitionRecords => {
      val conn = MySQLPoolManager.getMysqlManager.getConnection //从连接池中获取一个连接
      val preparedStatement = conn.prepareStatement(sql)
      val metaData = conn.getMetaData.getColumns(null, "%", tableName, "%") //通过连接获取表名对应数据表的元数据
      try {
        conn.setAutoCommit(false)
        partitionRecords.foreach(f = record => {
          /**
            * 获取data数据-存储插入和更新的值
            */
          val jsonDataObject = JSON.parseObject(record).get("data")

          /**
            * 将data中的存储的数组解析成单个的数据，便于分开
            */
          val jsonArray = JSON.parseArray(jsonDataObject.toString)

          val array = jsonArray.toArray

          val res = array.map(t => {
            val data = JSON.parseObject(t.toString)
            val database = JSON.parseObject(record).get("database").toString
            val pkNames = JSON.parseArray(JSON.parseObject(record).get("pkNames").toString).toArray()(0).toString
            val table = JSON.parseObject(record).get("table").toString
            val sqlType = JSON.parseObject(record).get("type").toString
            val name = data.getString("name")
            val idzz = data.getInteger("idzz")
            val value = data.getDouble("value")
            Array(name,idzz,value,database,pkNames,table,sqlType)
          })

          //注意:setString方法从1开始，record.getString()方法从0开始
          for (x <- 1 to res.length){
            for(i <- 1 to colNumbers) {
              val value = res(x-1)(i-1)
              val dateType= columnDataTypes (i-1)
              if( value != null ) { //如何值不为空,将类型转换为String
                preparedStatement.setString (i, value.toString)
                dateType match {
                  case  "Byte" => preparedStatement.setInt (i, res(x - 1)(i-1).toString().toInt)
                  case  "Short" => preparedStatement.setInt (i, res(x - 1)(i-1).toString().toInt)
                  case  "Int" => preparedStatement.setInt (i, res(x - 1)(i-1).toString().toInt)
                  case  "Long" => preparedStatement.setLong (i, res(x - 1)(i-1).toString().toLong)
                  case  "Boolean" => preparedStatement.setInt (i, if(res(x - 1)(i-1).toString() == "1" ) 1 else 0)
                  case  "Float" => preparedStatement.setFloat (i, res(x - 1)(i-1).toString().toFloat)
                  case  "Double" => {
                    preparedStatement.setDouble (i,res(x - 1)(i-1).toString().toDouble)
                    println("zhege  haowan ")
                  }
                  case  "String" => preparedStatement.setString (i, res(x - 1)(i-1).toString)
                  case  "Timestamp" => preparedStatement.setTimestamp (x, Timestamp.valueOf(res(i - 1)(i-1).toString()))
                  case  "Date" => preparedStatement.setDate (i, Date.valueOf(res(x - 1)(i-1).toString))
                  case _ => throw new RuntimeException (s"nonsupport ${dateType} !!!")
                }
              } else { //如果值为空,将值设为对应类型的空值
                metaData.absolute (i)
                preparedStatement.setNull (i, metaData.getInt ("DATA_TYPE"))
              }
            }
            //设置需要更新的字段值
            for(i <- 1 to updateColumns.length) {
              println("updateColumns"+updateColumns(i-1))
              val fieldIndex = insertColumns.indexOf(updateColumns(i-1))
              println("fieldIndex"+fieldIndex)
              val value = res(x-1)(fieldIndex)
              println("value"+value)
              val dataType = columnDataTypes (fieldIndex)
              //println(s"@@ $fieldIndex,$value,$dataType")
              if( value != null ) { //如何值不为空,将类型转换为String
                dataType match {
                  case  "Byte" => preparedStatement.setInt (colNumbers+i, res(x - 1)(i-1).toString().toInt)
                  case  "Short" => preparedStatement.setInt (colNumbers+i, res(x - 1)(i-1).toString().toInt)
                  case  "Int" => preparedStatement.setInt (colNumbers+i, res(x - 1)(i-1).toString().toInt)
                  case  "Long" => preparedStatement.setLong (colNumbers+i, res(x - 1)(i-1).toString().toLong)
                  case  "Boolean" => preparedStatement.setInt (colNumbers+i, if(res(x - 1)(i-1).toString() == "1" ) 1 else 0)
                  case  "Float" => preparedStatement.setFloat (colNumbers+i, res(x - 1)(i-1).toString().toFloat)
                  case  "Double" => {
                    preparedStatement.setDouble (colNumbers+i,res(x - 1)(i-1).toString().toDouble)
                    println("zhege  haowan ")
                  }
                  case  "String" => preparedStatement.setString (colNumbers+i, res(x - 1)(i-1).toString)
                  case  "Timestamp" => preparedStatement.setTimestamp (x, Timestamp.valueOf(res(i - 1)(i-1).toString()))
                  case  "Date" => preparedStatement.setDate (colNumbers+i, Date.valueOf(res(x - 1)(i-1).toString))
                  case _ => throw new RuntimeException (s"nonsupport ${dataType} !!!")
                }
              } else { //如果值为空,将值设为对应类型的空值
                metaData.absolute (fieldIndex)
                preparedStatement.setNull (colNumbers + i, metaData.getInt ("DATA_TYPE"))
              }
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
  /**
    * 拼装insertOrUpdate SQL 语句
    * @param tableName
    * @param cols
    * @param updateColumns
    * @return
    */
  def getInsertOrUpdateSql(tableName: String, cols: Array[String], updateColumns: Array[String]): String = {
    val colNumbers = cols.length
    var sqlStr = "insert into "+tableName+" values("
    for (i <- 1 to colNumbers) {
      sqlStr += "?"
      if (i != colNumbers) {
        sqlStr += ", "
      }
    }
    sqlStr += ") ON DUPLICATE KEY UPDATE "

    updateColumns.foreach(str => {
      sqlStr += s"$str = ?,"
    })

    sqlStr.substring(0, sqlStr.length - 1)
  }
}

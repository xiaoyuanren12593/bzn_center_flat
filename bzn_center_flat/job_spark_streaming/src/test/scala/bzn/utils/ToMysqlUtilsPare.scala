package bzn.utils

import java.sql.{Date, ResultSet, Timestamp}

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer

/**
  * author:xiaoYuanRen
  * Date:2019/12/3
  * Time:17:24
  * describe: spark操作mysql的写入/更新/删除/查询
  **/
trait ToMysqlUtilsPare {

  /**
    * 实时删除mysql数据
    * @param strContext sparkStreaming上下文
    * @param dsTreamData streamingData
    */
  def getPiwikDataDelete(strContext:StreamingContext,dsTreamData:DStream[String],deleteArray:(Array[String], Int, Array[String], String, String)) = {
    /**
      * 主键的字段
      */
    val idColumns = deleteArray._1

    val colNumbers = deleteArray._2

    /**
      * 字段类型
      */
    val columnDataTypes = deleteArray._3

    /**
      * 表名
      */
    val tableName = deleteArray._4

    /**
      * 主键
      */
    val id = deleteArray._5

    /**
      * 删除的sql
      */
    val sql = getDeleteSql(tableName,id)

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
  def getPiwikDataToMysql(strContext:StreamingContext,dsTreamData:DStream[String],insertArray: (Array[String], Int, Array[String], Array[String], String)): Unit = {

    /**
      * 插入的字段
      */
    val insertColumns = insertArray._1

    val colNumbers = insertArray._2

    /**
      * 更新的字段
      */
    val updateColumns = insertArray._3

    /**
      * 字段类型
      */
    val columnDataTypes = insertArray._4

    /**
      * 插入的表名
      */
    val tableName = insertArray._5

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

  /**
    * 将json的数据转化为数据rdd
    * @param DStreamData dstream数据
    */
  def getJsonToDStream(DStreamData:DStream[String],insertColumn:Array[String],columnDataTypes:Array[String]): DStream[Array[ArrayBuffer[Any]]] = {
    val result = DStreamData.map(record => {

      /**
        * 获取data数据-存储插入和更新的值
        */
      val jsonDataObject = JSON.parseObject(record).get("data")

      /**
        * 将data中的存储的数组解析成单个的数据，便于分开
        */
      val jsonArray = JSON.parseArray(jsonDataObject.toString)

      /**
        * 转化为单个的数组
        */
      val array = jsonArray.toArray

      val res = array.map(t => {
        val data = JSON.parseObject(t.toString)
        val database = JSON.parseObject(record).get("database").toString
        val pkNames = JSON.parseArray(JSON.parseObject(record).get("pkNames").toString).toArray()(0).toString
        val table = JSON.parseObject(record).get("table").toString
        val sqlType = JSON.parseObject(record).get("type").toString
        val oldData = JSON.parseObject(record).get("old")

        val colNumbers = insertColumn.length

//        val byte = data.toString.getBytes("ISO8859-1")
//
//        val str = new String(byte,"ISO8859-1")
//
//        println("str      "+str)
//
//        println (JSON.parseObject (str).get ("idvisitor"))

        val columnArray = scala.collection.mutable.ArrayBuffer[Any]()

        for(i <- 1 to colNumbers) {
          val columnValue = insertColumn(i-1)
          val dateType= columnDataTypes (i-1)
//          println(data.get(columnValue) + "  ")
          if( columnValue != null ) { //如何值不为空,将类型转换为String
            dateType match {
              case  "Byte" => columnArray.insert(i-1,data.getInteger(columnValue))
              case  "Short" => columnArray.insert(i-1,data.getInteger(columnValue))
              case  "Int" => columnArray.insert(i-1,data.getInteger(columnValue))
              case  "Long" => columnArray.insert(i-1,data.getLong(columnValue))
              case  "Boolean" => columnArray.insert(i-1,if(data.getBoolean(columnValue)) 1 else 0)
              case  "Float" => columnArray.insert(i-1,data.getFloat(columnValue))
              case  "Double" => columnArray.insert(i-1,data.getDouble(columnValue))
              case  "Decimal" => columnArray.insert(i-1,data.getBigDecimal(columnValue))
              case  "String" => columnArray.insert(i-1,data.getString(columnValue))
              case  "Timestamp" => columnArray.insert(i-1,data.getString(columnValue))
              case  "Date" => columnArray.insert(i-1,data.getString(columnValue))
              case _ => throw new RuntimeException (s"nonsupport $dateType !!!")
            }
          }
        }
        columnArray ++ Array(database,pkNames,table,sqlType)
      })
      for(elem <- res) {elem.foreach(x => print(x + "--->"))}
      println()
      res
    })
    result
  }

  /**
    * 通过insertOrUpdate的方式把DataFrame写入到MySQL中，注意：此方式，必须对表设置主键
    * @param tableName 表名字
    * @param rdd rdd结果集
    * @param updateColumns 更新字段的数组
    */
  def  insertOrUpdateDFtoDBUsePool(strContext:StreamingContext,tableName: String, rdd: RDD[Array[ArrayBuffer[Any]]],insertColumns:Array[String],columnDataTypes:Array[String],
                                  updateColumns: Array[String],colNumbers:Int,sql:String): Unit = {

    rdd.foreachPartition(partitionRecords => {
      val conn = MySQLPoolManagerBlackGray.getMysqlManager().getConnection //从连接池中获取一个连接
      val preparedStatement = conn.prepareStatement(sql)
      val metaData: ResultSet = conn.getMetaData.getColumns(null, "%", tableName, "%") //通过连接获取表名对应数据表的元数据
      try {
        conn.setAutoCommit(false)
        partitionRecords.foreach(f = record => {

          val arrayOne = record//数组第一层

          //注意:setString方法从1开始，record.getString()方法从0开始
          for (x <- 1 to arrayOne.length){
            for(i <- 1 to colNumbers) {
              val value = arrayOne(x-1)(i-1)
              val dateType= columnDataTypes (i-1)
              if( value != null ) { //如何值不为空,将类型转换为String
                preparedStatement.setString (i, value.toString)
                dateType match {
                  case  "Byte" => preparedStatement.setInt (i, arrayOne(x - 1)(i-1).toString.toInt)
                  case  "Short" => preparedStatement.setInt (i, arrayOne(x - 1)(i-1).toString.toInt)
                  case  "Int" => preparedStatement.setInt (i, arrayOne(x - 1)(i-1).toString.toInt)
                  case  "Long" => preparedStatement.setLong (i, arrayOne(x - 1)(i-1).toString.toLong)
                  case  "Boolean" => preparedStatement.setInt (i, if(arrayOne(x - 1)(i-1).toString== "1" ) 1 else 0)
                  case  "Float" => preparedStatement.setFloat (i, arrayOne(x - 1)(i-1).toString.toFloat)
                  case  "Double" => preparedStatement.setDouble (i,arrayOne(x - 1)(i-1).toString.toDouble)
                  case  "Decimal" => preparedStatement.setBigDecimal(i,java.math.BigDecimal.valueOf(arrayOne(x - 1)(i-1).toString.toDouble))
                  case  "String" => preparedStatement.setString (i, arrayOne(x - 1)(i-1).toString)
                  case  "Timestamp" => preparedStatement.setTimestamp (i, Timestamp.valueOf(arrayOne(i - 1)(i-1).toString))
                  case  "Date" => preparedStatement.setDate (i, Date.valueOf(arrayOne(x - 1)(i-1).toString))
                  case _ => throw new RuntimeException (s"nonsupport $dateType !!!")
                }
              } else { //如果值为空,将值设为对应类型的空值
                metaData.absolute (i)
                preparedStatement.setNull (i, metaData.getInt ("DATA_TYPE"))
              }
            }
            //设置需要更新的字段值
            for(i <- 1 to updateColumns.length) {
              val fieldIndex = insertColumns.indexOf(updateColumns(i-1))
              val value = arrayOne(x-1)(fieldIndex)
              val dataType = columnDataTypes (fieldIndex)
//              println(s"@@ $i   $fieldIndex,$value,$dataType")
//              println(s"%% $i   " +"dataType  "+ dataType  +"  "+ arrayOne(x - 1)(fieldIndex))  //测试
              if( value != null ) { //如何值不为空,将类型转换为String
                dataType match {
                  case  "Byte" => preparedStatement.setInt (colNumbers+i, arrayOne(x - 1)(fieldIndex).toString.toInt)
                  case  "Short" => preparedStatement.setInt (colNumbers+i, arrayOne(x - 1)(fieldIndex).toString.toInt)
                  case  "Int" => preparedStatement.setInt (colNumbers+i, arrayOne(x - 1)(fieldIndex).toString.toInt)
                  case  "Long" => preparedStatement.setLong (colNumbers+i, arrayOne(x - 1)(fieldIndex).toString.toLong)
                  case  "Boolean" => preparedStatement.setInt (colNumbers+i, if(arrayOne(x - 1)(fieldIndex).toString == "1" ) 1 else 0)
                  case  "Float" => preparedStatement.setFloat (colNumbers+i, arrayOne(x - 1)(fieldIndex).toString.toFloat)
                  case  "Double" => preparedStatement.setDouble (colNumbers+i,arrayOne(x - 1)(fieldIndex).toString.toDouble)
                  case  "Decimal" => preparedStatement.setBigDecimal(colNumbers+i,java.math.BigDecimal.valueOf(arrayOne(x - 1)(i-1).toString.toDouble))
                  case  "String" => preparedStatement.setString (colNumbers+i, arrayOne(x - 1)(fieldIndex).toString)
                  case  "Timestamp" => preparedStatement.setTimestamp (colNumbers+i, Timestamp.valueOf(arrayOne(i - 1)(fieldIndex).toString))
                  case  "Date" => preparedStatement.setDate (colNumbers+i, Date.valueOf(arrayOne(x - 1)(fieldIndex).toString))
                  case _ => throw new RuntimeException (s"nonsupport $dataType !!!")
                }
              } else { //如果值为空,将值设为对应类型的空值
                metaData.absolute (fieldIndex+1)
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
    * 删除表中的护具
    * @param strContext streamingcontext上下文
    * @param tableName 表名
    * @param rdd 预处理后的结果集
    * @param insertColumns 插入字段
    * @param columnDataTypes 字段类型
    * @param colNumbers 列数
    * @param sql sql语句
    */
  def deleteMysqlTableDataBatch(strContext:StreamingContext,tableName: String, rdd: RDD[Array[ArrayBuffer[Any]]],insertColumns:Array[String],
                                columnDataTypes:Array[String],colNumbers:Int,sql:String): Unit = {

    rdd.foreachPartition(partitionRecords => {
      val conn = MySQLPoolManagerBlackGray.getMysqlManager().getConnection //从连接池中获取一个连接
      val preparedStatement = conn.prepareStatement(sql)
      val metaData = conn.getMetaData.getColumns(null, "%", tableName, "%") //通过连接获取表名对应数据表的元数据
      try {
        conn.setAutoCommit(false)
        partitionRecords.foreach(record => {
          //注意:setString方法从1开始，record.getString()方法从0开始
          val arrayOne = record//数组第一层：原因：如果更新或者删除可能会同时处理多个

          //注意:setString方法从1开始，record.getString()方法从0开始
          for (x <- 1 to arrayOne.length) {
            for(i <- 1 to colNumbers) {
              val value = arrayOne(x-1)(i-1)
              val dateType = columnDataTypes (i - 1)
              if( value != null ) { //如何值不为空,将类型转换为String
                preparedStatement.setString (i, value.toString)
                dateType match {
                  case  "Byte" => preparedStatement.setInt (i, arrayOne(x - 1)(i-1).toString.toInt)
                  case  "Short" => preparedStatement.setInt (i, arrayOne(x - 1)(i-1).toString.toInt)
                  case  "Int" => preparedStatement.setInt (i, arrayOne(x - 1)(i-1).toString.toInt)
                  case  "Long" => preparedStatement.setLong (i, arrayOne(x - 1)(i-1).toString.toLong)
                  case  "Boolean" => preparedStatement.setInt (i, if(arrayOne(x - 1)(i-1).toString== "1" ) 1 else 0)
                  case  "Float" => preparedStatement.setFloat (i, arrayOne(x - 1)(i-1).toString.toFloat)
                  case  "Double" => preparedStatement.setDouble (i,arrayOne(x - 1)(i-1).toString.toDouble)
                  case  "Decimal" => preparedStatement.setBigDecimal(i,java.math.BigDecimal.valueOf(arrayOne(x - 1)(i-1).toString.toDouble))
                  case  "String" => preparedStatement.setString (i, arrayOne(x - 1)(i-1).toString)
                  case  "Timestamp" => preparedStatement.setTimestamp (i, Timestamp.valueOf(arrayOne(i - 1)(i-1).toString))
                  case  "Date" => preparedStatement.setDate (i, Date.valueOf(arrayOne(x - 1)(i-1).toString))
                  case _ => throw new RuntimeException (s"nonsupport $dateType !!!")
                }
              } else { //如果值为空,将值设为对应类型的空值
                metaData.absolute (i)
                preparedStatement.setNull (i, metaData.getInt ("DATA_TYPE"))
              }
            }
          }
          preparedStatement.addBatch()
        })
        preparedStatement.executeBatch()
        conn.commit()
      } catch {
        case e: Exception => println(s"@@ DeleteDFtoDBUsePool ${e.getMessage}")
        // do some log
      } finally {
        preparedStatement.close()
        conn.close()
      }
    })
  }

  /**
    * 拼装insertOrUpdate SQL 语句
    * @param tableName 表名
    * @param cols 列
    * @param updateColumns 更新列
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

  /**
    * 拼装delete SQL 语句
    * @param tableName 表名
    * @return
    */
  def getDeleteSql(tableName: String,id:String): String = {
    val sqlStr = "delete from "+tableName+" where "+id+" = ?"
    sqlStr
  }
}

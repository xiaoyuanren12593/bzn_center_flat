package bzn.job.common

import java.sql.{Connection, Date, DriverManager, ResultSet, Statement, Timestamp}
import java.util.Properties

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.io.Source

/**
  * author:xiaoYuanRen
  * Date:2019/12/17
  * Time:16:34
  * describe: this is new class
  **/
trait DataBaseUtil {

  /**
    * 通过insertOrUpdate的方式把DataFrame写入到MySQL中，注意：此方式，必须对表设置主键
    * @param tableName
    * @param resultDateFrame
    * @param updateColumns
    */
  def insertOrUpdateDFtoDBUsePoolNew(tableName: String, resultDateFrame: DataFrame, updateColumns: Array[String],
                                     urlFormat:String,userFormat:String,possWordFormat:String,driverFormat:String) {
    val colNumbers = resultDateFrame.columns.length
    val sql = getInsertOrUpdateSql(tableName, resultDateFrame.columns, updateColumns)
    val columnDataTypes = resultDateFrame.schema.fields.map(_.dataType)
    val properties = getProPerties
    val driver = properties.getProperty(driverFormat)
    val address = properties.getProperty(urlFormat)
    val user = properties.getProperty(userFormat)
    val pass = properties.getProperty(possWordFormat)
    println("############## sql = " + sql)
    resultDateFrame.foreachPartition(partitionRecords => {
      Class.forName(driver)
      val conn:Connection = DriverManager.getConnection(address,user,pass)
      val preparedStatement = conn.prepareStatement(sql)
      val metaData = conn.getMetaData.getColumns(null, "%", tableName, "%") //通过连接获取表名对应数据表的元数据
      try {
        conn.setAutoCommit(false)
        partitionRecords.foreach(record => {
          //注意:setString方法从1开始，record.getString()方法从0开始
          for (i <- 1 to colNumbers) {
            val value = record.get(i - 1)
            val dateType = columnDataTypes(i - 1)
            if (value != null) { //如何值不为空,将类型转换为String
//              println(value)
//              preparedStatement.setString(i, value.toString)
              dateType match {
                case _: ByteType => preparedStatement.setInt(i, record.getAs[Int](i - 1))
                case _: ShortType => preparedStatement.setInt(i, record.getAs[Int](i - 1))
                case _: IntegerType => preparedStatement.setInt(i, record.getAs[Int](i - 1))
                case _: LongType => preparedStatement.setLong(i, record.getAs[Long](i - 1))
                case _: BooleanType => preparedStatement.setInt(i, if (record.getAs[Boolean](i - 1)) 1 else 0)
                case _: FloatType => preparedStatement.setFloat(i, record.getAs[Float](i - 1))
                case _: DoubleType => preparedStatement.setDouble(i, record.getAs[Double](i - 1))
                case _: StringType => preparedStatement.setString(i, record.getAs[String](i - 1))
                case _: TimestampType => preparedStatement.setTimestamp(i, record.getAs[Timestamp](i - 1))
                case _: DateType => preparedStatement.setDate(i, record.getAs[Date](i - 1))
                case _ => throw new RuntimeException(s"nonsupport ${dateType} !!!")
              }
            } else { //如果值为空,将值设为对应类型的空值
              metaData.absolute(i)
              preparedStatement.setNull(i, metaData.getInt("DATA_TYPE"))
            }
          }
          //设置需要更新的字段值
          for (i <- 1 to updateColumns.length) {
            val fieldIndex = record.fieldIndex(updateColumns(i - 1))
            val value = record.get(fieldIndex)
            val dataType = columnDataTypes(fieldIndex)
//            println(s"@@ $fieldIndex,$value,$dataType")
            if (value != null) { //如何值不为空,将类型转换为String
              dataType match {
                case _: ByteType => preparedStatement.setInt(colNumbers + i, record.getAs[Int](fieldIndex))
                case _: ShortType => preparedStatement.setInt(colNumbers + i, record.getAs[Int](fieldIndex))
                case _: IntegerType => preparedStatement.setInt(colNumbers + i, record.getAs[Int](fieldIndex))
                case _: LongType => preparedStatement.setLong(colNumbers + i, record.getAs[Long](fieldIndex))
                case _: BooleanType => preparedStatement.setBoolean(colNumbers + i, record.getAs[Boolean](fieldIndex))
                case _: FloatType => preparedStatement.setFloat(colNumbers + i, record.getAs[Float](fieldIndex))
                case _: DoubleType => preparedStatement.setDouble(colNumbers + i, record.getAs[Double](fieldIndex))
                case _: StringType => preparedStatement.setString(colNumbers + i, record.getAs[String](fieldIndex))
                case _: TimestampType => preparedStatement.setTimestamp(colNumbers + i, record.getAs[Timestamp](fieldIndex))
                case _: DateType => preparedStatement.setDate(colNumbers + i, record.getAs[Date](fieldIndex))
                case _ => throw new RuntimeException(s"nonsupport ${dataType} !!!")
              }
            } else { //如果值为空,将值设为对应类型的空值
              metaData.absolute(fieldIndex+1)
              preparedStatement.setNull(colNumbers+i, metaData.getInt("DATA_TYPE"))
            }
          }
          preparedStatement.addBatch()
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

  /**
    * 通过insertOrUpdate的方式把DataFrame写入到MySQL中，注意：此方式，必须对表设置主键
    * @param tableName 表名字
    * @param rdd rdd结果集
    */
  def insertOrUpdateDFtoDBUsePool(tableName: String, rdd: DataFrame,
                                   insertColumns:Array[String],columnDataTypes:Array[String],
                                   colNumbers:Int,sql:String,urlFormat:String,userFormat:String,possWordFormat:String,driverFormat:String): Unit = {

    rdd.foreachPartition(partitionRecords => {
      val properties = getProPerties
      val driver = properties.getProperty(driverFormat)
      val address = properties.getProperty(urlFormat)
      val user = properties.getProperty(userFormat)
      val pass = properties.getProperty(possWordFormat)
      var conn:Connection = null
      Class.forName(driver)
      conn = DriverManager.getConnection(address,user,pass)
      val preparedStatement = conn.prepareStatement(sql)
      val metaData: ResultSet = conn.getMetaData.getColumns(null, "%", tableName, "%") //通过连接获取表名对应数据表的元数据
      try {
        conn.setAutoCommit(false)
        partitionRecords.foreach(f = record => {
          //注意:setString方法从1开始，record.getString()方法从0开始
          for(i <- 1 to colNumbers) {
            val value = record.get(i - 1)
            val dateType= columnDataTypes (i-1)
            if( value != null ) { //如何值不为空,将类型转换为String
              preparedStatement.setString (i, value.toString)
              dateType match {
                case  "Byte" => preparedStatement.setInt (i, record.get(i - 1).toString.toInt)
                case  "Short" => preparedStatement.setInt (i, record.get(i - 1).toString.toInt)
                case  "Int" => preparedStatement.setInt (i, record.get(i - 1).toString.toInt)
                case  "Long" => preparedStatement.setLong (i, record.get(i - 1).toString.toLong)
                case  "Boolean" => preparedStatement.setInt (i, if(record.get(i - 1).toString== "1" ) 1 else 0)
                case  "Float" => preparedStatement.setFloat (i, record.get(i - 1).toString.toFloat)
                case  "Double" => preparedStatement.setDouble (i,record.get(i - 1).toString.toDouble)
                case  "Decimal" => preparedStatement.setBigDecimal(i,java.math.BigDecimal.valueOf(record.get(i - 1).toString.toDouble))
                case  "String" => preparedStatement.setString (i, record.get(i - 1).toString)
                case  "Timestamp" => preparedStatement.setTimestamp (i, Timestamp.valueOf(record.get(i - 1).toString))
                case  "Date" => preparedStatement.setDate (i, Date.valueOf(record.get(i - 1).toString))
                case _ => throw new RuntimeException (s"nonsupport $dateType !!!")
              }
            } else { //如果值为空,将值设为对应类型的空值
              metaData.absolute (i+1)
              preparedStatement.setNull (i, metaData.getInt ("DATA_TYPE"))
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
    * sql
    * @param sql
    * @param urlFormat
    * @param userFormat
    * @param possWordFormat
    * @param driverFormat
    */
  def exeSql(sql:String,urlFormat:String,userFormat:String,possWordFormat:String,driverFormat:String): Unit = {
    val properties = getProPerties
    val driver = properties.getProperty(driverFormat)
    val address = properties.getProperty(urlFormat)
    val user = properties.getProperty(userFormat)
    val pass = properties.getProperty(possWordFormat)
    var connection:Connection = null
    var statement:Statement = null
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(address,user,pass)
      statement = connection.createStatement()
      connection.createStatement()
      val begin = System.currentTimeMillis()
      statement.executeUpdate(sql)
      val end = System.currentTimeMillis()
      System.out.println("执行（"+sql+"）耗时："+(end-begin)+"ms")

    } catch  {
      case e: Exception =>
        e.printStackTrace ()
    }finally {//关闭连接
      try {
        if(statement!=null){
          statement.close()
        }
        if(connection!=null){
          connection.close()
        }
      } catch  {
        case e: Exception =>
          e.printStackTrace ()
      }
    }
  }

  /**
    * 读取ClickHouse表的数据
    * @param sqlContext
    * @param tableName
    * @param url
    * @param user
    * @param possWord
    * @return
    */
  def  readClickHouseTable(sqlContext:SQLContext,tableName: String,url:String,user:String,possWord:String): DataFrame ={
    val prop = getProPerties
    sqlContext.
      read.
      format("jdbc")
      .option("url",prop.getProperty(url))
      .option("user", prop.getProperty(user))
      .option("password", prop.getProperty(possWord))
      .option("driver", "cc.blynk.clickhouse.ClickHouseDriver")
      .option("dbtable",tableName)
      .load()
  }

  /**
    *  写入ClickHouse
    * @param res
    * @param tableName
    * @param saveMode
    * @param url
    * @param user
    * @param possWord
    */
  def writeClickHouseTable(res:DataFrame,tableName: String,saveMode:SaveMode,url:String,user:String,possWord:String,driver:String): Unit ={
    var table = tableName
    val prop = getProPerties
    prop.setProperty("url", prop.getProperty(url))
    prop.setProperty("user", prop.getProperty(user) )
    prop.setProperty("password",  prop.getProperty(possWord))
    prop.setProperty("driver", prop.getProperty(driver))
    if (saveMode == SaveMode.Overwrite) {
      var conn: Connection = null
      try {
        conn = DriverManager.getConnection(
          prop.getProperty("url"),
          prop.getProperty("user"),
          prop.getProperty("password")
        )
        val stmt = conn.createStatement
        table = table.toLowerCase
        stmt.execute(s"truncate table $table")
        conn.close()
      }
      catch {
        case e: Exception =>
          println("Clickhouse Error:")
          e.printStackTrace()
      }
    }
    res
      .write.mode(SaveMode.Append)
      .option("batchsize","100000")
      .option("isolationLevel","NONE") //设置事务
      .option("numPartitions","1")//设置并发
      .jdbc(prop.getProperty("url"),table,prop)
  }

  /**
    * 获取 Mysql 表的数据
    * @param sqlContext
    * @param tableName 读取Mysql表的名字
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, tableName: String,user:String,pass:String,driver:String,url:String): DataFrame = {
    val properties: Properties = getProPerties
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty(url))
      .option("driver", properties.getProperty(driver))
      .option("user", properties.getProperty(user))
      .option("password", properties.getProperty(pass))
      //      .option("numPartitions","10")
      //      .option("partitionColumn","id")
      //      .option("lowerBound", "0")
      //      .option("upperBound","200")
      .option("dbtable", tableName)
      .load()
  }

  /**
    * 将DataFrame保存为Mysql表
    * @param dataFrame 需要保存的dataFrame
    * @param tableName 保存的mysql 表名
    * @param saveMode  保存的模式 ：Append、Overwrite、ErrorIfExists、Ignore
    */
  def saveASMysqlTable(dataFrame: DataFrame, tableName: String, saveMode: SaveMode,user:String,pass:String,driver:String,url:String) = {
    var table = tableName
    val properties: Properties = getProPerties
    val prop = new Properties //配置文件中的key 与 spark 中的 key 不同 所以 创建prop 按照spark 的格式 进行配置数据库
    prop.setProperty("user", properties.getProperty(user))
    prop.setProperty("password", properties.getProperty(pass))
    prop.setProperty("driver", properties.getProperty(driver))
    prop.setProperty("url", properties.getProperty(url))
    if (saveMode == SaveMode.Overwrite) {
      var conn: Connection = null
      try {
        conn = DriverManager.getConnection(
          prop.getProperty("url"),
          prop.getProperty("user"),
          prop.getProperty("password")
        )
        val stmt = conn.createStatement
        table = table.toLowerCase
        stmt.execute(s"truncate table $table") //为了不删除表结构，先truncate 再Append
        conn.close()
      }
      catch {
        case e: Exception =>
          println("MySQL Error:")
          e.printStackTrace()
      }
    }
    dataFrame.write.mode(SaveMode.Append)
      .option("batchsize","20000")
      .option("isolationLevel","NONE")
      .jdbc(prop.getProperty("url"), table, prop)

  }

  /**
    * 获取配置文件
    * @return
    */
  def getProPerties = {
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    var properties: Properties = new Properties()
    for (elem <- lines_source) {
      val split = elem.split("==")
      val key = split(0)
      val value = split(1)
      properties.setProperty(key,value)
    }

    properties
  }
}

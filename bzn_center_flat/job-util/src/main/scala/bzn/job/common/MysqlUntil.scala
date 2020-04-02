package bzn.job.common

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.io.Source

/**
  * author:xiaoYuanRen
  * Date:2019/9/30
  * Time:16:12
  * describe: this is new class
  **/
trait MysqlUntil {

  /**
    * 获取 Mysql 表的数据
    * @param sqlContext
    * @param tableName 读取Mysql表的名字
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, tableName: String,user:String,pass:String,driver:String,url:String): DataFrame = {
    val properties: Properties = getProPerties()
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

  def exeSql(sql:String,url:String,user:String,possWord:String) = {
    val properties = getProPerties()
    val driver = properties.getProperty("clickhouse.driver")
    val address = properties.getProperty("clickhouse.url")
    val user = properties.getProperty("clickhouse.username")
    val pass = properties.getProperty("clickhouse.password")
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
    * 将DataFrame保存为Mysql表
    * @param dataFrame 需要保存的dataFrame
    * @param tableName 保存的mysql 表名
    * @param saveMode  保存的模式 ：Append、Overwrite、ErrorIfExists、Ignore
    */
  def saveASMysqlTable(dataFrame: DataFrame, tableName: String, saveMode: SaveMode,user:String,pass:String,driver:String,url:String) = {
    var table = tableName
    val properties: Properties = getProPerties()
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
      .option("batchsize","50000")
      .option("isolationLevel","NONE")
      .jdbc(prop.getProperty("url"), table, prop)

  }

  /**
    * 获取配置文件
    * @return
    */
  def getProPerties() = {
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

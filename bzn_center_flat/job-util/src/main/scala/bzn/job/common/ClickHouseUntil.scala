package bzn.job.common

import java.util.Properties
import java.sql._
import java.util
import javax.sql.StatementEvent

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.io.Source

/*
* @Author:liuxiang
* @Date：2019/10/12
* @Describe:
*/
trait ClickHouseUntil {

  /**
    * 删除clickhouse数据
    * @param sqlContext
    * @param tableName
    * @param url
    * @param user
    * @param possWord
    */
  def deleteClickhouseData(sqlContext:SQLContext,tableName: String,url:String,user:String,possWord:String) = {

  }

  def exeSql(sql:String,url:String,user:String,possWord:String) = {
    val properties = getProPerties
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
    * 获取配置文件
    *
    * @return
    */
  def getProPerties = {
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    val properties: Properties = new Properties()
    for (elem <- lines_source) {
      val split = elem.split("==")
      val key = split(0)
      val value = split(1)
      properties.setProperty(key,value)
    }
    properties
  }
}

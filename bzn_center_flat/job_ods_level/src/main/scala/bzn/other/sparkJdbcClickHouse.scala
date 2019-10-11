package bzn.other

import java.sql.DriverManager
import java.util.Properties
import bzn.job.common.Until
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import ru.yandex.clickhouse.ClickHouseDriver
import ru.yandex.clickhouse.settings.ClickHouseProperties

import scala.io.Source

/*
* @Author:liuxiang
* @Date：2019/10/10
* @Describe:
*/
object sparkJdbcClickHouse extends  SparkUtil with Until{

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4
    val res: DataFrame = hiveContext.sql("select policy_id,policy_code from odsdb.ods_policy_detail")

    writeTable(res,"odsdb.ods_policy_test")
    sc.stop()

  }

  /**
    *
    */
  def  writeTable(res:DataFrame,tableName: String): Unit ={
    val properties: Properties = getProPerties()
    //配置文件中的key 与 spark 中的 key 不同 所以 创建prop 按照spark 的格式 进行配置数据库

   val clickDriver = new  ClickHouseDriver()
    DriverManager.deregisterDriver(clickDriver)
    val prop = new ClickHouseProperties()
    prop.setHost("jdbc:clickhouse://172.16.11.100:8123/odsdb")
    prop.setUser("default")
    prop.setPassword("iaisYuX4")
    res.write
      .format("jdbc")
      .option("","")
      .option("isolationLevel","NONE") //设置事务
      .option("numPartitions","1")//设置并发
      .mode("Append")

  }

  /**
    * 获取配置文件
    * @return
    */
  def getProPerties() : Properties= {
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

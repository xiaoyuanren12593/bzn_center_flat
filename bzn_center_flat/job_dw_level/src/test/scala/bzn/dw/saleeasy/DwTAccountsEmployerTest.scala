package bzn.dw.saleeasy

import java.util.Properties
import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source

/*
* @Author:liuxiang
* @Date：2019/9/24
* @Describe:
*/
object DwTAccountsEmployerTest extends SparkUtil{

    def main(args: Array[String]): Unit = {

      System.setProperty("HADOOP_USER_NAME", "hdfs")
      val appName = this.getClass.getName
      val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

      val sc = sparkConf._2
      val sqlContext = sparkConf._3
      val hiveContext = sparkConf._4
      val res = readMysqlTable(sqlContext,"t_accounts_employer")
      res.show(10)
      sc.stop()

    }

    /**
      * 获取 Mysql 表的数据
      * @param sqlContext
      * @param tableName 读取Mysql表的名字
      * @return 返回 Mysql 表的 DataFrame
      */
    def readMysqlTable(sqlContext: SQLContext, tableName: String): DataFrame = {
      val properties: Properties = getProPerties()
      sqlContext
        .read
        .format("jdbc")
        .option("url", properties.getProperty("mysql.url"))
        .option("driver", properties.getProperty("mysql.driver"))
        .option("user", properties.getProperty("mysql.username"))
        .option("password", properties.getProperty("mysql.password"))
        .option("numPartitions","10")
        .option("partitionColumn","id")
        .option("lowerBound", "0")
        .option("upperBound","200")
        .option("dbtable", tableName)
        .load()
    }

    /**
      * 获取配置文件
      *
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

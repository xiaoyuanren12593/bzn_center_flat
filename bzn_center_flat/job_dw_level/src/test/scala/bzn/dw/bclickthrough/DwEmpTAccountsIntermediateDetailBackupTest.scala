package bzn.dw.bclickthrough

import java.util.Properties

import bzn.dw.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source

/*
* @Author:liuxiang
* @Date：2019/12/11
* @Describe:
*/
object DwEmpTAccountsIntermediateDetailBackupTest extends SparkUtil with Until {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val sqlContext = sparkConf._3
    val hqlContext: HiveContext = sparkConf._4
    val res = readMysqlOtherTable(hqlContext)
    hqlContext.sql("truncate table dwdb.dw_t_accounts_employer_detail_backup")

     res.write.mode(SaveMode.Append).saveAsTable("dwdb.dw_t_accounts_employer_detail_backup")




    sc.stop()
  }


  def readMysqlOtherTable(hiveContext: HiveContext): DataFrame = {

    val properties: Properties = getProPerties()
    val url1: String = "jdbc:mysql://172.16.11.103:3306/odsdb?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"

    val frame = hiveContext.read.jdbc(url1, "t_accounts_employer", properties)
        .selectExpr("id")

    frame

  }





  /**
    * 获取配置文件
    *
    * @return
    */
  def getProPerties(): Properties = {
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

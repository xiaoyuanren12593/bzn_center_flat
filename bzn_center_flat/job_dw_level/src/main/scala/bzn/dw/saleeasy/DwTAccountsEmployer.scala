package bzn.dw.saleeasy

import java.util.Properties

import bzn.dw.util.SparkUtil
import bzn.job.common.DataBaseUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.io.Source

/*
* @Author:liuxiang
* @Date：2019/9/24
* @Describe:
*/ object DwTAccountsEmployer extends SparkUtil with DataBaseUtil{

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val sqlContext = sparkConf._3
    val hiveContext = sparkConf._4

    val tableName = "ods_t_accounts_employer_detail"
    val tableNameCK = "t_accounts_employer"
    val userMysql = "mysql.username.106"
    val passMysql = "mysql.password.106"
    val driverMysql = "mysql.driver"
    val urlMysql = "mysql.url.106.odsdb"

    val res = readMysqlTable(sqlContext: SQLContext, tableName: String,userMysql:String,passMysql:String,driverMysql:String,urlMysql:String)

//    res.repartition(1).write.mode(SaveMode.Overwrite).parquet("/dw_data/dw_data/dw_t_accounts_employer")

    val url = "clickhouse.url"
//    val urlTest = "clickhouse.url.odsdb.test"
    val user = "clickhouse.username"
    val possWord = "clickhouse.password"
    val driver = "clickhouse.driver"
    writeClickHouseTable(res:DataFrame,tableNameCK: String,SaveMode.Overwrite,url:String,user:String,possWord:String,driver:String)
//    writeClickHouseTable(res:DataFrame,tableName: String,SaveMode.Overwrite,urlTest:String,user:String,possWord:String,driver:String)

    sc.stop()

  }

}

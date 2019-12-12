package bzn.dw.bclickthrough

import bzn.dw.bclickthrough.DwEmpTAccountsIntermediateDetailBackup.{readMysqlTable, sparkConfInfo}
import bzn.dw.util.SparkUtil
import bzn.job.common.MysqlUntil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/12/11
* @Describe:
*/ object DwUnEmpTAccountsIntermediateDetailBuckupTest extends SparkUtil with  MysqlUntil {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val sqlContext = sparkConf._3
    val hiveContext = sparkConf._4
    val res = readMysqlTable(sqlContext, "t_accounts_un_employer", "mysql.username", "mysql.password", "mysql.driver", "mysql.url")
    hiveContext.sql("TRUNCATE TABLE dwdb.dw_t_accounts_un_employer_detail_backup")
    res.repartition(1).write.mode(SaveMode.Append).saveAsTable("dwdb.dw_t_accounts_un_employer_detail_backup")
    sc.stop()
  }
}

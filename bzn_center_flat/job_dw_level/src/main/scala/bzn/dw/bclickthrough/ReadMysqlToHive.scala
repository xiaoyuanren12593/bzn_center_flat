package bzn.dw.bclickthrough

import bzn.dw.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/10/31
* @Describe:
*/ object ReadMysqlToHive  extends SparkUtil with Until with MysqlUntil{

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hqlContext = sparkConf._4
    val sqlContext = sparkConf._3
    val res = readMysqlTable(sqlContext,"t_accounts_employer","mysql.username","mysql.password","mysql.driver","mysql.url")
    res.write.mode(SaveMode.Append).saveAsTable("dwdb.dw_t_accounts_employer_detail")

  }

}

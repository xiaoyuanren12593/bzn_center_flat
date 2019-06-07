package bzn.ods.code

import bzn.job.common.Until
import bzn.ods.policy.OdsPolicyDetailTest.sparkConfInfo
import bzn.ods.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/6/5
  * Time:9:22
  * describe: this is new class
  **/
object OdsProductPlanCodeTest extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    sc.stop()
  }


}

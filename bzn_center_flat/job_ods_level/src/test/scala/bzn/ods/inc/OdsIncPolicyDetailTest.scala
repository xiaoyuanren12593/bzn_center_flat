package bzn.ods.inc

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/7/23
  * Time:14:32
  * describe: 增量的保单数据
  **/
object OdsIncPolicyDetailTest extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    sc.stop()
  }

  /**
    * 得到增量的数据并存储前一天的数据
    * @param sqlContext
    */
  def getOdsIncPolicyDetail(sqlContext:HiveContext) ={
    /**
      *
      */
  }
}

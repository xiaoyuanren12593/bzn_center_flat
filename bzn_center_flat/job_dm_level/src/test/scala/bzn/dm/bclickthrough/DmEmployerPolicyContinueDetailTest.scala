package bzn.dm.bclickthrough

import bzn.dm.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/9/18
  * Time:19:07
  * describe: b点通 雇主业务续投 续保，提升业绩和人员延续
  **/
object DmEmployerPolicyContinueDetailTest extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    //    res.write.mode(SaveMode.Overwrite).saveAsTable("dwdb.dw_policy_premium_detail")
    sc.stop()
  }


}

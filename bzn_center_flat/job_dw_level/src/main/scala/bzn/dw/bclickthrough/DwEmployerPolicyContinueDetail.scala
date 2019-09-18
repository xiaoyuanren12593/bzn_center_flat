package bzn.dw.bclickthrough
import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/9/18
  * Time:19:07
  * describe: b点通 雇主业务续投 续保，提升业绩和人员延续
  **/
object DwEmployerPolicyContinueDetail extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    //    res.write.mode(SaveMode.Overwrite).saveAsTable("dwdb.dw_policy_premium_detail")
    sc.stop()
  }

  /**
    * 续投保保单统计
    * @param sqlContext //上下文
    */
  def continueProposalDetail(sqlContext:HiveContext) = {

  }

  /**
    * 续保保单统计
    * @param sqlContext //上下文
    */
  def continuePolicyDetail(sqlContext:HiveContext) = {

  }

  /**
    * 读取公共信息
    * @param sqlContext
    */
  def publicInfo(sqlContext:HiveContext) = {
    /**
      * 渠道表
      */

    /**
      * 销售
      */
  }
}

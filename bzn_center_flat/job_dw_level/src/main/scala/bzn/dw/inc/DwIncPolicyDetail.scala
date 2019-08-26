package bzn.dw.inc

import bzn.dw.util.SparkUtil
import bzn.job.common.{HbaseUtil, Until}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/7/23
  * Time:14:32
  * describe: 增量的保单数据
  **/
object DwIncPolicyDetail extends SparkUtil with Until with HbaseUtil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    getDwIncPolicyDetail(hiveContext)
    sc.stop()
  }


  /**
    * 得到增量的数据并存储前一天的数据
    * @param sqlContext  上下文
    */
  def getDwIncPolicyDetail(sqlContext:HiveContext): Unit = {
    /**
      * 读取前一天数据
      */
    val dwPolicyDetailYesterday= sqlContext.sql("select policy_id_inc,update_time_inc from dwdb.dw_policy_detail_yesterday")

    /**
      * 读取今天保单数据
      */
    val odsPolicyDetail = sqlContext.sql("select * from odsdb.ods_policy_detail")

    /**
      * 获取新增数据
      */
    odsPolicyDetail.join(dwPolicyDetailYesterday,odsPolicyDetail("policy_id")===dwPolicyDetailYesterday("policy_id_inc"),"leftouter")
      .where("policy_id_inc is null or update_time_inc <> policy_update_time")
      .registerTempTable("inc_table")

    val incDataRes =
      sqlContext.sql("select *,case when update_time_inc <> policy_update_time and policy_id_inc = policy_id then 1 else 0 end as inc_type from inc_table")
        .drop("update_time_inc")
        .drop("policy_id_inc")

    sqlContext.sql("truncate table dwdb.dw_policy_detail_inc")
    incDataRes.write.mode(SaveMode.Append).saveAsTable("dwdb.dw_policy_detail_inc")

    sqlContext.sql("truncate table dwdb.dw_policy_detail_yesterday")
    odsPolicyDetail.selectExpr("id","policy_id as policy_id_inc","policy_update_time as update_time_inc","dw_create_time")
      .write.mode(SaveMode.Append).saveAsTable("dwdb.dw_policy_detail_yesterday")

  }
}

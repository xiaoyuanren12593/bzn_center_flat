package bzn.ods.policy

import bzn.job.common.{MysqlUntil, Until}
import bzn.ods.policy.OdsPreserveDetailStreaming.clean
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/10/23
  * Time:10:43
  * describe: 当天保单的明细表
  **/
object OdsPolicyDetailStreamingTest extends SparkUtil with Until with MysqlUntil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = getOneAndTwoSystemData(hiveContext)
//    hiveContext.sql("truncate table odsdb.ods_policy_streaming_detail")
//    res.repartition(1).write.mode(SaveMode.Append).saveAsTable("odsdb.ods_policy_streaming_detail")
    sc.stop()
  }

  def getOneAndTwoSystemData(sqlContext:HiveContext) = {

    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("clean", (str: String) => clean(str))
    val user  = "mysql.username.106"
    val pass  = "mysql.password.106"
    val driver  = "mysql.driver"
    val url  = "mysql.url.106"

    /**
      * 2.0 业管保单表
      */
    val tablebTpProposalStreamingbBznbusi = "t_proposal_streaming_bznbusi"
    val tpProposalStreamingbBznbusi = readMysqlTable(sqlContext: SQLContext, tablebTpProposalStreamingbBznbusi: String,user:String,pass:String,driver:String,url:String)
      .where("business_type = 2")
      .selectExpr(
        "holder_name",
        "insurance_policy_no as policy_code",
        "sell_channel_code as channel_id",
        "sell_channel_name as channel_name",
        "status",
        "payment_status",//支付状态
        "ledger_status",//实收状态
        "case when first_insure_master_num is null then 0 else first_insure_master_num end as insured_count",
        "product_code",
        "create_time","update_time"
      )

   val res = tpProposalStreamingbBznbusi
     .selectExpr(
       "getUUID() as id",
       "clean(holder_name) as holder_name",
       "clean(policy_code) as policy_code",
       "clean(channel_id) as channel_id",
       "clean(channel_name) as channel_name",
       "status",
       "payment_status",//支付状态
       "ledger_status",//实收状态
       "insured_count",
       "clean(product_code) as product_code",
       "create_time",
       "update_time"
     )
    res.printSchema()
    res
  }
}

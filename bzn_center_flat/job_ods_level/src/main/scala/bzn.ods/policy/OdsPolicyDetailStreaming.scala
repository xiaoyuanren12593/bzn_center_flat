package bzn.ods.policy

import bzn.job.common.{MysqlUntil, Until}
import bzn.ods.policy.OdsPreserveDetailStreaming.clean
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/10/23
  * Time:10:43
  * describe: 当天保单的明细表
  **/
object OdsPolicyDetailStreaming extends SparkUtil with Until with MysqlUntil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = getOneAndTwoSystemData(hiveContext)
    hiveContext.sql("truncate table odsdb.ods_policy_streaming_detail")
    res.repartition(1).write.mode(SaveMode.Append).saveAsTable("odsdb.ods_policy_streaming_detail")
    sc.stop()
  }

  def getOneAndTwoSystemData(sqlContext:HiveContext): DataFrame = {

    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("clean", (str: String) => clean(str))

    val user  = "mysql.username.106"
    val pass  = "mysql.password.106"
    val driver  = "mysql.driver"
    val url  = "mysql.url.106"

    /**
      * 1.0保单表
      */
    val tableOdrPolicyStreamingBznprd = "odr_policy_streaming_bznprd"
    val odrPolicyStreamingBznprd = readMysqlTable(sqlContext: SQLContext, tableOdrPolicyStreamingBznprd: String,user:String,pass:String,driver:String,url:String)
      .selectExpr("id","order_id","policy_code","channel_id","channel_name","people_num","insure_code","create_time","update_time")

    /**
      * 1.0投保人表
      */
    val tableOdrPolicyHolderStreamingBznprd = "odr_policy_holder_streaming_bznprd"
    val odrPolicyHolderStreamingBznprd = readMysqlTable(sqlContext: SQLContext, tableOdrPolicyHolderStreamingBznprd: String,user:String,pass:String,driver:String,url:String)
      .selectExpr("name","policy_id")

    /**
      * 1.0订单表
      */
    val tableOdrOrderInfoStreamingBznprd = "odr_order_info_streaming_bznprd"
    val odrOrderInfoStreamingBznprd = readMysqlTable(sqlContext: SQLContext, tableOdrOrderInfoStreamingBznprd: String,user:String,pass:String,driver:String,url:String)
      .selectExpr("id","status")

    /**
      * 保单与投保人数据关联
      */
    val odrPolicyStreamingBznprdHolder = odrPolicyStreamingBznprd.join(odrPolicyHolderStreamingBznprd,odrPolicyStreamingBznprd("id")===odrPolicyHolderStreamingBznprd("policy_id"),"leftouter")
      .selectExpr("name","order_id","policy_code","channel_id","channel_name","people_num","insure_code","create_time","update_time")

    /**
      * 上述结果与订单数据关联
      */
    val odrPolicyStreamingBznprdHolderOrder = odrPolicyStreamingBznprdHolder.join(odrOrderInfoStreamingBznprd,odrPolicyStreamingBznprdHolder("order_id")===odrOrderInfoStreamingBznprd("id"),"leftouter")
      .where("length(name) > 0")
      .selectExpr("name as holder_name","policy_code","channel_id","channel_name","status","people_num as insured_count","insure_code as product_code","create_time","update_time")

    /**
      * 2.0 业管保单表
      */
    val tablebTpProposalStreamingbBznbusi = "t_proposal_streaming_bznbusi"
    val tpProposalStreamingbBznbusi = readMysqlTable(sqlContext: SQLContext, tablebTpProposalStreamingbBznbusi: String,user:String,pass:String,driver:String,url:String)
      .where("business_type = 2")
      .selectExpr(
        "holder_name",
        "insurance_proposal_no as policy_code",
        "sell_channel_code as channel_id",
        "sell_channel_name as channel_name",
        "status",
        "case when first_insure_master_num is null then 0 else first_insure_master_num end as insured_count",
        "product_code",
        "create_time","update_time"
      )

    /**
      * 1.0保单和批单进行合并
      */
    val res = odrPolicyStreamingBznprdHolderOrder.unionAll(tpProposalStreamingbBznbusi)
      .selectExpr(
        "getUUID() as id",
        "clean(holder_name) as holder_name",
        "clean(policy_code) as policy_code",
        "clean(channel_id) as channel_id",
        "clean(channel_name) as channel_name",
        "status",
        "insured_count",
        "clean(product_code) as product_code",
        "create_time",
        "update_time"
      )
    res
  }
}

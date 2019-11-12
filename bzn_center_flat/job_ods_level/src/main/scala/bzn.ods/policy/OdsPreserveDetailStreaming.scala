package bzn.ods.policy

import bzn.job.common.{MysqlUntil, Until}
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
object OdsPreserveDetailStreaming extends SparkUtil with Until with MysqlUntil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = getPreserveBusiness(hiveContext)
    hiveContext.sql("truncate table odsdb.ods_preserve_streaming_detail")
    res.repartition(1).write.mode(SaveMode.Append).saveAsTable("odsdb.ods_preserve_streaming_detail")
    sc.stop()
  }

  def getPreserveBusiness(sqlContext:HiveContext): DataFrame = {
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))

    val user  = "mysql.username.106"
    val pass  = "mysql.password.106"
    val driver  = "mysql.driver"
    val url  = "mysql.url.106"

    /**
      * 1.0业管批单表
      */
    val tablePlcPolicyPreserveStreamingBznprd = "plc_policy_preserve_streaming_bznprd"
    val plcPolicyPreserveStreamingBznprd = readMysqlTable(sqlContext: SQLContext, tablePlcPolicyPreserveStreamingBznprd: String,user:String,pass:String,driver:String,url:String)
      .selectExpr("id","policy_id as policy_id_master","policy_code","status","add_person_count","del_person_count","create_time","update_time")

    /**
      * 1.0投保人表
      */
    val tableOdrPolicyHolderStreamingBznprd = "odr_policy_holder_streaming_bznprd"
    val odrPolicyHolderStreamingBznprd = readMysqlTable(sqlContext: SQLContext, tableOdrPolicyHolderStreamingBznprd: String,user:String,pass:String,driver:String,url:String)
      .selectExpr("name","policy_id")

    val onePreserveRes = plcPolicyPreserveStreamingBznprd.join(odrPolicyHolderStreamingBznprd,plcPolicyPreserveStreamingBznprd("policy_id_master")===odrPolicyHolderStreamingBznprd("policy_id"),"leftouter")
      .where("length(name) > 0")
      .selectExpr(
        "id",
        "name as holder_name","policy_code","status","create_time","update_time",
        "case when add_person_count is null then 0 else add_person_count end as add_person_count",
        "case when del_person_count is null then 0 else del_person_count end as del_person_count","create_time","update_time")
      .selectExpr("id","holder_name","policy_code","status","'' as channel_id","'' as channel_name","(add_person_count-del_person_count) as insured_count","create_time","update_time")

    /**
      * 2.0业管批单表
      */
    val tableBPolicyPreservationStreamingBznbusi = "b_policy_preservation_streaming_bznbusi"
    val bPolicyPreservationStreamingBznbusi = readMysqlTable(sqlContext: SQLContext, tableBPolicyPreservationStreamingBznbusi: String,user:String,pass:String,driver:String,url:String)
      .where("business_type = 2")
      .selectExpr("id","holder_name","insurance_policy_no as policy_code","status","sell_channel_code as channel_id","sell_channel_name as channel_name",
        "case when inc_revise_sum is null then 0 else inc_revise_sum end as inc_revise_sum",
        "case when dec_revise_sum is null then 0 else dec_revise_sum end as dec_revise_sum","create_time","update_time"
      ).selectExpr(
      "cast(id as string) as id",
      "holder_name",
      "policy_code",
      "status",
      "channel_id",
      "channel_name",
      "(inc_revise_sum - dec_revise_sum) as insured_count",
      "create_time",
      "update_time"
    )

    val res = bPolicyPreservationStreamingBznbusi.unionAll(onePreserveRes)
      .selectExpr(
        "getUUID() as id",
        "id as preserve_id",
        "clean(holder_name) as holder_name",
        "clean(policy_code) as policy_code",
        "status",
        "clean(channel_id) as channel_id",
        "clean(channel_name) as channel_name",
        "insured_count",
        "create_time",
        "update_time"
      )

    res
  }
}

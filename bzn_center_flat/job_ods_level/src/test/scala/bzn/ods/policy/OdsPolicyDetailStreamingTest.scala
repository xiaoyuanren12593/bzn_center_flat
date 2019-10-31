package bzn.ods.policy

import bzn.job.common.{MysqlUntil, Until}
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

    val user  = "mysql.username.106"
    val pass  = "mysql.password.106"
    val driver  = "mysql.driver"
    val url  = "mysql.url.106"

    /**
      * 1.0保单表
      */
    val tableOdrPolicyStreamingBznprd = "odr_policy_streaming_bznprd"
    val odrPolicyStreamingBznprd = readMysqlTable(sqlContext: SQLContext, tableOdrPolicyStreamingBznprd: String,user:String,pass:String,driver:String,url:String)
      .selectExpr("id","order_id","people_num","insure_code","create_time","update_time")

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
      .selectExpr("name","order_id","insure_code","people_num")

    /**
      * 上述结果与订单数据关联
      */
    val odrPolicyStreamingBznprdHolderOrder = odrPolicyStreamingBznprdHolder.join(odrOrderInfoStreamingBznprd,odrPolicyStreamingBznprdHolder("order_id")===odrOrderInfoStreamingBznprd("id"),"leftouter")
      .where("status not  in (5,6,7) and length(name) > 0")
      .selectExpr("name as holder_name","insure_code","people_num as insured_count")

    /**
      * 读取产品表
      */
    val odsProductDetail = sqlContext.sql("select product_code,one_level_pdt_cate from odsdb.ods_product_detail")
      .where("one_level_pdt_cate = '蓝领外包' and product_code not in ('LGB000001','17000001')")

    val onePolicyRes = odrPolicyStreamingBznprdHolderOrder.join(odsProductDetail,odrPolicyStreamingBznprdHolderOrder("insure_code")===odsProductDetail("product_code"))
        .selectExpr("holder_name","insured_count")

    /**
      * 1.0业管批单表
      */
    val tablePlcPolicyPreserveStreamingBznprd = "plc_policy_preserve_streaming_bznprd"
    val plcPolicyPreserveStreamingBznprd = readMysqlTable(sqlContext: SQLContext, tablePlcPolicyPreserveStreamingBznprd: String,user:String,pass:String,driver:String,url:String)
      .selectExpr("policy_id as policy_id_master","status","add_person_count","del_person_count")

    val onePreserveRes = plcPolicyPreserveStreamingBznprd.join(odrPolicyHolderStreamingBznprd,plcPolicyPreserveStreamingBznprd("policy_id_master")===odrPolicyHolderStreamingBznprd("policy_id"),"leftouter")
      .where("status not in (3,6) and length(name) > 0")
      .selectExpr(
        "name as holder_name",
        "case when add_person_count is null then 0 else add_person_count end as add_person_count",
        "case when del_person_count is null then 0 else del_person_count end as del_person_count")
      .selectExpr("holder_name","(add_person_count-del_person_count) as insured_count")

    /**
      * 2.0 业管保单表
      */
    val tablebTpProposalStreamingbBznbusi = "t_proposal_streaming_bznbusi"
    val tpProposalStreamingbBznbusi = readMysqlTable(sqlContext: SQLContext, tablebTpProposalStreamingbBznbusi: String,user:String,pass:String,driver:String,url:String)
      .where("status not in(-1,4,9,10) and business_type = 2")
      .selectExpr("holder_name","case when first_insure_master_num is null then 0 else first_insure_master_num end as insured_count")

    /**
      * 2.0业管批单表
      */
    val tableBPolicyPreservationStreamingBznbusi = "b_policy_preservation_streaming_bznbusi"
    val bPolicyPreservationStreamingBznbusi = readMysqlTable(sqlContext: SQLContext, tableBPolicyPreservationStreamingBznbusi: String,user:String,pass:String,driver:String,url:String)
      .where("status not in (4,8,9)and business_type = 2")
      .selectExpr("holder_name","business_type",
      "case when inc_revise_sum is null then 0 else inc_revise_sum end as inc_revise_sum",
      "case when dec_revise_sum is null then 0 else dec_revise_sum end as dec_revise_sum"
    ).selectExpr("holder_name","(inc_revise_sum-dec_revise_sum) as insured_count")

   val res = onePolicyRes.unionAll(onePreserveRes).unionAll(tpProposalStreamingbBznbusi).unionAll(bPolicyPreservationStreamingBznbusi)
      .selectExpr("getUUID() as id","holder_name","insured_count","date_format(now(), 'yyyy-MM-dd HH:mm:ss') as create_time",
        "date_format(now(), 'yyyy-MM-dd HH:mm:ss') as update_time")
    res.printSchema()
    res
  }
}

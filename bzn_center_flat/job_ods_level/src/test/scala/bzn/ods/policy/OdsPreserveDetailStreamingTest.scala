package bzn.ods.policy

import bzn.job.common.{MysqlUntil, Until}
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/10/23
  * Time:10:43
  * describe: 当天保单的明细表
  **/
object OdsPreserveDetailStreamingTest extends SparkUtil with Until with MysqlUntil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = getPreserveBusiness(hiveContext)
//    hiveContext.sql("truncate table odsdb.ods_policy_streaming_detail")
//    res.repartition(1).write.mode(SaveMode.Append).saveAsTable("odsdb.ods_policy_streaming_detail")
    sc.stop()
  }

  def getPreserveBusiness(sqlContext:HiveContext): DataFrame = {

    import sqlContext.implicits._
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))

    val user  = "mysql.username.106"
    val pass  = "mysql.password.106"
    val driver  = "mysql.driver"
    val url  = "mysql.url.106"

    /**
      * 2.0 业管保单表
      */
    val tablebTpProposalStreamingbBznbusi = "t_proposal_bznbusi"
    val tpProposalStreamingbBznbusi = readMysqlTable(sqlContext: SQLContext, tablebTpProposalStreamingbBznbusi: String,user:String,pass:String,driver:String,url:String)
      .where("business_type = 2 and product_code not in ('P00001597','P00001638')")
      .selectExpr(
        "proposal_no",
        "insurance_policy_no as policy_code",
        "policy_no",
        "start_date",//保单起期
        "end_date",//投保止期
        "proposal_time",//申请时间
        "business_belong_user_name",
        "operation_user_name"
      )

    /**
      * 读取被保人企业表
      */
    val tableTpProposalSubjectCompanyBznbusi = "t_proposal_subject_company_bznbusi"
    val tpProposalSubjectCompanyBznbusi = readMysqlTable(sqlContext: SQLContext, tableTpProposalSubjectCompanyBznbusi: String,user:String,pass:String,driver:String,url:String)
      .selectExpr(
        "proposal_no as proposal_no_subject",
        "name"
      )

    /**
      * 上述结果和被保人企业数据进行关联
      */
    val proposalRes = tpProposalStreamingbBznbusi.join(tpProposalSubjectCompanyBznbusi,'proposal_no==='proposal_no_subject,"leftouter")
      .selectExpr(
        "proposal_no",
        "policy_code",
        "policy_no",
        "start_date as policy_start_date",//保单起期
        "end_date as policy_end_date",//投保止期
        "proposal_time as proposal_time_policy",//投保止期
        "trim(name) as insured_company",//被保人企业
        "case when business_belong_user_name = '' then null " +
          "when business_belong_user_name = '客户运营负责人' then null " +
          "when business_belong_user_name ='销售默认' then null " +
          "when business_belong_user_name = '运营默认' then null " +
          "else business_belong_user_name end as sales_name",
        "operation_user_name as biz_operator"
      )

    /**
      * 2.0业管批单表
      */
    val tableBPolicyPreservationStreamingBznbusi = "b_policy_preservation_streaming_bznbusi"
    val bPolicyPreservationStreamingBznbusi = readMysqlTable(sqlContext: SQLContext, tableBPolicyPreservationStreamingBznbusi: String,user:String,pass:String,driver:String,url:String)
      .where("business_type = 2")
      .selectExpr(
        "id",
        "inc_dec_order_no",
        "policy_no as policy_no_preserve",
        "holder_name",
        "insurance_policy_no as policy_code_preserve",
        "status",
        "sell_channel_code as channel_id",
        "sell_channel_name as channel_name",
        "apply_time as proposal_time_preserve",
        "start_date",
        "end_date",
        "insurance_name",
        "big_policy",
        "payment_type as sku_charge_type",
        "business_belong_user_name",
        "case when inc_revise_sum is null then 0 else inc_revise_sum end as inc_revise_sum",
        "case when dec_revise_sum is null then 0 else dec_revise_sum end as dec_revise_sum","create_time","update_time"
      ).selectExpr(
      "id",
      "inc_dec_order_no",
      "policy_no_preserve",
      "holder_name",
      "policy_code_preserve",
      "status",
      "channel_id",
      "channel_name",
      "proposal_time_preserve",
      "start_date as preserve_start_date",
      "end_date as preserve_end_date",
      "insurance_name",
      "big_policy",
      "sku_charge_type",
      "business_belong_user_name",
      "(inc_revise_sum - dec_revise_sum) as insured_count",
      "create_time",
      "update_time"
    )

    val res = bPolicyPreservationStreamingBznbusi.join(proposalRes,'policy_no_preserve==='policy_no,"leftouter")
      .selectExpr(
        "getUUID() as id",
        "proposal_no",
        "policy_code_preserve as policy_code",
        "policy_no_preserve as policy_no",
        "holder_name",
        "channel_id",
        "channel_name",
        "status",
        "big_policy",
        "proposal_time_policy",//投保止期
        "proposal_time_preserve",//批单投保时间
        "policy_start_date",//保单起期
        "policy_end_date",//投保止期
        "preserve_start_date",
        "preserve_end_date",
        "insured_count",
        "insured_company",//被保人企业
        "insurance_name",
        "sku_charge_type",
        "inc_dec_order_no",
        "case when business_belong_user_name is null or business_belong_user_name = '' then sales_name else business_belong_user_name end as sales_name",
        "biz_operator",
        "create_time",
        "update_time"
      )

    res.printSchema()
    res.show()
    res
  }
}

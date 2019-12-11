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
    import sqlContext.implicits._

    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("clean", (str: String) => clean(str))
    val user  = "mysql.username.106"
    val pass  = "mysql.password.106"
    val driver  = "mysql.driver"
    val url  = "mysql.url.106"

    /**
      * 2.0 业管保单表
      */
    val tablebTpProposalStreamingbBznbusi = "t_proposal_bznbusi"
    val tpProposalStreamingbBznbusi = readMysqlTable(sqlContext: SQLContext, tablebTpProposalStreamingbBznbusi: String,user:String,pass:String,driver:String,url:String)
      .where("business_type = 2 and create_time >= DATE_ADD(date_format(cast(now() as string),'yyyy-MM-dd 00:00:00'),-5) and product_code not in ('P00001597','P00001638')")
      .selectExpr(
        "proposal_no",
        "policy_category",//出单类型：年月单
        "insurance_policy_no as policy_code",
        "policy_no",
        "holder_name",
        "sell_channel_code as channel_id",
        "sell_channel_name as channel_name",
        "status",
        "payment_status",//支付状态
        "ledger_status",//实收状态
        "big_policy",//是否是大保单
        "proposal_time",//投保时间
        "start_date",//保单起期
        "end_date",//投保止期
        "case when first_insure_master_num is null then 0 else first_insure_master_num end as insured_count",
        "now() as update_data_time",
        "insurance_name",
        "product_code",
        "business_belong_user_name",
        "operation_user_name",
        "create_time",
        "update_time"
      )

//    /**
//      * 读取投保方案信息表
//      */
//    val tableTpProposalProductPlanBznbusi = "t_proposal_product_plan_bznbusi"
//    val tpProposalProductPlanBznbusi = readMysqlTable(sqlContext: SQLContext, tableTpProposalProductPlanBznbusi: String,user:String,pass:String,driver:String,url:String)
//      .selectExpr(
//        "proposal_no as proposal_no_plan",
//        "payment_type"
//      )

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
    val res = tpProposalStreamingbBznbusi.join(tpProposalSubjectCompanyBznbusi,'proposal_no==='proposal_no_subject,"leftouter")
      .selectExpr(
        "getUUID() as id",
        "proposal_no",
        "policy_code",
        "policy_no",
        "holder_name",
        "channel_id",
        "channel_name",
        "status",
        "payment_status",//支付状态
        "ledger_status",//实收状态
        "big_policy",//是否是大保单
        "proposal_time",//投保时间
        "start_date as policy_start_date",//保单起期
        "end_date as policy_end_date",//投保止期
        "insured_count",
        "trim(name) as insured_company",//被保人企业
        "policy_category as sku_charge_type",//方案类别
        "update_data_time",
        "trim(insurance_name) as insurance_name",
        "trim(product_code) as product_code",
        "clean(case when business_belong_user_name = '' then null when business_belong_user_name = '客户运营负责人' then null " + "when business_belong_user_name ='销售默认' then null when business_belong_user_name = '运营默认' then null else business_belong_user_name end) as sales_name",
        "clean(operation_user_name) as biz_operator",
        "create_time",
        "update_time"
      )
    res.show()
    res.printSchema()
    res
  }
}

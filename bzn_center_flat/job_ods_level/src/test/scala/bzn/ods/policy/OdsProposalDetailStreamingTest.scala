package bzn.ods.policy

import bzn.job.common.{MysqlUntil, Until}
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/10/23
  * Time:10:43
  * describe: 每天增量的投保单明细数据，与每天增量的批单明细数据
  **/
object OdsProposalDetailStreamingTest extends SparkUtil with Until with MysqlUntil{
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
    val tablebTpProposalStreamingbBznbusi = "t_proposal_streaming_bznbusi"
    val tpProposalStreamingbBznbusi = readMysqlTable(sqlContext: SQLContext, tablebTpProposalStreamingbBznbusi: String,user:String,pass:String,driver:String,url:String)
      .where("business_type = 2 and insurance_insure_time <= cast(date_format(DATE_ADD(now(),0),'yyyy-MM-dd 18:00:00') as timestamp) and " +
        "insurance_insure_time >= cast(date_format(DATE_ADD(now(),0),'yyyy-MM-dd 00:00:00') as timestamp)")
      .selectExpr(
        "proposal_no",
        "insurance_policy_no as policy_code",
        "status",
        "profession_code",//中华
        "profession_type",//国寿（JOB_CD_0009）， 泰康（1-3类）
        "cast(date_format(DATE_ADD(now(),1),'yyyy-MM-dd 18:00:00') as timestamp) as da",
        "cast(date_format(DATE_ADD(now(),1),'yyyy-MM-dd 00:00:00') as timestamp) as da1",
        "premium_price",//保费单价
        "first_insure_premium",//初投保费
        "case when first_insure_master_num is null then 0 else first_insure_master_num end as insured_count",
        "insurance_name"
      )

    /**
      * 读取投保方案信息表
      */
    val tableTpProposalProductPlanStreamingBznbusi = "t_proposal_product_plan_streaming_bznbusi"
    val tpProposalProductPlanStreamingBznbusi = readMysqlTable(sqlContext: SQLContext, tableTpProposalProductPlanStreamingBznbusi: String,user:String,pass:String,driver:String,url:String)
      .selectExpr(
        "proposal_no as proposal_no_plan",
        "plan_amount as sku_coverage",
        "case when injure_percent = 0.05 then 1 when injure_percent = 0.10 then 2 else null end as sku_ratio",
        "payment_type as sku_charge_type"
      )

    /**
      * 上述结果和被保人方案表进行关联
      */
    val resProposal = tpProposalStreamingbBznbusi.join(tpProposalProductPlanStreamingBznbusi,'proposal_no==='proposal_no_plan,"leftouter")
      .selectExpr(
        "getUUID() as id",
        "proposal_no",
        "policy_code",
        "status",
        "premium_price as sku_price",//保费单价
        "first_insure_premium as first_premium",//初投
        "insured_count",
        "insurance_name",
        "sku_coverage",
        "sku_ratio",
        "sku_charge_type"
      )

    resProposal.printSchema()

    /**
      * 读取保全表
      */
    val tableBPolicyPreservationStreamingOperatorBznbusi = "b_policy_preservation_streaming_operator_bznbusi"
    val bPolicyPreservationStreamingOperatorBznbusi = readMysqlTable(sqlContext: SQLContext, tableBPolicyPreservationStreamingOperatorBznbusi: String,user:String,pass:String,driver:String,url:String)
//      .where("")
      .selectExpr(
        "insurance_policy_no",
        "inc_revise_sum as add_person_count",
        "dec_revise_sum as del_person_count",
        "insurance_name",
        "preservation_type as preserve_type"
      )
    bPolicyPreservationStreamingOperatorBznbusi.show()
    resProposal
  }
}

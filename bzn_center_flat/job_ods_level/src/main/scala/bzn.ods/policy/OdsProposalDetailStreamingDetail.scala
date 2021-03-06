package bzn.ods.policy

import java.text.SimpleDateFormat
import java.util.Date

import bzn.job.common.{MysqlUntil, Until}
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/10/23
  * Time:10:43
  * describe: 每天增量的投保单明细数据，与每天增量的批单明细数据
  **/
object OdsProposalDetailStreamingDetail extends SparkUtil with Until with MysqlUntil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = getOneAndTwoSystemData(hiveContext)
    hiveContext.sql("truncate table odsdb.ods_proposal_operator_daily_detail")
    res.repartition(10).write.mode(SaveMode.Append).saveAsTable("odsdb.ods_proposal_operator_daily_detail")

    sc.stop()
  }

  def getOneAndTwoSystemData(sqlContext:HiveContext): DataFrame = {
    import sqlContext.implicits._

    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      date + ""
    })
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
        "proposal_no",
        "insurance_insure_time",
        "insurance_policy_no as policy_code",
        "status",
        "profession_code",//中华
        "profession_type",//国寿（JOB_CD_0009）， 泰康（1-3类）
        "cast(date_format(DATE_ADD(now(),1),'yyyy-MM-dd 18:00:00') as timestamp) as da",
        "cast(date_format(DATE_ADD(now(),1),'yyyy-MM-dd 00:00:00') as timestamp) as da1",
        "premium_price",//保费单价
        "first_insure_premium",//初投保费
        "4 as preserve_type",//业务类型
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
    val resProposal = tpProposalStreamingbBznbusi
      .join(tpProposalProductPlanStreamingBznbusi,'proposal_no==='proposal_no_plan,"leftouter")
      .selectExpr(
        "getUUID() as id",
        "policy_code",
        "insurance_insure_time",
        "insured_count as add_person_count",
        "cast('' as int) as del_person_count",
        "insurance_name",
        "cast('' as timestamp) as effective_date",
        "status",
        "4 as preserve_type",
        "profession_code",//中华
        "profession_type",//国寿（JOB_CD_0009）， 泰康（1-3类）
        "cast(premium_price as decimal(14,4)) as sku_price",//保费单价
        "cast(first_insure_premium as decimal(14,4)) as premium",//初投
        "cast(first_insure_premium as decimal(14,4)) as add_premium",
        "cast('' as decimal(14,4)) as del_premium",
        "cast(sku_coverage as decimal(14,0)) as sku_coverage",
        "sku_ratio",
        "sku_charge_type",
        "getNow() as dw_create_time"
      )

    val resProposalTAdd1 = resProposal.where("insurance_insure_time <= cast(date_format(DATE_ADD(now(),0),'yyyy-MM-dd 18:00:00') as timestamp) and " +
      "insurance_insure_time >= cast(date_format(DATE_ADD(now(),0),'yyyy-MM-dd 00:00:00') as timestamp)")
      .drop("insurance_insure_time")

    /**
      * 读取保全表
      */
    val tableBPolicyPreservationStreamingOperatorBznbusi = "b_policy_preservation_streaming_operator_bznbusi"
    val bPolicyPreservationStreamingOperatorBznbusi = readMysqlTable(sqlContext: SQLContext, tableBPolicyPreservationStreamingOperatorBznbusi: String,user:String,pass:String,driver:String,url:String)
      .selectExpr(
        "insurance_policy_no",
        "inc_revise_sum as add_person_count",
        "dec_revise_sum as del_person_count",
        "case when inc_revise_premium is null then 0 else inc_revise_premium end as inc_revise_premium",
        "case when dec_revise_premium is null then 0 else 0-dec_revise_premium end as dec_revise_premium",
        "insurance_name",
        "effective_date",
        "create_time",
        "business_type",
        "status",
        "preservation_type as preserve_type"
      )

    /**
      * 众安的数据
      */
    val zaInsureData = bPolicyPreservationStreamingOperatorBznbusi
      .where("effective_date >= cast(date_format(DATE_ADD(now(),0),'yyyy-MM-dd 00:00:00') as timestamp) " +
        "and effective_date <= cast(date_format(DATE_ADD(now(),0),'yyyy-MM-dd 18:00:00') as timestamp) and insurance_name like '%众安%' and " +
        "create_time <= cast(date_format(DATE_ADD(now(),0),'yyyy-MM-dd 18:00:00') as timestamp) and business_type = 2 and preserve_type = 1 and `status` = 7")

    /**
      * 非众安的数据 为了数据暂时先拿当天的数据  正常是 +1天
      */
    val notZaInsureData = bPolicyPreservationStreamingOperatorBznbusi
      .where("effective_date >= cast(date_format(DATE_ADD(now(),1),'yyyy-MM-dd 00:00:00') as timestamp) " +
        "and effective_date <= cast(date_format(DATE_ADD(now(),1),'yyyy-MM-dd 18:00:00') as timestamp) and insurance_name not like '%众安%' and " +
        "create_time <= cast(date_format(DATE_ADD(now(),0),'yyyy-MM-dd 18:00:00') as timestamp) and business_type = 2 and preserve_type = 1 and `status` = 7")

    val insureData = zaInsureData.unionAll(notZaInsureData)
      .selectExpr(
        "getUUID() as id",
        "insurance_policy_no as policy_code",
        "add_person_count",
        "del_person_count",
        "insurance_name",
        "effective_date",
        "status",
        "1 as preserve_type",
        "(inc_revise_premium+dec_revise_premium) as premium",//增减员
        "inc_revise_premium as add_premium",
        "dec_revise_premium as del_premium",
        "getNow() as dw_create_time"
      )

    /**
      * 为批单中的新增保单获取方案信息
      */
    val resProposalPreserve = resProposal.selectExpr(
      "policy_code as policy_code_slave",
      "profession_code",//中华
      "profession_type",//国寿（JOB_CD_0009）， 泰康（1-3类）
      "sku_price",//保费单价
      "sku_coverage",
      "sku_ratio",
      "sku_charge_type"
    )

    val insureDataRes = insureData.join(resProposalPreserve,insureData("policy_code")===resProposalPreserve("policy_code_slave"),"leftouter")
      .selectExpr(
        "id",
        "policy_code",
        "add_person_count",
        "del_person_count",
        "insurance_name",
        "effective_date",
        "status",
        "preserve_type",
        "profession_code",
        "profession_type",
        "sku_price",
        "premium",//增减员
        "add_premium",
        "del_premium",
        "sku_coverage",
        "sku_ratio",
        "sku_charge_type",
        "dw_create_time"
      )

    val res = insureDataRes.unionAll(resProposalTAdd1)
      .selectExpr(
        "id",
        "policy_code",
        "add_person_count",
        "del_person_count",
        "insurance_name",
        "effective_date",
        "status",
        "preserve_type",
        "clean(profession_code) as profession_code",
        "clean(profession_type) as profession_type",
        "sku_price",
        "premium",
        "add_premium",
        "del_premium",
        "sku_coverage",
        "sku_ratio",
        "sku_charge_type",
        "dw_create_time"
      )

    res
  }
}

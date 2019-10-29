package bzn.dw.bclickthrough

import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/10/29
  * Time:14:16
  * describe: 费雇主电子台账
  **/
object DwUnEmpTAccountSintermediateDetailTest extends SparkUtil with Until{
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = getAllBussnessPolicyDetail(hiveContext)
    //    res.write.mode(SaveMode.Overwrite).saveAsTable("dwdb.dw_policy_premium_detail")
    sc.stop()
  }

  /**
    * 获取所有业务条线的保单数据
    * @param sqlContext 上下文
    */
  def getAllBussnessPolicyDetail(sqlContext:HiveContext) = {

    /**
      * 读取保单明细表
      */
    val odsPolicyDetail= sqlContext.sql("select policy_code,case when source_system = '2.0' then '2' when source_system = '1.0' then '1' end as source_system,policy_status,policy_effect_date," +
      "policy_start_date,policy_end_date,insure_company_name,channel_id,channel_name,sales_name,first_premium,holder_name,invoice_type,product_code,policy_create_time,insured_subject from odsdb.ods_policy_detail")
      .where("policy_status in (0,1,-1) and policy_code is not null and policy_start_date>='2019-01-01 00:00:00'")

    /**
      * 读取产品表
      */
    val odsProductDetail = sqlContext.sql("select product_code as product_code_slave,product_name,product_desc,business_line from odsdb.ods_product_detail")

    /**
      * 读取方案表
      */
    val odsPolicyProductPlanDetail = sqlContext.sql("select policy_code as policy_code_slave,sku_coverage,sku_charge_type,sku_ratio,sku_price,sku_append,tech_service_rate,economic_rate,commission_discount_rate from odsdb.ods_policy_product_plan_detail")

    /**
      * 读取销售团队表
      */
    val odsEntSalesTeamDimension = sqlContext.sql("select sale_name,team_name from odsdb.ods_ent_sales_team_dimension")

    /**
      * 保单表和产品表进行关联
      */
    val policyAndPlanRes = odsPolicyDetail.join(odsPolicyProductPlanDetail,odsPolicyDetail("policy_code")===odsPolicyProductPlanDetail("policy_code_slave"),"leftouter")
      .selectExpr(
        "policy_code",
        "source_system",
        "policy_status",
        "policy_effect_date",
        "policy_start_date",
        "policy_end_date",
        "policy_create_time",
        "insure_company_name",
        "channel_id",
        "channel_name",
        "sales_name",
        "first_premium",
        "holder_name",
        "insured_subject",
        "invoice_type",
        "product_code",
        "sku_coverage",
        "sku_charge_type",
        "sku_ratio",
        "sku_price",
        "sku_append",
        "tech_service_rate",
        "economic_rate",
        "commission_discount_rate"
      )

    /**
      * 上结果与团队表进行关联
      */
    val policyAndPlanAndTeamRes = policyAndPlanRes.join(odsEntSalesTeamDimension,policyAndPlanRes("sales_name")===odsEntSalesTeamDimension("sale_name"),"leftouter")
      .selectExpr(
        "policy_code",
        "source_system",
        "policy_status",
        "policy_effect_date",
        "policy_start_date",
        "policy_end_date",
        "policy_create_time",
        "insure_company_name",
        "channel_id",
        "channel_name",
        "sales_name",
        "team_name",
        "first_premium",
        "holder_name",
        "insured_subject",
        "invoice_type",
        "product_code",
        "sku_coverage",
        "sku_charge_type",
        "sku_ratio",
        "sku_price",
        "sku_append",
        "tech_service_rate",
        "economic_rate",
        "commission_discount_rate"
      )

    getSportsPolicyDetail(sqlContext,policyAndPlanAndTeamRes,odsProductDetail)
  }

  /**
    * 获取体育保单数据信息
    * @param sqlContext
    */
  def getSportsPolicyDetail(sqlContext:HiveContext,policyAndPlanAndTeamRes:DataFrame,odsProductDetail:DataFrame) = {
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))

    val odsSportProductDetail = odsProductDetail.where("business_line in ('体育','场景','员福','健康')")

    val policyAndPlanAndTeamAndProductRes = policyAndPlanAndTeamRes.join(odsSportProductDetail,policyAndPlanAndTeamRes("product_code")===odsSportProductDetail("product_code_slave"))
      .selectExpr(
        "getUUID() as id",
        "case when 1 = 1 then null end as batch_no",
        "policy_code as policy_no",
        "case when 1 = 1 then null end as preserve_id",
        "case when 1 = 1 then null end as preserve_status",
        "case when 1 = 1 then null end as add_batch_code",
        "case when 1 = 1 then null end as del_batch_code",
        "source_system as data_source",
        "business_line as project_name",
        "product_code",
        "product_name",
        "product_desc as product_detail",
        "channel_name",
        "sales_name as business_owner",
        "team_name as business_region",
        "case when 1 = 1 then null end as business_source",
        "'1' as business_type",
        "if(policy_start_date >= policy_create_time,policy_start_date,policy_create_time) as performance_accounting_day",
        "holder_name",
        "insured_subject as insurer_name",
        "insure_company_name as underwriting_company",
        "policy_effect_date",
        "policy_start_date as policy_effective_time",
        "policy_end_date as policy_expire_time",
        "policy_status",
        "sku_coverage as plan_coverage",
        "first_premium as premium_total",
        "'1' as premium_pay_status",//保费实收状态
        "case when 1 = 1 then null end as behalf_number",
        "case when invoice_type is null then 0 else invoice_type end as premium_invoice_type",
        "'天津中策' as economy_company",
        "economic_rate as economy_rates",
        "cast((first_premium * economic_rate) as decimal(14,4)) as economy_fee",
        "tech_service_rate as technical_service_rates",
        "cast((first_premium * tech_service_rate) as decimal(14,4)) as technical_service_fee",
        "case when 1 = 1 then null end as consulting_service_rates",
        "case when 1 = 1 then null end as consulting_service_fee",
        "case when 1 = 1 then null end as service_fee_check_time",
        "case when 1 = 1 then null end as service_fee_check_status",
        "case when 1 = 1 then null end as has_brokerage",
        "commission_discount_rate as brokerage_ratio",
        "cast((first_premium * commission_discount_rate) as decimal(14,4)) as brokerage_fee",
        "case when 1 = 1 then null end as brokerage_pay_status",
        "case when 1 = 1 then null end as remake",
        "now() as create_time",
        "now() as update_time",
        "case when 1 = 1 then null end as operator"
      )
    policyAndPlanAndTeamAndProductRes
  }
}

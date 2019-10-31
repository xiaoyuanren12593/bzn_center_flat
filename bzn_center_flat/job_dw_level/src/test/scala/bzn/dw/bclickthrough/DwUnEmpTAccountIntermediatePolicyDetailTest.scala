package bzn.dw.bclickthrough

import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/10/29
  * Time:14:16
  * describe: 费雇主电子台账
  **/
object DwUnEmpTAccountIntermediatePolicyDetailTest extends SparkUtil with Until{
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    getAllBusinessPolicyDetail(hiveContext)
    //    res.write.mode(SaveMode.Overwrite).saveAsTable("dwdb.dw_policy_premium_detail")
    sc.stop()
  }

  /**
    * 获取所有业务条线的保单数据
    * @param sqlContext 上下文
    */
  def getAllBusinessPolicyDetail(sqlContext:HiveContext) = {

    /**
      * 读取保单明细表
      */
    val odsPolicyDetail= sqlContext.sql("select policy_code,case when source_system = '2.0' then '2' when source_system = '1.0' then '1' end as source_system,policy_status,policy_effect_date," +
      "policy_start_date,policy_end_date,insure_company_name,channel_id,channel_name,sales_name,first_premium,holder_name,invoice_type,product_code,policy_create_time,insured_subject from odsdb.ods_policy_detail")
      .where("policy_status in (0,1,-1) and policy_code is not null")

    /**
      * 读取产品表
      */
    val odsProductDetail = sqlContext.sql("select product_code as product_code_slave,product_name,product_desc,business_line from odsdb.ods_product_detail")

    /**
      * 读取方案表
      */
    val odsPolicyProductPlanDetail = sqlContext.sql("select policy_code as policy_code_slave,sku_coverage,sku_charge_type,sku_ratio,sku_price,sku_append,tech_service_rate,economic_rate,commission_discount_rate from odsdb.ods_policy_product_plan_detail")

    /**
      * 产品表  '体育','场景','员福','健康'
      */
    val odsSportProductDetail = odsProductDetail.where("business_line in ('体育','场景','员福','健康')")

    /**
      * 读取体育销售表
      */
    val odsSportsCustomersDimension = sqlContext.sql("select name,sales_name as sales_name_slave from  odsdb.ods_sports_customers_dimension")

    /**
      * 读取销售团队表
      */
    val odsEntSalesTeamDimension = sqlContext.sql("select sale_name,team_name from odsdb.ods_ent_sales_team_dimension")

    /**
      * 保单表和产品方案表进行关联
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
      * 结果与产品表进行关联
      */
    val policyAndPlanAndProductRes = policyAndPlanRes.join(odsSportProductDetail,policyAndPlanRes("product_code")===odsSportProductDetail("product_code_slave"))
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
        "product_name",
        "product_desc",
        "business_line",
        "sku_coverage",
        "sku_charge_type",
        "sku_ratio",
        "sku_price",
        "sku_append",
        "tech_service_rate",
        "economic_rate",
        "commission_discount_rate"
      )

    val policyAndPlanAndProductSportsSaleRes = policyAndPlanAndProductRes.join(odsSportsCustomersDimension,policyAndPlanAndProductRes("channel_name")===odsSportsCustomersDimension("name"),"leftouter")
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
        "case when sales_name is null and business_line = '体育' then sales_name_slave else sales_name end as sales_name",
        "first_premium",
        "holder_name",
        "insured_subject",
        "invoice_type",
        "product_code",
        "product_name",
        "product_desc",
        "business_line",
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
    val policyAndPlanAndTeamRes = policyAndPlanAndProductSportsSaleRes.join(odsEntSalesTeamDimension,policyAndPlanAndProductSportsSaleRes("sales_name")===odsEntSalesTeamDimension("sale_name"),"leftouter")
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
        "product_name",
        "product_desc",
        "business_line",
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
      *获取体育保单数据信息
      */
    val resPolicy = getSportsScenMemberHealthPolicyDetail(sqlContext,policyAndPlanAndTeamRes)
    resPolicy.printSchema()

  }

  /**
    * 获取体育保单数据信息
    * @param sqlContext 上下文
    */
  def getSportsScenMemberHealthPolicyDetail(sqlContext:HiveContext,policyAndPlanAndTeamRes:DataFrame): DataFrame = {
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("clean", (str: String) => clean(str))

    val policyAndPlanAndTeamAndProductRes = policyAndPlanAndTeamRes
      .where("policy_start_date>='2019-01-01 00:00:00'")
      .selectExpr(
        "getUUID() as id",
        "clean('') as batch_no",
        "policy_code as policy_no",
        "clean('') as preserve_id",
        "clean('') as preserve_status",
        "clean('') as add_batch_code",
        "clean('') as del_batch_code",
        "source_system as data_source",
        "business_line as project_name",
        "product_code",
        "product_name",
        "product_desc as product_detail",
        "channel_name",
        "sales_name as business_owner",
        "team_name as business_region",
        "clean('') as business_source",
        "'3' as business_type",
        "if(policy_start_date >= policy_create_time,policy_start_date,policy_create_time) as performance_accounting_day",
        "holder_name",
        "insured_subject as insurer_name",
        "insure_company_name as underwriting_company",
        "policy_effect_date",
        "policy_start_date as policy_effective_time",
        "policy_end_date as policy_expire_time",
        "cast(policy_status as string) as policy_status",
        "sku_coverage as plan_coverage",
        "first_premium as premium_total",
        "'1' as premium_pay_status",//保费实收状态
        "clean('')  behalf_number",
        "case when invoice_type is null then '0' else cast(invoice_type as string) end as premium_invoice_type",
        "'天津中策' as economy_company",
        "economic_rate as economy_rates",
        "cast((first_premium * economic_rate) as decimal(14,4)) as economy_fee",
        "tech_service_rate as technical_service_rates",
        "cast((first_premium * tech_service_rate) as decimal(14,4)) as technical_service_fee",
        "clean('') as consulting_service_rates",
        "clean('') as consulting_service_fee",
        "clean('') as service_fee_check_time",
        "clean('') as service_fee_check_status",
        "clean('') as has_brokerage",
        "commission_discount_rate as brokerage_ratio",
        "cast((first_premium * commission_discount_rate) as decimal(14,4)) as brokerage_fee",
        "clean('') as brokerage_pay_status",
        "clean('') as remake",
        "now() as create_time",
        "now() as update_time",
        "cast(clean('') as int) as operator"
      )
    policyAndPlanAndTeamAndProductRes
  }
}

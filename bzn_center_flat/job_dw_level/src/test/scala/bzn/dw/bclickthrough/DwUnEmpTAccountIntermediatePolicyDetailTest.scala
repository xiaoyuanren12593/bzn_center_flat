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
      * 读取销售团队表
      */
    val odsEntSalesTeamDimension = sqlContext.sql("select sale_name,team_name from odsdb.ods_ent_sales_team_dimension")

    /**
      * 读取保全表
      */
    val odsPreservationDetail = sqlContext.sql("select preserve_id,add_batch_code,add_premium,del_premium,del_batch_code,effective_date,preserve_start_date," +
      "preserve_end_date,preserve_status,pay_status,policy_code as policy_code_preserve,create_time,preserve_type from odsdb.ods_preservation_detail")
      .where("preserve_status = 1")

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

    /**
      *获取体育保单数据信息
      */
    val resPolicy = getSportsScenMemberHealthPolicyDetail(sqlContext,policyAndPlanAndTeamRes,odsProductDetail)
    resPolicy.printSchema()

  }

  /**
    * 获取体育保单数据信息
    * @param sqlContext 上下文
    */
  def getSportsScenMemberHealthPolicyDetail(sqlContext:HiveContext,policyAndPlanAndTeamRes:DataFrame,odsProductDetail:DataFrame): DataFrame = {
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("clean", (str: String) => clean(str))
    val odsSportProductDetail = odsProductDetail.where("business_line in ('体育','场景','员福','健康')")

    val policyAndPlanAndTeamAndProductRes = policyAndPlanAndTeamRes.join(odsSportProductDetail,policyAndPlanAndTeamRes("product_code")===odsSportProductDetail("product_code_slave"))
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

  /**
    * 体育，员福，场景的批单信息
    * @param sqlContext 上下文
    * @param policyAndPlanAndTeamRes 保单与方案团队结果
    * @param odsProductDetail 产品表
    * @param odsPreservationDetail 保全表
    * @return
    */
  def getSportsScenMemberPreserveDetail(sqlContext:HiveContext,policyAndPlanAndTeamRes:DataFrame,odsProductDetail:DataFrame,odsPreservationDetail:DataFrame): DataFrame = {
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("clean", (str: String) => clean(str))
    val odsSportProductDetail = odsProductDetail.where("business_line in ('体育','场景','员福')")

    val policyAndPlanAndTeamAndProductRes =  policyAndPlanAndTeamRes.join(odsSportProductDetail,policyAndPlanAndTeamRes("product_code")===odsSportProductDetail("product_code_slave"))
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
        "business_line",
        "product_code",
        "product_name",
        "product_desc",
        "sku_coverage",
        "sku_charge_type",
        "sku_ratio",
        "sku_price",
        "sku_append",
        "tech_service_rate",
        "economic_rate",
        "commission_discount_rate"
      )

    val policyAndPlanAndTeamAndProductPreserveRes = policyAndPlanAndTeamAndProductRes.join(odsPreservationDetail,policyAndPlanAndTeamAndProductRes("policy_code")===odsPreservationDetail("policy_code_preserve"))
      .where("if(preserve_start_date is null," +
        "if(preserve_end_date is not null and preserve_end_date>create_time,preserve_end_date,create_time)," +
        "if(preserve_start_date >= create_time,preserve_start_date,create_time)) >= '2019-01-01 00:00:00'")
      .selectExpr(
        "getUUID() as id",
        "clean('')  as batch_no",
        "policy_code as policy_no",
        "preserve_id",
        "cast(preserve_status as string) as preserve_status",
        "add_batch_code",
        "del_batch_code",
        "source_system as data_source",
        "business_line as project_name",
        "product_code",
        "product_name",
        "product_desc as product_detail",
        "channel_name",
        "sales_name as business_owner",
        "team_name as business_region",
        "clean('')  as business_source",
        "cast(preserve_type as string) as business_type",
        "if(preserve_start_date is null," +
          "if(preserve_end_date is not null and preserve_end_date>create_time,preserve_end_date,create_time)," +
          "if(preserve_start_date >= create_time,preserve_start_date,create_time)) as performance_accounting_day",
        "holder_name",
        "insured_subject as insurer_name",
        "insure_company_name as underwriting_company",
        "effective_date as policy_effect_date",
        "preserve_start_date as policy_effective_time",
        "preserve_end_date as policy_expire_time",
        "cast(policy_status as string) as policy_status",
        "sku_coverage as plan_coverage",
        "cast((if(add_premium is null,0,add_premium) + if(del_premium is null,0,del_premium)) as decimal(14,4)) as premium_total",
        "cast(pay_status as string) as premium_pay_status",//保费实收状态
        "clean('')  as behalf_number",
        "case when invoice_type is null then '0' else cast(invoice_type as string) end as premium_invoice_type",
        "'天津中策' as economy_company",
        "economic_rate as economy_rates",
        "cast(((if(add_premium is null,0,add_premium) + if(del_premium is null,0,del_premium)) * economic_rate) as decimal(14,4)) as economy_fee",
        "tech_service_rate as technical_service_rates",
        "cast(((if(add_premium is null,0,add_premium) + if(del_premium is null,0,del_premium)) * tech_service_rate) as decimal(14,4)) as technical_service_fee",
        "clean('')  as consulting_service_rates",
        "clean('')  as consulting_service_fee",
        "clean('')  as service_fee_check_time",
        "clean('')  as service_fee_check_status",
        "clean('')  as has_brokerage",
        "commission_discount_rate as brokerage_ratio",
        "cast(((if(add_premium is null,0,add_premium) + if(del_premium is null,0,del_premium)) * commission_discount_rate) as decimal(14,4)) as brokerage_fee",
        "clean('')  as brokerage_pay_status",
        "clean('')  as remake",
        "now() as create_time",
        "now() as update_time",
        "cast(clean('') as int) as operator"
      )

    policyAndPlanAndTeamAndProductPreserveRes
  }

}

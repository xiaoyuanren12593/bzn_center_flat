package bzn.dw.bclickthrough

import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/10/29
  * Time:14:16
  * describe: 费雇主电子台账
  **/
object DwUnEmpTAccountIntermediatePreserveHealthDetail extends SparkUtil with Until{
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = getAllBusinessPolicyDetail(hiveContext)
    res.repartition(10).write.mode(SaveMode.Append).saveAsTable("dwdb.dw_t_accounts_un_employer_intermediate_detail")
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
      * 读取健康批单数据
      */
    val odsHealthPreserceDetail = sqlContext.sql("select insurance_policy_no as policy_code_preserve,preserve_id,premium_total,holder_name as holder_name_master,insurer_name,channel_name as channel_name_master," +
      "policy_effective_time,create_time from odsdb.ods_health_preserce_detail")

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

    val resPreserve = getHealthMemberPreserveDetail(sqlContext,policyAndPlanAndTeamRes,odsProductDetail,odsHealthPreserceDetail)
    resPreserve
  }

  /**
    * 健康的批单信息
    * @param sqlContext 上下文
    * @param policyAndPlanAndTeamRes 保单与方案团队结果
    * @param odsProductDetail 产品表
    * @param odsHealthPreserceDetail 保全表
    * @return
    */
  def getHealthMemberPreserveDetail(sqlContext:HiveContext,policyAndPlanAndTeamRes:DataFrame,odsProductDetail:DataFrame,odsHealthPreserceDetail:DataFrame): DataFrame = {
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("clean", (str: String) => clean(str))
    val odsSportProductDetail = odsProductDetail.where("business_line in ('健康')")

    val policyAndPlanAndTeamAndProductRes =  policyAndPlanAndTeamRes.join(odsSportProductDetail,policyAndPlanAndTeamRes("product_code")===odsSportProductDetail("product_code_slave"))
      .selectExpr(
        "policy_code",
        "source_system",
<<<<<<< HEAD
        "policy_status","policy_effect_date",
=======
        "policy_status",
        "policy_effect_date",
>>>>>>> fea-xwc-add-user-label
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

    val policyAndPlanAndTeamAndProductPreserveRes = odsHealthPreserceDetail.join(policyAndPlanAndTeamAndProductRes,odsHealthPreserceDetail("policy_code_preserve")===policyAndPlanAndTeamAndProductRes("policy_code"))
      .where("policy_start_date >= '2019-01-01 00:00:00'")
      .selectExpr(
        "getUUID() as id",
        "clean('')  as batch_no",
        "policy_code as policy_no",
        "preserve_id",
        "clean('') as preserve_status",
        "clean('') as add_batch_code",
        "clean('') as del_batch_code",
        "source_system as data_source",
        "business_line as project_name",
        "product_code",
        "product_name",
        "product_desc as product_detail",
        "channel_name_master as channel_name",
        "sales_name as business_owner",
        "team_name as business_region",
        "clean('')  as business_source",
        "'1' as business_type",
        "policy_effective_time as performance_accounting_day",
        "holder_name_master",
        "insurer_name",
        "insure_company_name as underwriting_company",
        "cast(clean('') as timestamp) as policy_effect_date",
        "policy_effective_time",
        "cast(clean('') as timestamp) as policy_expire_time",
        "cast(policy_status as string) as policy_status",
        "sku_coverage as plan_coverage",
        "premium_total as premium_total",
        "'1' as premium_pay_status",//保费实收状态
        "clean('')  as behalf_number",
        "case when invoice_type is null then '0' else cast(invoice_type as string) end as premium_invoice_type",
        "'天津中策' as economy_company",
        "economic_rate as economy_rates",
        "cast((premium_total * economic_rate) as decimal(14,4)) as economy_fee",
        "tech_service_rate as technical_service_rates",
        "cast((premium_total * tech_service_rate) as decimal(14,4)) as technical_service_fee",
        "clean('')  as consulting_service_rates",
        "clean('')  as consulting_service_fee",
        "clean('')  as service_fee_check_time",
        "clean('')  as service_fee_check_status",
        "clean('')  as has_brokerage",
        "commission_discount_rate as brokerage_ratio",
        "cast((premium_total * commission_discount_rate) as decimal(14,4)) as brokerage_fee",
        "clean('')  as brokerage_pay_status",
        "clean('')  as remake",
        "now() as create_time",
        "now() as update_time",
        "cast(clean('') as int) as operator"
      )

    policyAndPlanAndTeamAndProductPreserveRes
  }

}

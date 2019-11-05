package bzn.dw.bclickthrough

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.bclickthrough.DwEmpTAccountsIntermediateDetailRes.clean
import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/11/5
* @Describe:
*/ object DwEmployerPreserveDetail extends SparkUtil with Until {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hqlContext = sparkConf._4
    val res = EmployerPreserveDetail(hqlContext)
    res.write.mode(SaveMode.Append).saveAsTable("dwdb.dw_t_accounts_employer_intermediate")
    sc.stop()
  }



  /**
    * 添加批单数据
    *
    * @param hqlContext
    */
  def EmployerPreserveDetail(hqlContext: HiveContext): DataFrame = {
    import hqlContext.implicits._
    hqlContext.udf.register("clean", (str: String) => clean(str))
    hqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      date + ""
    })

    /**
      * 读取批单表
      */

    val odsPolicyPreserveDetail = hqlContext.sql("select policy_id,preserve_id,policy_code,add_batch_code," +
      "del_batch_code,add_premium,del_premium,preserve_start_date,preserve_end_date,effective_date,preserve_type,pay_status,create_time,preserve_status from odsdb.ods_preservation_detail")

    /**
      * 读取保单明细表
      */

    val odsPolicyDetail = hqlContext.sql("select policy_id as policy_id_salve,product_code as insure_code,holder_name,insure_company_name,source_system,invoice_type,insured_subject,policy_status from odsdb.ods_policy_detail")


    /**
      * 批单表关联保单明细表
      */

    val policyAndPreserveDetailRes = odsPolicyPreserveDetail.join(odsPolicyDetail, 'policy_id === 'policy_id_salve, "leftouter")
      .selectExpr("policy_id", "preserve_id", "policy_code", "add_batch_code", "del_batch_code", "add_premium", "del_premium", "preserve_start_date", "preserve_end_date", "effective_date",
        "preserve_type", "pay_status", "create_time", "preserve_status", "insure_code", "holder_name", "insure_company_name", "source_system", "invoice_type", "insured_subject", "policy_status")


    /**
      * 读取产品方案表
      */

    val productPlan = hqlContext.sql("select policy_code as policy_code_salve, sku_charge_type,sku_price,sku_ratio,sku_append,sku_coverage,economic_rate,tech_service_rate,commission_discount_rate from odsdb.ods_policy_product_plan_detail")

    //关联产品方案表
    val preserveAndPorductPlan = policyAndPreserveDetailRes.join(productPlan, 'policy_code === 'policy_code_salve, "leftouter")
      .selectExpr("policy_id", "preserve_id", "policy_code", "add_batch_code", "del_batch_code", "add_premium", "del_premium", "preserve_start_date", "preserve_end_date", "effective_date",
        "preserve_type", "pay_status", "create_time", "preserve_status", "insure_code", "holder_name", "insure_company_name", "source_system", "invoice_type", "insured_subject", "policy_status",
        "sku_charge_type", "sku_price", "sku_ratio", "sku_append", "sku_coverage", "economic_rate", "tech_service_rate", "commission_discount_rate")

    /**
      * 读取销售表和团队表
      */

    val odsEntGuzhuSale = hqlContext.sql("select salesman,biz_operator,ent_name,channel_name,business_source from odsdb.ods_ent_guzhu_salesman_detail")

    val odsEntSaleTeam = hqlContext.sql("select sale_name,team_name from odsdb.ods_ent_sales_team_dimension")


    /**
      * 关联 销售表和团队表
      */
    val preserveAndSale = preserveAndPorductPlan.join(odsEntGuzhuSale, 'holder_name === 'ent_name, "leftouter")
      .selectExpr("policy_id", "preserve_id", "policy_code", "add_batch_code", "del_batch_code", "add_premium", "del_premium", "preserve_start_date", "preserve_end_date", "effective_date",
        "preserve_type", "pay_status", "create_time", "preserve_status", "insure_code", "holder_name", "insure_company_name", "source_system", "invoice_type", "insured_subject",
        "policy_status", "sku_charge_type", "sku_price", "sku_ratio", "sku_append", "sku_coverage", "economic_rate", "tech_service_rate", "commission_discount_rate", "salesman", "biz_operator", "business_source", "ent_name", "channel_name")


    val preserveAndsaleAndTeam = preserveAndSale.join(odsEntSaleTeam, 'salesman === 'sale_name, "leftouter")
      .selectExpr("policy_id", "preserve_id", "policy_code", "add_batch_code", "del_batch_code", "add_premium", "del_premium", "preserve_start_date", "preserve_end_date", "effective_date",
        "preserve_type", "pay_status", "create_time", "preserve_status", "insure_code", "holder_name", "insure_company_name", "source_system", "invoice_type", "insured_subject",
        "policy_status", "sku_charge_type", "sku_price", "sku_ratio", "sku_append", "sku_coverage", "economic_rate", "tech_service_rate", "commission_discount_rate", "salesman", "biz_operator", "business_source", "ent_name", "channel_name", "team_name")

    /**
      * 读取产品表
      */

    val odsPolicyProduct = hqlContext.sql("select product_desc,product_code,product_name,two_level_pdt_cate from odsdb.ods_product_detail")
    /**
      * 关联产品表
      */
    val resTemp = preserveAndsaleAndTeam.join(odsPolicyProduct, 'insure_code === 'product_code, "leftouter")
      .selectExpr("policy_id", "preserve_id", "policy_code", "add_batch_code", "del_batch_code",
        "case when add_premium is null then 0 else add_premium end as add_premium",
        "case when del_premium is null then 0 else del_premium end as del_premium",
        "preserve_start_date", "preserve_end_date", "effective_date",
        "preserve_type", "pay_status", "create_time", "preserve_status", "insure_code", "holder_name", "insure_company_name", "source_system", "invoice_type", "insured_subject",
        "policy_status", "sku_charge_type", "sku_price", "sku_ratio", "sku_append", "sku_coverage", "economic_rate", "tech_service_rate", "commission_discount_rate", "salesman", "biz_operator", "business_source", "ent_name", "channel_name", "team_name", "product_desc", "product_name", "two_level_pdt_cate")
      .where("policy_status in (0,1,-1) and if(preserve_start_date is null," +
        "if(preserve_end_date is null,create_time>=cast('2019-01-01' as timestamp),preserve_end_date>=cast('2019-01-01' as timestamp)),preserve_start_date >=cast('2019-01-01' as timestamp)) and preserve_status = 1 " +
        "and two_level_pdt_cate in ('外包雇主', '骑士保', '大货车', '零工保')")


    val res = resTemp.selectExpr(
      "getUUID() as id",
      "'' as batch_no",
      "policy_code as policy_no",
      "preserve_id",
      "add_batch_code",
      "del_batch_code",
      "cast(preserve_status as string) as preserve_status",
      "case source_system when '1.0' then '1' when '2.0' then '2' else source_system end as data_source",
      "product_desc as project_name",
      "insure_code as product_code",
      "product_name",
      "if(trim(channel_name)='直客',trim(ent_name),channel_name)  as channel_name",
      "salesman as business_owner",
      "team_name as business_region",
      "business_source",
      "cast(case preserve_type when 1 then 0 when 2 then 2 when 5 then 5 else preserve_type end as string) as business_type",
      "if(preserve_start_date is null,if(preserve_end_date is not null and preserve_end_date>=create_time,preserve_end_date,create_time),if(preserve_start_date>=create_time,preserve_start_date,create_time)) as performance_accounting_day",
      "biz_operator as operational_name",
      "holder_name",
      "insured_subject as insurer_name",
      "sku_price as plan_price",
      "sku_coverage as plan_coverage",
      "sku_append  as plan_append",
      "cast(if(sku_ratio is null,0,if(sku_ratio = 1,0.05,if(sku_ratio=2,0.1,sku_ratio))) as decimal(14,4)) as plan_disability_rate",
      "if(two_level_pdt_cate = '零工保','3',sku_charge_type) as plan_pay_type",
      "insure_company_name as underwriting_company",
      "effective_date as policy_effect_date",
      "preserve_start_date as policy_start_time",
      "case when preserve_start_date is null then (case when preserve_end_date is null then effective_date end) end as policy_effective_time",
      "preserve_end_date as policy_expire_time",
      "cast(policy_status as string) as policy_status",
      "cast((add_premium + del_premium) as decimal(14,4)) as premium_total",
      "cast(case pay_status when 1 then 0 when 2 then 1 when 3 then 3 else pay_status end as string) as premium_pay_status",
      "cast(invoice_type as string) as premium_invoice_type",
      "'' as economy_company",
      "economic_rate as economy_rates",
      "cast((add_premium + del_premium) * economic_rate as decimal(14,4))as economy_fee",
      "tech_service_rate as technical_service_rates",
      "round(cast((add_premium + del_premium) * tech_service_rate as decimal(14,4)),4) as technical_service_fee",
      "cast(1 as decimal(14,4)) as consulting_service_rates",
      "cast(1 as decimal(14,4)) as consulting_service_fee",
      "cast(getNow() as timestamp) as service_fee_check_time",
      "'' as service_fee_check_status",
      "'' as has_brokerage",
      "case when commission_discount_rate is null then 0 else commission_discount_rate end as brokerage_ratio",
      "cast((add_premium + del_premium) * commission_discount_rate as decimal(14,4))  as brokerage_fee",
      "'' as brokerage_pay_status",
      "'' as remake",
      "cast(getNow() as timestamp) as create_time",
      "cast(getNow() as timestamp) as update_time",
      "1 as operator")
    res
  }
}

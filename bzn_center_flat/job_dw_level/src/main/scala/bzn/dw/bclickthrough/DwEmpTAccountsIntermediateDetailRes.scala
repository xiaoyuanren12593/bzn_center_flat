package bzn.dw.bclickthrough

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

/*
* @Author:liuxiang
* @Date：2019/11/5
* @Describe:
*/
object DwEmpTAccountsIntermediateDetailRes extends SparkUtil with Until {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hqlContext = sparkConf._4
    val res = EmployerPolicyDetai(hqlContext)
    hqlContext.sql("truncate table dwdb.dw_t_accounts_employer_intermediate")
    res.write.mode(SaveMode.Append).saveAsTable("dwdb.dw_t_accounts_employer_intermediate")

    sc.stop()
  }



  def getDF(sc:SparkContext): Unit ={
    // 数组
    val arr = Array("batch_no","add_batch_code","del_batch_code","preserve_status","premium_pay_status","service_fee_check_status","has_brokerage","brokerage_pay_status","remake","operator")
    val rdd: RDD[String] = sc.parallelize(arr)
  }


  /**
    * 添加保单数据
    *
    * @param hqlContext
    */
  def EmployerPolicyDetai(hqlContext: HiveContext): DataFrame = {
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
      * 读取保单明细表
      */

    val odsPolicyDetail = hqlContext.sql("select id,policy_code,case source_system when '1.0' then '1' when '2.0' then '2' end as data_source," +
      "policy_status,policy_effect_date,policy_start_date,policy_end_date,product_code,insure_company_name,f" +
      "irst_premium,holder_name,insured_subject,invoice_type,preserve_policy_no,policy_create_time from odsdb.ods_policy_detail")
      .repartition(200)

    /**
      * 读取客户归属表
      */

    val odsEntGuzhuDetail = hqlContext.sql("select salesman,ent_name,biz_operator,channel_name,business_source from odsdb.ods_ent_guzhu_salesman_detail")

    /**
      * 保单明细表关联客户归属信息表
      */
    val policyAndGuzhuRes = odsPolicyDetail.join(odsEntGuzhuDetail, 'holder_name === 'ent_name, "leftouter")
      .selectExpr("policy_code", "product_code", "data_source", "policy_status", "policy_effect_date", "policy_start_date", "policy_end_date", "insure_company_name", "first_premium",
        "holder_name", "insured_subject", "invoice_type", "salesman", "ent_name", "channel_name", "biz_operator", "business_source", "preserve_policy_no", "policy_create_time")

    /**
      * 读取销售团队表
      */
    val odsEntSalesTeam = hqlContext.sql("select team_name,sale_name from odsdb.ods_ent_sales_team_dimension")

    /**
      * 将结果关联销售团队表
      */
    val policyAndGuzhuSalve = policyAndGuzhuRes.join(odsEntSalesTeam, 'salesman === 'sale_name, "leftouter")
      .selectExpr("policy_code", "product_code", "data_source", "policy_status", "policy_effect_date", "policy_start_date", "policy_end_date",
        "insure_company_name", "first_premium", "holder_name", "insured_subject", "invoice_type", "salesman", "team_name", "ent_name", "channel_name", "biz_operator", "business_source", "preserve_policy_no", "policy_create_time")

    /**
      * 读取方案类别表
      */
    val odsPolicyProductPlanDetail =
      hqlContext.sql("select policy_code as policy_code_salve,sku_price,sku_ratio,sku_append,sku_coverage,economic_rate,tech_service_rate,sku_charge_type,commission_discount_rate from odsdb.ods_policy_product_plan_detail")

    /**
      * 将上述结果关联方案类别表
      */
    val policyAndProductPlan = policyAndGuzhuSalve.join(odsPolicyProductPlanDetail, 'policy_code === 'policy_code_salve, "leftouter")
      .selectExpr("policy_code", "product_code", "data_source", "policy_status", "policy_effect_date", "policy_start_date", "policy_end_date",
        "insure_company_name", "first_premium", "holder_name", "insured_subject", "invoice_type", "salesman", "team_name", "ent_name", "channel_name", "biz_operator", "business_source",
        "sku_price", "sku_ratio", "sku_append", "sku_coverage", "economic_rate", "tech_service_rate", "sku_charge_type", "preserve_policy_no", "commission_discount_rate", "policy_create_time")

    /**
      * 读取产品表
      */
    val odsProductDetail = hqlContext.sql("select product_desc,product_code as insure_code,product_name,two_level_pdt_cate from odsdb.ods_product_detail")


    /**
      * 关联产品表
      */
    val resTemp = policyAndProductPlan.join(odsProductDetail, 'product_code === 'insure_code, "leftouter")
      .selectExpr("policy_code", "product_code", "product_desc", "product_name", "two_level_pdt_cate", "data_source", "policy_status", "policy_effect_date", "policy_start_date", "policy_end_date",
        "insure_company_name", "first_premium", "holder_name", "insured_subject", "invoice_type", "salesman", "team_name", "ent_name", "channel_name", "biz_operator", "business_source", "sku_price",
        "sku_ratio", "sku_append", "sku_coverage","economic_rate","tech_service_rate", "sku_charge_type", "preserve_policy_no", "commission_discount_rate", "policy_create_time")
      .where("policy_code !='' and policy_status in(0, 1) and policy_start_date >=cast('2019-01-01' as timestamp) and two_level_pdt_cate in ('外包雇主', '骑士保', '大货车', '零工保')")


    /**
      * 将结果注册成临时表
      */
    val res: DataFrame = resTemp.selectExpr(
      "getUUID() as id",
      "'' as batch_no",
      "policy_code as policy_no",
      "case preserve_id else  preserve_id'' as preserve_id",
      "'' as add_batch_code",
      "'' as del_batch_code",
      "'' as preserve_status",
      "data_source",
      "product_desc as project_name",
      "product_code",
      "product_name",
      "if(trim(channel_name)='直客',trim(ent_name),trim(channel_name)) as channel_name",
      "salesman as business_owner",
      "team_name as business_region",
      "business_source",
      "cast(if(preserve_policy_no is null,1,2) as string) as business_type",
      "if(policy_start_date >=policy_create_time,policy_start_date,policy_create_time) as performance_accounting_day",
      "biz_operator as operational_name",
      "holder_name",
      "insured_subject as insurer_name",
      "sku_price as plan_price",
      "sku_coverage as plan_coverage",
      "sku_append as plan_append",
      "cast(if(sku_ratio is null,0,if(sku_ratio = 1, 0.05,if(sku_ratio = 2, 0.1,sku_ratio))) as decimal(14,4)) as plan_disability_rate",
      "if(two_level_pdt_cate = '零工保','3',sku_charge_type) as plan_pay_type",
      "insure_company_name as underwriting_company",
      "policy_effect_date",
      "cast(now() as timestamp) as policy_start_time",
      "policy_start_date as policy_effective_time",
      "policy_end_date as policy_expire_time",
      "cast(policy_status as string) as policy_status",
      "first_premium as premium_total",
      "'' as premium_pay_status",
      "cast(invoice_type as string) as premium_invoice_type",
      "'' as economy_company",
      "economic_rate as economy_rates",
      "cast(first_premium * economic_rate as decimal(14,4)) as economy_fee",
      "tech_service_rate as technical_service_rates",
      "cast(first_premium * tech_service_rate as decimal(14,4)) as technical_service_fee",
      "cast(1 as decimal(14,4)) as consulting_service_rates",
      "cast(1 as decimal(14,4)) as consulting_service_fee",
      "cast(now() as timestamp) as service_fee_check_time",
      "'' as service_fee_check_status",
      "'' as has_brokerage",
      "case when commission_discount_rate is  null then 0 else commission_discount_rate end as brokerage_ratio",
      "cast(case when (first_premium * commission_discount_rate) is not null then (first_premium * commission_discount_rate) else 0 end as decimal(14,4)) as brokerage_fee",
      "'' as brokerage_pay_status",
      "'' as remake",
      "cast(now() as timestamp) as create_time",
      "cast(now() as timestamp) as update_time",
      "1 as operator").repartition(200)
    res
  }
}